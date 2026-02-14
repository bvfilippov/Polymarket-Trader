"""
Real-time trading strategy — matches Binance-derived BTC directional signals
to Polymarket prediction markets and generates trade decisions.

Operates on LiveMarketState which is updated continuously via WebSocket.
"""

import re
import time
from dataclasses import dataclass
from typing import Optional

from src.binance_data import LiveMarketState, estimate_btc_direction
from src.config import MAX_POSITION_SIZE, MIN_EDGE, TRADE_COOLDOWN, get_logger
from src.polymarket_client import BtcMarket

log = get_logger(__name__)


@dataclass
class TradeSignal:
    """A trade signal ready to be executed."""
    market: BtcMarket
    token_id: str
    side: str  # "BUY" or "SELL"
    amount: float  # USDC amount
    edge: float  # our estimated edge
    our_probability: float
    market_probability: float
    direction: str  # "up" or "down"
    reasons: list[str]
    urgency: float  # 0.0-1.0, how urgent (based on signal strength + leading indicators)


def parse_market_direction(market: BtcMarket) -> Optional[str]:
    """
    Determine what a YES outcome means for this market.
    Returns "up" if YES means BTC goes up, "down" if YES means BTC goes down.
    """
    q = market.question.lower()

    # "Up or down" style markets: YES typically means "up"
    if re.search(r"up\s*(or|/)\s*down", q):
        return "up"

    # Check down patterns first (more specific keywords like "drop", "below", "fall")
    down_patterns = [
        r"bitcoin.*below",
        r"btc.*below",
        r"bitcoin.*under",
        r"btc.*under",
        r"bitcoin.*down\b",
        r"btc.*down\b",
        r"bitcoin.*drop",
        r"btc.*drop",
        r"bitcoin.*fall",
        r"btc.*fall",
        r"bitcoin.*lower",
        r"btc.*lower",
    ]

    for pattern in down_patterns:
        if re.search(pattern, q):
            return "down"

    # Patterns indicating YES = BTC goes up
    up_patterns = [
        r"bitcoin.*above",
        r"btc.*above",
        r"bitcoin.*over",
        r"btc.*over",
        r"bitcoin.*reach",
        r"btc.*reach",
        r"bitcoin.*hit",
        r"btc.*hit",
        r"bitcoin.*up\b",
        r"btc.*up\b",
        r"bitcoin.*higher",
        r"btc.*higher",
    ]

    for pattern in up_patterns:
        if re.search(pattern, q):
            return "up"

    # For "up or down" style markets, check which side the title favors
    if "up or down" in q:
        return "up"  # YES typically means "up" in these markets

    return None


def extract_price_target(market: BtcMarket) -> Optional[float]:
    """Try to extract a BTC price target from the market question."""
    q = market.question

    # Match patterns like "$100,000", "$100k", "100000", "100k"
    patterns = [
        r"\$?([\d,]+(?:\.\d+)?)\s*k\b",  # "100k"
        r"\$?([\d,]+(?:\.\d+)?)\b",  # "$100,000" or "100000"
    ]

    for pattern in patterns:
        matches = re.findall(pattern, q)
        for match in matches:
            try:
                value = float(match.replace(",", ""))
                if "k" in q[q.find(match):q.find(match) + len(match) + 2].lower():
                    value *= 1000
                # Sanity check: BTC price should be in a reasonable range
                if 1000 < value < 1_000_000:
                    return value
            except ValueError:
                continue

    return None


def _compute_urgency(state: LiveMarketState, edge: float) -> float:
    """
    Compute urgency score based on how strongly leading indicators agree.
    High urgency = act now, low urgency = can wait.
    """
    urgency = 0.0

    # Strong book imbalance = urgent
    urgency += min(abs(state.book_imbalance), 0.4) * 0.5

    # Extreme trade flow = urgent
    flow_deviation = abs(state.trade_flow_ratio - 0.5)
    urgency += min(flow_deviation, 0.2) * 1.0

    # Whale activity = urgent
    whale_total = state.large_buy_count_5m + state.large_sell_count_5m
    if whale_total > 0:
        urgency += 0.15

    # Large edge = urgent
    urgency += min(abs(edge), 0.15) * 1.5

    return min(urgency, 1.0)


def generate_signals(
    state: LiveMarketState,
    markets: list[BtcMarket],
    last_trade_times: dict[str, float],
) -> list[TradeSignal]:
    """
    Match real-time Binance directional estimate with Polymarket markets
    and generate trade signals where we have edge.

    Args:
        state: Live market state from WebSocket streams
        markets: List of active BTC markets with prices
        last_trade_times: condition_id -> last trade timestamp (for cooldown)
    """
    if not state.ready:
        log.debug("Market state not ready yet, skipping signal generation")
        return []

    direction_estimate = estimate_btc_direction(state)
    our_direction = direction_estimate["direction"]
    our_confidence = direction_estimate["confidence"]
    reasons = direction_estimate["reasons"]

    signals = []
    now = time.time()

    for market in markets:
        # Cooldown check
        cid = market.condition_id
        last_trade = last_trade_times.get(cid, 0)
        if now - last_trade < TRADE_COOLDOWN:
            continue

        market_direction = parse_market_direction(market)
        if market_direction is None:
            continue

        # Determine what our model says the YES probability should be
        if market_direction == "up":
            our_yes_prob = our_confidence
        else:
            our_yes_prob = 1.0 - our_confidence

        # Get market's current YES price (= market's implied probability)
        market_yes_price = market.current_price_yes
        if market_yes_price is None or market_yes_price <= 0:
            continue

        # Calculate edge
        edge = our_yes_prob - market_yes_price

        # Discount edge for distant price targets
        price_target = extract_price_target(market)
        if price_target is not None and state.price > 0:
            distance_pct = abs(state.price - price_target) / state.price * 100
            if distance_pct > 10:
                edge *= 0.5

        if abs(edge) < MIN_EDGE:
            continue

        # Compute urgency from leading indicators
        urgency = _compute_urgency(state, edge)

        # Size based on edge magnitude and urgency
        size_factor = min(abs(edge) / 0.15, 1.0) * (0.5 + 0.5 * urgency)
        amount = round(MAX_POSITION_SIZE * size_factor, 2)
        amount = max(1.0, min(amount, MAX_POSITION_SIZE))

        # Decide which token to buy
        if edge > 0:
            token_id = market.token_id_yes
        else:
            token_id = market.token_id_no

        signal = TradeSignal(
            market=market,
            token_id=token_id,
            side="BUY",
            amount=amount,
            edge=edge,
            our_probability=our_yes_prob,
            market_probability=market_yes_price,
            direction=our_direction,
            reasons=reasons,
            urgency=urgency,
        )
        signals.append(signal)

    # Sort by urgency * |edge| (best immediate opportunities first)
    signals.sort(key=lambda s: s.urgency * abs(s.edge), reverse=True)
    return signals

"""
Trading strategy — matches Binance-derived BTC directional signals
to Polymarket prediction markets and generates trade decisions.
"""

import re
from dataclasses import dataclass
from typing import Optional

from src.binance_data import BinanceSnapshot, estimate_btc_direction
from src.config import MAX_POSITION_SIZE, MIN_EDGE, get_logger
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


def generate_signals(
    snapshot: BinanceSnapshot,
    markets: list[BtcMarket],
) -> list[TradeSignal]:
    """
    Match Binance directional estimate with Polymarket markets
    and generate trade signals where we have edge.
    """
    direction_estimate = estimate_btc_direction(snapshot)
    our_direction = direction_estimate["direction"]
    our_confidence = direction_estimate["confidence"]
    reasons = direction_estimate["reasons"]

    log.info(
        "Strategy: BTC direction=%s, confidence=%.3f",
        our_direction, our_confidence,
    )

    signals = []

    for market in markets:
        market_direction = parse_market_direction(market)
        if market_direction is None:
            log.debug("Skipping market (can't parse direction): %s", market.question)
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

        # Also consider the price target relative to current price
        price_target = extract_price_target(market)
        if price_target is not None:
            distance_pct = abs(snapshot.price - price_target) / snapshot.price * 100
            # If the target is very far from current price, reduce our confidence
            if distance_pct > 10:
                edge *= 0.5  # Halve edge for distant targets

        log.info(
            "Market: '%s' | direction=%s | our_yes=%.3f | mkt_yes=%.3f | edge=%.3f",
            market.question[:50], market_direction,
            our_yes_prob, market_yes_price, edge,
        )

        if abs(edge) < MIN_EDGE:
            log.debug("Edge too small (%.3f < %.3f), skipping", abs(edge), MIN_EDGE)
            continue

        # Decide trade direction and sizing
        if edge > 0:
            # Our YES prob > market price → BUY YES
            token_id = market.token_id_yes
            side = "BUY"
            amount = min(MAX_POSITION_SIZE, MAX_POSITION_SIZE * (abs(edge) / 0.2))
        else:
            # Our YES prob < market price → BUY NO (equivalent to selling YES)
            token_id = market.token_id_no
            side = "BUY"
            amount = min(MAX_POSITION_SIZE, MAX_POSITION_SIZE * (abs(edge) / 0.2))

        signal = TradeSignal(
            market=market,
            token_id=token_id,
            side=side,
            amount=round(amount, 2),
            edge=edge,
            our_probability=our_yes_prob,
            market_probability=market_yes_price,
            direction=our_direction,
            reasons=reasons,
        )
        signals.append(signal)

    # Sort by absolute edge (best opportunities first)
    signals.sort(key=lambda s: abs(s.edge), reverse=True)

    log.info("Generated %d trade signals", len(signals))
    return signals

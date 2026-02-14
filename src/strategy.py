"""
Lag-arbitrage strategy.

Core idea:
  Polymarket BTC prediction prices FOLLOW the real BTC price on Binance
  with a delay. When BTC makes a sharp move on Binance, Polymarket
  hasn't adjusted yet — we trade the gap.

Logic:
  1. Detect spike on Binance (price moved X% in last N seconds)
  2. Check Polymarket price for BTC prediction markets
     (these prices were set BEFORE the Binance move)
  3. Estimate what the Polymarket price SHOULD be after the move
  4. If delta > MIN_EDGE → trade immediately
"""

import re
import time
from dataclasses import dataclass
from typing import Optional

from src.binance_data import LiveMarketState, Spike, get_price_moves
from src.config import LIMIT_ORDER_OFFSET, MAX_POSITION_SIZE, MIN_EDGE, TRADE_COOLDOWN, get_logger
from src.polymarket_client import BtcMarket

log = get_logger(__name__)


@dataclass
class TradeSignal:
    market: BtcMarket
    token_id: str
    side: str  # "BUY"
    amount: float
    edge: float  # our estimated edge (lag delta)
    direction: str  # "up" or "down" — direction of BTC move
    btc_move_pct: float  # how much BTC moved
    market_price_stale: float  # Polymarket's (stale) price
    market_price_fair: float  # our estimate of fair price
    spike_strength: float
    reasons: list[str]
    limit_price: float = 0.0  # suggested limit order price


# ─── Market analysis ──────────────────────────────────────────────


def parse_market_direction(market: BtcMarket) -> Optional[str]:
    """Returns 'up' if YES means BTC goes up, 'down' if YES means BTC goes down."""
    q = market.question.lower()

    if re.search(r"up\s*(or|/)\s*down", q):
        return "up"

    down_patterns = [
        r"bitcoin.*below", r"btc.*below",
        r"bitcoin.*under", r"btc.*under",
        r"bitcoin.*down\b", r"btc.*down\b",
        r"bitcoin.*drop", r"btc.*drop",
        r"bitcoin.*fall", r"btc.*fall",
        r"bitcoin.*lower", r"btc.*lower",
    ]
    for p in down_patterns:
        if re.search(p, q):
            return "down"

    up_patterns = [
        r"bitcoin.*above", r"btc.*above",
        r"bitcoin.*over", r"btc.*over",
        r"bitcoin.*reach", r"btc.*reach",
        r"bitcoin.*hit", r"btc.*hit",
        r"bitcoin.*up\b", r"btc.*up\b",
        r"bitcoin.*higher", r"btc.*higher",
    ]
    for p in up_patterns:
        if re.search(p, q):
            return "up"

    return None


def extract_price_target(market: BtcMarket) -> Optional[float]:
    """Extract a BTC price target from the market question."""
    q = market.question
    patterns = [
        r"\$?([\d,]+(?:\.\d+)?)\s*k\b",
        r"\$?([\d,]+(?:\.\d+)?)\b",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, q)
        for match in matches:
            try:
                value = float(match.replace(",", ""))
                if "k" in q[q.find(match):q.find(match) + len(match) + 2].lower():
                    value *= 1000
                if 1000 < value < 1_000_000:
                    return value
            except ValueError:
                continue
    return None


# ─── Fair price estimation ────────────────────────────────────────


def estimate_fair_yes_price(
    market: BtcMarket,
    market_direction: str,
    btc_price: float,
    btc_move_pct: float,
    stale_yes_price: float,
) -> float:
    """
    Estimate what the YES price SHOULD be given that BTC just moved.

    The key insight: if BTC moved +0.3% and Polymarket's "BTC above $X"
    hasn't adjusted, the YES price should be higher than what's showing.

    We model the adjustment as: the market was priced at some implied
    probability. A BTC move shifts that probability. The Polymarket price
    hasn't caught up yet.
    """
    price_target = extract_price_target(market)

    if price_target is not None and btc_price > 0:
        # We can estimate the probability shift more precisely
        # How much closer/further are we from the target?
        old_distance_pct = abs(btc_price / (1 + btc_move_pct / 100) - price_target) / price_target * 100
        new_distance_pct = abs(btc_price - price_target) / price_target * 100

        # Direction matters: are we moving TOWARD or AWAY from the target?
        if market_direction == "up":
            # YES = BTC above target
            if btc_move_pct > 0:
                # BTC went up → closer to/above target → YES should increase
                delta = _probability_shift(old_distance_pct, new_distance_pct, btc_price, price_target)
            else:
                # BTC went down → further from target → YES should decrease
                delta = -_probability_shift(new_distance_pct, old_distance_pct, btc_price, price_target)
        else:
            # YES = BTC below target
            if btc_move_pct < 0:
                # BTC went down → closer to/below target → YES should increase
                delta = _probability_shift(old_distance_pct, new_distance_pct, btc_price, price_target)
            else:
                # BTC went up → further from target → YES should decrease
                delta = -_probability_shift(new_distance_pct, old_distance_pct, btc_price, price_target)

        fair_price = stale_yes_price + delta
    else:
        # No price target parseable — use simpler heuristic
        # Bigger BTC move → bigger expected price adjustment
        sensitivity = 0.5  # how much 1% BTC move shifts YES price
        if market_direction == "up":
            delta = btc_move_pct / 100 * sensitivity
        else:
            delta = -btc_move_pct / 100 * sensitivity

        fair_price = stale_yes_price + delta

    return max(0.01, min(0.99, fair_price))


def _probability_shift(
    old_distance: float,
    new_distance: float,
    btc_price: float,
    target: float,
) -> float:
    """
    Estimate probability shift based on how distance to target changed.

    Uses gamma-like model: sensitivity is highest near the target.
    At 0.2% from target, even a 0.3% BTC move creates a big probability shift.
    At 5% from target, same move barely matters.
    """
    distance_change = abs(old_distance - new_distance)
    nearest = min(old_distance, new_distance)

    # Gamma: sensitivity = 1 / (distance + epsilon)
    # At 0.1% distance: gamma = 10, at 1% = 1, at 5% = 0.2
    gamma = 1.0 / (nearest + 0.1)

    # Base shift scaled by gamma
    shift = distance_change * gamma * 0.1

    # Crossed the target = discontinuous jump
    crossed = (btc_price >= target) != (
        btc_price / (1 + (old_distance - new_distance) / 100) >= target
    )
    if crossed:
        shift = max(shift, 0.08)
        shift *= 1.5

    return min(shift, 0.20)


# ─── Signal generation ────────────────────────────────────────────


def generate_signals(
    state: LiveMarketState,
    markets: list[BtcMarket],
    last_trade_times: dict[str, float],
) -> list[TradeSignal]:
    """
    Generate lag-arbitrage signals.
    Finds markets where Polymarket price hasn't caught up to a Binance move.
    """
    if not state.ready:
        return []

    # Get price moves over different windows
    moves = get_price_moves(state)
    if not moves:
        return []

    # Find the most significant move
    best_move_key = max(moves, key=lambda k: abs(moves[k]))
    best_move_pct = moves[best_move_key]

    # No move worth trading
    if abs(best_move_pct) < 0.05:  # at least 0.05% move
        return []

    btc_direction = "up" if best_move_pct > 0 else "down"

    # Active spike info (if any)
    spike = state.active_spike
    spike_strength = spike.strength if spike and time.time() - spike.timestamp < 30 else 0.0

    signals = []
    now = time.time()

    for market in markets:
        cid = market.condition_id
        last_trade = last_trade_times.get(cid, 0)
        if now - last_trade < TRADE_COOLDOWN:
            continue

        market_direction = parse_market_direction(market)
        if market_direction is None:
            continue

        stale_yes = market.current_price_yes
        if stale_yes is None or stale_yes <= 0:
            continue

        # Estimate what the fair price should be NOW
        fair_yes = estimate_fair_yes_price(
            market, market_direction,
            state.price, best_move_pct, stale_yes,
        )

        edge = fair_yes - stale_yes
        reasons = []

        # Determine trade direction
        if edge > MIN_EDGE:
            # Fair price > stale price → YES is underpriced → BUY YES
            token_id = market.token_id_yes
            token_label = "YES"
            reasons.append(f"BTC {btc_direction} {abs(best_move_pct):.2f}% → YES underpriced")
        elif edge < -MIN_EDGE:
            # Fair price < stale price → NO is underpriced → BUY NO
            token_id = market.token_id_no
            token_label = "NO"
            edge = -edge  # make positive for sizing
            reasons.append(f"BTC {btc_direction} {abs(best_move_pct):.2f}% → NO underpriced")
        else:
            continue

        # Context reasons
        if spike_strength > 0:
            reasons.append(f"Active spike: strength={spike_strength:.2f}")
        if state.trade_flow_ratio > 0.6:
            reasons.append(f"Trade flow confirms: {state.trade_flow_ratio:.0%} buy")
        elif state.trade_flow_ratio < 0.4:
            reasons.append(f"Trade flow confirms: {1-state.trade_flow_ratio:.0%} sell")
        if abs(state.book_imbalance) > 0.15:
            reasons.append(f"Book imbalance: {state.book_imbalance:+.2f}")

        # Size: bigger edge + spike = bigger position
        edge_factor = min(edge / 0.10, 1.0)  # 10% edge = full size
        spike_factor = 0.5 + 0.5 * spike_strength  # spike adds up to 50% more
        amount = round(MAX_POSITION_SIZE * edge_factor * spike_factor, 2)
        amount = max(1.0, min(amount, MAX_POSITION_SIZE))

        # Limit order price: buy at stale + small offset (still below fair)
        if edge > MIN_EDGE:
            # Buying YES: pay slightly above stale, well below fair
            limit_price = min(stale_yes + LIMIT_ORDER_OFFSET, fair_yes - LIMIT_ORDER_OFFSET)
            limit_price = max(0.01, min(0.99, limit_price))
        else:
            # Buying NO: stale NO price + offset
            stale_no = market.current_price_no or (1.0 - stale_yes)
            limit_price = min(stale_no + LIMIT_ORDER_OFFSET, 0.99)
            limit_price = max(0.01, limit_price)

        signal = TradeSignal(
            market=market,
            token_id=token_id,
            side="BUY",
            amount=amount,
            edge=edge,
            direction=btc_direction,
            btc_move_pct=best_move_pct,
            market_price_stale=stale_yes,
            market_price_fair=fair_yes,
            spike_strength=spike_strength,
            reasons=reasons,
            limit_price=limit_price,
        )
        signals.append(signal)

        log.debug(
            "Signal: %s | %s $%.2f | stale=%.3f fair=%.3f edge=%.3f",
            market.question[:40], token_label, amount,
            stale_yes, fair_yes, edge,
        )

    # Sort by edge (biggest lag = best opportunity)
    signals.sort(key=lambda s: s.edge, reverse=True)
    return signals

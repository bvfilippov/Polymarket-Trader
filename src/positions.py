"""
Position manager with auto-exit.

Tracks open lag-arb positions and exits when:
  - Take-profit: Polymarket price catches up to fair value (lag closed)
  - Stop-loss: BTC reverses against our direction
  - Timeout: position held too long (lag should have closed by now)
"""

import time
from dataclasses import dataclass
from typing import Optional

from src.config import (
    MAX_POSITION_AGE,
    STOP_LOSS_BTC_REVERSAL,
    TAKE_PROFIT_RATIO,
    get_logger,
)

log = get_logger(__name__)


@dataclass
class Position:
    """An open lag-arbitrage position."""
    condition_id: str
    token_id: str
    token_label: str  # "YES" or "NO"
    market_question: str

    # Entry info
    entry_price: float  # Polymarket price at entry
    entry_amount: float  # USDC spent
    entry_shares: float  # shares acquired
    entry_time: float

    # Context at entry
    fair_price_at_entry: float
    btc_price_at_entry: float
    btc_direction: str  # "up" or "down"
    edge_at_entry: float

    # Order tracking
    order_id: Optional[str] = None

    @property
    def age_seconds(self) -> float:
        return time.time() - self.entry_time


@dataclass
class ExitSignal:
    """Signal to exit a position."""
    position: Position
    reason: str
    exit_price: float  # current Polymarket price for this token
    pnl_estimate: float  # estimated P&L in USDC


class PositionManager:
    """
    Manages open positions and generates exit signals.

    Exit conditions:
      1. Take-profit: Polymarket price moved ≥ TAKE_PROFIT_RATIO of the lag
      2. Stop-loss: BTC reversed ≥ STOP_LOSS_BTC_REVERSAL% against us
      3. Timeout: held longer than MAX_POSITION_AGE seconds
    """

    def __init__(self):
        self.positions: dict[str, Position] = {}  # condition_id → Position
        self.closed_pnl: float = 0.0
        self.closed_count: int = 0

    def open_position(
        self,
        condition_id: str,
        token_id: str,
        token_label: str,
        market_question: str,
        entry_price: float,
        entry_amount: float,
        fair_price: float,
        btc_price: float,
        btc_direction: str,
        edge: float,
        order_id: Optional[str] = None,
    ) -> Position:
        shares = entry_amount / entry_price if entry_price > 0 else 0
        pos = Position(
            condition_id=condition_id,
            token_id=token_id,
            token_label=token_label,
            market_question=market_question,
            entry_price=entry_price,
            entry_amount=entry_amount,
            entry_shares=shares,
            entry_time=time.time(),
            fair_price_at_entry=fair_price,
            btc_price_at_entry=btc_price,
            btc_direction=btc_direction,
            edge_at_entry=edge,
            order_id=order_id,
        )
        self.positions[condition_id] = pos
        log.info(
            "POSITION OPEN | %s %s @ %.3f (%.1f shares) | fair=%.3f | edge=%.1f%% | BTC=$%.0f",
            token_label, market_question[:40], entry_price, shares,
            fair_price, edge * 100, btc_price,
        )
        return pos

    def close_position(self, condition_id: str, reason: str, pnl: float = 0.0):
        pos = self.positions.pop(condition_id, None)
        if pos:
            self.closed_pnl += pnl
            self.closed_count += 1
            log.info(
                "POSITION CLOSED | %s %s | held %.0fs | pnl=$%.2f | %s",
                pos.token_label, pos.market_question[:40],
                pos.age_seconds, pnl, reason,
            )

    def check_exits(
        self,
        current_prices: dict[str, float],  # token_id → current price
        btc_price: float,
    ) -> list[ExitSignal]:
        """Check all open positions for exit conditions."""
        signals = []

        for cid, pos in list(self.positions.items()):
            current_price = current_prices.get(pos.token_id)
            if current_price is None:
                continue

            exit_signal = self._check_single(pos, current_price, btc_price)
            if exit_signal:
                signals.append(exit_signal)

        return signals

    def _check_single(
        self, pos: Position, current_price: float, btc_price: float,
    ) -> Optional[ExitSignal]:
        pnl = (current_price - pos.entry_price) * pos.entry_shares

        # 1. Take-profit: Polymarket caught up
        lag_total = pos.fair_price_at_entry - pos.entry_price
        if lag_total > 0:
            lag_closed = current_price - pos.entry_price
            lag_ratio = lag_closed / lag_total

            if lag_ratio >= TAKE_PROFIT_RATIO:
                return ExitSignal(
                    position=pos,
                    reason=f"TAKE PROFIT: lag {lag_ratio:.0%} closed ({pos.entry_price:.3f}→{current_price:.3f})",
                    exit_price=current_price,
                    pnl_estimate=pnl,
                )

        # 2. Stop-loss: BTC reversed
        if btc_price > 0 and pos.btc_price_at_entry > 0:
            btc_change = (btc_price - pos.btc_price_at_entry) / pos.btc_price_at_entry * 100

            if pos.btc_direction == "up" and btc_change < -STOP_LOSS_BTC_REVERSAL:
                return ExitSignal(
                    position=pos,
                    reason=f"STOP LOSS: BTC reversed {btc_change:+.2f}% (entered on UP)",
                    exit_price=current_price,
                    pnl_estimate=pnl,
                )
            elif pos.btc_direction == "down" and btc_change > STOP_LOSS_BTC_REVERSAL:
                return ExitSignal(
                    position=pos,
                    reason=f"STOP LOSS: BTC reversed {btc_change:+.2f}% (entered on DOWN)",
                    exit_price=current_price,
                    pnl_estimate=pnl,
                )

        # 3. Timeout
        if pos.age_seconds > MAX_POSITION_AGE:
            return ExitSignal(
                position=pos,
                reason=f"TIMEOUT: held {pos.age_seconds:.0f}s (max {MAX_POSITION_AGE}s)",
                exit_price=current_price,
                pnl_estimate=pnl,
            )

        return None

    @property
    def count(self) -> int:
        return len(self.positions)

    @property
    def total_exposure(self) -> float:
        return sum(p.entry_amount for p in self.positions.values())

    def get_position(self, condition_id: str) -> Optional[Position]:
        return self.positions.get(condition_id)

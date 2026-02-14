"""
Lag tracker — measures actual Binance → Polymarket delay.

Records when BTC moves on Binance and when Polymarket prices adjust.
Uses this data to calibrate the fair price model and estimate optimal timing.
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

from src.config import get_logger

log = get_logger(__name__)


@dataclass
class LagEvent:
    """A single lag measurement."""
    # Binance move
    btc_move_pct: float
    btc_move_time: float

    # Polymarket state at time of move
    market_condition_id: str
    market_price_before: float  # stale price
    market_price_expected: float  # our fair estimate

    # Resolution (filled when Polymarket adjusts)
    market_price_after: Optional[float] = None
    market_adjust_time: Optional[float] = None

    @property
    def lag_seconds(self) -> Optional[float]:
        if self.market_adjust_time and self.btc_move_time:
            return self.market_adjust_time - self.btc_move_time
        return None

    @property
    def prediction_error(self) -> Optional[float]:
        """Absolute error between our fair estimate and actual adjusted price."""
        if self.market_price_after is not None:
            return abs(self.market_price_expected - self.market_price_after)
        return None

    @property
    def resolved(self) -> bool:
        return self.market_price_after is not None


class LagTracker:
    """
    Measures actual lag between Binance moves and Polymarket adjustments.

    Usage:
      1. record_move() — when we detect a BTC spike
      2. check_adjustments() — periodically check if Polymarket caught up
      3. get_stats() — aggregate lag statistics for calibration
    """

    ADJUSTMENT_THRESHOLD = 0.5  # consider adjusted when 50% of expected move happened
    MAX_TRACKING_TIME = 600  # stop tracking after 10 minutes

    def __init__(self):
        self.events: deque[LagEvent] = deque(maxlen=200)
        self.resolved_lags: deque[float] = deque(maxlen=100)
        self.prediction_errors: deque[float] = deque(maxlen=100)

    def record_move(
        self,
        btc_move_pct: float,
        market_condition_id: str,
        market_price_before: float,
        market_price_expected: float,
    ):
        """Record a BTC move and the current (stale) Polymarket price."""
        event = LagEvent(
            btc_move_pct=btc_move_pct,
            btc_move_time=time.time(),
            market_condition_id=market_condition_id,
            market_price_before=market_price_before,
            market_price_expected=market_price_expected,
        )
        self.events.append(event)

    def check_adjustments(self, current_prices: dict[str, float]):
        """
        Check unresolved events to see if Polymarket prices adjusted.

        current_prices: token_id → current price
        """
        now = time.time()

        for event in self.events:
            if event.resolved:
                continue

            # Timeout — resolve with whatever price we have
            if now - event.btc_move_time > self.MAX_TRACKING_TIME:
                price = current_prices.get(event.market_condition_id)
                event.market_price_after = price if price else event.market_price_before
                event.market_adjust_time = now
                if event.prediction_error is not None:
                    self.prediction_errors.append(event.prediction_error)
                continue

            current = current_prices.get(event.market_condition_id)
            if current is None:
                continue

            # Check if price moved enough toward fair value
            expected_move = event.market_price_expected - event.market_price_before
            if abs(expected_move) < 0.001:
                event.market_price_after = current
                event.market_adjust_time = now
                continue

            actual_move = current - event.market_price_before
            adjustment_ratio = actual_move / expected_move if expected_move != 0 else 0

            if adjustment_ratio >= self.ADJUSTMENT_THRESHOLD:
                event.market_price_after = current
                event.market_adjust_time = now

                lag = event.lag_seconds
                if lag is not None:
                    self.resolved_lags.append(lag)
                if event.prediction_error is not None:
                    self.prediction_errors.append(event.prediction_error)

                log.info(
                    "LAG MEASURED | %.1fs | BTC %.2f%% | price %.3f→%.3f (expected %.3f) | err=%.3f",
                    lag or 0, event.btc_move_pct,
                    event.market_price_before, current,
                    event.market_price_expected,
                    event.prediction_error or 0,
                )

    def get_stats(self) -> dict:
        """Get aggregate lag statistics for logging/calibration."""
        stats = {
            "total_events": len(self.events),
            "resolved": sum(1 for e in self.events if e.resolved),
            "pending": sum(1 for e in self.events if not e.resolved),
        }

        if self.resolved_lags:
            lags = list(self.resolved_lags)
            stats["avg_lag_s"] = sum(lags) / len(lags)
            stats["median_lag_s"] = sorted(lags)[len(lags) // 2]
            stats["min_lag_s"] = min(lags)
            stats["max_lag_s"] = max(lags)

        if self.prediction_errors:
            errors = list(self.prediction_errors)
            stats["avg_pred_err"] = sum(errors) / len(errors)

        return stats

    @property
    def avg_lag(self) -> Optional[float]:
        """Average measured lag in seconds, or None if no data."""
        if not self.resolved_lags:
            return None
        return sum(self.resolved_lags) / len(self.resolved_lags)

    @property
    def avg_prediction_error(self) -> Optional[float]:
        if not self.prediction_errors:
            return None
        return sum(self.prediction_errors) / len(self.prediction_errors)

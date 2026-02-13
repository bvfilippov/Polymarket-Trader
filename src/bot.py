"""
Main bot loop — orchestrates the Binance data collection,
strategy signal generation, and Polymarket order execution.
"""

import signal
import sys
import time
import traceback

from src.binance_data import get_btc_snapshot
from src.config import DRY_RUN, MAX_POSITION_SIZE, MIN_EDGE, POLL_INTERVAL, get_logger
from src.polymarket_client import PolymarketClient
from src.strategy import TradeSignal, generate_signals

log = get_logger(__name__)


class TradingBot:
    def __init__(self):
        self.poly_client = PolymarketClient()
        self.running = False
        self.total_trades = 0
        self.total_spent = 0.0

        # Track active positions to avoid doubling up
        self.active_positions: dict[str, float] = {}  # condition_id -> amount

    def _print_banner(self):
        log.info("=" * 60)
        log.info("Polymarket BTC Trading Bot")
        log.info("=" * 60)
        log.info("Mode: %s", "DRY RUN" if DRY_RUN else "LIVE TRADING")
        log.info("Max position: $%.2f", MAX_POSITION_SIZE)
        log.info("Min edge: %.1f%%", MIN_EDGE * 100)
        log.info("Poll interval: %ds", POLL_INTERVAL)
        log.info("=" * 60)

    def _execute_signal(self, signal: TradeSignal) -> bool:
        """Execute a single trade signal."""
        cid = signal.market.condition_id

        # Check if we already have a position in this market
        existing = self.active_positions.get(cid, 0.0)
        if existing >= MAX_POSITION_SIZE:
            log.info(
                "Skipping '%s' — already at max position ($%.2f)",
                signal.market.question[:40], existing,
            )
            return False

        # Adjust amount to not exceed max
        remaining = MAX_POSITION_SIZE - existing
        amount = min(signal.amount, remaining)
        if amount < 1.0:
            log.info("Skipping — remaining allocation too small ($%.2f)", amount)
            return False

        log.info("-" * 40)
        log.info("TRADE SIGNAL:")
        log.info("  Market: %s", signal.market.question)
        log.info("  Direction: BTC %s", signal.direction.upper())
        log.info("  Action: %s token for $%.2f", signal.side, amount)
        log.info("  Our probability: %.1f%%", signal.our_probability * 100)
        log.info("  Market price: %.1f%%", signal.market_probability * 100)
        log.info("  Edge: %.1f%%", signal.edge * 100)
        log.info("  Reasons:")
        for reason in signal.reasons:
            log.info("    - %s", reason)

        result = self.poly_client.place_market_order(
            token_id=signal.token_id,
            side=signal.side,
            amount=amount,
        )

        if result is not None:
            self.active_positions[cid] = existing + amount
            self.total_trades += 1
            self.total_spent += amount
            log.info("  Result: %s", result)
            return True

        log.warning("  Order failed")
        return False

    def run_once(self):
        """Run a single iteration of the trading loop."""
        log.info("\n--- Tick at %s ---", time.strftime("%Y-%m-%d %H:%M:%S"))

        # 1. Get BTC data from Binance
        try:
            snapshot = get_btc_snapshot()
        except Exception as e:
            log.error("Failed to get Binance data: %s", e)
            return

        # 2. Find active BTC markets on Polymarket
        try:
            markets = self.poly_client.find_btc_markets(active_only=True)
        except Exception as e:
            log.error("Failed to find markets: %s", e)
            return

        if not markets:
            log.info("No active BTC markets found")
            return

        # 3. Fetch prices for each market
        for market in markets:
            try:
                self.poly_client.get_market_prices(market)
            except Exception as e:
                log.warning("Failed to get prices for %s: %s", market.market_slug, e)

        # 4. Generate trading signals
        signals = generate_signals(snapshot, markets)

        if not signals:
            log.info("No trade signals above minimum edge threshold")
            return

        # 5. Execute signals (best edge first, already sorted)
        executed = 0
        for sig in signals:
            if self._execute_signal(sig):
                executed += 1
            # Limit to 3 trades per tick
            if executed >= 3:
                break

        log.info(
            "Tick summary: %d/%d signals executed | Total trades: %d | Total spent: $%.2f",
            executed, len(signals), self.total_trades, self.total_spent,
        )

    def run(self):
        """Run the bot in a continuous loop."""
        self._print_banner()
        self.running = True

        def handle_shutdown(signum, frame):
            log.info("\nShutdown signal received, stopping...")
            self.running = False

        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        while self.running:
            try:
                self.run_once()
            except Exception:
                log.error("Unexpected error in main loop:\n%s", traceback.format_exc())

            if self.running:
                log.info("Sleeping %ds until next tick...", POLL_INTERVAL)
                # Sleep in small increments so we can respond to shutdown quickly
                for _ in range(POLL_INTERVAL):
                    if not self.running:
                        break
                    time.sleep(1)

        log.info("Bot stopped. Total trades: %d, Total spent: $%.2f",
                 self.total_trades, self.total_spent)

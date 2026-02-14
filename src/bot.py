"""
Async event-driven trading bot.

Architecture:
  1. BinanceStream task — WebSocket connection, updates LiveMarketState continuously
  2. Signal evaluator task — every N seconds reads state, generates signals, executes trades
  3. Market refresher task — periodically refreshes Polymarket BTC markets and their prices

All three run concurrently as asyncio tasks.
"""

import asyncio
import signal
import time
import traceback

from src.binance_data import BinanceStream, LiveMarketState, estimate_btc_direction
from src.config import (
    DRY_RUN,
    MARKET_REFRESH_INTERVAL,
    MAX_POSITION_SIZE,
    MIN_EDGE,
    SIGNAL_EVAL_INTERVAL,
    TRADE_COOLDOWN,
    get_logger,
)
from src.polymarket_client import BtcMarket, PolymarketClient
from src.strategy import TradeSignal, generate_signals

log = get_logger(__name__)


class TradingBot:
    def __init__(self):
        self.state = LiveMarketState()
        self.stream = BinanceStream(self.state)
        self.poly_client = PolymarketClient()

        self._running = False
        self.total_trades = 0
        self.total_spent = 0.0

        # Active BTC markets (refreshed periodically)
        self.markets: list[BtcMarket] = []

        # Position tracking
        self.active_positions: dict[str, float] = {}  # condition_id -> amount

        # Trade cooldowns
        self.last_trade_times: dict[str, float] = {}  # condition_id -> timestamp

        # Previous signal direction for change detection
        self._prev_direction: str | None = None
        self._prev_confidence: float = 0.5

    def _print_banner(self):
        log.info("=" * 60)
        log.info("Polymarket BTC Trading Bot (Real-Time)")
        log.info("=" * 60)
        log.info("Mode: %s", "DRY RUN" if DRY_RUN else "LIVE TRADING")
        log.info("Max position: $%.2f", MAX_POSITION_SIZE)
        log.info("Min edge: %.1f%%", MIN_EDGE * 100)
        log.info("Signal eval: every %.1fs", SIGNAL_EVAL_INTERVAL)
        log.info("Market refresh: every %ds", MARKET_REFRESH_INTERVAL)
        log.info("Trade cooldown: %ds", TRADE_COOLDOWN)
        log.info("Streams: aggTrade + kline_1m + kline_5m + depth20")
        log.info("=" * 60)

    def _execute_signal(self, sig: TradeSignal) -> bool:
        """Execute a single trade signal."""
        cid = sig.market.condition_id

        # Check position limit
        existing = self.active_positions.get(cid, 0.0)
        if existing >= MAX_POSITION_SIZE:
            return False

        remaining = MAX_POSITION_SIZE - existing
        amount = min(sig.amount, remaining)
        if amount < 1.0:
            return False

        log.info("─" * 50)
        log.info("TRADE | %s | BTC %s | urgency=%.2f",
                 sig.market.question[:50], sig.direction.upper(), sig.urgency)
        log.info("  BUY %s for $%.2f | edge=%.1f%% | our=%.1f%% vs mkt=%.1f%%",
                 "YES" if sig.token_id == sig.market.token_id_yes else "NO",
                 amount, sig.edge * 100,
                 sig.our_probability * 100, sig.market_probability * 100)

        result = self.poly_client.place_market_order(
            token_id=sig.token_id,
            side=sig.side,
            amount=amount,
        )

        if result is not None:
            self.active_positions[cid] = existing + amount
            self.last_trade_times[cid] = time.time()
            self.total_trades += 1
            self.total_spent += amount
            log.info("  Result: %s", result)
            return True

        log.warning("  Order failed")
        return False

    # ── Async tasks ─────────────────────────────────────────────

    async def _task_signal_evaluator(self):
        """Evaluate signals every SIGNAL_EVAL_INTERVAL seconds."""
        log.info("Signal evaluator started (interval=%.1fs)", SIGNAL_EVAL_INTERVAL)

        # Wait for data to accumulate
        while self._running and not self.state.ready:
            await asyncio.sleep(1)
        log.info("Market state ready, starting signal evaluation")

        tick = 0
        while self._running:
            try:
                tick += 1

                if not self.markets:
                    await asyncio.sleep(SIGNAL_EVAL_INTERVAL)
                    continue

                # Generate signals from current state
                signals = generate_signals(
                    self.state, self.markets, self.last_trade_times
                )

                # Log periodic status (every ~30 seconds)
                if tick % max(1, int(30 / SIGNAL_EVAL_INTERVAL)) == 0:
                    est = estimate_btc_direction(self.state)
                    log.info(
                        "STATUS | BTC=$%s | %s P(up)=%.1f%% | "
                        "book=%.2f | flow=%.0f%% | "
                        "whales=%dB/%dS | signals=%d | trades=%d ($%.0f)",
                        f"{self.state.price:,.0f}",
                        est["direction"].upper(),
                        est["confidence"] * 100,
                        self.state.book_imbalance,
                        self.state.trade_flow_ratio * 100,
                        self.state.large_buy_count_5m,
                        self.state.large_sell_count_5m,
                        len(signals),
                        self.total_trades,
                        self.total_spent,
                    )

                # Detect direction change
                est = estimate_btc_direction(self.state)
                if (self._prev_direction is not None
                        and est["direction"] != self._prev_direction):
                    log.info(
                        "DIRECTION CHANGE: %s -> %s (confidence %.1f%% -> %.1f%%)",
                        self._prev_direction.upper(), est["direction"].upper(),
                        self._prev_confidence * 100, est["confidence"] * 100,
                    )
                self._prev_direction = est["direction"]
                self._prev_confidence = est["confidence"]

                # Execute best signals
                executed = 0
                for sig in signals:
                    if self._execute_signal(sig):
                        executed += 1
                    if executed >= 2:  # Max 2 trades per evaluation
                        break

            except Exception:
                log.error("Signal evaluator error:\n%s", traceback.format_exc())

            await asyncio.sleep(SIGNAL_EVAL_INTERVAL)

    async def _task_market_refresher(self):
        """Periodically refresh Polymarket BTC markets and prices."""
        log.info("Market refresher started (interval=%ds)", MARKET_REFRESH_INTERVAL)

        while self._running:
            try:
                markets = self.poly_client.find_btc_markets(active_only=True)
                if markets:
                    for m in markets:
                        try:
                            self.poly_client.get_market_prices(m)
                        except Exception as e:
                            log.warning("Failed to get prices for %s: %s",
                                        m.market_slug, e)

                    self.markets = [
                        m for m in markets
                        if m.current_price_yes is not None
                    ]
                    log.info("Refreshed %d BTC markets with prices", len(self.markets))
                else:
                    log.warning("No BTC markets found on Polymarket")

            except Exception:
                log.error("Market refresh error:\n%s", traceback.format_exc())

            await asyncio.sleep(MARKET_REFRESH_INTERVAL)

    async def _task_health_monitor(self):
        """Monitor stream health and log warnings."""
        while self._running:
            await asyncio.sleep(10)

            if not self.state.connected:
                log.warning("Binance stream disconnected, waiting for reconnect...")
                continue

            now = time.time()
            if self.state.last_trade_time > 0 and now - self.state.last_trade_time > 10:
                log.warning("No trades received for %.0fs", now - self.state.last_trade_time)

    # ── Main entry points ───────────────────────────────────────

    async def run_async(self):
        """Run the bot with all concurrent tasks."""
        self._print_banner()
        self._running = True

        loop = asyncio.get_running_loop()
        for sig_name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig_name, self._shutdown)

        tasks = [
            asyncio.create_task(self.stream.connect(), name="binance-stream"),
            asyncio.create_task(self._task_signal_evaluator(), name="signal-eval"),
            asyncio.create_task(self._task_market_refresher(), name="market-refresh"),
            asyncio.create_task(self._task_health_monitor(), name="health-monitor"),
        ]

        log.info("All tasks started, bot is running")

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            log.info("Bot stopped. Trades: %d | Spent: $%.2f",
                     self.total_trades, self.total_spent)

    def _shutdown(self):
        """Handle shutdown signal."""
        log.info("Shutdown signal received...")
        self._running = False
        asyncio.create_task(self.stream.stop())
        for task in asyncio.all_tasks():
            if task.get_name() != "Task-1":
                task.cancel()

    def run(self):
        """Synchronous entry point."""
        asyncio.run(self.run_async())

    def run_once(self):
        """Run a single evaluation (for testing). Uses REST fallback."""
        import requests
        from src.binance_data import StateUpdater

        log.info("Running single evaluation with REST data...")

        # Fetch current klines via REST to populate state
        updater = StateUpdater(self.state)

        for interval, count in [("1m", 100), ("5m", 60)]:
            resp = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": interval, "limit": count},
                timeout=10,
            )
            resp.raise_for_status()
            for row in resp.json():
                candle_data = {
                    "k": {
                        "i": interval,
                        "t": row[0],
                        "o": row[1], "h": row[2], "l": row[3], "c": row[4],
                        "v": row[5], "q": row[7], "V": row[9],
                        "x": True,
                    }
                }
                updater.on_kline(candle_data)

        # Fetch depth
        resp = requests.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": "BTCUSDT", "limit": 20},
            timeout=10,
        )
        resp.raise_for_status()
        updater.on_depth(resp.json())

        # Fetch recent trades
        resp = requests.get(
            "https://api.binance.com/api/v3/aggTrades",
            params={"symbol": "BTCUSDT", "limit": 200},
            timeout=10,
        )
        resp.raise_for_status()
        for t in resp.json():
            updater.on_agg_trade(t)

        self.state.connected = True

        # Evaluate
        est = estimate_btc_direction(self.state)
        log.info("BTC=$%s | %s | P(up)=%.1f%%",
                 f"{self.state.price:,.0f}", est["direction"].upper(),
                 est["confidence"] * 100)
        for r in est["reasons"]:
            log.info("  %s", r)
        log.info("Book imbalance: %.3f | Flow: %.0f%% buy",
                 self.state.book_imbalance, self.state.trade_flow_ratio * 100)

        # Find markets and generate signals
        self.markets = self.poly_client.find_btc_markets(active_only=True)
        for m in self.markets:
            self.poly_client.get_market_prices(m)

        signals = generate_signals(self.state, self.markets, self.last_trade_times)
        log.info("Generated %d signals", len(signals))

        for sig in signals:
            self._execute_signal(sig)

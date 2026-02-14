"""
Lag-arbitrage trading bot.

Two trigger modes:
  1. Spike-triggered: BinanceStream detects a spike → immediately refresh
     Polymarket prices → evaluate → trade. Lowest latency.
  2. Periodic: every SIGNAL_EVAL_INTERVAL seconds, check for any
     price moves worth trading. Catches slower moves.

Concurrent async tasks:
  - BinanceStream: WebSocket feeds → LiveMarketState (continuous)
  - Spike handler: reacts to spike callbacks from BinanceStream
  - Periodic evaluator: checks for lag opportunities every 2s
  - Market refresher: refreshes Polymarket markets every 30s
"""

import asyncio
import signal
import time
import traceback

from src.binance_data import BinanceStream, LiveMarketState, Spike, get_price_moves
from src.config import (
    DRY_RUN,
    MARKET_REFRESH_INTERVAL,
    MAX_POSITION_SIZE,
    MIN_EDGE,
    SIGNAL_EVAL_INTERVAL,
    SPIKE_THRESHOLD_PCT,
    TRADE_COOLDOWN,
    get_logger,
)
from src.polymarket_client import BtcMarket, PolymarketClient
from src.strategy import TradeSignal, generate_signals

log = get_logger(__name__)


class TradingBot:
    def __init__(self):
        self.state = LiveMarketState()
        self.poly_client = PolymarketClient()

        # BinanceStream with spike callback
        self.stream = BinanceStream(self.state, on_spike=self._on_spike_sync)

        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._spike_queue: asyncio.Queue[Spike] = asyncio.Queue()

        self.total_trades = 0
        self.total_spent = 0.0

        self.markets: list[BtcMarket] = []
        self.active_positions: dict[str, float] = {}
        self.last_trade_times: dict[str, float] = {}

    def _on_spike_sync(self, spike: Spike):
        """Callback from SpikeDetector (runs in sync context). Puts spike into async queue."""
        if self._loop and self._running:
            self._loop.call_soon_threadsafe(self._spike_queue.put_nowait, spike)

    def _print_banner(self):
        log.info("=" * 60)
        log.info("Polymarket BTC Lag-Arbitrage Bot")
        log.info("=" * 60)
        log.info("Mode: %s", "DRY RUN" if DRY_RUN else "LIVE TRADING")
        log.info("Max position: $%.2f | Min edge: %.1f%%", MAX_POSITION_SIZE, MIN_EDGE * 100)
        log.info("Spike threshold: %.2f%% | Eval interval: %.1fs", SPIKE_THRESHOLD_PCT, SIGNAL_EVAL_INTERVAL)
        log.info("Market refresh: %ds | Trade cooldown: %ds", MARKET_REFRESH_INTERVAL, TRADE_COOLDOWN)
        log.info("Strategy: detect BTC move on Binance → trade Polymarket lag")
        log.info("=" * 60)

    def _execute_signal(self, sig: TradeSignal, trigger: str) -> bool:
        """Execute a trade signal."""
        cid = sig.market.condition_id

        existing = self.active_positions.get(cid, 0.0)
        if existing >= MAX_POSITION_SIZE:
            return False

        remaining = MAX_POSITION_SIZE - existing
        amount = min(sig.amount, remaining)
        if amount < 1.0:
            return False

        token_label = "YES" if sig.token_id == sig.market.token_id_yes else "NO"

        log.info("─" * 55)
        log.info("TRADE [%s] | %s", trigger, sig.market.question[:50])
        log.info("  BTC %s %.2f%% | BUY %s $%.2f",
                 sig.direction.upper(), abs(sig.btc_move_pct), token_label, amount)
        log.info("  stale=%.3f → fair=%.3f | edge=%.1f%% | spike=%.2f",
                 sig.market_price_stale, sig.market_price_fair,
                 sig.edge * 100, sig.spike_strength)
        for r in sig.reasons:
            log.info("  %s", r)

        result = self.poly_client.place_market_order(
            token_id=sig.token_id, side=sig.side, amount=amount,
        )

        if result is not None:
            self.active_positions[cid] = existing + amount
            self.last_trade_times[cid] = time.time()
            self.total_trades += 1
            self.total_spent += amount
            log.info("  OK: %s", result)
            return True

        log.warning("  FAIL")
        return False

    def _evaluate_and_trade(self, trigger: str) -> int:
        """Run signal generation and execute trades. Returns count of trades."""
        if not self.markets:
            return 0

        signals = generate_signals(self.state, self.markets, self.last_trade_times)
        if not signals:
            return 0

        executed = 0
        for sig in signals:
            if self._execute_signal(sig, trigger):
                executed += 1
            if executed >= 3:
                break
        return executed

    # ── Async tasks ─────────────────────────────────────────────

    async def _task_spike_handler(self):
        """React to spikes from BinanceStream — lowest latency path."""
        log.info("Spike handler ready")

        while self._running:
            try:
                spike = await asyncio.wait_for(self._spike_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if not self.markets:
                continue

            log.info(
                "SPIKE TRIGGER | %s %.2f%% in %ds | refreshing Polymarket prices...",
                spike.direction.upper(), spike.move_pct, spike.window_sec,
            )

            # Refresh Polymarket prices ASAP to get the freshest stale prices
            for m in self.markets:
                try:
                    self.poly_client.get_market_prices(m)
                except Exception:
                    pass

            # Evaluate and trade
            n = self._evaluate_and_trade("SPIKE")
            if n > 0:
                log.info("Spike trade: executed %d orders", n)

    async def _task_periodic_evaluator(self):
        """Periodic check for lag opportunities (catches slower moves)."""
        log.info("Periodic evaluator started (%.1fs)", SIGNAL_EVAL_INTERVAL)

        while self._running and not self.state.ready:
            await asyncio.sleep(1)
        log.info("State ready, periodic evaluation active")

        tick = 0
        while self._running:
            try:
                tick += 1

                # Evaluate
                n = self._evaluate_and_trade("PERIODIC")

                # Status log every ~30 seconds
                if tick % max(1, int(30 / SIGNAL_EVAL_INTERVAL)) == 0:
                    moves = get_price_moves(self.state)
                    move_strs = " | ".join(
                        f"{k}={v:+.3f}%" for k, v in sorted(moves.items())
                    )
                    log.info(
                        "STATUS | BTC=$%s | %s | book=%.2f | flow=%.0f%% | "
                        "trades=%d ($%.0f) | markets=%d",
                        f"{self.state.price:,.0f}",
                        move_strs or "no data",
                        self.state.book_imbalance,
                        self.state.trade_flow_ratio * 100,
                        self.total_trades, self.total_spent,
                        len(self.markets),
                    )

            except Exception:
                log.error("Evaluator error:\n%s", traceback.format_exc())

            await asyncio.sleep(SIGNAL_EVAL_INTERVAL)

    async def _task_market_refresher(self):
        """Refresh Polymarket BTC markets and prices."""
        log.info("Market refresher started (%ds)", MARKET_REFRESH_INTERVAL)

        while self._running:
            try:
                markets = self.poly_client.find_btc_markets(active_only=True)
                if markets:
                    for m in markets:
                        try:
                            self.poly_client.get_market_prices(m)
                        except Exception as e:
                            log.warning("Price fetch failed for %s: %s", m.market_slug, e)

                    self.markets = [m for m in markets if m.current_price_yes is not None]
                    log.info("Markets: %d active BTC markets", len(self.markets))
                    for m in self.markets[:5]:
                        log.info("  %.3f YES | %s", m.current_price_yes, m.question[:60])
                else:
                    log.warning("No BTC markets found")
            except Exception:
                log.error("Market refresh error:\n%s", traceback.format_exc())

            await asyncio.sleep(MARKET_REFRESH_INTERVAL)

    async def _task_health_monitor(self):
        while self._running:
            await asyncio.sleep(10)
            if not self.state.connected:
                log.warning("Binance disconnected")
            now = time.time()
            if self.state.last_trade_time > 0 and now - self.state.last_trade_time > 10:
                log.warning("No trades for %.0fs", now - self.state.last_trade_time)

    # ── Entry points ────────────────────────────────────────────

    async def run_async(self):
        self._print_banner()
        self._running = True
        self._loop = asyncio.get_running_loop()

        for sig_name in (signal.SIGINT, signal.SIGTERM):
            self._loop.add_signal_handler(sig_name, self._shutdown)

        tasks = [
            asyncio.create_task(self.stream.connect(), name="binance"),
            asyncio.create_task(self._task_spike_handler(), name="spike"),
            asyncio.create_task(self._task_periodic_evaluator(), name="eval"),
            asyncio.create_task(self._task_market_refresher(), name="markets"),
            asyncio.create_task(self._task_health_monitor(), name="health"),
        ]

        log.info("All tasks started — bot running")

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            log.info("Bot stopped. Trades: %d | Spent: $%.2f",
                     self.total_trades, self.total_spent)

    def _shutdown(self):
        log.info("Shutting down...")
        self._running = False
        asyncio.create_task(self.stream.stop())
        for task in asyncio.all_tasks():
            if task.get_name() not in ("Task-1",):
                task.cancel()

    def run(self):
        asyncio.run(self.run_async())

    def run_once(self):
        """Single evaluation with REST data (for testing)."""
        import requests
        from src.binance_data import StateUpdater

        log.info("Single evaluation (REST)...")
        updater = StateUpdater(self.state)

        # Fetch 1m klines
        resp = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 60},
            timeout=10,
        )
        resp.raise_for_status()
        for row in resp.json():
            updater.on_kline({"k": {
                "i": "1m", "t": row[0],
                "o": row[1], "h": row[2], "l": row[3], "c": row[4],
                "v": row[5], "q": row[7], "V": row[9], "x": True,
            }})

        # Fetch depth
        resp = requests.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": "BTCUSDT", "limit": 20}, timeout=10,
        )
        resp.raise_for_status()
        updater.on_depth(resp.json())

        # Fetch trades
        resp = requests.get(
            "https://api.binance.com/api/v3/aggTrades",
            params={"symbol": "BTCUSDT", "limit": 500}, timeout=10,
        )
        resp.raise_for_status()
        for t in resp.json():
            updater.on_agg_trade(t)

        self.state.connected = True

        moves = get_price_moves(self.state)
        log.info("BTC=$%s | moves: %s",
                 f"{self.state.price:,.0f}",
                 " | ".join(f"{k}={v:+.3f}%" for k, v in sorted(moves.items())))
        log.info("Book: %.3f | Flow: %.0f%% buy | Whales: %dB/%dS",
                 self.state.book_imbalance, self.state.trade_flow_ratio * 100,
                 self.state.large_buy_count_5m, self.state.large_sell_count_5m)

        self.markets = self.poly_client.find_btc_markets(active_only=True)
        for m in self.markets:
            self.poly_client.get_market_prices(m)

        signals = generate_signals(self.state, self.markets, self.last_trade_times)
        log.info("Signals: %d", len(signals))
        for sig in signals:
            self._execute_signal(sig, "ONCE")

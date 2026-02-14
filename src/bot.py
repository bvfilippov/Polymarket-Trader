"""
Lag-arbitrage trading bot.

Two trigger modes:
  1. Spike-triggered: BinanceStream detects a spike → immediately refresh
     Polymarket prices → evaluate → trade. Lowest latency.
  2. Periodic: every SIGNAL_EVAL_INTERVAL seconds, check for any
     price moves worth trading. Catches slower moves.

Concurrent async tasks:
  - BinanceStream: WebSocket feeds → LiveMarketState (continuous)
  - PolymarketStream: WebSocket feeds → real-time Polymarket prices
  - Spike handler: reacts to spike callbacks from BinanceStream
  - Periodic evaluator: checks for lag opportunities every 2s
  - Market refresher: refreshes Polymarket markets every 30s
  - Position monitor: checks exits (take-profit / stop-loss / timeout)
"""

import asyncio
import signal
import time
import traceback

from src.binance_data import BinanceStream, LiveMarketState, Spike, get_price_moves
from src.config import (
    DRY_RUN,
    LIMIT_ORDER_OFFSET,
    LIMIT_ORDER_TIMEOUT,
    MARKET_REFRESH_INTERVAL,
    MAX_POSITION_SIZE,
    MIN_EDGE,
    SIGNAL_EVAL_INTERVAL,
    SPIKE_THRESHOLD_PCT,
    TRADE_COOLDOWN,
    USE_LIMIT_ORDERS,
    get_logger,
)
from src.lag_tracker import LagTracker
from src.polymarket_client import BtcMarket, PolymarketClient
from src.polymarket_ws import PolymarketStream
from src.positions import ExitSignal, PositionManager
from src.strategy import TradeSignal, generate_signals

log = get_logger(__name__)


class TradingBot:
    def __init__(self):
        self.state = LiveMarketState()
        self.poly_client = PolymarketClient()
        self.poly_stream = PolymarketStream()
        self.position_mgr = PositionManager()
        self.lag_tracker = LagTracker()

        # BinanceStream with spike callback
        self.stream = BinanceStream(self.state, on_spike=self._on_spike_sync)

        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._spike_queue: asyncio.Queue[Spike] = asyncio.Queue()

        self.total_trades = 0
        self.total_spent = 0.0

        self.markets: list[BtcMarket] = []
        self.last_trade_times: dict[str, float] = {}

        # Pending limit orders: order_id → (condition_id, token_id, amount, placed_time)
        self._pending_orders: dict[str, tuple[str, str, float, float]] = {}

    def _on_spike_sync(self, spike: Spike):
        """Callback from SpikeDetector (runs in sync context)."""
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
        log.info("Orders: %s | Polymarket WS: enabled", "LIMIT" if USE_LIMIT_ORDERS else "MARKET (FOK)")
        log.info("Auto-exit: take-profit / stop-loss / timeout")
        log.info("Strategy: detect BTC move on Binance → trade Polymarket lag")
        log.info("=" * 60)

    # ── Price helpers ──────────────────────────────────────────

    def _get_token_prices(self) -> dict[str, float]:
        """Get current prices by token_id (from WS or market objects)."""
        prices = {}
        for m in self.markets:
            # Prefer WebSocket price, fall back to REST-fetched price
            ws_yes = self.poly_stream.get_price(m.token_id_yes)
            if ws_yes is not None:
                prices[m.token_id_yes] = ws_yes
                m.current_price_yes = ws_yes
            elif m.current_price_yes is not None:
                prices[m.token_id_yes] = m.current_price_yes

            ws_no = self.poly_stream.get_price(m.token_id_no)
            if ws_no is not None:
                prices[m.token_id_no] = ws_no
                m.current_price_no = ws_no
            elif m.current_price_no is not None:
                prices[m.token_id_no] = m.current_price_no

        return prices

    def _refresh_prices_from_ws(self):
        """Push latest WS prices into market objects."""
        self.poly_stream.update_market_prices(self.markets)

    # ── Trade execution ────────────────────────────────────────

    def _execute_signal(self, sig: TradeSignal, trigger: str) -> bool:
        """Execute a trade signal (entry)."""
        cid = sig.market.condition_id

        # Check existing position
        existing_pos = self.position_mgr.get_position(cid)
        if existing_pos is not None:
            return False

        token_label = "YES" if sig.token_id == sig.market.token_id_yes else "NO"

        log.info("─" * 55)
        log.info("TRADE [%s] | %s", trigger, sig.market.question[:50])
        log.info("  BTC %s %.2f%% | BUY %s $%.2f",
                 sig.direction.upper(), abs(sig.btc_move_pct), token_label, sig.amount)
        log.info("  stale=%.3f → fair=%.3f | edge=%.1f%% | spike=%.2f",
                 sig.market_price_stale, sig.market_price_fair,
                 sig.edge * 100, sig.spike_strength)
        if USE_LIMIT_ORDERS:
            log.info("  limit_price=%.3f", sig.limit_price)
        for r in sig.reasons:
            log.info("  %s", r)

        amount = min(sig.amount, MAX_POSITION_SIZE)
        if amount < 1.0:
            return False

        # Place order
        if USE_LIMIT_ORDERS and sig.limit_price > 0:
            # Limit order: size in shares, price is the limit
            shares = amount / sig.limit_price
            result = self.poly_client.place_limit_order(
                token_id=sig.token_id,
                side="BUY",
                price=sig.limit_price,
                size=round(shares, 2),
            )
            entry_price = sig.limit_price
        else:
            # FOK market order
            result = self.poly_client.place_market_order(
                token_id=sig.token_id, side=sig.side, amount=amount,
            )
            entry_price = sig.market_price_stale

        if result is None:
            log.warning("  ORDER FAILED")
            return False

        log.info("  OK: %s", result)

        # Register position
        self.position_mgr.open_position(
            condition_id=cid,
            token_id=sig.token_id,
            token_label=token_label,
            market_question=sig.market.question,
            entry_price=entry_price,
            entry_amount=amount,
            fair_price=sig.market_price_fair,
            btc_price=self.state.price,
            btc_direction=sig.direction,
            edge=sig.edge,
        )

        # Record lag event for tracking
        self.lag_tracker.record_move(
            btc_move_pct=sig.btc_move_pct,
            market_condition_id=sig.token_id,
            market_price_before=sig.market_price_stale,
            market_price_expected=sig.market_price_fair,
        )

        self.last_trade_times[cid] = time.time()
        self.total_trades += 1
        self.total_spent += amount
        return True

    def _execute_exit(self, exit_sig: ExitSignal) -> bool:
        """Execute an exit (sell position)."""
        pos = exit_sig.position

        log.info("─" * 55)
        log.info("EXIT | %s %s", pos.token_label, pos.market_question[:50])
        log.info("  %s", exit_sig.reason)
        log.info("  entry=%.3f → exit=%.3f | pnl=$%.2f | held %.0fs",
                 pos.entry_price, exit_sig.exit_price,
                 exit_sig.pnl_estimate, pos.age_seconds)

        # Sell shares — use market order for immediate exit
        result = self.poly_client.place_market_order(
            token_id=pos.token_id,
            side="SELL",
            amount=round(pos.entry_shares, 2),
        )

        if result is not None:
            self.position_mgr.close_position(
                pos.condition_id, exit_sig.reason, exit_sig.pnl_estimate,
            )
            log.info("  EXIT OK: %s", result)
            return True

        log.warning("  EXIT FAILED — will retry")
        return False

    def _evaluate_and_trade(self, trigger: str) -> int:
        """Run signal generation and execute trades. Returns count of trades."""
        if not self.markets:
            return 0

        # Update prices from WebSocket before evaluating
        self._refresh_prices_from_ws()

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
                "SPIKE TRIGGER | %s %.2f%% in %ds | refreshing prices...",
                spike.direction.upper(), spike.move_pct, spike.window_sec,
            )

            # Use WS prices if fresh, otherwise fall back to REST
            has_ws_prices = any(
                self.poly_stream.is_fresh(m.token_id_yes) for m in self.markets
            )

            if not has_ws_prices:
                # REST fallback for price refresh
                for m in self.markets:
                    try:
                        self.poly_client.get_market_prices(m)
                    except Exception:
                        pass
            else:
                self._refresh_prices_from_ws()

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

                # Evaluate signals
                n = self._evaluate_and_trade("PERIODIC")

                # Status log every ~30 seconds
                if tick % max(1, int(30 / SIGNAL_EVAL_INTERVAL)) == 0:
                    moves = get_price_moves(self.state)
                    move_strs = " | ".join(
                        f"{k}={v:+.3f}%" for k, v in sorted(moves.items())
                    )
                    lag_stats = self.lag_tracker.get_stats()
                    avg_lag = lag_stats.get("avg_lag_s", "?")
                    lag_str = f"{avg_lag:.1f}s" if isinstance(avg_lag, float) else avg_lag

                    log.info(
                        "STATUS | BTC=$%s | %s | book=%.2f | flow=%.0f%% | "
                        "trades=%d ($%.0f) | positions=%d | pnl=$%.2f | "
                        "avg_lag=%s | poly_ws=%s",
                        f"{self.state.price:,.0f}",
                        move_strs or "no data",
                        self.state.book_imbalance,
                        self.state.trade_flow_ratio * 100,
                        self.total_trades, self.total_spent,
                        self.position_mgr.count,
                        self.position_mgr.closed_pnl,
                        lag_str,
                        "OK" if self.poly_stream.connected else "OFF",
                    )

            except Exception:
                log.error("Evaluator error:\n%s", traceback.format_exc())

            await asyncio.sleep(SIGNAL_EVAL_INTERVAL)

    async def _task_position_monitor(self):
        """Monitor open positions for exit conditions (every 1s)."""
        log.info("Position monitor started")

        while self._running:
            try:
                if self.position_mgr.count > 0:
                    prices = self._get_token_prices()
                    exits = self.position_mgr.check_exits(prices, self.state.price)

                    for ex in exits:
                        self._execute_exit(ex)

                    # Also update lag tracker
                    self.lag_tracker.check_adjustments(prices)

            except Exception:
                log.error("Position monitor error:\n%s", traceback.format_exc())

            await asyncio.sleep(1)

    async def _task_market_refresher(self):
        """Refresh Polymarket BTC markets and prices."""
        log.info("Market refresher started (%ds)", MARKET_REFRESH_INTERVAL)

        while self._running:
            try:
                markets = self.poly_client.find_btc_markets(active_only=True)
                if markets:
                    # Always get initial REST prices
                    for m in markets:
                        try:
                            self.poly_client.get_market_prices(m)
                        except Exception as e:
                            log.warning("Price fetch failed for %s: %s", m.market_slug, e)

                    self.markets = [m for m in markets if m.current_price_yes is not None]
                    log.info("Markets: %d active BTC markets", len(self.markets))
                    for m in self.markets[:5]:
                        log.info("  %.3f YES | %s", m.current_price_yes, m.question[:60])

                    # Subscribe to Polymarket WebSocket for real-time updates
                    await self.poly_stream.subscribe(self.markets)
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
            if not self.poly_stream.connected:
                log.debug("Polymarket WS not connected")
            now = time.time()
            if self.state.last_trade_time > 0 and now - self.state.last_trade_time > 10:
                log.warning("No Binance trades for %.0fs", now - self.state.last_trade_time)

    # ── Entry points ────────────────────────────────────────────

    async def run_async(self):
        self._print_banner()
        self._running = True
        self._loop = asyncio.get_running_loop()

        for sig_name in (signal.SIGINT, signal.SIGTERM):
            self._loop.add_signal_handler(sig_name, self._shutdown)

        tasks = [
            asyncio.create_task(self.stream.connect(), name="binance"),
            asyncio.create_task(self.poly_stream.connect(), name="poly_ws"),
            asyncio.create_task(self._task_spike_handler(), name="spike"),
            asyncio.create_task(self._task_periodic_evaluator(), name="eval"),
            asyncio.create_task(self._task_market_refresher(), name="markets"),
            asyncio.create_task(self._task_position_monitor(), name="positions"),
            asyncio.create_task(self._task_health_monitor(), name="health"),
        ]

        log.info("All tasks started — bot running (%d tasks)", len(tasks))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            log.info(
                "Bot stopped. Trades: %d | Spent: $%.2f | PnL: $%.2f",
                self.total_trades, self.total_spent, self.position_mgr.closed_pnl,
            )
            lag_stats = self.lag_tracker.get_stats()
            if lag_stats.get("resolved", 0) > 0:
                log.info("Lag stats: %s", lag_stats)

    def _shutdown(self):
        log.info("Shutting down...")
        self._running = False
        asyncio.create_task(self.stream.stop())
        asyncio.create_task(self.poly_stream.stop())
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

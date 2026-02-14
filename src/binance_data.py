"""
Real-time Binance data collector via WebSocket streams.

Core idea: Polymarket BTC prediction prices LAG behind the real BTC price.
We detect sudden moves on Binance and trade Polymarket before it adjusts.

Spike detection (primary triggers):
  - Price moves: track price over 10s/30s/60s windows, detect sharp moves
  - Volume bursts: volume in last 30s vs 5min average
  - Order book sweeps: sudden depth drop on one side

Streams:
  - aggTrade:  real-time trades → price tracking, CVD, volume, whale detection
  - kline_1m:  1-minute candles → short-term context
  - depth20:   top-20 order book → imbalance + sweep detection
"""

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional

import numpy as np
import websockets

from src.config import (
    BINANCE_WS_URL,
    CVD_WINDOW,
    LARGE_TRADE_THRESHOLD,
    ORDERBOOK_DEPTH,
    SPIKE_THRESHOLD_PCT,
    SPIKE_WINDOWS,
    VOLUME_SPIKE_MULTIPLIER,
    get_logger,
)

log = get_logger(__name__)


# ─── Data structures ─────────────────────────────────────────────


@dataclass
class Trade:
    price: float
    qty: float
    quote_qty: float
    is_buyer_maker: bool  # True = seller aggressor, False = buyer aggressor
    timestamp: float


@dataclass
class PricePoint:
    """Lightweight price+time for spike detection."""
    price: float
    timestamp: float


@dataclass
class Candle:
    open_time: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    taker_buy_volume: float
    is_closed: bool


@dataclass
class OrderBookLevel:
    price: float
    qty: float


@dataclass
class Spike:
    """Detected price spike / momentum burst."""
    direction: str  # "up" or "down"
    move_pct: float  # absolute % move
    window_sec: int  # over how many seconds
    price_from: float
    price_to: float
    volume_ratio: float  # recent volume vs average (>1 = above avg)
    book_imbalance: float  # at time of spike
    timestamp: float

    @property
    def strength(self) -> float:
        """Composite spike strength 0-1. Bigger move + faster + more volume = stronger."""
        move_score = min(self.move_pct / 0.5, 1.0)  # 0.5% = max
        speed_score = max(0, 1.0 - self.window_sec / 60)  # faster = higher
        vol_score = min(self.volume_ratio / 5.0, 1.0)  # 5x volume = max
        return (move_score * 0.5 + speed_score * 0.25 + vol_score * 0.25)


@dataclass
class LiveMarketState:
    """
    In-memory state of BTC/USDT, updated in real-time via WebSocket.
    """
    # Current price
    price: float = 0.0
    timestamp: float = 0.0

    # Price history for spike detection (high-frequency, last ~120s)
    price_history: deque = field(default_factory=lambda: deque(maxlen=5000))

    # Detected spikes (recent)
    active_spike: Optional[Spike] = None
    spike_history: deque = field(default_factory=lambda: deque(maxlen=20))

    # Order book
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    bid_depth_usdt: float = 0.0
    ask_depth_usdt: float = 0.0
    book_imbalance: float = 0.0  # (bids-asks)/(bids+asks), [-1, 1]
    prev_bid_depth: float = 0.0  # for sweep detection
    prev_ask_depth: float = 0.0

    # Trade flow
    recent_trades: deque = field(default_factory=lambda: deque(maxlen=CVD_WINDOW))
    cvd: float = 0.0
    buy_volume_1m: float = 0.0
    sell_volume_1m: float = 0.0
    trade_flow_ratio: float = 0.5
    volume_30s: float = 0.0  # total volume last 30s
    volume_5m_avg_30s: float = 0.0  # avg 30s volume over 5 minutes

    # Large trades
    large_trades: deque = field(default_factory=lambda: deque(maxlen=50))
    large_buy_count_5m: int = 0
    large_sell_count_5m: int = 0

    # 1-minute candles (context only)
    candles_1m: deque = field(default_factory=lambda: deque(maxlen=60))

    # Stream health
    connected: bool = False
    last_trade_time: float = 0.0
    last_book_time: float = 0.0

    @property
    def ready(self) -> bool:
        return (
            self.price > 0
            and len(self.price_history) >= 100
            and self.bid_depth_usdt > 0
        )


# ─── Spike detector ──────────────────────────────────────────────


class SpikeDetector:
    """
    Detects sudden price moves by comparing current price to prices
    N seconds ago. This is the core of the lag-arbitrage strategy.
    """

    def __init__(self, state: LiveMarketState, on_spike: Optional[Callable] = None):
        self.state = state
        self.on_spike = on_spike  # callback when spike detected
        self._last_spike_time = 0.0

    def check(self) -> Optional[Spike]:
        """Check for spikes across all configured windows."""
        if len(self.state.price_history) < 10:
            return None

        now = time.time()

        # Don't fire spikes too frequently (min 3s between)
        if now - self._last_spike_time < 3:
            return None

        current_price = self.state.price
        best_spike = None

        for window in SPIKE_WINDOWS:
            cutoff = now - window
            # Find the oldest price within this window
            old_price = None
            for pp in self.state.price_history:
                if pp.timestamp >= cutoff:
                    old_price = pp.price
                    break

            if old_price is None or old_price == 0:
                continue

            move_pct = (current_price - old_price) / old_price * 100

            if abs(move_pct) < SPIKE_THRESHOLD_PCT:
                continue

            direction = "up" if move_pct > 0 else "down"

            # Volume context
            vol_ratio = 1.0
            if self.state.volume_5m_avg_30s > 0:
                vol_ratio = self.state.volume_30s / self.state.volume_5m_avg_30s

            spike = Spike(
                direction=direction,
                move_pct=abs(move_pct),
                window_sec=window,
                price_from=old_price,
                price_to=current_price,
                volume_ratio=vol_ratio,
                book_imbalance=self.state.book_imbalance,
                timestamp=now,
            )

            # Keep the strongest spike
            if best_spike is None or spike.strength > best_spike.strength:
                best_spike = spike

        if best_spike is not None:
            self._last_spike_time = now
            self.state.active_spike = best_spike
            self.state.spike_history.append(best_spike)

            log.info(
                "SPIKE %s | %.2f%% in %ds | $%.0f -> $%.0f | vol=%.1fx | strength=%.2f",
                best_spike.direction.upper(),
                best_spike.move_pct,
                best_spike.window_sec,
                best_spike.price_from,
                best_spike.price_to,
                best_spike.volume_ratio,
                best_spike.strength,
            )

            if self.on_spike:
                self.on_spike(best_spike)

        return best_spike


# ─── State updater ────────────────────────────────────────────────


class StateUpdater:
    """Processes raw WebSocket messages and updates LiveMarketState."""

    def __init__(self, state: LiveMarketState, spike_detector: Optional[SpikeDetector] = None):
        self.state = state
        self.spike_detector = spike_detector
        self._trade_count = 0

    def on_agg_trade(self, data: dict):
        """Handle aggTrade stream message."""
        price = float(data["p"])
        qty = float(data["q"])
        quote_qty = price * qty
        is_buyer_maker = data["m"]
        ts = data["T"] / 1000.0

        trade = Trade(
            price=price, qty=qty, quote_qty=quote_qty,
            is_buyer_maker=is_buyer_maker, timestamp=ts,
        )

        self.state.price = price
        self.state.timestamp = ts
        self.state.last_trade_time = time.time()

        # High-frequency price history for spike detection
        self.state.price_history.append(PricePoint(price, time.time()))

        self.state.recent_trades.append(trade)

        # CVD
        if not is_buyer_maker:
            self.state.cvd += quote_qty
        else:
            self.state.cvd -= quote_qty

        # Large trade detection
        if quote_qty >= LARGE_TRADE_THRESHOLD:
            self.state.large_trades.append(trade)
            direction = "SELL" if is_buyer_maker else "BUY"
            log.info(
                "WHALE %s %.4f BTC ($%.0f) @ %.0f",
                direction, qty, quote_qty, price,
            )

        self._trade_count += 1

        # Update flow metrics + check spikes every ~10 trades (not every tick)
        if self._trade_count % 10 == 0:
            self._update_trade_flow()
            if self.spike_detector:
                self.spike_detector.check()

    def _update_trade_flow(self):
        """Recompute trade flow and volume metrics."""
        now = time.time()
        cutoff_30s = now - 30
        cutoff_1m = now - 60
        cutoff_5m = now - 300

        buy_vol_1m = 0.0
        sell_vol_1m = 0.0
        vol_30s = 0.0
        vol_5m = 0.0
        large_buy = 0
        large_sell = 0

        for t in self.state.recent_trades:
            if t.timestamp >= cutoff_1m:
                if not t.is_buyer_maker:
                    buy_vol_1m += t.quote_qty
                else:
                    sell_vol_1m += t.quote_qty
            if t.timestamp >= cutoff_30s:
                vol_30s += t.quote_qty
            if t.timestamp >= cutoff_5m:
                vol_5m += t.quote_qty

        for t in self.state.large_trades:
            if t.timestamp >= cutoff_5m:
                if not t.is_buyer_maker:
                    large_buy += 1
                else:
                    large_sell += 1

        self.state.buy_volume_1m = buy_vol_1m
        self.state.sell_volume_1m = sell_vol_1m
        total = buy_vol_1m + sell_vol_1m
        self.state.trade_flow_ratio = buy_vol_1m / total if total > 0 else 0.5
        self.state.volume_30s = vol_30s
        # Average 30s volume = total 5m volume / 10 (ten 30s periods)
        self.state.volume_5m_avg_30s = vol_5m / 10.0 if vol_5m > 0 else vol_30s
        self.state.large_buy_count_5m = large_buy
        self.state.large_sell_count_5m = large_sell

    def on_kline(self, data: dict):
        """Handle kline stream message."""
        k = data["k"]
        candle = Candle(
            open_time=k["t"] / 1000.0,
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            quote_volume=float(k["q"]),
            taker_buy_volume=float(k["V"]),
            is_closed=k["x"],
        )
        candles = self.state.candles_1m
        if candle.is_closed:
            if candles and not candles[-1].is_closed:
                candles[-1] = candle
            else:
                candles.append(candle)
        else:
            if candles and not candles[-1].is_closed:
                candles[-1] = candle
            else:
                candles.append(candle)

    def on_depth(self, data: dict):
        """Handle depth stream message."""
        self.state.prev_bid_depth = self.state.bid_depth_usdt
        self.state.prev_ask_depth = self.state.ask_depth_usdt

        self.state.bids = [
            OrderBookLevel(float(p), float(q)) for p, q in data.get("bids", [])
        ]
        self.state.asks = [
            OrderBookLevel(float(p), float(q)) for p, q in data.get("asks", [])
        ]

        bid_depth = sum(b.price * b.qty for b in self.state.bids)
        ask_depth = sum(a.price * a.qty for a in self.state.asks)

        self.state.bid_depth_usdt = bid_depth
        self.state.ask_depth_usdt = ask_depth

        total = bid_depth + ask_depth
        self.state.book_imbalance = (bid_depth - ask_depth) / total if total > 0 else 0.0
        self.state.last_book_time = time.time()


# ─── WebSocket stream manager ────────────────────────────────────


class BinanceStream:
    """Manages Binance WebSocket combined stream connection."""

    COMBINED_URL = "wss://stream.binance.com:9443/stream"
    STREAMS = [
        "btcusdt@aggTrade",
        "btcusdt@kline_1m",
        f"btcusdt@depth{ORDERBOOK_DEPTH}@100ms",
    ]

    def __init__(self, state: LiveMarketState, on_spike: Optional[Callable] = None):
        self.state = state
        self.spike_detector = SpikeDetector(state, on_spike=on_spike)
        self.updater = StateUpdater(state, spike_detector=self.spike_detector)
        self._ws = None
        self._running = False

    async def connect(self):
        streams = "/".join(self.STREAMS)
        url = f"{self.COMBINED_URL}?streams={streams}"

        log.info("Connecting to Binance: %s", url)
        self._running = True
        retry_delay = 1

        while self._running:
            try:
                async with websockets.connect(url, ping_interval=30) as ws:
                    self._ws = ws
                    self.state.connected = True
                    retry_delay = 1
                    log.info("Binance connected — streaming %d feeds", len(self.STREAMS))

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            self._dispatch(json.loads(raw_msg))
                        except Exception as e:
                            log.warning("Message error: %s", e)

            except websockets.ConnectionClosed as e:
                log.warning("WS disconnected: %s", e)
            except Exception as e:
                log.error("WS error: %s", e)

            self.state.connected = False
            if self._running:
                log.info("Reconnecting in %ds...", retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    def _dispatch(self, msg: dict):
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if "aggTrade" in stream:
            self.updater.on_agg_trade(data)
        elif "kline" in stream:
            self.updater.on_kline(data)
        elif "depth" in stream:
            self.updater.on_depth(data)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()


# ─── Price move calculator for lag detection ──────────────────────


def get_price_moves(state: LiveMarketState) -> dict:
    """
    Calculate price moves over multiple windows.
    Returns dict with move_Ns keys (e.g. move_10s, move_30s, move_60s).
    """
    if not state.price_history:
        return {}

    now = time.time()
    current = state.price
    moves = {}

    for window in SPIKE_WINDOWS:
        cutoff = now - window
        old_price = None
        for pp in state.price_history:
            if pp.timestamp >= cutoff:
                old_price = pp.price
                break

        if old_price and old_price > 0:
            moves[f"move_{window}s"] = (current - old_price) / old_price * 100

    return moves

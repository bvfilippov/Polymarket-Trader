"""
Real-time Binance data collector via WebSocket streams.

Streams:
  - aggTrade:  real-time trades → CVD, trade flow, large trade detection
  - kline_1m:  1-minute candles → fast RSI, EMA, VWAP
  - kline_5m:  5-minute candles → medium-term confirmation
  - depth20:   top-20 order book → bid/ask imbalance (leading indicator)

All data is held in-memory in rolling windows and accessible
via the LiveMarketState object.
"""

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import websockets

from src.config import (
    BINANCE_WS_URL,
    CVD_WINDOW,
    LARGE_TRADE_THRESHOLD,
    ORDERBOOK_DEPTH,
    get_logger,
)

log = get_logger(__name__)


# ─── Data structures ─────────────────────────────────────────────


@dataclass
class Trade:
    price: float
    qty: float
    quote_qty: float
    is_buyer_maker: bool  # True = seller aggressor (sell), False = buyer aggressor (buy)
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
class LiveMarketState:
    """
    In-memory state of BTC/USDT market, updated in real-time.
    This is the single source of truth the strategy reads from.
    """
    # Current price
    price: float = 0.0
    timestamp: float = 0.0

    # Order book (leading indicator)
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    bid_depth_usdt: float = 0.0  # total bid liquidity in USDT
    ask_depth_usdt: float = 0.0  # total ask liquidity in USDT
    book_imbalance: float = 0.0  # (bids - asks) / (bids + asks), range [-1, 1]

    # Trade flow (leading indicator)
    recent_trades: deque = field(default_factory=lambda: deque(maxlen=CVD_WINDOW))
    cvd: float = 0.0  # Cumulative Volume Delta (buy vol - sell vol)
    buy_volume_1m: float = 0.0  # aggressive buy volume last 1 minute
    sell_volume_1m: float = 0.0  # aggressive sell volume last 1 minute
    trade_flow_ratio: float = 0.5  # buy_vol / total_vol, 0.5 = neutral

    # Large trades (whale detection)
    large_trades: deque = field(default_factory=lambda: deque(maxlen=50))
    large_buy_count_5m: int = 0
    large_sell_count_5m: int = 0

    # 1-minute candles (fast signals)
    candles_1m: deque = field(default_factory=lambda: deque(maxlen=100))
    rsi_1m: float = 50.0
    ema_fast_1m: float = 0.0  # EMA(9)
    ema_slow_1m: float = 0.0  # EMA(21)
    vwap: float = 0.0  # Session VWAP

    # 5-minute candles (confirmation)
    candles_5m: deque = field(default_factory=lambda: deque(maxlen=60))
    rsi_5m: float = 50.0
    ema_fast_5m: float = 0.0  # EMA(9)
    ema_slow_5m: float = 0.0  # EMA(21)
    macd_5m: float = 0.0
    macd_signal_5m: float = 0.0

    # Momentum
    price_change_5m: float = 0.0
    price_change_15m: float = 0.0
    price_change_1h: float = 0.0

    # Stream health
    connected: bool = False
    last_trade_time: float = 0.0
    last_book_time: float = 0.0

    @property
    def ready(self) -> bool:
        """True when we have enough data to generate signals."""
        return (
            self.price > 0
            and len(self.candles_1m) >= 25
            and len(self.candles_5m) >= 15
            and len(self.recent_trades) >= 50
            and self.bid_depth_usdt > 0
        )


# ─── Indicator calculations ──────────────────────────────────────


def _ema_update(prev: float, value: float, period: int) -> float:
    """Incremental EMA update (no need to recalculate from scratch)."""
    if prev == 0.0:
        return value
    k = 2.0 / (period + 1)
    return value * k + prev * (1 - k)


def _compute_rsi_from_candles(candles: deque, period: int = 14) -> float:
    """Compute RSI from a deque of candles."""
    if len(candles) < period + 1:
        return 50.0

    closes = [c.close for c in candles]
    gains = []
    losses = []

    for i in range(len(closes) - period, len(closes)):
        delta = closes[i] - closes[i - 1]
        if delta > 0:
            gains.append(delta)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(delta))

    avg_gain = np.mean(gains) if gains else 0.0
    avg_loss = np.mean(losses) if losses else 0.0001

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _compute_vwap(candles: deque) -> float:
    """Compute VWAP from candles."""
    if not candles:
        return 0.0

    cum_tp_vol = 0.0
    cum_vol = 0.0

    for c in candles:
        typical_price = (c.high + c.low + c.close) / 3.0
        cum_tp_vol += typical_price * c.volume
        cum_vol += c.volume

    return cum_tp_vol / cum_vol if cum_vol > 0 else 0.0


def _compute_macd(candles: deque) -> tuple[float, float]:
    """Compute MACD(12,26,9) from candles."""
    if len(candles) < 26:
        return 0.0, 0.0

    closes = [c.close for c in candles]

    # EMA 12 and 26
    ema12 = closes[0]
    ema26 = closes[0]
    k12 = 2.0 / 13
    k26 = 2.0 / 27

    macd_values = []
    for price in closes:
        ema12 = price * k12 + ema12 * (1 - k12)
        ema26 = price * k26 + ema26 * (1 - k26)
        macd_values.append(ema12 - ema26)

    # Signal line (EMA 9 of MACD)
    signal = macd_values[0]
    k9 = 2.0 / 10
    for v in macd_values:
        signal = v * k9 + signal * (1 - k9)

    return macd_values[-1], signal


# ─── State updater ────────────────────────────────────────────────


class StateUpdater:
    """Processes raw WebSocket messages and updates LiveMarketState."""

    def __init__(self, state: LiveMarketState):
        self.state = state

    def on_agg_trade(self, data: dict):
        """Handle aggTrade stream message."""
        price = float(data["p"])
        qty = float(data["q"])
        quote_qty = price * qty
        is_buyer_maker = data["m"]  # True = sell aggressor
        ts = data["T"] / 1000.0

        trade = Trade(
            price=price,
            qty=qty,
            quote_qty=quote_qty,
            is_buyer_maker=is_buyer_maker,
            timestamp=ts,
        )

        self.state.price = price
        self.state.timestamp = ts
        self.state.last_trade_time = time.time()

        # Add to rolling window
        self.state.recent_trades.append(trade)

        # Update CVD: buyer aggressor adds, seller aggressor subtracts
        if not is_buyer_maker:
            self.state.cvd += quote_qty
        else:
            self.state.cvd -= quote_qty

        # Large trade detection
        if quote_qty >= LARGE_TRADE_THRESHOLD:
            self.state.large_trades.append(trade)
            direction = "SELL" if is_buyer_maker else "BUY"
            log.info(
                "WHALE: %s %.4f BTC ($%.0f) @ %.2f",
                direction, qty, quote_qty, price,
            )

        # Recompute 1-min flow metrics from recent trades
        self._update_trade_flow()

    def _update_trade_flow(self):
        """Recompute trade flow metrics from recent trades window."""
        now = time.time()
        cutoff_1m = now - 60
        cutoff_5m = now - 300

        buy_vol = 0.0
        sell_vol = 0.0
        large_buy = 0
        large_sell = 0

        for t in self.state.recent_trades:
            if t.timestamp >= cutoff_1m:
                if not t.is_buyer_maker:
                    buy_vol += t.quote_qty
                else:
                    sell_vol += t.quote_qty

        for t in self.state.large_trades:
            if t.timestamp >= cutoff_5m:
                if not t.is_buyer_maker:
                    large_buy += 1
                else:
                    large_sell += 1

        self.state.buy_volume_1m = buy_vol
        self.state.sell_volume_1m = sell_vol
        total = buy_vol + sell_vol
        self.state.trade_flow_ratio = buy_vol / total if total > 0 else 0.5
        self.state.large_buy_count_5m = large_buy
        self.state.large_sell_count_5m = large_sell

    def on_kline(self, data: dict):
        """Handle kline stream message (1m or 5m)."""
        k = data["k"]
        interval = k["i"]
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

        if interval == "1m":
            self._update_candle(self.state.candles_1m, candle)
            self._recompute_1m_indicators()
        elif interval == "5m":
            self._update_candle(self.state.candles_5m, candle)
            self._recompute_5m_indicators()

    def _update_candle(self, candles: deque, candle: Candle):
        """Add or update a candle in the deque."""
        if candle.is_closed:
            # Closed candle — append as final
            if candles and not candles[-1].is_closed:
                candles[-1] = candle  # Replace the live candle
            else:
                candles.append(candle)
        else:
            # Live (in-progress) candle — update or append
            if candles and not candles[-1].is_closed:
                candles[-1] = candle
            else:
                candles.append(candle)

    def _recompute_1m_indicators(self):
        """Recompute fast indicators from 1m candles."""
        candles = self.state.candles_1m
        if len(candles) < 2:
            return

        last_close = candles[-1].close

        # Fast EMA(9) and slow EMA(21)
        self.state.ema_fast_1m = _ema_update(self.state.ema_fast_1m, last_close, 9)
        self.state.ema_slow_1m = _ema_update(self.state.ema_slow_1m, last_close, 21)

        # RSI(14) on 1m
        self.state.rsi_1m = _compute_rsi_from_candles(candles, 14)

        # VWAP
        self.state.vwap = _compute_vwap(candles)

        # Price changes
        if len(candles) >= 6:
            self.state.price_change_5m = (
                (last_close - candles[-6].close) / candles[-6].close * 100
            )
        if len(candles) >= 16:
            self.state.price_change_15m = (
                (last_close - candles[-16].close) / candles[-16].close * 100
            )
        if len(candles) >= 61:
            self.state.price_change_1h = (
                (last_close - candles[-61].close) / candles[-61].close * 100
            )

    def _recompute_5m_indicators(self):
        """Recompute medium-term indicators from 5m candles."""
        candles = self.state.candles_5m
        if len(candles) < 2:
            return

        last_close = candles[-1].close

        self.state.ema_fast_5m = _ema_update(self.state.ema_fast_5m, last_close, 9)
        self.state.ema_slow_5m = _ema_update(self.state.ema_slow_5m, last_close, 21)
        self.state.rsi_5m = _compute_rsi_from_candles(candles, 14)
        self.state.macd_5m, self.state.macd_signal_5m = _compute_macd(candles)

    def on_depth(self, data: dict):
        """Handle depth stream message (top-N order book)."""
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
    """
    Manages multiple Binance WebSocket streams in a single connection
    using the combined stream URL.
    """

    COMBINED_URL = "wss://stream.binance.com:9443/stream"
    STREAMS = [
        "btcusdt@aggTrade",
        "btcusdt@kline_1m",
        "btcusdt@kline_5m",
        f"btcusdt@depth{ORDERBOOK_DEPTH}@100ms",
    ]

    def __init__(self, state: LiveMarketState):
        self.state = state
        self.updater = StateUpdater(state)
        self._ws = None
        self._running = False

    async def connect(self):
        """Connect and subscribe to all streams."""
        streams = "/".join(self.STREAMS)
        url = f"{self.COMBINED_URL}?streams={streams}"

        log.info("Connecting to Binance WebSocket: %s", url)

        self._running = True
        retry_delay = 1

        while self._running:
            try:
                async with websockets.connect(url, ping_interval=30) as ws:
                    self._ws = ws
                    self.state.connected = True
                    retry_delay = 1
                    log.info("Binance WebSocket connected, streaming %d feeds", len(self.STREAMS))

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            self._dispatch(json.loads(raw_msg))
                        except Exception as e:
                            log.warning("Error processing message: %s", e)

            except websockets.ConnectionClosed as e:
                log.warning("WebSocket disconnected: %s", e)
            except Exception as e:
                log.error("WebSocket error: %s", e)

            self.state.connected = False
            if self._running:
                log.info("Reconnecting in %ds...", retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    def _dispatch(self, msg: dict):
        """Route a combined stream message to the right handler."""
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if "aggTrade" in stream:
            self.updater.on_agg_trade(data)
        elif "kline" in stream:
            self.updater.on_kline(data)
        elif "depth" in stream:
            self.updater.on_depth(data)

    async def stop(self):
        """Stop the stream."""
        self._running = False
        if self._ws:
            await self._ws.close()


# ─── Direction estimator (reads from LiveMarketState) ─────────────


def estimate_btc_direction(state: LiveMarketState) -> dict:
    """
    Real-time directional estimate based on leading + confirming indicators.

    Leading indicators (react first, highest weight):
      - Order book imbalance
      - Trade flow (CVD, buy/sell ratio)
      - Large trade pressure (whale tracking)

    Confirming indicators (react after, lower weight):
      - RSI on 1m / 5m
      - EMA crossovers
      - MACD on 5m
      - VWAP position
      - Momentum
    """
    signals = []
    bullish_score = 0.0
    total_weight = 0.0

    # ── LEADING: Order book imbalance (weight 3) ──
    weight = 3.0
    total_weight += weight
    imb = state.book_imbalance
    if imb > 0.15:
        score = 0.5 + min(imb, 0.5) * 0.6  # up to 0.8
        bullish_score += weight * score
        signals.append(f"Book imbalance BULLISH ({imb:+.2f}, bids>{asks_label(state)})")
    elif imb < -0.15:
        score = 0.5 - min(abs(imb), 0.5) * 0.6  # down to 0.2
        bullish_score += weight * score
        signals.append(f"Book imbalance BEARISH ({imb:+.2f}, asks>{bids_label(state)})")
    else:
        bullish_score += weight * 0.5
        signals.append(f"Book imbalance neutral ({imb:+.2f})")

    # ── LEADING: Trade flow / CVD (weight 3) ──
    weight = 3.0
    total_weight += weight
    ratio = state.trade_flow_ratio
    if ratio > 0.6:
        bullish_score += weight * 0.75
        signals.append(f"Trade flow BULLISH (buy {ratio:.0%} of volume)")
    elif ratio > 0.55:
        bullish_score += weight * 0.6
        signals.append(f"Trade flow leaning buy ({ratio:.0%})")
    elif ratio < 0.4:
        bullish_score += weight * 0.25
        signals.append(f"Trade flow BEARISH (buy only {ratio:.0%})")
    elif ratio < 0.45:
        bullish_score += weight * 0.4
        signals.append(f"Trade flow leaning sell ({ratio:.0%})")
    else:
        bullish_score += weight * 0.5
        signals.append(f"Trade flow neutral ({ratio:.0%})")

    # ── LEADING: Large trades / whales (weight 2.5) ──
    weight = 2.5
    total_weight += weight
    lb = state.large_buy_count_5m
    ls = state.large_sell_count_5m
    if lb > ls + 1:
        bullish_score += weight * 0.75
        signals.append(f"Whale pressure BUY ({lb} buys vs {ls} sells in 5m)")
    elif ls > lb + 1:
        bullish_score += weight * 0.25
        signals.append(f"Whale pressure SELL ({ls} sells vs {lb} buys in 5m)")
    elif lb > 0 or ls > 0:
        bullish_score += weight * 0.5
        signals.append(f"Whale activity mixed ({lb}B/{ls}S in 5m)")
    else:
        bullish_score += weight * 0.5
        signals.append("No whale activity")

    # ── CONFIRMING: RSI 1m (weight 1.5) ──
    weight = 1.5
    total_weight += weight
    rsi = state.rsi_1m
    if rsi < 25:
        bullish_score += weight * 0.8
        signals.append(f"RSI(1m) oversold ({rsi:.0f})")
    elif rsi < 40:
        bullish_score += weight * 0.6
        signals.append(f"RSI(1m) low ({rsi:.0f})")
    elif rsi > 75:
        bullish_score += weight * 0.2
        signals.append(f"RSI(1m) overbought ({rsi:.0f})")
    elif rsi > 60:
        bullish_score += weight * 0.4
        signals.append(f"RSI(1m) high ({rsi:.0f})")
    else:
        bullish_score += weight * 0.5
        signals.append(f"RSI(1m) neutral ({rsi:.0f})")

    # ── CONFIRMING: EMA cross 1m (weight 1.5) ──
    weight = 1.5
    total_weight += weight
    if state.ema_fast_1m > 0 and state.ema_slow_1m > 0:
        if state.ema_fast_1m > state.ema_slow_1m:
            bullish_score += weight * 0.65
            signals.append("EMA(9) > EMA(21) on 1m")
        else:
            bullish_score += weight * 0.35
            signals.append("EMA(9) < EMA(21) on 1m")
    else:
        bullish_score += weight * 0.5

    # ── CONFIRMING: MACD 5m (weight 1.5) ──
    weight = 1.5
    total_weight += weight
    macd_diff = state.macd_5m - state.macd_signal_5m
    if macd_diff > 0:
        bullish_score += weight * 0.65
        signals.append(f"MACD(5m) bullish ({macd_diff:+.1f})")
    else:
        bullish_score += weight * 0.35
        signals.append(f"MACD(5m) bearish ({macd_diff:+.1f})")

    # ── CONFIRMING: VWAP position (weight 1.5) ──
    weight = 1.5
    total_weight += weight
    if state.vwap > 0:
        vwap_pct = (state.price - state.vwap) / state.vwap * 100
        if vwap_pct > 0.1:
            bullish_score += weight * 0.6
            signals.append(f"Price above VWAP ({vwap_pct:+.2f}%)")
        elif vwap_pct < -0.1:
            bullish_score += weight * 0.4
            signals.append(f"Price below VWAP ({vwap_pct:+.2f}%)")
        else:
            bullish_score += weight * 0.5
            signals.append(f"Price at VWAP ({vwap_pct:+.2f}%)")
    else:
        bullish_score += weight * 0.5

    # ── CONFIRMING: 5m momentum (weight 1) ──
    weight = 1.0
    total_weight += weight
    if state.price_change_5m > 0.2:
        bullish_score += weight * 0.65
        signals.append(f"5m momentum UP ({state.price_change_5m:+.2f}%)")
    elif state.price_change_5m < -0.2:
        bullish_score += weight * 0.35
        signals.append(f"5m momentum DOWN ({state.price_change_5m:+.2f}%)")
    else:
        bullish_score += weight * 0.5
        signals.append(f"5m momentum flat ({state.price_change_5m:+.2f}%)")

    # Final probability
    probability_up = bullish_score / total_weight
    probability_up = max(0.15, min(0.85, probability_up))
    direction = "up" if probability_up > 0.5 else "down"

    return {
        "direction": direction,
        "confidence": probability_up,
        "reasons": signals,
    }


def asks_label(state: LiveMarketState) -> str:
    return f"${state.bid_depth_usdt:,.0f}B/${state.ask_depth_usdt:,.0f}A"


def bids_label(state: LiveMarketState) -> str:
    return f"${state.ask_depth_usdt:,.0f}A/${state.bid_depth_usdt:,.0f}B"

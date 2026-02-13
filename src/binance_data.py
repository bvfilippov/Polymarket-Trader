"""
Binance data collector — fetches BTC/USDT price data and computes technical indicators
to generate directional signals for Polymarket BTC prediction markets.
"""

import time
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import requests

from src.config import get_logger

log = get_logger(__name__)

BINANCE_BASE_URL = "https://api.binance.com"


@dataclass
class BinanceSnapshot:
    """A point-in-time snapshot of BTC market data from Binance."""
    price: float
    price_change_1h: float  # % change over 1h
    price_change_4h: float  # % change over 4h
    price_change_24h: float  # % change over 24h
    volume_24h: float  # 24h USDT volume
    rsi_14: float  # RSI(14) on 1h candles
    ema_short: float  # EMA(12) on 1h candles
    ema_long: float  # EMA(26) on 1h candles
    macd: float  # MACD line
    macd_signal: float  # MACD signal line
    bollinger_upper: float
    bollinger_lower: float
    atr_14: float  # Average True Range(14)
    timestamp: float


def fetch_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    limit: int = 100,
) -> pd.DataFrame:
    """Fetch kline/candlestick data from Binance public API."""
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ])

    for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
        df[col] = df[col].astype(float)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    return df


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def compute_bollinger(series: pd.Series, period: int = 20, std_dev: float = 2.0):
    sma = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    return upper, lower


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return true_range.rolling(period).mean()


def get_btc_snapshot() -> BinanceSnapshot:
    """Build a complete BTC market snapshot from Binance data."""
    log.info("Fetching BTC data from Binance...")

    # Fetch 1h candles (last 100 hours)
    df_1h = fetch_klines("BTCUSDT", "1h", 100)

    current_price = df_1h["close"].iloc[-1]

    # Price changes
    price_1h_ago = df_1h["close"].iloc[-2] if len(df_1h) >= 2 else current_price
    price_4h_ago = df_1h["close"].iloc[-5] if len(df_1h) >= 5 else current_price
    price_24h_ago = df_1h["close"].iloc[-25] if len(df_1h) >= 25 else current_price

    price_change_1h = (current_price - price_1h_ago) / price_1h_ago * 100
    price_change_4h = (current_price - price_4h_ago) / price_4h_ago * 100
    price_change_24h = (current_price - price_24h_ago) / price_24h_ago * 100

    # 24h volume from the last 24 candles
    volume_24h = df_1h["quote_volume"].iloc[-24:].sum()

    # Technical indicators
    rsi_14 = compute_rsi(df_1h["close"], 14).iloc[-1]

    ema_short = compute_ema(df_1h["close"], 12).iloc[-1]
    ema_long = compute_ema(df_1h["close"], 26).iloc[-1]

    macd_line = ema_short - ema_long
    macd_signal_series = compute_ema(
        compute_ema(df_1h["close"], 12) - compute_ema(df_1h["close"], 26), 9
    )
    macd_signal = macd_signal_series.iloc[-1]

    bb_upper, bb_lower = compute_bollinger(df_1h["close"], 20, 2.0)
    atr_14 = compute_atr(df_1h, 14).iloc[-1]

    snapshot = BinanceSnapshot(
        price=current_price,
        price_change_1h=price_change_1h,
        price_change_4h=price_change_4h,
        price_change_24h=price_change_24h,
        volume_24h=volume_24h,
        rsi_14=rsi_14,
        ema_short=ema_short,
        ema_long=ema_long,
        macd=macd_line,
        macd_signal=macd_signal,
        bollinger_upper=bb_upper.iloc[-1],
        bollinger_lower=bb_lower.iloc[-1],
        atr_14=atr_14,
        timestamp=time.time(),
    )

    log.info(
        "BTC snapshot: price=%.2f, RSI=%.1f, MACD=%.2f, change_1h=%.2f%%",
        snapshot.price, snapshot.rsi_14, snapshot.macd, snapshot.price_change_1h,
    )
    return snapshot


def estimate_btc_direction(snapshot: BinanceSnapshot) -> dict:
    """
    Estimate the probability that BTC will go UP in the near term,
    based on the Binance data snapshot.

    Returns dict with:
      - direction: "up" or "down"
      - confidence: float 0.0-1.0 (our estimated probability of going up)
      - reasons: list of strings explaining the signal
    """
    signals = []
    bullish_score = 0.0
    total_weight = 0.0

    # 1. RSI signal (weight 2)
    weight = 2.0
    total_weight += weight
    if snapshot.rsi_14 < 30:
        bullish_score += weight * 0.8  # Oversold → likely bounce up
        signals.append(f"RSI oversold ({snapshot.rsi_14:.1f})")
    elif snapshot.rsi_14 < 45:
        bullish_score += weight * 0.6
        signals.append(f"RSI below midpoint ({snapshot.rsi_14:.1f})")
    elif snapshot.rsi_14 > 70:
        bullish_score += weight * 0.2  # Overbought → likely pullback
        signals.append(f"RSI overbought ({snapshot.rsi_14:.1f})")
    elif snapshot.rsi_14 > 55:
        bullish_score += weight * 0.4
        signals.append(f"RSI above midpoint ({snapshot.rsi_14:.1f})")
    else:
        bullish_score += weight * 0.5  # Neutral
        signals.append(f"RSI neutral ({snapshot.rsi_14:.1f})")

    # 2. MACD signal (weight 2)
    weight = 2.0
    total_weight += weight
    macd_diff = snapshot.macd - snapshot.macd_signal
    if macd_diff > 0:
        bullish_score += weight * 0.65
        signals.append(f"MACD bullish (diff={macd_diff:.2f})")
    else:
        bullish_score += weight * 0.35
        signals.append(f"MACD bearish (diff={macd_diff:.2f})")

    # 3. EMA crossover signal (weight 1.5)
    weight = 1.5
    total_weight += weight
    if snapshot.ema_short > snapshot.ema_long:
        bullish_score += weight * 0.65
        signals.append("EMA12 > EMA26 (bullish)")
    else:
        bullish_score += weight * 0.35
        signals.append("EMA12 < EMA26 (bearish)")

    # 4. Bollinger Band position (weight 1.5)
    weight = 1.5
    total_weight += weight
    bb_range = snapshot.bollinger_upper - snapshot.bollinger_lower
    if bb_range > 0:
        bb_position = (snapshot.price - snapshot.bollinger_lower) / bb_range
        if bb_position < 0.2:
            bullish_score += weight * 0.7  # Near lower band → mean reversion up
            signals.append(f"Price near lower Bollinger Band ({bb_position:.2f})")
        elif bb_position > 0.8:
            bullish_score += weight * 0.3  # Near upper band → mean reversion down
            signals.append(f"Price near upper Bollinger Band ({bb_position:.2f})")
        else:
            bullish_score += weight * 0.5
            signals.append(f"Price mid Bollinger range ({bb_position:.2f})")
    else:
        bullish_score += weight * 0.5

    # 5. Short-term momentum (weight 2)
    weight = 2.0
    total_weight += weight
    if snapshot.price_change_1h > 1.0:
        bullish_score += weight * 0.6
        signals.append(f"Strong 1h momentum (+{snapshot.price_change_1h:.2f}%)")
    elif snapshot.price_change_1h > 0:
        bullish_score += weight * 0.55
        signals.append(f"Positive 1h momentum (+{snapshot.price_change_1h:.2f}%)")
    elif snapshot.price_change_1h < -1.0:
        bullish_score += weight * 0.4
        signals.append(f"Strong 1h drop ({snapshot.price_change_1h:.2f}%)")
    else:
        bullish_score += weight * 0.45
        signals.append(f"Slight 1h decline ({snapshot.price_change_1h:.2f}%)")

    # 6. Medium-term trend (weight 1)
    weight = 1.0
    total_weight += weight
    if snapshot.price_change_4h > 0:
        bullish_score += weight * 0.6
        signals.append(f"4h trend up (+{snapshot.price_change_4h:.2f}%)")
    else:
        bullish_score += weight * 0.4
        signals.append(f"4h trend down ({snapshot.price_change_4h:.2f}%)")

    # Calculate probability
    probability_up = bullish_score / total_weight
    # Clamp to reasonable range (avoid extreme confidence)
    probability_up = max(0.15, min(0.85, probability_up))

    direction = "up" if probability_up > 0.5 else "down"

    log.info(
        "Direction estimate: %s (P(up)=%.3f) based on %d signals",
        direction, probability_up, len(signals),
    )

    return {
        "direction": direction,
        "confidence": probability_up,
        "reasons": signals,
    }

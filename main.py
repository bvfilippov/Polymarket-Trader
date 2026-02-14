#!/usr/bin/env python3
"""Entry point for the Polymarket BTC Trading Bot (Real-Time)."""

import argparse
import sys

from src.config import get_logger

log = get_logger(__name__)


def cmd_run(args):
    """Run the bot in continuous real-time mode."""
    from src.bot import TradingBot
    bot = TradingBot()
    bot.run()


def cmd_once(args):
    """Run a single evaluation cycle (REST fallback)."""
    from src.bot import TradingBot
    bot = TradingBot()
    bot.run_once()


def cmd_markets(args):
    """List active BTC prediction markets on Polymarket."""
    from src.polymarket_client import PolymarketClient

    client = PolymarketClient()
    markets = client.find_btc_markets(active_only=True)

    if not markets:
        print("No active BTC markets found.")
        return

    for i, m in enumerate(markets, 1):
        client.get_market_prices(m)
        print(f"\n[{i}] {m.question}")
        print(f"    Slug: {m.market_slug}")
        print(f"    End: {m.end_date}")
        if m.current_price_yes is not None:
            print(f"    YES: ${m.current_price_yes:.4f}  NO: ${m.current_price_no:.4f}")
        print(f"    Condition: {m.condition_id[:16]}...")


def cmd_snapshot(args):
    """Show real-time BTC analysis from Binance (REST fallback)."""
    import requests
    from src.binance_data import LiveMarketState, StateUpdater, estimate_btc_direction

    state = LiveMarketState()
    updater = StateUpdater(state)

    print("Fetching BTC data from Binance...")

    # Fetch klines
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

    # Display
    estimate = estimate_btc_direction(state)

    print(f"\n{'='*55}")
    print(f"  BTC/USDT: ${state.price:,.2f}")
    print(f"{'='*55}")
    print(f"\n  LEADING INDICATORS:")
    print(f"  Order book imbalance:  {state.book_imbalance:+.3f}")
    print(f"    Bid depth:  ${state.bid_depth_usdt:>12,.0f}")
    print(f"    Ask depth:  ${state.ask_depth_usdt:>12,.0f}")
    print(f"  Trade flow (1m):       {state.trade_flow_ratio:.0%} buy")
    print(f"    Buy volume:  ${state.buy_volume_1m:>12,.0f}")
    print(f"    Sell volume: ${state.sell_volume_1m:>12,.0f}")
    print(f"  Whales (5m):           {state.large_buy_count_5m}B / {state.large_sell_count_5m}S")

    print(f"\n  CONFIRMING INDICATORS:")
    print(f"  RSI(1m):    {state.rsi_1m:>6.1f}")
    print(f"  RSI(5m):    {state.rsi_5m:>6.1f}")
    print(f"  EMA(9/21) 1m:  {state.ema_fast_1m:>10,.2f} / {state.ema_slow_1m:>10,.2f}")
    print(f"  MACD(5m):   {state.macd_5m:>+8.1f}  signal: {state.macd_signal_5m:>+8.1f}")
    print(f"  VWAP:       ${state.vwap:>10,.2f}")
    print(f"  Change 5m:  {state.price_change_5m:>+6.2f}%")
    print(f"  Change 15m: {state.price_change_15m:>+6.2f}%")
    print(f"  Change 1h:  {state.price_change_1h:>+6.2f}%")

    print(f"\n{'='*55}")
    print(f"  VERDICT:  BTC {estimate['direction'].upper()}")
    print(f"  P(up):    {estimate['confidence']:.1%}")
    print(f"{'='*55}")
    print(f"  Signals:")
    for r in estimate["reasons"]:
        print(f"    {r}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket BTC Trading Bot — real-time trading using Binance WebSocket streams"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("run", help="Run the bot in real-time continuous mode")
    subparsers.add_parser("once", help="Run a single evaluation cycle (REST)")
    subparsers.add_parser("markets", help="List active BTC prediction markets")
    subparsers.add_parser("snapshot", help="Show BTC analysis with leading indicators")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    commands = {
        "run": cmd_run,
        "once": cmd_once,
        "markets": cmd_markets,
        "snapshot": cmd_snapshot,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()

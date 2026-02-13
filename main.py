#!/usr/bin/env python3
"""Entry point for the Polymarket BTC Trading Bot."""

import argparse
import json
import sys

from src.config import get_logger

log = get_logger(__name__)


def cmd_run(args):
    """Run the trading bot in continuous mode."""
    from src.bot import TradingBot
    bot = TradingBot()
    bot.run()


def cmd_once(args):
    """Run a single iteration of the trading loop."""
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
    """Show current BTC data from Binance."""
    from src.binance_data import get_btc_snapshot, estimate_btc_direction

    snapshot = get_btc_snapshot()
    estimate = estimate_btc_direction(snapshot)

    print(f"\n{'='*50}")
    print(f"BTC Price: ${snapshot.price:,.2f}")
    print(f"{'='*50}")
    print(f"Change 1h:  {snapshot.price_change_1h:+.2f}%")
    print(f"Change 4h:  {snapshot.price_change_4h:+.2f}%")
    print(f"Change 24h: {snapshot.price_change_24h:+.2f}%")
    print(f"Volume 24h: ${snapshot.volume_24h:,.0f}")
    print(f"RSI(14):    {snapshot.rsi_14:.1f}")
    print(f"EMA(12):    ${snapshot.ema_short:,.2f}")
    print(f"EMA(26):    ${snapshot.ema_long:,.2f}")
    print(f"MACD:       {snapshot.macd:.2f}")
    print(f"MACD Sig:   {snapshot.macd_signal:.2f}")
    print(f"BB Upper:   ${snapshot.bollinger_upper:,.2f}")
    print(f"BB Lower:   ${snapshot.bollinger_lower:,.2f}")
    print(f"ATR(14):    ${snapshot.atr_14:,.2f}")
    print(f"{'='*50}")
    print(f"Direction:  {estimate['direction'].upper()}")
    print(f"Confidence: {estimate['confidence']:.1%}")
    print(f"Signals:")
    for r in estimate['reasons']:
        print(f"  - {r}")


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket BTC Trading Bot — trade BTC prediction markets using Binance signals"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("run", help="Run the bot continuously")
    subparsers.add_parser("once", help="Run a single trading iteration")
    subparsers.add_parser("markets", help="List active BTC prediction markets")
    subparsers.add_parser("snapshot", help="Show current BTC analysis from Binance")

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

#!/usr/bin/env python3
"""Polymarket BTC Lag-Arbitrage Bot — entry point."""

import argparse
import sys

from src.config import get_logger

log = get_logger(__name__)


def cmd_run(args):
    from src.bot import TradingBot
    TradingBot().run()


def cmd_once(args):
    from src.bot import TradingBot
    TradingBot().run_once()


def cmd_markets(args):
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
    import requests
    from src.binance_data import LiveMarketState, StateUpdater, get_price_moves

    state = LiveMarketState()
    updater = StateUpdater(state)

    print("Fetching BTC data from Binance...")

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

    resp = requests.get(
        "https://api.binance.com/api/v3/depth",
        params={"symbol": "BTCUSDT", "limit": 20}, timeout=10,
    )
    resp.raise_for_status()
    updater.on_depth(resp.json())

    resp = requests.get(
        "https://api.binance.com/api/v3/aggTrades",
        params={"symbol": "BTCUSDT", "limit": 500}, timeout=10,
    )
    resp.raise_for_status()
    for t in resp.json():
        updater.on_agg_trade(t)

    moves = get_price_moves(state)

    print(f"\n{'='*55}")
    print(f"  BTC/USDT: ${state.price:,.2f}")
    print(f"{'='*55}")

    print(f"\n  PRICE MOVES (lag-arb triggers):")
    for k, v in sorted(moves.items()):
        bar = "+" * int(abs(v) * 20) if v > 0 else "-" * int(abs(v) * 20)
        print(f"    {k:>10s}: {v:+.3f}%  {bar}")

    print(f"\n  ORDER BOOK:")
    print(f"    Imbalance:  {state.book_imbalance:+.3f}")
    print(f"    Bid depth:  ${state.bid_depth_usdt:>12,.0f}")
    print(f"    Ask depth:  ${state.ask_depth_usdt:>12,.0f}")

    print(f"\n  TRADE FLOW (1m):")
    print(f"    Buy ratio:  {state.trade_flow_ratio:.0%}")
    print(f"    Buy vol:    ${state.buy_volume_1m:>12,.0f}")
    print(f"    Sell vol:   ${state.sell_volume_1m:>12,.0f}")
    print(f"    Vol 30s:    ${state.volume_30s:>12,.0f}")
    print(f"    Vol avg 30s:${state.volume_5m_avg_30s:>12,.0f}")

    print(f"\n  WHALES (5m): {state.large_buy_count_5m}B / {state.large_sell_count_5m}S")

    if state.active_spike:
        s = state.active_spike
        print(f"\n  ACTIVE SPIKE: {s.direction.upper()} {s.move_pct:.2f}% in {s.window_sec}s (strength={s.strength:.2f})")
    else:
        print(f"\n  No active spike")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket BTC Lag-Arbitrage Bot"
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("run", help="Run bot in real-time (WebSocket)")
    sub.add_parser("once", help="Single evaluation (REST)")
    sub.add_parser("markets", help="List active BTC markets")
    sub.add_parser("snapshot", help="BTC snapshot with lag-arb data")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    {"run": cmd_run, "once": cmd_once, "markets": cmd_markets, "snapshot": cmd_snapshot}[args.command](args)


if __name__ == "__main__":
    main()

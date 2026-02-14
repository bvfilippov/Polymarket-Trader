"""
Polymarket WebSocket client for real-time price updates.

Eliminates 200-500ms REST latency per price refresh by streaming
live orderbook changes.

Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
"""

import asyncio
import json
import time
from typing import Callable, Optional

import websockets

from src.config import get_logger
from src.polymarket_client import BtcMarket

log = get_logger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PolymarketStream:
    """Real-time Polymarket price stream via WebSocket."""

    def __init__(self, on_price_update: Optional[Callable] = None):
        self._ws = None
        self._running = False
        self._subscribed_assets: set[str] = set()
        self._prices: dict[str, float] = {}  # asset_id → midpoint
        self._last_update: dict[str, float] = {}  # asset_id → timestamp
        self.on_price_update = on_price_update
        self.connected = False

    def get_price(self, asset_id: str) -> Optional[float]:
        """Get latest price for an asset, or None if no WS data."""
        return self._prices.get(asset_id)

    def get_last_update(self, asset_id: str) -> float:
        """Get timestamp of last price update for an asset."""
        return self._last_update.get(asset_id, 0)

    def is_fresh(self, asset_id: str, max_age: float = 5.0) -> bool:
        """Check if we have a recent price update (within max_age seconds)."""
        last = self._last_update.get(asset_id, 0)
        return (time.time() - last) < max_age

    async def connect(self):
        """Connect and stream Polymarket price updates."""
        self._running = True
        retry_delay = 1

        while self._running:
            try:
                async with websockets.connect(WS_URL, ping_interval=30) as ws:
                    self._ws = ws
                    self.connected = True
                    retry_delay = 1
                    log.info("Polymarket WS connected")

                    # Re-subscribe on reconnect
                    if self._subscribed_assets:
                        await self._send_subscribe(list(self._subscribed_assets))

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            self._handle_message(json.loads(raw_msg))
                        except Exception as e:
                            log.debug("Polymarket WS parse: %s", e)

            except websockets.ConnectionClosed as e:
                log.warning("Polymarket WS disconnected: %s", e)
            except Exception as e:
                log.error("Polymarket WS error: %s", e)

            self.connected = False
            if self._running:
                log.info("Polymarket WS reconnecting in %ds...", retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    async def subscribe(self, markets: list[BtcMarket]):
        """Subscribe to price updates for the given markets."""
        asset_ids = []
        for m in markets:
            if m.token_id_yes:
                asset_ids.append(m.token_id_yes)
            if m.token_id_no:
                asset_ids.append(m.token_id_no)

        new_assets = set(asset_ids) - self._subscribed_assets
        if not new_assets:
            return

        self._subscribed_assets.update(new_assets)

        if self._ws and self.connected:
            await self._send_subscribe(list(new_assets))

    async def _send_subscribe(self, asset_ids: list[str]):
        msg = {
            "assets_ids": asset_ids,
            "type": "market",
        }
        try:
            await self._ws.send(json.dumps(msg))
            log.info("Polymarket WS: subscribed to %d assets", len(asset_ids))
        except Exception as e:
            log.warning("Polymarket WS subscribe error: %s", e)

    def _handle_message(self, msg: dict):
        """Process incoming WebSocket message and extract price updates."""
        # Handle list of events (batch)
        if isinstance(msg, list):
            for item in msg:
                self._handle_single(item)
            return

        self._handle_single(msg)

    def _handle_single(self, msg: dict):
        """Handle a single message/event."""
        asset_id = msg.get("asset_id", "")
        if not asset_id:
            # Some messages wrap data differently
            asset_id = msg.get("market", {}).get("asset_id", "") if isinstance(msg.get("market"), dict) else ""

        if not asset_id:
            return

        price = self._extract_price(msg)
        if price is not None and 0 < price < 1:
            old_price = self._prices.get(asset_id)
            self._prices[asset_id] = price
            self._last_update[asset_id] = time.time()

            if self.on_price_update:
                self.on_price_update(asset_id, price)

            # Log significant price changes
            if old_price is not None and abs(price - old_price) > 0.005:
                log.debug("Polymarket price: %s… %.3f→%.3f", asset_id[:12], old_price, price)

    def _extract_price(self, msg: dict) -> Optional[float]:
        """Try to extract a price/midpoint from the message."""
        # Direct price field
        for key in ("price", "mid", "midpoint", "last_price"):
            if key in msg:
                try:
                    return float(msg[key])
                except (ValueError, TypeError):
                    pass

        # Nested market data
        market = msg.get("market")
        if isinstance(market, dict):
            for key in ("mid", "midpoint", "price", "last_price"):
                if key in market:
                    try:
                        return float(market[key])
                    except (ValueError, TypeError):
                        pass

        # Compute midpoint from best bid/ask
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        if bids and asks:
            try:
                best_bid = float(bids[0]["price"]) if isinstance(bids[0], dict) else float(bids[0][0])
                best_ask = float(asks[0]["price"]) if isinstance(asks[0], dict) else float(asks[0][0])
                if 0 < best_bid < best_ask < 1:
                    return (best_bid + best_ask) / 2
            except (ValueError, TypeError, IndexError, KeyError):
                pass

        return None

    def update_market_prices(self, markets: list[BtcMarket]):
        """Push latest WS prices into BtcMarket objects."""
        for m in markets:
            yes = self._prices.get(m.token_id_yes)
            no = self._prices.get(m.token_id_no)
            if yes is not None:
                m.current_price_yes = yes
            if no is not None:
                m.current_price_no = no

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

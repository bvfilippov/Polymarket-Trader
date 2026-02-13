"""
Polymarket client — discovers BTC prediction markets via Gamma API
and places trades via the CLOB API using py-clob-client.
"""

from dataclasses import dataclass
from typing import Optional

import requests

from src.config import (
    GAMMA_API_URL,
    POLYMARKET_CHAIN_ID,
    POLYMARKET_FUNDER_ADDRESS,
    POLYMARKET_HOST,
    POLYMARKET_PRIVATE_KEY,
    POLYMARKET_SIGNATURE_TYPE,
    DRY_RUN,
    get_logger,
)

log = get_logger(__name__)


@dataclass
class BtcMarket:
    """Represents a Polymarket BTC prediction market."""
    condition_id: str
    question: str
    token_id_yes: str
    token_id_no: str
    market_slug: str
    end_date: str
    current_price_yes: Optional[float] = None
    current_price_no: Optional[float] = None


class PolymarketClient:
    def __init__(self):
        self._clob_client = None
        self._authenticated = False

    def _get_clob_client(self):
        """Lazy-initialize the CLOB client."""
        if self._clob_client is not None:
            return self._clob_client

        from py_clob_client.client import ClobClient

        if POLYMARKET_PRIVATE_KEY:
            self._clob_client = ClobClient(
                POLYMARKET_HOST,
                key=POLYMARKET_PRIVATE_KEY,
                chain_id=POLYMARKET_CHAIN_ID,
                signature_type=POLYMARKET_SIGNATURE_TYPE,
                funder=POLYMARKET_FUNDER_ADDRESS or None,
            )
            try:
                creds = self._clob_client.create_or_derive_api_creds()
                self._clob_client.set_api_creds(creds)
                self._authenticated = True
                log.info("CLOB client authenticated successfully")
            except Exception as e:
                log.warning("Failed to authenticate CLOB client: %s", e)
                self._authenticated = False
        else:
            self._clob_client = ClobClient(POLYMARKET_HOST)
            log.info("CLOB client initialized in read-only mode (no private key)")

        return self._clob_client

    def find_btc_markets(self, active_only: bool = True) -> list[BtcMarket]:
        """
        Search the Gamma API for active BTC prediction markets.
        Looks for markets with BTC/Bitcoin related tags and keywords.
        """
        log.info("Searching for BTC prediction markets on Polymarket...")
        markets = []

        # Search for events tagged with crypto/bitcoin
        params = {
            "active": "true" if active_only else "false",
            "closed": "false",
            "limit": 50,
            "order": "volume",
            "ascending": "false",
        }

        try:
            # Search events
            resp = requests.get(
                f"{GAMMA_API_URL}/events",
                params={**params, "tag": "crypto"},
                timeout=15,
            )
            resp.raise_for_status()
            events = resp.json()

            for event in events:
                title = event.get("title", "").lower()
                slug = event.get("slug", "").lower()

                # Filter for BTC-related markets
                if not any(kw in title or kw in slug for kw in ["btc", "bitcoin"]):
                    continue

                for mkt in event.get("markets", []):
                    token_ids = mkt.get("clobTokenIds", [])
                    if len(token_ids) < 2:
                        continue

                    market = BtcMarket(
                        condition_id=mkt.get("conditionId", ""),
                        question=mkt.get("question", event.get("title", "")),
                        token_id_yes=token_ids[0],
                        token_id_no=token_ids[1],
                        market_slug=mkt.get("slug", slug),
                        end_date=mkt.get("endDate", ""),
                    )
                    markets.append(market)

            # Also search directly in markets endpoint for broader coverage
            resp2 = requests.get(
                f"{GAMMA_API_URL}/markets",
                params={**params, "limit": 100},
                timeout=15,
            )
            resp2.raise_for_status()
            gamma_markets = resp2.json()

            seen_conditions = {m.condition_id for m in markets}
            for mkt in gamma_markets:
                question = mkt.get("question", "").lower()
                slug = mkt.get("slug", "").lower()

                if not any(kw in question or kw in slug for kw in ["btc", "bitcoin"]):
                    continue

                cid = mkt.get("conditionId", "")
                if cid in seen_conditions:
                    continue

                token_ids = mkt.get("clobTokenIds", [])
                if len(token_ids) < 2:
                    continue

                market = BtcMarket(
                    condition_id=cid,
                    question=mkt.get("question", ""),
                    token_id_yes=token_ids[0],
                    token_id_no=token_ids[1],
                    market_slug=mkt.get("slug", ""),
                    end_date=mkt.get("endDate", ""),
                )
                markets.append(market)

        except requests.RequestException as e:
            log.error("Failed to fetch markets from Gamma API: %s", e)

        log.info("Found %d BTC prediction markets", len(markets))
        return markets

    def get_market_prices(self, market: BtcMarket) -> BtcMarket:
        """Fetch current midpoint prices for a market's YES and NO tokens."""
        client = self._get_clob_client()

        try:
            price_yes = client.get_midpoint(market.token_id_yes)
            price_no = client.get_midpoint(market.token_id_no)
            market.current_price_yes = float(price_yes) if price_yes else None
            market.current_price_no = float(price_no) if price_no else None
            log.info(
                "Market '%s': YES=%.4f, NO=%.4f",
                market.question[:60],
                market.current_price_yes or 0,
                market.current_price_no or 0,
            )
        except Exception as e:
            log.warning("Failed to get prices for market %s: %s", market.market_slug, e)

        return market

    def place_market_order(
        self,
        token_id: str,
        side: str,
        amount: float,
    ) -> Optional[dict]:
        """
        Place a market order (FOK) on Polymarket.

        Args:
            token_id: The CLOB token ID (YES or NO token)
            side: "BUY" or "SELL"
            amount: Dollar amount to spend (for BUY) or shares to sell (for SELL)
        """
        if DRY_RUN:
            log.info(
                "[DRY RUN] Would place %s order: token=%s, amount=%.2f",
                side, token_id[:16] + "...", amount,
            )
            return {"status": "dry_run", "side": side, "amount": amount}

        if not self._authenticated:
            log.error("Cannot place order: CLOB client not authenticated")
            return None

        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        client = self._get_clob_client()
        order_side = BUY if side.upper() == "BUY" else SELL

        try:
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=order_side,
                order_type=OrderType.FOK,
            )
            signed_order = client.create_market_order(order_args)
            resp = client.post_order(signed_order, OrderType.FOK)
            log.info("Order placed: %s", resp)
            return resp
        except Exception as e:
            log.error("Failed to place order: %s", e)
            return None

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> Optional[dict]:
        """
        Place a limit order (GTC) on Polymarket.

        Args:
            token_id: The CLOB token ID
            side: "BUY" or "SELL"
            price: Limit price (0.0 - 1.0)
            size: Number of shares
        """
        if DRY_RUN:
            log.info(
                "[DRY RUN] Would place limit %s: token=%s, price=%.4f, size=%.2f",
                side, token_id[:16] + "...", price, size,
            )
            return {"status": "dry_run", "side": side, "price": price, "size": size}

        if not self._authenticated:
            log.error("Cannot place order: CLOB client not authenticated")
            return None

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        client = self._get_clob_client()
        order_side = BUY if side.upper() == "BUY" else SELL

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=order_side,
            )
            signed_order = client.create_order(order_args)
            resp = client.post_order(signed_order, OrderType.GTC)
            log.info("Limit order placed: %s", resp)
            return resp
        except Exception as e:
            log.error("Failed to place limit order: %s", e)
            return None

    def get_open_orders(self) -> list:
        """Fetch all open orders."""
        if not self._authenticated:
            return []

        from py_clob_client.clob_types import OpenOrderParams

        client = self._get_clob_client()
        try:
            return client.get_orders(OpenOrderParams())
        except Exception as e:
            log.error("Failed to fetch open orders: %s", e)
            return []

    def cancel_all_orders(self) -> bool:
        """Cancel all open orders."""
        if DRY_RUN:
            log.info("[DRY RUN] Would cancel all orders")
            return True

        if not self._authenticated:
            return False

        client = self._get_clob_client()
        try:
            client.cancel_all()
            log.info("All orders cancelled")
            return True
        except Exception as e:
            log.error("Failed to cancel orders: %s", e)
            return False

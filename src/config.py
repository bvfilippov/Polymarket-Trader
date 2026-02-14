import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Polymarket
POLYMARKET_HOST = "https://clob.polymarket.com"
POLYMARKET_CHAIN_ID = 137
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
POLYMARKET_SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))

# Gamma API for market discovery
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Binance
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"

# Trading parameters
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", "10.0"))
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.02"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Spike detection — the core of lag-arbitrage
SPIKE_THRESHOLD_PCT = float(os.getenv("SPIKE_THRESHOLD_PCT", "0.15"))  # % move to trigger
SPIKE_WINDOWS = [int(x) for x in os.getenv("SPIKE_WINDOWS", "10,30,60").split(",")]  # seconds
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "3.0"))  # vs avg

# Real-time parameters
SIGNAL_EVAL_INTERVAL = float(os.getenv("SIGNAL_EVAL_INTERVAL", "2.0"))  # seconds
MARKET_REFRESH_INTERVAL = int(os.getenv("MARKET_REFRESH_INTERVAL", "30"))  # seconds (faster!)
PRICE_REFRESH_ON_SPIKE = True  # re-fetch Polymarket prices when spike detected
ORDERBOOK_DEPTH = int(os.getenv("ORDERBOOK_DEPTH", "20"))
LARGE_TRADE_THRESHOLD = float(os.getenv("LARGE_TRADE_THRESHOLD", "50000"))  # USDT
CVD_WINDOW = int(os.getenv("CVD_WINDOW", "500"))
TRADE_COOLDOWN = int(os.getenv("TRADE_COOLDOWN", "15"))  # shorter cooldown for lag arb

# Position management / auto-exit
TAKE_PROFIT_RATIO = float(os.getenv("TAKE_PROFIT_RATIO", "0.7"))  # exit when 70% of lag closed
STOP_LOSS_BTC_REVERSAL = float(os.getenv("STOP_LOSS_BTC_REVERSAL", "0.15"))  # % BTC reversal to trigger stop
MAX_POSITION_AGE = int(os.getenv("MAX_POSITION_AGE", "300"))  # max hold time (seconds)

# Limit orders
USE_LIMIT_ORDERS = os.getenv("USE_LIMIT_ORDERS", "true").lower() == "true"
LIMIT_ORDER_OFFSET = float(os.getenv("LIMIT_ORDER_OFFSET", "0.005"))  # price offset for limit orders
LIMIT_ORDER_TIMEOUT = int(os.getenv("LIMIT_ORDER_TIMEOUT", "10"))  # cancel unfilled after N seconds

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

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
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.03"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Real-time strategy parameters
SIGNAL_EVAL_INTERVAL = float(os.getenv("SIGNAL_EVAL_INTERVAL", "5.0"))  # seconds
MARKET_REFRESH_INTERVAL = int(os.getenv("MARKET_REFRESH_INTERVAL", "120"))  # seconds
ORDERBOOK_DEPTH = int(os.getenv("ORDERBOOK_DEPTH", "20"))  # levels
LARGE_TRADE_THRESHOLD = float(os.getenv("LARGE_TRADE_THRESHOLD", "50000"))  # USDT
CVD_WINDOW = int(os.getenv("CVD_WINDOW", "300"))  # trades to keep
TRADE_COOLDOWN = int(os.getenv("TRADE_COOLDOWN", "30"))  # seconds between trades

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

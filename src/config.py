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

# Trading parameters
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", "10.0"))
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.05"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

import os
from pathlib import Path

from dotenv import load_dotenv


load_dotenv(Path(__file__).with_name(".env"))


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _get_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _get_str_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


EXECUTION_MODE = _get_str("EXECUTION_MODE", "paper")

TELEGRAM_ENABLED = _get_bool("TELEGRAM_ENABLED", True)
TELEGRAM_BOT_TOKEN = _get_str("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = _get_str("TELEGRAM_CHAT_ID", "")
TELEGRAM_CHAT_IDS = _get_str_list("TELEGRAM_CHAT_IDS")

BINGX_ENABLED = _get_bool("BINGX_ENABLED", False)
BINGX_API_KEY = _get_str("BINGX_API_KEY", "")
BINGX_SECRET_KEY = _get_str("BINGX_SECRET_KEY", "")
BINGX_BASE_URL = _get_str("BINGX_BASE_URL", "https://open-api.bingx.com")

TOP_VOLUME_SYMBOLS_COUNT = _get_int("TOP_VOLUME_SYMBOLS_COUNT", 20)
TOP_GAINERS_COUNT = _get_int("TOP_GAINERS_COUNT", 10)
TOP_LOSERS_COUNT = _get_int("TOP_LOSERS_COUNT", 10)
SCAN_INTERVAL_SECONDS = _get_int("SCAN_INTERVAL_SECONDS", 300)

START_BALANCE_USDT = _get_float("START_BALANCE_USDT", 100.0)
MAX_OPEN_POSITIONS = _get_int("MAX_OPEN_POSITIONS", 15)
FIXED_MARGIN_PCT = _get_float("FIXED_MARGIN_PCT", 0.05)
DAILY_LOSS_LIMIT_USDT = _get_float("DAILY_LOSS_LIMIT_USDT", 0.0)
MAX_CONSECUTIVE_LOSSES = _get_int("MAX_CONSECUTIVE_LOSSES", 0)
MAX_TOTAL_DRAWDOWN_PCT = _get_float("MAX_TOTAL_DRAWDOWN_PCT", 0.0)
STRONG_SIGNAL_SIZE_MULT = _get_float("STRONG_SIGNAL_SIZE_MULT", 1.0)
B_SIGNAL_SIZE_MULT = _get_float("B_SIGNAL_SIZE_MULT", 0.7)
C_SIGNAL_SIZE_MULT = _get_float("C_SIGNAL_SIZE_MULT", 0.4)
REJECT_SIGNAL_SIZE_MULT = _get_float("REJECT_SIGNAL_SIZE_MULT", 0.15)
BTC_REGIME_CONFLICT_PENALTY = _get_float("BTC_REGIME_CONFLICT_PENALTY", 0.14)
CORE_MAX_RISK_PCT = _get_float("CORE_MAX_RISK_PCT", 0.0125)
ALT_MAX_RISK_PCT = _get_float("ALT_MAX_RISK_PCT", 0.009)
LOW_CAP_MAX_RISK_PCT = _get_float("LOW_CAP_MAX_RISK_PCT", 0.005)

STOP_LOSS_PCT = _get_float("STOP_LOSS_PCT", 0.025)
TAKE_PROFIT_PCT = _get_float("TAKE_PROFIT_PCT", 0.05)

LEVERAGE_MODE = _get_str("LEVERAGE_MODE", "dynamic")
MAX_ALLOWED_LEVERAGE = _get_int("MAX_ALLOWED_LEVERAGE", 50)
FIXED_LEVERAGE = _get_int("FIXED_LEVERAGE", 20)
BINGX_DEFAULT_TYPE = _get_str("BINGX_DEFAULT_TYPE", "swap")
BINGX_MARGIN_MODE = _get_str("BINGX_MARGIN_MODE", "cross")
DRY_RUN_EXECUTION = _get_bool("DRY_RUN_EXECUTION", False)
FUNDING_RATE_ENABLED = _get_bool("FUNDING_RATE_ENABLED", True)
FUNDING_RATE_STRONG_THRESHOLD = _get_float("FUNDING_RATE_STRONG_THRESHOLD", 0.0008)
FUNDING_RATE_EXTREME_THRESHOLD = _get_float("FUNDING_RATE_EXTREME_THRESHOLD", 0.0015)

BINANCE_WS_BASE = _get_str("BINANCE_WS_BASE", "wss://fstream.binance.com")
BYBIT_REST_BASE = _get_str("BYBIT_REST_BASE", "https://api.bybit.com")
INVERT_SIGNALS = _get_bool("INVERT_SIGNALS", False)

HEARTBEAT_SECONDS = _get_int("HEARTBEAT_SECONDS", 60)
OPEN_POSITIONS_REPORT_SECONDS = _get_int("OPEN_POSITIONS_REPORT_SECONDS", 600)
MAX_POSITION_DEPOSIT_DRAWDOWN_PCT = _get_float("MAX_POSITION_DEPOSIT_DRAWDOWN_PCT", 0.20)
COOLDOWN_SECONDS = _get_int("COOLDOWN_SECONDS", 300)
MAX_SILENCE_SECONDS = _get_int("MAX_SILENCE_SECONDS", 60)

ANTI_FOMO_ENABLED = _get_bool("ANTI_FOMO_ENABLED", True)
ANTI_FOMO_LOOKBACK = _get_int("ANTI_FOMO_LOOKBACK", 5)
ANTI_FOMO_MAX_MOVE_PCT = _get_float("ANTI_FOMO_MAX_MOVE_PCT", 0.025)

STOPLOSS_COOLDOWN_SECONDS = _get_int("STOPLOSS_COOLDOWN_SECONDS", 3600)

LOW_PRICE_COIN_THRESHOLD = _get_float("LOW_PRICE_COIN_THRESHOLD", 0.10)
LOW_PRICE_REQUIRES_RETEST = _get_bool("LOW_PRICE_REQUIRES_RETEST", False)

EXTENSION_FILTER_ENABLED = _get_bool("EXTENSION_FILTER_ENABLED", True)
EXTENSION_LOOKBACK = _get_int("EXTENSION_LOOKBACK", 10)
MAX_EXTENSION_FROM_LOCAL_LOW_PCT = _get_float("MAX_EXTENSION_FROM_LOCAL_LOW_PCT", 0.12)
MAX_EXTENSION_FROM_LOCAL_HIGH_PCT = _get_float("MAX_EXTENSION_FROM_LOCAL_HIGH_PCT", 0.12)


# =========================
# BLOCK FILTER CONTROL MODE
# =========================

BLOCK_MODE = _get_str("BLOCK_MODE", "loose")   # strict | balanced | loose

ENABLE_BLOCK_SIGNAL_CLASS_REJECT = _get_bool("ENABLE_BLOCK_SIGNAL_CLASS_REJECT", True)
ENABLE_BLOCK_STRUCTURE_FILTER = _get_bool("ENABLE_BLOCK_STRUCTURE_FILTER", True)
ENABLE_BLOCK_BREAKOUT_NO_VOLUME = _get_bool("ENABLE_BLOCK_BREAKOUT_NO_VOLUME", True)
ENABLE_BLOCK_PANIC_REGIME = _get_bool("ENABLE_BLOCK_PANIC_REGIME", True)
ENABLE_BLOCK_ALT_RECLAIM_CONTEXT = _get_bool("ENABLE_BLOCK_ALT_RECLAIM_CONTEXT", True)
ENABLE_BLOCK_OI_NOT_READY = _get_bool("ENABLE_BLOCK_OI_NOT_READY", True)
ENABLE_BLOCK_HTF_CONFLICT = _get_bool("ENABLE_BLOCK_HTF_CONFLICT", True)
ENABLE_BLOCK_LOW_PRICE_RETEST = _get_bool("ENABLE_BLOCK_LOW_PRICE_RETEST", True)
ENABLE_BLOCK_EXTENSION = _get_bool("ENABLE_BLOCK_EXTENSION", True)
ENABLE_BLOCK_ANTI_FOMO = _get_bool("ENABLE_BLOCK_ANTI_FOMO", True)


# =========================
# MODE SWITCHER
# =========================

if BLOCK_MODE == "strict":
    pass

elif BLOCK_MODE == "balanced":
    ENABLE_BLOCK_SIGNAL_CLASS_REJECT = False
    ENABLE_BLOCK_STRUCTURE_FILTER = False
    ENABLE_BLOCK_BREAKOUT_NO_VOLUME = True
    ENABLE_BLOCK_PANIC_REGIME = False
    ENABLE_BLOCK_ALT_RECLAIM_CONTEXT = False
    ENABLE_BLOCK_OI_NOT_READY = False
    ENABLE_BLOCK_HTF_CONFLICT = False
    ENABLE_BLOCK_LOW_PRICE_RETEST = False
    ENABLE_BLOCK_EXTENSION = True
    ENABLE_BLOCK_ANTI_FOMO = True

elif BLOCK_MODE == "loose":
    ENABLE_BLOCK_SIGNAL_CLASS_REJECT = False
    ENABLE_BLOCK_STRUCTURE_FILTER = False
    ENABLE_BLOCK_BREAKOUT_NO_VOLUME = False
    ENABLE_BLOCK_PANIC_REGIME = False
    ENABLE_BLOCK_ALT_RECLAIM_CONTEXT = False
    ENABLE_BLOCK_OI_NOT_READY = False
    ENABLE_BLOCK_HTF_CONFLICT = False
    ENABLE_BLOCK_LOW_PRICE_RETEST = False
    ENABLE_BLOCK_EXTENSION = False
    ENABLE_BLOCK_ANTI_FOMO = False


ALLOW_REJECT_IF_HIGH_RR = True
HIGH_RR_OVERRIDE_THRESHOLD = 3.0

RSI_DIVERGENCE_ENABLED = True
MACD_DIVERGENCE_ENABLED = True
DIVERGENCE_LOOKBACK = 20
MAX_DIVERGENCE_EXTENSION_PCT = 0.15

ENABLE_BASE_BREAKOUT = True
BASE_LOOKBACK_4H = 16
BASE_VOLUME_MULTIPLIER = 1.4
BASE_MAX_MOVE_FROM_RANGE = 0.20

ENABLE_HTF_REVERSAL = True
REVERSAL_LOOKBACK_4H = 20
REVERSAL_VOLUME_MULTIPLIER = 1.3
MAX_REVERSAL_EXTENSION_PCT = 0.18

ENABLE_ORDER_BLOCK = True
ORDER_BLOCK_LOOKBACK = 30

ENABLE_BTC_REGIME_FILTER = True

ENABLE_CHART_PATTERNS = True
CHART_PATTERN_LOOKBACK_4H = 80
MAX_CHART_PATTERN_EXTENSION_PCT = 0.16

USE_ATR_TRAILING = True
TP1_ENABLED = True
TP2_ENABLED = True

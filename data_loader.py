import time
from typing import Optional

import ccxt
import pandas as pd

from config import API_KEY, SECRET_KEY, BINGX_MAX_OHLCV_LIMIT
from utils import safe_print


TIMEFRAME_TO_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def make_exchange() -> ccxt.Exchange:
    exchange = ccxt.bingx({
        "apiKey": API_KEY,
        "secret": SECRET_KEY,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
        },
    })
    exchange.load_markets()
    return exchange


def _timeframe_ms(timeframe: str) -> int:
    if timeframe not in TIMEFRAME_TO_MS:
        raise ValueError(f"Неизвестный timeframe: {timeframe}")
    return TIMEFRAME_TO_MS[timeframe]


def fetch_ohlcv_df(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    total_limit: int,
    sleep_seconds: float = 0.15,
) -> pd.DataFrame:
    if total_limit <= 0:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    tf_ms = _timeframe_ms(timeframe)
    max_per_request = min(BINGX_MAX_OHLCV_LIMIT, 1440)

    all_rows = []
    fetched = 0
    since: Optional[int] = None

    while fetched < total_limit:
        batch_limit = min(max_per_request, total_limit - fetched)

        rows = exchange.fetch_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            since=since,
            limit=batch_limit,
        )

        if not rows:
            break

        all_rows.extend(rows)
        fetched = len(all_rows)

        last_ts = rows[-1][0]
        since = last_ts + tf_ms

        if len(rows) < batch_limit:
            break

        time.sleep(sleep_seconds)

    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(
        all_rows,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )

    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    if len(df) > total_limit:
        df = df.iloc[-total_limit:].reset_index(drop=True)

    safe_print(f"Loaded {symbol} {timeframe}: {len(df)} candles")
    return df


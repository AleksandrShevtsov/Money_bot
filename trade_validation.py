import csv
from datetime import datetime, timedelta

import requests

from trade_history import CSV_HEADERS


BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"


def load_trades(path="trades.csv"):
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        return []

    has_header = rows[0] == CSV_HEADERS
    payload = rows[1:] if has_header else rows
    result = []
    for row in payload:
        if len(row) != len(CSV_HEADERS):
            continue
        result.append(dict(zip(CSV_HEADERS, row)))
    return result


def fetch_window(symbol, center_dt, minutes_before=20, minutes_after=20):
    start = int((center_dt - timedelta(minutes=minutes_before)).timestamp() * 1000)
    end = int((center_dt + timedelta(minutes=minutes_after)).timestamp() * 1000)
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": start,
        "endTime": end,
        "limit": 1000,
    }
    response = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return []
    return payload


def side_aligned(side, move_pct):
    if side == "BUY":
        return move_pct > 0
    if side == "SELL":
        return move_pct < 0
    return False


def validate_trades(path="trades.csv"):
    trades = load_trades(path)
    if not trades:
        print("No trades found.")
        return

    aligned_5m = 0
    checked = 0
    for row in trades:
        symbol = row["symbol"]
        side = row["side"]
        close_ts = datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S")

        try:
            candles = fetch_window(symbol, close_ts)
        except Exception as e:
            print(f"WARN {symbol} {row['time']} | kline_unavailable | error={e}")
            continue

        if not candles:
            print(f"WARN {symbol} {row['time']} | empty_klines")
            continue

        idx = 0
        target_ms = int(close_ts.timestamp() * 1000)
        for i, candle in enumerate(candles):
            if int(candle[0]) >= target_ms:
                idx = i
                break

        p0 = float(candles[idx][4])
        p5 = float(candles[min(idx + 5, len(candles) - 1)][4])
        move_5m = ((p5 - p0) / p0) * 100 if p0 > 0 else 0.0

        ok = side_aligned(side, move_5m)
        checked += 1
        if ok:
            aligned_5m += 1

        print(
            f"{symbol} {side} {row['time']} | move_5m={move_5m:.3f}% | "
            f"aligned={ok} | pnl={row['pnl']}"
        )

    if checked == 0:
        print("No trades were validated against market data.")
        return

    hit_rate = aligned_5m / checked * 100
    print(f"\nValidated trades: {checked}/{len(trades)} | 5m alignment: {hit_rate:.2f}%")


if __name__ == "__main__":
    validate_trades("trades.csv")

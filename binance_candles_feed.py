import requests


def fetch_klines(symbol, interval="15m", limit=200):
    url = "https://fapi.binance.com/fapi/v1/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
    except requests.RequestException:
        return []

    data = r.json()
    if not isinstance(data, list):
        return []

    candles = []

    for k in data:
        if not isinstance(k, list) or len(k) < 6:
            continue
        candles.append({
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5])
        })

    return candles

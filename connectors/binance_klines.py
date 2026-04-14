import requests
import time


class BinanceKlinesClient:
    def __init__(self, rest_base="https://fapi.binance.com", interval="5m", limit=120, refresh_seconds=60):
        self.rest_base = rest_base
        self.interval = interval
        self.limit = limit
        self.refresh_seconds = refresh_seconds
        self.session = requests.Session()
        self.cache = {}

    def _fetch_klines(self, symbol):
        url = f"{self.rest_base}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": self.interval, "limit": self.limit}
        r = self.session.get(url, params=params, timeout=10)
        r.raise_for_status()
        raw = r.json()
        candles = []
        for x in raw:
            candles.append({
                "open_time": int(x[0]),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "volume": float(x[5]),
                "close_time": int(x[6]),
            })
        return candles

    def get_klines(self, symbol):
        now = time.time()
        item = self.cache.get(symbol)
        if item and (now - item["ts"] < self.refresh_seconds):
            return item["candles"]
        try:
            candles = self._fetch_klines(symbol)
            self.cache[symbol] = {"ts": now, "candles": candles}
            return candles
        except Exception:
            return item["candles"] if item else []

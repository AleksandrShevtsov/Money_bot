import requests


class BybitOIClient:
    def __init__(self, rest_base="https://api.bybit.com"):
        self.rest_base = rest_base
        self.last_oi = {}
        self.session = requests.Session()

    def fetch_open_interest(self, symbol):
        try:
            clean_symbol = symbol.replace("/USDT", "").replace(":USDT", "")
            url = f"{self.rest_base}/v5/market/open-interest"
            params = {"category": "linear", "symbol": clean_symbol, "intervalTime": "5min"}
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            rows = data.get("result", {}).get("list", [])
            if not rows:
                return None
            return float(rows[0]["openInterest"])
        except Exception:
            return None

    def get_oi_pair(self, symbol):
        current = self.fetch_open_interest(symbol)
        previous = self.last_oi.get(symbol)
        if current is not None:
            self.last_oi[symbol] = current
        return current, previous

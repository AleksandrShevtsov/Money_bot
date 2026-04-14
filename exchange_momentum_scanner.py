import requests


class ExchangeMomentumScanner:

    def __init__(self):

        self.base_url = "https://fapi.binance.com"

        self.url = f"{self.base_url}/fapi/v1/ticker/24hr"

        self.session = requests.Session()


    def fetch_all_tickers(self):

        response = self.session.get(self.url, timeout=10)

        response.raise_for_status()

        return response.json()


    def get_top_symbols(
        self,
        top_n=20,
        min_volume=10_000_000,
        min_price=0.01,
    ):

        data = self.fetch_all_tickers()

        filtered = []

        for x in data:

            symbol = x.get("symbol", "")

            if not symbol.endswith("USDT"):
                continue

            try:

                volume = float(x.get("quoteVolume", 0))

                change = float(x.get("priceChangePercent", 0))

                last_price = float(x.get("lastPrice", 0))

            except Exception:
                continue


            if volume < min_volume:
                continue


            if last_price < min_price:
                continue


            filtered.append((symbol, change))


        top_up = sorted(filtered, key=lambda x: x[1], reverse=True)[:top_n]

        top_down = sorted(filtered, key=lambda x: x[1])[:top_n]


        symbols = []

        seen = set()


        for symbol, _ in top_up + top_down:

            if symbol not in seen:

                seen.add(symbol)

                symbols.append(symbol)


        return symbols
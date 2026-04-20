import json
import time
import threading
from collections import deque

import websocket


class BinanceMarketFeed:
    def __init__(self, symbols, ws_base="wss://fstream.binance.com"):
        self.symbols = symbols
        self.ws_base = ws_base
        self.trades = {s: deque() for s in symbols}
        self.last_price = {s: None for s in symbols}
        self.best_bid_qty = {s: None for s in symbols}
        self.best_ask_qty = {s: None for s in symbols}
        self.ws = None
        self._running = False
        self._last_msg_ts = 0.0
        self._lock = threading.Lock()

    def _build_url(self):
        streams = []
        for symbol in self.symbols:
            low = symbol.lower()
            streams.append(f"{low}@aggTrade")
            streams.append(f"{low}@bookTicker")
        return f"{self.ws_base}/stream?streams={'/'.join(streams)}"

    def start(self):
        if self._running:
            return
        self._running = True
        threading.Thread(target=self._run_forever, daemon=True).start()

    def stop(self):
        self._running = False
        try:
            if self.ws is not None:
                self.ws.close()
        except Exception:
            pass

    def _run_forever(self):
        while self._running:
            try:
                self._last_msg_ts = time.time()
                self.ws = websocket.WebSocketApp(
                    self._build_url(),
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10, reconnect=0)
            except Exception:
                time.sleep(3)

            if self._running:
                time.sleep(3)

    def on_open(self, ws):
        self._last_msg_ts = time.time()
        print("connected")

    def on_error(self, ws, error):
        pass

    def on_close(self, ws, close_status_code, close_msg):
        pass

    def _prune_old_trades(self, symbol, window_sec=45):
        cutoff = time.time() - window_sec
        dq = self.trades.get(symbol)
        if dq is None:
            return
        while dq and dq[0]["ts"] < cutoff:
            dq.popleft()

    def ensure_alive(self, max_silence_sec=60):
        if (time.time() - self._last_msg_ts) > max_silence_sec:
            try:
                if self.ws is not None:
                    self.ws.close()
            except Exception:
                pass

    def on_message(self, ws, message):
        self._last_msg_ts = time.time()
        packet = json.loads(message)
        data = packet.get("data", {})
        stream = packet.get("stream", "")
        symbol = data.get("s")
        if symbol not in self.trades:
            return

        if stream.endswith("@aggTrade"):
            price = float(data["p"])
            qty = float(data["q"])
            usd_size = price * qty
            side = "sell" if data["m"] else "buy"
            trade = {
                "price": price,
                "usd_size": usd_size,
                "side": side,
                "is_large": usd_size >= 30000,
                "ts": time.time(),
            }
            with self._lock:
                self.last_price[symbol] = price
                self.trades[symbol].append(trade)
                self._prune_old_trades(symbol)

        elif stream.endswith("@bookTicker"):
            try:
                with self._lock:
                    self.best_bid_qty[symbol] = float(data["B"])
                    self.best_ask_qty[symbol] = float(data["A"])
                    if self.last_price[symbol] is None:
                        bid = float(data["b"])
                        ask = float(data["a"])
                        self.last_price[symbol] = (bid + ask) / 2.0
            except Exception:
                pass

    def get_recent_trades(self, symbol):
        with self._lock:
            self._prune_old_trades(symbol)
            return list(self.trades.get(symbol, ()))

    def get_last_price(self, symbol):
        with self._lock:
            return self.last_price.get(symbol)

    def get_orderbook_imbalance(self, symbol):
        with self._lock:
            bid_qty = self.best_bid_qty.get(symbol)
            ask_qty = self.best_ask_qty.get(symbol)
        if bid_qty is None or ask_qty is None:
            return 0.0
        denom = bid_qty + ask_qty
        if denom <= 0:
            return 0.0
        return (bid_qty - ask_qty) / denom

    def snapshot(self, symbol):
        if symbol not in self.trades:
            return [], None, 0.0
        return (
            self.get_recent_trades(symbol),
            self.get_last_price(symbol),
            self.get_orderbook_imbalance(symbol),
        )

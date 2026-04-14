import time


class FeedHealthMonitor:
    def __init__(self, max_feed_silence_seconds: int = 60, min_trades_for_ready: int = 3, oi_ttl_seconds: int = 900):
        self.max_feed_silence_seconds = max_feed_silence_seconds
        self.min_trades_for_ready = min_trades_for_ready
        self.oi_ttl_seconds = oi_ttl_seconds
        self.last_oi_ts = {}

    def note_oi(self, symbol: str, oi_value):
        if oi_value is not None:
            self.last_oi_ts[symbol] = time.time()

    def feed_ready(self, market_feed) -> bool:
        if market_feed is None:
            return False
        return (time.time() - market_feed._last_msg_ts) <= self.max_feed_silence_seconds

    def symbol_ready(self, market_feed, symbol: str) -> bool:
        if market_feed is None:
            return False
        if not self.feed_ready(market_feed):
            return False
        trades = market_feed.get_recent_trades(symbol)
        return len(trades) >= self.min_trades_for_ready

    def oi_ready(self, symbol: str) -> bool:
        last_ts = self.last_oi_ts.get(symbol)
        if last_ts is None:
            return False
        return (time.time() - last_ts) <= self.oi_ttl_seconds

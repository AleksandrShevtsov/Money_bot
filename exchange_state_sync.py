class ExchangeStateSync:
    def __init__(self, executor, enabled: bool):
        self.executor = executor
        self.enabled = enabled

    def fetch_remote_positions(self) -> list[dict]:
        if not self.enabled:
            return []

        try:
            return self.executor.fetch_open_positions()
        except Exception:
            return []

    def map_by_symbol(self) -> dict:
        result = {}
        for pos in self.fetch_remote_positions():
            symbol = pos.get("symbol")
            if symbol:
                result[symbol] = pos
        return result

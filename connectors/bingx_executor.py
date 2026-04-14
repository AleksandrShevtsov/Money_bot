from __future__ import annotations

import ccxt

from config import BINGX_API_KEY, BINGX_SECRET_KEY, BINGX_DEFAULT_TYPE, BINGX_MARGIN_MODE, DRY_RUN_EXECUTION


class BingXExecutor:
    def __init__(self):
        self.enabled = bool(BINGX_API_KEY and BINGX_SECRET_KEY and not DRY_RUN_EXECUTION)
        self.exchange = ccxt.bingx({
            'apiKey': BINGX_API_KEY,
            'secret': BINGX_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': BINGX_DEFAULT_TYPE},
        })
        self.loaded = False

    def _ensure(self):
        if not self.loaded:
            self.exchange.load_markets()
            self.loaded = True

    def set_leverage(self, symbol: str, leverage: int):
        if not self.enabled:
            return None
        self._ensure()
        return self.exchange.set_leverage(leverage, symbol, {'marginMode': BINGX_MARGIN_MODE})

    def market_order(self, symbol: str, side: str, amount: float):
        if not self.enabled:
            return {'id': 'paper_or_dry_run'}
        self._ensure()
        return self.exchange.create_order(symbol, 'market', side.lower(), amount, None, {'marginMode': BINGX_MARGIN_MODE})

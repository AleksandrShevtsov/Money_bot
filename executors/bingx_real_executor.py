import hashlib
import hmac
import time
from urllib.parse import urlencode

import requests


class BingXRealExecutor:
    def __init__(
        self,
        api_key: str,
        secret_key: str,
        enabled: bool = False,
        base_url: str = "https://open-api.bingx.com",
    ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.enabled = enabled
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-BX-APIKEY": self.api_key})

    def has_credentials(self):
        return bool(self.api_key and self.secret_key)

    def _sign_params(self, params: dict) -> dict:
        params = dict(params)
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000

        query = urlencode(sorted(params.items()))
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        params["signature"] = signature
        return params

    def _post(self, path: str, params: dict):
        url = f"{self.base_url}{path}"
        signed = self._sign_params(params)
        response = self.session.post(url, params=signed, timeout=15)
        response.raise_for_status()
        return response.json()

    def _get(self, path: str, params: dict):
        url = f"{self.base_url}{path}"
        signed = self._sign_params(params)
        response = self.session.get(url, params=signed, timeout=15)
        response.raise_for_status()
        return response.json()

    def set_leverage(self, symbol: str, side: str, leverage: int):
        if not self.enabled:
            return {"mode": "paper", "action": "set_leverage", "symbol": symbol, "side": side, "leverage": leverage}

        return self._post(
            "/openApi/swap/v2/trade/leverage",
            {
                "symbol": symbol,
                "side": side,
                "leverage": leverage,
            },
        )

    def place_market_order(self, symbol: str, side: str, quantity: float, position_side: str = None):
        if not self.enabled:
            return {
                "mode": "paper",
                "action": "place_market_order",
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "position_side": position_side,
            }

        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
        }

        if position_side:
            params["positionSide"] = position_side

        return self._post("/openApi/swap/v2/trade/order", params)

    def reduce_position(self, symbol: str, side: str, quantity: float, position_side: str = None):
        if not self.enabled:
            return {
                "mode": "paper",
                "action": "reduce_position",
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "position_side": position_side,
            }

        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
            "reduceOnly": True,
        }

        if position_side:
            params["positionSide"] = position_side

        return self._post("/openApi/swap/v2/trade/order", params)

    def close_all_positions(self, symbol: str = None):
        if not self.enabled:
            return {"mode": "paper", "action": "close_all_positions", "symbol": symbol}

        params = {}
        if symbol:
            params["symbol"] = symbol

        return self._post("/openApi/swap/v2/trade/closeAllPositions", params)

    def fetch_open_positions(self):
        if not self.enabled:
            return []

        payload = self._get("/openApi/swap/v2/user/positions", {})
        rows = payload.get("data") or payload.get("result") or []
        positions = []

        for row in rows:
            try:
                amount = abs(float(row.get("positionAmt", row.get("availableAmt", 0)) or 0))
            except Exception:
                amount = 0.0

            if amount <= 0:
                continue

            side = row.get("positionSide") or row.get("side") or ""
            entry_price = float(row.get("avgPrice", row.get("entryPrice", 0)) or 0)
            positions.append(
                {
                    "symbol": row.get("symbol"),
                    "position_side": side,
                    "qty": amount,
                    "entry_price": entry_price,
                    "raw": row,
                }
            )

        return positions

    def test_connection(self):
        if not self.has_credentials():
            return {"enabled": self.enabled, "ok": False, "reason": "missing_bingx_credentials"}

        try:
            payload = self._get("/openApi/swap/v2/user/positions", {})
            return {
                "enabled": self.enabled,
                "ok": True,
                "reason": "ok",
                "raw_keys": list(payload.keys()) if isinstance(payload, dict) else [],
            }
        except Exception as e:
            return {"enabled": self.enabled, "ok": False, "reason": str(e)}

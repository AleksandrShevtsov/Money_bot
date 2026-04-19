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

    def _normalize_symbol(self, symbol: str) -> str:
        clean = str(symbol or "").strip().upper()
        if "-" in clean:
            return clean
        if clean.endswith("USDT"):
            return f"{clean[:-4]}-USDT"
        return clean

    def _normalize_quantity(self, quantity: float) -> str:
        value = max(0.0, float(quantity or 0.0))
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text or "0"

    def _ensure_success(self, payload, action: str):
        if not isinstance(payload, dict):
            raise RuntimeError(f"{action} failed: invalid_response")

        code = payload.get("code")
        if code not in (0, "0", None):
            message = payload.get("msg") or payload.get("message") or "api_rejected"
            raise RuntimeError(f"{action} failed: code={code} msg={message}")

        return payload

    def _sign_params(self, params: dict) -> dict:
        params = dict(params)
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000

        query = urlencode(params)
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

    def _extract_balance_from_payload(self, payload):
        if not isinstance(payload, dict):
            return None

        candidates = []
        for root_key in ("data", "result"):
            root = payload.get(root_key)
            if isinstance(root, dict):
                candidates.append(root)
                nested_balance = root.get("balance")
                if isinstance(nested_balance, dict):
                    candidates.append(nested_balance)
                elif isinstance(nested_balance, list):
                    candidates.extend(item for item in nested_balance if isinstance(item, dict))
            elif isinstance(root, list):
                candidates.extend(item for item in root if isinstance(item, dict))

        preferred_assets = {"USDT", "USDⓢ", "USDT-FUTURES", "USDT_PERP"}
        asset_keys = ("asset", "currency", "coin")
        balance_keys = (
            "availableMargin",
            "availableBalance",
            "availableAmt",
            "balance",
            "equity",
            "walletBalance",
        )

        preferred_rows = []
        generic_rows = []

        for row in candidates:
            asset_name = ""
            for key in asset_keys:
                value = row.get(key)
                if value is not None:
                    asset_name = str(value).upper()
                    break
            if asset_name in preferred_assets:
                preferred_rows.append(row)
            else:
                generic_rows.append(row)

        for row in preferred_rows + generic_rows:
            for key in balance_keys:
                value = row.get(key)
                if value in (None, ""):
                    continue
                try:
                    return float(value)
                except Exception:
                    continue

        return None

    def set_leverage(self, symbol: str, side: str, leverage: int):
        if not self.enabled:
            return {"mode": "paper", "action": "set_leverage", "symbol": symbol, "side": side, "leverage": leverage}

        payload = self._post(
            "/openApi/swap/v2/trade/leverage",
            {
                "symbol": self._normalize_symbol(symbol),
                "side": side,
                "leverage": leverage,
            },
        )
        return self._ensure_success(payload, "set_leverage")

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
            "symbol": self._normalize_symbol(symbol),
            "side": side,
            "type": "MARKET",
            "quantity": self._normalize_quantity(quantity),
        }

        if position_side:
            params["positionSide"] = position_side

        payload = self._post("/openApi/swap/v2/trade/order", params)
        return self._ensure_success(payload, "place_market_order")

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
            "symbol": self._normalize_symbol(symbol),
            "side": side,
            "type": "MARKET",
            "quantity": self._normalize_quantity(quantity),
            "reduceOnly": True,
        }

        if position_side:
            params["positionSide"] = position_side

        payload = self._post("/openApi/swap/v2/trade/order", params)
        return self._ensure_success(payload, "reduce_position")

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

    def fetch_account_balance(self):
        if not self.enabled:
            return {"mode": "paper", "balance": None}

        last_error = None
        for path in (
            "/openApi/swap/v2/user/balance",
            "/openApi/swap/v2/user/account",
            "/openApi/swap/v2/user/positions",
        ):
            try:
                payload = self._get(path, {})
                balance = self._extract_balance_from_payload(payload)
                if balance is not None:
                    return {
                        "ok": True,
                        "balance": balance,
                        "path": path,
                        "payload": payload,
                    }
            except Exception as e:
                last_error = str(e)

        return {"ok": False, "balance": None, "reason": last_error or "balance_unavailable"}

    def test_connection(self):
        if not self.has_credentials():
            return {"enabled": self.enabled, "ok": False, "reason": "missing_bingx_credentials"}

        try:
            payload = self._get("/openApi/swap/v2/user/positions", {})
            code = payload.get("code") if isinstance(payload, dict) else None
            return {
                "enabled": self.enabled,
                "ok": code == 0,
                "reason": "ok" if code == 0 else payload.get("msg", "api_rejected"),
                "raw_keys": list(payload.keys()) if isinstance(payload, dict) else [],
            }
        except Exception as e:
            return {"enabled": self.enabled, "ok": False, "reason": str(e)}

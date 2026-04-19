import hashlib
import hmac
import math
import time
from urllib.parse import urlencode

import ccxt
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
        self._market_client = None
        self._trade_client = None

    def has_credentials(self):
        return bool(self.api_key and self.secret_key)

    def _normalize_symbol(self, symbol: str) -> str:
        clean = str(symbol or "").strip().upper()
        if "-" in clean:
            return clean
        if clean.endswith("USDT"):
            return f"{clean[:-4]}-USDT"
        return clean

    def _project_symbol(self, symbol: str) -> str:
        clean = str(symbol or "").strip().upper()
        if "/" in clean:
            clean = clean.split("/")[0] + "USDT"
        clean = clean.replace(":USDT", "").replace("-USDT", "USDT")
        return clean

    def _normalize_quantity(self, quantity: float) -> str:
        value = max(0.0, float(quantity or 0.0))
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text or "0"

    def _ccxt(self):
        if self._market_client is None:
            self._market_client = ccxt.bingx({
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            })
            self._market_client.load_markets()
        return self._market_client

    def _private_ccxt(self):
        if self._trade_client is None:
            self._trade_client = ccxt.bingx({
                "apiKey": self.api_key,
                "secret": self.secret_key,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            })
            self._trade_client.load_markets()
        return self._trade_client

    def _contract_market(self, symbol: str):
        market_id = self._normalize_symbol(symbol)
        exchange = self._ccxt()
        rows = exchange.markets_by_id.get(market_id, []) or []
        for row in rows:
            if row.get("contract"):
                return row
        return None

    def supports_contract(self, symbol: str) -> bool:
        return self._contract_market(symbol) is not None

    def _contract_ccxt_symbol(self, symbol: str) -> str:
        market = self._contract_market(symbol)
        if market is not None:
            return market["symbol"]
        normalized = self._normalize_symbol(symbol)
        if normalized.endswith("-USDT"):
            return normalized.replace("-USDT", "/USDT:USDT")
        return normalized

    def precheck_market_order(self, symbol: str, quantity: float, price: float, available_balance: float | None = None):
        requested_qty = max(0.0, float(quantity or 0.0))
        entry_price = max(0.0, float(price or 0.0))

        if requested_qty <= 0:
            return {"ok": False, "reason": "qty_non_positive"}
        if entry_price <= 0:
            return {"ok": False, "reason": "price_non_positive"}
        if available_balance is not None and float(available_balance) <= 0:
            return {"ok": False, "reason": "available_balance_zero"}

        market = self._contract_market(symbol)
        if market is None:
            return {
                "ok": False,
                "symbol": self._normalize_symbol(symbol),
                "reason": "market_rules_unavailable",
            }

        exchange = self._ccxt()
        try:
            normalized_qty = float(exchange.amount_to_precision(market["symbol"], requested_qty))
        except Exception:
            normalized_qty = requested_qty

        min_qty = float((((market.get("limits") or {}).get("amount") or {}).get("min")) or 0.0)
        min_notional = float((((market.get("limits") or {}).get("cost") or {}).get("min")) or 0.0)
        notional = normalized_qty * entry_price
        step_size = float((market.get("precision") or {}).get("amount") or 0.0)

        required_qty = requested_qty
        if min_qty > 0:
            required_qty = max(required_qty, min_qty)
        if min_notional > 0 and entry_price > 0:
            required_qty = max(required_qty, min_notional / entry_price)
        if step_size > 0:
            required_qty = math.ceil(required_qty / step_size) * step_size

        try:
            required_qty = float(exchange.amount_to_precision(market["symbol"], required_qty))
        except Exception:
            pass

        if normalized_qty <= 0:
            return {"ok": False, "reason": "qty_rounds_to_zero", "market_symbol": market["symbol"]}
        if min_qty > 0 and normalized_qty < min_qty:
            return {
                "ok": False,
                "reason": "qty_below_min",
                "market_symbol": market["symbol"],
                "min_qty": min_qty,
                "quantity": normalized_qty,
                "suggested_quantity": required_qty,
            }
        if min_notional > 0 and notional < min_notional:
            return {
                "ok": False,
                "reason": "notional_below_min",
                "market_symbol": market["symbol"],
                "min_notional": min_notional,
                "notional": notional,
                "quantity": normalized_qty,
                "suggested_quantity": required_qty,
            }

        return {
            "ok": True,
            "symbol": self._normalize_symbol(symbol),
            "market_symbol": market["symbol"],
            "quantity": normalized_qty,
            "notional": notional,
            "min_qty": min_qty,
            "min_notional": min_notional,
        }

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

        ccxt_symbol = self._contract_ccxt_symbol(symbol)
        try:
            payload = self._private_ccxt().set_leverage(
                leverage,
                ccxt_symbol,
                {"marginMode": "cross"},
            )
            return {"code": 0, "msg": "", "data": payload}
        except Exception as e:
            raise RuntimeError(f"set_leverage failed via ccxt: {e}")

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

        ccxt_symbol = self._contract_ccxt_symbol(symbol)
        ccxt_amount = float(params["quantity"])
        try:
            payload = self._private_ccxt().create_order(
                ccxt_symbol,
                "market",
                side.lower(),
                ccxt_amount,
                None,
                {"positionSide": position_side} if position_side else {},
            )
            return {"code": 0, "msg": "", "data": payload}
        except Exception:
            payload = self._post("/openApi/swap/v2/trade/order", params)
            return self._ensure_success(payload, "place_market_order")

    def place_protective_orders(
        self,
        symbol: str,
        close_side: str,
        quantity: float,
        stop_loss_price: float | None = None,
        take_profit_price: float | None = None,
        position_side: str | None = None,
    ):
        if not self.enabled:
            return {
                "mode": "paper",
                "action": "place_protective_orders",
                "symbol": symbol,
                "close_side": close_side,
                "quantity": quantity,
                "stop_loss_price": stop_loss_price,
                "take_profit_price": take_profit_price,
            }

        ccxt_symbol = self._contract_ccxt_symbol(symbol)
        ccxt_amount = float(self._normalize_quantity(quantity))
        results = {}

        if stop_loss_price is not None:
            sl_params = {
                "stopLossPrice": float(stop_loss_price),
                "reduceOnly": True,
            }
            if position_side:
                sl_params["positionSide"] = position_side
            sl_order = self._private_ccxt().create_order(
                ccxt_symbol,
                "market",
                close_side.lower(),
                ccxt_amount,
                None,
                sl_params,
            )
            results["stop_loss"] = sl_order

        if take_profit_price is not None:
            tp_params = {
                "takeProfitPrice": float(take_profit_price),
                "reduceOnly": True,
            }
            if position_side:
                tp_params["positionSide"] = position_side
            tp_order = self._private_ccxt().create_order(
                ccxt_symbol,
                "market",
                close_side.lower(),
                ccxt_amount,
                None,
                tp_params,
            )
            results["take_profit"] = tp_order

        return {"code": 0, "msg": "", "data": results}

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

        ccxt_symbol = self._contract_ccxt_symbol(symbol)
        ccxt_amount = float(params["quantity"])
        ccxt_params = {"reduceOnly": True}
        if position_side:
            ccxt_params["positionSide"] = position_side
        try:
            payload = self._private_ccxt().create_order(
                ccxt_symbol,
                "market",
                side.lower(),
                ccxt_amount,
                None,
                ccxt_params,
            )
            return {"code": 0, "msg": "", "data": payload}
        except Exception:
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

        try:
            rows = self._private_ccxt().fetch_positions()
            positions = []
            for row in rows:
                contracts = abs(float(row.get("contracts") or 0.0))
                if contracts <= 0:
                    continue
                side = str(row.get("side") or "").upper()
                positions.append(
                    {
                        "symbol": self._project_symbol((row.get("info") or {}).get("symbol") or row.get("id") or row.get("symbol")),
                        "position_side": "LONG" if side == "LONG" else "SHORT",
                        "qty": contracts,
                        "entry_price": float(row.get("entryPrice") or row.get("entryPrice", 0.0) or 0.0),
                        "raw": row,
                    }
                )
            return positions
        except Exception:
            pass

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
                    "symbol": self._project_symbol(row.get("symbol")),
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

        try:
            balance = self._private_ccxt().fetch_balance()
            usdt = balance.get("USDT") or {}
            free_balance = usdt.get("free")
            if free_balance is not None:
                return {
                    "ok": True,
                    "balance": float(free_balance),
                    "path": "ccxt.fetch_balance",
                    "payload": balance.get("info"),
                }
        except Exception as e:
            last_error = str(e)
        else:
            last_error = None

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
            balance = self._private_ccxt().fetch_balance()
            usdt = balance.get("USDT") or {}
            if usdt.get("total") is not None:
                return {
                    "enabled": self.enabled,
                    "ok": True,
                    "reason": "ok",
                    "raw_keys": list((balance.get("info") or {}).keys()) if isinstance(balance.get("info"), dict) else [],
                }
        except Exception:
            pass

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

import time

from market_structure import detect_market_structure, structure_allows_side
from breakout_volume_filter import breakout_volume_confirms
from signal_quality import classify_signal_quality

from entry_filters import blocked_by_anti_fomo
from late_entry_filters import is_low_price_coin, blocked_by_extension
from telegram_notifier import TelegramNotifier
from executors.bingx_real_executor import BingXRealExecutor
from connectors.binance_stream import BinanceMarketFeed
from connectors.bybit_client import BybitOIClient
from main_block_control import apply_block_filters
from config import (
    EXECUTION_MODE,
    TELEGRAM_ENABLED,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_CHAT_IDS,
    BINGX_ENABLED,
    BINGX_API_KEY,
    BINGX_SECRET_KEY,
    BINGX_BASE_URL,
    ANTI_FOMO_ENABLED,
    ANTI_FOMO_LOOKBACK,
    ANTI_FOMO_MAX_MOVE_PCT,
    LOW_PRICE_COIN_THRESHOLD,
    EXTENSION_FILTER_ENABLED,
    EXTENSION_LOOKBACK,
    MAX_EXTENSION_FROM_LOCAL_LOW_PCT,
    MAX_EXTENSION_FROM_LOCAL_HIGH_PCT,
    TOP_VOLUME_SYMBOLS_COUNT,
    TOP_GAINERS_COUNT,
    TOP_LOSERS_COUNT,
    SCAN_INTERVAL_SECONDS,
    START_BALANCE_USDT,
    FIXED_MARGIN_PCT,
    MAX_OPEN_POSITIONS,
    DAILY_LOSS_LIMIT_USDT,
    MAX_CONSECUTIVE_LOSSES,
    MAX_TOTAL_DRAWDOWN_PCT,
    STOP_LOSS_PCT,
    TAKE_PROFIT_PCT,
    INVERT_SIGNALS,
    HEARTBEAT_SECONDS,
    OPEN_POSITIONS_REPORT_SECONDS,
    MAX_POSITION_DEPOSIT_DRAWDOWN_PCT,
    COOLDOWN_SECONDS,
    STOPLOSS_COOLDOWN_SECONDS,
    LOW_PRICE_REQUIRES_RETEST,
    BINANCE_WS_BASE,
    BYBIT_REST_BASE,
    MAX_SILENCE_SECONDS,
    ALLOW_REJECT_IF_HIGH_RR,
    HIGH_RR_OVERRIDE_THRESHOLD,
    RSI_DIVERGENCE_ENABLED,
    MACD_DIVERGENCE_ENABLED,
    ENABLE_BASE_BREAKOUT,
    BASE_LOOKBACK_4H,
    BASE_VOLUME_MULTIPLIER,
    BASE_MAX_MOVE_FROM_RANGE,
    ENABLE_HTF_REVERSAL,
    REVERSAL_LOOKBACK_4H,
    REVERSAL_VOLUME_MULTIPLIER,
    MAX_REVERSAL_EXTENSION_PCT,
    ENABLE_ORDER_BLOCK,
    ORDER_BLOCK_LOOKBACK,
    ENABLE_BTC_REGIME_FILTER,
    ENABLE_CHART_PATTERNS,
    CHART_PATTERN_LOOKBACK_4H,
    MAX_CHART_PATTERN_EXTENSION_PCT,
    BTC_REGIME_CONFLICT_PENALTY,
    FUNDING_RATE_ENABLED,
    FUNDING_RATE_STRONG_THRESHOLD,
    FUNDING_RATE_EXTREME_THRESHOLD,
)
from utils import log, log_green, log_red, log_yellow, log_cyan
from exchange_momentum_scanner import ExchangeMomentumScanner
from binance_candles_feed import fetch_klines
from htf_trend_filter import detect_htf_trend
from breakout_detector import detect_range_breakout, confirm_breakout_with_orderflow
from trendline_detector import detect_trendline_breakout, confirm_trendline_breakout
from retest_detector import detect_retest_after_breakout
from fast_move_detector import detect_fast_move
from acceleration_detector import detect_price_acceleration
from strategy import build_signal
from position_manager import PositionManager
from smart_exit_manager import SmartExitManager
from trade_history import ensure_history_files, append_trade
from bot_state_store import BotStateStore
from feed_health import FeedHealthMonitor
from risk_guard import RiskGuard
from exchange_state_sync import ExchangeStateSync
from volatility_regime import market_regime, adaptive_threshold
from oi_context import classify_oi_price_context
from liquidity_levels import build_volume_profile, nearest_level, detect_liquidity_sweep, is_false_breakout
from time_filters import trading_window_allows_entry
from confirmation_filters import multi_bar_breakout_confirmation
from divergence_detector import (
    detect_double_divergence,
    detect_macd_divergence,
    detect_rsi_divergence,
    divergence_not_overextended,
)
from base_breakout_detector import detect_base_breakout, not_overextended_from_base, confirm_base_breakout_entry_15m
from reversal_detector import (
    confirm_reversal_entry_15m,
    detect_htf_reversal,
    reversal_not_overextended,
)
from btc_regime_filter import detect_btc_regime
from order_block_detector import detect_order_block, confirm_order_block_retest
from chart_pattern_detector import (
    detect_best_chart_pattern,
    confirm_chart_pattern_entry_15m,
    chart_pattern_not_overextended,
)
from funding_context import classify_funding_context


class SmartMomentumPaperBot:
    def __init__(self):
        ensure_history_files()

        self.scanner = ExchangeMomentumScanner()
        self.balance = START_BALANCE_USDT
        self.symbols = []
        self.positions = {}
        self.cooldown_until = {}
        self.last_signal = {}
        self.last_heartbeat = 0.0
        self.last_open_positions_report = 0.0
        self.market_feed = None
        self.oi_client = BybitOIClient(rest_base=BYBIT_REST_BASE)
        self.state_store = BotStateStore()
        self.feed_health = FeedHealthMonitor(max_feed_silence_seconds=MAX_SILENCE_SECONDS)
        self.risk_guard = RiskGuard(
            daily_loss_limit_usdt=DAILY_LOSS_LIMIT_USDT,
            max_consecutive_losses=MAX_CONSECUTIVE_LOSSES,
            max_total_drawdown_pct=MAX_TOTAL_DRAWDOWN_PCT,
        )

        self.position_manager = PositionManager(entry_pct=FIXED_MARGIN_PCT)
        self.exit_manager = SmartExitManager()

        self.notifier = TelegramNotifier(
            bot_token=TELEGRAM_BOT_TOKEN,
            chat_id=TELEGRAM_CHAT_ID,
            chat_ids=TELEGRAM_CHAT_IDS,
            enabled=TELEGRAM_ENABLED,
        )

        self.executor = BingXRealExecutor(
            api_key=BINGX_API_KEY,
            secret_key=BINGX_SECRET_KEY,
            enabled=(EXECUTION_MODE == "real" and BINGX_ENABLED),
            base_url=BINGX_BASE_URL,
        )
        self.exchange_sync = ExchangeStateSync(
            executor=self.executor,
            enabled=(EXECUTION_MODE == "real" and BINGX_ENABLED),
        )

        self.restore_runtime_state()
        self.sync_with_exchange_state()
        self.check_integrations()

    def invert_side_if_needed(self, side: str) -> str:
        if not INVERT_SIGNALS:
            return side
        if side == "BUY":
            return "SELL"
        if side == "SELL":
            return "BUY"
        return side

    def configure_market_feed(self):
        desired = list(self.symbols)
        if not desired:
            return

        current = self.market_feed.symbols if self.market_feed is not None else []
        if current == desired:
            return

        if self.market_feed is not None:
            self.market_feed.stop()

        self.market_feed = BinanceMarketFeed(desired, ws_base=BINANCE_WS_BASE)
        self.market_feed.start()
        log_cyan(f"ORDERFLOW FEED started for {len(desired)} symbols")

    def _exchange_position_side(self, side: str) -> str:
        return "LONG" if side == "BUY" else "SHORT"

    def _exchange_close_side(self, side: str) -> str:
        return "SELL" if side == "BUY" else "BUY"

    def _symbol_profile(self, symbol: str, current_price: float | None = None) -> str:
        if symbol in {"BTCUSDT", "ETHUSDT"}:
            return "CORE"
        if current_price is not None and current_price < LOW_PRICE_COIN_THRESHOLD:
            return "LOW_CAP"
        return "ALT"

    def _safe_detect(self, label, fn, *args, default=None, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log_yellow(f"DETECT FAIL {label} | error={e}")
            return default

    def _fmt_money(self, value):
        try:
            return f"{float(value):,.2f} USDT"
        except Exception:
            return "n/a"

    def _fmt_price(self, value):
        try:
            price = float(value)
        except Exception:
            return "n/a"

        if price >= 1000:
            return f"{price:,.2f}"
        if price >= 1:
            return f"{price:,.4f}"
        return f"{price:,.6f}"

    def _fetch_real_balance(self):
        if EXECUTION_MODE != "real" or not BINGX_ENABLED:
            return {"ok": False, "balance": None, "reason": "real_mode_disabled"}
        if not self.executor.has_credentials():
            return {"ok": False, "balance": None, "reason": "missing_bingx_credentials"}
        return self.executor.fetch_account_balance()

    def _startup_status_message(self, real_balance_info=None):
        mode_label = "REAL" if EXECUTION_MODE == "real" and BINGX_ENABLED else "PAPER"
        lines = [
            "Bot started",
            f"Mode: {mode_label}",
            f"Runtime balance: {self._fmt_money(self.balance)}",
            f"Max open positions: {MAX_OPEN_POSITIONS}",
            f"Entry allocation: {FIXED_MARGIN_PCT * 100:.1f}%",
        ]

        if real_balance_info and real_balance_info.get("ok"):
            lines.append(f"BingX balance: {self._fmt_money(real_balance_info.get('balance'))}")
        elif EXECUTION_MODE == "real" and BINGX_ENABLED:
            lines.append(
                f"BingX balance: unavailable ({real_balance_info.get('reason', 'unknown') if real_balance_info else 'unknown'})"
            )

        return "\n".join(lines)

    def _is_strong_signal_class(self, signal_class: str) -> bool:
        return signal_class in {"A", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"}

    def _normalize_signal_class(
        self,
        signal_class,
        score,
        strong_reversal_context=False,
        retest_confirmation=None,
        breakout_confirmation=None,
        trendline_confirmation=None,
        base_entry=None,
        reversal_entry=None,
        order_block_entry=None,
        chart_pattern_entry=None,
        regime_name="range_day",
        symbol_profile="ALT",
    ):
        normalized = signal_class
        reasons = []
        confirmed_entry = any(
            item is not None for item in [
                retest_confirmation,
                breakout_confirmation,
                trendline_confirmation,
                base_entry,
                reversal_entry,
                order_block_entry,
                chart_pattern_entry,
            ]
        )

        if normalized == "REJECT" and score >= 0.72 and (strong_reversal_context or confirmed_entry):
            normalized = "B"
            reasons.append("score_class_sync_reject_up")
        elif normalized == "C" and score >= 0.68 and (strong_reversal_context or confirmed_entry):
            normalized = "B"
            reasons.append("score_class_sync_up")
        elif normalized == "B" and score < 0.32 and not strong_reversal_context and retest_confirmation is None:
            normalized = "C"
            reasons.append("score_class_sync_down")

        if symbol_profile == "LOW_CAP" and regime_name == "high_volatility_panic" and normalized == "B" and not strong_reversal_context:
            normalized = "C"
            reasons.append("low_cap_panic_downgrade")

        return normalized, reasons

    def _hydrate_runtime_position(self, symbol, pos):
        if not isinstance(pos, dict):
            return None

        entry = float(pos.get("entry", 0.0) or 0.0)
        qty = float(pos.get("qty", 0.0) or 0.0)
        side = pos.get("side", "BUY")
        take = float(pos.get("take", entry) or entry)
        stop = float(pos.get("stop", entry) or entry)
        level_data = pos.get("level_data") or {}

        pos["entry"] = entry
        pos["qty"] = qty
        pos["side"] = side
        pos["take"] = take
        pos["stop"] = stop
        pos["tp1"] = float(pos.get("tp1", level_data.get("tp1", take)) or take)
        pos["tp2"] = float(pos.get("tp2", level_data.get("tp2", take)) or take)
        pos["liquidity_target"] = pos.get("liquidity_target", level_data.get("liquidity_target"))
        pos["margin"] = float(pos.get("margin", 0.0) or 0.0)
        pos["notional"] = float(pos.get("notional", qty * entry) or (qty * entry))
        pos["risk_usdt"] = float(pos.get("risk_usdt", abs(entry - stop) * qty) or 0.0)
        pos["leverage"] = int(pos.get("leverage", 1) or 1)
        pos["atr_pct"] = float(pos.get("atr_pct", 0.0) or 0.0)
        pos["opened_at"] = float(pos.get("opened_at", time.time()) or time.time())
        pos["signal_score"] = float(pos.get("signal_score", 0.0) or 0.0)
        pos["signal_class"] = str(pos.get("signal_class", "REJECT") or "REJECT")
        pos["symbol_profile"] = pos.get("symbol_profile") or self._symbol_profile(symbol, current_price=entry)
        pos["account_balance_at_open"] = float(pos.get("account_balance_at_open", self.balance) or self.balance)
        pos["btc_regime"] = pos.get("btc_regime", "unknown")
        pos["btc_bias"] = pos.get("btc_bias", "NEUTRAL")
        pos["funding_label"] = pos.get("funding_label", "unknown")
        pos["funding_rate"] = float(pos.get("funding_rate", 0.0) or 0.0)
        pos["htf_context"] = pos.get("htf_context", "")
        pos["entry_context"] = pos.get("entry_context", "")
        pos["be_moved"] = bool(pos.get("be_moved", False))
        pos["partial_done"] = bool(pos.get("partial_done", False))
        pos["trail_active"] = bool(pos.get("trail_active", False))

        if pos["signal_class"] == "C" and pos["signal_score"] >= 0.78:
            pos["signal_class"] = "B"
            pos["reason"] = f"{pos.get('reason', '')}|runtime_class_sync_up"

        if pos["symbol_profile"] == "LOW_CAP" and pos["signal_class"] in {"B", "C", "REJECT"}:
            pos["leverage"] = min(pos["leverage"], 8 if pos["signal_class"] == "B" else 5)
        elif pos["signal_class"] in {"C", "REJECT"}:
            pos["leverage"] = min(pos["leverage"], 14 if pos["symbol_profile"] == "ALT" else 20)

        return pos

    def _trade_log_meta(self, pos, exit_type):
        return {
            "signal_class": pos.get("signal_class", ""),
            "signal_score": float(pos.get("signal_score", 0.0) or 0.0),
            "rr_value": float(pos.get("rr_value", 0.0) or 0.0),
            "exit_type": exit_type,
            "btc_regime": pos.get("btc_regime", ""),
            "symbol_profile": pos.get("symbol_profile", ""),
            "htf_context": pos.get("htf_context", ""),
            "entry_context": pos.get("entry_context", ""),
        }

    def check_integrations(self):
        tg_result = self.notifier.test_connection()
        if tg_result.get("ok"):
            log_green("TELEGRAM connection ok")
        else:
            log_yellow(f"TELEGRAM connection issue | reason={tg_result.get('reason')}")

        if BINGX_ENABLED:
            bingx_result = self.executor.test_connection()
            if bingx_result.get("ok"):
                mode = "trade-enabled" if EXECUTION_MODE == "real" else "connected-paper"
                log_green(f"BINGX connection ok | mode={mode}")
                if TELEGRAM_ENABLED:
                    self.notifier.send(f"✅ BINGX connected ({mode})")
            else:
                log_yellow(f"BINGX connection issue | reason={bingx_result.get('reason')}")
                if TELEGRAM_ENABLED:
                    self.notifier.send(f"⚠️ BINGX connection issue: {bingx_result.get('reason')}")

        real_balance_info = None
        if EXECUTION_MODE == "real" and BINGX_ENABLED:
            real_balance_info = self._fetch_real_balance()
            if real_balance_info.get("ok") and real_balance_info.get("balance") is not None:
                self.balance = float(real_balance_info["balance"])
                self.risk_guard.initialize_balance(self.balance)
                log_green(
                    f"BINGX balance synced | balance={self._fmt_money(self.balance)} | "
                    f"path={real_balance_info.get('path', 'unknown')}"
                )
            else:
                log_yellow(
                    f"BINGX balance unavailable | reason={real_balance_info.get('reason', 'unknown')}"
                )

        startup_text = self._startup_status_message(real_balance_info=real_balance_info)
        log_cyan(startup_text.replace("\n", " | "))
        if TELEGRAM_ENABLED:
            self.notifier.send(startup_text)

    def _empty_position_slot(self, symbol: str):
        if symbol not in self.positions:
            self.positions[symbol] = None
        if symbol not in self.cooldown_until:
            self.cooldown_until[symbol] = 0
        if symbol not in self.last_signal:
            self.last_signal[symbol] = "NONE"

    def serialize_positions(self):
        data = {}
        for symbol, pos in self.positions.items():
            if pos is not None:
                data[symbol] = pos
        return data

    def save_runtime_state(self):
        self.state_store.save(
            {
                "balance": self.balance,
                "positions": self.serialize_positions(),
                "cooldown_until": self.cooldown_until,
                "last_signal": self.last_signal,
                "risk_guard": self.risk_guard.snapshot(),
            }
        )

    def restore_runtime_state(self):
        state = self.state_store.load()
        if not state:
            self.risk_guard.initialize_balance(self.balance)
            return

        self.balance = float(state.get("balance", self.balance))
        raw_positions = state.get("positions", {}) or {}
        self.positions = {}
        for symbol, pos in raw_positions.items():
            hydrated = self._hydrate_runtime_position(symbol, pos)
            if hydrated is not None:
                self.positions[symbol] = hydrated
        self.cooldown_until = state.get("cooldown_until", {}) or {}
        self.last_signal = state.get("last_signal", {}) or {}
        self.risk_guard.hydrate(state.get("risk_guard", {}) or {})
        self.risk_guard.initialize_balance(self.balance)

    def sync_with_exchange_state(self):
        remote = self.exchange_sync.map_by_symbol()
        if not remote:
            self.save_runtime_state()
            return

        for symbol in list(self.positions.keys()):
            pos = self.positions.get(symbol)
            if pos is None:
                continue
            if symbol not in remote:
                log_yellow(f"SYNC CLEAR {symbol} | reason=missing_on_exchange")
                self.positions[symbol] = None

        for symbol, remote_pos in remote.items():
            self._empty_position_slot(symbol)
            local = self.positions.get(symbol)
            if local is not None:
                continue

            qty = float(remote_pos.get("qty", 0.0) or 0.0)
            entry = float(remote_pos.get("entry_price", 0.0) or 0.0)
            position_side = str(remote_pos.get("position_side", "")).upper()
            side = "BUY" if "LONG" in position_side else "SELL"

            self.positions[symbol] = {
                "side": side,
                "entry": entry,
                "qty": qty,
                "stop": entry,
                "take": entry,
                "tp1": entry,
                "tp2": entry,
                "leverage": 1,
                "margin": 0.0,
                "notional": qty * entry,
                "risk_usdt": 0.0,
                "level_data": None,
                "liquidity_target": None,
                "atr_pct": 0.0,
                "be_moved": False,
                "partial_done": False,
                "trail_active": False,
                "opened_at": time.time(),
                "reason": "restored_from_exchange",
                "signal_score": 0.0,
                "signal_class": "SYNCED",
                "symbol_profile": self._symbol_profile(symbol, current_price=entry),
                "account_balance_at_open": self.balance,
                "btc_regime": "unknown",
                "btc_bias": "NEUTRAL",
                "funding_label": "unknown",
                "funding_rate": 0.0,
                "htf_context": "",
                "entry_context": "",
                "external_sync_only": True,
            }
            log_yellow(f"SYNC RESTORE {symbol} | side={side} | qty={qty:.4f} | entry={entry:.4f}")

        self.save_runtime_state()

    def _sync_reduce_on_exchange(self, symbol, pos, quantity):
        if EXECUTION_MODE != "real" or not BINGX_ENABLED:
            return True

        reduce_qty = round(quantity, 6)
        if reduce_qty <= 0:
            return False

        try:
            self.executor.reduce_position(
                symbol=symbol,
                side=self._exchange_close_side(pos["side"]),
                quantity=reduce_qty,
                position_side=self._exchange_position_side(pos["side"]),
            )
            return True
        except Exception as e:
            log_red(f"REAL CLOSE ERROR {symbol}: {e}")
            self.notifier.send(f"❌ REAL CLOSE ERROR {symbol}: {e}")
            return False

    def update_symbols(self):
        self.symbols = self.scanner.get_priority_symbols(
            top_volume_n=TOP_VOLUME_SYMBOLS_COUNT,
            top_gainers_n=TOP_GAINERS_COUNT,
            top_losers_n=TOP_LOSERS_COUNT,
        )
        self.configure_market_feed()

        log_cyan("UPDATED SYMBOL LIST:")
        log_cyan(
            f"TOTAL SYMBOLS SELECTED - {len(self.symbols)} symbols | "
            f"volume_top={TOP_VOLUME_SYMBOLS_COUNT} | gainers_top={TOP_GAINERS_COUNT} | losers_top={TOP_LOSERS_COUNT}"
        )
        for s in self.symbols:
            self._empty_position_slot(s)
            print(" -", s)

    def count_open_positions(self):
        return sum(1 for pos in self.positions.values() if pos is not None)

    def open_position(
        self,
        symbol,
        side,
        entry_price,
        score,
        reason,
        candles,
        signal_class="REJECT",
        strategy_meta=None,
        levels_candles=None,
    ):
        if self.positions.get(symbol) is not None:
            return

        if self.count_open_positions() >= MAX_OPEN_POSITIONS:
            log_yellow(f"SKIP {symbol} | reason=max_open_positions")
            return

        allowed, risk_reason = self.risk_guard.can_open_new_position(self.balance)
        if not allowed:
            log_yellow(f"SKIP {symbol} | reason={risk_reason}")
            return

        pos = self.position_manager.build_position(
            balance=self.balance,
            side=side,
            price=entry_price,
            sl_pct=STOP_LOSS_PCT,
            tp_pct=TAKE_PROFIT_PCT,
            score=score,
            candles=candles,
            signal_class=signal_class,
            levels_candles=levels_candles,
            symbol_profile=(strategy_meta or {}).get("symbol_profile", "ALT"),
        )

        pos["opened_at"] = time.time()
        pos["reason"] = reason
        pos["signal_score"] = score
        pos["signal_class"] = signal_class
        pos["account_balance_at_open"] = self.balance
        if strategy_meta:
            pos.update(strategy_meta)

        self.positions[symbol] = pos

        log_green(
            f"OPEN {symbol} {side} | class={signal_class} | entry={pos['entry']:.4f} | "
            f"notional={pos.get('notional', 0.0):.2f} | "
            f"margin={pos.get('margin', 0.0):.2f} | "
            f"qty={pos['qty']:.4f} | lev=x{pos['leverage']} | "
            f"SL={pos['stop']:.4f} | TP1={pos.get('tp1', pos['take']):.4f} | TP2={pos.get('tp2', pos['take']):.4f}"
        )

        risk = abs(pos["entry"] - pos["stop"]) * pos["qty"]
        reward = abs(pos["take"] - pos["entry"]) * pos["qty"]

        self.notifier.send(
            f"🟢 OPEN {symbol}\n"
            f"Class: {signal_class}\n"
            f"Side: {side}\n"
            f"Entry: {pos['entry']:.6f}\n"
            f"SL: {pos['stop']:.6f}\n"
            f"TP1: {pos.get('tp1', pos['take']):.6f}\n"
            f"TP2: {pos.get('tp2', pos['take']):.6f}\n"
            f"Qty: {pos['qty']:.4f}\n"
            f"Lev: x{pos['leverage']}\n"
            f"Notional: {pos.get('notional', 0.0):.2f} USDT\n"
            f"Risk: {risk:.2f} USDT\n"
            f"Reward: {reward:.2f} USDT\n"
            f"Reason: {reason}"
        )

        if EXECUTION_MODE == "real" and BINGX_ENABLED:
            try:
                leverage_side = self._exchange_position_side(side)
                self.executor.set_leverage(symbol, leverage_side, int(pos["leverage"]))
                self.executor.place_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=round(pos["qty"], 6),
                    position_side=leverage_side,
                )
                self.notifier.send(f"✅ REAL ORDER SENT {symbol} {side}")
            except Exception as e:
                self.positions[symbol] = None
                self.notifier.send(f"❌ REAL ORDER ERROR {symbol}: {e}")
                return

        level_data = pos.get("level_data")
        if level_data:
            log_green(
                f"LEVELS {symbol} | source={level_data['source']} | "
                f"support={level_data['support']} | resistance={level_data['resistance']} | "
                f"rr={level_data['rr']:.2f}"
            )

        self.save_runtime_state()

    def close_position(self, symbol, price, reason):
        pos = self.positions.get(symbol)

        if pos is None:
            return

        if not self._sync_reduce_on_exchange(symbol, pos, pos["qty"]):
            return

        if pos["side"] == "BUY":
            pnl = (price - pos["entry"]) * pos["qty"]
        else:
            pnl = (pos["entry"] - price) * pos["qty"]

        self.balance += pnl
        trade_meta = self._trade_log_meta(pos, reason)

        append_trade(
            symbol=symbol,
            side=pos["side"],
            entry=pos["entry"],
            exit_price=price,
            qty=pos["qty"],
            pnl=pnl,
            reason=reason,
            balance_after=self.balance,
            signal_class=trade_meta["signal_class"],
            signal_score=trade_meta["signal_score"],
            rr_value=trade_meta["rr_value"],
            exit_type=trade_meta["exit_type"],
            btc_regime=trade_meta["btc_regime"],
            symbol_profile=trade_meta["symbol_profile"],
            htf_context=trade_meta["htf_context"],
            entry_context=trade_meta["entry_context"],
        )

        result = "PLUS" if pnl >= 0 else "MINUS"

        log_yellow(
            f"CLOSE {symbol} {pos['side']} | class={pos.get('signal_class', 'REJECT')} | entry={pos['entry']:.4f} | "
            f"exit={price:.4f} | qty={pos['qty']:.4f} | pnl={pnl:.2f} | "
            f"{result} | balance={self.balance:.2f} | reason={reason}"
        )

        self.notifier.send(
            f"🔴 CLOSE {symbol}\n"
            f"Class: {pos.get('signal_class', 'REJECT')}\n"
            f"Side: {pos['side']}\n"
            f"Entry: {pos['entry']:.6f}\n"
            f"Exit: {price:.6f}\n"
            f"Qty: {pos['qty']:.4f}\n"
            f"PnL: {pnl:.2f} USDT\n"
            f"Reason: {reason}\n"
            f"Balance: {self.balance:.2f}"
        )

        self.positions[symbol] = None

        if reason == "stop_loss":
            self.cooldown_until[symbol] = time.time() + STOPLOSS_COOLDOWN_SECONDS
        else:
            self.cooldown_until[symbol] = time.time() + COOLDOWN_SECONDS

        self.risk_guard.register_closed_trade(pnl, self.balance)
        self.save_runtime_state()

    def partial_close(self, symbol, price):
        pos = self.positions.get(symbol)

        if pos is None or pos.get("partial_done"):
            return

        fraction = self.exit_manager.get_partial_fraction()

        qty_close = pos["qty"] * fraction
        qty_left = pos["qty"] - qty_close

        if qty_close <= 0 or qty_left <= 0:
            return

        if not self._sync_reduce_on_exchange(symbol, pos, qty_close):
            return

        if pos["side"] == "BUY":
            pnl = (price - pos["entry"]) * qty_close
        else:
            pnl = (pos["entry"] - price) * qty_close

        self.balance += pnl
        trade_meta = self._trade_log_meta(pos, "partial_close")

        append_trade(
            symbol=symbol,
            side=pos["side"],
            entry=pos["entry"],
            exit_price=price,
            qty=qty_close,
            pnl=pnl,
            reason="partial_close",
            balance_after=self.balance,
            signal_class=trade_meta["signal_class"],
            signal_score=trade_meta["signal_score"],
            rr_value=trade_meta["rr_value"],
            exit_type=trade_meta["exit_type"],
            btc_regime=trade_meta["btc_regime"],
            symbol_profile=trade_meta["symbol_profile"],
            htf_context=trade_meta["htf_context"],
            entry_context=trade_meta["entry_context"],
        )

        pos["qty"] = qty_left
        pos["partial_done"] = True
        pos["margin"] *= (1 - fraction)
        pos["notional"] *= (1 - fraction)

        log_cyan(
            f"PARTIAL {symbol} {pos['side']} | exit={price:.4f} | "
            f"closed_qty={qty_close:.4f} | remain_qty={qty_left:.4f} | "
            f"pnl={pnl:.2f} | balance={self.balance:.2f}"
        )

        self.notifier.send(
            f"🟡 PARTIAL CLOSE {symbol}\n"
            f"Class: {pos.get('signal_class', 'REJECT')}\n"
            f"Side: {pos['side']}\n"
            f"Entry: {pos['entry']:.6f}\n"
            f"Exit: {price:.6f}\n"
            f"Closed qty: {qty_close:.4f}\n"
            f"Remain qty: {qty_left:.4f}\n"
            f"PnL: {pnl:.2f} USDT\n"
            f"Balance: {self.balance:.2f}"
        )

        self.save_runtime_state()

    def manage_position(self, symbol, current_price, signal_side, orderflow_bias=0.0, oi_bias=0.0):
        pos = self.positions.get(symbol)
        if pos is None or current_price is None:
            return
        if pos.get("external_sync_only"):
            return

        account_balance_at_open = float(pos.get("account_balance_at_open", self.balance) or self.balance)
        max_position_drawdown = account_balance_at_open * MAX_POSITION_DEPOSIT_DRAWDOWN_PCT
        if self.exit_manager.unrealized_pnl(pos, current_price) <= -max_position_drawdown:
            self.close_position(symbol, current_price, "max_deposit_drawdown_exit")
            return

        if self.exit_manager.should_be_and_partial_on_profit(pos, current_price):
            if not pos.get("be_moved"):
                old_stop, new_stop = self.exit_manager.apply_break_even(pos)
                if old_stop != new_stop:
                    log_cyan(f"BE {symbol} | old_SL={old_stop:.4f} | new_SL={new_stop:.4f}")
                    self.notifier.send(f"🟦 BE {symbol}\nOld SL: {old_stop:.6f}\nNew SL: {new_stop:.6f}")

            if not pos.get("partial_done"):
                self.partial_close(symbol, current_price)

        pos = self.positions.get(symbol)
        if pos is None:
            return

        if self.exit_manager.should_move_to_break_even(pos, current_price):
            old_stop, new_stop = self.exit_manager.apply_break_even(pos)
            if old_stop != new_stop:
                log_cyan(f"BE {symbol} | old_SL={old_stop:.4f} | new_SL={new_stop:.4f}")
                self.notifier.send(f"🟦 BE {symbol}\nOld SL: {old_stop:.6f}\nNew SL: {new_stop:.6f}")

        if self.exit_manager.should_partial_close(pos, current_price):
            self.partial_close(symbol, current_price)

        pos = self.positions.get(symbol)
        if pos is None:
            return

        if self.exit_manager.should_activate_trailing(pos, current_price):
            old_stop, new_stop = self.exit_manager.apply_trailing(pos, current_price)
            if old_stop != new_stop:
                log_cyan(f"TRAIL {symbol} | old_SL={old_stop:.4f} | new_SL={new_stop:.4f}")
                self.notifier.send(f"🟪 TRAIL {symbol}\nOld SL: {old_stop:.6f}\nNew SL: {new_stop:.6f}")

        pos = self.positions.get(symbol)
        if pos is None:
            return

        if self.exit_manager.should_early_exit_no_followthrough(pos, current_price):
            self.close_position(symbol, current_price, "early_exit_no_followthrough")
            return

        if self.exit_manager.should_take_liquidity_target(pos, current_price):
            self.close_position(symbol, current_price, "liquidity_target")
            return

        if pos["side"] == "BUY":
            if current_price <= pos["stop"]:
                self.close_position(symbol, current_price, "stop_loss")
                return
            if current_price >= pos["take"]:
                self.close_position(symbol, current_price, "take_profit")
                return
        else:
            if current_price >= pos["stop"]:
                self.close_position(symbol, current_price, "stop_loss")
                return
            if current_price <= pos["take"]:
                self.close_position(symbol, current_price, "take_profit")
                return

        hold_seconds = self.exit_manager.hold_seconds_for_position(pos)
        if time.time() - pos["opened_at"] < hold_seconds:
            return

        if self.exit_manager.should_exit_on_adverse_flow(pos, orderflow_bias=orderflow_bias, oi_bias=oi_bias):
            self.close_position(symbol, current_price, "adverse_flow_exit")
            return

        # reverse signal закрывает только слабые сделки
        if pos.get("signal_class") in {"C", "REJECT"}:
            if pos["side"] == "BUY" and signal_side == "SELL":
                self.close_position(symbol, current_price, "reverse_signal")
                return
            if pos["side"] == "SELL" and signal_side == "BUY":
                self.close_position(symbol, current_price, "reverse_signal")
                return

    def print_open_positions(self):
        any_open = False
        for symbol, pos in self.positions.items():
            if pos is None:
                continue

            any_open = True
            candles = fetch_klines(symbol, "15m", 5)
            now_price = candles[-1]["close"] if candles else 0.0

            if pos["side"] == "BUY":
                pnl = (now_price - pos["entry"]) * pos["qty"]
            else:
                pnl = (pos["entry"] - now_price) * pos["qty"]

            log_cyan(
                f"OPEN_POS {symbol} | {pos['side']} | class={pos.get('signal_class', 'REJECT')} | "
                f"entry={self._fmt_price(pos['entry'])} -> now={self._fmt_price(now_price)} | "
                f"PnL={self._fmt_money(pnl)} | risk={self._fmt_money(pos.get('risk_usdt', 0.0))} | "
                f"margin={self._fmt_money(pos.get('margin', 0.0))} | lev=x{pos['leverage']} | "
                f"SL={self._fmt_price(pos['stop'])} | TP1={self._fmt_price(pos.get('tp1', pos['take']))} | "
                f"TP2={self._fmt_price(pos.get('tp2', pos['take']))}"
            )

        if not any_open:
            log_cyan("OPEN_POS none")

    def build_open_positions_report(self):
        rows = []
        total_unrealized = 0.0

        for symbol, pos in self.positions.items():
            if pos is None:
                continue

            candles = fetch_klines(symbol, "15m", 5)
            now_price = candles[-1]["close"] if candles else 0.0

            if pos["side"] == "BUY":
                pnl = (now_price - pos["entry"]) * pos["qty"]
            else:
                pnl = (pos["entry"] - now_price) * pos["qty"]

            total_unrealized += pnl
            rows.append(
                f"{symbol} | {pos['side']} | class={pos.get('signal_class', 'REJECT')} | "
                f"entry={self._fmt_price(pos['entry'])} | now={self._fmt_price(now_price)} | "
                f"pnl={self._fmt_money(pnl)} | risk={self._fmt_money(pos.get('risk_usdt', 0.0))} | "
                f"SL={self._fmt_price(pos['stop'])} | TP1={self._fmt_price(pos.get('tp1', pos['take']))} | "
                f"TP2={self._fmt_price(pos.get('tp2', pos['take']))} | lev=x{pos['leverage']} | "
                f"btc={pos.get('btc_regime', '')} | funding={pos.get('funding_label', '')}"
            )

        if not rows:
            return "OPEN_POS none"

        return (
            f"Open positions\n"
            f"Count: {len(rows)}\n"
            f"Unrealized PnL: {self._fmt_money(total_unrealized)}\n"
            f"Runtime balance: {self._fmt_money(self.balance)}\n\n"
            + "\n".join(rows)
        )
    

    def analyze_symbol(self, symbol):
        htf_trend = detect_htf_trend(symbol)

        candles = fetch_klines(symbol, "15m", 200)
        if not candles:
            return
        candles_1h = fetch_klines(symbol, "1h", 200) or []
        candles_4h = fetch_klines(symbol, "4h", 120) or []

        current_price = float(candles[-1]["close"])

        structure_4h = detect_market_structure(candles_4h)
        structure_15m = detect_market_structure(candles)
        volume_confirmed_4h, _, _ = breakout_volume_confirms(candles_4h)
        volume_confirmed_15m, last_vol, avg_vol = breakout_volume_confirms(candles)
        regime_4h = market_regime(candles_4h)
        regime_name = regime_4h.get("name", "range_day")
        btc_regime = self._safe_detect("btc_regime", detect_btc_regime, default=None) if ENABLE_BTC_REGIME_FILTER else {
            "regime": "disabled",
            "bias": "NEUTRAL",
            "strength": 0.0,
            "reason": "btc_regime_disabled",
        }
        if not btc_regime:
            btc_regime = {
                "regime": "unknown",
                "bias": "NEUTRAL",
                "strength": 0.0,
                "reason": "btc_regime_unavailable",
            }
        funding_context = classify_funding_context(
            symbol,
            strong_threshold=FUNDING_RATE_STRONG_THRESHOLD,
            extreme_threshold=FUNDING_RATE_EXTREME_THRESHOLD,
        ) if FUNDING_RATE_ENABLED else {
            "rate": None,
            "label": "funding_disabled",
            "continuation_bias": "NEUTRAL",
            "reversal_bias": "NEUTRAL",
            "score_bias": 0.0,
        }

        breakout_4h = detect_range_breakout(candles_4h)
        trendline_4h = detect_trendline_breakout(candles_4h)
        retest_4h = None
        if trendline_4h:
            retest_4h = detect_retest_after_breakout(candles_4h, trendline_4h)
        elif breakout_4h:
            retest_4h = detect_retest_after_breakout(candles_4h, breakout_4h)

        breakout_confirmation = detect_range_breakout(candles)
        trendline_confirmation = detect_trendline_breakout(candles)

        retest_confirmation = None
        if trendline_confirmation:
            retest_confirmation = detect_retest_after_breakout(candles, trendline_confirmation)
        elif breakout_confirmation:
            retest_confirmation = detect_retest_after_breakout(candles, breakout_confirmation)

        fast_move_4h = detect_fast_move(candles_4h)
        acceleration_4h = detect_price_acceleration(candles_4h)
        fast_move = detect_fast_move(candles)
        acceleration = detect_price_acceleration(candles)

        pattern = None
        trades = []
        imbalance = 0.0
        if self.market_feed is not None:
            trades, feed_price, imbalance = self.market_feed.snapshot(symbol)
            if feed_price is not None:
                current_price = float(feed_price)

        oi_now, oi_prev = self.oi_client.get_oi_pair(symbol)
        oi_data = classify_oi_price_context(candles_4h, oi_now, oi_prev, lookback=4)
        oi_ready = oi_data.get("label") != "oi_unavailable"

        symbol_profile = self._symbol_profile(symbol, current_price=current_price)

        liquidity_sweep_4h = self._safe_detect("liquidity_sweep_4h", detect_liquidity_sweep, candles_4h, default=None)
        liquidity_sweep = self._safe_detect("liquidity_sweep_15m", detect_liquidity_sweep, candles, default=None)
        breakout_multi_bar = bool(breakout_confirmation and multi_bar_breakout_confirmation(candles, breakout_confirmation))
        trendline_multi_bar = bool(trendline_confirmation and multi_bar_breakout_confirmation(candles, trendline_confirmation))

        base_breakout = None
        base_entry = None
        if ENABLE_BASE_BREAKOUT and candles_4h:
            base_breakout = self._safe_detect(
                "base_breakout",
                detect_base_breakout,
                candles_4h,
                lookback=BASE_LOOKBACK_4H,
                volume_mult=BASE_VOLUME_MULTIPLIER,
                default=None,
            )
            if base_breakout and not not_overextended_from_base(
                base_breakout,
                current_price,
                max_move_from_base=BASE_MAX_MOVE_FROM_RANGE,
            ):
                base_breakout = None
            if base_breakout:
                base_entry = self._safe_detect(
                    "base_breakout_15m_confirm",
                    confirm_base_breakout_entry_15m,
                    candles,
                    base_breakout,
                    default=None,
                )
                if base_entry is None:
                    base_breakout = None

        htf_reversal = None
        reversal_entry = None
        if ENABLE_HTF_REVERSAL and candles_4h:
            htf_reversal = self._safe_detect(
                "htf_reversal",
                detect_htf_reversal,
                candles_4h,
                lookback=REVERSAL_LOOKBACK_4H,
                volume_mult=REVERSAL_VOLUME_MULTIPLIER,
                default=None,
            )
            if htf_reversal:
                reversal_entry = self._safe_detect(
                    "reversal_entry_15m",
                    confirm_reversal_entry_15m,
                    candles,
                    htf_reversal,
                    default=None,
                )
                if (
                    reversal_entry is None
                    or not reversal_not_overextended(
                        htf_reversal,
                        current_price,
                        max_extension_pct=MAX_REVERSAL_EXTENSION_PCT,
                    )
                ):
                    htf_reversal = None
                    reversal_entry = None

        rsi_div_4h = self._safe_detect("rsi_div_4h", detect_rsi_divergence, candles_4h, default=None) if RSI_DIVERGENCE_ENABLED and candles_4h else None
        macd_div_4h = self._safe_detect("macd_div_4h", detect_macd_divergence, candles_4h, default=None) if MACD_DIVERGENCE_ENABLED and candles_4h else None
        double_div_4h = self._safe_detect("double_div_4h", detect_double_divergence, candles_4h, default=None) if candles_4h else None
        rsi_div_15m = self._safe_detect("rsi_div_15m", detect_rsi_divergence, candles, default=None) if RSI_DIVERGENCE_ENABLED else None
        macd_div_15m = self._safe_detect("macd_div_15m", detect_macd_divergence, candles, default=None) if MACD_DIVERGENCE_ENABLED else None
        double_div_15m = self._safe_detect("double_div_15m", detect_double_divergence, candles, default=None)

        if rsi_div_4h and not divergence_not_overextended(
            candles,
            rsi_div_4h["direction"],
            pivot_index=rsi_div_15m.get("pivot_index") if rsi_div_15m else rsi_div_4h.get("pivot_index"),
        ):
            rsi_div_4h = None
        if macd_div_4h and not divergence_not_overextended(
            candles,
            macd_div_4h["direction"],
            pivot_index=macd_div_15m.get("pivot_index") if macd_div_15m else macd_div_4h.get("pivot_index"),
        ):
            macd_div_4h = None
        if double_div_4h and not divergence_not_overextended(
            candles,
            double_div_4h["direction"],
            pivot_index=double_div_15m.get("pivot_index") if double_div_15m else double_div_4h.get("pivot_index"),
        ):
            double_div_4h = None

        order_block = self._safe_detect(
            "order_block",
            detect_order_block,
            candles_4h,
            lookback=ORDER_BLOCK_LOOKBACK,
            default=None,
        ) if ENABLE_ORDER_BLOCK and candles_4h else None
        order_block_entry = self._safe_detect(
            "order_block_retest",
            confirm_order_block_retest,
            candles,
            order_block,
            default=None,
        ) if order_block else None

        chart_pattern = self._safe_detect(
            "chart_pattern_4h",
            detect_best_chart_pattern,
            candles_4h[-CHART_PATTERN_LOOKBACK_4H:] if candles_4h else candles_4h,
            default=None,
        ) if ENABLE_CHART_PATTERNS and candles_4h else None
        chart_pattern_entry = self._safe_detect(
            "chart_pattern_15m_confirm",
            confirm_chart_pattern_entry_15m,
            candles,
            chart_pattern,
            default=None,
        ) if chart_pattern else None
        if chart_pattern:
            if (
                chart_pattern_entry is None
                or not chart_pattern_not_overextended(
                    chart_pattern,
                    current_price,
                    max_extension_pct=MAX_CHART_PATTERN_EXTENSION_PCT,
                )
            ):
                chart_pattern = None
                chart_pattern_entry = None

        sig = build_signal(
            symbol=symbol,
            trades=trades,
            imbalance=imbalance,
            oi_now=oi_now,
            oi_prev=oi_prev,
            oi_context=oi_data,
            pattern=pattern,
            breakout_confirmation=breakout_4h,
            trendline_confirmation=trendline_4h,
            retest_confirmation=retest_4h,
            regime=regime_4h,
            liquidity_sweep=liquidity_sweep_4h,
            htf_trend=htf_trend,
            structure=structure_4h,
            symbol_profile=symbol_profile,
        )

        if breakout_confirmation and sig.side != "HOLD" and breakout_confirmation["direction"] == sig.side:
            sig.score = max(sig.score, 0.42)
            sig.reason = f"{sig.reason}|{breakout_confirmation['reason']}"
            sig.entry_price = breakout_confirmation.get("entry_price", sig.entry_price)

        if trendline_confirmation and sig.side != "HOLD" and trendline_confirmation["direction"] == sig.side:
            sig.score = max(sig.score, 0.42)
            sig.reason = f"{sig.reason}|{trendline_confirmation['reason']}"
            sig.entry_price = trendline_confirmation.get("entry_price", sig.entry_price)

        if retest_confirmation and sig.side != "HOLD" and retest_confirmation["direction"] == sig.side:
            sig.score = max(sig.score, 0.48)
            sig.reason = f"{sig.reason}|{retest_confirmation['reason']}"
            sig.entry_price = retest_confirmation["entry_price"]

        # fast_move/acceleration только усиливают подтвержденный сигнал
        if fast_move and sig.side != "HOLD" and fast_move["direction"] == sig.side:
            sig.score = max(sig.score, 0.40)
            sig.reason = f"{sig.reason}|{fast_move['reason']}"

        if acceleration and sig.side != "HOLD" and acceleration["direction"] == sig.side:
            sig.score = max(sig.score, 0.44)
            sig.reason = f"{sig.reason}|{acceleration['reason']}"

        sig.side = self.invert_side_if_needed(sig.side)

        hold_candidates = []

        if base_breakout:
            if sig.side == base_breakout["direction"]:
                sig.score = max(sig.score, max(0.55, float(base_breakout["strength"])))
                sig.reason = f"{sig.reason}|base_breakout_trigger|{base_entry['reason']}"
                sig.entry_price = base_entry["entry_price"]
            hold_candidates.append(
                {
                    "direction": base_breakout["direction"],
                    "entry_price": base_entry["entry_price"],
                    "strength": float(base_breakout["strength"]),
                    "reason": f"{base_breakout['reason']}|{base_entry['reason']}",
                }
            )

        if htf_reversal and reversal_entry:
            if sig.side == htf_reversal["direction"]:
                sig.score = max(sig.score, max(0.58, float(htf_reversal["strength"])))
                sig.reason = f"{sig.reason}|{htf_reversal['pattern']}|{reversal_entry['reason']}"
                sig.entry_price = reversal_entry["entry_price"]
            hold_candidates.append(
                {
                    "direction": htf_reversal["direction"],
                    "entry_price": reversal_entry["entry_price"],
                    "strength": float(htf_reversal["strength"]),
                    "reason": f"{htf_reversal['reason']}|{reversal_entry['reason']}",
                }
            )

        divergence_candidates = []
        if double_div_4h and double_div_15m and double_div_4h["direction"] == double_div_15m["direction"]:
            divergence_candidates.append(
                {
                    "direction": double_div_4h["direction"],
                    "entry_price": current_price,
                    "strength": max(0.72, double_div_4h["strength"], double_div_15m["strength"]),
                    "reason": "double_divergence_confirmed",
                }
            )
        if rsi_div_4h and rsi_div_15m and rsi_div_4h["direction"] == rsi_div_15m["direction"]:
            divergence_candidates.append(
                {
                    "direction": rsi_div_4h["direction"],
                    "entry_price": current_price,
                    "strength": max(0.60, rsi_div_4h["strength"], rsi_div_15m["strength"]),
                    "reason": "rsi_divergence_mtf",
                }
            )
        if macd_div_4h and macd_div_15m and macd_div_4h["direction"] == macd_div_15m["direction"]:
            divergence_candidates.append(
                {
                    "direction": macd_div_4h["direction"],
                    "entry_price": current_price,
                    "strength": max(0.60, macd_div_4h["strength"], macd_div_15m["strength"]),
                    "reason": "macd_divergence_mtf",
                }
            )

        divergence_signal = max(divergence_candidates, key=lambda item: item["strength"]) if divergence_candidates else None
        double_divergence_confirmed = any(item["reason"] == "double_divergence_confirmed" for item in divergence_candidates)
        if divergence_signal:
            if sig.side == divergence_signal["direction"]:
                sig.score = max(sig.score, divergence_signal["strength"])
                sig.reason = f"{sig.reason}|{divergence_signal['reason']}"
                sig.entry_price = divergence_signal["entry_price"]
            hold_candidates.append(divergence_signal)

        if order_block and order_block_entry:
            if sig.side == order_block["direction"]:
                sig.score = max(sig.score, max(0.57, float(order_block["strength"])))
                sig.reason = f"{sig.reason}|{order_block['pattern']}|{order_block_entry['reason']}"
                sig.entry_price = order_block_entry["entry_price"]
            hold_candidates.append(
                {
                    "direction": order_block["direction"],
                    "entry_price": order_block_entry["entry_price"],
                    "strength": float(order_block["strength"]),
                    "reason": f"{order_block['reason']}|{order_block_entry['reason']}",
                }
            )

        if chart_pattern and chart_pattern_entry:
            if sig.side == chart_pattern["direction"]:
                sig.score = max(sig.score, max(0.58, float(chart_pattern["strength"])))
                sig.reason = f"{sig.reason}|{chart_pattern['pattern']}|{chart_pattern_entry['reason']}"
                sig.entry_price = chart_pattern_entry["entry_price"]
            hold_candidates.append(
                {
                    "direction": chart_pattern["direction"],
                    "entry_price": chart_pattern_entry["entry_price"],
                    "strength": float(chart_pattern["strength"]),
                    "reason": f"{chart_pattern['reason']}|{chart_pattern_entry['reason']}",
                }
            )

        if sig.side == "HOLD" and hold_candidates:
            best_candidate = max(hold_candidates, key=lambda item: item["strength"])
            sig.side = best_candidate["direction"]
            sig.score = max(sig.score, best_candidate["strength"])
            sig.reason = f"{sig.reason}|{best_candidate['reason']}"
            sig.entry_price = best_candidate["entry_price"]

        strong_reversal_context = bool(
            (htf_reversal and htf_reversal.get("direction") == sig.side)
            or (divergence_signal and divergence_signal.get("direction") == sig.side)
            or (
                chart_pattern
                and chart_pattern.get("direction") == sig.side
                and chart_pattern.get("pattern") in {
                    "head_and_shoulders",
                    "inverse_head_and_shoulders",
                    "double_bottom",
                    "double_top",
                    "triple_bottom",
                    "triple_top",
                    "falling_wedge",
                    "rising_wedge",
                }
            )
        )
        continuation_context = bool(
            (breakout_4h and breakout_4h.get("direction") == sig.side)
            or (trendline_4h and trendline_4h.get("direction") == sig.side)
            or (base_breakout and base_breakout.get("direction") == sig.side)
            or (chart_pattern and chart_pattern.get("direction") == sig.side and chart_pattern.get("pattern") in {
                "ascending_triangle",
                "descending_triangle",
                "symmetrical_triangle",
                "rectangle",
                "cup_and_handle",
                "inverse_cup_and_handle",
            })
        )

        # мягкий HTF penalty
        if htf_trend == "BULL" and sig.side == "SELL":
            sig.score = max(0.0, sig.score - 0.05)
            sig.reason = f"{sig.reason}|soft_htf_bull_penalty"

        if htf_trend == "BEAR" and sig.side == "BUY":
            sig.score = max(0.0, sig.score - 0.05)
            sig.reason = f"{sig.reason}|soft_htf_bear_penalty"

        if btc_regime["bias"] == sig.side and sig.side in {"BUY", "SELL"}:
            sig.score = max(sig.score, min(0.78, sig.score + 0.04 + btc_regime["strength"] * 0.02))
            sig.reason = f"{sig.reason}|{btc_regime['reason']}"
        elif btc_regime["bias"] not in {"NEUTRAL", sig.side} and sig.side in {"BUY", "SELL"}:
            penalty = BTC_REGIME_CONFLICT_PENALTY * (0.6 if strong_reversal_context else 1.0)
            sig.score = max(0.0, sig.score - penalty)
            sig.reason = f"{sig.reason}|btc_regime_conflict"

        if (
            funding_context["continuation_bias"] == sig.side
            and continuation_context
            and not strong_reversal_context
            and sig.side in {"BUY", "SELL"}
        ):
            sig.score = max(0.0, sig.score + funding_context["score_bias"])
            sig.reason = f"{sig.reason}|{funding_context['label']}"
        elif (
            funding_context["reversal_bias"] == sig.side
            and strong_reversal_context
            and sig.side in {"BUY", "SELL"}
        ):
            sig.score = max(sig.score, min(0.8, sig.score + abs(funding_context["score_bias"]) * 0.5))
            sig.reason = f"{sig.reason}|funding_reversal_alignment"

        volume_confirmed = volume_confirmed_15m or volume_confirmed_4h
        structure_ok = structure_allows_side(structure_4h, sig.side) if sig.side in ("BUY", "SELL") else False
        base_15m_confirmed = bool(base_breakout and base_entry)

        signal_class, quality_reasons = classify_signal_quality(
            side=sig.side,
            score=sig.score,
            breakout_confirmation=breakout_confirmation,
            trendline_confirmation=trendline_confirmation,
            retest_confirmation=retest_confirmation,
            fast_move=fast_move,
            acceleration=acceleration,
            htf_trend=htf_trend,
            volume_confirmed=volume_confirmed,
            structure_ok=structure_ok,
            regime_name=regime_name,
            liquidity_sweep=(liquidity_sweep if liquidity_sweep else liquidity_sweep_4h),
            multi_bar_confirmed=(breakout_multi_bar or trendline_multi_bar),
            base_breakout=(base_breakout if base_15m_confirmed else None),
            reversal_signal=htf_reversal,
            reversal_confirmed=bool(reversal_entry),
            divergence_signal=divergence_signal,
            double_divergence=double_divergence_confirmed,
            order_block_signal=order_block,
            order_block_confirmed=bool(order_block_entry),
            chart_pattern_signal=chart_pattern,
            chart_pattern_confirmed=bool(chart_pattern_entry),
        )

        signal_class, sync_reasons = self._normalize_signal_class(
            signal_class=signal_class,
            score=sig.score,
            strong_reversal_context=strong_reversal_context,
            retest_confirmation=retest_confirmation,
            breakout_confirmation=breakout_confirmation,
            trendline_confirmation=trendline_confirmation,
            base_entry=base_entry,
            reversal_entry=reversal_entry,
            order_block_entry=order_block_entry,
            chart_pattern_entry=chart_pattern_entry,
            regime_name=regime_name,
            symbol_profile=symbol_profile,
        )
        quality_reasons.extend(sync_reasons)

        sig.signal_class = signal_class
        sig.reason = f"{sig.reason}|class={signal_class}|q={','.join(quality_reasons)}"

        if self.last_signal.get(symbol) != sig.side:
            log(
                f"{symbol} signal {self.last_signal.get(symbol, 'NONE')} -> {sig.side} | "
                f"trend={htf_trend} | regime={regime_name} | btc={btc_regime['regime']} | class={sig.signal_class} | "
                f"score={sig.score:.3f} | reason={sig.reason}"
            )
            self.last_signal[symbol] = sig.side

        if self.positions.get(symbol) is not None:
            self.manage_position(symbol, current_price, sig.side)
            return

        if time.time() < self.cooldown_until.get(symbol, 0):
            return

        local_entry_confirmed = any(
            item is not None and item.get("direction") == sig.side
            for item in [breakout_confirmation, trendline_confirmation, retest_confirmation, fast_move, acceleration]
        )
        specialized_entry_confirmed = (
            (base_breakout is not None and base_entry is not None and base_breakout.get("direction") == sig.side)
            or (htf_reversal is not None and reversal_entry is not None and htf_reversal.get("direction") == sig.side)
            or (divergence_signal is not None and divergence_signal.get("direction") == sig.side)
            or (order_block is not None and order_block_entry is not None and order_block.get("direction") == sig.side)
            or (chart_pattern is not None and chart_pattern_entry is not None and chart_pattern.get("direction") == sig.side)
        )

        if sig.side in {"BUY", "SELL"} and not (local_entry_confirmed or specialized_entry_confirmed):
            log_yellow(f"BLOCKED {symbol} | reason=no_15m_entry_confirmation")
            return

        # дешёвые монеты: нужен хотя бы retest или сильный RR
        if LOW_PRICE_REQUIRES_RETEST:
            if is_low_price_coin(current_price, LOW_PRICE_COIN_THRESHOLD):
                if sig.side == "BUY" and retest_confirmation is None and base_entry is None and order_block_entry is None:
                    log_yellow(f"BLOCKED {symbol} | reason=low_price_requires_retest")
                    return

        blocked_ext = False
        ext_value = 0.0
        if EXTENSION_FILTER_ENABLED and sig.side in ("BUY", "SELL"):
            blocked_ext, ext_value = blocked_by_extension(
                candles=candles,
                side=sig.side,
                lookback=EXTENSION_LOOKBACK,
                max_ext_low_pct=MAX_EXTENSION_FROM_LOCAL_LOW_PCT,
                max_ext_high_pct=MAX_EXTENSION_FROM_LOCAL_HIGH_PCT,
            )

        blocked = False
        move_pct = 0.0
        if sig.side in ("BUY", "SELL") and ANTI_FOMO_ENABLED:
            blocked, move_pct = blocked_by_anti_fomo(
                candles=candles,
                side=sig.side,
                lookback=ANTI_FOMO_LOOKBACK,
                max_move_pct=ANTI_FOMO_MAX_MOVE_PCT,
            )

        # preview позиции: RR по уровням
        level_data = None
        rr_value = 0.0

        if sig.side in {"BUY", "SELL"}:
            try:
                preview_pos = self.position_manager.build_position(
                    balance=self.balance,
                    side=sig.side,
                    price=(sig.entry_price if sig.entry_price else current_price),
                    sl_pct=STOP_LOSS_PCT,
                    tp_pct=TAKE_PROFIT_PCT,
                    score=sig.score,
                    candles=candles,
                    signal_class=sig.signal_class,
                    levels_candles=candles_1h,
                    symbol_profile=symbol_profile,
                )
                level_data = preview_pos.get("level_data")
                if level_data:
                    rr_value = float(level_data.get("rr", 0.0))
            except Exception as e:
                log_yellow(f"RR PREVIEW FAIL {symbol} | error={e}")
                level_data = None
                rr_value = 0.0

        # если сигнал формально слабый, но RR высокий — разрешаем
        if (
            sig.signal_class == "REJECT"
            and ALLOW_REJECT_IF_HIGH_RR
            and rr_value >= HIGH_RR_OVERRIDE_THRESHOLD
        ):
            log_yellow(
                f"REJECT OVERRIDDEN {symbol} | rr={rr_value:.2f} | class=REJECT -> class=B"
            )
            sig.signal_class = "B"
            sig.reason = f"{sig.reason}|reject_overridden_by_rr={rr_value:.2f}"

        strong_signal_class = self._is_strong_signal_class(sig.signal_class)
        weak_signal_class = sig.signal_class in {"C", "REJECT"}
        high_rr_trade = rr_value >= HIGH_RR_OVERRIDE_THRESHOLD

        if order_block and order_block.get("direction") != sig.side and not strong_reversal_context:
            if order_block.get("pattern") == "bullish_order_block" and sig.side == "SELL":
                log_yellow(f"BLOCKED {symbol} | reason=against_bullish_order_block")
                return
            if order_block.get("pattern") == "bearish_order_block" and sig.side == "BUY":
                log_yellow(f"BLOCKED {symbol} | reason=against_bearish_order_block")
                return

        if btc_regime["bias"] == "BUY" and sig.side == "SELL" and not strong_reversal_context and rr_value < HIGH_RR_OVERRIDE_THRESHOLD:
            log_yellow(f"BLOCKED {symbol} | reason=btc_bullish_conflict")
            return
        if btc_regime["bias"] == "SELL" and sig.side == "BUY" and not strong_reversal_context and rr_value < HIGH_RR_OVERRIDE_THRESHOLD:
            log_yellow(f"BLOCKED {symbol} | reason=btc_bearish_conflict")
            return

        if (
            symbol_profile == "LOW_CAP"
            and regime_name == "high_volatility_panic"
            and continuation_context
            and not strong_reversal_context
            and not strong_signal_class
        ):
            log_yellow(f"BLOCKED {symbol} | reason=low_cap_panic_continuation")
            return

        if (
            funding_context["continuation_bias"] == sig.side
            and continuation_context
            and not strong_reversal_context
            and abs(float(funding_context.get("rate") or 0.0)) >= FUNDING_RATE_STRONG_THRESHOLD
            and rr_value < HIGH_RR_OVERRIDE_THRESHOLD
        ):
            log_yellow(f"BLOCKED {symbol} | reason=funding_conflict")
            return

        # для low-cap не пускаем C/REJECT без retest
        if symbol_profile == "LOW_CAP" and retest_confirmation is None and sig.signal_class not in {
            "A", "B", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"
        }:
            log_yellow(f"BLOCKED {symbol} | reason=low_cap_needs_better_context")
            return

        if (
            symbol_profile == "LOW_CAP"
            and continuation_context
            and not strong_reversal_context
            and retest_confirmation is None
            and liquidity_sweep is None
            and not high_rr_trade
        ):
            log_yellow(f"BLOCKED {symbol} | reason=low_cap_continuation_needs_retest")
            return

        if (
            symbol_profile == "LOW_CAP"
            and weak_signal_class
            and (not level_data or level_data.get("source") != "levels")
            and not high_rr_trade
        ):
            log_yellow(f"BLOCKED {symbol} | reason=low_cap_needs_structural_levels")
            return

        # breakout без объема разрешаем только если есть retest или RR очень высокий
        if breakout_confirmation and not volume_confirmed and retest_confirmation is None and rr_value < 2.5:
            log_yellow(
                f"BLOCKED {symbol} | reason=breakout_no_volume_low_rr | last_vol={last_vol:.2f} | avg_vol={avg_vol:.2f}"
            )
            return

        allowed, reason = apply_block_filters(
            symbol,
            sig,
            structure_ok=structure_ok,
            volume_confirmed=volume_confirmed,
            panic_regime=(regime_name == "high_volatility_panic" or btc_regime["regime"] == "panic"),
            reclaim_needed=(
                symbol_profile in {"ALT", "LOW_CAP"}
                and liquidity_sweep is None
                and retest_confirmation is None
                and sig.signal_class not in {"A", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"}
            ),
            oi_ready=oi_ready,
            htf_conflict=(btc_regime["bias"] not in {"NEUTRAL", sig.side} and btc_regime["strength"] >= 0.8),
            extension_block=blocked_ext,
            anti_fomo_block=blocked,
        )

        if not allowed:
            log_yellow(f"BLOCKED {symbol} | reason={reason}")
            return

        # отдельные проверки валидности пробоя
        if breakout_confirmation and not breakout_multi_bar and retest_confirmation is None and rr_value < 2.8:
            log_yellow(f"BLOCKED {symbol} | reason=breakout_not_held")
            return

        if trendline_confirmation and not trendline_multi_bar and retest_confirmation is None and rr_value < 2.8:
            log_yellow(f"BLOCKED {symbol} | reason=trendline_not_held")
            return

        if is_false_breakout(candles, breakout_confirmation) or is_false_breakout(candles, trendline_confirmation):
            log_yellow(f"BLOCKED {symbol} | reason=false_breakout")
            return

        if sig.side in ("BUY", "SELL"):
            entry_price = sig.entry_price if sig.entry_price else current_price

            strategy_meta = {
                "rr_value": rr_value,
                "symbol_profile": symbol_profile,
                "btc_regime": btc_regime["regime"],
                "btc_bias": btc_regime["bias"],
                "funding_label": funding_context["label"],
                "funding_rate": funding_context["rate"],
                "htf_context": "|".join(
                    item for item in [
                        breakout_4h["reason"] if breakout_4h else "",
                        trendline_4h["reason"] if trendline_4h else "",
                        retest_4h["reason"] if retest_4h else "",
                        base_breakout["pattern"] if base_breakout else "",
                        htf_reversal["pattern"] if htf_reversal else "",
                        divergence_signal["reason"] if divergence_signal else "",
                        order_block["pattern"] if order_block else "",
                        chart_pattern["pattern"] if chart_pattern else "",
                        fast_move_4h["reason"] if fast_move_4h else "",
                        acceleration_4h["reason"] if acceleration_4h else "",
                    ] if item
                ),
                "entry_context": "|".join(
                    item for item in [
                        breakout_confirmation["reason"] if breakout_confirmation else "",
                        trendline_confirmation["reason"] if trendline_confirmation else "",
                        retest_confirmation["reason"] if retest_confirmation else "",
                        base_entry["reason"] if base_entry else "",
                        reversal_entry["reason"] if reversal_entry else "",
                        order_block_entry["reason"] if order_block_entry else "",
                        chart_pattern_entry["reason"] if chart_pattern_entry else "",
                        fast_move["reason"] if fast_move else "",
                        acceleration["reason"] if acceleration else "",
                    ] if item
                ),
            }

            self.open_position(
                symbol=symbol,
                side=sig.side,
                entry_price=entry_price,
                score=sig.score,
                reason=sig.reason,
                candles=candles,
                signal_class=sig.signal_class,
                strategy_meta=strategy_meta,
                levels_candles=candles_1h,
            )

    def heartbeat(self):
        if self.market_feed is not None:
            self.market_feed.ensure_alive(MAX_SILENCE_SECONDS)
        self.sync_with_exchange_state()
        can_trade, risk_reason = self.risk_guard.can_open_new_position(self.balance)
        log_cyan(
            f"heartbeat | balance={self.balance:.2f} | open={self.count_open_positions()}/{MAX_OPEN_POSITIONS} | "
            f"mode={EXECUTION_MODE} | feed_ready={self.feed_health.feed_ready(self.market_feed)} | "
            f"trading_enabled={can_trade}{'' if can_trade else f' | reason={risk_reason}'}"
        )
        if time.time() - self.last_open_positions_report >= OPEN_POSITIONS_REPORT_SECONDS:
            report_text = self.build_open_positions_report()
            self.print_open_positions()
            self.notifier.send(report_text)
            self.last_open_positions_report = time.time()
        self.save_runtime_state()

    def run(self):
        log("Smart Momentum Paper Bot started")

        while True:
            try:
                self.update_symbols()

                for symbol in self.symbols:
                    try:
                        self.analyze_symbol(symbol)
                    except Exception as e:
                        log_red(f"ERROR {symbol}: {e}")

                if time.time() - self.last_heartbeat > HEARTBEAT_SECONDS:
                    self.heartbeat()
                    self.last_heartbeat = time.time()

                time.sleep(SCAN_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                log_red(f"GLOBAL ERROR: {e}")
                self.notifier.send(f"❌ GLOBAL ERROR: {e}")
                time.sleep(10)


if __name__ == "__main__":
    bot = SmartMomentumPaperBot()
    bot.run()

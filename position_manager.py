from config import (
    LEVERAGE_MODE,
    FIXED_LEVERAGE,
    MAX_ALLOWED_LEVERAGE,
    CORE_MAX_RISK_PCT,
    ALT_MAX_RISK_PCT,
    LOW_CAP_MAX_RISK_PCT,
)
from entry_filters import signal_size_multiplier
from levels import calculate_sl_tp_from_levels
from volatility_regime import atr_pct


class PositionManager:
    def __init__(self, entry_pct=0.03):
        self.entry_pct = entry_pct
    
    def dynamic_leverage(self, score):
        score = max(0.30, min(score, 1.00))
        
        min_score = 0.30
        max_score = 1.00
        
        min_lev = 10
        max_lev = 50
        
        ratio = (score - min_score) / (max_score - min_score)
        lev = min_lev + ratio * (max_lev - min_lev)
        
        lev = int(round(lev))
        
        # ограничение максимального плеча
        lev = min(lev, MAX_ALLOWED_LEVERAGE)
        
        return lev

    def _max_leverage_for_context(self, symbol_profile, signal_class):
        if symbol_profile == "CORE":
            caps = {"strong": 35, "B": 28, "C": 20, "reject": 12}
        elif symbol_profile == "LOW_CAP":
            caps = {"strong": 16, "B": 12, "C": 8, "reject": 5}
        else:
            caps = {"strong": 26, "B": 20, "C": 14, "reject": 9}

        if signal_class in {"A", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"}:
            return caps["strong"]
        if signal_class == "B":
            return caps["B"]
        if signal_class == "C":
            return caps["C"]
        return caps["reject"]

    def get_leverage(self, score, symbol_profile="ALT", signal_class="REJECT"):
        if LEVERAGE_MODE == "fixed":
            return min(FIXED_LEVERAGE, MAX_ALLOWED_LEVERAGE, self._max_leverage_for_context(symbol_profile, signal_class))
        return min(
            self.dynamic_leverage(score),
            MAX_ALLOWED_LEVERAGE,
            self._max_leverage_for_context(symbol_profile, signal_class),
        )

    def _risk_cap_pct(self, symbol_profile, signal_class):
        if symbol_profile == "CORE":
            return CORE_MAX_RISK_PCT
        if symbol_profile == "LOW_CAP":
            return LOW_CAP_MAX_RISK_PCT
        return ALT_MAX_RISK_PCT

    def build_position(
        self,
        balance,
        side,
        price,
        sl_pct,
        tp_pct,
        score,
        candles=None,
        signal_class="REJECT",
        levels_candles=None,
        symbol_profile="ALT",
    ):
        lev = self.get_leverage(score, symbol_profile=symbol_profile, signal_class=signal_class)

        size_mult = signal_size_multiplier(score, signal_class=signal_class)
        margin = balance * self.entry_pct
        notional = margin * lev
        qty = notional / price if price else 0.0

        level_data = None

        level_source_candles = levels_candles if levels_candles else candles

        if level_source_candles:
            strict_level_data = calculate_sl_tp_from_levels(
                side=side,
                entry_price=price,
                candles=level_source_candles,
                fallback_sl_pct=sl_pct,
                fallback_tp_pct=tp_pct,
                level_buffer_pct=0.001,
                min_rr=1.35,
            )
            soft_level_data = calculate_sl_tp_from_levels(
                side=side,
                entry_price=price,
                candles=level_source_candles,
                fallback_sl_pct=sl_pct,
                fallback_tp_pct=tp_pct,
                level_buffer_pct=0.0015,
                min_rr=1.05,
            )
            level_candidates = [strict_level_data, soft_level_data]

            if (
                strict_level_data.get("source") != "levels"
                and candles
                and level_source_candles is not candles
            ):
                retry_level_data = calculate_sl_tp_from_levels(
                    side=side,
                    entry_price=price,
                    candles=candles,
                    fallback_sl_pct=sl_pct,
                    fallback_tp_pct=tp_pct,
                    level_buffer_pct=0.002,
                    min_rr=0.95,
                )
                level_candidates.append(retry_level_data)

            structural_candidates = [item for item in level_candidates if item and item.get("source") == "levels"]
            if structural_candidates:
                level_data = max(structural_candidates, key=lambda item: float(item.get("rr", 0.0) or 0.0))
            else:
                fallback_candidates = [item for item in level_candidates if item]
                level_data = max(fallback_candidates, key=lambda item: float(item.get("rr", 0.0) or 0.0))

            stop = level_data["stop"]
            take = level_data.get("tp2") or level_data["take"]
        else:
            if side == "BUY":
                stop = price * (1 - sl_pct)
                take = price * (1 + tp_pct)
            else:
                stop = price * (1 + sl_pct)
                take = price * (1 - tp_pct)

        if side == "BUY":
            risk_usdt = max(0.0, (price - stop) * qty)
        else:
            risk_usdt = max(0.0, (stop - price) * qty)

        max_risk_usdt = balance * self._risk_cap_pct(symbol_profile, signal_class)
        if risk_usdt > max_risk_usdt and risk_usdt > 0:
            risk_ratio = max_risk_usdt / risk_usdt
            qty *= risk_ratio
            margin *= risk_ratio
            notional *= risk_ratio
            risk_usdt = max_risk_usdt

        return {
            "side": side,
            "entry": price,
            "qty": qty,
            "stop": stop,
            "take": take,
            "tp1": level_data.get("tp1", take) if level_data else take,
            "tp2": level_data.get("tp2", take) if level_data else take,
            "leverage": lev,
            "margin": margin,
            "notional": notional,
            "risk_usdt": risk_usdt,
            "level_data": level_data,
            "liquidity_target": level_data.get("liquidity_target") if level_data else None,
            "atr_pct": atr_pct(candles, period=14) if candles else 0.0,
            "signal_class": signal_class,
            "be_moved": False,
            "partial_done": False,
            "trail_active": False,
        }

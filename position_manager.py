from config import LEVERAGE_MODE, FIXED_LEVERAGE, MAX_ALLOWED_LEVERAGE
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

    def get_leverage(self, score):
        if LEVERAGE_MODE == "fixed":
            return FIXED_LEVERAGE
        return self.dynamic_leverage(score)

    def build_position(self, balance, side, price, sl_pct, tp_pct, score, candles=None, signal_class="REJECT"):
        lev = self.get_leverage(score)

        size_mult = signal_size_multiplier(score)
        margin = balance * self.entry_pct * size_mult
        notional = margin * lev
        qty = notional / price if price else 0.0

        level_data = None

        if candles:
            strict_level_data = calculate_sl_tp_from_levels(
                side=side,
                entry_price=price,
                candles=candles,
                fallback_sl_pct=sl_pct,
                fallback_tp_pct=tp_pct,
                level_buffer_pct=0.001,
                min_rr=1.35,
            )
            soft_level_data = calculate_sl_tp_from_levels(
                side=side,
                entry_price=price,
                candles=candles,
                fallback_sl_pct=sl_pct,
                fallback_tp_pct=tp_pct,
                level_buffer_pct=0.0015,
                min_rr=1.05,
            )
            level_data = strict_level_data if strict_level_data.get("source") == "levels" else soft_level_data

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

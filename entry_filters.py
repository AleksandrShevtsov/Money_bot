from config import (
    STRONG_SIGNAL_SIZE_MULT,
    B_SIGNAL_SIZE_MULT,
    C_SIGNAL_SIZE_MULT,
    REJECT_SIGNAL_SIZE_MULT,
)


def detect_recent_move_pct(candles, lookback=5):
    if candles is None or len(candles) < lookback:
        return 0.0

    start_price = candles[-lookback]["close"]
    end_price = candles[-1]["close"]

    if start_price <= 0:
        return 0.0

    return (end_price - start_price) / start_price


def blocked_by_anti_fomo(candles, side, lookback=5, max_move_pct=0.025):
    move_pct = detect_recent_move_pct(candles, lookback=lookback)

    if side == "BUY" and move_pct >= max_move_pct:
        return True, move_pct

    if side == "SELL" and move_pct <= -max_move_pct:
        return True, move_pct

    return False, move_pct


def signal_size_multiplier(score, signal_class="REJECT"):
    if signal_class in {"A", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"}:
        return STRONG_SIGNAL_SIZE_MULT
    if signal_class == "B":
        return B_SIGNAL_SIZE_MULT
    if signal_class == "C":
        return C_SIGNAL_SIZE_MULT
    if signal_class == "REJECT":
        return REJECT_SIGNAL_SIZE_MULT

    if score >= 0.80:
        return STRONG_SIGNAL_SIZE_MULT
    if score >= 0.55:
        return B_SIGNAL_SIZE_MULT
    return C_SIGNAL_SIZE_MULT

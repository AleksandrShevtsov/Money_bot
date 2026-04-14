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


def signal_size_multiplier(score):
    if score >= 0.80:
        return 1.0
    if score >= 0.55:
        return 0.75
    return 0.5
def _candle_body_pct(candle):
    open_price = float(candle["open"])
    close_price = float(candle["close"])
    if open_price <= 0:
        return 0.0
    return abs(close_price - open_price) / open_price


def blocked_by_target_distance(candles, side, min_room_pct=0.012, lookback=120):
    if not candles or side not in {"BUY", "SELL"}:
        return False, 0.0

    recent = candles[-lookback:] if len(candles) > lookback else candles
    last = float(recent[-1]["close"])
    if last <= 0:
        return False, 0.0

    if side == "BUY":
        resistances = [float(c["high"]) for c in recent[:-1] if float(c["high"]) > last]
        if not resistances:
            return False, 0.0
        nearest = min(resistances)
        room_pct = (nearest - last) / last
        return room_pct < min_room_pct, room_pct

    supports = [float(c["low"]) for c in recent[:-1] if float(c["low"]) < last]
    if not supports:
        return False, 0.0
    nearest = max(supports)
    room_pct = (last - nearest) / last
    return room_pct < min_room_pct, room_pct


def base_compression_score(candles, lookback=24):
    if not candles or len(candles) < lookback:
        return 0.0

    window = candles[-lookback:]
    highs = [float(c["high"]) for c in window]
    lows = [float(c["low"]) for c in window]
    closes = [float(c["close"]) for c in window]
    mean_close = sum(closes) / len(closes)
    if mean_close <= 0:
        return 0.0

    width_pct = (max(highs) - min(lows)) / mean_close
    last_body = _candle_body_pct(window[-1])
    avg_body = sum(_candle_body_pct(c) for c in window) / len(window)

    # Чем уже база и меньше тело последней свечи, тем выше качество сжатия.
    compression = max(0.0, 1.0 - min(1.0, width_pct / 0.06))
    smoothness = max(0.0, 1.0 - min(1.0, last_body / max(1e-9, avg_body * 1.8)))
    return round(0.65 * compression + 0.35 * smoothness, 3)


def blocked_after_impulse(candles, side, strong_body_pct=0.006, streak=3):
    if side not in {"BUY", "SELL"} or len(candles) < streak:
        return False, 0

    seq = candles[-streak:]
    passed = 0
    for c in seq:
        open_price = float(c["open"])
        close_price = float(c["close"])
        if open_price <= 0:
            continue
        body_pct = abs(close_price - open_price) / open_price
        if side == "BUY" and close_price > open_price and body_pct >= strong_body_pct:
            passed += 1
        elif side == "SELL" and close_price < open_price and body_pct >= strong_body_pct:
            passed += 1
    return passed == streak, passed


def atr_pct(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    true_ranges = []
    sample = candles[-(period + 1):]
    for i in range(1, len(sample)):
        high = float(sample[i]["high"])
        low = float(sample[i]["low"])
        prev_close = float(sample[i - 1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    atr = sum(true_ranges) / len(true_ranges)
    last_close = float(sample[-1]["close"])
    if last_close <= 0:
        return 0.0
    return atr / last_close


def blocked_by_atr_band(candles, min_atr_pct=0.002, max_atr_pct=0.03):
    value = atr_pct(candles)
    if value <= 0:
        return False, value
    return value < min_atr_pct or value > max_atr_pct, value


def blocked_by_huge_last_candle(candles, side, ratio_threshold=2.2, lookback=20):
    if side not in {"BUY", "SELL"} or len(candles) < lookback + 1:
        return False, 0.0

    window = candles[-(lookback + 1):-1]
    last = candles[-1]

    avg_range = sum(float(c["high"]) - float(c["low"]) for c in window) / len(window)
    last_range = float(last["high"]) - float(last["low"])
    if avg_range <= 0:
        return False, 0.0

    direction_match = (side == "BUY" and float(last["close"]) > float(last["open"])) or (
        side == "SELL" and float(last["close"]) < float(last["open"])
    )
    ratio = last_range / avg_range
    return direction_match and ratio >= ratio_threshold, ratio

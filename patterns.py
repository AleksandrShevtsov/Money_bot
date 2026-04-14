def detect_double_bottom(candles):
    if len(candles) < 30:
        return None

    last_lows = sorted(candles[-20:], key=lambda x: x["low"])[:2]
    diff = abs(last_lows[0]["low"] - last_lows[1]["low"]) / last_lows[0]["low"]

    if diff < 0.01:
        return {"direction": "BUY", "pattern": "double_bottom", "strength": 0.7}

    return None


def detect_double_top(candles):
    if len(candles) < 30:
        return None

    last_highs = sorted(candles[-20:], key=lambda x: x["high"], reverse=True)[:2]
    diff = abs(last_highs[0]["high"] - last_highs[1]["high"]) / last_highs[0]["high"]

    if diff < 0.01:
        return {"direction": "SELL", "pattern": "double_top", "strength": 0.7}

    return None


def detect_best_pattern(candles):
    for fn in (detect_double_bottom, detect_double_top):
        p = fn(candles)
        if p:
            return p
    return None

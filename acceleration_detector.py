def detect_price_acceleration(candles, window=4):
    if len(candles) < window:
        return None

    closes = [c["close"] for c in candles[-window:]]

    diffs = [closes[i] - closes[i-1] for i in range(1, len(closes))]

    if all(d > 0 for d in diffs):
        return {"direction": "BUY", "reason": "price_acceleration_up"}

    if all(d < 0 for d in diffs):
        return {"direction": "SELL", "reason": "price_acceleration_down"}

    return None
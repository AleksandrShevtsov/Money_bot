def detect_fast_move(candles, window=5, threshold=0.01):
    if len(candles) < window:
        return None

    start = candles[-window]["close"]
    end = candles[-1]["close"]

    change = (end - start) / start

    if change > threshold:
        return {"direction": "BUY", "reason": "fast_up_move"}

    if change < -threshold:
        return {"direction": "SELL", "reason": "fast_down_move"}

    return None
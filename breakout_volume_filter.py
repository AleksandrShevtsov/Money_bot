def breakout_volume_confirms(candles, volume_multiplier=1.0, avg_lookback=15):
    if candles is None or len(candles) < avg_lookback + 1:
        return False, 0.0, 0.0

    last = candles[-1]["volume"]
    avg = sum(c["volume"] for c in candles[-avg_lookback-1:-1]) / avg_lookback

    if avg <= 0:
        return False, last, avg

    confirmed = last >= avg * volume_multiplier
    return confirmed, last, avg

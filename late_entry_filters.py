def is_low_price_coin(price, threshold=0.10):
    return price < threshold


def extension_from_local_low(candles, lookback=12):
    if candles is None or len(candles) < lookback:
        return 0.0

    local_low = min(c["low"] for c in candles[-lookback:])
    current_price = candles[-1]["close"]

    if local_low <= 0:
        return 0.0

    return (current_price - local_low) / local_low


def extension_from_local_high(candles, lookback=12):
    if candles is None or len(candles) < lookback:
        return 0.0

    local_high = max(c["high"] for c in candles[-lookback:])
    current_price = candles[-1]["close"]

    if local_high <= 0:
        return 0.0

    return (local_high - current_price) / local_high


def blocked_by_extension(
    candles,
    side,
    lookback=12,
    max_ext_low_pct=0.08,
    max_ext_high_pct=0.08,
):
    if not candles:
        return False, 0.0

    if side == "BUY":
        ext = extension_from_local_low(candles, lookback=lookback)
        return ext >= max_ext_low_pct, ext

    if side == "SELL":
        ext = extension_from_local_high(candles, lookback=lookback)
        return ext >= max_ext_high_pct, ext

    return False, 0.0
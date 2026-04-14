def true_range(current, prev_close):
    high = current["high"]
    low = current["low"]
    if prev_close is None:
        return high - low
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def average_true_range(candles, period=14):
    if candles is None or len(candles) < period + 1:
        return 0.0

    ranges = []
    prev_close = candles[-period - 1]["close"]
    for candle in candles[-period:]:
        ranges.append(true_range(candle, prev_close))
        prev_close = candle["close"]

    if not ranges:
        return 0.0
    return sum(ranges) / len(ranges)


def atr_pct(candles, period=14):
    if not candles:
        return 0.0
    atr = average_true_range(candles, period=period)
    close = candles[-1]["close"]
    if close <= 0:
        return 0.0
    return atr / close


def realized_volatility(candles, lookback=20):
    if candles is None or len(candles) < lookback + 1:
        return 0.0

    closes = [c["close"] for c in candles[-lookback - 1:]]
    returns = []
    for i in range(1, len(closes)):
        prev_close = closes[i - 1]
        if prev_close <= 0:
            continue
        returns.append((closes[i] - prev_close) / prev_close)

    if not returns:
        return 0.0

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    return variance ** 0.5


def adaptive_threshold(base_value, atr_ratio, floor_mult=0.65, cap_mult=1.75, baseline_atr=0.012):
    if atr_ratio <= 0:
        return base_value

    mult = atr_ratio / baseline_atr
    if mult < floor_mult:
        mult = floor_mult
    if mult > cap_mult:
        mult = cap_mult
    return base_value * mult


def market_regime(candles):
    if candles is None or len(candles) < 30:
        return {"name": "unknown", "atr_pct": 0.0, "realized_vol": 0.0}

    atr_ratio = atr_pct(candles, period=14)
    realized_vol = realized_volatility(candles, lookback=20)

    closes = [c["close"] for c in candles[-20:]]
    first = closes[0]
    last = closes[-1]
    drift = abs(last - first) / max(first, 1e-9)

    highs = [c["high"] for c in candles[-20:]]
    lows = [c["low"] for c in candles[-20:]]
    width = (max(highs) - min(lows)) / max(min(lows), 1e-9)

    if atr_ratio >= 0.035 or realized_vol >= 0.03:
        name = "high_volatility_panic"
    elif drift >= width * 0.55 and atr_ratio >= 0.012:
        name = "trend_day"
    elif atr_ratio <= 0.009 and width <= 0.025:
        name = "squeeze"
    else:
        name = "range_day"

    return {
        "name": name,
        "atr_pct": atr_ratio,
        "realized_vol": realized_vol,
        "drift": drift,
        "width": width,
    }

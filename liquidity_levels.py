def _price_bins(low, high, bins):
    if bins <= 0:
        return []

    if high <= low:
        return []

    step = (high - low) / bins
    return [low + step * i for i in range(bins + 1)]


def build_volume_profile(candles, bins=24, lookback=80):
    if candles is None or len(candles) < 10:
        return {"bins": [], "hvn": [], "lvn": []}

    window = candles[-lookback:]
    low = min(c["low"] for c in window)
    high = max(c["high"] for c in window)

    if high <= low:
        return {"bins": [], "hvn": [], "lvn": []}

    edges = _price_bins(low, high, bins)

    if not edges or len(edges) != bins + 1:
        return {"bins": [], "hvn": [], "lvn": []}

    volumes = [0.0 for _ in range(bins)]

    for candle in window:
        typical_price = (candle["high"] + candle["low"] + candle["close"]) / 3
        volume = candle.get("volume", 0.0)

        for idx in range(bins):
            left = edges[idx]
            right = edges[idx + 1]

            if idx == bins - 1:
                fits = left <= typical_price <= right
            else:
                fits = left <= typical_price < right

            if fits:
                volumes[idx] += volume
                break

    if not volumes:
        return {"bins": [], "hvn": [], "lvn": []}

    avg_volume = sum(volumes) / len(volumes)
    bin_centers = [(edges[i] + edges[i + 1]) / 2 for i in range(bins)]

    hvn = [bin_centers[i] for i, v in enumerate(volumes) if v >= avg_volume * 1.35]
    lvn = [bin_centers[i] for i, v in enumerate(volumes) if v <= avg_volume * 0.65]

    return {"bins": list(zip(bin_centers, volumes)), "hvn": hvn, "lvn": lvn}


def nearest_level(price, levels, side):
    if not levels:
        return None

    if side == "BUY":
        above = [lvl for lvl in levels if lvl > price]
        return min(above) if above else None

    below = [lvl for lvl in levels if lvl < price]
    return max(below) if below else None


def detect_liquidity_sweep(candles, lookback=20, wick_threshold_mult=1.2):
    if candles is None or len(candles) < lookback + 2:
        return None

    window = candles[-lookback - 1:-1]
    last = candles[-1]

    prior_high = max(c["high"] for c in window)
    prior_low = min(c["low"] for c in window)

    body = abs(last["close"] - last["open"])
    upper_wick = last["high"] - max(last["close"], last["open"])
    lower_wick = min(last["close"], last["open"]) - last["low"]

    if last["high"] > prior_high and last["close"] < prior_high and upper_wick > body * wick_threshold_mult:
        return {
            "direction": "SELL",
            "reason": "liquidity_sweep_high_reject",
            "swept_level": prior_high,
        }

    if last["low"] < prior_low and last["close"] > prior_low and lower_wick > body * wick_threshold_mult:
        return {
            "direction": "BUY",
            "reason": "liquidity_sweep_low_reclaim",
            "swept_level": prior_low,
        }

    return None


def is_false_breakout(candles, breakout, tolerance_pct=0.0007):
    if candles is None or len(candles) < 2 or breakout is None:
        return False

    last = candles[-1]
    level = breakout.get("breakout_level", breakout.get("trendline_price"))

    if level is None:
        return False

    if breakout["direction"] == "BUY":
        return last["close"] < level * (1 - tolerance_pct)

    return last["close"] > level * (1 + tolerance_pct)

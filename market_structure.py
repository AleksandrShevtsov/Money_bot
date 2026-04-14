def detect_market_structure(candles, lookback=12):
    if candles is None or len(candles) < lookback:
        return {
            "trend": "unknown",
            "higher_high": False,
            "higher_low": False,
            "lower_high": False,
            "lower_low": False,
            "reason": "not_enough_candles",
        }

    window = candles[-lookback:]
    highs = [c["high"] for c in window]
    lows = [c["low"] for c in window]

    recent_high = max(highs[-4:])
    previous_high = max(highs[:-4])

    recent_low = min(lows[-4:])
    previous_low = min(lows[:-4])

    higher_high = recent_high > previous_high
    higher_low = recent_low > previous_low
    lower_high = recent_high < previous_high
    lower_low = recent_low < previous_low

    if higher_high and higher_low:
        trend = "bullish_structure"
    elif lower_high and lower_low:
        trend = "bearish_structure"
    else:
        trend = "mixed_structure"

    return {
        "trend": trend,
        "higher_high": higher_high,
        "higher_low": higher_low,
        "lower_high": lower_high,
        "lower_low": lower_low,
        "reason": trend,
    }


def structure_allows_side(structure_data, side):
    if not structure_data:
        return False

    trend = structure_data.get("trend", "unknown")

    if side == "BUY":
        return trend == "bullish_structure"

    if side == "SELL":
        return trend == "bearish_structure"

    return False

def detect_retest_after_breakout(candles, breakout_data, tolerance_pct=0.0015):
    """
    Ищет retest после уже найденного breakout.
    breakout_data должен содержать:
    {
        "direction": "BUY" | "SELL",
        "trendline_price": ... или "breakout_level": ...,
        "entry_price": ...
    }

    Возвращает:
    {
        "direction": ...,
        "entry_price": ...,
        "reason": "retest_after_breakout_buy" | "retest_after_breakout_sell",
        "retest_level": ...
    }
    или None
    """
    if not candles or len(candles) < 3 or not breakout_data:
        return None

    last = candles[-1]
    prev = candles[-2]

    level = breakout_data.get("trendline_price")
    if level is None:
        level = breakout_data.get("breakout_level")
    if level is None:
        return None

    direction = breakout_data.get("direction")
    if direction == "BUY":
        # Цена должна вернуться к уровню и закрыться снова выше
        touched = last["low"] <= level * (1 + tolerance_pct)
        reclaimed = last["close"] > level
        prev_confirmed = prev["close"] > level
        if touched and reclaimed and prev_confirmed:
            entry_price = (last["open"] + last["close"]) / 2
            return {
                "direction": "BUY",
                "entry_price": entry_price,
                "reason": "retest_after_breakout_buy",
                "retest_level": level,
            }

    if direction == "SELL":
        # Цена должна вернуться к уровню и закрыться снова ниже
        touched = last["high"] >= level * (1 - tolerance_pct)
        rejected = last["close"] < level
        prev_confirmed = prev["close"] < level
        if touched and rejected and prev_confirmed:
            entry_price = (last["open"] + last["close"]) / 2
            return {
                "direction": "SELL",
                "entry_price": entry_price,
                "reason": "retest_after_breakout_sell",
                "retest_level": level,
            }

    return None

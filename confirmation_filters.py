def multi_bar_breakout_confirmation(candles, breakout, bars=2, tolerance_pct=0.0008):
    if candles is None or len(candles) < bars + 1 or breakout is None:
        return False

    level = breakout.get("breakout_level", breakout.get("trendline_price"))
    if level is None:
        return False

    recent = candles[-bars:]
    if breakout["direction"] == "BUY":
        return all(c["close"] > level * (1 - tolerance_pct) for c in recent)

    return all(c["close"] < level * (1 + tolerance_pct) for c in recent)

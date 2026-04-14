def detect_range_breakout(candles, lookback=20, breakout_pct=0.0015):
    """
    Ищет выход из диапазона (флэта).

    candles: список словарей вида
    {open, high, low, close}

    Возвращает:
    {
        direction: BUY / SELL
        breakout_level: float
        range_high: float
        range_low: float
        reason: str
    }
    либо None
    """

    if candles is None or len(candles) < lookback + 2:
        return None

    window = candles[-lookback-1:-1]
    last = candles[-1]

    range_high = max(c["high"] for c in window)
    range_low = min(c["low"] for c in window)

    last_close = last["close"]

    width = (range_high - range_low) / max(range_low, 1e-9)

    # фильтр: слишком широкий диапазон — это не флэт
    if width > 0.02:
        return None

    if last_close > range_high * (1 + breakout_pct):
        return {
            "direction": "BUY",
            "breakout_level": range_high,
            "range_high": range_high,
            "range_low": range_low,
            "reason": "range_breakout_up",
        }

    if last_close < range_low * (1 - breakout_pct):
        return {
            "direction": "SELL",
            "breakout_level": range_low,
            "range_high": range_high,
            "range_low": range_low,
            "reason": "range_breakout_down",
        }

    return None


def count_recent_large_trades(trades, side=None):
    filtered = [t for t in trades if t.get("is_large")]

    if side:
        filtered = [t for t in filtered if t.get("side") == side]

    return len(filtered)


def confirm_breakout_with_orderflow(
    trades,
    imbalance,
    oi_now,
    oi_prev,
    breakout,
):
    """
    Подтверждает breakout через:
    - стакан
    - крупные сделки
    - open interest
    """

    if breakout is None:
        return None

    direction = breakout["direction"]

    large_buy = count_recent_large_trades(trades, "buy")
    large_sell = count_recent_large_trades(trades, "sell")

    oi_up = bool(oi_now and oi_prev and oi_now > oi_prev * 1.001)

    if direction == "BUY":
        if imbalance > 0.20 and large_buy >= 2 and oi_up:
            return {
                "direction": "BUY",
                "reason": "confirmed_range_breakout_up",
                "large_buy": large_buy,
                "large_sell": large_sell,
                "imbalance": imbalance,
                "oi_up": oi_up,
                "breakout_level": breakout["breakout_level"],
            }

    if direction == "SELL":
        if imbalance < -0.20 and large_sell >= 2 and oi_up:
            return {
                "direction": "SELL",
                "reason": "confirmed_range_breakout_down",
                "large_buy": large_buy,
                "large_sell": large_sell,
                "imbalance": imbalance,
                "oi_up": oi_up,
                "breakout_level": breakout["breakout_level"],
            }

    return None

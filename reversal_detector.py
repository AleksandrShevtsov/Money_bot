from __future__ import annotations


def _midpoint(candle: dict) -> float:
    return (float(candle["open"]) + float(candle["close"])) / 2.0


def _avg_volume(candles: list[dict]) -> float:
    vols = [float(c.get("volume", 0.0)) for c in candles if c.get("volume") is not None]
    if not vols:
        return 0.0
    return sum(vols) / len(vols)


def _body(candle: dict) -> float:
    return abs(float(candle["close"]) - float(candle["open"]))


def _range(candle: dict) -> float:
    return max(float(candle["high"]) - float(candle["low"]), 1e-9)


def _lower_wick(candle: dict) -> float:
    lo = float(candle["low"])
    o = float(candle["open"])
    c = float(candle["close"])
    return min(o, c) - lo


def _upper_wick(candle: dict) -> float:
    hi = float(candle["high"])
    o = float(candle["open"])
    c = float(candle["close"])
    return hi - max(o, c)


def _trend_drop_pct(candles: list[dict], lookback: int) -> float:
    segment = candles[-lookback:]
    first = float(segment[0]["close"])
    last = float(segment[-1]["close"])
    return (first - last) / max(first, 1e-9)


def _trend_rise_pct(candles: list[dict], lookback: int) -> float:
    segment = candles[-lookback:]
    first = float(segment[0]["close"])
    last = float(segment[-1]["close"])
    return (last - first) / max(first, 1e-9)


def detect_htf_reversal(candles, volume_mult=1.3, lookback=20):
    if candles is None or len(candles) < max(lookback + 2, 25):
        return None

    last = candles[-1]
    prev = candles[-2]
    ref_window = candles[-lookback - 1 : -1]

    avg_vol = _avg_volume(ref_window)
    last_vol = float(last.get("volume", 0.0))
    vol_ok = avg_vol <= 0.0 or last_vol >= avg_vol * volume_mult

    last_close = float(last["close"])
    last_open = float(last["open"])
    last_high = float(last["high"])
    last_low = float(last["low"])

    lower_wick = _lower_wick(last)
    upper_wick = _upper_wick(last)
    body = _body(last)
    full_range = _range(last)

    drop_pct = _trend_drop_pct(candles[:-1], min(lookback, len(candles) - 1))
    rise_pct = _trend_rise_pct(candles[:-1], min(lookback, len(candles) - 1))

    window_lows = min(float(c["low"]) for c in ref_window)
    window_highs = max(float(c["high"]) for c in ref_window)
    midpoint = _midpoint(last)

    # 1) Capitulation reversal BUY
    reclaim_part = (last_close - last_low) / max(full_range, 1e-9)
    if (
        drop_pct >= 0.06
        and lower_wick >= body * 1.4
        and reclaim_part >= 0.55
        and vol_ok
        and last_close > float(prev["close"])
    ):
        strength = min(0.96, 0.70 + drop_pct + max(0.0, reclaim_part - 0.55) * 0.5)
        return {
            "direction": "BUY",
            "pattern": "capitulation_reversal_buy",
            "entry_price": midpoint,
            "strength": round(max(0.85, strength), 3),
            "reason": "capitulation_reversal_buy",
            "reversal_level": window_lows,
            "anchor_price": last_low,
        }

    # 2) Exhaustion reversal SELL
    rejection_part = (last_high - last_close) / max(full_range, 1e-9)
    if (
        rise_pct >= 0.06
        and upper_wick >= body * 1.4
        and rejection_part >= 0.55
        and vol_ok
        and last_close < float(prev["close"])
    ):
        strength = min(0.96, 0.70 + rise_pct + max(0.0, rejection_part - 0.55) * 0.5)
        return {
            "direction": "SELL",
            "pattern": "exhaustion_reversal_sell",
            "entry_price": midpoint,
            "strength": round(max(0.85, strength), 3),
            "reason": "exhaustion_reversal_sell",
            "reversal_level": window_highs,
            "anchor_price": last_high,
        }

    # 3) Reclaim after breakdown BUY
    broke_down = float(prev["close"]) < window_lows
    reclaimed = last_close > window_lows and last_open <= window_lows * 1.002
    if broke_down and reclaimed and vol_ok and last_close > float(prev["high"]):
        return {
            "direction": "BUY",
            "pattern": "reclaim_after_breakdown_buy",
            "entry_price": midpoint,
            "strength": 0.83,
            "reason": "reclaim_after_breakdown_buy",
            "reversal_level": window_lows,
            "anchor_price": last_low,
        }

    # 4) Rejection after false breakout SELL
    false_break = float(prev["close"]) > window_highs
    rejected = last_close < window_highs and last_open >= window_highs * 0.998
    if false_break and rejected and vol_ok and upper_wick >= body:
        return {
            "direction": "SELL",
            "pattern": "rejection_after_false_breakout_sell",
            "entry_price": midpoint,
            "strength": 0.83,
            "reason": "rejection_after_false_breakout_sell",
            "reversal_level": window_highs,
            "anchor_price": last_high,
        }

    return None


def confirm_reversal_entry_15m(candles_15m, reversal_signal):
    if not candles_15m or len(candles_15m) < 4 or not reversal_signal:
        return None

    direction = reversal_signal.get("direction")
    level = float(reversal_signal.get("reversal_level", reversal_signal.get("entry_price", 0.0)))

    last = candles_15m[-1]
    prev = candles_15m[-2]
    prev2 = candles_15m[-3]

    last_open = float(last["open"])
    last_close = float(last["close"])
    last_high = float(last["high"])
    last_low = float(last["low"])

    impulse_range = _range(last)
    avg_recent_range = sum(_range(c) for c in candles_15m[-8:-1]) / max(len(candles_15m[-8:-1]), 1)
    is_impulse = impulse_range >= avg_recent_range * 1.15

    if direction == "BUY":
        # breakout in direction
        if last_close > max(float(prev["high"]), float(prev2["high"])) and is_impulse:
            return {"entry_price": _midpoint(last), "reason": "reversal_15m_breakout_confirm_buy"}

        # retest after reversal candle
        touched = last_low <= level * 1.002
        reclaimed = last_close > level and float(prev["close"]) >= level * 0.998
        if touched and reclaimed:
            return {"entry_price": _midpoint(last), "reason": "reversal_15m_retest_confirm_buy"}

        # first impulse
        if last_close > last_open and is_impulse and float(prev["close"]) > float(prev["open"]):
            return {"entry_price": _midpoint(last), "reason": "reversal_15m_impulse_confirm_buy"}

    if direction == "SELL":
        if last_close < min(float(prev["low"]), float(prev2["low"])) and is_impulse:
            return {"entry_price": _midpoint(last), "reason": "reversal_15m_breakout_confirm_sell"}

        touched = last_high >= level * 0.998
        rejected = last_close < level and float(prev["close"]) <= level * 1.002
        if touched and rejected:
            return {"entry_price": _midpoint(last), "reason": "reversal_15m_retest_confirm_sell"}

        if last_close < last_open and is_impulse and float(prev["close"]) < float(prev["open"]):
            return {"entry_price": _midpoint(last), "reason": "reversal_15m_impulse_confirm_sell"}

    return None


def reversal_not_overextended(reversal_signal, current_price, max_reversal_extension_pct=0.18):
    if not reversal_signal or current_price is None:
        return False

    direction = reversal_signal.get("direction")
    anchor = float(reversal_signal.get("anchor_price", reversal_signal.get("entry_price", 0.0)))
    current_price = float(current_price)

    if anchor <= 0:
        return False

    if direction == "BUY":
        move_pct = (current_price - anchor) / anchor
        return move_pct <= max_reversal_extension_pct

    if direction == "SELL":
        move_pct = (anchor - current_price) / anchor
        return move_pct <= max_reversal_extension_pct

    return False

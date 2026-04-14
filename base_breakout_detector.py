from __future__ import annotations


def _midpoint(candle: dict) -> float:
    return (float(candle["open"]) + float(candle["close"])) / 2.0


def _avg_volume(candles: list[dict]) -> float:
    volumes = [float(c.get("volume", 0.0)) for c in candles if c.get("volume") is not None]
    if not volumes:
        return 0.0
    return sum(volumes) / len(volumes)


def _compression_score(base_high: float, base_low: float, close_price: float) -> float:
    width = (base_high - base_low) / max(base_low, 1e-9)
    close_location = abs(close_price - ((base_high + base_low) / 2.0)) / max((base_high - base_low), 1e-9)
    raw = 0.75 - (width * 0.9) - (close_location * 0.15)
    return max(0.25, min(0.75, raw))


def detect_base_breakout(candles, lookback=16, volume_mult=1.4, max_base_height_pct=0.30):
    if candles is None or len(candles) < max(lookback + 2, 12):
        return None

    best_signal = None
    for base_len in range(8, min(20, len(candles) - 1) + 1):
        if base_len > lookback:
            continue

        base_window = candles[-base_len - 1:-1]
        last = candles[-1]

        base_high = max(float(c["high"]) for c in base_window)
        base_low = min(float(c["low"]) for c in base_window)
        base_height_pct = (base_high - base_low) / max(base_low, 1e-9)
        if base_height_pct > max_base_height_pct:
            continue

        avg_volume = _avg_volume(base_window)
        last_volume = float(last.get("volume", 0.0))
        volume_ok = avg_volume <= 0.0 or last_volume >= avg_volume * volume_mult
        if not volume_ok:
            continue

        last_close = float(last["close"])
        last_high = float(last["high"])
        last_low = float(last["low"])
        entry_price = _midpoint(last)
        compression = _compression_score(base_high, base_low, last_close)
        volume_ratio = last_volume / max(avg_volume, 1e-9) if avg_volume > 0 else 1.0

        if last_close > base_high and last_high >= base_high:
            breakout_pct = (last_close - base_high) / max(base_high, 1e-9)
            strength = compression + (breakout_pct * 4.0) + max(0.0, volume_ratio - 1.0) * 0.15
            candidate = {
                "direction": "BUY",
                "pattern": "base_breakout_up",
                "base_high": base_high,
                "base_low": base_low,
                "entry_price": entry_price,
                "strength": round(max(0.45, min(0.95, strength)), 3),
                "reason": "base_breakout_up",
                "base_length": base_len,
            }
            if best_signal is None or candidate["strength"] > best_signal["strength"]:
                best_signal = candidate

        if last_close < base_low and last_low <= base_low:
            breakout_pct = (base_low - last_close) / max(base_low, 1e-9)
            strength = compression + (breakout_pct * 4.0) + max(0.0, volume_ratio - 1.0) * 0.15
            candidate = {
                "direction": "SELL",
                "pattern": "base_breakout_down",
                "base_high": base_high,
                "base_low": base_low,
                "entry_price": entry_price,
                "strength": round(max(0.45, min(0.95, strength)), 3),
                "reason": "base_breakout_down",
                "base_length": base_len,
            }
            if best_signal is None or candidate["strength"] > best_signal["strength"]:
                best_signal = candidate

    return best_signal


def confirm_base_breakout_entry_15m(candles_15m, base_signal):
    if not candles_15m or len(candles_15m) < 4 or not base_signal:
        return None

    direction = base_signal.get("direction")
    base_high = float(base_signal.get("base_high", 0.0))
    base_low = float(base_signal.get("base_low", 0.0))
    last = candles_15m[-1]
    prev = candles_15m[-2]
    prev2 = candles_15m[-3]
    entry_price = _midpoint(last)

    if direction == "BUY":
        breakout_hold = float(last["close"]) > max(float(prev["high"]), float(prev2["high"]))
        retest_hold = float(last["low"]) <= base_high * 1.002 and float(last["close"]) >= base_high
        first_impulse = float(last["close"]) > float(last["open"]) and float(prev["close"]) > float(prev["open"])
        if breakout_hold:
            return {"entry_price": entry_price, "reason": "base_breakout_15m_hold_buy"}
        if retest_hold:
            return {"entry_price": entry_price, "reason": "base_breakout_15m_retest_buy"}
        if first_impulse and float(last["close"]) > base_high:
            return {"entry_price": entry_price, "reason": "base_breakout_15m_impulse_buy"}

    if direction == "SELL":
        breakout_hold = float(last["close"]) < min(float(prev["low"]), float(prev2["low"]))
        retest_hold = float(last["high"]) >= base_low * 0.998 and float(last["close"]) <= base_low
        first_impulse = float(last["close"]) < float(last["open"]) and float(prev["close"]) < float(prev["open"])
        if breakout_hold:
            return {"entry_price": entry_price, "reason": "base_breakout_15m_hold_sell"}
        if retest_hold:
            return {"entry_price": entry_price, "reason": "base_breakout_15m_retest_sell"}
        if first_impulse and float(last["close"]) < base_low:
            return {"entry_price": entry_price, "reason": "base_breakout_15m_impulse_sell"}

    return None


def not_overextended_from_base(base_signal, current_price, max_move_from_base=0.20):
    if not base_signal or current_price is None:
        return False

    current_price = float(current_price)
    base_high = float(base_signal.get("base_high", 0.0))
    base_low = float(base_signal.get("base_low", 0.0))
    direction = base_signal.get("direction")

    if direction == "BUY":
        if base_high <= 0:
            return False
        move_pct = (current_price - base_high) / base_high
        return move_pct <= max_move_from_base

    if direction == "SELL":
        if base_low <= 0:
            return False
        move_pct = (base_low - current_price) / base_low
        return move_pct <= max_move_from_base

    return False

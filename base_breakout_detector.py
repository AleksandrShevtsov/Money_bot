from __future__ import annotations


def _midpoint(candle: dict) -> float:
    return (float(candle["open"]) + float(candle["close"])) / 2.0


def _avg_volume(candles: list[dict]) -> float:
    volumes = [float(c.get("volume", 0.0)) for c in candles if c.get("volume") is not None]
    if not volumes:
        return 0.0
    return sum(volumes) / len(volumes)


def detect_base_breakout(candles, lookback=16, volume_mult=1.4, max_base_height_pct=0.30):
    if candles is None or len(candles) < max(lookback + 2, 12):
        return None

    base_len = max(8, min(int(lookback), 20))
    if len(candles) < base_len + 2:
        return None

    base_window = candles[-base_len - 1 : -1]
    last = candles[-1]

    base_high = max(float(c["high"]) for c in base_window)
    base_low = min(float(c["low"]) for c in base_window)
    base_height_pct = (base_high - base_low) / max(base_low, 1e-9)
    if base_height_pct > max_base_height_pct:
        return None

    avg_volume = _avg_volume(base_window)
    last_volume = float(last.get("volume", 0.0))
    volume_ok = avg_volume <= 0.0 or last_volume >= avg_volume * volume_mult

    last_close = float(last["close"])
    last_high = float(last["high"])
    last_low = float(last["low"])
    entry_price = _midpoint(last)

    if last_close > base_high and last_high >= base_high and volume_ok:
        breakout_pct = (last_close - base_high) / max(base_high, 1e-9)
        strength = min(1.0, 0.45 + breakout_pct * 4.0 + (last_volume / max(avg_volume, 1e-9) - 1.0) * 0.15)
        return {
            "direction": "BUY",
            "pattern": "base_breakout_up",
            "base_high": base_high,
            "base_low": base_low,
            "entry_price": entry_price,
            "strength": round(max(strength, 0.40), 3),
            "reason": "base_breakout_up",
        }

    if last_close < base_low and last_low <= base_low and volume_ok:
        breakout_pct = (base_low - last_close) / max(base_low, 1e-9)
        strength = min(1.0, 0.45 + breakout_pct * 4.0 + (last_volume / max(avg_volume, 1e-9) - 1.0) * 0.15)
        return {
            "direction": "SELL",
            "pattern": "base_breakout_down",
            "base_high": base_high,
            "base_low": base_low,
            "entry_price": entry_price,
            "strength": round(max(strength, 0.40), 3),
            "reason": "base_breakout_down",
        }

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

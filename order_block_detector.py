from __future__ import annotations


def _midpoint(candle: dict) -> float:
    return (float(candle["open"]) + float(candle["close"])) / 2.0


def _body_pct(candle: dict) -> float:
    body = abs(float(candle["close"]) - float(candle["open"]))
    low = float(candle["low"])
    return body / max(low, 1e-9)


def detect_order_block(candles, lookback=30):
    if not candles or len(candles) < max(lookback, 12):
        return None

    window = candles[-lookback:]
    for idx in range(len(window) - 4, 1, -1):
        base_candle = window[idx]
        next_candle = window[idx + 1]
        third_candle = window[idx + 2]
        context = window[max(0, idx - 12):idx + 1]
        recent_high = max(float(c["high"]) for c in context)
        recent_low = min(float(c["low"]) for c in context)

        base_open = float(base_candle["open"])
        base_close = float(base_candle["close"])
        zone_low = min(base_open, base_close)
        zone_high = max(base_open, base_close)
        impulse_1 = _body_pct(next_candle)
        impulse_2 = _body_pct(third_candle)

        if base_close < base_open:
            broke_up = float(third_candle["close"]) > recent_high * 0.997
            if impulse_1 >= 0.008 and impulse_2 >= 0.006 and broke_up:
                return {
                    "direction": "BUY",
                    "pattern": "bullish_order_block",
                    "zone_low": zone_low,
                    "zone_high": zone_high,
                    "entry_price": zone_high,
                    "strength": 0.79,
                    "reason": "bullish_order_block",
                }

        if base_close > base_open:
            broke_down = float(third_candle["close"]) < recent_low * 1.003
            if impulse_1 >= 0.008 and impulse_2 >= 0.006 and broke_down:
                return {
                    "direction": "SELL",
                    "pattern": "bearish_order_block",
                    "zone_low": zone_low,
                    "zone_high": zone_high,
                    "entry_price": zone_low,
                    "strength": 0.79,
                    "reason": "bearish_order_block",
                }

    return None


def confirm_order_block_retest(candles, ob_signal):
    if not candles or len(candles) < 3 or not ob_signal:
        return None

    last = candles[-1]
    zone_low = float(ob_signal["zone_low"])
    zone_high = float(ob_signal["zone_high"])
    last_close = float(last["close"])
    last_high = float(last["high"])
    last_low = float(last["low"])

    if ob_signal["direction"] == "BUY":
        touched = last_low <= zone_high and last_high >= zone_low
        reclaimed = last_close >= zone_high
        if touched and reclaimed:
            return {"entry_price": _midpoint(last), "reason": "order_block_retest_buy"}

    if ob_signal["direction"] == "SELL":
        touched = last_high >= zone_low and last_low <= zone_high
        rejected = last_close <= zone_low
        if touched and rejected:
            return {"entry_price": _midpoint(last), "reason": "order_block_retest_sell"}

    return None

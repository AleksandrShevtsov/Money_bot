def classify_oi_price_context(candles, oi_now, oi_prev, lookback=4):
    if candles is None or len(candles) < lookback + 1:
        return {"bias": 0.0, "label": "no_context"}
    if oi_now is None or oi_prev is None or oi_prev <= 0:
        return {"bias": 0.0, "label": "oi_unavailable"}

    start_close = candles[-lookback - 1]["close"]
    end_close = candles[-1]["close"]
    price_change = (end_close - start_close) / max(start_close, 1e-9)
    oi_change = (oi_now - oi_prev) / max(oi_prev, 1e-9)

    if price_change > 0 and oi_change > 0:
        return {"bias": 0.22, "label": "long_build_up", "price_change": price_change, "oi_change": oi_change}
    if price_change < 0 and oi_change > 0:
        return {"bias": -0.22, "label": "short_build_up", "price_change": price_change, "oi_change": oi_change}
    if price_change > 0 and oi_change < 0:
        return {"bias": 0.08, "label": "short_covering", "price_change": price_change, "oi_change": oi_change}
    if price_change < 0 and oi_change < 0:
        return {"bias": -0.08, "label": "long_liquidation", "price_change": price_change, "oi_change": oi_change}

    return {"bias": 0.0, "label": "neutral_oi", "price_change": price_change, "oi_change": oi_change}

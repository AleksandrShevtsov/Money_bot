from __future__ import annotations


def _midpoint(candle: dict) -> float:
    return (float(candle["open"]) + float(candle["close"])) / 2.0


def _avg_volume(candles: list[dict]) -> float:
    values = [float(c.get("volume", 0.0)) for c in candles if c.get("volume") is not None]
    if not values:
        return 0.0
    return sum(values) / len(values)


def extract_pivots(candles, window=2, min_move_pct=0.008):
    if not candles or len(candles) < window * 2 + 5:
        return []

    pivots = []
    last_price = None
    for i in range(window, len(candles) - window):
        candle = candles[i]
        high = float(candle["high"])
        low = float(candle["low"])

        left = candles[i - window:i]
        right = candles[i + 1:i + 1 + window]

        is_high = all(high >= float(x["high"]) for x in left + right)
        is_low = all(low <= float(x["low"]) for x in left + right)

        if is_high:
            if last_price is None or abs(high - last_price) / max(last_price, 1e-9) >= min_move_pct:
                pivots.append({"index": i, "type": "high", "price": high})
                last_price = high

        if is_low:
            if last_price is None or abs(low - last_price) / max(last_price, 1e-9) >= min_move_pct:
                pivots.append({"index": i, "type": "low", "price": low})
                last_price = low

    pivots.sort(key=lambda x: x["index"])
    compressed = []
    for pivot in pivots:
        if not compressed:
            compressed.append(pivot)
            continue
        prev = compressed[-1]
        if prev["type"] == pivot["type"]:
            if pivot["type"] == "high" and pivot["price"] >= prev["price"]:
                compressed[-1] = pivot
            elif pivot["type"] == "low" and pivot["price"] <= prev["price"]:
                compressed[-1] = pivot
        else:
            compressed.append(pivot)
    return compressed


def _equal(a: float, b: float, tol_pct: float = 0.02) -> bool:
    return abs(a - b) / max(min(abs(a), abs(b)), 1e-9) <= tol_pct


def _build_pattern(direction, pattern, strength, trigger_level, entry_price, extra=None):
    payload = {
        "direction": direction,
        "pattern": pattern,
        "strength": round(max(0.45, min(0.92, strength)), 3),
        "trigger_level": float(trigger_level),
        "entry_price": float(entry_price),
        "reason": pattern,
    }
    if extra:
        payload.update(extra)
    return payload


def detect_double_bottom(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    lows = [p for p in pivots[-8:] if p["type"] == "low"]
    highs = [p for p in pivots[-8:] if p["type"] == "high"]
    if len(lows) < 2 or len(highs) < 1:
        return None

    l1, l2 = lows[-2], lows[-1]
    if l2["index"] <= l1["index"]:
        return None
    if not _equal(l1["price"], l2["price"], 0.018):
        return None

    middle_highs = [p for p in highs if l1["index"] < p["index"] < l2["index"]]
    if not middle_highs:
        return None
    neckline = max(p["price"] for p in middle_highs)
    return _build_pattern("BUY", "double_bottom", 0.73, neckline, neckline, {"anchor_price": l2["price"]})


def detect_double_top(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    highs = [p for p in pivots[-8:] if p["type"] == "high"]
    lows = [p for p in pivots[-8:] if p["type"] == "low"]
    if len(highs) < 2 or len(lows) < 1:
        return None

    h1, h2 = highs[-2], highs[-1]
    if h2["index"] <= h1["index"]:
        return None
    if not _equal(h1["price"], h2["price"], 0.018):
        return None

    middle_lows = [p for p in lows if h1["index"] < p["index"] < h2["index"]]
    if not middle_lows:
        return None
    neckline = min(p["price"] for p in middle_lows)
    return _build_pattern("SELL", "double_top", 0.73, neckline, neckline, {"anchor_price": h2["price"]})


def detect_triple_bottom(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    lows = [p for p in pivots[-10:] if p["type"] == "low"]
    highs = [p for p in pivots[-10:] if p["type"] == "high"]
    if len(lows) < 3 or len(highs) < 2:
        return None

    trio = lows[-3:]
    if not all(_equal(trio[0]["price"], item["price"], 0.02) for item in trio[1:]):
        return None
    neckline = max(p["price"] for p in highs if trio[0]["index"] < p["index"] < trio[-1]["index"])
    return _build_pattern("BUY", "triple_bottom", 0.78, neckline, neckline, {"anchor_price": trio[-1]["price"]})


def detect_triple_top(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    highs = [p for p in pivots[-10:] if p["type"] == "high"]
    lows = [p for p in pivots[-10:] if p["type"] == "low"]
    if len(highs) < 3 or len(lows) < 2:
        return None

    trio = highs[-3:]
    if not all(_equal(trio[0]["price"], item["price"], 0.02) for item in trio[1:]):
        return None
    neckline = min(p["price"] for p in lows if trio[0]["index"] < p["index"] < trio[-1]["index"])
    return _build_pattern("SELL", "triple_top", 0.78, neckline, neckline, {"anchor_price": trio[-1]["price"]})


def detect_head_and_shoulders(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    seq = pivots[-5:]
    if len(seq) < 5:
        return None
    if [x["type"] for x in seq] != ["high", "low", "high", "low", "high"]:
        return None
    ls, low1, head, low2, rs = seq
    if head["price"] <= ls["price"] or head["price"] <= rs["price"]:
        return None
    if not _equal(ls["price"], rs["price"], 0.035):
        return None
    neckline = (low1["price"] + low2["price"]) / 2.0
    strength = 0.76 + min((head["price"] - max(ls["price"], rs["price"])) / max(head["price"], 1e-9), 0.12)
    return _build_pattern("SELL", "head_and_shoulders", strength, neckline, neckline, {"anchor_price": head["price"]})


def detect_inverse_head_and_shoulders(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    seq = pivots[-5:]
    if len(seq) < 5:
        return None
    if [x["type"] for x in seq] != ["low", "high", "low", "high", "low"]:
        return None
    ls, high1, head, high2, rs = seq
    if head["price"] >= ls["price"] or head["price"] >= rs["price"]:
        return None
    if not _equal(ls["price"], rs["price"], 0.035):
        return None
    neckline = (high1["price"] + high2["price"]) / 2.0
    strength = 0.76 + min((min(ls["price"], rs["price"]) - head["price"]) / max(min(ls["price"], rs["price"]), 1e-9), 0.12)
    return _build_pattern("BUY", "inverse_head_and_shoulders", strength, neckline, neckline, {"anchor_price": head["price"]})


def detect_triangle(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    highs = [p for p in pivots[-8:] if p["type"] == "high"]
    lows = [p for p in pivots[-8:] if p["type"] == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return None

    h1, h2 = highs[-2], highs[-1]
    l1, l2 = lows[-2], lows[-1]

    descending_highs = h2["price"] < h1["price"] * 0.995
    ascending_lows = l2["price"] > l1["price"] * 1.005
    flat_highs = _equal(h1["price"], h2["price"], 0.012)
    flat_lows = _equal(l1["price"], l2["price"], 0.012)

    if flat_highs and ascending_lows:
        return _build_pattern("BUY", "ascending_triangle", 0.74, h2["price"], h2["price"], {"anchor_price": l2["price"]})
    if flat_lows and descending_highs:
        return _build_pattern("SELL", "descending_triangle", 0.74, l2["price"], l2["price"], {"anchor_price": h2["price"]})
    if descending_highs and ascending_lows:
        last_close = float(candles[-1]["close"])
        direction = "BUY" if last_close >= (h2["price"] + l2["price"]) / 2.0 else "SELL"
        trigger = h2["price"] if direction == "BUY" else l2["price"]
        anchor = l2["price"] if direction == "BUY" else h2["price"]
        return _build_pattern(direction, "symmetrical_triangle", 0.70, trigger, trigger, {"anchor_price": anchor})
    return None


def detect_wedge(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    highs = [p for p in pivots[-8:] if p["type"] == "high"]
    lows = [p for p in pivots[-8:] if p["type"] == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return None

    h1, h2 = highs[-2], highs[-1]
    l1, l2 = lows[-2], lows[-1]
    highs_down = h2["price"] < h1["price"] * 0.995
    lows_down = l2["price"] < l1["price"] * 0.995
    highs_up = h2["price"] > h1["price"] * 1.005
    lows_up = l2["price"] > l1["price"] * 1.005

    if highs_down and lows_down:
        return _build_pattern("BUY", "falling_wedge", 0.72, h2["price"], h2["price"], {"anchor_price": l2["price"]})
    if highs_up and lows_up:
        return _build_pattern("SELL", "rising_wedge", 0.72, l2["price"], l2["price"], {"anchor_price": h2["price"]})
    return None


def detect_rectangle(candles, pivots=None):
    pivots = pivots or extract_pivots(candles)
    highs = [p for p in pivots[-8:] if p["type"] == "high"]
    lows = [p for p in pivots[-8:] if p["type"] == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return None
    if not _equal(highs[-1]["price"], highs[-2]["price"], 0.015):
        return None
    if not _equal(lows[-1]["price"], lows[-2]["price"], 0.015):
        return None

    last_close = float(candles[-1]["close"])
    midpoint = (highs[-1]["price"] + lows[-1]["price"]) / 2.0
    direction = "BUY" if last_close >= midpoint else "SELL"
    trigger = highs[-1]["price"] if direction == "BUY" else lows[-1]["price"]
    anchor = lows[-1]["price"] if direction == "BUY" else highs[-1]["price"]
    return _build_pattern(direction, "rectangle", 0.67, trigger, trigger, {"anchor_price": anchor})


def detect_cup_and_handle(candles):
    if not candles or len(candles) < 40:
        return None
    closes = [float(c["close"]) for c in candles[-40:]]
    left = closes[:15]
    bottom = closes[15:25]
    right = closes[25:35]
    handle = closes[35:]

    left_high = max(left)
    right_high = max(right)
    cup_low = min(bottom)
    handle_low = min(handle)
    if _equal(left_high, right_high, 0.03) and cup_low < min(left_high, right_high) * 0.92 and handle_low > cup_low * 1.04:
        return _build_pattern("BUY", "cup_and_handle", 0.71, right_high, right_high, {"anchor_price": handle_low})
    return None


def detect_inverse_cup_and_handle(candles):
    if not candles or len(candles) < 40:
        return None
    closes = [float(c["close"]) for c in candles[-40:]]
    left = closes[:15]
    top = closes[15:25]
    right = closes[25:35]
    handle = closes[35:]

    left_low = min(left)
    right_low = min(right)
    cup_high = max(top)
    handle_high = max(handle)
    if _equal(left_low, right_low, 0.03) and cup_high > max(left_low, right_low) * 1.08 and handle_high < cup_high * 0.98:
        return _build_pattern("SELL", "inverse_cup_and_handle", 0.71, right_low, right_low, {"anchor_price": handle_high})
    return None


def detect_best_chart_pattern(candles):
    pivots = extract_pivots(candles, window=2, min_move_pct=0.008)
    detectors = (
        detect_inverse_head_and_shoulders,
        detect_head_and_shoulders,
        detect_triple_bottom,
        detect_triple_top,
        detect_double_bottom,
        detect_double_top,
        detect_triangle,
        detect_wedge,
        detect_rectangle,
    )
    candidates = []
    for detector in detectors:
        pattern = detector(candles, pivots) if detector not in {detect_cup_and_handle, detect_inverse_cup_and_handle} else detector(candles)
        if pattern:
            candidates.append(pattern)

    cup_pattern = detect_cup_and_handle(candles)
    if cup_pattern:
        candidates.append(cup_pattern)
    inverse_cup = detect_inverse_cup_and_handle(candles)
    if inverse_cup:
        candidates.append(inverse_cup)

    if not candidates:
        return None
    return max(candidates, key=lambda x: x["strength"])


def confirm_chart_pattern_entry_15m(candles_15m, pattern_signal):
    if not candles_15m or len(candles_15m) < 4 or not pattern_signal:
        return None

    last = candles_15m[-1]
    prev = candles_15m[-2]
    prev2 = candles_15m[-3]
    trigger = float(pattern_signal.get("trigger_level", pattern_signal.get("entry_price", 0.0)))
    direction = pattern_signal.get("direction")
    avg_vol = _avg_volume(candles_15m[-8:-1])
    last_vol = float(last.get("volume", 0.0))
    volume_ok = avg_vol <= 0.0 or last_vol >= avg_vol * 1.08

    if direction == "BUY":
        if float(last["close"]) > max(trigger, float(prev["high"]), float(prev2["high"])) and volume_ok:
            return {"entry_price": _midpoint(last), "reason": "pattern_15m_breakout_buy"}
        if float(last["low"]) <= trigger * 1.002 and float(last["close"]) >= trigger and volume_ok:
            return {"entry_price": _midpoint(last), "reason": "pattern_15m_retest_buy"}
        if float(last["close"]) > float(last["open"]) and float(prev["close"]) > float(prev["open"]) and float(last["close"]) > trigger:
            return {"entry_price": _midpoint(last), "reason": "pattern_15m_impulse_buy"}

    if direction == "SELL":
        if float(last["close"]) < min(trigger, float(prev["low"]), float(prev2["low"])) and volume_ok:
            return {"entry_price": _midpoint(last), "reason": "pattern_15m_breakout_sell"}
        if float(last["high"]) >= trigger * 0.998 and float(last["close"]) <= trigger and volume_ok:
            return {"entry_price": _midpoint(last), "reason": "pattern_15m_retest_sell"}
        if float(last["close"]) < float(last["open"]) and float(prev["close"]) < float(prev["open"]) and float(last["close"]) < trigger:
            return {"entry_price": _midpoint(last), "reason": "pattern_15m_impulse_sell"}

    return None


def chart_pattern_not_overextended(pattern_signal, current_price, max_extension_pct=0.16):
    if not pattern_signal or current_price is None:
        return False
    direction = pattern_signal.get("direction")
    anchor = float(pattern_signal.get("anchor_price", pattern_signal.get("trigger_level", 0.0)))
    current_price = float(current_price)
    if anchor <= 0:
        return False

    if direction == "BUY":
        return (current_price - anchor) / anchor <= max_extension_pct
    if direction == "SELL":
        return (anchor - current_price) / anchor <= max_extension_pct
    return False

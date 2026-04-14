def _local_lows(candles, window=2):
    lows = []
    for i in range(window, len(candles) - window):
        current = candles[i]["low"]
        ok = True
        for j in range(i - window, i + window + 1):
            if j == i:
                continue
            if candles[j]["low"] <= current:
                ok = False
                break
        if ok:
            lows.append({"index": i, "price": float(current)})
    return lows


def _local_highs(candles, window=2):
    highs = []
    for i in range(window, len(candles) - window):
        current = candles[i]["high"]
        ok = True
        for j in range(i - window, i + window + 1):
            if j == i:
                continue
            if candles[j]["high"] >= current:
                ok = False
                break
        if ok:
            highs.append({"index": i, "price": float(current)})
    return highs


def _dedupe_levels(levels, min_distance_pct=0.003):
    if not levels:
        return []

    levels = sorted(levels)
    result = [levels[0]]

    for lvl in levels[1:]:
        prev = result[-1]
        if prev <= 0:
            continue
        if abs(lvl - prev) / prev >= min_distance_pct:
            result.append(lvl)

    return result


def find_support_levels(candles, window=2):
    lows = _local_lows(candles, window=window)
    return _dedupe_levels([x["price"] for x in lows])


def find_resistance_levels(candles, window=2):
    highs = _local_highs(candles, window=window)
    return _dedupe_levels([x["price"] for x in highs])


def get_nearest_support(price, support_levels):
    below = [lvl for lvl in support_levels if lvl < price]
    return max(below) if below else None


def get_nearest_resistance(price, resistance_levels):
    above = [lvl for lvl in resistance_levels if lvl > price]
    return min(above) if above else None


def get_second_support(price, support_levels):
    below = [lvl for lvl in support_levels if lvl < price]
    below = sorted(below, reverse=True)
    return below[1] if len(below) > 1 else None


def get_second_resistance(price, resistance_levels):
    above = [lvl for lvl in resistance_levels if lvl > price]
    above = sorted(above)
    return above[1] if len(above) > 1 else None


def calculate_sl_tp_from_levels(
    side,
    entry_price,
    candles,
    fallback_sl_pct=0.012,
    fallback_tp_pct=0.03,
    level_buffer_pct=0.002,
    min_rr=1.5,
):
    supports = find_support_levels(candles)
    resistances = find_resistance_levels(candles)

    support = get_nearest_support(entry_price, supports)
    resistance = get_nearest_resistance(entry_price, resistances)
    second_support = get_second_support(entry_price, supports)
    second_resistance = get_second_resistance(entry_price, resistances)

    if side == "BUY":
        if support is not None and resistance is not None:
            stop = support * (1 - level_buffer_pct)
            take = resistance * (1 - level_buffer_pct)
            risk = entry_price - stop
            reward = take - entry_price
            if risk > 0 and reward > 0:
                rr = reward / risk
                if rr >= min_rr:
                    tp2 = second_resistance * (1 - level_buffer_pct) if second_resistance is not None else take
                    return {
                        "stop": stop,
                        "take": take,
                        "tp1": take,
                        "tp2": tp2,
                        "support": support,
                        "resistance": resistance,
                        "liquidity_target": tp2 if tp2 > take else None,
                        "rr": rr,
                        "source": "levels",
                    }

        stop = entry_price * (1 - fallback_sl_pct)
        take = entry_price * (1 + fallback_tp_pct)
        rr = (take - entry_price) / max(1e-9, entry_price - stop)
        return {
            "stop": stop,
            "take": take,
            "tp1": take,
            "tp2": take,
            "support": support,
            "resistance": resistance,
            "liquidity_target": None,
            "rr": rr,
            "source": "fallback",
        }

    if support is not None and resistance is not None:
        stop = resistance * (1 + level_buffer_pct)
        take = support * (1 + level_buffer_pct)
        risk = stop - entry_price
        reward = entry_price - take
        if risk > 0 and reward > 0:
            rr = reward / risk
            if rr >= min_rr:
                tp2 = second_support * (1 + level_buffer_pct) if second_support is not None else take
                return {
                    "stop": stop,
                    "take": take,
                    "tp1": take,
                    "tp2": tp2,
                    "support": support,
                    "resistance": resistance,
                    "liquidity_target": tp2 if tp2 < take else None,
                    "rr": rr,
                    "source": "levels",
                }

    stop = entry_price * (1 + fallback_sl_pct)
    take = entry_price * (1 - fallback_tp_pct)
    rr = (entry_price - take) / max(1e-9, stop - entry_price)
    return {
        "stop": stop,
        "take": take,
        "tp1": take,
        "tp2": take,
        "support": support,
        "resistance": resistance,
        "liquidity_target": None,
        "rr": rr,
        "source": "fallback",
    }

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


def _line_price(x1, y1, x2, y2, x):
    if x2 == x1:
        return y2
    slope = (y2 - y1) / (x2 - x1)
    return y1 + slope * (x - x1)


def _nearest_support(candles, price):
    lows = _local_lows(candles, 2)
    below = [x["price"] for x in lows if x["price"] < price]
    return max(below) if below else None


def _nearest_resistance(candles, price):
    highs = _local_highs(candles, 2)
    above = [x["price"] for x in highs if x["price"] > price]
    return min(above) if above else None


def detect_trendline_breakout(candles, confluence_threshold_pct=0.002):
    if len(candles) < 30:
        return None

    lows = _local_lows(candles, 2)
    highs = _local_highs(candles, 2)
    last_idx = len(candles) - 1
    last = candles[-1]

    if len(lows) >= 2:
        p1, p2 = lows[-2], lows[-1]
        if p2["price"] > p1["price"]:
            line_now = _line_price(p1["index"], p1["price"], p2["index"], p2["price"], last_idx)
            if last["close"] < line_now * 0.998:
                support = _nearest_support(candles[:-1], line_now)
                confluence = False
                if support:
                    confluence = abs(line_now - support) / support <= confluence_threshold_pct
                entry_price = (last["open"] + last["close"]) / 2 if confluence else last["close"]
                return {
                    "direction": "SELL",
                    "line_type": "uptrend_support_break",
                    "trendline_price": line_now,
                    "level_price": support,
                    "level_confluence": confluence,
                    "entry_price": entry_price,
                    "reason": "trendline_break_down",
                }

    if len(highs) >= 2:
        p1, p2 = highs[-2], highs[-1]
        if p2["price"] < p1["price"]:
            line_now = _line_price(p1["index"], p1["price"], p2["index"], p2["price"], last_idx)
            if last["close"] > line_now * 1.002:
                resistance = _nearest_resistance(candles[:-1], line_now)
                confluence = False
                if resistance:
                    confluence = abs(line_now - resistance) / resistance <= confluence_threshold_pct
                entry_price = (last["open"] + last["close"]) / 2 if confluence else last["close"]
                return {
                    "direction": "BUY",
                    "line_type": "downtrend_resistance_break",
                    "trendline_price": line_now,
                    "level_price": resistance,
                    "level_confluence": confluence,
                    "entry_price": entry_price,
                    "reason": "trendline_break_up",
                }

    return None


def count_recent_large_trades(trades, side=None):
    items = [t for t in trades if t.get("is_large")]
    if side:
        items = [t for t in items if t.get("side") == side]
    return len(items)


def confirm_trendline_breakout(trades, imbalance, oi_now, oi_prev, breakout):
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
                "reason": "confirmed_trendline_break_up",
                "entry_price": breakout["entry_price"],
                "level_confluence": breakout["level_confluence"],
                "trendline_price": breakout["trendline_price"],
                "level_price": breakout["level_price"],
                "large_buy": large_buy,
                "large_sell": large_sell,
                "imbalance": imbalance,
                "oi_up": oi_up,
            }

    if direction == "SELL":
        if imbalance < -0.20 and large_sell >= 2 and oi_up:
            return {
                "direction": "SELL",
                "reason": "confirmed_trendline_break_down",
                "entry_price": breakout["entry_price"],
                "level_confluence": breakout["level_confluence"],
                "trendline_price": breakout["trendline_price"],
                "level_price": breakout["level_price"],
                "large_buy": large_buy,
                "large_sell": large_sell,
                "imbalance": imbalance,
                "oi_up": oi_up,
            }

    return None

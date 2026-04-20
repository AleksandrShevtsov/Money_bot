"""
Predictive flow layer inspired by flow/cumulative-delta style indicators.

The goal is not to replicate TradingView code one-to-one, but to bring the
same core ideas into the bot in a fast, rule-based form:
1. Measure directional participation from candles and trade flow.
2. Track cumulative pressure and its slope.
3. Detect price/flow disagreement (exhaustion).
4. Convert that into a directional bias that can validate or reject entries.
"""


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _candle_flow_score(candle):
    high = _safe_float(candle.get("high"))
    low = _safe_float(candle.get("low"))
    close = _safe_float(candle.get("close"))
    open_price = _safe_float(candle.get("open"))
    volume = _safe_float(candle.get("volume"))

    candle_range = max(high - low, 1e-12)
    body = close - open_price
    close_location = ((close - low) / candle_range) * 2.0 - 1.0
    body_norm = _clamp(body / candle_range, -1.0, 1.0)
    return volume * (0.65 * close_location + 0.35 * body_norm)


def _signed_trade_flow(trades):
    if not trades:
        return 0.0, 0.0

    buy_flow = 0.0
    sell_flow = 0.0
    for trade in trades:
        usd_size = _safe_float(trade.get("usd_size"))
        side = str(trade.get("side", "")).lower()
        if side == "buy":
            buy_flow += usd_size
        elif side == "sell":
            sell_flow += usd_size
    return buy_flow, sell_flow


def _price_change_pct(candles, lookback):
    if not candles or len(candles) < lookback + 1:
        return 0.0
    start = _safe_float(candles[-(lookback + 1)].get("close"))
    end = _safe_float(candles[-1].get("close"))
    if start <= 0:
        return 0.0
    return (end - start) / start


def _flow_slope(series, tail):
    if len(series) < tail + 1:
        return 0.0
    return series[-1] - series[-(tail + 1)]


def analyze_predictive_flow(candles, trades=None, imbalance=0.0, lookback=34, slope_window=8):
    if not candles or len(candles) < max(10, slope_window + 2):
        return {
            "bias": "NEUTRAL",
            "strength": 0.0,
            "score_bias": 0.0,
            "exhaustion": False,
            "reason": "predictive_flow_unavailable",
        }

    window = candles[-lookback:] if len(candles) > lookback else candles
    candle_scores = [_candle_flow_score(c) for c in window]
    cumulative = []
    running = 0.0
    for score in candle_scores:
        running += score
        cumulative.append(running)

    raw_slope = _flow_slope(cumulative, min(slope_window, len(cumulative) - 1))
    avg_abs_score = sum(abs(x) for x in candle_scores) / max(1, len(candle_scores))
    normalized_slope = raw_slope / max(avg_abs_score * max(1, slope_window), 1e-12)
    normalized_slope = _clamp(normalized_slope, -1.5, 1.5)

    buy_flow, sell_flow = _signed_trade_flow(trades or [])
    total_trade_flow = buy_flow + sell_flow
    trade_delta = 0.0
    if total_trade_flow > 0:
        trade_delta = (buy_flow - sell_flow) / total_trade_flow

    price_move = _price_change_pct(window, min(6, len(window) - 1))
    price_direction = 1.0 if price_move > 0 else -1.0 if price_move < 0 else 0.0
    flow_direction = 1.0 if normalized_slope > 0 else -1.0 if normalized_slope < 0 else 0.0
    exhaustion = price_direction != 0.0 and flow_direction != 0.0 and price_direction != flow_direction

    composite = (
        normalized_slope * 0.55
        + _clamp(trade_delta, -1.0, 1.0) * 0.25
        + _clamp(float(imbalance or 0.0), -1.0, 1.0) * 0.20
    )
    if exhaustion:
        composite *= 0.55

    strength = round(min(1.0, abs(composite)), 3)
    if composite >= 0.18:
        bias = "BUY"
    elif composite <= -0.18:
        bias = "SELL"
    else:
        bias = "NEUTRAL"

    reasons = []
    if bias == "BUY":
        reasons.append("predictive_flow_buy")
    elif bias == "SELL":
        reasons.append("predictive_flow_sell")
    else:
        reasons.append("predictive_flow_neutral")
    if exhaustion:
        reasons.append("flow_price_divergence")
    if abs(trade_delta) >= 0.2:
        reasons.append("trade_delta_support")
    if abs(float(imbalance or 0.0)) >= 0.12:
        reasons.append("book_imbalance_support")

    return {
        "bias": bias,
        "strength": strength,
        "score_bias": round(composite * 0.12, 4),
        "exhaustion": exhaustion,
        "trade_delta": round(trade_delta, 4),
        "slope": round(normalized_slope, 4),
        "reason": "|".join(reasons),
    }


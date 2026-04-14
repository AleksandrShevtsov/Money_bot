from __future__ import annotations

from typing import Any

from config import DIVERGENCE_LOOKBACK, MAX_DIVERGENCE_EXTENSION_PCT


def _extract_series(candles: list[dict[str, float]], key: str) -> list[float]:
    return [float(c.get(key, 0.0)) for c in candles]


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [values[0]]
    for val in values[1:]:
        out.append(alpha * val + (1.0 - alpha) * out[-1])
    return out


def _rsi(values: list[float], period: int = 14) -> list[float]:
    if len(values) < period + 1:
        return []

    gains = []
    losses = []
    for i in range(1, len(values)):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    rsi_vals = [50.0] * period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

        if avg_loss == 0:
            rsi_vals.append(100.0)
            continue

        rs = avg_gain / avg_loss
        rsi_vals.append(100.0 - (100.0 / (1.0 + rs)))

    if len(rsi_vals) < len(values):
        pad = [50.0] * (len(values) - len(rsi_vals))
        rsi_vals = pad + rsi_vals

    return rsi_vals[-len(values):]


def _macd_histogram(values: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> list[float]:
    if len(values) < slow + signal:
        return []

    ema_fast = _ema(values, fast)
    ema_slow = _ema(values, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = _ema(macd_line, signal)
    return [m - s for m, s in zip(macd_line, signal_line)]


def _last_two_local_extremes(values: list[float], mode: str, lookback: int) -> list[tuple[int, float]]:
    if len(values) < 5:
        return []

    start = max(2, len(values) - lookback)
    pivots: list[tuple[int, float]] = []

    for i in range(start, len(values) - 2):
        left = values[i - 2:i]
        right = values[i + 1:i + 3]
        center = values[i]
        if mode == "low":
            if center <= min(left) and center <= min(right):
                pivots.append((i, center))
        else:
            if center >= max(left) and center >= max(right):
                pivots.append((i, center))

    return pivots[-2:]


def _calc_strength(price_a: float, price_b: float, osc_a: float, osc_b: float) -> float:
    price_move = abs(price_b - price_a) / max(abs(price_a), 1e-9)
    osc_move = abs(osc_b - osc_a) / max(abs(osc_a), 1e-9)
    raw = (price_move * 1.3) + (osc_move * 0.7)
    return max(0.30, min(0.90, round(raw, 3)))


def detect_rsi_divergence(candles: list[dict[str, float]]) -> dict[str, Any] | None:
    closes = _extract_series(candles, "close")
    if len(closes) < 40:
        return None

    rsi_vals = _rsi(closes, period=14)
    if len(rsi_vals) != len(closes):
        return None

    lows = _last_two_local_extremes(closes, mode="low", lookback=DIVERGENCE_LOOKBACK)
    highs = _last_two_local_extremes(closes, mode="high", lookback=DIVERGENCE_LOOKBACK)

    if len(lows) == 2:
        (i1, p1), (i2, p2) = lows
        r1, r2 = rsi_vals[i1], rsi_vals[i2]
        if p2 < p1 and r2 > r1:
            return {
                "direction": "BUY",
                "pattern": "rsi_bullish_divergence",
                "strength": _calc_strength(p1, p2, r1, r2),
                "reason": "rsi_bullish_divergence",
            }

    if len(highs) == 2:
        (i1, p1), (i2, p2) = highs
        r1, r2 = rsi_vals[i1], rsi_vals[i2]
        if p2 > p1 and r2 < r1:
            return {
                "direction": "SELL",
                "pattern": "rsi_bearish_divergence",
                "strength": _calc_strength(p1, p2, r1, r2),
                "reason": "rsi_bearish_divergence",
            }

    return None


def detect_macd_divergence(candles: list[dict[str, float]]) -> dict[str, Any] | None:
    closes = _extract_series(candles, "close")
    if len(closes) < 50:
        return None

    histogram = _macd_histogram(closes, fast=12, slow=26, signal=9)
    if len(histogram) != len(closes):
        return None

    lows = _last_two_local_extremes(closes, mode="low", lookback=DIVERGENCE_LOOKBACK)
    highs = _last_two_local_extremes(closes, mode="high", lookback=DIVERGENCE_LOOKBACK)

    if len(lows) == 2:
        (i1, p1), (i2, p2) = lows
        h1, h2 = histogram[i1], histogram[i2]
        if p2 < p1 and h2 > h1:
            return {
                "direction": "BUY",
                "pattern": "macd_histogram_divergence",
                "strength": _calc_strength(p1, p2, h1, h2),
                "reason": "macd_histogram_divergence",
            }

    if len(highs) == 2:
        (i1, p1), (i2, p2) = highs
        h1, h2 = histogram[i1], histogram[i2]
        if p2 > p1 and h2 < h1:
            return {
                "direction": "SELL",
                "pattern": "macd_histogram_divergence",
                "strength": _calc_strength(p1, p2, h1, h2),
                "reason": "macd_histogram_divergence",
            }

    return None


def divergence_not_overextended(
    candles: list[dict[str, float]],
    direction: str,
    lookback: int = DIVERGENCE_LOOKBACK,
    max_extension_pct: float = MAX_DIVERGENCE_EXTENSION_PCT,
) -> bool:
    if len(candles) < 2:
        return False

    closes = _extract_series(candles, "close")
    last_price = closes[-1]
    window = closes[-lookback:] if len(closes) >= lookback else closes

    if direction == "BUY":
        base = min(window)
        if base <= 0:
            return False
        move = (last_price - base) / base
    else:
        base = max(window)
        if base <= 0:
            return False
        move = (base - last_price) / base

    return move <= max_extension_pct

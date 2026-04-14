from __future__ import annotations

from binance_candles_feed import fetch_klines
from volatility_regime import atr_pct


def _rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0

    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def detect_btc_regime():
    candles = fetch_klines("BTCUSDT", "4h", 120)
    if not candles or len(candles) < 40:
        return {
            "regime": "unknown",
            "bias": "NEUTRAL",
            "strength": 0.0,
            "reason": "btc_regime_unavailable",
        }

    closes = [float(c["close"]) for c in candles]
    last_close = closes[-1]
    slow_ma = sum(closes[-20:]) / 20.0
    fast_ma = sum(closes[-8:]) / 8.0
    impulse = (closes[-1] - closes[-6]) / max(closes[-6], 1e-9)
    volatility = atr_pct(candles, period=14)
    rsi_value = _rsi(closes, period=14)

    if volatility >= 0.045 and impulse <= -0.05:
        return {
            "regime": "panic",
            "bias": "SELL",
            "strength": 0.92,
            "reason": "btc_panic_regime",
        }

    width = (max(closes[-20:]) - min(closes[-20:])) / max(min(closes[-20:]), 1e-9)
    if volatility <= 0.012 and width <= 0.05:
        return {
            "regime": "squeeze",
            "bias": "NEUTRAL",
            "strength": 0.62,
            "reason": "btc_squeeze_regime",
        }

    if last_close > slow_ma and fast_ma > slow_ma and rsi_value >= 52:
        return {
            "regime": "bullish",
            "bias": "BUY",
            "strength": min(0.9, 0.58 + max(0.0, impulse) * 4.0),
            "reason": "btc_bullish_regime",
        }

    if last_close < slow_ma and fast_ma < slow_ma and rsi_value <= 48:
        return {
            "regime": "bearish",
            "bias": "SELL",
            "strength": min(0.9, 0.58 + max(0.0, -impulse) * 4.0),
            "reason": "btc_bearish_regime",
        }

    return {
        "regime": "range",
        "bias": "NEUTRAL",
        "strength": 0.45,
        "reason": "btc_range_regime",
    }

from binance_candles_feed import fetch_klines


def detect_htf_trend(symbol):
    candles = fetch_klines(symbol, "4h", 200)
    if candles is None or len(candles) < 50:
        return "FLAT"

    closes = [c["close"] for c in candles[-50:]]
    if len(closes) < 50:
        return "FLAT"

    sma_fast = sum(closes[-10:]) / 10
    sma_slow = sum(closes[-50:]) / 50

    if sma_fast > sma_slow:
        return "BULL"

    if sma_fast < sma_slow:
        return "BEAR"

    return "FLAT"

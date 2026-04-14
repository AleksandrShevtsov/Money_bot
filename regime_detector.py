def detect_regime(prices):
    if len(prices) < 20:
        return "unknown"

    recent = prices[-10:]
    older = prices[-20:-10]

    recent_avg = sum(recent) / len(recent)
    older_avg = sum(older) / len(older)

    change = abs(recent_avg - older_avg) / older_avg

    if change < 0.003:
        return "range"
    if recent_avg > older_avg:
        return "trend_up"
    return "trend_down"

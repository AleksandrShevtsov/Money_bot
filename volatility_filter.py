def volatility_ok(prices, min_move_pct=0.002):
    if len(prices) < 5:
        return False
    high = max(prices[-5:])
    low = min(prices[-5:])
    move = (high - low) / low
    return move >= min_move_pct

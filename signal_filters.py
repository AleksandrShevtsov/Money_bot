def allow_long(trend, volatility_ok_value, regime):
    if trend != "bull":
        return False
    if not volatility_ok_value:
        return False
    if regime == "range":
        return False
    return True


def allow_short(trend, volatility_ok_value, regime):
    if trend != "bear":
        return False
    if not volatility_ok_value:
        return False
    if regime == "range":
        return False
    return True

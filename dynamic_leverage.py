def get_dynamic_leverage(score):
    if score >= 0.8:
        return 20
    if score >= 0.6:
        return 15
    return 10

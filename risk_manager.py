"""
Risk and frequency validation helpers.
"""


def min_rr_for_context(signal_class, symbol_profile="ALT", continuation_context=False, strong_reversal_context=False):
    if signal_class in {"BASE_A", "PATTERN_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "A"}:
        base_rr = 1.8 if strong_reversal_context else 1.6
    elif signal_class == "B":
        base_rr = 1.5
    elif signal_class == "C":
        base_rr = 2.0
    else:
        base_rr = 3.0

    if continuation_context and signal_class in {"B", "C", "REJECT"}:
        base_rr = max(base_rr, 2.0)

    if symbol_profile == "LOW_CAP":
        base_rr += 0.4
    elif symbol_profile == "ALT":
        base_rr += 0.1

    return round(base_rr, 2)


def rr_validation_reason(rr_value, min_rr):
    if rr_value <= 0:
        return "rr_unavailable"
    if rr_value < min_rr:
        return f"rr_below_min({rr_value:.2f}<{min_rr:.2f})"
    return None


def trim_trade_timestamps(timestamps, now_ts, window_seconds=3600):
    return [ts for ts in timestamps if now_ts - float(ts) <= window_seconds]


def frequency_limit_reason(timestamps, now_ts, max_trades_per_hour):
    recent = trim_trade_timestamps(timestamps, now_ts, window_seconds=3600)
    if max_trades_per_hour > 0 and len(recent) >= max_trades_per_hour:
        return f"trade_frequency_limit({len(recent)}/{max_trades_per_hour})", recent
    return None, recent


def low_cap_limit_reason(open_positions, max_low_cap_positions):
    if max_low_cap_positions <= 0:
        return None
    active_low_cap = sum(
        1
        for pos in open_positions.values()
        if pos is not None and pos.get("symbol_profile") == "LOW_CAP"
    )
    if active_low_cap >= max_low_cap_positions:
        return f"max_low_cap_positions({active_low_cap}/{max_low_cap_positions})"
    return None


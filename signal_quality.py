def classify_signal_quality(
    side,
    score,
    breakout_confirmation=None,
    trendline_confirmation=None,
    retest_confirmation=None,
    fast_move=None,
    acceleration=None,
    htf_trend=None,
    volume_confirmed=False,
    structure_ok=False,
    regime_name=None,
    liquidity_sweep=None,
    multi_bar_confirmed=False,
):
    reasons = []

    if retest_confirmation:
        reasons.append("retest")
    if breakout_confirmation:
        reasons.append("breakout")
    if trendline_confirmation:
        reasons.append("trendline")
    if fast_move and fast_move.get("direction") == side:
        reasons.append("fast_move")
    if acceleration and acceleration.get("direction") == side:
        reasons.append("acceleration")
    if volume_confirmed:
        reasons.append("volume")
    if structure_ok:
        reasons.append("structure")
    if liquidity_sweep and liquidity_sweep.get("direction") == side:
        reasons.append("liquidity")
    if multi_bar_confirmed:
        reasons.append("hold")

    strong_htf = (
        (side == "BUY" and htf_trend == "BULL") or
        (side == "SELL" and htf_trend == "BEAR")
    )

    if strong_htf:
        reasons.append("htf")
    if regime_name:
        reasons.append(f"regime:{regime_name}")

    if retest_confirmation and volume_confirmed and structure_ok and strong_htf and multi_bar_confirmed and score >= 0.40:
        return "A", reasons

    if (breakout_confirmation or trendline_confirmation) and structure_ok and multi_bar_confirmed and score >= 0.32:
        return "B", reasons

    if (fast_move or acceleration or liquidity_sweep) and score >= 0.28:
        return "C", reasons

    return "REJECT", reasons


def quality_position_multiplier(signal_class):
    if signal_class == "A":
        return 1.0
    if signal_class == "B":
        return 0.7
    if signal_class == "C":
        return 0.4
    return 0.0

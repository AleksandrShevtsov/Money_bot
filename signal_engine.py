"""
Signal validation helpers.

These functions keep the strategy rule-based and fast while separating
high-level entry quality checks from the main trading loop.
"""


def quality_anchor_count(
    retest_confirmation=None,
    order_block_entry=None,
    liquidity_sweep=None,
    divergence_signal=None,
    chart_pattern_entry=None,
    base_entry=None,
    reversal_entry=None,
):
    return sum(
        1
        for item in (
            retest_confirmation,
            order_block_entry,
            liquidity_sweep,
            divergence_signal,
            chart_pattern_entry,
            base_entry,
            reversal_entry,
        )
        if item is not None
    )


def impulse_only_setup(
    fast_move=None,
    acceleration=None,
    breakout_confirmation=None,
    trendline_confirmation=None,
    retest_confirmation=None,
    order_block_entry=None,
    liquidity_sweep=None,
    divergence_signal=None,
    chart_pattern_entry=None,
    base_entry=None,
    reversal_entry=None,
):
    return (
        (fast_move is not None or acceleration is not None)
        and breakout_confirmation is None
        and trendline_confirmation is None
        and retest_confirmation is None
        and order_block_entry is None
        and liquidity_sweep is None
        and divergence_signal is None
        and chart_pattern_entry is None
        and base_entry is None
        and reversal_entry is None
    )


def conflict_reason(
    side,
    htf_trend=None,
    structure_4h=None,
    order_block=None,
    continuation_context=False,
    strong_reversal_context=False,
):
    if side not in {"BUY", "SELL"}:
        return None

    structure_trend = (structure_4h or {}).get("trend")
    if htf_trend == "BULL" and side == "SELL" and not strong_reversal_context:
        return "htf_bias_conflict"
    if htf_trend == "BEAR" and side == "BUY" and not strong_reversal_context:
        return "htf_bias_conflict"

    if structure_trend == "bullish_structure" and side == "SELL" and continuation_context and not strong_reversal_context:
        return "market_structure_conflict"
    if structure_trend == "bearish_structure" and side == "BUY" and continuation_context and not strong_reversal_context:
        return "market_structure_conflict"

    if order_block and order_block.get("direction") and order_block.get("direction") != side and not strong_reversal_context:
        return "order_block_conflict"

    return None


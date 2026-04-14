from config import (
    ENABLE_BLOCK_SIGNAL_CLASS_REJECT,
    ENABLE_BLOCK_STRUCTURE_FILTER,
    ENABLE_BLOCK_BREAKOUT_NO_VOLUME,
    ENABLE_BLOCK_PANIC_REGIME,
    ENABLE_BLOCK_ALT_RECLAIM_CONTEXT,
    ENABLE_BLOCK_OI_NOT_READY,
    ENABLE_BLOCK_HTF_CONFLICT,
    ENABLE_BLOCK_LOW_PRICE_RETEST,
    ENABLE_BLOCK_EXTENSION,
    ENABLE_BLOCK_ANTI_FOMO,
)


def apply_block_filters(
    symbol,
    sig,
    structure_ok=True,
    volume_confirmed=True,
    panic_regime=False,
    reclaim_needed=False,
    oi_ready=True,
    htf_conflict=False,
    extension_block=False,
    anti_fomo_block=False,
):
    strong_signal = sig.signal_class in {"A", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"}

    if sig.signal_class == "REJECT":
        if ENABLE_BLOCK_SIGNAL_CLASS_REJECT:
            return False, "signal_class_reject"

    if not structure_ok:
        if ENABLE_BLOCK_STRUCTURE_FILTER and not strong_signal:
            return False, "structure_filter"

    if not volume_confirmed:
        if ENABLE_BLOCK_BREAKOUT_NO_VOLUME:
            return False, "breakout_no_volume"

    if panic_regime:
        if ENABLE_BLOCK_PANIC_REGIME:
            return False, "panic_regime"

    if reclaim_needed:
        if ENABLE_BLOCK_ALT_RECLAIM_CONTEXT and not strong_signal:
            return False, "alt_needs_reclaim_context"

    if not oi_ready:
        if ENABLE_BLOCK_OI_NOT_READY:
            return False, "oi_not_ready"

    if htf_conflict:
        if ENABLE_BLOCK_HTF_CONFLICT:
            return False, "htf_conflict"

    if extension_block:
        if ENABLE_BLOCK_EXTENSION:
            return False, "extension_filter"

    if anti_fomo_block:
        if ENABLE_BLOCK_ANTI_FOMO:
            return False, "anti_fomo"

    return True, "allowed"

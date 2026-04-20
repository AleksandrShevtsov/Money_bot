"""
Execution gating helpers for cooldowns and re-entry control.
"""


def structure_signature(structure):
    if not structure:
        return "unknown"
    return str(structure.get("trend") or structure.get("reason") or "unknown")


def reentry_block_reason(symbol, now_ts, cooldown_until, current_structure, last_stop_meta):
    if now_ts < float(cooldown_until.get(symbol, 0) or 0):
        return "symbol_cooldown_active"

    meta = last_stop_meta.get(symbol) or {}
    if not meta:
        return None

    required_new_structure = meta.get("require_new_structure", False)
    blocked_structure = meta.get("structure")
    if required_new_structure and blocked_structure and structure_signature(current_structure) == blocked_structure:
        return f"reentry_requires_new_structure({blocked_structure})"

    return None


def register_stop_reentry_requirement(last_stop_meta, symbol, structure, side):
    last_stop_meta[symbol] = {
        "structure": structure_signature(structure),
        "side": side,
        "require_new_structure": True,
    }


def clear_reentry_requirement_if_changed(last_stop_meta, symbol, current_structure):
    meta = last_stop_meta.get(symbol) or {}
    if not meta:
        return
    if structure_signature(current_structure) != meta.get("structure"):
        last_stop_meta.pop(symbol, None)


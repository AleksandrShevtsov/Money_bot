from datetime import datetime, timezone


def current_utc_hour():
    return datetime.now(timezone.utc).hour


def trading_window_allows_entry(symbol: str):
    hour = current_utc_hour()

    # Самые тонкие часы ликвидности и funding-переходы штрафуем.
    if hour in {1, 2, 3}:
        return False, "low_liquidity_hours"

    if hour in {0, 8, 16}:
        return False, "funding_window"

    if symbol in {"BTCUSDT", "ETHUSDT"}:
        return True, "core_session"

    if hour in {22, 23}:
        return False, "late_alt_session"

    return True, "session_ok"

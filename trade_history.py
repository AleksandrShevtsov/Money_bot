import csv
import json
from pathlib import Path
from datetime import datetime


TRADES_CSV = "trades.csv"
STATE_JSON = "state.json"
TRADE_HEADERS = [
    "time",
    "symbol",
    "side",
    "entry",
    "exit",
    "qty",
    "pnl",
    "result",
    "reason",
    "balance_after",
    "signal_class",
    "signal_score",
    "rr_value",
    "exit_type",
    "btc_regime",
    "symbol_profile",
    "htf_context",
    "entry_context",
]


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_history_files():
    csv_path = Path(TRADES_CSV)
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(TRADE_HEADERS)
    else:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        if rows:
            header = rows[0]
            if header != TRADE_HEADERS:
                expanded = [TRADE_HEADERS]
                for row in rows[1:]:
                    row_map = {header[i]: row[i] for i in range(min(len(header), len(row)))}
                    expanded.append([row_map.get(col, "") for col in TRADE_HEADERS])
                with csv_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(expanded)

    state_path = Path(STATE_JSON)
    if not state_path.exists():
        state_path.write_text(json.dumps({"closed_trades": 0}, ensure_ascii=False, indent=2), encoding="utf-8")


def append_trade(
    symbol,
    side,
    entry,
    exit_price,
    qty,
    pnl,
    reason,
    balance_after,
    signal_class="",
    signal_score=0.0,
    rr_value=0.0,
    exit_type="",
    btc_regime="",
    symbol_profile="",
    htf_context="",
    entry_context="",
):
    ensure_history_files()

    result = "PLUS" if pnl >= 0 else "MINUS"

    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            _now_str(),
            symbol,
            side,
            round(entry, 6),
            round(exit_price, 6),
            round(qty, 6),
            round(pnl, 6),
            result,
            reason,
            round(balance_after, 6),
            signal_class,
            round(signal_score, 6),
            round(rr_value, 6),
            exit_type,
            btc_regime,
            symbol_profile,
            htf_context,
            entry_context,
        ])

    path = Path(STATE_JSON)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        state = {"closed_trades": 0}

    state["closed_trades"] = int(state.get("closed_trades", 0)) + 1
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

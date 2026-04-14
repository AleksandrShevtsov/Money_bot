import csv
import json
from pathlib import Path
from datetime import datetime


TRADES_CSV = "trades.csv"
STATE_JSON = "state.json"


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_history_files():
    csv_path = Path(TRADES_CSV)
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
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
            ])

    state_path = Path(STATE_JSON)
    if not state_path.exists():
        state_path.write_text(json.dumps({"closed_trades": 0}, ensure_ascii=False, indent=2), encoding="utf-8")


def append_trade(symbol, side, entry, exit_price, qty, pnl, reason, balance_after):
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
        ])

    path = Path(STATE_JSON)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        state = {"closed_trades": 0}

    state["closed_trades"] = int(state.get("closed_trades", 0)) + 1
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

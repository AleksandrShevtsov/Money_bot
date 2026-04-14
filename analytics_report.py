import csv
from collections import defaultdict


def load_trades_csv(path="trades.csv"):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def build_signal_type_report(path="trades.csv"):
    rows = load_trades_csv(path)
    stats = defaultdict(lambda: {
        "count": 0,
        "wins": 0,
        "losses": 0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "net": 0.0,
    })

    for row in rows:
        signal_type = row.get("reason", "unknown")
        pnl = float(row.get("pnl", 0) or 0)

        s = stats[signal_type]
        s["count"] += 1
        s["net"] += pnl

        if pnl >= 0:
            s["wins"] += 1
            s["gross_profit"] += pnl
        else:
            s["losses"] += 1
            s["gross_loss"] += abs(pnl)

    report = []
    for signal_type, s in stats.items():
        winrate = (s["wins"] / s["count"] * 100) if s["count"] else 0.0
        pf = (s["gross_profit"] / s["gross_loss"]) if s["gross_loss"] > 0 else 999.0
        report.append({
            "signal_type": signal_type,
            "count": s["count"],
            "wins": s["wins"],
            "losses": s["losses"],
            "winrate_pct": round(winrate, 2),
            "profit_factor": round(pf, 2),
            "net": round(s["net"], 2),
        })

    report.sort(key=lambda x: x["net"], reverse=True)
    return report


def build_signal_class_report(path="trades.csv"):
    rows = load_trades_csv(path)
    stats = defaultdict(lambda: {
        "count": 0,
        "wins": 0,
        "losses": 0,
        "net": 0.0,
    })

    for row in rows:
        signal_class = row.get("signal_class", "") or "UNKNOWN"
        pnl = float(row.get("pnl", 0) or 0)
        s = stats[signal_class]
        s["count"] += 1
        s["net"] += pnl
        if pnl >= 0:
            s["wins"] += 1
        else:
            s["losses"] += 1

    report = []
    for signal_class, s in stats.items():
        winrate = (s["wins"] / s["count"] * 100) if s["count"] else 0.0
        report.append({
            "signal_class": signal_class,
            "count": s["count"],
            "wins": s["wins"],
            "losses": s["losses"],
            "winrate_pct": round(winrate, 2),
            "net": round(s["net"], 2),
        })

    report.sort(key=lambda x: x["net"], reverse=True)
    return report


if __name__ == "__main__":
    report = build_signal_type_report("trades.csv")
    for row in report:
        print(row)

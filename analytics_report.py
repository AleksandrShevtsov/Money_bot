import csv
from collections import defaultdict


def load_trades_csv(path="trades.csv"):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def build_equity_curve(path="trades.csv"):
    rows = load_trades_csv(path)
    curve = []
    for row in rows:
        try:
            equity = float(row.get("balance_after", 0) or 0)
        except Exception:
            continue
        curve.append({
            "time": row.get("time", ""),
            "symbol": row.get("symbol", ""),
            "exit_type": row.get("exit_type", "") or row.get("reason", ""),
            "equity": round(equity, 6),
        })
    return curve


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


def build_exit_type_report(path="trades.csv"):
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
        exit_type = row.get("exit_type", "") or row.get("reason", "") or "UNKNOWN"
        pnl = float(row.get("pnl", 0) or 0)
        s = stats[exit_type]
        s["count"] += 1
        s["net"] += pnl
        if pnl >= 0:
            s["wins"] += 1
            s["gross_profit"] += pnl
        else:
            s["losses"] += 1
            s["gross_loss"] += abs(pnl)

    report = []
    for exit_type, s in stats.items():
        winrate = (s["wins"] / s["count"] * 100) if s["count"] else 0.0
        pf = (s["gross_profit"] / s["gross_loss"]) if s["gross_loss"] > 0 else 999.0
        report.append({
            "exit_type": exit_type,
            "count": s["count"],
            "wins": s["wins"],
            "losses": s["losses"],
            "winrate_pct": round(winrate, 2),
            "profit_factor": round(pf, 2),
            "net": round(s["net"], 2),
        })

    report.sort(key=lambda x: x["net"], reverse=True)
    return report


def build_stop_loss_context_report(path="trades.csv"):
    rows = load_trades_csv(path)
    stats = defaultdict(lambda: {
        "count": 0,
        "net": 0.0,
        "symbols": set(),
    })

    for row in rows:
        exit_type = row.get("exit_type", "") or row.get("reason", "")
        if exit_type != "stop_loss":
            continue
        signal_class = row.get("signal_class", "") or "UNKNOWN"
        htf_context = row.get("htf_context", "") or "no_htf_context"
        context_key = f"{signal_class} | {htf_context}"
        pnl = float(row.get("pnl", 0) or 0)
        s = stats[context_key]
        s["count"] += 1
        s["net"] += pnl
        s["symbols"].add(row.get("symbol", ""))

    report = []
    for context_key, s in stats.items():
        report.append({
            "context": context_key,
            "count": s["count"],
            "net": round(s["net"], 2),
            "symbols": ",".join(sorted(sym for sym in s["symbols"] if sym)),
        })

    report.sort(key=lambda x: (x["net"], -x["count"]))
    return report


def build_context_report(path="trades.csv", field="htf_context"):
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
        key = (row.get(field, "") or "UNKNOWN").strip() or "UNKNOWN"
        pnl = float(row.get("pnl", 0) or 0)
        s = stats[key]
        s["count"] += 1
        s["net"] += pnl
        if pnl >= 0:
            s["wins"] += 1
            s["gross_profit"] += pnl
        else:
            s["losses"] += 1
            s["gross_loss"] += abs(pnl)

    report = []
    for key, s in stats.items():
        winrate = (s["wins"] / s["count"] * 100) if s["count"] else 0.0
        pf = (s["gross_profit"] / s["gross_loss"]) if s["gross_loss"] > 0 else 999.0
        report.append({
            field: key,
            "count": s["count"],
            "wins": s["wins"],
            "losses": s["losses"],
            "winrate_pct": round(winrate, 2),
            "profit_factor": round(pf, 2),
            "net": round(s["net"], 2),
        })

    report.sort(key=lambda x: x["net"], reverse=True)
    return report


def build_context_blacklist_candidates(path="trades.csv"):
    rows = load_trades_csv(path)
    stats = defaultdict(lambda: {
        "count": 0,
        "stop_losses": 0,
        "net": 0.0,
    })

    for row in rows:
        signal_class = row.get("signal_class", "") or "UNKNOWN"
        htf_context = row.get("htf_context", "") or "no_htf_context"
        entry_context = row.get("entry_context", "") or "no_entry_context"
        key = f"{signal_class} | {htf_context} | {entry_context}"
        pnl = float(row.get("pnl", 0) or 0)
        exit_type = row.get("exit_type", "") or row.get("reason", "")
        s = stats[key]
        s["count"] += 1
        s["net"] += pnl
        if exit_type == "stop_loss":
            s["stop_losses"] += 1

    report = []
    for key, s in stats.items():
        if s["count"] < 1:
            continue
        report.append({
            "context": key,
            "count": s["count"],
            "stop_losses": s["stop_losses"],
            "net": round(s["net"], 2),
        })

    report.sort(key=lambda x: (x["net"], -x["stop_losses"], -x["count"]))
    return report


def build_improvement_recommendations(path="trades.csv"):
    exit_report = build_exit_type_report(path)
    stop_loss_report = build_stop_loss_context_report(path)
    recommendations = []

    reverse_signal = next((row for row in exit_report if row["exit_type"] == "reverse_signal"), None)
    if reverse_signal and reverse_signal["net"] < 0:
        recommendations.append(
            "Ослабить reverse_signal как основной выход и оставлять его аварийным только для слабых сделок."
        )

    partial_close = next((row for row in exit_report if row["exit_type"] == "partial_close"), None)
    liquidity_target = next((row for row in exit_report if row["exit_type"] == "liquidity_target"), None)
    if partial_close and liquidity_target and partial_close["net"] > 0 and liquidity_target["net"] > 0:
        recommendations.append(
            "Сильные сделки держать до partial_close/liquidity_target дольше, а trailing включать позже."
        )

    for row in stop_loss_report[:5]:
        if row["net"] < 0:
            recommendations.append(f"Проверить blacklist для контекста: {row['context']}")

    return recommendations


if __name__ == "__main__":
    report = build_signal_type_report("trades.csv")
    for row in report:
        print(row)

from dataclasses import dataclass, field


@dataclass
class Signal:
    symbol: str
    side: str
    score: float
    reason: str
    entry_price: float | None = None
    signal_class: str = "REJECT"
    components: dict = field(default_factory=dict)


def _sum_trade_flow(trades):
    buy = sum(t["usd_size"] for t in trades if t["side"] == "buy")
    sell = sum(t["usd_size"] for t in trades if t["side"] == "sell")
    return buy, sell


def build_signal(
    symbol,
    trades,
    imbalance,
    oi_now,
    oi_prev,
    pattern=None,
    breakout_confirmation=None,
    trendline_confirmation=None,
    retest_confirmation=None,
    regime=None,
    oi_context=None,
    liquidity_sweep=None,
    htf_trend=None,
    structure=None,
    symbol_profile="ALT",
):
    reasons = []
    entry_override = None

    components = {
        "price_action": 0.0,
        "orderflow": 0.0,
        "oi": 0.0,
        "htf_alignment": 0.0,
        "exhaustion_penalty": 0.0,
        "regime_adjustment": 0.0,
        "liquidity": 0.0,
    }

    if breakout_confirmation:
        direction = breakout_confirmation["direction"]
        weight = 0.20
        if regime and regime.get("name") == "trend_day":
            weight += 0.08
        if regime and regime.get("name") == "range_day":
            weight -= 0.04
        components["price_action"] += weight if direction == "BUY" else -weight
        reasons.append(breakout_confirmation.get("reason", "breakout"))

    if trendline_confirmation:
        direction = trendline_confirmation["direction"]
        weight = 0.18
        if regime and regime.get("name") in {"trend_day", "squeeze"}:
            weight += 0.05
        components["price_action"] += weight if direction == "BUY" else -weight
        reasons.append(trendline_confirmation.get("reason", "trendline"))
        entry_override = trendline_confirmation.get("entry_price") or entry_override

    if retest_confirmation:
        direction = retest_confirmation["direction"]
        weight = 0.22
        if regime and regime.get("name") == "trend_day":
            weight += 0.04
        components["price_action"] += weight if direction == "BUY" else -weight
        reasons.append(retest_confirmation.get("reason", "retest"))
        entry_override = retest_confirmation.get("entry_price") or entry_override

    if pattern:
        direction = pattern["direction"]
        components["price_action"] += 0.12 if direction == "BUY" else -0.12
        reasons.append(pattern["pattern"])

    if trades:
        buy, sell = _sum_trade_flow(trades)
        total = buy + sell
        if total > 0:
            pressure = (buy - sell) / total
            components["orderflow"] += pressure * 0.40

        if imbalance > 0:
            components["orderflow"] += min(imbalance, 0.45) * 0.35
        if imbalance < 0:
            components["orderflow"] += max(imbalance, -0.45) * 0.35

    if oi_context:
        components["oi"] += oi_context.get("bias", 0.0)
        reasons.append(oi_context.get("label", "oi"))
    elif oi_now and oi_prev:
        if oi_now > oi_prev * 1.001:
            components["oi"] += 0.08
        elif oi_now < oi_prev * 0.999:
            components["oi"] -= 0.04

    tentative_score = sum(components.values())
    tentative_side = "BUY" if tentative_score > 0 else "SELL"

    if htf_trend == "BULL":
        components["htf_alignment"] += 0.12 if tentative_side == "BUY" else -0.10
    elif htf_trend == "BEAR":
        components["htf_alignment"] += 0.12 if tentative_side == "SELL" else -0.10

    structure_trend = (structure or {}).get("trend")
    if structure_trend == "bullish_structure":
        components["htf_alignment"] += 0.10 if tentative_side == "BUY" else -0.08
    elif structure_trend == "bearish_structure":
        components["htf_alignment"] += 0.10 if tentative_side == "SELL" else -0.08

    if liquidity_sweep:
        direction = liquidity_sweep["direction"]
        components["liquidity"] += 0.18 if direction == "BUY" else -0.18
        reasons.append(liquidity_sweep.get("reason", "liquidity_sweep"))

    regime_name = (regime or {}).get("name")
    if regime_name == "high_volatility_panic":
        components["regime_adjustment"] -= 0.18 if tentative_side == "BUY" else 0.18
    elif regime_name == "squeeze":
        if breakout_confirmation or trendline_confirmation:
            components["regime_adjustment"] += 0.08 if tentative_side == "BUY" else -0.08
    elif regime_name == "range_day":
        if breakout_confirmation and not retest_confirmation:
            components["regime_adjustment"] -= 0.10 if tentative_side == "BUY" else 0.10

    if symbol_profile == "ALT":
        if breakout_confirmation and not retest_confirmation:
            components["exhaustion_penalty"] -= 0.08 if tentative_side == "BUY" else 0.08
        if abs(components["orderflow"]) < 0.06:
            components["exhaustion_penalty"] -= 0.06 if tentative_side == "BUY" else 0.06
    else:
        components["regime_adjustment"] += 0.03 if tentative_side == "BUY" else -0.03

    score = round(sum(components.values()), 3)

    if score >= 0.32:
        return Signal(
            symbol=symbol,
            side="BUY",
            score=score,
            reason="|".join(reasons) if reasons else "buy_setup",
            entry_price=entry_override,
            signal_class="REJECT",
            components=components,
        )

    if score <= -0.32:
        return Signal(
            symbol=symbol,
            side="SELL",
            score=round(abs(score), 3),
            reason="|".join(reasons) if reasons else "sell_setup",
            entry_price=entry_override,
            signal_class="REJECT",
            components=components,
        )

    return Signal(
        symbol=symbol,
        side="HOLD",
        score=round(abs(score), 3),
        reason="weak_signal",
        entry_price=None,
        signal_class="REJECT",
        components=components,
    )

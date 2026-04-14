import os
from typing import Dict, Optional

import joblib
import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.volatility import AverageTrueRange

from config import (
    MODEL_PATH,
    LONG_PROB_THRESHOLD,
    SHORT_PROB_THRESHOLD,
    LOOKBACK_BARS_15M,
    MIN_ADX,
)


def _add_m15_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["m15_rsi"] = RSIIndicator(df["close"], window=14).rsi()

    stoch = StochasticOscillator(
        high=df["high"], low=df["low"], close=df["close"], window=14, smooth_window=3
    )
    df["m15_stoch_k"] = stoch.stoch()
    df["m15_stoch_d"] = stoch.stoch_signal()

    ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
    ema50 = EMAIndicator(df["close"], window=50).ema_indicator()

    df["m15_ema20_dist"] = (df["close"] - ema20) / df["close"]
    df["m15_ema50_dist"] = (df["close"] - ema50) / df["close"]

    macd = MACD(df["close"], window_slow=26, window_fast=12, window_sign=9)
    df["m15_macd"] = macd.macd()
    df["m15_macd_signal"] = macd.macd_signal()
    df["m15_macd_hist"] = df["m15_macd"] - df["m15_macd_signal"]

    atr = AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()
    df["m15_atr_pct"] = atr / df["close"]

    df["m15_adx"] = ADXIndicator(df["high"], df["low"], df["close"], window=14).adx()

    df["m15_ret_1"] = df["close"].pct_change(1)
    df["m15_ret_3"] = df["close"].pct_change(3)
    df["m15_ret_5"] = df["close"].pct_change(5)

    df["m15_volume_change"] = df["volume"].pct_change()

    body = (df["close"] - df["open"]).abs()
    full = (df["high"] - df["low"]).replace(0, np.nan)
    df["m15_body_pct"] = body / full
    df["m15_upper_wick_pct"] = (df["high"] - df[["open", "close"]].max(axis=1)) / full
    df["m15_lower_wick_pct"] = (df[["open", "close"]].min(axis=1) - df["low"]) / full

    return df


def _add_h4_context(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
    ema50 = EMAIndicator(df["close"], window=50).ema_indicator()
    ema100 = EMAIndicator(df["close"], window=100).ema_indicator()

    df["h4_ema20_above_50"] = (ema20 > ema50).astype(int)
    df["h4_ema50_above_100"] = (ema50 > ema100).astype(int)
    df["h4_adx"] = ADXIndicator(df["high"], df["low"], df["close"], window=14).adx()
    df["h4_rsi"] = RSIIndicator(df["close"], window=14).rsi()

    return df


def _find_swings(series: pd.Series, window: int = 6, kind: str = "low"):
    vals = series.values
    levels = []

    for i in range(window, len(vals) - window):
        center = vals[i]
        left = vals[i - window:i]
        right = vals[i + 1:i + 1 + window]

        if kind == "low":
            if center <= left.min() and center <= right.min():
                levels.append(center)
        else:
            if center >= left.max() and center >= right.max():
                levels.append(center)

    return levels


def _merge_levels(levels, threshold_pct=0.004, max_levels=6):
    if not levels:
        return []

    levels = sorted(levels)
    zones = [[levels[0]]]

    for lv in levels[1:]:
        zone_mean = np.mean(zones[-1])
        if abs(lv - zone_mean) / zone_mean <= threshold_pct:
            zones[-1].append(lv)
        else:
            zones.append([lv])

    merged = [float(np.mean(z)) for z in zones]
    return merged[:max_levels]


def _nearest_zone_features(price: float, supports, resistances):
    support = max([x for x in supports if x <= price], default=np.nan)
    resistance = min([x for x in resistances if x >= price], default=np.nan)

    dist_support = np.nan if pd.isna(support) else (price - support) / price
    dist_resistance = np.nan if pd.isna(resistance) else (resistance - price) / price

    return {
        "dist_to_support_pct": dist_support,
        "dist_to_resistance_pct": dist_resistance,
        "support_touches": len(supports),
        "resistance_touches": len(resistances),
        "near_support": int(not pd.isna(dist_support) and dist_support <= 0.01),
        "near_resistance": int(not pd.isna(dist_resistance) and dist_resistance <= 0.01),
    }


def _build_live_feature_row(m15: pd.DataFrame, h4: pd.DataFrame) -> pd.DataFrame:

    m15 = _add_m15_features(m15)
    h4 = _add_h4_context(h4)

    supports = _merge_levels(_find_swings(h4["low"], kind="low"))
    resistances = _merge_levels(_find_swings(h4["high"], kind="high"))

    h4_last = h4.iloc[-1]
    price = float(m15["close"].iloc[-1])

    zone = _nearest_zone_features(price, supports, resistances)

    merged = m15.copy()

    merged["h4_ema20_above_50"] = h4_last["h4_ema20_above_50"]
    merged["h4_ema50_above_100"] = h4_last["h4_ema50_above_100"]
    merged["h4_adx"] = h4_last["h4_adx"]
    merged["h4_rsi"] = h4_last["h4_rsi"]

    for k, v in zone.items():
        merged[k] = v

    base_features = [
        col for col in merged.columns
        if col not in ["timestamp", "open", "high", "low", "close", "volume"]
    ]

    feature_frames = []

    for feature in base_features:

        if feature not in merged.columns:
            continue

        lag_block = pd.DataFrame({
            f"{feature}_lag_{i}": merged[feature].shift(i)
            for i in range(LOOKBACK_BARS_15M)
        })

        feature_frames.append(lag_block)

    if not feature_frames:
        return pd.DataFrame()

    row = pd.concat(feature_frames, axis=1)

    row = row.fillna(0)

    return row.tail(1)


def _trend_from_h4(h4: pd.DataFrame) -> str:
    h4 = _add_h4_context(h4)
    last = h4.iloc[-1]

    ema20 = EMAIndicator(h4["close"], window=20).ema_indicator().iloc[-1]
    ema50 = EMAIndicator(h4["close"], window=50).ema_indicator().iloc[-1]
    ema100 = EMAIndicator(h4["close"], window=100).ema_indicator().iloc[-1]
    price = h4["close"].iloc[-1]

    bull = ema100 < ema50 < ema20 < price and last["h4_adx"] >= MIN_ADX
    bear = ema100 > ema50 > ema20 > price and last["h4_adx"] >= MIN_ADX

    if bull:
        return "bull"
    if bear:
        return "bear"
    return "flat"


def _fallback_probability(m15: pd.DataFrame, h4: pd.DataFrame) -> float:
    m15 = _add_m15_features(m15)
    last = m15.iloc[-1]
    trend = _trend_from_h4(h4)

    score = 0.5

    if trend == "bull":
        score += 0.10
    elif trend == "bear":
        score -= 0.10

    if last["m15_macd_hist"] > 0:
        score += 0.07
    else:
        score -= 0.07

    if last["m15_rsi"] > 55:
        score += 0.06
    elif last["m15_rsi"] < 45:
        score -= 0.06

    return float(min(max(score, 0.0), 1.0))


def get_signal(symbol: str, m15: pd.DataFrame, h4: pd.DataFrame) -> Dict:
    price = float(m15["close"].iloc[-1])
    trend = _trend_from_h4(h4)

    live_row = _build_live_feature_row(m15, h4)
    if live_row.empty:
        return {
            "symbol": symbol,
            "price": price,
            "prob_long": 0.5,
            "trend": trend,
            "signal": "HOLD",
            "reason": "not_enough_live_features",
        }

    prob_long = None
    reason = "model"

    if os.path.exists(MODEL_PATH):
        try:
            model = joblib.load(MODEL_PATH)

            if hasattr(model, "feature_names_in_"):
                cols = list(model.feature_names_in_)
                X = live_row.reindex(columns=cols, fill_value=0)
            else:
                X = live_row

            probs = model.predict_proba(X)[0]
            prob_long = float(probs[1]) if len(probs) > 1 else 0.5
        except Exception:
            prob_long = _fallback_probability(m15, h4)
            reason = "fallback_after_model_error"
    else:
        prob_long = _fallback_probability(m15, h4)
        reason = "fallback_no_model"

    signal = "HOLD"

    if trend == "bull" and prob_long >= LONG_PROB_THRESHOLD:
        signal = "BUY"
    elif trend == "bear" and prob_long <= SHORT_PROB_THRESHOLD:
        signal = "SELL"

    return {
        "symbol": symbol,
        "price": price,
        "prob_long": float(prob_long),
        "trend": trend,
        "signal": signal,
        "reason": reason,
    }
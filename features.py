from __future__ import annotations

import numpy as np
import pandas as pd

from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.volatility import AverageTrueRange
from config import LOOKBACK_BARS_15M

BASE_15M_FEATURES = [
    "m15_rsi",
    "m15_stoch_k",
    "m15_stoch_d",
    "m15_ema20_dist",
    "m15_ema50_dist",
    "m15_macd",
    "m15_macd_signal",
    "m15_macd_hist",
    "m15_atr_pct",
    "m15_adx",
    "m15_ret_1",
    "m15_ret_3",
    "m15_ret_6",
    "m15_volume_change",
    "m15_body_pct",
    "m15_upper_wick_pct",
    "m15_lower_wick_pct",
    "h4_ema20_above_50",
    "h4_ema50_above_100",
    "h4_adx",
    "h4_rsi",
    "dist_to_support_pct",
    "dist_to_resistance_pct",
    "support_touches",
    "resistance_touches",
    "near_support",
    "near_resistance",
]


def add_15m_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if df is None or df.empty:
        return df

    df["m15_rsi"] = RSIIndicator(df["close"], window=14).rsi()

    stoch = StochasticOscillator(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        window=14,
        smooth_window=3
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

    atr = AverageTrueRange(
        df["high"],
        df["low"],
        df["close"],
        window=14
    ).average_true_range()

    df["m15_atr_pct"] = atr / df["close"]
    df["m15_adx"] = ADXIndicator(df["high"], df["low"], df["close"], window=14).adx()

    df["m15_ret_1"] = df["close"].pct_change(1)
    df["m15_ret_3"] = df["close"].pct_change(3)
    df["m15_ret_6"] = df["close"].pct_change(6)

    df["m15_volume_change"] = df["volume"].pct_change()

    body = (df["close"] - df["open"]).abs()
    full = (df["high"] - df["low"]).replace(0, np.nan)

    df["m15_body_pct"] = body / full
    df["m15_upper_wick_pct"] = (df["high"] - df[["open", "close"]].max(axis=1)) / full
    df["m15_lower_wick_pct"] = (df[["open", "close"]].min(axis=1) - df["low"]) / full

    return df


def add_h4_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if df is None or df.empty:
        return df

    ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
    ema50 = EMAIndicator(df["close"], window=50).ema_indicator()
    ema100 = EMAIndicator(df["close"], window=100).ema_indicator()

    df["h4_ema20_above_50"] = (ema20 > ema50).astype(int)
    df["h4_ema50_above_100"] = (ema50 > ema100).astype(int)
    df["h4_adx"] = ADXIndicator(df["high"], df["low"], df["close"], window=14).adx()
    df["h4_rsi"] = RSIIndicator(df["close"], window=14).rsi()

    return df


def build_lagged_features(df: pd.DataFrame, lookback: int = LOOKBACK_BARS_15M) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    if not BASE_15M_FEATURES:
        raise ValueError("BASE_15M_FEATURES пустой. Проверь файл features.py")

    missing_cols = [col for col in BASE_15M_FEATURES if col not in df.columns]
    if missing_cols:
        raise ValueError(f"В DataFrame отсутствуют нужные колонки: {missing_cols}")

    lagged_parts = []

    for feature in BASE_15M_FEATURES:
        part = pd.DataFrame(
            {
                f"{feature}_lag_{i}": df[feature].shift(i)
                for i in range(lookback)
            },
            index=df.index
        )
        lagged_parts.append(part)

    if not lagged_parts:
        raise ValueError("Не удалось собрать lagged_parts для признаков")

    out = pd.concat(lagged_parts, axis=1)
    out["timestamp"] = df["timestamp"]
    out["close"] = df["close"]

    return out
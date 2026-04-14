from __future__ import annotations

import pandas as pd
import numpy as np

from config import (
    SYMBOLS,
    TF_HIGH,
    TF_ENTRY,
    HIST_LIMIT_HIGH,
    HIST_LIMIT_ENTRY,
    TP_PCT,
    SL_PCT,
    TARGET_HORIZON_BARS_15M,
    DATASET_PATH,
    MIN_ADX,
)
from data_loader import make_exchange, fetch_ohlcv_df
from features import add_h4_features, add_15m_features, build_lagged_features
from levels import build_zones, nearest_zone_features
from utils import log, log_green


def _safe_zone_features(price: float, supports, resistances) -> dict:
    zone = nearest_zone_features(price, supports, resistances)
    
    defaults = {
        "dist_to_support_pct": np.nan,
        "dist_to_resistance_pct": np.nan,
        "support_touches": 0,
        "resistance_touches": 0,
        "near_support": 0,
        "near_resistance": 0,
        "support_strength": 0.0,
        "resistance_strength": 0.0,
        "support_width_pct": 0.0,
        "resistance_width_pct": 0.0,
    }
    
    defaults.update(zone)
    return defaults


def _is_impulse_candle(row: pd.Series) -> int:
    atr_pct = row.get("m15_atr_pct", np.nan)
    body_pct = row.get("m15_body_pct", np.nan)
    
    if pd.isna(atr_pct) or pd.isna(body_pct):
        return 0
    
    return int(body_pct > 0.90 and atr_pct > 0.010)


def _rejection_up(row: pd.Series) -> int:
    lower = row.get("m15_lower_wick_pct", np.nan)
    body = row.get("m15_body_pct", np.nan)
    
    if pd.isna(lower) or pd.isna(body):
        return 0
    
    return int(lower > body * 0.6)


def _rejection_down(row: pd.Series) -> int:
    upper = row.get("m15_upper_wick_pct", np.nan)
    body = row.get("m15_body_pct", np.nan)
    
    if pd.isna(upper) or pd.isna(body):
        return 0
    
    return int(upper > body * 0.6)


def _h4_trend_flags(row: pd.Series):
    bull = (
            row.get("h4_ema20_above_50", 0) == 1
            and row.get("h4_ema50_above_100", 0) == 1
            and row.get("h4_adx", 0) >= MIN_ADX
    )
    
    bear = (
            row.get("h4_ema20_above_50", 0) == 0
            and row.get("h4_ema50_above_100", 0) == 0
            and row.get("h4_adx", 0) >= MIN_ADX
    )
    
    return bull, bear


def _valid_setup(row: pd.Series) -> bool:
    """
    Очень мягкая версия:
    - тренд есть
    - рядом с зоной ИЛИ разумно близко к ней
    - rejection желателен, но не обязателен
    """
    bull, bear = _h4_trend_flags(row)
    
    near_support = int(row.get("near_support", 0))
    near_resistance = int(row.get("near_resistance", 0))
    
    dist_support = row.get("dist_to_support_pct", np.nan)
    dist_resistance = row.get("dist_to_resistance_pct", np.nan)
    
    rej_up = int(row.get("rejection_up", 0))
    rej_down = int(row.get("rejection_down", 0))
    
    impulse = int(row.get("impulse_candle", 0))
    
    support_close = (not pd.isna(dist_support)) and dist_support <= 0.025
    resistance_close = (not pd.isna(dist_resistance)) and dist_resistance <= 0.025
    
    long_ok = bull and (near_support == 1 or support_close or rej_up == 1)
    short_ok = bear and (near_resistance == 1 or resistance_close or rej_down == 1)
    
    # импульс не запрещает setup полностью, только ослабляет его
    if impulse == 1 and not (rej_up == 1 or rej_down == 1):
        return False
    
    return bool(long_ok or short_ok)


def _trade_outcome_long(close_series: pd.Series, idx: int, tp_pct: float, sl_pct: float):
    if idx + TARGET_HORIZON_BARS_15M >= len(close_series):
        return None
    
    entry = float(close_series.iloc[idx])
    tp = entry * (1 + tp_pct)
    sl = entry * (1 - sl_pct)
    
    future = close_series.iloc[idx + 1: idx + 1 + TARGET_HORIZON_BARS_15M]
    if len(future) < TARGET_HORIZON_BARS_15M:
        return None
    
    for px in future:
        if px >= tp:
            return 1
        if px <= sl:
            return 0
    
    final_px = float(future.iloc[-1])
    return 1 if final_px > entry else 0


def _trade_outcome_short(close_series: pd.Series, idx: int, tp_pct: float, sl_pct: float):
    if idx + TARGET_HORIZON_BARS_15M >= len(close_series):
        return None
    entry = float(close_series.iloc[idx])
    tp = entry * (1 - tp_pct)
    sl = entry * (1 + sl_pct)
    
    future = close_series.iloc[idx + 1: idx + 1 + TARGET_HORIZON_BARS_15M]
    if len(future) < TARGET_HORIZON_BARS_15M:
        return None
    
    for px in future:
        if px <= tp:
            return 1
        if px >= sl:
            return 0
    
    final_px = float(future.iloc[-1])
    return 1 if final_px < entry else 0


def _build_side_target(row: pd.Series, close_series: pd.Series, idx: int):
    bull, bear = _h4_trend_flags(row)
    
    near_support = int(row.get("near_support", 0))
    near_resistance = int(row.get("near_resistance", 0))
    
    dist_support = row.get("dist_to_support_pct", np.nan)
    dist_resistance = row.get("dist_to_resistance_pct", np.nan)
    
    rej_up = int(row.get("rejection_up", 0))
    rej_down = int(row.get("rejection_down", 0))
    
    support_close = (not pd.isna(dist_support)) and dist_support <= 0.025
    resistance_close = (not pd.isna(dist_resistance)) and dist_resistance <= 0.025
    
    long_setup = bull and (near_support == 1 or support_close or rej_up == 1)
    short_setup = bear and (near_resistance == 1 or resistance_close or rej_down == 1)
    
    if long_setup and not short_setup:
        return _trade_outcome_long(close_series, idx, TP_PCT, SL_PCT)
    
    if short_setup and not long_setup:
        return _trade_outcome_short(close_series, idx, TP_PCT, SL_PCT)
    
    # если оба или ни один — берём по более близкой зоне
    if long_setup and short_setup:
        ds = dist_support if not pd.isna(dist_support) else 999.0
        dr = dist_resistance if not pd.isna(dist_resistance) else 999.0
        if ds < dr:
            return _trade_outcome_long(close_series, idx, TP_PCT, SL_PCT)
        elif dr < ds:
            return _trade_outcome_short(close_series, idx, TP_PCT, SL_PCT)
    
    return None


def build_symbol_dataset(exchange, symbol: str) -> pd.DataFrame:
    h4 = fetch_ohlcv_df(exchange, symbol, TF_HIGH, HIST_LIMIT_HIGH)
    m15 = fetch_ohlcv_df(exchange, symbol, TF_ENTRY, HIST_LIMIT_ENTRY)
    
    if h4 is None or h4.empty:
        raise ValueError(f"{symbol}: пустой h4 dataframe")
    
    if m15 is None or m15.empty:
        raise ValueError(f"{symbol}: пустой m15 dataframe")
    
    h4 = add_h4_features(h4)
    
    supports, resistances = build_zones(h4)
    
    h4_ctx = h4[
        ["timestamp", "h4_ema20_above_50", "h4_ema50_above_100", "h4_adx", "h4_rsi", "close"]
    ].copy()
    h4_ctx = h4_ctx.rename(columns={"close": "h4_close"})
    
    zone_rows = []
    for _, row in h4.iterrows():
        zone_rows.append(
            {
                "timestamp": row["timestamp"],
                **_safe_zone_features(float(row["close"]), supports, resistances)
            }
        )
    
    zone_df = pd.DataFrame(zone_rows)
    h4_ctx = h4_ctx.merge(zone_df, on="timestamp", how="left")
    
    m15 = add_15m_features(m15)
    
    merged = pd.merge_asof(
        m15.sort_values("timestamp"),
        h4_ctx.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )
    
    if merged.empty:
        raise ValueError(f"{symbol}: merge_asof дал пустой dataframe")
    
    merged["impulse_candle"] = merged.apply(_is_impulse_candle, axis=1)
    merged["rejection_up"] = merged.apply(_rejection_up, axis=1)
    merged["rejection_down"] = merged.apply(_rejection_down, axis=1)
    merged["valid_setup"] = merged.apply(_valid_setup, axis=1).astype(int)
    
    close_series = merged["close"].reset_index(drop=True)
    targets = []
    
    for i, row in merged.reset_index(drop=True).iterrows():
        if row["valid_setup"] != 1:
            targets.append(None)
            continue
        
        target = _build_side_target(row, close_series, i)
        targets.append(target)
    
    merged["target"] = targets
    
    merged = merged.dropna(subset=["target"]).reset_index(drop=True)
    
    if merged.empty:
        raise ValueError(f"{symbol}: после фильтра valid_setup + target датасет пустой")
    
    merged["target"] = merged["target"].astype(int)
    
    lagged = build_lagged_features(merged)
    if lagged.empty:
        raise ValueError(f"{symbol}: build_lagged_features вернул пустой dataframe")
    
    keep_cols = [
        "timestamp",
        "close",
        "target",
        "valid_setup",
        "impulse_candle",
        "rejection_up",
        "rejection_down",
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
        "support_strength",
        "resistance_strength",
        "support_width_pct",
        "resistance_width_pct",
    ]
    
    aligned = merged.iloc[-len(lagged):].reset_index(drop=True)
    lagged = lagged.reset_index(drop=True)
    
    for col in keep_cols:
        if col in aligned.columns and col not in lagged.columns:
            lagged[col] = aligned[col]
    
    lagged["symbol"] = symbol
    lagged = lagged.replace([np.inf, -np.inf], 0).fillna(0)
    
    if lagged.empty:
        raise ValueError(f"{symbol}: после подготовки lagged датасет пустой")
    
    return lagged


def build_all_dataset() -> pd.DataFrame:
    exchange = make_exchange()
    frames = []
    
    for symbol in SYMBOLS:
        log(f"Building dataset for {symbol}")
        try:
            df_symbol = build_symbol_dataset(exchange, symbol)
            if df_symbol is not None and not df_symbol.empty:
                frames.append(df_symbol)
                log_green(f"{symbol}: rows={len(df_symbol)}")
            else:
                log(f"{symbol}: пустой dataset")
        except Exception as e:
            log(f"Skip {symbol}: {e}")
    
    if not frames:
        raise ValueError(
            "Не удалось собрать ни одного датасета. Проверь SYMBOLS, уровни, признаки и разметку target."
        )
    
    dataset = pd.concat(frames, axis=0, ignore_index=True)
    
    csv_path = DATASET_PATH.replace(".parquet", ".csv")
    dataset.to_csv(csv_path, index=False)
    
    log_green(f"Dataset saved: {csv_path} | rows={len(dataset)}")
    return dataset


if __name__ == "__main__":
    build_all_dataset()




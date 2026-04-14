import os
import joblib
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

from config import DATASET_PATH, MODEL_PATH
from utils import log, log_green, log_red


def load_dataset():
    csv_path = DATASET_PATH.replace(".parquet", ".csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Файл датасета не найден: {csv_path}\n"
            f"Сначала запусти: python dataset_builder.py"
        )

    df = pd.read_csv(csv_path)

    if df.empty:
        raise ValueError("Датасет пустой")

    return df


def prepare_features(df: pd.DataFrame):
    if "target" not in df.columns:
        raise ValueError("В датасете нет колонки target")

    df = df.dropna(subset=["target"]).copy()
    df["target"] = df["target"].astype(int)

    # служебные колонки, которые не должны идти в модель
    drop_cols = {
        "target",
        "symbol",
        "timestamp",
        "close",
    }

    feature_cols = [c for c in df.columns if c not in drop_cols]

    if not feature_cols:
        raise ValueError("После фильтрации не осталось признаков для обучения")

    X = df[feature_cols].copy()
    y = df["target"].copy()

    # заменяем inf и nan
    X = X.replace([float("inf"), float("-inf")], 0).fillna(0)

    return X, y, feature_cols


def train():
    df = load_dataset()

    log(f"Dataset rows: {len(df)}")

    X, y, feature_cols = prepare_features(df)

    class_counts = y.value_counts().to_dict()
    log(f"Class distribution: {class_counts}")

    if y.nunique() < 2:
        raise ValueError("В target только один класс. Обучение невозможно.")

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        shuffle=True,
        random_state=42,
        stratify=y,
    )

    model = RandomForestClassifier(
        n_estimators=400,
        max_depth=12,
        min_samples_leaf=4,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )

    log("Start training model...")
    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    report = classification_report(
        y_test,
        preds,
        digits=4,
        zero_division=0,
    )

    log("Classification report:")
    log(report)

    # сохраним имена признаков прямо в объекте модели
    model.feature_names_in_ = feature_cols

    joblib.dump(model, MODEL_PATH)
    log_green(f"Model saved: {MODEL_PATH}")


if __name__ == "__main__":
    try:
        train()
    except Exception as e:
        log_red(f"Training error: {e}")
        raise

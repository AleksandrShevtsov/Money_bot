import requests


def fetch_funding_rate(symbol, base_url="https://fapi.binance.com"):
    try:
        response = requests.get(
            f"{base_url.rstrip('/')}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        return float(data.get("lastFundingRate", 0.0) or 0.0)
    except Exception:
        return None


def classify_funding_context(
    symbol,
    funding_rate=None,
    strong_threshold=0.0008,
    extreme_threshold=0.0015,
):
    if funding_rate is None:
        funding_rate = fetch_funding_rate(symbol)

    if funding_rate is None:
        return {
            "rate": None,
            "label": "funding_unavailable",
            "continuation_bias": "NEUTRAL",
            "reversal_bias": "NEUTRAL",
            "score_bias": 0.0,
        }

    if funding_rate >= extreme_threshold:
        return {
            "rate": funding_rate,
            "label": "extreme_positive_funding",
            "continuation_bias": "BUY",
            "reversal_bias": "SELL",
            "score_bias": -0.08,
        }

    if funding_rate >= strong_threshold:
        return {
            "rate": funding_rate,
            "label": "positive_funding",
            "continuation_bias": "BUY",
            "reversal_bias": "SELL",
            "score_bias": -0.04,
        }

    if funding_rate <= -extreme_threshold:
        return {
            "rate": funding_rate,
            "label": "extreme_negative_funding",
            "continuation_bias": "SELL",
            "reversal_bias": "BUY",
            "score_bias": -0.08,
        }

    if funding_rate <= -strong_threshold:
        return {
            "rate": funding_rate,
            "label": "negative_funding",
            "continuation_bias": "SELL",
            "reversal_bias": "BUY",
            "score_bias": -0.04,
        }

    return {
        "rate": funding_rate,
        "label": "neutral_funding",
        "continuation_bias": "NEUTRAL",
        "reversal_bias": "NEUTRAL",
        "score_bias": 0.0,
    }

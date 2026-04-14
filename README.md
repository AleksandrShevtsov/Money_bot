# Whale Flow Bot

Торговый бот для крипто-фьючерсов с `paper` и `real` режимами.

## Что реально делает проект

Бот использует:
- Binance Futures candles для price action, market structure и volatility context;
- live Binance websocket (`aggTrade` + `bookTicker`) для orderflow и дисбаланса стакана;
- Bybit open interest для контекста набора/сброса позиций;
- BingX REST API для реального исполнения;
- локальное сохранение runtime state и базовую сверку позиций с биржей.

## Как устроена стратегия

Стратегия теперь не опирается на один фиксированный триггер. Она собирает weighted signal из нескольких блоков:
- `price action`: range breakout, trendline breakout, retest;
- `orderflow`: дисбаланс buy/sell и стакана;
- `OI context`: `long build-up`, `short build-up`, `short covering`, `long liquidation`;
- `HTF alignment`: совпадение с 4h-трендом и market structure;
- `liquidity`: sweep high/low и volume-profile уровни;
- `regime adjustment`: trend day, range day, squeeze, high-volatility panic;
- `exhaustion penalty`: штраф за поздние и слабые входы, особенно на альтах.

Дополнительно стратегия использует:
- ATR/volatility-normalized thresholds вместо полностью фиксированных порогов;
- false-breakout filter;
- multi-bar breakout hold confirmation;
- time-based entry filters;
- отдельный профиль для `BTC/ETH` и для альтов.

## Выходы и сопровождение

Бот поддерживает:
- partial close;
- break-even;
- trailing stop;
- early exit without follow-through;
- reverse signal exit;
- adverse orderflow / OI exit;
- liquidity target exit.

## Основные файлы

- `main.py` — основной цикл, интеграция стратегии, риск-слоя и исполнения.
- `strategy.py` — weighted scoring и сборка торгового сигнала.
- `volatility_regime.py` — ATR, realized volatility и market regimes.
- `oi_context.py` — интерпретация связки `price + open interest`.
- `liquidity_levels.py` — volume profile, HVN/LVN, liquidity sweep, false breakout.
- `confirmation_filters.py` — multi-bar hold confirmation.
- `time_filters.py` — временные ограничения на вход.
- `smart_exit_manager.py` — логика сопровождения и выхода.
- `executors/bingx_real_executor.py` — real execution и чтение позиций BingX.
- `bot_state_store.py`, `exchange_state_sync.py`, `risk_guard.py`, `feed_health.py` — устойчивость рантайма, risk limits и health checks.

## Установка

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Настройка

1. Скопируй `.env.example` в `.env`.
2. Для первого запуска оставь `EXECUTION_MODE=paper`.
3. Заполни Telegram-поля, если нужны уведомления.
4. Заполни BingX-ключи только для `real` режима.
5. При необходимости задай risk limits:
   `DAILY_LOSS_LIMIT_USDT`, `MAX_CONSECUTIVE_LOSSES`, `MAX_TOTAL_DRAWDOWN_PCT`.

## Запуск

```bash
python main.py
```

## Важно

- Секреты нельзя хранить в `config.py` или в git.
- Если ключи уже были опубликованы, их нужно перевыпустить.
- Перед `real` режимом нужен paper-forward test.
- BingX REST-часть и синхронизацию позиций лучше дополнительно проверить на безопасном аккаунте.

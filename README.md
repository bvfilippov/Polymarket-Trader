# Polymarket BTC Lag-Arbitrage Bot

Цена на Polymarket BTC-прогнозах **следует за ценой битка на Binance с задержкой**. Этот бот ловит движения на Binance и торгует на Polymarket до того как цена подстроится.

## Принцип

```
Binance: BTC растёт +0.3% за 10 секунд
         │
         │  задержка (секунды → минуты)
         ▼
Polymarket: "BTC above $100k?" — цена YES всё ещё старая
         │
         ▼
Бот: BUY YES (limit order) → мониторим позицию → EXIT когда Polymarket подтянется
```

**Не предсказываем** направление. **Реагируем** на движение которое уже произошло на Binance, но ещё не отразилось на Polymarket.

## Архитектура

```
Binance WebSocket                    Polymarket WebSocket
  ├── aggTrade → цена, сделки        ├── market → цены в реальном времени
  ├── kline_1m → свечи               └── book → orderbook updates
  └── depth20  → стакан
         │                                    │
         ▼                                    ▼
  ┌─────────────────────┐          ┌─────────────────────┐
  │  SpikeDetector      │          │  PolymarketStream   │
  │  LiveMarketState    │          │  (real-time prices)  │
  └────────┬────────────┘          └────────┬────────────┘
           │                                │
     ┌─────┴──────┐                         │
     ▼            ▼                         │
  [SPIKE]     [PERIODIC]                    │
  мгновенно   каждые 2с                    │
     │            │                         │
     ▼            ▼                         ▼
  Estimate fair price (should be) ←── Use WS prices (no REST lag)
  Compare vs stale price (is now)
  If lag > MIN_EDGE → LIMIT ORDER
           │
           ▼
  ┌─────────────────────┐
  │  PositionManager    │
  │  ├── Take-profit    │  ← Polymarket догнала → SELL
  │  ├── Stop-loss      │  ← BTC развернулся → SELL
  │  └── Timeout        │  ← слишком долго держим → SELL
  └─────────────────────┘
           │
           ▼
  ┌─────────────────────┐
  │  LagTracker         │
  │  Измеряет реальный  │
  │  лаг для калибровки │
  └─────────────────────┘
```

## Два режима срабатывания

1. **Spike-triggered** — BTC сделал резкое движение → бот мгновенно обновляет цены Polymarket → считает lag → торгует. Минимальная задержка.

2. **Periodic** — каждые 2 секунды проверяет накопленные движения за 10/30/60 секунд. Ловит более медленные, но всё ещё не отражённые движения.

## Как считается edge

Для рынка "BTC above $100k?" с YES = $0.35:

1. BTC вырос на 0.3% за 30 секунд ($99,700 → $100,000)
2. Бот оценивает, что fair YES price = $0.42 (ближе к таргету)
3. Polymarket ещё показывает $0.35 (stale)
4. Edge = 0.42 - 0.35 = **0.07 (7%)**
5. MIN_EDGE = 0.02 → сигнал на BUY YES

Модель учитывает:
- Расстояние до ценового таргета (ближе = больший сдвиг вероятности)
- Пересечение таргета (крестим $100k = резкий скачок вероятности)
- Объём при спайке (больше объём = надёжнее движение)

## Управление позициями

Бот не просто покупает — он **управляет позициями**:

- **Take-profit**: когда Polymarket цена догнала ≥70% от нашего fair value → SELL
- **Stop-loss**: BTC развернулся ≥0.15% против нашего направления → SELL
- **Timeout**: позиция открыта >5 мин (лаг должен был закрыться) → SELL

## Limit orders

Вместо FOK market orders (проскальзывание на тонком стакане), бот ставит **limit orders** по цене чуть выше stale — получаем лучший fill, всё ещё ниже fair value.

## Измерение лага

**LagTracker** записывает:
- Когда BTC двинулся на Binance
- Когда Polymarket подстроилась

Собирает статистику: средний лаг, медиана, ошибка предсказания fair price. Используется для калибровки модели.

## Установка

```bash
pip install -r requirements.txt
cp .env.example .env
# Добавьте POLYMARKET_PRIVATE_KEY
```

## Использование

```bash
python main.py snapshot   # Текущие движения BTC + данные для lag-arb
python main.py markets    # Активные BTC-рынки на Polymarket
python main.py once       # Один цикл оценки (REST)
python main.py run        # Нон-стоп бот (WebSocket)
```

## Конфигурация

| Переменная | Описание | По умолчанию |
|---|---|---|
| `SPIKE_THRESHOLD_PCT` | % движения для срабатывания спайка | 0.15 |
| `SPIKE_WINDOWS` | Окна отслеживания (секунды) | 10,30,60 |
| `SIGNAL_EVAL_INTERVAL` | Периодическая проверка (сек) | 2.0 |
| `MARKET_REFRESH_INTERVAL` | Обновление рынков Polymarket (сек) | 30 |
| `TRADE_COOLDOWN` | Пауза между сделками на рынке (сек) | 15 |
| `MIN_EDGE` | Минимальный lag для торговли | 0.02 |
| `MAX_POSITION_SIZE` | Макс. позиция (USDC) | 10.0 |
| `LARGE_TRADE_THRESHOLD` | Порог whale detection (USDT) | 50000 |
| `DRY_RUN` | Без реальных сделок | true |
| `TAKE_PROFIT_RATIO` | Закрыть при % закрытия лага | 0.7 |
| `STOP_LOSS_BTC_REVERSAL` | Stop-loss при развороте BTC (%) | 0.15 |
| `MAX_POSITION_AGE` | Макс. время удержания (сек) | 300 |
| `USE_LIMIT_ORDERS` | Limit orders вместо FOK | true |
| `LIMIT_ORDER_OFFSET` | Сдвиг цены limit order | 0.005 |

# Call Integration Service — согласованный план реализации

**Статус:** архитектурные решения согласованы; реализация не начата.  
**Назначение:** единый контракт для владельца проекта, Codex и ревью.  
**Принцип:** строго выполнить тестовое задание; технические дополнения делать только для доказуемой надёжности, тестируемости и наблюдаемости. Не добавлять функциональность «на всякий случай».

## 1. Цель и обязательный сценарий работодателя

Реализовать Python 3.10+ сервис с FastAPI, MongoDB, RabbitMQ и Docker Compose:

1. `docker compose up` поднимает MongoDB, RabbitMQ и FastAPI-приложение.
2. `python producer.py` на **хосте** публикует четыре сообщения: по два звонка для двух кампаний.
3. `GET /campaigns/camp_001/calls` возвращает **ровно два звонка** (JSON; схема Pydantic в `/docs`).
4. Неизвестная кампания: HTTP 200 с **точным текстом** `Кампания не найдена`.
5. Некорректные сообщения логируются, не приводят к падению консьюмера.
6. Консьюмер работает **внутри FastAPI-приложения**, а ACK отправляется только после завершения успешной записи в MongoDB.
7. Сдаётся Git-репозиторий с кодом, `Dockerfile`, `docker-compose.yml`, README с тремя командами проверки.

**Важно:** установка локальных Python-зависимостей (`python -m pip install -e .`) описывается как предварительный шаг для запуска `producer.py` на хосте. Docker Compose собирает приложение автономно. Воспроизводимый сценарий не требует ручной настройки MongoDB или RabbitMQ.

## 2. Согласованный стек

| Область | Решение | Мотивация |
|---|---|---|
| Python | 3.12 в Docker; заявленная совместимость с Python 3.10+ — только если реально проверена | Современный runtime без необоснованного заявления о поддержке |
| API | FastAPI, Pydantic v2 | Стандартный async API + OpenAPI |
| MongoDB | PyMongo Async (`AsyncMongoClient`) | Поддерживаемый асинхронный драйвер, без Motor |
| RabbitMQ | RabbitMQ 4.3.x (фиксированный рабочий тег), основная quorum queue | Native delayed retry, delivery limit, at-least-once dead-lettering |
| AMQP клиент | aio-pika (`connect_robust`) | Асинхронные соединения и восстановление |
| Логи | structlog, JSON в Docker, console локально | Структурированные события и контекст обработки |
| Конфигурация | pydantic-settings + `.env.example` | Проверяемые настройки |
| Зависимости | `pyproject.toml`, pip/editable install | Минимум инструментальных накладных расходов |
| Качество | pytest, pytest-asyncio, httpx, Ruff, Pyright | Автотесты + типизация |
| Запуск | Docker Compose, один Uvicorn worker | Требование «consumer внутри FastAPI», контроль жизненного цикла |

Версии базовых контейнеров и библиотек фиксировать воспроизводимым образом; не использовать `latest`.

## 3. Контракт сообщения RabbitMQ

```json
{
  "schema_version": 1,
  "campaign": {
    "campaign_id": "camp_001",
    "name": "Напоминание о платеже",
    "created_at": "2026-10-20T10:00:00Z"
  },
  "call": {
    "call_id": "call_0001",
    "campaign_id": "camp_001",
    "phone": "79001234567",
    "status": "success",
    "created_at": "2026-10-20T10:06:00Z"
  }
}
```

- `message_id` задаётся в **AMQP properties**, не дублируется в JSON.
- Схема версионируется (`schema_version = 1`). Неизвестную версию отклоняем как permanent message error.
- ID и имя — обязательные непустые строки. `phone` — **строка**, а не число; не вводить неподтверждённый ТЗ regex номера. `status` — обязательная непустая строка, **не ограничивать** enum `success/failed`, которого нет в ТЗ.
- Все `created_at` — timezone-aware; нормализовать к UTC. MongoDB хранит BSON datetime (миллисекундная точность), сравнение дубликатов нормализовать к этой точности.
- `campaign.campaign_id == call.campaign_id`. Валидацию всего события закончить **до первой записи**.
- Лишние поля и нарушения схемы обрабатываются согласно явно выбранной Pydantic policy; предпочтительно `extra='forbid'`, зафиксировать это в тестах.
- **Бизнес-предположение:** результаты звонков неизменяемы (immutable final result). Это **не требование ТЗ**, а объявленное нами допущение, которое требуется объяснить в README.

## 4. MongoDB: коллекции и инварианты

**Коллекции**: `campaigns` (`campaign_id`, `name`, `created_at`) и `calls` (`call_id`, `campaign_id`, `phone`, `status`, `created_at`). В API не отдавать Mongo `_id`.

**Индексы:** `campaigns.campaign_id` UNIQUE; `calls.call_id` UNIQUE; `calls.campaign_id` обычный.

**Алгоритм:**

1. Провалидировать событие полностью.
2. Идемпотентно создать кампанию с `$setOnInsert`; если она уже есть, сравнить нормализованные бизнес-поля (конфликтующие значения — permanent error).
3. Идемпотентно создать звонок с `$setOnInsert`; существующий идентичный документ = успешный повтор, несовпадающий = permanent error.
4. Отправлять ACK после завершения **обоих** шагов и проверки конфликтов.
5. Обрабатывать конкурентный `DuplicateKeyError` чтением сохранённого документа и сравнением: не считать любой duplicate-key успехом.

**Без multi-document транзакции:** наличие кампании без звонков — допустимое промежуточное состояние. После сбоя между двумя upsert повторная доставка достроит отсутствующий звонок. Транзакция не сделала бы атомарной связку MongoDB+RabbitMQ и не отменила бы необходимость идемпотентности.

## 5. RabbitMQ: топология и delivery semantics

**Основная очередь:** `calls.queue`, type `quorum`, durable.  
**DLX:** `calls.dlx` (direct exchange).  
**DLQ:** `calls.dlq`, durable, с binding к DLX. Не включать автоматический обратный цикл DLQ → основная очередь.

**Политика основной очереди** (RabbitMQ 4.3+, проверить реальное применение в Management UI / HTTP API):

- `delayed-retry-type=failed`, `delayed-retry-min=2000`, `delayed-retry-max=10000` (мс, линейная задержка);
- `delivery-limit=5` (**граничные условия и фактическое число доставок проверить integration-тестом**);
- `dead-letter-exchange=calls.dlx`, корректный routing key;
- `dead-letter-strategy=at-least-once`, `overflow=reject-publish`;
- при необходимости лимит длины очереди с учётом удерживаемых dead-letter сообщений; не допускать скрытой потери данных.

На стороне консьюмера: manual ACK, `prefetch_count=1` для простого и детерминированного тестового. Сообщения persistent. Продюсер использует publisher confirms, `mandatory=True` и обрабатывает unroutable/negative-confirm ошибки.

**Ключевой RabbitMQ 4.3 нюанс:** при AMQP 0-9-1 `reject(requeue=True)` считается неудачной доставкой и участвует в `delivery-limit`; `nack(requeue=True)` не увеличивает этот счётчик, поэтому не использовать его вместо `reject` в нашем transient-retry. Применение конкретных policies проверить реальным брокером.

### Матрица исходов

| Ситуация | Действие |
|---|---|
| Сохранён новый звонок | ACK |
| Идентичная повторная доставка | ACK |
| Невалидный JSON, Pydantic, неподдерживаемая версия, несовпадение ID | Логируем безопасные поля → `reject(requeue=False)` → DLX/DLQ |
| Конфликт неизменяемого `call_id` или `campaign_id` | `reject(requeue=False)` → DLQ |
| Кратковременный сбой БД (при работающей в целом БД) | `reject(requeue=True)` → native delayed retry → после лимита DLQ |
| Полная длительная недоступность MongoDB | Пауза приёма новых сообщений; не выжигать лимит доставки по всем сообщениям; readiness=503; после восстановления возобновление |
| Ошибки конфигурации/прав/неожиданное исключение | ERROR/CRITICAL с traceback; остановка потребления / нездоровое состояние, без ACK; не гнать поток корректных сообщений в DLQ |
| Потеря канала/процесса до ACK | Сообщение может быть доставлено повторно; upsert делает повтор безопасным |

**`reject(requeue=False)` означает запрос брокеру на dead-lettering, а не синхронное доказательство, что сообщение уже физически в DLQ.** Факт проверяется интеграционным тестом. Если DLX недоступен, at-least-once dead-lettering удерживает исходное сообщение с оговорками RabbitMQ.

Для глобального отказа БД порог и возобновление сделать **минимальными**, без отдельного circuit-breaker фреймворка; не блокировать event loop синхронными ожиданиями. Необходимо тестировать оба случая — локальный transient и глобальную потерю БД.

## 6. HTTP API

- `GET /campaigns/{campaign_id}/calls` — Pydantic response model со списком звонков **только заданной** кампании, без `_id`; ISO8601 timestamps.
- Если кампания существует, но звонков нет → `200` + `[]`.
- Если кампании нет → `200` + **plain text** `Кампания не найдена` (точное требование работодателя). В OpenAPI документировать оба варианта ответа, не заменять на 404.
- `GET /health` — liveness, без синхронной проверки внешних сервисов, 200 для живого процесса.
- `GET /ready` — readiness, MongoDB ping с коротким timeout + RabbitMQ connection/channel + жизнеспособность консьюмера; 200 либо 503.
- Не добавлять посторонние бизнес-эндпоинты.

## 7. Lifecycle и инфраструктура

- FastAPI `lifespan` создаёт и закрывает MongoDB-клиент и RabbitMQ-соединение, запускает и корректно останавливает consumer.
- Обнаруживать неожиданное завершение фоновой задачи: `/ready` не должен показывать READY при умершем consumer; инициировать управляемое завершение/перезапуск контейнера, если recovery невозможен.
- На startup использовать ограниченные попытки подключения + fail-fast (после исчерпания завершить приложение с ненулевым кодом); Docker Compose healthchecks и restart policy. Для краткосрочного отключения RabbitMQ — robust reconnect.
- При shutdown: прекратить новые доставки, дождаться in-flight в рамках timeout, затем закрыть канал. Не превращать `CancelledError` в `reject` или DLQ.
- Один Uvicorn worker; несколько worker-процессов запустили бы несколько консьюмеров. В реальном отдельном проде сервисы можно масштабировать иначе, но ТЗ требует consumer внутри FastAPI.
- Compose хранит MongoDB и RabbitMQ данные в volumes, задаёт healthchecks, публикует API и AMQP port; RabbitMQ Management UI — только для локальной разработки.
- **Брокерная топология как код:** `rabbitmq/definitions.json` плюс `rabbitmq/rabbitmq.conf` (или другой полностью автоматизированный, воспроизводимый механизм) с at-boot import. Важно, чтобы неточности RabbitMQ policies выявлялись integration-проверкой, а не оставались предположением.
- Корневой `producer.py` подключается к хостовому `localhost:5672`; контейнер приложения — к сервису `rabbitmq:5672`; без ручного редактирования файлов.

## 8. Наблюдаемость и безопасность

- `structlog`, локально ConsoleRenderer, внутри Docker JSONRenderer (один JSON на строку) в stdout/stderr; интегрировать обычные Uvicorn/FastAPI-логи.
- Рекомендуемые поля: UTC timestamp, level, event, service, component, message_id, call_id, campaign_id, attempt/delivery_count, error_type, action. Для каждого сообщения очищать contextvars в начале/конце обработки.
- Использовать понятные события: `consumer_started`, `message_invalid`, `call_saved`, `duplicate_ignored`, `db_retry_scheduled`, `message_rejected`, `consumer_stopped`, `consumer_failed`.
- Не выводить полный `phone`, тело события, креды и сырой Pydantic errors() с input; для диагностики — имена полей, типы ошибок, IDs и безопасный контекст.
- Не писать `message_saved_to_dlq` сразу после reject — корректнее `message_rejected` / `dead_letter_requested`.
- Настройки/секреты не коммитить в `.env`; только `.env.example` с безопасными значениями для локальной разработки.

## 9. Модули и границы ответственности

```text
call-integration/
├── src/call_service/
│   ├── __init__.py
│   ├── main.py          # app factory / lifespan, wiring
│   ├── config.py        # pydantic-settings
│   ├── log_config.py    # structlog
│   ├── schemas.py       # events + HTTP models
│   ├── api.py           # routers
│   ├── consumer.py      # Rabbit delivery / ack / reject / lifecycle
│   ├── handler.py       # validation + domain flow
│   ├── mongo.py         # indexes + idempotent read/write
│   ├── rabbit.py        # connection & broker topology validation
│   └── exceptions.py   # meaningful error categories
├── tests/{unit,integration,e2e}/
├── rabbitmq/{rabbitmq.conf,definitions.json}
├── producer.py
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

Без Celery, Redis, отдельного worker-процесса, стороннего DI-фреймворка, MongoDB ORM и иерархий абстрактных классов. `handler` не должен импортировать aio-pika; `consumer` не должен знать, как устроены документы MongoDB; API и consumer используют один repository в рамках app instance.

## 10. Этапы для Codex: выполнять строго последовательно

Codex после **каждого** этапа **останавливается** и присылает: список изменённых файлов, краткое описание решений, выполненные команды + точный результат, остаточные проблемы, предполагаемый следующий шаг. Следующий этап — только после ревью.

### Этап 1. Скелет проекта и воспроизводимая инфраструктура

**Задача:** создать `pyproject.toml` (runtime/dev dependencies), Python-пакет, `config.py`, `log_config.py`, app factory/lifespan skeleton, Dockerfile, Compose, RabbitMQ config + definitions/policies, `.env.example`, `.gitignore`. Сервис должен стартовать, отвечать на простой `/health`, а БД и брокер — быть доступны. Реализацию бизнес-логики **не делать**.

**Приёмка:** `docker compose up --build` создаёт три сервиса без ручных действий; RabbitMQ версии 4.3+, policies реально действуют на `calls.queue`, DLX/DLQ привязаны; `/health` = 200; установка editable package выполняется; Ruff/Pyright без ошибок по существующему коду.

### Этап 2. Контракт событий, MongoDB и unit/integration тесты хранилища

**Задача:** `schemas.py`, `mongo.py`, `exceptions.py`, `handler.py` без знания RabbitMQ. Валидировать событие; создать индексы; сделать безопасный `setOnInsert` + сравнение существующего документа, обработать гонку duplicate key. Настроить UTC datetimes и конвертацию BSON precision.

**Приёмка:** валидация до записей; две кампании/четыре звонка, идемпотентный повтор, конфликт → typed error; сбой после записи кампании допускает успешный повтор; unit + Mongo integration тесты; Ruff/Pyright/pytest pass.

### Этап 3. RabbitMQ producer + consumer + обработка сбоев

**Задача:** рабочий корневой `producer.py`, `rabbit.py`, `consumer.py`: publisher confirms и mandatory, persistent events, фоновой consumer в lifespan, manual ACK, корректная маршрутизация ошибок, prefetch=1, native retry, DLQ, robust connection. Логирование ошибок без PII, контролируемое shutdown.

**Приёмка:** 4 сообщения записаны; ACK строго после успешной БД; дубликат ACK; invalid → DLQ и следующий valid обработан; кратковременный сбой → отложенная повторная доставка; при падении до ACK повтор без дубликата; timeout/конфигурационные ошибки не маскируются успешными ACK. Реальная проверка delivery-limit и политики RabbitMQ.

### Этап 4. Полный API, readiness и устойчивость жизненного цикла

**Задача:** `GET /campaigns/{campaign_id}/calls`, `/ready`, окончательная реализация lifespan/supervision/shutdown/startup recovery, Swagger для успешного JSON и отсутствующей кампании с текстом 200.

**Приёмка:** у `camp_001` ровно два звонка; другая кампания не загрязняет ответ; существующая кампания без звонков → `[]`; отсутствующая → HTTP 200 + точный текст; схема звонка видна в `/docs`; `/ready` 503 при потере Mongo/Rabbit/consumer и 200 после восстановления; неожиданный сбой consumer виден и приводит к управляемому recovery/restart.

### Этап 5. E2E, чистка, README и сдача

**Задача:** полный E2E с реальными RabbitMQ/Mongo/FastAPI, проверка сценария работодателя с хоста, повторного запуска producer, DLQ и продолжения работы после ошибки; интеграционные тесты для временных отказов. Удалить действительно дублирующиеся тесты, но не терять покрытие сценариев. README с точными командами и блоком обоснования решений.

**Приёмка:** `docker compose down -v` (ТОЛЬКО тестовая среда!) → `docker compose up --build` → `python producer.py` → GET показывает два звонка; повтор producer → снова два; malformed → DLQ и valid после него обрабатывается; `pytest` (unit/integration/e2e согласно документации), Ruff, Pyright проходят; README воспроизводим с чистой машины/окружения. Никаких незафиксированных секретов и «временных» хаков.

## 11. Обязательный раздел README «Архитектурные решения»

Коротко объяснить не только *что* выбрано, но и *почему*:

1. Consumer внутри FastAPI — буквальное требование ТЗ; один worker и lifecycle через lifespan.
2. Quorum queue + native delayed retry — брокерный backoff вместо ручного цикла TTL-очередей, учитываем RabbitMQ 4.3+.
3. DLQ и at-least-once dead-lettering — некорректные события не блокируют поток, не теряем их при временном отказе DLQ (с оговорками по broker topology).
4. Manual ACK после двух записей — at-least-once обработка, **не exactly-once delivery**.
5. Unique indexes + `$setOnInsert` + сравнение данных — идемпотентность, корректная обработка повторов и конфликтов.
6. Отказ от multi-document транзакции — кампания без звонков допустима, частичное состояние исправляется повторной доставкой; MongoDB транзакция не атомарна с RabbitMQ ACK.
7. Immutable call result — наше явное допущение ввиду отсутствия изменений статусов в ТЗ.
8. Structlog JSON, без телефонов/payload — трассируемость и минимизация утечек PII.
9. `/health` и `/ready` — разделяем живость приложения и готовность интеграций.
10. Ограничения: один экземпляр RabbitMQ и MongoDB в Compose **не дают HA**; DLQ требует наблюдения и ручного redrive; при полной недоступности БД обработка приостанавливается; production deployment потребовал бы кластеризации, секрет-менеджмента и внешнего мониторинга.

Раздел README должен быть **коротким, техническим, на русском**, без громких утверждений «100% гарантия доставки».

## 12. Протокол работы с Codex и ревью

- Codex получает **этот документ + только текущий этап**. Не переходить к следующему этапу самостоятельно.
- Запрет на произвольное расширение требований и преждевременную реализацию этапов.
- Попросить Codex оставлять короткий отчёт: изменённые файлы, архитектурные решения, проверки и их вывод, что не удалось проверить.
- После отчёта вместе проводим ревью **диффа**, тестов, логов и крайних случаев; фиксируем баги отдельным коротким корректирующим заданием.
- Не заявлять «All checks passed», если какие-то проверки не запускались или контейнеры недоступны. Отделять unit-проверки от реального E2E.
- Изменение согласованной архитектуры — только по явному решению владельца проекта и с обновлением этого плана и README.

## Полезные первоисточники

- Исходное тестовое задание (файл, приложенный в диалоге).
- RabbitMQ Quorum Queues: https://www.rabbitmq.com/docs/quorum-queues
- RabbitMQ definitions import: https://www.rabbitmq.com/docs/definitions
- RabbitMQ publisher confirms: https://www.rabbitmq.com/docs/confirms
- MongoDB PyMongo Async: https://www.mongodb.com/docs/languages/python/pymongo-driver/current/reference/migration/
- aio-pika API: https://docs.aio-pika.com/apidoc.html

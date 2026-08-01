# remnanode-exporter

Ловит тех, кто грузит ноды.

Читает Redis Streams, которые Remnawave 3.x публикует при
`EXPORT_TO_STREAM_ENABLED=true`, обогащает IP через MaxMind GeoLite2, складывает
всё в ClickHouse и показывает в Grafana. Написан на Go, один статический бинарь,
никакого рантайма.

```
Remnawave ──► Valkey Streams ──► remnanode-exporter ──► ClickHouse ──► Grafana
                                          ▲
                                   MaxMind GeoLite2
                                (страна / город / ASN / хостинг)
```

Grafana своя не тащится — подключается та, что уже есть. Существующий
[дашборд 25064](https://grafana.com/grafana/dashboards/25064-remnawave-monitoring-dashboard/)
это не заменяет и не ломает: там Prometheus-метрики железа, здесь — поведение
аккаунтов.

## Что именно ловим

| Сигнал | Откуда | Что означает |
| --- | --- | --- |
| Трафик по юзеру и ноде, пик за 5 минут | `user_usage` | кто реально выжирает канал |
| Число разных /24 и /48 сетей у одного аккаунта | `node_connections` | подписку раздали |
| Число стран и ASN одновременно | `node_connections` + GeoIP | продали доступ |
| IP в хостинг-ASN (OVH, Hetzner, AWS, Aeza…) | `node_connections` + GeoIP | перепродажа или цепочка прокси |
| Один IP у нескольких аккаунтов | `node_connections` | общий выходной прокси, реселлер |
| Частота запросов подписки, число User-Agent | `subscription_requests` | скрипт, парсер, ссылка гуляет по чату |
| `curl` / `python-requests` / боты в UA | `subscription_requests` | автоматизация, а не клиент |

Всё это сводится в один `score` — колонка в дашборде **Abuse Radar**, сортировка
по убыванию. Формула простая и лежит в
[`internal/schema/sql/03_views.sql`](internal/schema/sql/03_views.sql), крути под себя.

## Источник данных

Схемы сообщений взяты не на глаз, а из контракта панели —
`@remnawave/backend-contract@3.0.0`, `models/export-stream/export-stream.schema.ts`
(он же `RemnawaveUserUsageStreamMessageDto` и соседи в OpenAPI):

| Стрим | DTO | Периодичность |
| --- | --- | --- |
| `ioraw:export:user_usage` | `RemnawaveUserUsageStreamMessageDto` | батчами по мере учёта трафика |
| `ioraw:export:subscription_requests` | `RemnawaveSubscriptionRequestStreamMessageDto` | на каждый запрос подписки |
| `ioraw:export:node_connections` | `RemnawaveNodeConnectionsStreamMessageDto` | снапшот на ноду примерно раз в 5 минут |

## Запуск

### 1. Включить экспорт в панели

В `.env` Remnawave:

```env
EXPORT_TO_STREAM_ENABLED=true
EXPORT_TO_STREAM_MAXLEN=3000
```

`EXPORT_TO_STREAM_MAXLEN` — это `MAXLEN ~` стрима: сколько сообщений держится,
пока консьюмер отстаёт. 3000 по умолчанию маловато, если экспортер лежал ночь —
подними до 100000+, места это ест немного.

Перезапусти панель и убедись, что данные реально льются:

```bash
# deploy/remnawave/stream-debugger.yml кладётся рядом с compose-файлом панели
docker compose -f docker-compose.yml -f stream-debugger.yml up -d rw-stream-debugger
docker logs -f rw-stream-debugger
```

Пусто в логах — значит дело в панели, а не в экспортере. Убей дебаггер, как
только увидел сообщения: он читает те же стримы.

### 2. Поднять экспортер на сервере панели

Ему нужен сокет Valkey, поэтому он живёт там же, где панель. Grafana в комплект
не входит — предполагается, что она у тебя уже есть.

```bash
git clone <this repo> && cd remnanode-exporter
cp .env.example .env
$EDITOR .env
docker compose up -d --build
```

Имена тома и сети Remnawave зависят от имени compose-проекта, так что проверь
свои и пропиши в `.env`:

```bash
docker volume ls  | grep valkey     # -> VALKEY_SOCKET_VOLUME
docker network ls | grep remnawave  # -> REMNAWAVE_NETWORK
```

Если Valkey торчит по TCP — просто `REDIS_URL=redis://:password@host:6379/0`,
сокет не обязателен.

### 3. Подключить свою Grafana

`CLICKHOUSE_BIND` в `.env` — адрес, по которому Grafana достучится до ClickHouse.
На `0.0.0.0` не вешать, 9000 это нативный протокол без TLS.

```env
CLICKHOUSE_BIND=127.0.0.1     # Grafana на этом хосте, вне docker
CLICKHOUSE_BIND=172.17.0.1    # Grafana в docker на этом хосте
CLICKHOUSE_BIND=100.x.x.x     # Grafana на другом сервере, через NetBird
```

Дальше три шага в самой Grafana:

1. **Плагин** — добавь `GF_INSTALL_PLUGINS=grafana-clickhouse-datasource` в
   окружение её контейнера и перезапусти.
2. **Датасорс** — Connections → Add new connection → ClickHouse. Host — то же
   значение, что в `CLICKHOUSE_BIND`, port `9000`, protocol **Native**, база
   `remnawave`, юзер и пароль из `.env`.
3. **Дашборды** — Dashboards → New → Import, по очереди три JSON из
   [`dashboards/`](dashboards). Датасорс в них переменная, Grafana подставит
   ClickHouse сама.

Метрики самого экспортера (необязательно) — `/metrics` на `127.0.0.1:9102`.
Готовый scrape-конфиг: [`deploy/vmagent/remnanode-exporter.yml`](deploy/vmagent/remnanode-exporter.yml).
Порт именно 9102, потому что 9101 обычно занят cAdvisor'ом.

## Дашборды

Три штуки, живут рядом с [дашбордом 25064](https://grafana.com/grafana/dashboards/25064-remnawave-monitoring-dashboard/)
без конфликтов — разные датасорсы:

**Node Load** (`rw-node-load`) — кто грузит ноду прямо сейчас. Стек трафика по
нодам, топ-10 аккаунтов, таблица топ-талкеров с пиковыми Mbps за 5-минутку
(короткий всплеск не размазывается по часу), топ-5 аккаунтов на каждой ноде с
долей от ноды. Сверху фильтр по нодам.

**Abuse Radar** (`rw-abuse-radar`) — главная таблица со `score`, плюс: сколько
сетей у аккаунта во времени, адреса, за которыми сидит больше одного аккаунта,
подключения из дата-центров, география каждого аккаунта.

**Subscription Requests** (`rw-sub-requests`) — запросы подписки по типу клиента,
самые активные аккаунты, разбивка по клиентам/странам/ASN и сырой лог последних
запросов.

## GeoIP

Нужен бесплатный аккаунт [GeoLite2](https://www.maxmind.com/en/geolite2/signup).
`MAXMIND_ACCOUNT_ID` и `MAXMIND_LICENSE_KEY` в `.env` — контейнер `geoipupdate`
качает `GeoLite2-City` и `GeoLite2-ASN` и обновляет раз в сутки, экспортер
переоткрывает файлы на ходу (`GEOIP_RELOAD_INTERVAL`, по умолчанию 6h).

Без ключей всё продолжает работать, но страна/город/ASN будут пустыми, а вместе
с ними отвалится детект хостингов. Определение «это дата-центр» — по ключевым
словам в названии AS-организации, список в
[`internal/geoip/geoip.go`](internal/geoip/geoip.go) (`DefaultHostingKeywords`).

Адреса схлопываются в /24 (IPv4) и /48 (IPv6): считать уникальные сети, а не
уникальные адреса, — единственный способ не ловить ложную «раздачу» на каждом
мобильном операторе с CGNAT.

## Имена вместо цифр

В стримах ездят числовые id. Юзеры резолвятся через API панели
(`/api/users/stream` отдаёт тот же числовой `id`) — нужен `REMNAWAVE_API_TOKEN`.

С нодами хуже: REST API оперирует только `uuid` и **нигде не отдаёт bigint
`nodeId`**, который лежит в стримах. Поэтому имена нод берутся одним из двух
способов:

* `REMNAWAVE_POSTGRES_DSN` — read-only доступ к базе панели (сам запрос
  переопределяется через `REMNAWAVE_NODES_SQL`);
* `NODE_NAMES=1:Germany-1,2:Netherlands-1` — статический маппинг руками.

Не настроил ни то, ни другое — в дашбордах будет `node-1`, `node-2`. Всё
остальное работает.

## Как это устроено

* **Consumer group** на каждый стрим, `XREADGROUP` плюс `XAUTOCLAIM` для
  зависших записей. Несколько реплик экспортера с одинаковым `REDIS_GROUP` и
  разными `REDIS_CONSUMER` делят нагрузку.
* **At-least-once**: `XACK` только после успешной вставки в ClickHouse. Падение
  между вставкой и ack продублирует записи — роллапы аддитивные, а там, где это
  важно, считается `uniq()` по сущностям, а не сырые строки.
* **Битое сообщение не блокирует стрим**: логируется, ack-ается, едем дальше
  (счётчик `remnanode_exporter_messages_failed_total`).
* **Схема ClickHouse вшита в бинарь** и применяется на старте. Апгрейд
  экспортера апгрейдит таблицы, в том числе на уже инициализированном томе, где
  `docker-entrypoint-initdb.d` давно не отрабатывает.
* **TTL**: сырые снапшоты подключений 30 дней, запросы подписки 60, трафик 90,
  5-минутные роллапы 90–365. Правится в `internal/schema/sql/`.
* **ClickHouse ужат под соседей**: по умолчанию он забирает до 90% RAM, а тут
  делит машину с панелью, Postgres и Valkey. В
  [`deploy/clickhouse/config.d/exporter.xml`](deploy/clickhouse/config.d/exporter.xml)
  ему оставлена треть памяти и урезаны кеши. Сервер выделенный — подними обратно.

## Метрики

`http://127.0.0.1:9102/metrics` — прочитано и провалено сообщений по стримам,
вставлено строк по таблицам, латентность вставки, размеры справочников и
`remnanode_exporter_stream_pending` — отставание консьюмера, вот его и надо
алертить. `/healthz` пингует Redis.

## Разработка

```bash
make test          # юнит-тесты, без внешних зависимостей
make lint
make build

# полный прогон по живому ClickHouse: схема, пайплайн и КАЖДЫЙ запрос из дашбордов
docker run --rm -d -p 9000:9000 --name ch clickhouse/clickhouse-server:24.12-alpine
make e2e
```

`internal/e2e` вытаскивает `rawSql` из всех панелей `dashboards/*.json`,
подставляет макросы Grafana и выполняет их. Опечатка в панели ломает тесты, а не
дашборд в проде.

## Лицензия

MIT.

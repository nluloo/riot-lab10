# ЛР №8 «Контейнеризация распределённого приложения с помощью Docker»

Вариант 11 — «Аренда автомобилей» (C#/.NET 8). Работа продолжает линию ЛР №5 → №6 → №7:
предметная модель, протокол и механизмы отказоустойчивости не переписывались, приложение
переносится из процесса-на-хосте в контейнеры. Сервер принимает запросы от клиента по TCP
на порту 5000, пишет и читает две независимые копии PostgreSQL (Primary и Replica),
публикует доменные события в Broker, а перед фиксацией изменения согласует версию данных
с пятью узлами кластера (выборы лидера по Bully, большинство 3 из 5).

Отличие ЛР №8 от ЛР №7 именно в способе запуска. В №7 каждый компонент был отдельным
процессом, который запускался своей консолью, а адреса были зашиты в код как
`127.0.0.1:port`. В №8 тех же компонентов девять, но они живут в контейнерах: четыре
многоэтапных `Dockerfile`, `compose.yaml` с десятью сервисами (девять поднимаются по
умолчанию, клиент — в профиле `tools`), пользовательская сеть `riot-lab8`, именованные
тома под обе базы, `healthcheck` + `depends_on`, переменные окружения вместо литералов и
обработка SIGTERM, чтобы `docker compose stop` не обрывал операцию на середине.

Сохранено без изменений: формат запроса/ответа (одна строка JSON), перечень операций,
измерение расхождения копий по `replication_meta.lsn`, failover Primary → Replica,
модель Pub/Sub с очередями и попытками доставки, алгоритм выборов и консенсуса, меню
клиента 1–21, 92 юнит-теста из ЛР №7. Всё, что добавилось, — конфигурация, сеть, тома,
завершение по сигналу и 35 тестов (конфигурация контейнеров, интеграция со стендом,
нагрузка), плюс прогоны Частей 1–4, записанные в файлы-доказательства.

Файлы-доказательства лежат в `.temp/lab8/` (от корня репозитория). Все цитаты журналов,
числа и тексты ошибок в этом README взяты из них дословно; пути в тексте — относительные
от `labs/lab8`.

## Архитектура

Данные идут по одной цепочке; стрелка — направление запроса, пунктир — асинхронное событие.

```
Client.exe (хост)  или  контейнер client (профиль tools)
        │  TCP :5000   5000:5000 опубликован наружу
        ▼
┌────────────────────────────── контейнер server ──────────────────────────────────────┐
│ Server/Program.cs → RequestHandler                                                  │
│   ├ выбор экземпляра данных + heartbeat-опрос (InstanceManager)                     │
│   ├ запись в активную копию → репликация затронутых таблиц во вторую (Replicator)   │
│   ├ Publisher → broker:5001            (событие после COMMIT)                        │
│   └ Cluster → node1:6001 … node5:6005  (консенсус перед фиксацией)                  │
└──────┬───────────────────┬───────────────────────────┬───────────────────────────────┘
       │ Host=primary       │ Host=replica              │ broker:5001        │ nodeN:600N
       ▼                    ▼                           ▼                    ▼
  primary                replica                 broker               node1 … node5
  postgres:16-alpine   postgres:16-alpine          riot-lab8/broker     riot-lab8/nodes
  5433 → 5432          5434 → 5432                      │ deliver (TCP :5001)
                                                        ▼
                              Subscribers.exe на хосте: Logger, CarService,
                              NotificationService, AnalyticsService (меню 1–10)
```

| Файл / класс | Ответственность |
|---|---|
| `DistributedSystem/Server/Program.cs` | читает `SERVER_PORT`, `PROCESSING_DELAY_MS`, `HEARTBEAT_INTERVAL_MS`, `CARRENTAL_*_CONNECTION`, `PUBLISHER_BROKERS`, `CLUSTER_MEMBERS`; `TcpListener(IPAddress.Any, port)`; SIGTERM → `cts.Cancel()`; цикл соединений |
| `Server/RequestHandler.cs` | 20 операций протокола, выбор активной копии, транзакция/ROLLBACK, репликация, публикация события, консенсус, формирование `Response` |
| `Server/InstanceManager.cs` | `CHECK`, heartbeat-статусы, failover, возврат на Primary, `PrepareSchema` после восстановления |
| `Server/DataInstance.cs` | обёртка над экземпляром: `Ping()`, `GetLsn()`, статус; «БД отвечает, но схемы нет» → Unavailable |
| `Server/CarRentalDatabase.cs` | SQL к `locations/cars/customers/rentals/replication_meta`, `GetLsn`, `BumpLsn … FOR UPDATE`, адреса по умолчанию 127.0.0.1:5433/5434 |
| `Server/Replicator.cs` | `Replicate` (затронутые таблицы), `FullSync` (все реплицируемые + удаление лишних), `Compare` (отпечатки строк + lsn) |
| `Server/Publisher.cs` | разбор `PUBLISHER_BROKERS`, соединения с брокерами, `publish` с ConnectTimeout 1000 мс / RequestTimeout 2000 мс, деградация при недоступности |
| `Server/Cluster.cs` | опрос взглядов узлов, выбор лидера, `PROPOSE`, `CHECK`, `fail/restore`, эксперименты 1–4 |
| `Server/Journal.cs` | формат `[HH:mm:ss.fff] scope -> message;`, кольцо на 300 записей, `Console.WriteLine` → видно в `docker logs` |
| `Broker/Program.cs`, `EventBroker.cs`, `EventQueue.cs`, `DeliveryManager.cs`, `SubscriptionRegistry.cs` | подписки, маршрутизация, 3 попытки с паузой 1500 мс, очередь 50 событий и 5 повторов на подписчика, насос раз в 5000 мс, SIGTERM |
| `Nodes/ClusterNode.cs`, `Nodes/Program.cs` | состояние узла, heartbeat-опрос соседей, Bully-выборы, голосование по версии, COMMIT, SIGTERM |
| `Shared/Protocol/*` | `Request { operation, data }`, `Response { success, message, instance, events, consensus, data }` |
| `Shared/Events/PubSubMessages.cs`, `DomainEvent.cs` | операции hello/subscribe/unsubscribe/info/publish/deliver/result и статусы; EventId — 8 hex-символов |
| `Shared/Cluster/ClusterRules.cs`, `ClusterMember.cs`, `ClusterMessages.cs` | `Majority = n/2 + 1`, `PickLeader` = максимальный id среди доступных, разбор `CLUSTER_MEMBERS` |
| `Client/Program.cs` | меню 1–21 + 0, `SERVER_HOST`/`SERVER_PORT`, печать `SUCCESS/FAIL [Instance]`, `событие:`, `консенсус:` |
| `Subscribers/Program.cs` | четыре подписчика с задержками 1000/3000/2000/500 мс, меню 1–10, `SUBSCRIBER_BROKER_HOST/PORT` |
| `compose.yaml`, `*/Dockerfile`, `db/init.sql`, `.dockerignore`, `.env.example` | собственно контейнеризация: образы, сеть, тома, порядок старта, схема при первом пуске |

## Репликация: что копируется и как измеряется расхождение

Копируются четыре таблицы — `locations, cars, customers, rentals`
(`CarRentalDatabase.ReplicatedTables`), и это объявлено прямо в баннере сервера:
`Общий ресурс: свободный автомобиль (cars.is_available), реплицируются locations, cars,
customers, rentals`. Общая точка — флаг `cars.is_available`: один и тот же автомобиль
нельзя арендовать дважды, поэтому его состояние обязано совпадать в обеих копиях.

Версия состояния — не LSN WAL и не репликация PostgreSQL, а счётчик в таблице
`replication_meta (id, lsn BIGINT, updated_at)`, одна строка с `id = 1`. Любое изменение
в активной копии поднимает счётчик (`BumpLsn` с `SELECT … FOR UPDATE`), после переноса
таблиц сервер копирует значение счётчика во вторую копию в том же транзакционном
контексте (`Replicator.Replicate` → `SetLsn` + `AlignSequences`). Расхождение — это
разность `lsn` двух копий плюс построчное сравнение отпечатков (`Replicator.Compare`),
поэтому «копии равны» требует совпадения и содержимого, и версии:

```csharp
public bool IsConsistent => DataMatch && PrimaryLsn == ReplicaLsn;   // Server/Replicator.cs
```

Как это видно в журнале контейнера `server` (`.temp/lab8/p4-00-server.log`):

```
[01:05:23.182] Client 3 -> Replication -> START
[01:05:23.192] Client 3 -> Replication -> SUCCESS, 6 строк передано в Replica (lsn = 3)
[01:05:23.316] Client 6 -> Compare -> Primary = Replica
```

Возврат второй копии в согласованное состояние после отказа — полная синхронизация
(`p4-01-server.log`), где видно не только перенос, но и удаление лишнего:

```
[01:20:17.175] Client 11 -> Synchronization -> SUCCESS, перенесено 6 строк из Primary в Replica, удалено 2, lsn = 4
```

`6 строк` — это 4 автомобиля + 2 локации до первой аренды; при возврате автомобиля
переносится 5 строк (`p4b-0a-server.log`: `Replication -> SUCCESS, 5 строк передано в Replica (lsn = 7)`),
потому что в `rentals` появляется вторая запись, а лишняя строка в копии удаляется.
Heartbeat отдельно фиксирует, что версия изменилась: `Heartbeat -> Primary -> данные
изменились (lsn = 1)` (`p3-server.log`).

Отдельный случай — экземпляр вернулся, но базы пустые (том пересоздан). Тогда сервер
не просто помечает его доступным, а пересоздаёт схему (`InstanceManager.PrepareSchema`),
что видно в `p4-05-server-restored.log`:

```
[01:21:37.605] Heartbeat -> Primary -> AVAILABLE (lsn = 4)
[01:21:37.617] Heartbeat -> Primary -> SCHEMA READY после восстановления
[01:21:37.623] Client 26 -> Compare -> Primary = Replica
```

## Pub/Sub и кластер: что перенесено из ЛР №6 и №7 без изменений

Механика та же, изменился только способ достучаться до соседа — вместо `127.0.0.1` имя
сервиса из compose. Publisher живёт внутри сервера (`Server/Publisher.cs`), брокеров ему
перечисляет `PUBLISHER_BROKERS`; событие публикуется после COMMIT и его неудача не
отменяет запрос клиента — это прямо сказано в тексте ошибки:

```
[01:37:34.226] Publisher -> publish -> broker:5001: BROKER UNAVAILABLE (1002 мс) — Не удалось подключиться к broker:5001: The operation has timed out.. Запрос клиента от этого не отменяется.
```
(`.temp/lab8/p4b-0b-server.log`, при этом клиент получил `SUCCESS … Номер аренды: 1`.)

Доставка: не более 3 попыток с паузой 1500 мс, очередь 50 событий на подписчика, не более
5 повторов из очереди, насос раз в 5000 мс — эти числа печатает сам брокер при старте
(`p3-broker.log`):

```
Broker Broker-1 — ЛР №6, модель Pub/Sub
Порт: 5001. Повторная доставка: не более 3 попыток, пауза 1500 мс.
Очередь на подписчика: 50 событий, не более 5 повторов, проверка очереди каждые 5000 мс.
Publisher -> Broker -> Subscriber. Broker знает подписки, Publisher — нет.
```

Кластер: пять узлов, лидер — доступный узел с наибольшим `NODE_ID`, подтверждается
большинством (`Majority = 3`). Лидер выбирается на старте и переизбирается при отказе
(`p3-node5.log`):

```
[01:00:12.440] Node 5 -> Node 5 elected as LEADER (acks = 5, majority = 3)
[01:00:12.440] Node 5 -> Node 5 -> ROLE LEADER
[01:00:12.446] Node 5 -> Node 5 признан лидером (объявление от Node 1), Node 5 -> LEADER
```

Голосование по версии: узел отвечает `YES`, только если предложенная версия свежее
зафиксированной (`p4-04-node3.log`):

```
[01:20:46.962] Node 3 -> Node 3 голосует YES: предложено Version=5, зафиксировано 4
[01:20:46.964] Node 3 -> Node 3 -> COMMIT Version=5
```

Сервер фиксирует тот же раунд у себя (`p4-04-server.log`), включая момент, когда двух
узлов нет и большинство держится на трёх:

```
[01:20:46.966] Cluster -> Leader Node 5 -> PROPOSE Version=5: CONSENSUS ACHIEVED, YES = 3, NO = 0, без ответа = 2, Majority = 3, доступно 3/5
```

Отказ узла виден как `NO RESPONSE` в таблице состояний, а не как падение сервера:

```
    3   NO RESPONSE  —         node3:6003         Version = 0
   Leader: Node 5. Доступно 4/5, большинство 3 (решение возможно).
```
(`.temp/lab8/p4c-02-after-stop.txt`, после `docker compose stop node3`.)

## Протокол

Транспорт один для всех трёх слоёв: TCP, длина сообщения не задаётся отдельно — границей
служит перевод строки, тело — JSON. `LineConnection` общий для клиента, Publisher↔Broker
и Server↔узлы, поэтому интеграционные тесты берут соединение оттуда же, а не пишут своё.

**Запрос клиента → сервер** (`Shared/Protocol/Request.cs`):

```json
{ "operation": "CreateRental", "data": { "carId": 1234, "customerName": "Ivanov A.A.", "phone": "+375293334455", "startDate": "2026-11-12", "days": 1 } }
```

**Ответ сервера** (`Shared/Protocol/Response.cs`) — пять полей, три из них специфичны для
ЛР №6/№7 и заполняются только когда механизм участвовал в операции:

| Поле | Смысл | Когда null / пусто |
|---|---|---|
| `success` | успех операции | — |
| `message` | человекочитаемый итог, его печатает клиент после `SUCCESS [Instance]:` | — |
| `instance` | `Primary` или `Replica` — через какой экземпляр реально шёл запрос | оба экземпляра недоступны, либо операция их не использует |
| `events` | что произошло с событием операции: в какой брокер ушло, сколько подписчиков получило | операция события не формирует |
| `consensus` | лидер, голоса, большинство, вердикт | операция консенсуса не требовала / узлы молчат |
| `data` | payload: массив автомобилей, состояние, таблица узлов, отчёт о сравнении | read-only операции без данных |

Имена операций, принимаемые `RequestHandler` (20): `SearchCar`, `CreateRental`,
`CreateRentalNoTransaction`, `CancelRental`, `ReturnCar`, `ShowState`, `ResetData`,
`ShowInstances`, `CompareData`, `SyncData`, `ReplicationLog`, `BrokerStatus`,
`ClusterStatus`, `ClusterCheck`, `ClusterPropose`, `ClusterFailNode`, `ClusterRestoreNode`,
`ClusterExperiments`, плюс неизвестная строка → `Неизвестная операция`.

Формы `data` по операциям: `SearchCar` — массив автомобилей; `ShowState` — `{ lsn, cars[],
rentals[] }`; `ShowInstances` — `{ active, instances[] }`; `CompareData` — `{ primaryLsn,
replicaLsn, isConsistent, differences[] }`; операции кластера и брокера — массив
человекочитаемых строк таблицы, который клиент печатает как есть.

**Server(Publisher) ↔ Broker ↔ Subscriber** (`Shared/Events/PubSubMessages.cs`):
операции `hello`, `subscribe`, `unsubscribe`, `info`, `publish`, `deliver`, `result`.
Статусы публикации: `ACCEPTED`, `NO_SUBSCRIBERS`, `UNKNOWN_EVENT_TYPE`, `INVALID_EVENT`,
`BROKER_UNAVAILABLE`; доставки: `OK`, `UNAVAILABLE`, `ERROR`, `TIMEOUT`,
`CONNECTION_LOST`; подписки: `OK`, `PARTIAL`, `UNKNOWN_SUBSCRIBER`, `UNKNOWN_EVENT_TYPE`,
`NO_EVENT_TYPES`, `NO_CONNECTION`; ответа `info`: `READY` или `BROKER_UNAVAILABLE`.
Событие — `DomainEvent { EventType, EntityId, ClientId, EventId }`, где `EventId` — 8
hex-символов, он попадает в журнал и в строку клиента:

```
событие: RentalCreated #1 (event 5c4cfeae) -> Broker-1: ACCEPTED, получателей 4
```
(`.temp/lab8/p4d-client.txt`; если подписчиков нет — `-> Broker-1: NO_SUBSCRIBERS`, если
брокер недоступен — `-> broker:5001: BROKER_UNAVAILABLE, Broker не принял событие`.)

Типы событий и подписки фиксированы: `RentalCreated`, `RentalCancelled`, `CarReturned`;
Logger — все три (очередь не ведётся), CarService — `RentalCreated, CarReturned`,
NotificationService — `RentalCreated, RentalCancelled`, AnalyticsService — все три
(очередь разрешена) — см. `SUBSCRIBE … -> OK` в `p4d-subscribers.log`.

**Server ↔ узлы** (`Shared/Cluster/ClusterMessages.cs`): `heartbeat`, `state`, `elect`,
`propose`, `vote`, `commit`, `info`, `check`, `fail`, `restore`, `result` — envelope тот
же `PubSubMessage`, в `data` лежит payload конкретного сообщения
(`NodeViewMessage`, `ElectMessage`, `ProposeMessage { version, value }`,
`VoteMessage { accept, vote, message }`, `ConsensusResultMessage`, `ClusterInfoMessage`,
`NodeControlMessage`). Состояния: `AVAILABLE`, `FAILED`, `NO RESPONSE`, роли —
`LEADER`, `FOLLOWER`.

## Меню клиента

Пункты 1–21 + выход — дословно из `Client/Program.cs` (так же они выглядят в каждом логе
`.temp/lab8/p4-*-client.txt`):

| № | Пункт | Что отправляет |
|---|---|---|
| 1 | Найти автомобиль | `SearchCar` |
| 2 | Создать аренду (с транзакцией) | `CreateRental` |
| 3 | Отменить аренду | `CancelRental` |
| 4 | Вернуть автомобиль | `ReturnCar` |
| 5 | Показать данные из активного экземпляра | `ShowState` |
| 6 | Без транзакции, ошибка между операциями | `CreateRentalNoTransaction` |
| 7 | С транзакцией, COMMIT | `CreateRental` (успешная ветка) |
| 8 | С транзакцией, ошибка и ROLLBACK | `CreateRental` (сбойная ветка) |
| 9 | Сбросить данные в обеих копиях | `ResetData` |
| 10 | Состояние экземпляров (Primary, Replica) | `ShowInstances` |
| 11 | Сравнить копии | `CompareData` |
| 12 | Синхронизировать копии | `SyncData` |
| 13 | Журнал репликации | `ReplicationLog` |
| 14 | Эксперимент: три изменения подряд и сверка копий | серия операций + `CompareData` |
| 15 | Состояние Broker: подписки, кто на связи, очередь | `BrokerStatus` |
| 16 | Состояние кластера: узлы, роли, лидер, большинство | `ClusterStatus` |
| 17 | Heartbeat-проверка: обнаружить отказ и выбрать лидера | `ClusterCheck` |
| 18 | Предложить значение кластеру (проверка консенсуса) | `ClusterPropose` |
| 19 | Отметить узел FAILED (отказ) | `ClusterFailNode` |
| 20 | Восстановить узел (AVAILABLE) | `ClusterRestoreNode` |
| 21 | Эксперименты 1-4: отказ 0, 1, 2, 3 узлов и решение | `ClusterExperiments` |
| 0 | Выход | закрывает соединение |

Пункты 6–8 и 14 — это те же операции, что в ЛР №5/№6, но с заранее выбранным сценарием
сбоя, чтобы демонстрировать ROLLBACK и расхождение копий одним нажатием.

## Запуск

Все команды выполняются из `labs/lab8` (контекст сборки — подкаталог
`DistributedSystem`, поэтому `-f DistributedSystem/<Проект>/Dockerfile DistributedSystem`).

```bash
docker build -t riot-lab8/server:1.0 -f DistributedSystem/Server/Dockerfile DistributedSystem
docker build -t riot-lab8/broker:1.0 -f DistributedSystem/Broker/Dockerfile DistributedSystem
docker build -t riot-lab8/nodes:1.0  -f DistributedSystem/Nodes/Dockerfile  DistributedSystem
docker build -t riot-lab8/client:1.0 -f DistributedSystem/Client/Dockerfile DistributedSystem

docker compose config -q      # синтаксис compose.yaml, без запуска
docker compose up -d          # 9 контейнеров default-профиля
docker compose --profile tools run --rm client   # клиент внутри сети (опционально)
docker compose logs -f server
```

Результат сборки и размеров — `.temp/lab8/p2-builds.txt`: `Server rc=0 size=197MB`,
`Broker rc=0 size=196MB`, `Nodes rc=0 size=196MB`, `Client rc=0 size=195MB`; `config -q`
— `rc=0` (`p3-config.txt`); `up -d` — девять `Running` + `primary/replica … Healthy`
(`p3-up.txt`).

Остановка/перезапуск одного сервиса и моделирование отказа (команды взяты из прогонов
`.temp/lab8-run-p4*.sh`):

```bash
docker compose stop broker          && docker compose start broker
docker compose stop node3           && docker compose start node3
docker compose stop primary         && docker compose start primary
docker compose stop server          # корректная остановка: SIGTERM, журнал пишет STOPPING
docker compose kill -s KILL server  # жёсткий kill посреди операции
docker compose down                 # контейнеры и сеть удаляются, тома остаются
```

Переменные окружения — кто читает, что по умолчанию, что сломается без значения:

| Переменная | Кто читает | Значение по умолчанию | Что без него |
|---|---|---|---|
| `SERVER_PORT` | `Server/Program.cs`, `Client/Program.cs` | 5000 | сервер слушает не тот порт, что опубликован в `ports: 5000:5000` — клиент с хоста не дойдёт |
| `PROCESSING_DELAY_MS` | `Server/Program.cs` | 1500 | задержка до lock исчезает, узлы не успевают завершить выборы/голосование, замеры нагрузки смещаются |
| `HEARTBEAT_INTERVAL_MS` | `Server/Program.cs` (2000), `Nodes/Program.cs` (1500) | 2000 / 1500 | сервер и узлы опрашивают соседей с разным темпом; в compose проставлено 2000 всем, чтобы журнал совпадал |
| `CARRENTAL_PRIMARY_CONNECTION` | `Server/Program.cs` | `Host=127.0.0.1;Port=5433;…` | без переопределения внутри контейнера это обращение к самому себе: `Primary -> UNAVAILABLE` (`p1-badaddr-server-log.txt`) |
| `CARRENTAL_REPLICA_CONNECTION` | то же | `Host=127.0.0.1;Port=5434;…` | аналогично; репликация и failover недоступны |
| `PUBLISHER_BROKERS` | `Server/Publisher.cs` | `127.0.0.1:5001` | пустая строка — Pub/Sub выключен (баннер `ВЫКЛЮЧЕН`), мусор — `адрес Broker '…' некорректен — пропущен` и откат на `127.0.0.1:5001`, т.е. «в себя» |
| `CLUSTER_MEMBERS` | `Server/Cluster.cs`, `Nodes/Program.cs` | 5 узлов на `127.0.0.1:6001-6005` | пустая строка — консенсус выключен; мусор — `FormatException: CLUSTER_MEMBERS: не разобран элемент '…'` |
| `NODE_ID` | `Nodes/Program.cs` | нет, обязателен | контейнер завершается rc=1: `NODE_ID должен быть целым числом 1..5, получено ''…` (`p1-badnode.txt`) |
| `BROKER_ID` | `Broker/Program.cs` | `Broker-1` | в журнале и в `Response.events` печатается безымянный брокер |
| `BROKER_PORT` | `Broker/Program.cs` | 5001 | Publisher идёт по `broker:5001` и не находит брокер |
| `BROKER_RETRY_DELAY_MS` | `Broker/Program.cs` | 1500 | пауза между попытками доставки меняется, баннер перестаёт соответствовать поведению |
| `BROKER_QUEUE_PUMP_MS` | `Broker/Program.cs` | 5000 | период насоса очереди; 0 — накопленные события не отдаются |
| `SERVER_HOST` | `Client/Program.cs` | `127.0.0.1` | клиент в контейфере стучится сам в себя вместо `server:5000` |
| `SUBSCRIBER_BROKER_HOST` / `_PORT` | `Subscribers/Program.cs` | `127.0.0.1` / 5001 | хостовые подписчики не видят брокера, опубликованного на 5001 |
| `POSTGRES_PASSWORD` | `compose.yaml` (оба postgres и обе строки подключения server) | `postgres` | пароль в контейнере и в `Host=primary;…Password=…` разойдутся — аутентификация падает |
| `TZ` | все сервисы compose | не задан (в compose — `Europe/Minsk`) | журнал контейнеров в UTC, таймстампы не сходятся с хостовыми `Subscribers.exe`/`Client.exe` |

`.env.example` содержит только `POSTGRES_PASSWORD` и `PROCESSING_DELAY_MS`: адреса сервисов
в переменные не вынесены сознательно — это имена из сети `riot-lab8`, а не настройка
окружения, и `localhost` их заменить не должен.

## Часть 1 — один компонент в контейнере

Определяем основной контейнеризируемый компонент: сервер (у него самые внешние
зависимости — обе БД, брокер, узлы). Базовый образ — `mcr.microsoft.com/dotnet/sdk:8.0`
для сборки и `mcr.microsoft.com/dotnet/runtime:8.0` для запуска; приложения ASP.NET нет,
поэтому ASP.NET-образ не берём. `Server/Dockerfile` — восемь шагов, сначала копируется
только `.csproj` и выполняется `dotnet restore` (слой зависимостей пересобирается только
при изменении csproj), затем источники и `dotnet publish -c Release -o /publish
--no-restore`; финальный слой несёт только `/publish`.

| Что проверено | Команда | Результат (файл) |
|---|---|---|
| обычный `dotnet build` решения работает | `dotnet build DistributedSystem/DistributedSystem.sln` | rc=0 (`p0-build.txt`) |
| образ собран | `docker build -t riot-lab8/server:1.0 …` | `riot-lab8/server:1.0 43f8288b61f6 197MB` (`p1-images.txt`) |
| состав образа | `docker history` | `ENTRYPOINT ["dotnet" "Server.dll"]`, `EXPOSE map[5000/tcp:{}]`, `COPY /publish ./` (`p1-images.txt`) |
| контейнер запущен и слушает наружу | `docker run -d --name lab8-p1-server --network riot-lab8 -p 5000:5000 …` | id `be7b3f34e98f…`, rc=0 (`p1-run.txt`) |
| состояние | `docker inspect` | `running=true exitcode=0 published=5000` (`p1-inspect.txt`) |
| переменные внутри контейнера | `docker inspect … .Config.Env` | `PUBLISHER_BROKERS=`, `CLUSTER_MEMBERS=`, `PROCESSING_DELAY_MS=400`, `TZ=Europe/Minsk`, плюс `APP_UID=1654`, `ASPNETCORE_HTTP_PORTS=8080` от базового образа (`p1-env.txt`) |
| журнал виден из Docker | `docker logs lab8-p1-server` | `Server -> READY, ожидание клиентов...` (`p1-server-log.txt`) |
| клиент с хоста по опубликованному порту | `Client.exe` | `SUCCESS [Primary]: Аренда создана. Выполнен COMMIT в Primary. Номер аренды: 1.` и `SUCCESS [Primary]: Автомобилей: 4. Аренд: 1. Читает Primary (lsn = 1).` (`p1-client.txt`) |

Схема и демо-данные создаются при первом пуске томов: `docker exec lab8-postgres-primary
psql -U postgres -d carrental -tc "SELECT count(*) FROM cars"` → `cars=4
replication_meta=1` в обеих копиях (`p1-db-schema.txt`).

Отработка ошибок Части 1:

| Ситуация | Что видно |
|---|---|
| контейнер завершился сразу | `NODE_ID должен быть целым числом 1..5, получено ''…`, `docker run rc=1`, в `docker ps -a` — `lab8-p1-nodes-bad Exited (1) Less than a second ago` (`p1-badnode.txt`) — причина читается из stdout, `docker logs` не нужен отдельный инструмент |
| порт занят | `Bind for 0.0.0.0:5000 failed: port is already allocated.`, rc=125 (`p1-portbusy.txt`) |
| адреса БД недостижимы из контейнера | сервер поднимается и честно деградирует: `Pub/Sub: ВЫКЛЮЧЕН (PUBLISHER_BROKERS пуст) — операции работают как в ЛР №5`, `Кластер узлов: ВЫКЛЮЧЕН (CLUSTER_MEMBERS пуст) — операции работают как в ЛР №6`, `Primary -> UNAVAILABLE, сервер продолжит работу без этого экземпляра`, `Initial -> Проверка начального состояния пропущена: один из экземпляров недоступен` (`p1-badaddr-server-log.txt`), клиент получает `FAIL: Недоступны оба экземпляра данных. Запрос не выполнен, сервер продолжает работу.` (`p1-badaddr-client.txt`) |

## Часть 2 — все компоненты в отдельных контейнерах

Решение, что запускать отдельно: сервер, брокер, пять узлов и оба PostgreSQL — у каждого
свой процесс, свой порт и свой цикл жизни. Клиент тоже контейнеризирован, но в профиле
`tools`: он интерактивный, основной прогон делался хостовым `Client.exe`. Подписчики
оставлены на хосте (`Subscribers.exe`): их задача — показать задержки обработки и режимы
сбоя, и управлять ими из меню удобнее, чем через `docker attach`.

| Компонент | Образ | Размер | ENTRYPOINT |
|---|---|---|---|
| Server | `riot-lab8/server:1.0` | 197 MB | `dotnet Server.dll` |
| Broker | `riot-lab8/broker:1.0` | 196 MB | `dotnet Broker.dll` |
| Nodes (1 образ на 5 узлов) | `riot-lab8/nodes:1.0` | 196 MB | `dotnet Nodes.dll` |
| Client | `riot-lab8/client:1.0` | 195 MB | `dotnet Client.dll` |

Контейнеры подняты вручную в сеть `riot-lab8-manual` с `--name`, чтобы addresses
совпадали с именами контейнеров; окружение у каждого своё (`p2-ps.txt`):

```
lab8-p2-server: PUBLISHER_BROKERS=lab8-broker:5001 CLUSTER_MEMBERS=1=lab8-node1:6001,2=lab8-node2:6002,3=lab8-node3:6003,4=lab8-node4:6004,5=lab8-node5:6005 SERVER_PORT=5000 PROCESSING_DELAY_MS=400 CARRENTAL_PRIMARY_CONNECTION=Host=lab8-postgres-primary;Port=5432;…
lab8-broker: BROKER_ID=Broker-1
```

В `docker ps` узлы видны без опубликованных портов (`lab8-node5 … 6001/tcp`) — им наружу
ничего не нужно, достаточно внутренней сети; наружу публикуются только 5000, 5001, 5433,
5434 (для хостового клиента, подписчиков и тестов).

Журналы: брокер печатает баннер и доставку (`p2-broker-delivery.txt`), узел 5 — выборы
(`p2-node5-tail.txt`), сервер — `SCHEMA READY, lsn = 1` и `данные изменились (lsn = 1)`
(`p2-server-banner.txt`), база отвечает на запись из контейнера (`p2-db-io.txt`:
`INSERT 0 1`, затем `DELETE 1`). Кластер в этой конфигурации видит узлы по именам
контейнеров (`p2-cluster.txt`; там узлы запущены `docker run --name lab8-nodeN`, в
compose-стенде `CLUSTER_MEMBERS` содержит имена сервисов, и та же строка выглядит
`node5:6005` — `p5-01-status.txt`):

```
    5   AVAILABLE    LEADER    lab8-node5:6005    Version = 0
   Leader: Node 5. Доступно 5/5, большинство 3 (решение возможно).
```

## Часть 3 — сеть и Docker Compose

`compose.yaml` объявляет `name: lab8` (в Docker Desktop это «папка» проекта), сеть
`networks.default.name: riot-lab8`, тома `primary-data`/`replica-data`, десять сервисов.
У каждого сервиса задан `container_name` равный имени сервиса — `server`, `broker`, `node1`…
`node5`, `primary`, `replica`: без него compose добавил бы номер экземпляра (`lab8-node1-1`),
а внутри сети все ходят по именам сервисов в любом случае, так что имена только для человека.

| Проверка | Результат |
|---|---|
| `docker compose config -q` | rc=0 (`p3-config.txt`, `p5-run-stdout.txt`) |
| `docker compose up -d` | 9 контейнеров `Running`, `Container primary Healthy`, `Container replica Healthy`, rc=0 (`p3-up.txt`) |
| `docker compose ps` | `server riot-lab8/server:1.0 "dotnet Server.dll" … 0.0.0.0:5000->5000/tcp`; `primary postgres:16-alpine … Up (healthy) 0.0.0.0:5433->5432/tcp` |
| метки проекта | `docker inspect server` → `com.docker.compose.project = lab8`, `…service = server` |
| сеть | `riot-lab8 bridge`, адреса `172.21.0.x` (`p3-network-containers.txt`) |
| схема при первом пуске | `db/init.sql` примонтирован в `/docker-entrypoint-initdb.d`, том создан, `cars=4`, `replication_meta` c `lsn = 0` (`p1-db-schema.txt`) |
| клиент внутри сети | `docker compose --profile tools run --rm -T client` → `Сервер: server:5000`, rc=0 (`p3-client-ops.log`, после переименования — `p5-04-container-client.txt`) |

Почему именно имена сервисов, а не `localhost` — три эксперимента (`p3-network.txt`):

* Э1: `connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection
  refused` — внутри контейнера `localhost` это сам контейнер; то же обращение по имени
  сервиса возвращает `4`.
* Э2: в default bridge имя не резолвится — `psql: error: could not translate host name
  "lab8-postgres-primary" to address: Name does not resolve`, зато по IP соседа проходит.
  Отсюда требование пользовательской сети (в ней есть встроенный DNS).
* Э3: контейнер в отдельной проверочной сети получает её адрес — `network=lab8-netcheck=
  172.20.0.2`, сеть `bridge`, scope `local`; все девять контейнеров основной `riot-lab8`
  сидят в `172.21.0.0/16` (`p3-network-containers.txt`, снимок сделан до финального
  переименования контейнеров: там ещё `lab8-server-1 172.21.0.10/16`,
  `lab8-primary-1 172.21.0.9/16`, `lab8-node2-1 172.21.0.2/16`; сейчас это `server`,
  `primary`, `node2` с теми же адресами).

Полный прогон операций из контейнера (`p3-client-ops.log`) — все три механизма за один
заход:

```
SUCCESS [Primary]: Аренда создана. Выполнен COMMIT в Primary. Номер аренды: 1.
событие: RentalCreated #1 (event e75e77cf) -> Broker-1: NO_SUBSCRIBERS
консенсус: лидер Node 5: YES = 5, NO = 0, без ответа = 0, Majority = 3 -> CONSENSUS ACHIEVED
SUCCESS [Primary]: Автомобилей: 4. Аренд: 1. Читает Primary (lsn = 3).
  Broker-1: READY, подписчиков на связи 0, событий в очереди 0
   Leader: Node 5. Доступно 5/5, большинство 3 (решение возможно).
```

`NO_SUBSCRIBERS` здесь — корректный результат: подписчики в этом прогоне не запущены, а
брокер принимает событие и отвечает, что публиковать некому.

## Часть 4 — тестирование и отказные прогоны

Каждый сценарий — отдельный прогон со снимками журналов. Итоговая таблица (все строки
цитаты из соответствующих файлов):

| № | Сценарий | Реакция приложения |
|---|---|---|
| 0 | штатный режим + хостовые подписчики | `EVENT RECEIVED RentalCreated #1 (event 5c4cfeae, client 15)` → `ROUTING RentalCreated -> AnalyticsService, CarService, Logger, NotificationService` → `RentalCreated #1 -> CarService (attempt 1)` → `PROCESS FINISH RentalCreated #1 (клиент 15) за 3002 мс`; у клиента `получателей 4` (`p4d-broker.txt`, `p4d-subscribers.log`) |
| 1 | `docker compose stop broker` | запись проходит: `Replication -> SUCCESS, 6 строк передано в Replica (lsn = 9)`, затем `Publisher -> publish -> broker:5001: BROKER UNAVAILABLE (1002 мс) … Запрос клиента от этого не отменяется.`; клиент: `SUCCESS … Номер аренды: 1` + `событие: … BROKER_UNAVAILABLE` (`p4b-0b-server.log`, `p4b-0b-client.txt`) |
| 2 | SIGTERM брокеру | `[01:58:17.753] Broker-1 -> STOPPING, получен SIGTERM (docker stop)` + `Broker Broker-1 остановлен.`, соединение Publisher закрывается штатно (`p4e-broker-queue-drain.txt`) |
| 3 | `docker compose start broker` | Publisher переподключается лениво, при первой публикации: `[01:55:11.574] Broker-1 -> PUBLISHER CONNECTED Server`; подписчики при перезапуске процесса: `SUBSCRIBER CONNECTED Logger, подписок пока нет` + `SUBSCRIBE Logger […] -> OK` (`p4e-broker-after-resubscribe.txt`) |
| 4 | `docker compose stop node1 node2` (живы 3 из 5) | `Node 5 -> Majority = 3 (доступно узлов 3 из 5)`, `CONSENSUS ACHIEVED`, `COMMIT Version=5 зафиксирована узлами Node 3, Node 4, Node 5`; у клиента `Leader: Node 5. Доступно 3/5, большинство 3 (решение возможно).` (`p4-04-node5.log`, `p4-04-client.txt`) |
| 5 | то же для `node3` | при остановке — `[01:50:04.986] Node 3 -> STOPPING, получен SIGTERM (docker stop)` и `Node 3 остановлен.`, у клиента `Доступно 4/5`; после `docker start node3` — `Leader: Node 5. Доступно 5/5` (`p4c-02-node3-sigterm.txt`, `p4c-02-after-stop.txt`, `p4c-02-restored.txt`; то же коротким именем вне compose — `docker stop node3` / `docker start node3` в `p5-02-*`) |
| 6 | `docker compose stop primary` | `Heartbeat -> Primary -> UNAVAILABLE (lsn = 4)` → `Client 24 -> Failover -> START` → `Failover -> SUCCESS, запрос идёт через Replica` → `Read from Replica -> SUCCESS`; репликация в этот момент `Replication -> SKIPPED, Primary -> UNAVAILABLE` (`p4-05-server-failover.log`) |
| 7 | `docker compose start primary` | `Heartbeat -> Primary -> AVAILABLE (lsn = 4)`, `SCHEMA READY после восстановления`, `Compare -> Primary = Replica` (`p4-05-server-restored.log`); на стороне клиента во время отказа запрос читает Replica: `Активный экземпляр (через него шёл последний запрос): Replica` + `Replica AVAILABLE lsn = 4 [активный] автомобилей 4, аренд 0`, при этом Primary показан последней известной версией: `данные недоступны, показана последняя известная версия` (`p4-05-client-restored.txt`) |
| 8 | `docker compose stop server` | `[01:21:38.174] Server -> STOPPING, получен SIGTERM (docker stop)` + `Сервер остановлен.`, в `docker compose stop` — `Stopping`/`Stopped` без `Killed` (`p4-06-server-sigterm.txt`, `p4-06-stop.txt`) |
| 9 | `docker compose kill -s KILL server` посреди операции | клиент: `Не удалось обработать ответ сервера` (`p4-06-client-killed.txt`); после пересоздания контейнера данные и версия на месте: `Primary -> SCHEMA READY, lsn = 4`, `Compare -> Primary = Replica`, `Read from Primary -> SUCCESS, 3 доступных автомобилей` (`p4-06-server-after-kill.log`) |
| 10 | `docker compose down` и снова `up -d` | удаляются контейнеры и сеть (`Network riot-lab8 Removed`), тома остаются (`lab8_primary-data`, `lab8_replica-data` в `docker volume ls`); после подъёма — `Автомобилей: 4. Аренд: 1. Читает Primary (lsn = 13).` и `#1 1234 AB-7 Active` (`p4b-0f-after-up.txt`, `p4-07-down.txt`) |

Отдельно про очередь Broker'а: она живёт в памяти процесса. В прогоне `p4c` события
копились в очередь недоступных подписчиков — `QUEUED RentalCreated -> CarService:
сохранено в очереди (3), ждёт восстановления подписчика` — но после `docker compose
stop/start broker` очередь пуста, в логе нет ни `FLUSH`, ни `RETRY из очереди`, а
переподключившиеся подписчики получают статус `SUBSCRIBER CONNECTED …, подписок пока
нет`. То есть переживание события в очереди гарантировано только пока жив контейнер
брокера; при его перезапуске событие не воспроизводится (Publisher шлёт событие один раз
и не хранит его сам). Возобновление подписчика без перезапуска процесса — пункт 6 меню
`Subscribers.exe`; в автоматическом прогоне `p4e` он не удал, потому что меню после «6»
ждёт имя подписчика, а генератор вводил просто `6\n6\n6\n` →
`Подписчик не зарегистрирован: '6'. Такого нет в списке.` Восстановление показано
перезапуском процесса подписчиков (строки `CONNECT` + `SUBSCRIBE … -> OK` выше).

## Unit-тесты

`dotnet test` по решению: **127 тестов, 127 пройдено, общее время 1,3633 минуты**
(`.temp/test-final2.log`). Разбивка по классам — из того же лога по числу пройденных
строк:

| Класс | Тестов | Что проверяет | Новое в ЛР №8 |
|---|---|---|---|
| `Tests/PubSubBrokerTests.cs` | 27 | маршрутизация, попытки доставки, очереди, режимы сбоя подписчика | нет |
| `Tests/ContainerConfigTests.cs` | 25 | значения `compose.yaml` разбираются кодом так же, как их разберёт контейнер | да |
| `Tests/ReplicationTests.cs` | 22 | перенос затронутых таблиц, `lsn`, сравнение копий, failover | нет |
| `Tests/RequestHandlerTests.cs` | 19 | 20 операций, транзакция/ROLLBACK, тексты ошибок | нет |
| `Tests/ClusterTests.cs` | 16 | `Majority = n/2 + 1`, `PickLeader`, голоса, вердикты | нет |
| `Tests/PubSubIntegrationTests.cs` | 8 | Publisher↔Broker↔подписчик в-process | нет |
| `Tests/ContainerIntegrationTests.cs` | 7 | операции против поднятого стенда | да |
| `Tests/LoadTests.cs` | 3 | пропускная способность, перцентили, конфликт за автомобиль | да |

Итого новых — **35** (25 + 7 + 3), перенесено из ЛР №7 — 92.

Требование → тесты (для ответов на защите):

| Требование методички | Тесты |
|---|---|
| конфигурация задаётся окружением, а не кодом | `MemberParse_ComposeClusterMembers_GivesFiveNodesByServiceName`, `Publisher_ComposeBrokerAddress_ParsedAsServiceName`, `ConnectionString_ComposePrimary_ParsesWithHostEqualToServiceName`, `ConnectionString_ComposeReplica_DiffersFromPrimaryOnlyByHost`, `Database_DefaultConnections_UsePortsPublishedByCompose` |
| неверные параметры конфигурации обнаруживаются, а не рушат процесс | `Cluster_DefectiveMembersSetting_ThrowsBeforeServerStarts`, `MemberParse_ComposeLikeSettingWithDefect_ThrowsFormatException`, `Publisher_MalformedBrokerAddress_IsSkippedAndNoticedInJournal`, `Publisher_SettingWithoutAnyValidAddress_FallsBackToLoopback` |
| имена сервисов резолвятся только внутри сети | `Publisher_BrokerHostOfComposeNetwork_IsNotResolvableFromHost`, `CarRentalDatabase_ComposeConnectionString_PingReturnsFalseOutsideTheNetwork` |
| деградация при отключённых расширениях (Pub/Sub и кластер) | `Cluster_EmptyMembersSetting_IsDisabledWithoutThrowing`, `Server_WithEmptyClusterAndBrokerSettings_WorksWithoutBothExtensions` |
| основные операции работают в контейнерах | `ContainerizedServer_MainOperations_ChangeData`, `ContainerizedServer_BrokerReceivesEvent_AfterCreateRental`, `ContainerizedServer_ConsensusWorks_OnFiveNodesWithMajority` |
| некорректный запрос не обрывает сервер | `ContainerizedServer_InvalidRequest_AnswersErrorAndStaysAlive` |
| недоступные Broker и узлы | `Server_UnreachableBrokerAndNodes_CompletesRequestWithoutBoth` |
| восстановление сервиса и чтение старых записей | `Recovery_AfterServiceRestart_NewInstanceReadsRecordsOfTheOldOne` |
| корректный гейт контейнерных тестов | `ContainerStand_GateClosesTestsAndFixesSkipMessage` |
| нагрузочное тестирование | `Load_MixedOperations_ThroughputAndLatency`, `Load_ParallelCreateRental_SameCar_ExactlyOneSucceeds`, `Load_ContainerizedServer_MixedOperations` |

Гейтинг. Тесты, идущие против стенда, открываются единственной переменной
(`ContainerStand.Enabled => Environment.GetEnvironmentVariable("RUN_CONTAINER_TESTS") == "1"`).
Без Docker `dotnet test` остаётся зелёным, а тест пишет причину
(`.temp/test-skip.log`):

```
пропущено: стенд не поднят (docker compose up -d) или не задан RUN_CONTAINER_TESTS=1
```

xUnit 2.4 не умеет снимать тест на лету, поэтому реализовано как «ничего не проверили и
вернулись успешно + напечатали причину», и отдельный тест
`ContainerStand_GateClosesTestsAndFixesSkipMessage` фиксирует и текст, и сам closed-gate,
чтобы skipping не превратился в тихое молчание. С поднятым стендом и `RUN_CONTAINER_TESTS=1`
тот же запуск даёт 127/127.

Нагрузочные тесты против стенда сделаны только читающими: `RunContainerClient` шлёт
`SearchCar`, `ShowState`, `ShowInstances`. Причина практическая — 30 запросов с записью на
стенде, где `PROCESSING_DELAY_MS = 1500` и каждый `CreateRental` поднимает `lsn`,
изменили бы демо-состояние и рассинхронизировали бы прологи Части 4, которые считаются по
`lsn`. Единственный конкурентный тест с записью —
`Load_ParallelCreateRental_SameCar_ExactlyOneSucceeds`: 20 потоков на один автомобиль,
но против тестовой базы `carrental_test`, а не стенда; результат `requests=20 ok=1
rejected=19`, `активных аренд у 1234 AB-7: 1 (ожидалось 1)` — общий ресурс защищён.

Замеры (`.temp/load-inprocess.txt` и `.temp/load-container.txt` — каталог `.temp` в корне
репозитория, туда пишет `LoadReport`; прогон 2026-11-03 13:55:49):

| Замер | Клиенты | Запросов | ok/err | throughput | avg | p50 | p95 | max |
|---|---|---|---|---|---|---|---|---|
| in-process, смешанные 4 операции | 10 × 4 | 40 | 40/0 | 66.9 req/s | 119.5 | 20 | 314 | 319 |
| in-process, конфликт за автомобиль | 20 | 20 | 1 ok / 19 rejected | 270.3 req/s | — | — | — | wall 74 мс |
| контейнерный стенд, 3 read-операции | 10 × 3 | 30 | 30/0 | 361.4 req/s | 23.2 | 24 | 34 | 34 |

По операциям в in-process-прогоне: `SearchCar` 11.4 мс в среднем, `ShowState` 5.3,
`CreateRental` 167.3, `ReturnCar` 294.1 — разрыв объясняется задержкой `PROCESSING_DELAY_MS`
и обходом обеих копий, а не сетью; в контейнерном прогоне все операции укладываются в 34 мс
потому, что reads не берут lock и не ждут задержку.

## Обработка ошибок

Ситуации из требования 6 методички и фактическое поведение:

| Ситуация | Поведение / где видно |
|---|---|
| Docker-образ не удалось собрать | сборка падает на шаге `dotnet publish`, `rc≠0`, лог целиком в файле прогона (`p1-build.log`, `p2-build-*.log`); в наших прогонах rc=0 у всех четырёх |
| контейнер не удалось запустить | `NODE_ID должен быть целым числом 1..5, получено ''…`, `docker run rc=1`, статус `Exited (1) Less than a second ago` — причина в stdout, видно без `docker logs` (`p1-badnode.txt`) |
| сервер недоступен | клиент: `Сервер недоступен. Сначала запустите проект Server`; при обрыве в середине — `Не удалось обработать ответ сервера` (`p4-06-client-killed.txt`) |
| необходимый сервис недоступен | Broker: `BROKER UNAVAILABLE (1001 мс) … Запрос клиента от этого не отменяется.` и `событие: … BROKER_UNAVAILABLE`; узлы: `Consensus -> SKIPPED: узлы кластера не отвечают, операция выполняется без консенсуса`; сервис не падает |
| подключение к БД завершилось ошибкой | `Primary -> UNAVAILABLE, сервер продолжит работу без этого экземпляра`; оба недоступны → `Недоступны оба экземпляра данных. Запрос не выполнен, сервер продолжает работу.` (`p1-badaddr-*`) |
| сетевое взаимодействие между контейнерами не работает | `could not translate host name …` в default bridge и `Connection refused` на `localhost` внутри контейнера (`p3-network.txt`); в compose это лечится пользовательской сетью `riot-lab8` и именами сервисов |
| указаны неправильные параметры конфигурации | `CLUSTER_MEMBERS: не разобран элемент '…', ожидается вид 1=127.0.0.1:6001`, `адрес Broker '…' некорректен — пропущен`, `ни один адрес Broker не разобран, использован 127.0.0.1:5001`; сервер поднимается без этих расширений |
| необходимый порт недоступен или занят | `Bind for 0.0.0.0:5000 failed: port is already allocated.`, rc=125 (`p1-portbusy.txt`) |
| приложение получило некорректный запрос | `Получен пустой запрос`, `Некорректный JSON`, `Некорректный запрос`, `Неизвестная операция` — соединение закрывается, сервер продолжает (`ContainerizedServer_InvalidRequest_AnswersErrorAndStaysAlive`) |
| контейнер остановился во время выполнения операции | SIGTERM: `Server -> STOPPING, получен SIGTERM (docker stop)` → `Сервер остановлен.`; SIGKILL: клиент получает `Не удалось обработать ответ сервера`, данные при этом не теряются (`p4-06-*`) |
| после повторного запуска сервис временно недоступен | `depends_on: condition: service_healthy` + healthcheck `pg_isready` (interval 2s, retries 30), сервер стартует только когда обе копии healthy; сервер, поднявшийся раньше БД, пишет `Initial -> Проверка начального состояния пропущена` и догоняет heartbeat'ом |
| данные отсутствуют или недоступны после перезапуска | тома `lab8_primary-data`/`lab8_replica-data` переживают `down` (`p4-07-down.txt`), `db/init.sql` создаёт схему на пустом томе, а `PrepareSchema` пересоздаёт её, если экземпляр вернулся пустым: `Primary -> SCHEMA READY после восстановления` |
| Docker Compose не удалось запустить из-за ошибки конфигурации | проверка без запуска: `docker compose config -q` (rc=0 в `p3-config.txt`); при ошибке compose печатает имя файла и строку, стенд не поднимается вообще, а не наполовину |

## Доработки

**Вся конфигурация через переменные окружения — реализовано (01.11.2026)**
`SERVER_PORT`, `PROCESSING_DELAY_MS`, `HEARTBEAT_INTERVAL_MS`,
`CARRENTAL_PRIMARY_CONNECTION`, `CARRENTAL_REPLICA_CONNECTION`, `PUBLISHER_BROKERS`,
`CLUSTER_MEMBERS`, `NODE_ID`, `BROKER_ID`, `BROKER_PORT`, `BROKER_RETRY_DELAY_MS`,
`BROKER_QUEUE_PUMP_MS`, `SERVER_HOST`, `SUBSCRIBER_BROKER_HOST`, `SUBSCRIBER_BROKER_PORT`
с дефолтами, равными константам ЛР №7, — запуск на хосте не изменился (`Server/Program.cs`,
`Nodes/Program.cs`, `Client/Program.cs`, `Subscribers/Program.cs`).

**`TcpListener(IPAddress.Any, port)` вместо `IPAddress.Loopback` — реализовано (01.11.2026)**
в `Server/Program.cs` и `Broker/Program.cs`. Loopback-слушатель в контейнере недоступен
извне вообще: публикуемый порт был бы открыт, но соединение разрывалось. Побочный плюс —
тот же образ работает и на хосте.

**SIGTERM graceful shutdown — реализовано (01.11.2026)**
`PosixSignalRegistration` в Server, Broker и Nodes. Каждый пишет в журнал
`STOPPING, получен SIGTERM (docker stop)` и гасит `CancellationTokenSource`; обработчики
клиентских соединений завершают текущую операцию. Узел получил обработчик при финальном
разборе — раньше контейнер узла умирал молча, и в `p4c-02-node3-sigterm.txt` теперь видны
и `STOPPING`, и `Node 3 остановлен.`

**Пересоздание схемы при возвращении экземпляра — реализовано (01.11.2026)**
`InstanceManager.PrepareSchema` вызывается только на переходе unavailable → available,
поэтому не мешает чтению и спасает пустой том (строка `SCHEMA READY после восстановления`).

**`db/init.sql` и отдельная тестовая база — реализовано (01.11.2026)**
`init.sql` монтируется в `/docker-entrypoint-initdb.d` обеих копий и создаёт `carrental`,
`carrental_test`, пять таблиц и демо-данные (2 локации, 4 автомобиля, `replication_meta.lsn
= 0`). Тесты (`Tests/TestRig.cs`) ходят в `carrental_test` на 5433/5434, поэтому прогон
тестов не трогает состояние стенда.

**healthcheck + depends_on по состоянию — реализовано (01.11.2026)**
`pg_isready -U postgres -d carrental`, `interval: 2s`, `timeout: 3s`, `retries: 30`;
`server` ждёт `service_healthy` обеих копий и `service_started` брокера. В `p3-up.txt`
видно `Waiting` → `Healthy` перед стартом сервера.

**Тесты контейнеризации — реализовано (01.11.2026)**
`Tests/ContainerConfigTests.cs` (25), `Tests/ContainerIntegrationTests.cs` (7, гейт
`RUN_CONTAINER_TESTS=1`), `Tests/LoadTests.cs` (3, отчёт в `.temp/load-*.txt`).

**Многоэтапная сборка + `.dockerignore` — реализовано (01.11.2026)**
`sdk:8.0 → runtime:8.0`, публикация в `/publish`, в финальный слой попадают только dll;
контекст сборки чистится от `bin/obj/.git/Tests/Subscribers/*.md/.env/Dockerfile`.

**Клиент в профиле `tools` — реализовано (01.11.2026)**
`docker compose up -d` поднимает 9 контейнеров, интерактивный клиент остаётся хостовым;
внутри сети его можно запустить одной командой с `--profile tools`.

## Ограничения, которые оставили сознательно

1. **`container_name` у каждого сервиса равен имени сервиса** (`server`, `broker`,
   `node1…node5`, `primary`, `replica`). Без него compose выдал бы имя с номером
   экземпляра — `lab8-node1-1`, и у всех пяти узлов был бы суффикс `-1`, потому что узлы —
   это пять отдельных сервисов по одному контейнеру (общий сервис с `--scale` не подходит:
   каждому нужен свой `NODE_ID`). Группировку в Docker Desktop даёт не имя контейнера, а
   метка проекта (`com.docker.compose.project = lab8`), так что «папка» остаётся.
   Плата за короткие имена: они глобальны на хосте (одновременно с другим стендом, где есть
   контейнер `server`, этот не поднимется) и `docker compose up --scale` для таких сервисов
   запрещён. Внутри сети пиры обращаются по именам **сервисов**, так что на связность имена
   контейнеров не влияют.
2. **Конфигурационные тесты хранят значения compose как константы и не парсят
   `compose.yaml`** (`Compose` в `Tests/ContainerConfigTests.cs`). При смене пароля или
   адреса надо синхронизировать константы вручную; первыми упадут интеграционные тесты,
   что и есть сигнал рассинхрона.
3. **Тесты, трогющие PostgreSQL, требуют поднятого стенда.** `ResetPair()` в
   `RequestHandlerTests`, `ReplicationTests`, `LoadTests`, `ContainerIntegrationTests`,
   `ContainerConfigTests` работают с `carrental_test` на 5433/5434; голый `dotnet test`
   без Docker красный. Контейнерные тесты закрыты гейтом `RUN_CONTAINER_TESTS=1` и
   печатают текст пропуска.
4. **InstanceManager помечает экземпляр Unavailable, если БД отвечает, но схемы нет**
   (`DataInstance`: `Ping()` ok, `GetLsn()` бросает `NpgsqlException`). `PrepareSchema`
   срабатывает только на переходе unavailable → available, поэтому созданный руками
   `DROP SCHEMA` на живом экземпляре не восстановится — спасает только `db/init.sql` на
   свежем томе или перезапуск сервиса.
5. **Publisher при мусорном `PUBLISHER_BROKERS` откатывается на `127.0.0.1:5001`** —
   внутри контейнера это «в себя». Поведение зафиксировано тестом
   `Publisher_SettingWithoutAnyValidAddress_FallsBackToLoopback` и документировано в
   `Tests/ContainerConfigTests.cs`: откат не молчит, он виден в журнале строкой
   `ни один адрес Broker не разобран, использован 127.0.0.1:5001`.
6. **Клиент в контейнере — опция, а не основной путь.** Он в профиле `tools`, потому что
   интерактивный; основной прогон — хостовый `Client.exe` по опубликованному 5000.
7. **Порты PostgreSQL опубликованы как 5433/5434, а не 5432**, потому что 5432 на хосте
   занят локальным инстансом. Внутри сети `riot-lab8` — обычный `Port=5432`, и это видно
   в `CARRENTAL_PRIMARY_CONNECTION`.
8. **Многоэтапная сборка без `--self-contained`.** В финальном слое рантайм берётся из
   `mcr.microsoft.com/dotnet/runtime:8.0`; экономия на самостоятельном bundle сознательно
   принесена в пользу стандартного и читаемого Dockerfile.
9. **Один образ `riot-lab8/nodes:1.0` на пять узлов.** Узлы различаются только `NODE_ID`,
   `CLUSTER_MEMBERS` и публикуемым портом, поэтому `EXPOSE 6001` в `Nodes/Dockerfile`
   условен для node1: в `docker compose ps` у `node2…node5` он виден как незакрытый
   `6001/tcp` и ни на что не влияет.
10. **`TZ: Europe/Minsk` проставлен всем сервисам.** Без него журналы контейнеров в UTC,
    и строки сервера, брокера и хостовых подписчиков не сходятся по времени; проверено
    `docker exec primary date` → `+03` (на старом имени контейнера — то же самое).
11. **Секретов в образе нет, но и хранилища секретов тоже нет.** Пароль `postgres` —
    значение для учебного стенда, переопределяется `POSTGRES_PASSWORD`. `.env` исключён из
    контекста сборки `.dockerignore` и не попадает в git (`.gitignore` в `labs/lab8`),
    `.env.example` — только образец с именами переменных.

## Что сказать на защите

1. Проект не переписывался: то же приложение, что в ЛР №7, только каждый компонент вынесен
   в контейнер, а адреса взяты из окружения.
2. Основной компонент — сервер; базовый образ `sdk:8.0` для сборки, `runtime:8.0` для
   запуска; в финальном слое только `/publish`, 197 МБ (`p1-images.txt`).
3. Многоэтапность решает две вещи: слой restore не пересобирается от правки `.cs` и в
   образ не попадает SDK.
4. `.dockerignore` отсекает `bin/obj/.git/Tests/Subscribers/*.md/.env` — без него контекст
   тянул бы мусор и секреты.
5. Контейнеров девять: server, broker, node1…node5, primary, replica; client — десятый,
   в профиле `tools`.
6. Сеть пользовательская `riot-lab8`, потому что только в ней работает встроенный DNS;
   в default bridge имя не резолвится (Э2), а `localhost` внутри контейнера — это сам
   контейнер (Э1). Поэтому `Host=primary`, `broker:5001`, `node3:6003`.
7. Имена контейнеров = имена сервисов (`container_name`), а «папка» lab8 в Docker Desktop —
   это метка проекта `name: lab8`; суффикс `-1` от дефолтного нейминга не нужен, потому что
   каждый узел — отдельный сервис, и пиры адресуются по именам сервисов.
8. `EXPOSE` — документация, публикация делается `ports:`; видно по `6001/tcp` у node2…5.
9. Порты БД наружу — 5433/5434 (локальный 5432 занят), внутри сети 5432.
10. Порядок старта: `healthcheck pg_isready` + `depends_on: service_healthy`, иначе сервер
    стартует против пустой базы и первый `CHECK` даёт `UNAVAILABLE`.
11. Отказ сервисов смоделирован и разобран: broker (запись проходит, событие помечено
    `BROKER_UNAVAILABLE`), узлы (большинство 3/5 держит консенсус), primary (failover на
    replica + `SCHEMA READY после восстановления`), server (SIGTERM штатно, SIGKILL —
    клиент видит `Не удалось обработать ответ сервера`, данные целы).
12. Данные переживают `down`: `docker volume ls` показывает `lab8_primary-data` и
    `lab8_replica-data`, после `up -d` читается `Аренд: 1 … lsn = 13` и запись
    `#1 1234 AB-7 Active`.
13. Очередь брокера — in-memory: после перезапуска контейнера накопленные события не
    воспроизводятся; в журнале это видно по отсутствию `FLUSH` после рестарта.
14. Тесты: 127, из них 35 новых для ЛР №8; контейнерные гейтятся `RUN_CONTAINER_TESTS=1`,
    нагрузочные против стенда только читающие, конфликтный тест с записью идёт в
    `carrental_test`.
15. Нагрузку сравнили: 66.9 req/s в in-process со сценарием записи против 361.4 req/s
    чтения на контейнерном стенде при p95 34 мс — узкое место не Docker, а
    `PROCESSING_DELAY_MS` и обход двух копий.

## Контрольные вопросы — краткие ответы

1. **Что такое Docker?** Платформа сборки и запуска приложения в изолированных контейнерах
   на общем ядре Linux/WSL2. Здесь: четыре `Dockerfile` в `*/Dockerfile`, образы
   `riot-lab8/{server,broker,nodes,client}:1.0`, запуск через `docker compose up -d`.
2. **Что такое контейнеризация?** Вынос каждого процесса приложения в отдельный контейнер со
   своим файловой системой-слоем, сетью и окружением. У нас 9 процессов ЛР №7 стали 9
   сервисами compose, а конфиг переехал из кода в `environment:`.
3. **Чем контейнер отличается от виртуальной машины?** У ВМ своё ядро и гипервизор, у
   контейнера — общее ядро и изоляция namespace/cgroups. Практически: образ сервера 197 МБ
   против гигабайтного образа ВМ, старт за секунды (`p3-up.txt`), а память PostgreSQL
   ограничена тем же хостом, что и сервер.
4. **Что такое Docker Image?** Immutable-набор слоёв, из которого создаётся контейнер.
   `riot-lab8/server:1.0 43f8288b61f6 197MB` (`p1-images.txt`), слои видно в
   `docker history`: `COPY /publish ./`.
5. **Что такое Docker Container?** Запущенный экземпляр образа с записями в верхнем слое и
   своей сетевой идентичностью. `lab8-server-1 … Up 5 minutes 0.0.0.0:5000->5000/tcp`,
   `lab8-primary-1 … Up (healthy)` (`p3-ps.txt`).
6. **Что такое Dockerfile?** Декларация сборки образа. `Server/Dockerfile`: `FROM sdk:8.0`
   → `COPY Server.csproj` → `dotnet restore` → `COPY` источников → `dotnet publish -c
   Release -o /publish --no-restore` → `FROM runtime:8.0` → `COPY --from=build` →
   `EXPOSE` → `ENTRYPOINT`.
7. **Для чего используется инструкция ENTRYPOINT?** Задаёт главный процесс контейнера,
   который получает SIGTERM от `docker stop`. У нас `ENTRYPOINT ["dotnet","Server.dll"]`, и
   потому `docker compose stop server` доходит до `PosixSignalRegistration` и даёт строку
   `Server -> STOPPING, получен SIGTERM (docker stop)`.
8. **Что такое Docker Engine?** Демон с containerd/Runc, REST-API и CLI: собирает образы,
   создаёт контейнеры, сети (`bridge`) и тома. Всё, что мы видим командами `docker`/`compose`.
9. **Для чего используется Docker Desktop?** GUI + локальный движок на Windows: список
   контейнеров по проекту `lab8`, логи, volumes, порт 5000 наружу. Плюс WSL2-бэкенд, на
   котором живут наши `/172.21.0.x`.
10. **Что такое Docker Network?** Виртуальная сеть между контейнерами. `riot-lab8` —
    пользовательская bridge-сеть (`p3-network-containers.txt`), в ней работает DNS по именам
    сервисов; default bridge резолвить имена не умеет (`p3-network.txt` Э2).
11. **Почему контейнеры могут взаимодействовать по сетевым именам?** В пользовательской сети
    есть встроенный DNS-сервер, регистрирующий имена сервисов/контейнеров. Отсюда
    `Host=primary`, `broker:5001`, `1=node1:6001,…` в `compose.yaml` — и ровно это
    проверяют `MemberParse_ComposeClusterMembers_GivesFiveNodesByServiceName` и
    `Publisher_ComposeBrokerAddress_ParsedAsServiceName`.
12. **Почему localhost внутри контейнера не указывает на другой контейнер?** У каждого
    контейнера своё сетевое пространство, `127.0.0.1` — его собственный loopback. Доказательство
    Э1: `connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection
    refused`, тогда как по имени сервиса тот же запрос возвращает `4`.
13. **Что такое Docker Compose?** Декларативный описатель многосервисного стенда: образы,
    окружение, порты, тома, сеть, порядок старта. `compose.yaml` (10 сервисов),
    `docker compose config -q` → rc=0, `docker compose up -d` → 9 контейнеров (п3).
14. **Что такое Environment Variables и зачем они нужны?** Настройка процесса извне без
    пересборки образа — один образ работает и на хосте, и в сети compose. Полный перечень и
    дефолты — в таблице раздела «Запуск»; без `TZ` журналы разъезжаются по времени, с пустым
    `CARRENTAL_PRIMARY_CONNECTION` сервер уходит в `Primary -> UNAVAILABLE`.
15. **Для чего используется Docker Volume?** Персистентные данные вне слоя контейнера.
    `lab8_primary-data`/`lab8_replica-data` переживают `docker compose down`
    (`p4-07-down.txt`), и после `up -d` читается `Аренд: 1 … (lsn = 13)`; `db/init.sql`
    монтируется read-only в `/docker-entrypoint-initdb.d`.
16. **Почему инструкция EXPOSE не означает автоматическую публикацию порта?** `EXPOSE` —
    метаданные о том, что процесс слушает; публикует только `ports:`. В `p3-ps.txt` у
    `node2…node5` видно `6001/tcp` (EXPOSE) без маппинга и `0.0.0.0:600N->600N/tcp` (ports).
17. **Почему контейнеризация сама по себе не обеспечивает сохранность данных?** Слой записи
    контейнера живёт до его удаления, а перезапуск с `--rm` или `down` стирает всё. Нужно
    либо том (у нас — для обеих баз), либо внешнее хранилище; события в очереди Broker'а —
    ровно тот случай, который мы сознательно не делаем переживающим рестарт: после
    `stop/start broker` очередь пуста.
18. **Чем Docker отличается от Kubernetes и почему контейнеризация рассматривается перед
    оркестрацией?** Docker решает «упаковать и запустить один узел», Compose — «поднять
    стенд на одной машине». Kubernetes добавляет расписание на кластер машин, desired state,
    self-healing и rolling update, но не заменяет образ и конфигурацию. Логичный порядок:
    сначала сделать компонент самодостаточным контейнером (наши `Dockerfile` и env-конфиг),
    потом описать стенд (compose), и только потом — оркестратор; наши узлы с `NODE_ID` и
    `CLUSTER_MEMBERS` — уже то, что в Kubernetes стало бы StatefulSet с стабильными именами.

## Файлы проекта

| Путь | Что внутри |
|---|---|
| `compose.yaml` | 10 сервисов, сеть `riot-lab8`, тома `primary-data`/`replica-data`, healthcheck, профиль `tools` |
| `db/init.sql` | схема `carrental` + `carrental_test`, `replication_meta`, 2 локации и 4 автомобиля |
| `.env.example` | образец `POSTGRES_PASSWORD`, `PROCESSING_DELAY_MS`; в git не попадает, `.gitignore` закрывает `.env` |
| `DistributedSystem/.dockerignore` | `bin/obj/.vs/.idea/TestResults/.git/*.md/*.log/.env/Tests/Subscribers/*.sln/**/Dockerfile` |
| `DistributedSystem/Server/Dockerfile` | многоэтапная сборка сервера, `EXPOSE 5000`, `ENTRYPOINT dotnet Server.dll` |
| `DistributedSystem/Broker/Dockerfile` | то же для брокера, `EXPOSE 5001` |
| `DistributedSystem/Nodes/Dockerfile` | один образ на пять узлов, `EXPOSE 6001` (условно для node1) |
| `DistributedSystem/Client/Dockerfile` | клиент, `EXPOSE 5000` показывает, куда он ходит |
| `DistributedSystem/Server/*.cs` | `Program.cs`, `RequestHandler.cs`, `InstanceManager.cs`, `DataInstance.cs`, `CarRentalDatabase.cs`, `Replicator.cs`, `Publisher.cs`, `Cluster.cs`, `Journal.cs` |
| `DistributedSystem/Broker/*.cs` | `Program.cs`, `EventBroker.cs`, `EventQueue.cs`, `DeliveryManager.cs`, `SubscriptionRegistry.cs`, `SubscriberChannel.cs`, `PublisherConnection.cs`, `SubscriberConnection.cs` |
| `DistributedSystem/Nodes/*.cs` | `Program.cs`, `ClusterNode.cs` |
| `DistributedSystem/Shared/**` | `Protocol/` (`Request`, `Response`, `CreateRentalData`, `ClusterData`, `InstanceInfo`, `RentalIdData`), `Events/` (`DomainEvent`, `PubSubMessages`, `PubSubJson`, `LineConnection`, `EventJournal`, `EventTypes`), `Cluster/` (`ClusterRules`, `ClusterMember`, `ClusterMessages`, `ClusterTransport`), `Models/` |
| `DistributedSystem/Client/Program.cs` | меню 1–21, `SERVER_HOST`/`SERVER_PORT` |
| `DistributedSystem/Subscribers/Program.cs`, `SubscriberInstance.cs` | четыре подписчика, режимы сбоя, задержки, меню 1–10 |
| `DistributedSystem/Tests/*.cs` | 8 классов, 127 тестов; новые — `ContainerConfigTests.cs`, `ContainerIntegrationTests.cs`, `LoadTests.cs`; инфраструктура — `TestRig.cs`, `FakeSubscriber.cs`, `GlobalUsings.cs` |
| `.temp/lab8/`, `.temp/load-*.txt`, `.temp/test-final2.log` | файлы-доказательства прогонов Частей 1–4, нагрузочных замеров и тестов |

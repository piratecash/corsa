# CORSA

## English

**CORSA** is a decentralized messenger and DEX-oriented communication project that is currently being rebuilt around the **Gazeta** protocol.

Current repository status:

- current Corsa version: see `internal/core/config.CorsaVersion`
- Go-based desktop and node foundation
- desktop chat-style UI with identity list and direct messaging
- mesh-style peer sync between local nodes
- node role model: `full` or `client`
- identity based on public-key fingerprint addresses
- signed and encrypted direct addressed messages over the mesh
- signed box-key discovery with local TOFU trust pinning
- every message carries a UUID, deletion flag, timestamp, and optional TTL
- messages outside the allowed clock drift are rejected and not forwarded
- `Gazeta` anonymous encrypted notices with TTL
- primary wire protocol is JSON frames over TCP

Main docs:

- **roadmap: [docs/roadmap.md](docs/roadmap.md)** — protocol evolution plan: mesh relay, gossip, routing, reputation, onion DM, BLE transport, Meshtastic bridge, and more
- architecture: [docs/architecture.md](docs/architecture.md)
- protocol: [docs/protocol.md](docs/protocol.md)
- encryption: [docs/encryption.md](docs/encryption.md)
- donate: [docs/donate.md](docs/donate.md)

Desktop localization:

- built-in UI languages: English, Arabic, Spanish, Chinese, Russian, French
- startup default can be set with `CORSA_LANGUAGE=en|ar|es|zh|ru|fr`
- language can also be switched directly in the desktop UI
- desktop remembers the last selected language per identity

Repository layout:

- `cmd/corsa-desktop` — desktop application with embedded local node
- `cmd/corsa-node` — standalone node process
- `internal/core` — protocol, identity, node, services
- `docs` — architecture and protocol notes

Git hooks:

- hook installer: `scripts/install-hooks.sh`
- current pre-commit hook runs `make lint`
- install with: `make install-hooks`
- verify with: `make hooks-status`
- expected value: `./scripts/hooks/`

Run examples:

```bash
CORSA_LISTEN_ADDRESS=:64646 CORSA_BOOTSTRAP_PEER=127.0.0.1:64647 GOCACHE=$(pwd)/.gocache GOMODCACHE=$(pwd)/.gomodcache go run ./cmd/corsa-desktop
```

```bash
CORSA_LISTEN_ADDRESS=:64647 CORSA_BOOTSTRAP_PEER=127.0.0.1:64646 GOCACHE=$(pwd)/.gocache GOMODCACHE=$(pwd)/.gomodcache go run ./cmd/corsa-desktop
```

Default identity files are created under `.corsa/` and are separated by listen port.

Default bootstrap seed:

- `65.108.204.190:64646`

For public or VPS nodes, the practical network settings are:

- `CORSA_LISTEN_ADDRESS` — local bind address
- `CORSA_LISTENER` — explicit inbound listener override: `1` enables listen, `0` disables it
- `CORSA_ADVERTISE_ADDRESS` — external address announced to peers
- `CORSA_BOOTSTRAP_PEERS` — comma-separated seed list
- `CORSA_TRUST_STORE_PATH` — local pinned-contact trust database
- `CORSA_QUEUE_STATE_PATH` — persisted pending-delivery and relay-retry state
- `CORSA_NODE_TYPE` — `full` or `client`
- `CORSA_MAX_OUTGOING_PEERS` — max outbound peer sessions, default `8`
- `CORSA_MAX_INCOMING_PEERS` — optional inbound peer cap; `0` means no app-level cap
- `CORSA_MAX_CLOCK_DRIFT_SECONDS` — allowed past/future clock drift for relayed messages, default `600`

Message metadata and relay rules:

- each `SEND_MESSAGE` frame carries `message-id`, `flag`, `timestamp`, and `ttl-seconds`
- `FETCH_MESSAGE_IDS <topic>` returns only UUIDs for lightweight sync
- `FETCH_MESSAGE <topic> <uuid>` loads one message by UUID
- the preferred protocol form is one JSON object per line
- supported flags:
  - `immutable`
  - `sender-delete`
  - `any-delete`
  - `auto-delete-ttl`
- `auto-delete-ttl` messages are removed automatically after `ttl-seconds`
- nodes reject and do not forward messages that are too far in the past or future
- for direct messages, `ttl-seconds` is optional and acts as delivery lifetime counted from `created_at`
- pending direct-message delivery and delivery-receipt retry state are persisted on disk and survive node restarts
- desktop delivery lifecycle for outgoing direct messages is: `queued -> retrying -> failed/expired -> delivered/seen`

Node roles:

- `full` — forwards mesh traffic, relays direct messages and `Gazeta` notices
- `client` — syncs peers and contacts, stores local traffic, but does not forward mesh traffic
- `client` defaults to `CORSA_LISTENER=0`, so it keeps outbound sessions and identity sync without advertising a dialable peer endpoint
- `full` defaults to `CORSA_LISTENER=1`
- `listener` only controls whether a node accepts inbound connections; it does not change relay behavior
- even if `client` is started with `CORSA_LISTENER=1`, it must not be used as a relay for foreign traffic
- a `client` still sends its own direct messages and delivery receipts upstream; the restriction only applies to third-party traffic
- default for `corsa-node`: `full`
- default for `corsa-desktop`: `full`
- recommended future default for mobile/light client: `client`

Example:

```bash
CORSA_LISTEN_ADDRESS=:64646 \
CORSA_ADVERTISE_ADDRESS=<your-public-ip>:64646 \
CORSA_BOOTSTRAP_PEERS=65.108.204.190:64646 \
GOCACHE=$(pwd)/.gocache \
GOMODCACHE=$(pwd)/.gomodcache \
go run ./cmd/corsa-node
```

Docker example:

```bash
docker compose up -d --build
```

The container runs as non-root user `corsa` (`uid=10001`).

The included [docker-compose.yaml](docker-compose.yaml) already uses:

- `restart: unless-stopped`
- named volume `corsa-data`
- non-root runtime user

Manual `docker run` example:

```bash
docker volume create corsa-data
docker run -d -p 64646:64646 \
  --name corsa-node \
  --restart unless-stopped \
  -e CORSA_LISTEN_ADDRESS=:64646 \
  -e CORSA_ADVERTISE_ADDRESS=<your-public-ip>:64646 \
  -e CORSA_BOOTSTRAP_PEERS=65.108.204.190:64646 \
  -v corsa-data:/home/corsa/.corsa \
  corsa-node
```

For host bind mount usage, prepare the directory for `uid=10001` first:

```bash
mkdir -p .corsa
sudo chown -R 10001:10001 .corsa
docker run -d -p 64646:64646 \
  --name corsa-node \
  --restart unless-stopped \
  -e CORSA_LISTEN_ADDRESS=:64646 \
  -e CORSA_ADVERTISE_ADDRESS=<your-public-ip>:64646 \
  -e CORSA_BOOTSTRAP_PEERS=65.108.204.190:64646 \
  -v $(pwd)/.corsa:/home/corsa/.corsa \
  corsa-node
```

Donate:

- PirateCash: `PB2vfGqfagNb12DyYTZBYWGnreyt7E4Pug`
- Cosanta: `Cbbp3meofT1ESU5p4d9ucXpXw9pxKCMEyi`
- PIRATE / COSANTA (BEP-20): `0x52be29951B0D10d5eFa48D58363a25fE5Cc097e9`
- Bitcoin: `bc1q2ph64sryt6skegze6726fp98u44kjsc5exktap`
- DASH: `Xv7U37XKp5d4fjvbeuganwhqXN7Sm4JJkt`
- Zcash (t-address): `t1bDVifcVuXWW8QctVdhuEghm8WqZjvU1US`
- Zcash (shielded): `zs1hwyqs4mfrynq0ysjmhv8wuau5zam0gwpx8ujfv8epgyufkmmsp6t7cfk9y0th7qyx7fsc5azm08`
- Monero: `4AzdEoZxeGMFkdtAxaNLAZakqEVsWpVb2at4u6966WGDiXkS7ZPyi7haeThTGUAWXVKDTmQ9DYTWRHMjGVSBW82xRQqPxkg`

Source: [pirate.cash/en/donate](https://pirate.cash/en/donate/)

---

## Русский

**CORSA** — это децентрализованный мессенджер и коммуникационный слой для DEX, который сейчас пересобирается вокруг протокола **Gazeta**.

Текущее состояние репозитория:

- текущая версия Corsa: см. `internal/core/config.CorsaVersion`
- Go-основа для desktop-приложения и ноды
- desktop UI в формате чата: список identity слева и direct messaging справа
- mesh-синхронизация между локальными узлами
- модель ролей узла: `full` или `client`
- identity через адрес как fingerprint публичного ключа
- подписанные и зашифрованные адресные direct-сообщения поверх mesh
- подписанное распространение box key и локальный TOFU trust pinning
- каждое сообщение несет UUID, флаг удаления, timestamp и опциональный TTL
- сообщения вне допустимого дрейфа времени отклоняются и не форвардятся
- анонимные зашифрованные объявления `Gazeta` с временем жизни

Основные документы:

- **дорожная карта: [docs/roadmap.ru.md](docs/roadmap.ru.md)** — план развития протокола: mesh relay, gossip, маршрутизация, репутация, onion DM, BLE транспорт, Meshtastic bridge и другое
- архитектура: [docs/architecture.md](docs/architecture.md)
- протокол: [docs/protocol.md](docs/protocol.md)
- шифрование: [docs/encryption.md](docs/encryption.md)
- донаты: [docs/donate.md](docs/donate.md)

Локализация desktop-клиента:

- встроенные языки UI: английский, арабский, испанский, китайский, русский и французский
- стартовый язык можно задать через `CORSA_LANGUAGE=en|ar|es|zh|ru|fr`
- язык также можно переключать прямо в desktop UI
- desktop-клиент запоминает последний выбранный язык отдельно для каждой identity

Структура репозитория:

- `cmd/corsa-desktop` — desktop-приложение со встроенной локальной нодой
- `cmd/corsa-node` — отдельный процесс ноды
- `internal/core` — протокол, identity, нода, сервисы
- `docs` — описание архитектуры и протокола

Git хуки:

- установщик хука: `scripts/install-hooks.sh`
- текущий `pre-commit` запускает `make lint`
- установить: `make install-hooks`
- проверить: `make hooks-status`
- ожидаемое значение: `./scripts/hooks/`

Примеры запуска:

```bash
CORSA_LISTEN_ADDRESS=:64646 CORSA_BOOTSTRAP_PEER=127.0.0.1:64647 GOCACHE=$(pwd)/.gocache GOMODCACHE=$(pwd)/.gomodcache go run ./cmd/corsa-desktop
```

```bash
CORSA_LISTEN_ADDRESS=:64647 CORSA_BOOTSTRAP_PEER=127.0.0.1:64646 GOCACHE=$(pwd)/.gocache GOMODCACHE=$(pwd)/.gomodcache go run ./cmd/corsa-desktop
```

По умолчанию identity-файлы создаются в `.corsa/` и разделяются по порту прослушивания.

Для публичных или VPS-нод практические сетевые настройки такие:

- `CORSA_LISTEN_ADDRESS` — локальный bind-адрес
- `CORSA_LISTENER` — явное переопределение входящего listener: `1` включает прослушивание, `0` выключает
- `CORSA_ADVERTISE_ADDRESS` — внешний адрес, который нода объявляет пирам
- `CORSA_BOOTSTRAP_PEERS` — список seed-нод через запятую
- `CORSA_TRUST_STORE_PATH` — локальная база pinned-контактов
- `CORSA_QUEUE_STATE_PATH` — persisted-состояние очереди доставки и relay retry
- `CORSA_NODE_TYPE` — `full` или `client`
- `CORSA_MAX_OUTGOING_PEERS` — максимум исходящих peer-session, по умолчанию `8`
- `CORSA_MAX_INCOMING_PEERS` — опциональный лимит на входящие peer-соединения; `0` означает без app-level лимита
- `CORSA_MAX_CLOCK_DRIFT_SECONDS` — допустимый дрейф времени для ретранслируемых сообщений, по умолчанию `600`

Метаданные сообщений и правила relay:

- каждый `SEND_MESSAGE` кадр несет `message-id`, `flag`, `timestamp` и `ttl-seconds`
- `FETCH_MESSAGE_IDS <topic>` возвращает только UUID для легкой синхронизации
- `FETCH_MESSAGE <topic> <uuid>` загружает одно сообщение по UUID
- поддерживаемые флаги:
  - `immutable`
  - `sender-delete`
  - `any-delete`
  - `auto-delete-ttl`
- сообщения с `auto-delete-ttl` автоматически удаляются после `ttl-seconds`
- ноды отклоняют и не форвардят сообщения, которые слишком далеко в прошлом или будущем
- для direct message поле `ttl-seconds` опционально и задает срок доставки, считающийся от `created_at`
- состояние очереди доставки direct message и retry delivery receipt сохраняется на диск и переживает рестарт ноды
- desktop показывает жизненный цикл исходящего direct message как: `queued -> retrying -> failed/expired -> delivered/seen`
- основной wire-format теперь: один JSON-объект на строку

Роли узла:

- `full` — форвардит mesh-трафик, ретранслирует direct messages и `Gazeta` notices
- `client` — синкает peers и contacts, хранит локальный трафик, но не делает mesh-forwarding
- `client` по умолчанию работает с `CORSA_LISTENER=0`, то есть держит исходящие сессии и синхронизацию identity без объявления dialable peer endpoint
- `full` по умолчанию работает с `CORSA_LISTENER=1`
- `listener` управляет только входящими соединениями и не меняет relay-роль узла
- даже если `client` запущен с `CORSA_LISTENER=1`, его нельзя использовать как relay для чужого трафика
- при этом `client` все равно отправляет свои собственные direct messages и delivery receipts вверх по upstream; ограничение касается только чужого трафика
- значение по умолчанию для `corsa-node`: `full`
- значение по умолчанию для `corsa-desktop`: `full`
- рекомендуемое будущее значение для mobile/light client: `client`

Bootstrap seed по умолчанию:

- `65.108.204.190:64646`

Пример:

```bash
CORSA_LISTEN_ADDRESS=:64646 \
CORSA_ADVERTISE_ADDRESS=<your-public-ip>:64646 \
CORSA_BOOTSTRAP_PEERS=65.108.204.190:64646 \
GOCACHE=$(pwd)/.gocache \
GOMODCACHE=$(pwd)/.gomodcache \
go run ./cmd/corsa-node
```

Пример через Docker:

```bash
docker compose up -d --build
```

Контейнер запускается не от `root`, а от пользователя `corsa` (`uid=10001`).

[docker-compose.yaml](docker-compose.yaml) уже использует:

- `restart: unless-stopped`
- named volume `corsa-data`
- запуск не от `root`

Ручной пример через `docker run`:

```bash
docker volume create corsa-data
docker run -d -p 64646:64646 \
  --name corsa-node \
  --restart unless-stopped \
  -e CORSA_LISTEN_ADDRESS=:64646 \
  -e CORSA_ADVERTISE_ADDRESS=<your-public-ip>:64646 \
  -e CORSA_BOOTSTRAP_PEERS=65.108.204.190:64646 \
  -v corsa-data:/home/corsa/.corsa \
  corsa-node
```

Для использования bind mount с хоста сначала подготовь каталог под `uid=10001`:

```bash
mkdir -p .corsa
sudo chown -R 10001:10001 .corsa
docker run -d -p 64646:64646 \
  --name corsa-node \
  --restart unless-stopped \
  -e CORSA_LISTEN_ADDRESS=:64646 \
  -e CORSA_ADVERTISE_ADDRESS=<your-public-ip>:64646 \
  -e CORSA_BOOTSTRAP_PEERS=65.108.204.190:64646 \
  -v $(pwd)/.corsa:/home/corsa/.corsa \
  corsa-node
```

Donate:

- PirateCash: `PB2vfGqfagNb12DyYTZBYWGnreyt7E4Pug`
- Cosanta: `Cbbp3meofT1ESU5p4d9ucXpXw9pxKCMEyi`
- PIRATE / COSANTA (BEP-20): `0x52be29951B0D10d5eFa48D58363a25fE5Cc097e9`
- Bitcoin: `bc1q2ph64sryt6skegze6726fp98u44kjsc5exktap`
- DASH: `Xv7U37XKp5d4fjvbeuganwhqXN7Sm4JJkt`
- Zcash (t-address): `t1bDVifcVuXWW8QctVdhuEghm8WqZjvU1US`
- Zcash (shielded): `zs1hwyqs4mfrynq0ysjmhv8wuau5zam0gwpx8ujfv8epgyufkmmsp6t7cfk9y0th7qyx7fsc5azm08`
- Monero: `4AzdEoZxeGMFkdtAxaNLAZakqEVsWpVb2at4u6966WGDiXkS7ZPyi7haeThTGUAWXVKDTmQ9DYTWRHMjGVSBW82xRQqPxkg`

Источник: [pirate.cash/en/donate](https://pirate.cash/en/donate/)

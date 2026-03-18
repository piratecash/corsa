# CORSA

## English

**CORSA** is a decentralized messenger and DEX-oriented communication project that is currently being rebuilt around the **Gazeta** protocol.

Current repository status:

- current client version: `0.1 alpha`
- Go-based desktop and node foundation
- desktop chat-style UI with identity list and direct messaging
- mesh-style peer sync between local nodes
- node role model: `full` or `client`
- identity based on public-key fingerprint addresses
- signed and encrypted direct addressed messages over the mesh
- signed box-key discovery with local TOFU trust pinning
- `Gazeta` anonymous encrypted notices with TTL

Main docs:

- architecture: [docs/architecture.md](docs/architecture.md)
- protocol: [docs/protocol.md](docs/protocol.md)
- encryption: [docs/encryption.md](docs/encryption.md)

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
- `CORSA_ADVERTISE_ADDRESS` — external address announced to peers
- `CORSA_BOOTSTRAP_PEERS` — comma-separated seed list
- `CORSA_TRUST_STORE_PATH` — local pinned-contact trust database
- `CORSA_NODE_TYPE` — `full` or `client`

Node roles:

- `full` — forwards mesh traffic, relays direct messages and `Gazeta` notices
- `client` — syncs peers and contacts, stores local traffic, but does not forward mesh traffic
- default for `corsa-node`: `full`
- default for `corsa-desktop`: `full`
- recommended future default for mobile/light client: `client`

Example:

```bash
CORSA_LISTEN_ADDRESS=:64646 \
CORSA_ADVERTISE_ADDRESS=203.0.113.10:64646 \
CORSA_BOOTSTRAP_PEERS=198.51.100.20:64646,198.51.100.21:64646 \
GOCACHE=$(pwd)/.gocache \
GOMODCACHE=$(pwd)/.gomodcache \
go run ./cmd/corsa-node
```

Docker example:

```bash
docker build -t corsa-node .
docker run --rm -p 64646:64646 \
  -e CORSA_LISTEN_ADDRESS=:64646 \
  -e CORSA_ADVERTISE_ADDRESS=203.0.113.10:64646 \
  -e CORSA_BOOTSTRAP_PEERS=198.51.100.20:64646,198.51.100.21:64646 \
  -v $(pwd)/.corsa:/app/.corsa \
  corsa-node
```

---

## Русский

**CORSA** — это децентрализованный мессенджер и коммуникационный слой для DEX, который сейчас пересобирается вокруг протокола **Gazeta**.

Текущее состояние репозитория:

- текущая версия клиента: `0.1 alpha`
- Go-основа для desktop-приложения и ноды
- desktop UI в формате чата: список identity слева и direct messaging справа
- mesh-синхронизация между локальными узлами
- модель ролей узла: `full` или `client`
- identity через адрес как fingerprint публичного ключа
- подписанные и зашифрованные адресные direct-сообщения поверх mesh
- подписанное распространение box key и локальный TOFU trust pinning
- анонимные зашифрованные объявления `Gazeta` с временем жизни

Основные документы:

- архитектура: [docs/architecture.md](docs/architecture.md)
- протокол: [docs/protocol.md](docs/protocol.md)
- шифрование: [docs/encryption.md](docs/encryption.md)

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
- `CORSA_ADVERTISE_ADDRESS` — внешний адрес, который нода объявляет пирам
- `CORSA_BOOTSTRAP_PEERS` — список seed-нод через запятую
- `CORSA_TRUST_STORE_PATH` — локальная база pinned-контактов
- `CORSA_NODE_TYPE` — `full` или `client`

Роли узла:

- `full` — форвардит mesh-трафик, ретранслирует direct messages и `Gazeta` notices
- `client` — синкает peers и contacts, хранит локальный трафик, но не делает mesh-forwarding
- значение по умолчанию для `corsa-node`: `full`
- значение по умолчанию для `corsa-desktop`: `full`
- рекомендуемое будущее значение для mobile/light client: `client`

Bootstrap seed по умолчанию:

- `65.108.204.190:64646`

Пример:

```bash
CORSA_LISTEN_ADDRESS=:64646 \
CORSA_ADVERTISE_ADDRESS=203.0.113.10:64646 \
CORSA_BOOTSTRAP_PEERS=198.51.100.20:64646,198.51.100.21:64646 \
GOCACHE=$(pwd)/.gocache \
GOMODCACHE=$(pwd)/.gomodcache \
go run ./cmd/corsa-node
```

Пример через Docker:

```bash
docker build -t corsa-node .
docker run --rm -p 64646:64646 \
  -e CORSA_LISTEN_ADDRESS=:64646 \
  -e CORSA_ADVERTISE_ADDRESS=203.0.113.10:64646 \
  -e CORSA_BOOTSTRAP_PEERS=198.51.100.20:64646,198.51.100.21:64646 \
  -v $(pwd)/.corsa:/app/.corsa \
  corsa-node
```

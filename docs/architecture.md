# CORSA Architecture

## English

### Overview

The repository contains the current Go-based CORSA stack:

- `corsa-desktop`: desktop app with an embedded local node
- `corsa-node`: standalone node process
- shared core packages for identity, transport, protocol, encryption, and UI-facing services

### Goals

- keep protocol and node logic independent from any UI toolkit
- let every desktop instance behave like a peer in the mesh
- support future Android/mobile work without rewriting the core
- keep cryptography, trust, and relay logic inside reusable core packages
- support distinct node roles for full relay nodes and client-only nodes

### Layout

- `cmd/corsa-desktop`: desktop entrypoint
- `cmd/corsa-node`: standalone node entrypoint
- `internal/app/desktop`: desktop composition, runtime, and Gio window
- `internal/app/node`: standalone node composition
- `internal/core/config`: environment-driven config and default paths
- `internal/core/identity`: `ed25519` identity, `X25519` box keys, key binding signatures
- `internal/core/directmsg`: encrypted and signed direct-message envelopes
- `internal/core/gazeta`: encrypted anonymous notice transport
- `internal/core/chatlog`: append-only file-backed chat message persistence (see [chatlog.md](chatlog.md)). Owned by `DesktopClient` (service layer), not by `node.Service`.
- `internal/core/node`: mesh node, trust store, peer sync, relay (see [mesh.md](mesh.md) for the full mesh network documentation). Does not own message persistence — delegates to a registered `MessageStore` handler (see [chatlog.md](chatlog.md)).
- `internal/core/service`: desktop-facing application service layer (see [dm_router.md](dm_router.md) for the DMRouter service layer). `DesktopClient` owns `chatlog.Store` and implements `node.MessageStore`.
- `internal/core/protocol`: protocol models
- `internal/core/transport`: p2p transport abstractions
- `internal/platform/mobile`: future mobile bindings
- see [debug.md](debug.md) for log levels and protocol tracing

### Runtime model

Node roles:

- `full`: relays mesh traffic, forwards direct messages and `Gazeta` notices
- `client`: syncs peers and contacts, stores local traffic, but does not forward mesh traffic
- current defaults:
  - `corsa-node` => `full`
  - `corsa-desktop` => `full`
  - future mobile/light client => `client`

Desktop mode:

1. load or create identity
2. load or create trust store
3. start embedded local node
4. connect to bootstrap peers
5. sync peers and contacts
6. render the local chat UI over the local node state

Standalone node mode:

1. load identity
2. load trust store
3. start TCP listener
4. sync peers and contacts
5. store and relay messages / notices

### Trust model

Current trust and discovery flow:

- fingerprint address is derived from the `ed25519` public key
- `boxkey` is signed by the same identity key
- peers verify `address + pubkey + boxkey + boxsig`
- the first valid contact set is pinned locally (TOFU)
- conflicting key rotations are ignored and recorded as trust conflicts

### Recommended next steps

1. surface trust conflicts in the desktop UI
2. add signatures to `Gazeta` notices
3. move from line protocol to structured frames
4. ~~add persistent storage for message history~~ — done, see [chatlog.md](chatlog.md)
5. add mobile/light-client bindings over the same core

---

## Русский

### Обзор

Репозиторий сейчас содержит актуальный Go-стек CORSA:

- `corsa-desktop`: desktop-приложение со встроенной локальной нодой
- `corsa-node`: отдельный процесс ноды
- общие core-пакеты для identity, транспорта, протокола, шифрования и UI-сервисов

### Цели

- держать протокол и логику ноды независимыми от конкретного UI toolkit
- сделать так, чтобы каждый desktop-инстанс был полноценным peer в mesh
- оставить возможность для будущего Android/mobile клиента без переписывания core
- держать криптографию, trust и relay-логику в переиспользуемых пакетах
- поддерживать разные роли узла: полный relay-узел и client-only узел

### Структура

- `cmd/corsa-desktop`: точка входа desktop-приложения
- `cmd/corsa-node`: точка входа standalone-ноды
- `internal/app/desktop`: сборка desktop-приложения, runtime и Gio-окно
- `internal/app/node`: сборка standalone-ноды
- `internal/core/config`: конфиг из env и дефолтные пути
- `internal/core/identity`: identity на `ed25519`, `X25519` box keys, подписи привязки ключей
- `internal/core/directmsg`: зашифрованные и подписанные direct-message envelopes
- `internal/core/gazeta`: зашифрованный анонимный transport для notices
- `internal/core/chatlog`: append-only хранение истории сообщений на диске (см. [chatlog.md](chatlog.md)). Владеет `DesktopClient` (сервисный слой), а не `node.Service`.
- `internal/core/node`: mesh-нода, trust store, peer sync, relay (см. [mesh.md](mesh.md) для полной документации mesh-сети). Не владеет хранением сообщений — делегирует зарегистрированному обработчику `MessageStore` (см. [chatlog.md](chatlog.md)).
- `internal/core/service`: сервисный слой для desktop-клиента (см. [dm_router.md](dm_router.md) для сервисного слоя DMRouter). `DesktopClient` владеет `chatlog.Store` и реализует `node.MessageStore`.
- `internal/core/protocol`: модели протокола
- `internal/core/transport`: p2p-абстракции транспорта
- `internal/platform/mobile`: будущие mobile bindings
- см. [debug.md](debug.md) для уровней логирования и трассировки протокола

### Модель запуска

Роли узла:

- `full`: ретранслирует mesh-трафик, direct messages и `Gazeta` notices
- `client`: синкает peers и contacts, хранит локальный трафик, но не форвардит mesh-трафик
- текущие значения по умолчанию:
  - `corsa-node` => `full`
  - `corsa-desktop` => `full`
  - будущий mobile/light client => `client`

В desktop-режиме:

1. загружается или создается identity
2. загружается или создается trust store
3. запускается встроенная локальная нода
4. нода подключается к bootstrap peers
5. нода синкает peers и contacts
6. UI показывает чат поверх состояния локальной ноды

В режиме standalone-ноды:

1. загружается identity
2. загружается trust store
3. поднимается TCP listener
4. нода синкает peers и contacts
5. нода хранит и ретранслирует сообщения / notices

### Trust model

Текущая схема доверия и discovery:

- fingerprint-адрес получается из `ed25519` public key
- `boxkey` подписывается тем же identity key
- peer проверяет связку `address + pubkey + boxkey + boxsig`
- первый валидный набор ключей pin-ится локально по модели TOFU
- конфликтующие замены ключей игнорируются и записываются как trust conflicts

### Следующие шаги

1. показать trust conflicts в desktop UI
2. добавить подписи для `Gazeta` notices
3. перейти от line protocol к structured frames
4. ~~добавить персистентное хранение истории сообщений~~ — сделано, см. [chatlog.md](chatlog.md)
5. сделать mobile/light-client bindings поверх того же core

# Identity Commands

## English

All identity commands require no arguments.

### POST /rpc/v1/identity/identities

All known identities (ed25519 fingerprints observed on the network). The set
is bounded (most recent observations, capped at `maxKnownIdentities`); on a
busy relay the oldest idle entries are evicted, so a long-lived node returns a
recent window rather than every identity ever seen. Trusted contacts are not
sourced from this set — use `fetch_trusted_contacts` for the authoritative,
persistent contact list.

#### CLI

```bash
corsa-cli fetchIdentities
```

#### Console

```
fetchIdentities
```

### POST /rpc/v1/identity/contacts

All contacts (identities with associated metadata such as display name).

#### CLI

```bash
corsa-cli fetchContacts
```

#### Console

```
fetchContacts
```

### POST /rpc/v1/identity/trusted_contacts

Trusted contacts (TOFU pinned — identity verified on first contact and locked).

#### CLI

```bash
corsa-cli fetchTrustedContacts
```

#### Console

```
fetchTrustedContacts
```

---

## Русский

Все команды идентификации не требуют аргументов.

### POST /rpc/v1/identity/identities

Все известные идентификаторы (ed25519-отпечатки, обнаруженные в сети). Набор
ограничен (последние наблюдения, лимит `maxKnownIdentities`); на нагруженном
relay самые старые неактивные записи вытесняются, поэтому долгоживущий узел
возвращает недавнее окно, а не все когда-либо виденные идентичности. Доверенные
контакты берутся не отсюда — авторитетный персистентный список даёт
`fetch_trusted_contacts`.

#### CLI

```bash
corsa-cli fetchIdentities
```

#### Консоль

```
fetchIdentities
```

### POST /rpc/v1/identity/contacts

Все контакты (идентификаторы с ассоциированными метаданными, например отображаемое имя).

#### CLI

```bash
corsa-cli fetchContacts
```

#### Консоль

```
fetchContacts
```

### POST /rpc/v1/identity/trusted_contacts

Доверенные контакты (закреплённые по TOFU — идентификатор подтверждён при первом контакте и зафиксирован).

#### CLI

```bash
corsa-cli fetchTrustedContacts
```

#### Консоль

```
fetchTrustedContacts
```

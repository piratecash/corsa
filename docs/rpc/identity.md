# Identity Commands

## English

All identity commands require no arguments.

### POST /rpc/v1/identity/identities

All known identities (ed25519 fingerprints observed on the network).

#### CLI

```bash
corsa-cli fetch_identities
```

#### Console

```
fetch_identities
```

### POST /rpc/v1/identity/contacts

All contacts (identities with associated metadata such as display name).

#### CLI

```bash
corsa-cli fetch_contacts
```

#### Console

```
fetch_contacts
```

### POST /rpc/v1/identity/trusted_contacts

Trusted contacts (TOFU pinned — identity verified on first contact and locked).

#### CLI

```bash
corsa-cli fetch_trusted_contacts
```

#### Console

```
fetch_trusted_contacts
```

---

## Русский

Все команды идентификации не требуют аргументов.

### POST /rpc/v1/identity/identities

Все известные идентификаторы (ed25519-отпечатки, обнаруженные в сети).

#### CLI

```bash
corsa-cli fetch_identities
```

#### Консоль

```
fetch_identities
```

### POST /rpc/v1/identity/contacts

Все контакты (идентификаторы с ассоциированными метаданными, например отображаемое имя).

#### CLI

```bash
corsa-cli fetch_contacts
```

#### Консоль

```
fetch_contacts
```

### POST /rpc/v1/identity/trusted_contacts

Доверенные контакты (закреплённые по TOFU — идентификатор подтверждён при первом контакте и зафиксирован).

#### CLI

```bash
corsa-cli fetch_trusted_contacts
```

#### Консоль

```
fetch_trusted_contacts
```

# Identity Commands

## English

All identity commands require no arguments.

### POST /rpc/v1/identity/identities

All known identities (ed25519 fingerprints observed on the network).

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

Все известные идентификаторы (ed25519-отпечатки, обнаруженные в сети).

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

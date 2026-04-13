# Notice Commands

## English

### POST /rpc/v1/notice/list

Fetch anonymous encrypted notices (Gazeta subsystem). No arguments.

Notices are broadcast messages encrypted with the network-wide Gazeta key. They are anonymous — the sender identity is not disclosed.

#### CLI

```bash
corsa-cli fetchNotices
```

#### Console

```
fetchNotices
```

---

## Русский

### POST /rpc/v1/notice/list

Получение анонимных зашифрованных уведомлений (подсистема Gazeta). Без аргументов.

Уведомления — это broadcast-сообщения, зашифрованные общесетевым ключом Gazeta. Они анонимны — идентификатор отправителя не раскрывается.

#### CLI

```bash
corsa-cli fetchNotices
```

#### Консоль

```
fetchNotices
```

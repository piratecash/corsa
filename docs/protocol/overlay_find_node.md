# FIND_NODE: inactive codec candidate

## English

The frame codec, answer transcript/verification and inactive local state blocks, including a consumed-only
target cache/signing coordinator, are implemented. See [local state boundaries](../design/inactive-find-node-state.md).
These blocks are not connected to production. Production
dispatchers do not accept these frames, no initiator is installed, and neither overlay capability is
advertised. This is a partial implementation toward inactive PR-4, not completed PR-4/5, an approved
G2 contract or permission to activate D1. See the [design decision](../design/overlay-design-decision.md),
[admission proposal](../design/d1-admission-budget.md) and [release gates](../release.md).

### Frame encoding

Frames are UTF-8 JSON objects, at most 1024 bytes. Every listed field is required and non-null;
unknown fields, duplicate keys (including escaped aliases), wrong types, trailing data and unsupported
versions are refused. Field names are case sensitive. Marshaling emits exactly this order:

| Type | Fields |
| --- | --- |
| `find_node` | `type`, `v`, `id`, `target`, `challenge` |
| `find_node_answer` | `type`, `v`, `id`, `target`, `pubkey`, `sig` |

`v` is 1. `id` is an unsigned 64-bit integer local to a session and direction, including 0; it must
be present. `target` is a nonzero 20-byte identity encoded as 40 lowercase hexadecimal characters.
`challenge` is exactly 32 public bytes in canonical unpadded URL base64 (43 characters). `pubkey`
is exactly 32 bytes in canonical standard padded base64 (44 characters). `sig` is exactly 64 bytes
in canonical unpadded URL base64 (86 characters). There is no `issued_at`, address, record or box key.
The target key must pass `identity.ParsePublicKey` and its fingerprint must equal `target`.

The codec is stateless. It cannot establish fresh challenge generation, session rights, rate limits,
deduplication, source-group fairness or deadlines. Before handling a future frame, **both** network
dispatchers must also require an unambiguous named classifier result matching its parsed type,
a valid proven v2 session, negotiated rights and resource admission. That integration is pending.
In particular, ordinary JSON decoding alone does not grant overlay rights to legacy sessions.

### Answer transcript and verification

The signed transcript is:

```text
"corsa-find-node-answer-v1" || 0x00 || uint16be(len(network)) || network
  || target[20] || challenge[32]
```

The network is explicit, nonempty UTF-8 without a byte order mark and at most 65535 bytes. The codec
accepts no private key and exposes no signing operation. Verification uses the retained request's
challenge and target, checks its request ID, validates the target key and calls its checked
`PublicKey.Verify`. The caller must additionally correlate the original session; this pure function
does not know the connection. Rejected forwarded target keys are not evidence against the neighbour.

The inactive target coordinator provides immutable network/target/challenge inputs only after consuming
actual local budgets. External signing uses `identity.SignPayload`, whose RawURL Base64 result is
decoded to exactly 64 signature bytes. The coordinator accepts the future local signer's result; it
does not itself cryptographically verify arbitrary completion bytes. The cached signed body is identical
across up to three recipients, while each reply uses its own session's request ID. A local reservation,
consumed permit or composed diagnostic sample is not AdmittedAtT or a network answer acknowledgement.

The test vector uses network `gazeta-devnet`, ID 1, target
`ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee` and challenge bytes `30` through `4f` (hex). The transcript
is 93 bytes; its SHA-256 is `5cb7e64704431c96f0eb7883b6b6909a26bf115f2b02f48878c6e8852c6c0dfe`.
Exact request/answer bytes and the published target signature are pinned by
`internal/core/protocol/find_node_test.go`. Negative cases include changed network, ID, target and
challenge, small-order keys, null/overflow fields, unknown fields and noncanonical base64.

## Русский

Реализованы кодек кадров, транскрипт/проверка ответа и неактивные локальные блоки состояния, включая
target cache/signing coordinator с завершением только после Consume. См.
[границы состояния](../design/inactive-find-node-state.md). К production они не подключены. Production-диспетчеры
эти кадры не принимают, инициатор не подключён, обе overlay capability не объявляются. Это часть
неактивного PR-4, а не завершённые PR-4/5, утверждённый G2 или разрешение активации D1. См.
[решение D](../design/overlay-design-decision.md), [проект допуска](../design/d1-admission-budget.md)
и [гейты выпуска](../release.md).

### Кодирование кадров

Кадр — JSON-объект UTF-8 не больше 1024 байт. Все перечисленные поля обязательны и не могут быть
`null`. Неизвестные поля, дубли ключей (включая escaped-алиасы), неверные типы, данные после объекта
и неподдерживаемые версии отвергаются. Регистр имён значим. Marshal выдаёт ровно такой порядок:

| Тип | Поля |
| --- | --- |
| `find_node` | `type`, `v`, `id`, `target`, `challenge` |
| `find_node_answer` | `type`, `v`, `id`, `target`, `pubkey`, `sig` |

`v` равен 1. `id` — обязательное беззнаковое 64-битное число, локальное сессии и направлению;
значение 0 допустимо. `target` — ненулевая identity из 20 байт, 40 строчных hex-символов. `challenge`
содержит ровно 32 публичных байта в каноническом URL base64 без padding (43 символа). `pubkey` —
32 байта в каноническом стандартном base64 с padding (44 символа). `sig` — 64 байта в каноническом
URL base64 без padding (86 символов). Полей `issued_at`, адреса, записи и box-ключа нет. Ключ цели
проходит `identity.ParsePublicKey`, его fingerprint совпадает с `target`.

Кодек не хранит состояния: свежесть вызова, права сессии, лимиты, дедуп, справедливость по группам
и сроки им не обеспечиваются. Перед будущей обработкой **оба** сетевых диспетчера также обязаны
проверить однозначный named-результат классификатора с тем же типом, доказанную v2-сессию,
согласованные права и ресурсный допуск. Эта интеграция ещё не выполнена. Обычный JSON-разбор
не даёт overlay-права legacy-сессии.

### Транскрипт ответа и проверка

Подписываются байты:

```text
"corsa-find-node-answer-v1" || 0x00 || uint16be(len(network)) || network
  || target[20] || challenge[32]
```

Сеть передаётся явно: непустой UTF-8 без BOM, не больше 65535 байт. Кодек не принимает приватный ключ
и не предоставляет операцию подписи. Проверка берёт challenge и target из сохранённого запроса,
сопоставляет ID, проверяет ключ цели и вызывает проверенный `PublicKey.Verify`. Вызывающий отдельно
сопоставляет исходную сессию: чистая функция соединения не знает. Неверный пересланный ключ цели
не является доказательством нарушения соседа.

Неактивный target coordinator выдаёт неизменяемые network/target/challenge только после списания
фактических локальных бюджетов. Внешняя подпись использует `identity.SignPayload`: RawURL Base64
декодируется в ровно 64 байта. Coordinator принимает результат будущего локального signer, но не
проверяет криптографически произвольные completion bytes. Подписанное cached body одинаково у максимум
трёх получателей, envelope ID каждого ответа соответствует его сессии. Локальный reservation,
consumed permit или diagnostic snapshot не являются AdmittedAtT либо сетевым подтверждением ответа.

Вектор: сеть `gazeta-devnet`, ID 1, цель `ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee`, challenge —
байты от `30` до `4f` включительно (hex). Транскрипт имеет 93 байта; SHA-256 равен
`5cb7e64704431c96f0eb7883b6b6909a26bf115f2b02f48878c6e8852c6c0dfe`. Точные JSON и опубликованная
подпись цели закреплены в `internal/core/protocol/find_node_test.go`. Негативные случаи проверяют
смену сети, ID, цели и вызова, ключи малого порядка, null/overflow, неизвестные поля и неканонический
base64.

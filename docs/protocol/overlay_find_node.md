# FIND_NODE: wire candidate

## English

The codec, answer verification and bounded target and relay state are implemented. Closed diagnostic
and test construction can exercise both peer read paths and the original transport writers over
TLSv2. Standard nodes keep FIND_NODE inactive and advertise neither overlay capability. The
initiator remains part of the test stand. G2 and public activation remain open.
See the [local state boundaries](../design/inactive-find-node-state.md),
[design decision](../design/overlay-design-decision.md) and [release gates](../release.md).

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

The codec is stateless. Challenge generation, session rights, rate limits, deduplication, fairness
and deadlines belong to the calling path. The closed path in both peer readers uses the named
frame classifier, original command limits and original proven TLSv2 session with negotiated
rights. Request admission uses a complete eligible membership capture before retaining work.
A forwarded request keeps a bounded pending entry tied to its original source and target sessions.
A matching answer restores the original request ID and consumes that return path once.

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

The closed target path supplies immutable network/target/challenge inputs after consuming
local budgets. The local signer uses `identity.SignPayload`, whose RawURL Base64 result is
decoded to exactly 64 signature bytes. The coordinator retains the original signer's completion.
The initiator verifies the returned body against its retained request and session. The cached signed body is identical
across up to three recipients, while each reply uses its own session's request ID. A local reservation,
consumed permit or composed diagnostic sample is not AdmittedAtT or a network answer acknowledgement.

The test vector uses network `gazeta-devnet`, ID 1, target
`ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee` and challenge bytes `30` through `4f` (hex). The transcript
is 93 bytes; its SHA-256 is `5cb7e64704431c96f0eb7883b6b6909a26bf115f2b02f48878c6e8852c6c0dfe`.
Exact request/answer bytes and the published target signature are pinned by
`internal/core/protocol/find_node_test.go`. Negative cases include changed network, ID, target and
challenge, small-order keys, null/overflow fields, unknown fields and noncanonical base64.

## Русский

Реализованы кодек, проверка ответа и ограниченное состояние цели и ретранслятора.
Закрытая диагностическая и тестовая сборки могут проверять оба пути чтения и штатные writers
поверх TLSv2. Штатные узлы оставляют FIND_NODE неактивным и не объявляют обе overlay capability.
Инициатор остаётся частью стенда. G2 и публичная активация открыты.
См. [границы состояния](../design/inactive-find-node-state.md),
[решение D](../design/overlay-design-decision.md) и [порядок выпуска](../release.md).

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

Кодек не хранит состояния. Свежесть вызова, права сессии, лимиты, дедуп, справедливость по группам и
сроки относятся к вызывающему пути. Закрытый путь в обоих peer readers использует классификатор
именованных кадров, штатные лимиты команд и исходную доказанную TLSv2-сессию с согласованными
правами. До приёма работы нужен полный снимок подходящих сессий. Для пересланного запроса
ограниченная pending-запись сохраняет исходные сессии источника и цели. Совпавший ответ
восстанавливает исходный ID запроса и использует обратный путь один раз.

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

Закрытый путь цели передаёт неизменяемые network/target/challenge после списания
локальных бюджетов. Локальная подпись использует `identity.SignPayload`: RawURL Base64
декодируется в ровно 64 байта. Coordinator сохраняет завершение исходной подписи.
Инициатор проверяет тело по сохранённому запросу и исходной сессии. Подписанное cached body одинаково у максимум
трёх получателей, envelope ID каждого ответа соответствует его сессии. Локальный reservation,
consumed permit или diagnostic snapshot не являются AdmittedAtT либо сетевым подтверждением ответа.

Вектор: сеть `gazeta-devnet`, ID 1, цель `ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee`, challenge —
байты от `30` до `4f` включительно (hex). Транскрипт имеет 93 байта; SHA-256 равен
`5cb7e64704431c96f0eb7883b6b6909a26bf115f2b02f48878c6e8852c6c0dfe`. Точные JSON и опубликованная
подпись цели закреплены в `internal/core/protocol/find_node_test.go`. Негативные случаи проверяют
смену сети, ID, цели и вызова, ключи малого порядка, null/overflow, неизвестные поля и неканонический
base64.

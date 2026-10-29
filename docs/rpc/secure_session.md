# Secure Session Commands

## English

The operator's actions on the **endpoint bindings** of the v2 downgrade protection ([protocol/session_v2.md](../protocol/session_v2.md), "Operator actions on endpoint bindings"). A binding says "this endpoint, dialled by this node, answered over v2 — a v1 answer from it is a downgrade". Ordinary (general) bindings last 30 days from the last v2 session; protected ones never expire and are never lowered automatically.

All three commands are **`TransportLoopbackOnly`** ([rpc.md](../rpc.md), "Transport policy"): an HTTP caller must reach the listener over a loopback socket **and** the listener must have credentials configured (`CORSA_RPC_USERNAME` / `CORSA_RPC_PASSWORD`); the desktop console calls them in-process. A remote socket or an unauthenticated listener gets `403` before the node is asked anything — the policy is declared when the command is registered, also on a node that does not serve the commands (there they answer `503` to an allowed caller).

There is **no bulk removal** — neither of all bindings nor of all bindings of an identity. Every endpoint is named.

Errors are mapped by type, never by text: a refusal of what was named → `400`; an unavailable store or mutation → `503`. **Endpoint mutation activation is not authorised** under the [2026-10-29 owner decision](../design/endpoint-binding-write-outcomes-decision.md); removal and confirmed demotion must refuse at their domain entrypoints until the required checks and separate activation decision. Read-only list/preview are distinct. The approved write contract distinguishes not-applied, indeterminate, applied-with-cleanup-error and committed; a generic write error no longer promises unchanged disk. Structured outcome/client handling remains under implementation and verification.

| Refusal | Meaning |
|---|---|
| `endpoint_binding_not_found` | no live binding is held under the address |
| `endpoint_binding_protected` | `removeEndpointBinding` named a protected binding; demote it instead |
| `endpoint_binding_not_protected` | `demoteEndpointBindings` named an ordinary binding |
| `endpoint_binding_selection_empty` / `_too_large` / `_duplicate` | no endpoint, more than 256, or two addresses of one binding |
| `endpoint_binding_selection_changed` | the `confirm` token is not the dry run's token of this selection as it is now |
| `endpoint_binding_page_limit` | `limit` outside 1..1000 |

An endpoint argument is `host:port` in any spelling of the endpoint (letter case, IPv6 compression, an IPv4-mapped address are one binding), or the exact stored address of a record kept under an address with no canonical form. At most 512 bytes.

### listEndpointBindings

The live bindings, a page at a time, in key order.

| Argument | Type | |
|---|---|---|
| `identity` | 40-char hex, optional | bindings whose last proven identity, **or** whose `protected_by`, is this identity |
| `endpoint` | host:port, optional | the one binding this address names |
| `after` | host:port, optional | cursor: the page starts after this key (`next_after` of the previous page) |
| `limit` | 1..1000, optional | page size, default 200 |

```json
{
  "bindings": [
    {
      "endpoint": "192.0.2.1:64646",
      "key_form": "canonical",
      "transport": "tcp",
      "source_group": "192.0.0.0/16",
      "tier": "protected",
      "pool": "K",
      "protected_by": "1122000000000000000000000000000000000000",
      "identity": "3344000000000000000000000000000000000000",
      "last_v2": "2026-05-28T21:46:40Z",
      "first_at": "2026-05-28T21:46:40Z"
    },
    {
      "endpoint": "192.0.2.2:64646",
      "key_form": "canonical",
      "transport": "tcp",
      "source_group": "192.0.0.0/16",
      "tier": "general",
      "identity": "3344000000000000000000000000000000000000",
      "last_v2": "2026-05-28T21:46:40Z",
      "expires_at": "2026-06-27T21:46:40Z",
      "first_at": "2026-05-28T21:46:40Z",
      "demoted_at": "2026-05-28T21:46:40Z"
    }
  ],
  "total": 2
}
```

| Field | Type | |
|---|---|---|
| `endpoint` | string | the key the binding is held under |
| `key_form` | `canonical` \| `non_canonical` | `non_canonical`: a record an earlier build stored under an address without a canonical form; only that exact address names it |
| `transport` | `tcp` \| `tor` \| `i2p`, absent | absent for a non-canonical record |
| `source_group` | string | the group the binding was created in |
| `tier` | `protected` \| `general` | |
| `pool` | `K` \| `R`, protected only | the pool the protection counts against |
| `protected_by` | identity, protected only | whose standing gave the protection |
| `identity` | identity | the last identity proven on the endpoint over v2 — what was seen there, not who is expected there |
| `last_v2` | RFC 3339 | the last v2 session (not rewritten on renewal of a protected binding) |
| `expires_at` | RFC 3339, general only | 30 days after the later of `last_v2` and `demoted_at` |
| `first_at` | RFC 3339, absent | creation; absent for a binding written before creation times were kept |
| `derived_from` | host:port, absent | the name a derived key was reached by |
| `demoted_at` | RFC 3339, absent | when an operator demoted the binding |

The times of a binding — `last_v2`, `expires_at`, `first_at`, `demoted_at`, and the `expires_at` of a demotion — are on the store's **trusted time**, not the wall clock as it reads ([session_v2.md](../protocol/session_v2.md), "The clock of the terms"). While a wall clock jump is in force (`fetchRouteSummary.secure_sessions.clock_jump_active`) they lag the wall clock by the jump: an `expires_at` that looks past is still in force. A renewal never moves `last_v2` back, so while the wall clock is set back `last_v2` may lie ahead of it.
| `total` | int | live bindings the filters keep, over every page |
| `next_after` | host:port, absent | cursor of the next page; absent on the last one |

This is the one answer that pairs endpoints with identities, and it shows only the current state: no earlier owner of an endpoint appears in any answer — those stay in the local journal.

### removeEndpointBinding

Removes one **ordinary** binding. A protected binding is refused (`endpoint_binding_protected`): a protection ends only by a confirmed demotion.

| Argument | Type |
|---|---|
| `endpoint` | host:port, required — exactly one |

```json
{"operation_id": "5f0c…", "endpoint": "192.0.2.2:64646", "key_form": "canonical"}
```

The removal is written to disk before it leaves memory and journalled as `endpoint_binding_removed_by_operator` with the endpoint, the identity and `operation_id`. After it, v1 is admitted on the endpoint again for an identity that is not pinned.

### demoteEndpointBindings

Moves **selected** protected bindings to general for 30 days. Two steps:

1. **Dry run** — without `confirm`. Changes nothing. Shows each endpoint, the term it would get and the consequence, and returns `confirm_token`.
2. **Confirmation** — the same `endpoints` and `confirm` = that token. The token is a digest of exactly that selection with the protection each binding holds and the identity last proven on it: another selection, or the same one after a protection or an owner changed (which would make `identity_pin_remains` stale), is refused with `endpoint_binding_selection_changed`.

**What the token is not.** `confirm_token` is deterministic — a plain SHA-256 over the selection and its state, with no secret and no expiry — so anyone who knows the bindings can compute it. It guards against the operator's mistake of confirming a set other than the one shown, not against an adversary: the right to confirm comes only from the authenticated loopback RPC.

| Argument | Type | |
|---|---|---|
| `endpoints` | array of host:port, or one comma-separated string | required, 1..256, each endpoint once |
| `confirm` | string, optional | the dry run's `confirm_token` |

Dry run:

```json
{
  "confirmed": false,
  "endpoints": [
    {"endpoint": "192.0.2.1:64646", "key_form": "canonical", "pool": "K", "expires_at": "2026-06-27T21:46:40Z", "identity_pin_remains": true}
  ],
  "consequence": "each endpoint becomes an ordinary binding: after 30 days without a v2 session on it, it accepts v1 again from an identity that is not pinned",
  "confirm_token": "9d4e…"
}
```

Confirmation: the same shape with `"confirmed": true`, the term counted from the confirmation, and `operation_id` instead of `confirm_token`.

| Field | |
|---|---|
| `pool` | the pool the protection was counted against |
| `expires_at` | the term: 30 days after the demotion, extended by every later v2 session |
| `identity_pin_remains` | the last identity proven on the endpoint is pinned: a v1 session naming **that** identity stays refused on any address. It does not keep another identity off the endpoint |

The target write uses one complete store snapshot. Its result follows the [four approved outcomes](../design/endpoint-binding-write-outcomes-decision.md): a post-publication error can leave a changed valid file, and failed restoration does not establish rollback. A stale selection never authorises applying an edit; exhaustive competing-mutator coverage is required before enabling mutations. Audit and typed clients must distinguish the four outcomes—not-applied, indeterminate, applied-with-cleanup-error and committed—and a separate restoration result, with operation ID and exact endpoints; do not log every error as an unchanged action. The command examples below describe the interface, not permission to activate it.

#### CLI / console

```
listEndpointBindings identity=<id> limit=50
removeEndpointBinding 192.0.2.2:64646
demoteEndpointBindings 192.0.2.1:64646 192.0.2.3:64646
demoteEndpointBindings endpoints=192.0.2.1:64646,192.0.2.3:64646 confirm=<token>
```

### listEndpointDialDelays

The endpoints this node delays its dials to: an endpoint whose **correct** answer — to the identity-less probe, or a v1 welcome — claimed a protocol version below this node's minimum is held on its own, 30 s doubling per further such answer up to 6 h, so that a node that really is old is not redialled every few seconds ([session_v2.md](../protocol/session_v2.md)). No argument.

```json
[{"endpoint": "192.0.2.5:64646", "reason": "claimed_old_version", "claimed_version": 27, "steps": 3, "next_attempt_at": "2026-10-16T12:02:00Z"}]
```

| Field | |
|---|---|
| `reason` | `claimed_old_version` — the only reason today |
| `claimed_version` | the version the latest answer announced; a claim, not a proof |
| `steps` | how often the delay has grown since it last started over; the next one doubles on it |
| `next_attempt_at` | when the next dial may go; past once the wait ran out, while the count still holds |

The delay is in memory only. Only a successful outbound connection to the same endpoint — v2 proven, or v1 after `auth_ok` with the pin respected — resets it; `retryEndpointDial` ends the current wait and keeps the count; an inbound session of the same identity changes nothing.

### retryEndpointDial

Ends the current old-version wait of **one** endpoint after its peer was upgraded. It is one more attempt, not a reset: `steps` stays, so a peer that still claims an old version is delayed longer, not from 30 s again. The command dials nothing — the node's next dial there may go at once. It lifts nothing else: not the v2 retry backoff (`dials_held_by_v2_retry_backoff`), not the connection manager's cooldown, no pin, no endpoint binding, no other address's delay — a v1 answer from a bound endpoint stays a refused downgrade, and a pinned identity stays refused over v1. An endpoint without a delay is refused with a validation error.

| Argument | Type |
|---|---|
| `endpoint` | host:port, required — exactly one |

```json
{"endpoint": "192.0.2.5:64646", "retried": true}
```

#### CLI / console

```
listEndpointDialDelays
retryEndpointDial 192.0.2.5:64646
```

### Identity pin preview and confirmation

`previewIdentityPinRemoval identity=<40 lowercase hex>` shows one existing identity pin: `identity`, original `pinned_at`, `consequence`, `consequence_version`, opaque `confirm_token` and `removal_available`. It changes no protection. Show the exact identity and consequence before a separate explicit confirmation. `confirmIdentityPinRemoval identity=<same identity> confirm=<token>` is a distinct operation. Both commands are `TransportLoopbackOnly`: real loopback socket and configured authentication are required. Their UI counterparts `previewIdentityPinRemovalUI` / `confirmIdentityPinRemovalUI` are `TransportInProcessOnly`, including when HTTP is authenticated on loopback. The SDK's typed methods use these in-process entrypoints.

**Removal is currently unavailable:** preview reports `removal_available=false`; a structurally valid confirm returns `503 identity_pin_removal_unavailable`. Verified durable storage, an external restart fence and the recovery workflow must be implemented and admitted before enabling it. The normal node continues operating. No identity pin, endpoint binding or other v2 requirement changes through these preparatory commands.

A token is bound to the running store, exact identity, latest accepted proof revision and warning version. Another identity's proof leaves it valid; a new proof of this identity, remove/re-pin or restart invalidates it. Tokens are credentials: diagnostic formatting redacts them, and they belong only in the preview response and explicit confirmation field. No bulk selection is accepted. Missing/zero identity, an absent pin or a malformed confirmation is `400`; an unavailable or unreadable store is `503`.


### Current local persistence implementation

The internal C3 transaction and restoration-only `Service.RecoverIdentityPinProtection` port implement strict local save outcomes, guard recovery and a fatal storage latch. They grant no admission: public confirm stays unavailable, and a test fence cannot enable it. The concrete verified-session adapter and offline CLI are an explicit [review candidate](../operations/identity-pin-recovery-candidate.md). Enabling still requires independently provisioned external storage and an admitted supervisor/operator-finalize workflow. Consequence version 2 explicitly binds the inactive candidate stop/one-operation/finalize requirement. A restored receipt alone never authorises network restart. The [dedicated desktop dialog](../operations/identity-pin-desktop.md) presents an exact-identity preview and explicit acknowledgement; production confirmation remains unavailable.

---

## Русский

Действия оператора над **привязками адресов** защиты v2 от понижения ([protocol/session_v2.md](../protocol/session_v2.md), «Действия оператора над привязками адресов»). Привязка говорит: «этот адрес, набранный этим узлом, ответил по v2 — ответ v1 с него есть понижение». Обычные (general) привязки живут 30 суток с последней сессии v2; защищённые не истекают и автоматически не понижаются.

Все три команды — **`TransportLoopbackOnly`** ([rpc.md](../rpc.md), «Транспортная политика»): HTTP-вызов должен прийти на слушатель через loopback-сокет, **и** у слушателя должны быть настроены учётные данные (`CORSA_RPC_USERNAME` / `CORSA_RPC_PASSWORD`); консоль desktop вызывает их внутри процесса. Удалённый сокет или слушатель без аутентификации получают `403` раньше, чем узел о чём-то спрошен, — политика объявлена при регистрации команды, в том числе на узле, который эти команды не обслуживает (там допущенный вызов получает `503`).

**Массового снятия нет** — ни всех привязок, ни всех привязок одной identity. Каждый адрес называется явно.

Ошибки сопоставляются по типу, а не по тексту: отказ по названному → `400`; недоступное хранилище или изменение → `503`. **Активация изменения endpoint-привязок не разрешена** [решением владельца от 2026-10-29](../design/endpoint-binding-write-outcomes-decision.ru.md): снятие и подтверждённое понижение должны отказывать в доменных точках входа до необходимых проверок и отдельного решения о включении. Чтение списка/preview выделены отдельно. Утверждённый контракт различает «не применено», «результат неопределён», «применено, ошибка очистки» и подтверждённый commit; общая ошибка записи больше не обещает неизменённый диск. Структурированные исходы и их обработка клиентами ещё реализуются и проверяются.

| Отказ | Значение |
|---|---|
| `endpoint_binding_not_found` | под этим адресом нет действующей привязки |
| `endpoint_binding_protected` | `removeEndpointBinding` назвал защищённую привязку; её понижают, а не снимают |
| `endpoint_binding_not_protected` | `demoteEndpointBindings` назвал обычную привязку |
| `endpoint_binding_selection_empty` / `_too_large` / `_duplicate` | ни одного адреса, больше 256 или два адреса одной привязки |
| `endpoint_binding_selection_changed` | токен `confirm` — не токен пробного прогона этого набора в его нынешнем состоянии |
| `endpoint_binding_page_limit` | `limit` вне 1..1000 |

Аргумент-адрес — `host:port` в любом написании адреса (регистр, сжатие IPv6, IPv4-mapped — одна привязка) или точный сохранённый адрес записи, хранящейся под адресом без канонической формы. Не длиннее 512 байт.

### listEndpointBindings

Действующие привязки постранично, в порядке ключей.

| Аргумент | Тип | |
|---|---|---|
| `identity` | 40 hex-символов, необязательный | привязки, у которых последняя доказанная identity **или** `protected_by` — эта identity |
| `endpoint` | host:port, необязательный | одна привязка, которую называет этот адрес |
| `after` | host:port, необязательный | курсор: страница начинается после этого ключа (`next_after` предыдущей страницы) |
| `limit` | 1..1000, необязательный | размер страницы, по умолчанию 200 |

Ответ — как в английской секции.

| Поле | Тип | |
|---|---|---|
| `endpoint` | строка | ключ, под которым хранится привязка |
| `key_form` | `canonical` \| `non_canonical` | `non_canonical`: запись, которую прежняя сборка сохранила под адресом без канонической формы; её называет только этот точный адрес |
| `transport` | `tcp` \| `tor` \| `i2p`, может отсутствовать | отсутствует у неканонической записи |
| `source_group` | строка | группа, в которой привязка создана |
| `tier` | `protected` \| `general` | |
| `pool` | `K` \| `R`, только у защищённой | пул, в котором учтена защита |
| `protected_by` | identity, только у защищённой | чьё положение дало защиту |
| `identity` | identity | последняя identity, доказанная на адресе по v2, — что там видели, а не кого там ждут |
| `last_v2` | RFC 3339 | последняя сессия v2 (у защищённой привязки при продлении не перезаписывается) |
| `expires_at` | RFC 3339, только у обычной | 30 суток после более позднего из `last_v2` и `demoted_at` |
| `first_at` | RFC 3339, может отсутствовать | создание; отсутствует у привязки, записанной до того, как время создания стало храниться |
| `derived_from` | host:port, может отсутствовать | имя, через которое достигнут производный ключ |
| `demoted_at` | RFC 3339, может отсутствовать | когда оператор понизил привязку |

Времена привязки — `last_v2`, `expires_at`, `first_at`, `demoted_at` и `expires_at` понижения — идут по **доверенному времени** хранилища, а не по стеночным часам как они есть ([session_v2.md](../protocol/session_v2.md), «Часы сроков»). Пока действует скачок стеночных часов (`fetchRouteSummary.secure_sessions.clock_jump_active`), они отстают от стеночных часов на величину скачка: `expires_at`, который выглядит прошедшим, ещё действует. Продление никогда не сдвигает `last_v2` назад, поэтому, пока часы переведены назад, `last_v2` может быть впереди них.
| `total` | int | сколько действующих привязок оставляют фильтры, по всем страницам |
| `next_after` | host:port, может отсутствовать | курсор следующей страницы; на последней отсутствует |

Это единственный ответ, в котором адреса сопоставлены с identity, и он показывает только текущее состояние: прежние владельцы адреса не появляются ни в одном ответе — они остаются в локальном журнале.

### removeEndpointBinding

Снимает одну **обычную** привязку. Защищённая отклоняется (`endpoint_binding_protected`): защита заканчивается только подтверждённым понижением.

| Аргумент | Тип |
|---|---|
| `endpoint` | host:port, обязательный — ровно один |

Ответ: `operation_id`, `endpoint`, `key_form`. Снятие записывается на диск раньше, чем уходит из памяти, и пишется в журнал как `endpoint_binding_removed_by_operator` с адресом, identity и `operation_id`. После него адрес снова допускает v1 для незакреплённой identity.

### demoteEndpointBindings

Переводит **выбранные** защищённые привязки в general на 30 суток. Два шага:

1. **Пробный прогон** — без `confirm`. Ничего не меняет. Показывает каждый адрес, срок, который он получит, и последствие и возвращает `confirm_token`.
2. **Подтверждение** — те же `endpoints` и `confirm` = этот токен. Токен — дайджест ровно этого набора с защитой каждой привязки и последней доказанной на ней identity: другой набор или тот же после изменения защиты или владельца (из-за которого `identity_pin_remains` устарел бы) отклоняется с `endpoint_binding_selection_changed`.

**Чем токен не является.** `confirm_token` детерминирован — обычный SHA-256 от набора и его состояния, без секрета и срока, — поэтому его может вычислить любой, кто знает привязки. Он защищает от ошибки оператора — подтвердить не тот набор, что был показан, — а не от злоумышленника: право подтверждать даёт только аутентифицированный loopback-RPC.

| Аргумент | Тип | |
|---|---|---|
| `endpoints` | массив host:port или одна строка через запятую | обязательный, 1..256, каждый адрес один раз |
| `confirm` | строка, необязательный | `confirm_token` пробного прогона |

Ответ пробного прогона — `"confirmed": false`, `endpoints`, `consequence` (последствие: каждый адрес становится обычной привязкой и через 30 суток без сессии v2 на нём снова принимает v1 от незакреплённой identity) и `confirm_token`. Подтверждение — та же форма с `"confirmed": true`, сроком от момента подтверждения и `operation_id` вместо `confirm_token`.

| Поле | |
|---|---|
| `pool` | пул, в котором была учтена защита |
| `expires_at` | срок: 30 суток после понижения, продлевается каждой следующей сессией v2 |
| `identity_pin_remains` | последняя доказанная на адресе identity закреплена: сессия v1, называющая **эту** identity, по-прежнему отклоняется на любом адресе. Другую identity на адресе это не останавливает |

Целевая запись использует один полный снимок хранилища. Результат следует [четырём утверждённым исходам](../design/endpoint-binding-write-outcomes-decision.ru.md): ошибка после публикации может оставить изменённый валидный файл, а неудачное восстановление не доказывает rollback. Устаревший выбор не разрешает изменение; до включения требуется полный охват конкурирующих мутаций. Аудит и typed-клиенты различают четыре исхода — «не применено», «неопределённо», «применено, ошибка очистки» и commit — и отдельный результат восстановления, с operation ID и точными endpoints; нельзя писать о каждом отказе как о неизменённом действии. Примеры команд ниже описывают интерфейс, а не разрешение включить его.

#### CLI / консоль

```
listEndpointBindings identity=<id> limit=50
removeEndpointBinding 192.0.2.2:64646
demoteEndpointBindings 192.0.2.1:64646 192.0.2.3:64646
demoteEndpointBindings endpoints=192.0.2.1:64646,192.0.2.3:64646 confirm=<token>
```

### listEndpointDialDelays

Адреса, дозвоны к которым узел задерживает: адрес, чей **корректный** ответ — на пробу без identity или v1-welcome — заявил версию протокола ниже минимума этого узла, задерживается сам по себе: 30 с, удвоение на каждый следующий такой ответ, до 6 ч, чтобы действительно старый узел не перезванивался каждые несколько секунд ([session_v2.md](../protocol/session_v2.md)). Аргументов нет.

```json
[{"endpoint": "192.0.2.5:64646", "reason": "claimed_old_version", "claimed_version": 27, "steps": 3, "next_attempt_at": "2026-10-16T12:02:00Z"}]
```

| Поле | |
|---|---|
| `reason` | `claimed_old_version` — сегодня единственная причина |
| `claimed_version` | версия, которую объявил последний ответ; заявление, не доказательство |
| `steps` | сколько раз задержка выросла с последнего сброса; следующая удвоится от него |
| `next_attempt_at` | когда может пойти следующий дозвон; в прошлом, когда ожидание кончилось, а счёт ещё действует |

Задержка живёт только в памяти. Сбрасывает её только успешное исходящее подключение к тому же адресу — доказанная v2 или v1 после `auth_ok` с соблюдением pin; `retryEndpointDial` заканчивает текущее ожидание и сохраняет счёт; входящая сессия той же identity ничего не меняет.

### retryEndpointDial

Заканчивает текущее ожидание задержки за старую версию **одного** адреса после обновления пира. Это ещё одна попытка, а не сброс: `steps` сохраняется, поэтому пир, всё ещё заявляющий старую версию, задерживается дольше, а не снова с 30 с. Команда ничего не набирает — следующий дозвон узла туда может пойти сразу. Больше ничего не снимает: ни отступ повторов v2 (`dials_held_by_v2_retry_backoff`), ни охлаждение менеджера соединений, ни pin, ни привязку адреса, ни задержки других адресов — ответ v1 от привязанного адреса остаётся отвергнутым понижением, а закреплённая identity — отвергнутой по v1. Адрес без задержки отклоняется ошибкой валидации.

| Аргумент | Тип |
|---|---|
| `endpoint` | host:port, обязательный — ровно один |

```json
{"endpoint": "192.0.2.5:64646", "retried": true}
```

#### CLI / консоль

```
listEndpointDialDelays
retryEndpointDial 192.0.2.5:64646
```

### Preview и подтверждение identity pin

`previewIdentityPinRemoval identity=<40 строчных hex>` показывает один существующий identity pin: `identity`, исходный `pinned_at`, `consequence`, `consequence_version`, непрозрачный `confirm_token` и `removal_available`. Защита не меняется. До отдельного явного подтверждения оператору показываются точная identity и предупреждение. `confirmIdentityPinRemoval identity=<та же identity> confirm=<token>` — отдельная операция. Обе команды имеют `TransportLoopbackOnly`: нужны настоящий loopback socket и настроенная аутентификация. UI-варианты `previewIdentityPinRemovalUI` / `confirmIdentityPinRemovalUI` имеют `TransportInProcessOnly`, даже для аутентифицированного HTTP на loopback. Типизированные методы SDK используют эти внутрипроцессные entrypoints.

**Снятие пока недоступно:** preview сообщает `removal_available=false`, структурно корректный confirm возвращает `503 identity_pin_removal_unavailable`. До включения должны быть реализованы и допущены проверенное durable storage, внешний restart fence и recovery workflow. Обычный узел продолжает работать. Подготовительные команды не меняют identity pin, endpoint-привязки или другие требования v2.

Token связан с работающим хранилищем, точной identity, ревизией последнего принятого proof и версией предупреждения. Proof другой identity его не отменяет; новое proof этой identity, снятие с повторным pin или restart отменяют. Token — credential: диагностическое форматирование его скрывает; он передаётся только в ответе preview и явном поле подтверждения. Массового выбора нет. Отсутствующая/нулевая identity, отсутствующий pin или некорректное подтверждение дают `400`; недоступное или нечитаемое хранилище — `503`.

### Текущая локальная реализация сохранения

Внутренняя транзакция С-3 и восстанавливающий port `Service.RecoverIdentityPinProtection` реализуют строгие локальные save outcomes, guard recovery и fatal storage latch. Они не дают допуска: публичный confirm недоступен, test fence его не включает. Concrete verified-session adapter и offline CLI являются явным [review candidate](../operations/identity-pin-recovery-candidate.md). Для включения всё ещё нужны независимо подготовленное внешнее хранение и допущенный supervisor/operator-finalize workflow. Версия последствия 2 явно связывает token с неактивным требованием кандидата stop/одна операция/finalize. Одного restored receipt недостаточно для перезапуска сети. [Отдельный desktop-диалог](../operations/identity-pin-desktop.md) показывает точную identity, preview и явное согласие; production-подтверждение остаётся недоступным.

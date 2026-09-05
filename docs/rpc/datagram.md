# Datagram Commands

## English

Read-only observability for the datagram transport layer
(`docs/refactoring/datagram-transport.md`). The whole group is gated by the
`CORSA_ENABLE_DATAGRAM_V1` feature flag: on a node built without the plane the
commands answer **503 Unavailable** and are hidden from `help` and
autocomplete, exactly like the routing group on a node without distance-vector
routing.

Every command in this group is a **pure read**. None of them reserves a replay
slot, rotates the explore counter, dials a peer or spends a cryptographic
budget — the probe is what an artifact owner puts on a periodic ticker, and a
diagnostic with side effects on that path changes what it measures.

### fetchDatagramSummary

Diagnostics of the local plane: what the conveyor decided, what the
per-neighbour admission budget did, what the weighted class queue holds, what
the anti-replay cache refused, and the limits they all run on.

```bash
corsa-cli fetchDatagramSummary
```

Response:
```json
{
  "enabled": true,
  "endpoint": true,
  "transit": true,
  "dtypes": [],
  "registered_dtypes": [],
  "metrics": {
    "Observed": 128, "Accepted": 120, "Delivered": 40, "Forwarded": 80,
    "Answered": 0, "Dropped": 8,
    "UnknownDType": 6, "RefusedAnswers": 0,
    "ByMode": {"routed": {"Observed": 128, "Delivered": 40, "Forwarded": 80, "Answered": 0, "Dropped": 8}},
    "DropsByReason": {"unknown_dtype": 6, "admission": 2, "frame_too_large": 1},
    "ReverseEvents": {}
  },
  "admission": {"Admitted": 128, "RefusedBytes": 0, "RefusedFrames": 2},
  "queue": {"ControlFrames": 0, "BulkFrames": 0, "ControlBytes": 0, "BulkBytes": 0},
  "replay": {
    "Counters": {
      "Reserved": 128, "Duplicates": 3, "Committed": 125, "Released": 0,
      "StaleReleases": 0, "RejectedCapacity": 0, "RejectedNoisyPeer": 0,
      "EvictedNoisyPeer": 0, "ExpiredSwept": 61, "AbandonedReservations": 0
    },
    "Held": 64
  },
  "reverse": {"Held": 12, "LocalSlots": 3, "LocalRefusals": {"get_identity": 4}},
  "limits": {"Peer": {"BytesPerSecond": 1048576}, "Queue": {}, "Reverse": {}, "Replay": {}}
}
```

The response has **exactly these eleven top-level keys**. Zero-valued entries of
`DropsByReason` and `ReverseEvents` are omitted, so a reason that has never fired
is absent rather than `0`.

| Field | Type | Description |
|---|---|---|
| `enabled` | bool | Always `true` in a successful response; the disabled case is a 503 |
| `endpoint` | bool | Whether this node advertises `mesh_datagram_v1`. It follows the feature flag and the node type, never the registry: the capability states that the envelope is understood (§6 of the protocol spec), so it is `true` here as well, where the registry is empty and `dtypes` is the empty set. **There is no readiness barrier left to gate it** — the plane has no durable state to recover, so a node with a layer always advertises it |
| `transit` | bool | Whether this node advertises `mesh_datagram_transit_v1` (full nodes only). Independent of `endpoint`: relaying a type it cannot read is what a transit node is for |
| `dtypes` | array | The types this build handles as an ENDPOINT — the value of the `dtypes` handshake field. Derived from the registry, so it can never claim a type the node has no handler for; empty here means the empty set really goes on the wire as `[]` (§6.1) |
| `registered_dtypes` | array | The types actually present in the registry. Empty in PR-0: no type ships yet. It always equals `dtypes`; the pair is published so a disagreement is visible at a glance |
| `metrics` | object | The conveyor's decision breakdown, per mode and per drop reason. Everything the layer refuses is refused silently on the wire, so this is the only place a drop is observable. Four reasons come from OUTSIDE the conveyor and move only `DropsByReason`, never `Observed`: `frame_too_large` is the `MaxFrameLine` gate, counted before the parser and kept apart from `malformed` so the gate is distinguishable from a parser refusal; `plane_not_negotiated` is a datagram that arrived on a connection whose handshake never established `mesh_datagram_v1` — it exists because §2 makes that refusal silent on the WIRE rather than invisible to the operator: without the counter a neighbour off the plane pushing frames at line rate is indistinguishable from ordinary load; `writer_refused` is a frame the writer turned away AFTER the class queue released it — the queue never puts such a frame back, so without the counter it would vanish from every number the node publishes; `writer_panicked` is the same loss to a writer that CRASHED instead of answering, kept apart because `writer_refused` is backpressure that rises with load while any non-zero `writer_panicked` is a defect in the adapter with a crash report waiting in the log. `unproven_sender`, in contrast, comes FROM the conveyor and does move `Observed`: the layer refused to let a §7 hook judge by a name the sender picked for itself |
| `admission` | object | The per-neighbour §5 budget counters (admitted, refused for bytes, refused for frames, refused verifications) |
| `queue` | object | Current depth of the weighted class queue, per lane |
| `replay` | object | The base anti-replay cache: `Counters` (its lifetime counter set) and `Held` (records held right now, expired-but-retained included). This is the ONLY place the §5 fairness refusals surface — `RejectedNoisyPeer` and `EvictedNoisyPeer` say the cache refused or evicted the noisiest owner's record under pressure, `RejectedCapacity` that it was at its ceiling with nothing to reclaim, and `AbandonedReservations` that the watchdog reclaimed a pipeline branch which never reached commit or release (a non-zero value is a defect, not load). `Held` belongs beside them because a refusal reads differently against a full cache than against an empty one; the ceiling itself is `limits.Replay` |
| `reverse` | object | The reverse-state table: `Held` (open request records), `LocalSlots` (how many of the shared local-request slots are occupied — read against `limits.Reverse.PerUpstreamCap`) and `LocalRefusals` (per dtype, how many of THIS node's own requests that quota turned away). The table was absent from this summary entirely until the resource-measurement work, which made it the one component of the plane whose pressure could not be seen — and the only one whose overflow REFUSES instead of evicting. That distinction is why `LocalRefusals` is keyed by dtype: every locally originated exchange shares one bucket, so a busy subsystem can stop another from ever asking without exceeding a limit of its own, and a bare refusal count cannot name the victim. Transit refusals are deliberately NOT attributed this way — their dtype arrives from the wire, and a map keyed on it would let a stranger grow this node's memory one invented type name at a time; those are counted by `metrics.DropsByReason["reverse_slot_capped"]` instead |
| `limits` | object | The §5 numbers in force, so a reader never has to guess which build the counters came from |

**Fields removed together with the durable half of the layer.** They are listed
because an old client may still look for them, and their absence is now
normative: `plane_ready`, `boot_generation`, `boot_generation_trusted`,
`quarantine`, `owed_profile_announcements`, `identity_publisher`,
`identity_publication_gap`, `identity_publication_converged`,
`identity_publication_unpublished`, and the `Stored` / `DuplicateNotified`
counters inside `metrics`. There is no recovery barrier, no boot generation, no
durable store and no profile registry behind them any more — the node does not
even create `datagram_boot.json` in its data directory.

### datagramReachable

Probe whether a datagram of a given type would find a first hop to the
destination right now.

```bash
corsa-cli datagramReachable <identity> <dtype>
```

| Argument | Required | Description |
|---|---|---|
| `identity` | yes | Destination peer Ed25519 fingerprint |
| `dtype` | **yes** | The datagram type. It decides the last-hop gate (§4.4, §6.1), and there is no "no particular type" to ask about — see the note below |

**There is no third argument.** `req_caps` used to be one, and both the argument
and the envelope field are gone: a path-wide capability requirement is what made
an old relay refuse a new endpoint protocol, and the only capability gate left is
the ROLE gate over `mesh_datagram_v1` / `mesh_datagram_transit_v1`, which reads
what the PEER advertises and nothing the frame carries.

Response:
```json
{
  "identity": "abc123...",
  "dtype": "push_identity",
  "reachable": false,
  "reason": "unsupported_dtype"
}
```

| Field | Type | Description |
|---|---|---|
| `reachable` | bool | Whether there is somebody to give the first hop to |
| `reason` | string | Present only when `reachable` is false. `no_route` — the routing table has nothing; `unsupported_dtype` — the destination is a live neighbour whose declared `dtypes` set omits this type (a node that declared no type omits every type); `missing_capability` — a candidate does not advertise one of the two ROLE capabilities of §6 |
| `missing_capability` | string | Present only with `reason: "missing_capability"`: the role capability the path was missing — `mesh_datagram_v1` or `mesh_datagram_transit_v1` |

**The two negatives are different facts, and that is why the reason is
published.** §6.1 makes a negative live answer about SUPPORT cancel a cached
`dtype` confirmation immediately — that is `unsupported_dtype`. A
destination that is merely off the routing table says nothing about support, and
acting on it would clear a good confirmation on every transient route loss.

**The guarantee is one-way.** `reachable: false` means a send performed at the
same moment over the same data would **not** have been queued — it would have
returned `no_route`, or a gate's `rejected`, the last-hop dtype gate included,
and `reason` names which of the two.
`reachable: true` promises nothing: the probe is TOCTOU by construction, the
route may disappear between the two calls, and no read-only interface can fix
that. It also proves nothing about the remote endpoint's support for the type
beyond what the last-hop gate can check for a direct peer.

The probe reads the **fresh** per-destination lookup, the same source a locally
originated send reads, so an action taken right after a route appears is not
answered with "unreachable" while the send would already work.

**`dtype` is mandatory, and the previous wording was wrong.** This page used to
call it optional and to say an absent one "exercises no last-hop gate". Nothing
implemented that reading: the query carries a `domain.DType`, an absent one is
the empty string, and the last-hop gate asks the destination's declared set
about that empty name — which is in no explicit set, so every
live neighbour answered `unsupported_dtype`. An operator was told a reachable
destination was unreachable, and told it with the one reason §6.1 makes
actionable.

Of the two honest fixes, only one is available at this boundary. Skipping the
gate for an explicitly unset type would need the LAYER to represent "no type" —
`datagram.ReachabilityQuery` carries a bare `domain.DType`, and whether a gate
applies is the layer's decision, not the RPC's. Until the layer exposes that
(an optional dtype on the query, or an exported "any type" sentinel the last-hop
gate honours), the command requires the type, which is also what the node's own
`DatagramReachable` has always documented.

#### The arguments must describe a datagram that can really be sent

Both diagnostics answer for **one concrete frame**, so the boundary accepts
exactly what the wire accepts and nothing more. The parsed arguments are
assembled into the routed frame a local send would put on the wire and handed to
the **same `Validate` that `RoutedFrameBuilder.Build` runs**; a frame that fails
it is answered with **400 Bad Request** carrying the wire's own refusal, instead
of a reachability verdict. A route reported for a frame that a real send drops
before the queue is not a diagnostic — it is a wrong answer with a next hop in
it.

What is left to refuse is the destination and the type: an identity that is not
40 lowercase hex or is the all-zero sentinel, a `dtype` outside
`[a-z0-9_]{1,64}`, and a `route_policy` that is neither `best` nor `explore`.

The fields neither command takes are filled with the values the **layer** fixes,
so the frame under validation is the one a send builds and not a lookalike:
`v`/`mode`/`ttl`/`max_ttl` are the origin constants of §2.1 and §4.1, `class` is
`control`, `src` and the auth block stand in for what the node's identity and key
supply, and `payload` is empty. The header has no optional field left to omit —
`req_caps` and `ext` are gone from the envelope — so the validated frame differs
from a real send only in the values the layer would have supplied.

### explainDatagramRoute

The ranked next-hop plan a real send would build — the datagram counterpart of
`explainFileRoute`.

```bash
corsa-cli explainDatagramRoute <identity> <dtype> [route_policy]
```

`dtype` is required for the same reason it is required on the probe: the plan is
built over the same gates.

`route_policy` is `best` (default) or `explore`; any other value is refused.
There is no `req_caps` argument, for the reason stated on the probe.

Response:
```json
{
  "route_policy": "best",
  "first_candidate_guaranteed": true,
  "candidates": [
    {
      "next_hop": "abc123...",
      "hops": 1,
      "protocol_version": 27,
      "connected_at": "2026-04-01T12:00:00Z",
      "uptime_seconds": 3600.5,
      "route_source": "direct",
      "discovery_plane": "mesh",
      "best": true
    }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `route_policy` | string | The policy the plan was built for |
| `first_candidate_guaranteed` | bool | `false` under `explore`: the rotation counter advances on a send, a read-only plan neither moves nor reserves it, and under concurrent sends of the same key "the next candidate" is not defined in advance. Only `best` promises that element 0 is what the send would try first |
| `candidates[].next_hop` | string | The neighbour the frame would be handed to |
| `candidates[].hops` | int | Distance to the destination; the direct session reports 1 |
| `candidates[].protocol_version` | int | The **normalized** ranking key: `min(reported, local)`. A peer claiming a higher version is capped rather than zeroed, so a staged rollout is not starved |
| `candidates[].connected_at` | string | When the chosen connection was established; omitted when unknown |
| `candidates[].uptime_seconds` | float | Derived from `connected_at`, clamped at zero so peer clock skew never renders as negative uptime |
| `candidates[].route_source` | string | **Trust axis**: how the route is proven — `direct`, `hop_ack`, `announcement` or `local`. Omitted together with `discovery_plane` when the resolver attributed nothing |
| `candidates[].discovery_plane` | string | **Plane axis**: which plane produced the route — `mesh` today, `overlay` once the structured overlay answers. It is not a trust rank and takes part in no ranking key |
| `candidates[].best` | bool | `true` only for index 0 |

The two attribution fields are **orthogonal and both rendered**. A hop found
through the overlay that turned out to be a live session reads
`"route_source": "direct"` together with `"discovery_plane": "overlay"`, and
neither fact is derivable from the other — which is the whole reason there are
two fields rather than one enum. They are omitted **together** when no resolver
attributed the route: an absent field is the honest rendering of "nobody said",
while filling in `mesh` would print a claim no plane ever made.

The metadata of each candidate describes **one concrete connection** — the one
the live send path would try first, outbound session preferred over inbound —
and comes from the same helper the send itself uses. It is never an aggregate
across a peer's sockets: ranking by the maximum version of one socket while the
bytes leave over another is a bug the file router already shipped once.

---

## Русский

Read-only наблюдаемость слоя транспорта датаграмм
(`docs/refactoring/datagram-transport.md`). Вся группа закрыта фиче-флагом
`CORSA_ENABLE_DATAGRAM_V1`: на узле без плоскости команды отвечают
**503 Unavailable** и скрыты из `help` и автодополнения — ровно как группа
маршрутизации на узле без distance-vector.

Каждая команда группы — **чистое чтение**. Ни одна не резервирует реплей-слот,
не крутит счётчик `explore`, не дозванивается до пира и не тратит
криптографический бюджет: на пробе у владельца артефакта сидит периодический
тикер, а диагностика с побочными эффектами на этом пути меняет то, что измеряет.

### fetchDatagramSummary

Диагностика локальной плоскости: что решил конвейер, что сделал бюджет допуска
по соседям, что лежит во взвешенной классовой очереди, что отверг кэш
анти-реплея и на каких лимитах всё это работает.

```bash
corsa-cli fetchDatagramSummary
```

Схема ответа — см. английскую секцию выше. Верхнеуровневых ключей ровно
одиннадцать.

| Поле | Тип | Описание |
|---|---|---|
| `enabled` | bool | В успешном ответе всегда `true`; выключенная плоскость — это 503 |
| `endpoint` | bool | Рекламирует ли узел `mesh_datagram_v1`. Следует за фиче-флагом и типом узла, а не за реестром: возможность утверждает понимание конверта (§6 спеки протокола), поэтому здесь `true`, хотя реестр пуст, а `dtypes` — пустое множество. **Барьера готовности, который мог бы это погасить, больше нет**: у плоскости нет durable-состояния, которое надо восстанавливать, поэтому узел со собранным слоем рекламирует возможность всегда |
| `transit` | bool | Рекламирует ли узел `mesh_datagram_transit_v1` (только полные узлы). Независимо от `endpoint`: пересылать нечитаемый тип и есть работа транзита |
| `dtypes` | array | Типы, которые эта сборка обрабатывает как КОНЕЧНЫЙ УЗЕЛ, — значение поля `dtypes` в рукопожатии. Выводится из реестра, поэтому не может объявить тип, для которого нет обработчика; пусто здесь означает, что на провод уходит явный пустой массив `[]` (§6.1) |
| `registered_dtypes` | array | Типы, реально присутствующие в реестре. В PR-0 пусто: ни одного типа ещё не выпущено. Всегда равно `dtypes`; пара публикуется, чтобы расхождение было видно сразу |
| `metrics` | object | Разбивка решений конвейера по режимам и причинам дропа. Всё, что слой отвергает, отвергается на проводе молча, поэтому это единственное место, где дроп вообще наблюдаем. Четыре причины приходят ИЗВНЕ конвейера и двигают только `DropsByReason`, но не `Observed`: `frame_too_large` — гейт `MaxFrameLine`, считается до парсера и держится отдельно от `malformed`, чтобы гейт отличался от отказа парсера; `plane_not_negotiated` — датаграмма, пришедшая по соединению, чьё рукопожатие не установило `mesh_datagram_v1`; она существует потому, что §2 делает такой отказ молчаливым НА ПРОВОДЕ, а не невидимым для оператора — без счётчика сосед вне плоскости, льющий кадры на скорости линка, неотличим от штатной нагрузки; `writer_refused` — кадр, который writer отверг УЖЕ ПОСЛЕ выдачи из классовой очереди: очередь такие кадры назад не кладёт, и без счётчика он исчезал бы изо всех публикуемых узлом чисел; `writer_panicked` — та же потеря, но writer не ответил, а УПАЛ; держится отдельно, потому что `writer_refused` — backpressure, растущий с нагрузкой, а любое ненулевое `writer_panicked` — дефект адаптера, к которому в логе уже лежит crash-report. `unproven_sender`, наоборот, приходит ИЗ конвейера и `Observed` двигает: слой не дал §7-хуку решать по имени, которое отправитель выбрал себе сам |
| `admission` | object | Счётчики бюджета §5 по соседям (допущено, отказано по байтам, по кадрам, по проверкам подписи) |
| `queue` | object | Текущая глубина взвешенной классовой очереди по полосам |
| `replay` | object | Базовый кэш анти-реплея: `Counters` (счётчики за всё время жизни) и `Held` (сколько записей он держит прямо сейчас, включая истёкшие, но ещё удерживаемые). Это ЕДИНСТВЕННОЕ место, где видны отказы честности §5: `RejectedNoisyPeer` и `EvictedNoisyPeer` означают, что под давлением кэш отверг или вытеснил запись самого шумного владельца, `RejectedCapacity` — что он упёрся в потолок и освобождать было нечего, а `AbandonedReservations` — что сторож забрал ветку конвейера, не дошедшую ни до commit, ни до release (ненулевое значение здесь — дефект, а не нагрузка). `Held` стоит рядом потому, что отказ читается по-разному при полном и при пустом кэше; сам потолок — в `limits.Replay` |
| `reverse` | object | Таблица reverse-состояния: `Held` (открытые записи запросов), `LocalSlots` (сколько общих слотов локальных запросов занято — читать против `limits.Reverse.PerUpstreamCap`) и `LocalRefusals` (по dtype: скольким СОБСТВЕННЫМ запросам этого узла квота отказала). До работ по измерению ресурсов таблицы в этой сводке не было вовсе — она была единственным компонентом плоскости, чьё давление не видно, и единственным, чьё переполнение ОТКАЗЫВАЕТ, а не вытесняет. Из-за этого `LocalRefusals` и разложена по dtype: все локальные обмены делят один бакет, поэтому активная подсистема способна лишить другую возможности спросить, не превысив ни одного собственного лимита, а голый счётчик отказов не назовёт жертву. Транзитные отказы намеренно НЕ атрибутируются так же: их dtype приходит с провода, и карта с таким ключом позволила бы постороннему растить память узла по одному выдуманному имени за раз — они считаются через `metrics.DropsByReason["reverse_slot_capped"]` |
| `limits` | object | Действующие числа §5 — чтобы читателю не приходилось угадывать, из какой сборки счётчики |

**Поля, снятые вместе с durable-половиной слоя.** Они перечислены потому, что
старый клиент может их ещё искать, и их отсутствие теперь нормативно:
`plane_ready`, `boot_generation`, `boot_generation_trusted`, `quarantine`,
`owed_profile_announcements`, `identity_publisher`, `identity_publication_gap`,
`identity_publication_converged`, `identity_publication_unpublished`, а также
счётчики `Stored` и `DuplicateNotified` внутри `metrics`. За ними больше не стоит
ни барьера восстановления, ни boot-поколения, ни durable-хранилища, ни реестра
профилей — узел даже не создаёт `datagram_boot.json` в data-dir.

### datagramReachable

Проба: найдётся ли прямо сейчас первый хоп для датаграммы указанного типа к
адресату.

```bash
corsa-cli datagramReachable <identity> <dtype>
```

| Аргумент | Обязателен | Описание |
|---|---|---|
| `identity` | да | Ed25519-отпечаток узла-адресата |
| `dtype` | **да** | Тип датаграммы. Он решает last-hop-гейт (§4.4, §6.1), и спрашивать «без конкретного типа» не о чем — см. замечание ниже |

**Третьего аргумента нет.** Им был `req_caps`, и ушли и аргумент, и поле
конверта: требование возможностей ко всему пути — ровно то, из-за чего старый
релей отказывал новому протоколу конечных узлов, а единственный оставшийся гейт
по возможностям — РОЛЕВОЙ, по `mesh_datagram_v1` / `mesh_datagram_transit_v1`, и
он читает то, что рекламирует ПИР, а не то, что несёт кадр.

| Поле ответа | Тип | Описание |
|---|---|---|
| `reachable` | bool | Есть ли кому отдать первый хоп |
| `reason` | string | Присутствует только при `reachable: false`. `no_route` — в таблице маршрутов ничего нет; `unsupported_dtype` — адресат живой сосед, но в его объявленном наборе `dtypes` этого типа нет (узел, не объявивший ни одного типа, не поддерживает ни один); `missing_capability` — кандидат не рекламирует одну из двух РОЛЕВЫХ возможностей §6 |
| `missing_capability` | string | Только при `reason: "missing_capability"`: ролевая возможность, которой не хватило пути, — `mesh_datagram_v1` либо `mesh_datagram_transit_v1` |

**Два отрицания — разные факты, поэтому причина публикуется.** §6.1 требует,
чтобы отрицательный live-ответ о ПОДДЕРЖКЕ немедленно отменял кэшированное
подтверждение `dtype` — это `unsupported_dtype`. Адресат, которого
просто нет в таблице маршрутов, о поддержке не говорит ничего, и реакция на него
стирала бы хорошее подтверждение при каждом дребезге маршрута.

**Гарантия односторонняя.** `reachable: false` означает, что отправка,
выполненная в тот же момент над теми же данными, **не была бы поставлена** —
вернула бы `no_route` либо `rejected` гейта, включая last-hop-гейт по типу, и
`reason` называет, какой именно из двух.
`reachable: true` не обещает ничего: проба — TOCTOU по построению, маршрут
может исчезнуть между двумя вызовами, и никакой read-only интерфейс этого не
устранит. Поддержку типа удалённым адресатом она тоже не доказывает — сверх
того, что для прямого пира даёт last-hop-гейт.

Проба читает **свежий** per-destination lookup — тот же источник, что и
локально созданная отправка, — поэтому действие пользователя сразу после
появления маршрута не получает «недостижим», когда отправка уже сработала бы.

**`dtype` обязателен, и прежняя формулировка была неверна.** Эта страница
называла его необязательным и утверждала, что при отсутствии last-hop-гейт «не
срабатывает». Ничто этого не реализовывало: запрос несёт `domain.DType`,
отсутствие — это пустая строка, а last-hop-гейт спрашивает объявленный набор
адресата про это пустое имя, которого нет ни в одном объявленном списке, —
поэтому любой живой сосед отвечал `unsupported_dtype`. Оператор получал
«недостижим» на достижимом пире, причём с той самой причиной, на которую §6.1
предписывает реагировать.

Из двух честных решений на этой границе доступно только одно. Пропуск гейта для
явно незаданного типа требует, чтобы «тип не задан» умел выражать САМ СЛОЙ:
`datagram.ReachabilityQuery` несёт голый `domain.DType`, и решение о
применимости гейта принадлежит слою, а не RPC. Пока слой этого не предоставил
(опциональный dtype в запросе либо экспортированный «любой тип», который
уважает last-hop-гейт), команда требует тип — ровно как всегда и было записано
в документации самого `DatagramReachable` на узле.

#### Аргументы обязаны описывать датаграмму, которую реально можно отправить

Обе диагностики отвечают про **один конкретный кадр**, поэтому граница
принимает ровно то, что принимает провод, и ничего сверх. Разобранные аргументы
собираются в тот routed-кадр, который положила бы на провод локальная отправка,
и передаются в **тот же `Validate`, который выполняет `RoutedFrameBuilder.Build`**;
кадр, его не прошедший, получает **400 Bad Request** с собственным отказом
провода внутри, а не вердикт о достижимости. Маршрут, сообщённый для кадра, который
реальная отправка отбросит ещё до очереди, — это не диагностика, а неправильный
ответ со следующим хопом внутри.

Отвергать здесь осталось адресата и тип: identity, не являющуюся 40 hex в нижнем
регистре или равную нулевому sentinel, `dtype` вне `[a-z0-9_]{1,64}` и
`route_policy`, не равную ни `best`, ни `explore`.

Поля, которых обе команды не принимают, заполняются теми значениями, которые
фиксирует **слой**, — чтобы под проверкой был кадр реальной отправки, а не
похожий на него: `v`/`mode`/`ttl`/`max_ttl` — константы origin из §2.1 и §4.1,
`class` — `control`, `src` и блок auth подставлены вместо того, что даёт
идентичность и ключ узла, `payload` пуст. Опускать в заголовке больше нечего —
`req_caps` и `ext` из конверта убраны, — поэтому проверяемый кадр отличается от
реальной отправки только теми значениями, которые подставил бы слой.

### explainDatagramRoute

Ранжированный план следующих хопов, который построила бы реальная отправка, —
датаграммный аналог `explainFileRoute`.

```bash
corsa-cli explainDatagramRoute <identity> <dtype> [route_policy]
```

`dtype` обязателен по той же причине, что и в пробе: план строится теми же
гейтами.

`route_policy` — `best` (по умолчанию) или `explore`; любое другое значение
отклоняется. Аргумента `req_caps` нет — по причине, названной в пробе.

| Поле | Тип | Описание |
|---|---|---|
| `route_policy` | string | Политика, под которую построен план |
| `first_candidate_guaranteed` | bool | `false` при `explore`: счётчик ротации двигается на ОТПРАВКЕ, read-only план его не трогает и не резервирует, а при конкурентных отправках одного ключа «следующий кандидат» вообще не определён заранее. Только `best` обещает, что элемент 0 — это то, что отправка попробует первым |
| `candidates[].next_hop` | string | Сосед, которому был бы передан кадр |
| `candidates[].hops` | int | Расстояние до адресата; прямая сессия отдаёт 1 |
| `candidates[].protocol_version` | int | **Нормализованный** ключ ранжирования: `min(reported, local)`. Пир, заявивший версию выше локальной, клампится, а не обнуляется, — иначе staged rollout голодал бы |
| `candidates[].connected_at` | string | Момент установления выбранного соединения; опускается, если неизвестен |
| `candidates[].uptime_seconds` | float | Производное от `connected_at`, зажатое снизу нулём, чтобы перекос часов пира не давал отрицательный аптайм |
| `candidates[].route_source` | string | **Ось доверия**: чем маршрут подтверждён — `direct`, `hop_ack`, `announcement` или `local`. Опускается вместе с `discovery_plane`, если резолвер не проставил атрибуцию |
| `candidates[].discovery_plane` | string | **Ось плоскости**: какая плоскость дала маршрут — сегодня `mesh`, `overlay` после появления структурного оверлея. Рангом доверия НЕ является и ни в один ключ ранжирования не входит |
| `candidates[].best` | bool | `true` только для индекса 0 |

Две оси атрибуции **ортогональны и рендерятся обе**. Хоп, найденный через
оверлей и оказавшийся живой сессией, отдаёт `"route_source": "direct"` вместе с
`"discovery_plane": "overlay"`, и ни один из фактов не выводится из другого —
ровно поэтому полей два, а не одно перечисление. Опускаются они **вместе**,
если атрибуцию не проставил никто: отсутствие поля — честный рендер «никто не
сказал», тогда как подставленный `mesh` был бы утверждением, которого ни одна
плоскость не делала.

Метаданные каждого кандидата описывают **одно конкретное соединение** — то,
которое живой путь отправки попробует первым (outbound-сессия приоритетнее
inbound), — и берутся из того же хелпера, которым пользуется сама отправка.
Это никогда не агрегат по всем сокетам пира: ранжирование по максимальной
версии с одного сокета, когда байты уходят по другому, — баг, который файловый
роутер уже однажды выпустил.

# Compact Route Announce Frame (Phase 4)

## English

This document specifies the `route_announce_v3` wire frame introduced in
Phase 4 of the cluster-mesh plan
(`docs/cluster-mesh/phase-4-compact-wire-signed.md` §3.1, overview §7.1),
gated by the `mesh_routing_v3` capability. The frame is the compact
replacement for the legacy `announce_routes` / `routes_update` pair: a
single frame type carries both full sync and delta via the `kind`
discriminator, and the redundant `Origin` field is dropped from the
wire — the receiver derives the uplink from the sending session, so
`Origin` carries no routing-relevant information after the Phase 1
per-uplink storage refactor (overview §4.1).

### Wire format

```
{
  "type":      "route_announce_v3",
  "kind":      "full" | "delta",
  "epoch":     7,
  "issued_at": "2026-05-27T12:00:00Z",
  "entries": [
    { "identity": "<dest_fp>", "hops": 2, "seq_no": 41, "extra": {...}, "sig": "<b64>" }
  ]
}
```

`kind` discriminates a self-contained baseline (`full`) from an
incremental update (`delta`). A `full` frame replaces the prior view
of the sender's table for this session; a `delta` carries only entries
that changed since the last frame to this peer and requires a prior
baseline.

`epoch` is the sender's local table-generation counter. It increments
when the sender resets its routing table (e.g. process restart). The
receiver tracks the per-peer high-water mark and uses it to
distinguish a stale-process replay (`incoming < known` → drop the
frame) from a fresh table that invalidates the diff baseline
(`incoming > known` → see "Epoch handling" below). The default value
on a freshly constructed `routing.Table` is `1`; in-process resets are
a documented Phase 4 follow-up.

`issued_at` is the sender-local RFC3339 timestamp — a diagnostic
freshness hint. It is NOT consulted for acceptance decisions
(per-(Identity, sender) SeqNo monotonicity governs that — overview
§4.3).

Each entry:

- `identity` is the destination fingerprint. There is no `origin`
  field; the receiver synthesises Origin as the sending identity on
  ingest (sender-originated semantic — identical to the
  `Origin = localOrigin` emit the legacy senders use post-Phase-1).
- `hops` is the sender's local hop count as `uint8`. The receiver adds
  `+1` on ingest (receiver convention). `hops == HopsInfinity` (16) is
  the withdrawal marker; values above 16 are clamped on send to keep
  the withdrawal semantic intact through type narrowing.
- `seq_no` is the per-(Identity, sender) wire SeqNo synthesised by the
  sender (Phase 1 §3.1.4). Receivers reject stale entries via the
  existing watermark.
- `extra` is an opaque JSON object forwarded unchanged through transit
  — the same forward-compat semantic legacy `AnnounceRouteFrame.Extra`
  provides. Carried verbatim so a signature over canonical bytes stays
  reproducible after the entry has crossed nodes that do not
  understand the payload.
- `sig` is the optional Ed25519 signature over the entry's canonical
  bytes by the destination identity's key. The signer
  (`signOwnOriginV3Entries`) only populates this field for entries
  whose `Identity == localIdentity`; everything else passes through
  with whatever bytes the original signer produced (or empty for
  unsigned claims). **Production advertise is currently OFF** AND
  **the receive-side verifier is correspondingly unreachable in
  production**: `localCapabilities` does NOT include
  `mesh_attested_links_v1` even when `EnableMeshRoutingV3` is on
  (because the production emit path
  `route_store.AnnounceProjectionFor` iterates the bucket map and
  never emits the local identity — the synthetic self-route lives in
  `Lookup`/`Snapshot` and is not stored, so the signer would have no
  input), and the negotiated cap set is the intersection of local
  and remote advertised caps (`peer_sessions.go`
  `intersectCapabilities`). A remote peer that advertises
  `mesh_attested_links_v1` therefore still negotiates an EMPTY
  intersection for that cap against a current local node, so
  `peerSupportsAttestedLinks(session.address)` returns false and the
  verifier branch in `handleRouteAnnounceV3` is skipped — sig bytes
  flow through storage as informational only. The verifier code,
  the `AttestedSigVerified` storage field, and the `scoreSignedBonus`
  trust-ladder are kept wired so the day local re-enables the
  advertise (Phase 5 anchor publication, which adds the
  self-attestation entry stream of Identity == localIdentity, per-
  emitter SeqNo, anchor metadata in `Extra`), the intersection
  becomes non-empty against any peer that also advertises the cap
  and the verifier engages without further wiring. Until then the
  only way to exercise the verifier path is the synthetic unit-test
  fixtures in `routing_announce_v3_attest_test.go` that wire the cap
  directly onto a test session. See "Capability negotiation" below
  for the trust ladder and "Canonical signing bytes" below for the
  layout.

### Capability negotiation

`mesh_routing_v3` is additive. The send-side selection rule is:

- Both peers advertise `mesh_routing_v3` AND `mesh_routing_v1` AND
  `mesh_relay_v1` → use `route_announce_v3`.
- Otherwise fall back to `routes_update` (`mesh_routing_v2`) for delta
  and `announce_routes` (`mesh_routing_v1`) for full / first-sync, per
  the existing classifier.

`mesh_routing_v3` is meaningful only when `mesh_routing_v1` is also
negotiated, mirroring the v2 rule: v1 gates the legacy fallback the
mixed-version flow depends on, and a peer advertising v3 without v1
is treated as legacy. Capability registration: `CapMeshRoutingV3` in
`internal/core/domain/capability.go`.

### Receive contract

Implementation: `handleRouteAnnounceV3` in
`internal/core/node/routing_announce.go`. The handler synthesises
`Origin = senderIdentity` for every entry and feeds the result through
the existing `applyAnnounceEntries` pipeline (own-origin forgery guard,
transit-withdrawal authority, UpdateRoute / WithdrawRoute dispatch,
drain trigger), so trust classification and per-entry validation reuse
the legacy receive path exactly.

#### Epoch handling

The per-peer epoch watermark lives on `AnnouncePeerState`
(`ObserveV3Epoch` / `KnownV3Epoch`) and resets on every session
boundary (`MarkDisconnected` / `MarkReconnected`):

- `V3EpochStale` (incoming < known): a replay from an older peer
  process. The whole frame is dropped; the watermark is left
  untouched.
- `V3EpochApply` (incoming == known, or first frame this session):
  normal path. Apply the entries.
- `V3EpochReset` (incoming > known): the peer's table is fresh, so
  any diff baseline we hold described the OLD table. A `kind="full"`
  frame is self-contained and simply re-establishes the baseline for
  the new epoch (no resync needed). A `kind="delta"` frame after a
  reset has no valid baseline to diff against, so it is treated like
  the delta-before-baseline desync below.

Note: after a peer restart the per-(Identity, Uplink) SeqNo
watermarks are NOT reset on epoch advance, so stale claims persist
until TTL expiry. A full epoch-driven SeqNo reset is a documented
Phase 4 follow-up.

#### Baseline gate

A `kind="delta"` frame before any baseline this session (or after an
epoch reset that invalidated the prior baseline) is a desync — the
receiver replies with `request_resync` and drops the delta, mirroring
the `routes_update` baseline gate exactly. `request_resync` is the
v2-gated escape hatch; a v3 peer carries `mesh_routing_v2` in every
real deployment (`localCapabilities` advertises both), so the fast
recovery works. A hypothetical v3-only peer (no v2) simply recovers
on its next periodic `kind="full"` cycle, which re-baselines without
any signal — `request_resync` is an optimisation, not a correctness
requirement.

### Canonical signing bytes

The optional `sig` field on each entry covers the deterministic byte
string returned by `RouteAnnounceV3Entry.CanonicalSigningBytes()`:

```
lenpfx(identity) || lenpfx(extra)
```

where `lenpfx(x)` is an 8-byte big-endian length followed by the field
bytes. A length-prefixed binary encoding is used instead of JSON
because JSON is not canonical (encoder-dependent key order and
whitespace) and would make signatures non-reproducible. `extra` is
hashed verbatim from the wire bytes: transit forwards it unchanged,
so the verifier reconstructs identical bytes even after the entry
has crossed intermediate nodes.

**Hops, epoch, and seq_no are all deliberately excluded.** All three
fields vary per emitter:

- `hops` increments on every transit hop (receiver convention, `+1`
  on ingest).
- `epoch` is the SENDING node's local table-generation counter; the
  origin signs with their own epoch, but a transit node forwarding
  the entry stamps the wire frame with their own (different) epoch.
- `seq_no` on the wire is synthesised by the EMITTER via the outbound
  SeqNo helpers (see `route_store.go`
  `nextOutboundSeqLockedPerPeer` / `nextOutboundSeqLockedBroadcast`).
  Transit re-emit can advance the wire seq_no past the origin's
  native value (outboundMax+1 monotonicity guard) while forwarding
  `AttestedSig` verbatim, so including seq_no in the signed payload
  would make the origin's signature unverifiable after the first
  re-emit.

Including any of them would make multi-hop transit verification
impossible — a final receiver re-computing canonical bytes from the
last emitter's wire frame would never match the origin's signature.
Excluding all three lets the signature attest to the stable
properties of the identity's claim (`identity`, `extra`) while
leaving per-emitter routing / table-generation / sequencing metadata
on the wire (carried for receiver-side use, but not signed). This
mirrors the design of attestation protocols such as BGP RPKI ROAs,
which sign stable asset properties rather than per-hop routing
metric. Replay protection across epochs is provided by per-(Identity,
sender) SeqNo monotonicity (overview §4.3), not by epoch in the
signed bytes;
epoch on the wire still serves its purpose of signalling sender
table reset to the receiver (`ObserveV3Epoch` path) — it just is not
part of the signed payload.

This is the byte-stable contract the `mesh_attested_links_v1` signer
and verifier share (Phase 4 §3.2 — see also
`docs/protocol/attested_links.md`). `sig` validation is gated on the
per-session capability negotiation: receivers run the ed25519
verifier (and apply the trust-score bonus) only when both peers
negotiated `mesh_attested_links_v1`; otherwise the bytes are
forwarded through but treated as informational.

### Wire size and chunking

The send path applies the same `MaxFrameLine` (128 KiB) budget every
announce-plane frame must fit under. `chunkRouteAnnounceV3EntriesBySize`
splits an entry slice greedily into frames that each fit the budget;
entries whose own marshalled size exceeds the budget are reported via
`announce_routes_entry_oversize_dropped` and excluded from the wire
chunks (identical contract to the legacy chunker). The truthful-
delivery rule mirrors `SendAnnounceRoutes` / `SendRoutesUpdate`: any
skipped entry forces `SendRouteAnnounceV3` to return `false` so the
caller's per-peer cache stays untouched and the next cycle retries.

---

## Русский

Этот документ описывает wire-фрейм `route_announce_v3`, введённый в
Phase 4 cluster-mesh-плана
(`docs/cluster-mesh/phase-4-compact-wire-signed.md` §3.1, overview §7.1),
гейтится capability `mesh_routing_v3`. Фрейм — компактная замена
устаревшей пары `announce_routes` / `routes_update`: один тип фрейма
несёт и full sync, и delta через дискриминатор `kind`, а избыточное
поле `Origin` уходит с провода — получатель выводит uplink из
отправляющей сессии, поэтому `Origin` после Phase 1 per-uplink
storage refactor (overview §4.1) не несёт информации, релевантной для
маршрутизации.

### Wire-формат

```
{
  "type":      "route_announce_v3",
  "kind":      "full" | "delta",
  "epoch":     7,
  "issued_at": "2026-05-27T12:00:00Z",
  "entries": [
    { "identity": "<dest_fp>", "hops": 2, "seq_no": 41, "extra": {...}, "sig": "<b64>" }
  ]
}
```

`kind` различает self-contained baseline (`full`) и инкрементальный
update (`delta`). `full` заменяет предыдущее представление таблицы
отправителя на этой сессии; `delta` несёт только изменившиеся
entries и требует существующего baseline.

`epoch` — локальный счётчик генерации таблицы отправителя.
Инкрементируется при reset таблицы (например, при рестарте процесса).
Получатель ведёт per-peer high-water и различает stale-replay
(`incoming < known` → drop) и свежую таблицу, инвалидирующую diff
baseline (`incoming > known` → см. «Обработка epoch» ниже). Дефолт у
свежесозданной `routing.Table` — `1`; in-process reset — задокументи-
рованный Phase 4 follow-up.

`issued_at` — RFC3339-timestamp отправителя как freshness-хинт для
диагностики; для accept-решений не используется (per-(Identity,
sender) SeqNo-монотонность — overview §4.3).

Каждый entry:

- `identity` — fingerprint назначения. Поля `origin` нет; получатель
  на ingest синтезирует Origin как identity отправителя (sender-
  originated semantic — идентично emit `Origin = localOrigin` post-
  Phase-1).
- `hops` — локальный hop-count отправителя как `uint8`. Получатель
  добавляет `+1` (receiver convention). `hops == HopsInfinity` (16) —
  маркер withdrawal; значения выше 16 клампятся на отправке, чтобы
  withdrawal-семантика пережила type narrowing.
- `seq_no` — per-(Identity, sender) wire SeqNo, синтезированный
  отправителем (Phase 1 §3.1.4). Получатели отбрасывают stale через
  существующий watermark.
- `extra` — opaque JSON, пересылается transit-узлами без изменений
  (тот же forward-compat semantic, что у legacy
  `AnnounceRouteFrame.Extra`). Хранится дословно — это гарантирует
  воспроизводимость подписи над canonical bytes после прохождения
  через узлы, не понимающие payload.
- `sig` — опциональная Ed25519-подпись над canonical bytes entry
  ключом destination-identity. Signer (`signOwnOriginV3Entries`)
  заполняет это поле только для entries с `Identity ==
  localIdentity`; всё остальное прокидывается с теми байтами, что
  изначально произвёл origin (или пустым для unsigned claims).
  **Production advertise сейчас ВЫКЛЮЧЕН** И **соответственно
  receive-side verifier в production недостижим**: `localCapabilities`
  НЕ включает `mesh_attested_links_v1` даже когда
  `EnableMeshRoutingV3` включён (потому что production emit-путь
  `route_store.AnnounceProjectionFor` итерирует bucket-map и никогда
  не эмитит local identity — synthetic self-route живёт в
  `Lookup`/`Snapshot` и не хранится, у signer'а просто нет входа), а
  negotiated cap-set — это пересечение local и remote advertised
  caps (`peer_sessions.go::intersectCapabilities`). Удалённый peer,
  advertise'ящий `mesh_attested_links_v1`, всё равно negotiate'ит
  ПУСТОЕ пересечение по этой cap против текущего local-узла, поэтому
  `peerSupportsAttestedLinks(session.address)` возвращает false и
  ветка verifier'а в `handleRouteAnnounceV3` пропускается — sig-байты
  текут через storage только как informational. Код verifier'а, поле
  storage `AttestedSigVerified` и trust-ladder `scoreSignedBonus`
  остаются wired, чтобы в тот день когда local re-enable'ит advertise
  (Phase 5 anchor publication, которая добавляет self-attestation
  entry stream Identity == localIdentity, per-emitter SeqNo, anchor
  metadata в `Extra`), пересечение стало непустым против любого
  peer'а, тоже advertise'ящего cap, и verifier engaged без
  дополнительной wiring. До тех пор единственный способ exercise
  verifier-пути — synthetic unit-test фикстуры в
  `routing_announce_v3_attest_test.go`, которые wires cap напрямую
  на test session. См. «Negotiation capability» ниже про trust
  ladder и «Канонические байты для подписи» про layout.

### Negotiation capability

`mesh_routing_v3` additive. Правило выбора на send-side:

- Обе стороны advertise `mesh_routing_v3` И `mesh_routing_v1` И
  `mesh_relay_v1` → используется `route_announce_v3`.
- Иначе fallback на `routes_update` (`mesh_routing_v2`) для delta и
  `announce_routes` (`mesh_routing_v1`) для full / first-sync —
  существующий классификатор.

`mesh_routing_v3` имеет смысл только когда `mesh_routing_v1` тоже
negotiated (зеркало v2-правила): v1 гейтит legacy-fallback, на
котором держится mixed-version flow, и peer, анонсирующий v3 без v1,
трактуется как legacy. Регистрация capability: `CapMeshRoutingV3` в
`internal/core/domain/capability.go`.

### Receive-контракт

Реализация: `handleRouteAnnounceV3` в
`internal/core/node/routing_announce.go`. Handler синтезирует
`Origin = senderIdentity` для каждого entry и прогоняет результат
через существующий `applyAnnounceEntries` (own-origin forgery guard,
transit-withdrawal authority, UpdateRoute/WithdrawRoute dispatch,
drain trigger), переиспользуя legacy-receive-путь как есть.

#### Обработка epoch

Per-peer epoch watermark живёт в `AnnouncePeerState`
(`ObserveV3Epoch` / `KnownV3Epoch`) и сбрасывается на границах
сессии (`MarkDisconnected` / `MarkReconnected`):

- `V3EpochStale` (incoming < known): replay от старого процесса
  peer'а. Весь фрейм отбрасывается; watermark не трогается.
- `V3EpochApply` (incoming == known или первый фрейм сессии):
  нормальный путь. Apply entries.
- `V3EpochReset` (incoming > known): таблица peer'а свежая, наш diff
  baseline описывал СТАРУЮ. `kind="full"` self-contained и
  ре-устанавливает baseline для нового epoch (resync не нужен).
  `kind="delta"` после reset не имеет валидного baseline для диффа и
  обрабатывается как delta-before-baseline ниже.

Замечание: после рестарта peer'а per-(Identity, Uplink) SeqNo-
watermarks НЕ сбрасываются по epoch — stale claims живут до TTL
expiry. Полный epoch-driven SeqNo reset — задокументированный Phase 4
follow-up.

#### Baseline gate

`kind="delta"` до какого-либо baseline этой сессии (или после epoch-
reset, инвалидирующего baseline) — desync: receiver отвечает
`request_resync` и дропает delta (зеркало `routes_update`-gate).
`request_resync` v2-gated, но v3-peer на практике несёт и v2
(`localCapabilities` advertise оба), поэтому fast recovery работает.
Гипотетический v3-only peer (без v2) восстанавливается на следующем
periodic `kind="full"` — без сигнала. `request_resync` — оптимизация,
не требование корректности.

### Канонические байты для подписи

Опциональное поле `sig` каждого entry покрывает детерминированный
byte-string, возвращаемый
`RouteAnnounceV3Entry.CanonicalSigningBytes()`:

```
lenpfx(identity) || lenpfx(extra)
```

где `lenpfx(x)` — 8-байтный big-endian length plus байты поля.
Длино-префиксное бинарное кодирование используется вместо JSON,
т.к. JSON не canonical (порядок ключей и whitespace зависят от
encoder) и подписи были бы невоспроизводимыми. `extra` хешируется
дословно из wire-байтов: transit форвардит без изменений, поэтому
verifier восстанавливает идентичные байты после прохождения через
intermediate-узлы.

**Hops, epoch и seq_no — все три исключены.** Все три поля меняются
per-emitter:

- `hops` инкрементируется на каждом transit hop (receiver
  convention, `+1` на ingest).
- `epoch` — локальный table-generation counter ОТПРАВЛЯЮЩЕГО узла;
  origin подписывает своим epoch, но transit-узел, форвардящий
  entry, штампует wire-frame своим (другим) epoch.
- `seq_no` на проводе синтезируется EMITTER'ом через outbound SeqNo
  helpers (`nextOutboundSeqLockedPerPeer` / `Broadcast` в
  `route_store.go`). Transit re-emit может advance wire seq_no
  выше native origin-значения (outboundMax+1 monotonicity guard),
  при этом форвардя `AttestedSig` verbatim — поэтому включение
  seq_no сделало бы подпись origin'а unverifiable после первого
  re-emit.

Включение любого из них сделало бы multi-hop transit verification
невозможной — финальный receiver, re-computing canonical bytes из
wire-frame последнего emitter'а, никогда не получит match с
подписью origin'а. Исключение всех трёх позволяет подписи
удостоверять stable properties claim'а identity (`identity`,
`extra`), оставляя
per-emitter routing / table-generation metadata на wire (carry для
receiver-side use, но не signed). Тот же подход у attestation-
протоколов вроде BGP RPKI ROAs. Защита от replay через epochs
обеспечивается per-(Identity, sender) SeqNo-монотонностью (overview
§4.3), не epoch в signed bytes; epoch на проводе по-прежнему
служит для сигнализации sender table reset receiver'у
(`ObserveV3Epoch` path).

Это byte-stable контракт, который signer/verifier
`mesh_attested_links_v1` разделяют (Phase 4 §3.2 — см. также
`docs/protocol/attested_links.md`). Валидация `sig` гейтится per-
session capability negotiation: receiver запускает ed25519-verifier
(и применяет trust-score bonus) только когда обе стороны negotiated
`mesh_attested_links_v1`; иначе bytes форвардятся, но трактуются
informational.

### Wire-размер и chunking

Send-путь применяет тот же бюджет `MaxFrameLine` (128 KiB), под
который должен лезть любой announce-plane фрейм.
`chunkRouteAnnounceV3EntriesBySize` режет entry-слайс жадно на
фреймы, каждый из которых лезет в бюджет; entries, чья собственная
сериализация превышает бюджет, репортятся через
`announce_routes_entry_oversize_dropped` и исключаются из wire-
chunks (контракт идентичен legacy-чанкеру). Правило truthful-
delivery зеркалит `SendAnnounceRoutes` / `SendRoutesUpdate`: любой
skipped entry заставляет `SendRouteAnnounceV3` вернуть `false`, чтобы
per-peer cache caller'а остался нетронутым и следующий cycle
ретраил.

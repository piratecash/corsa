# Attested Route Links (Phase 4)

## English

This document specifies the `mesh_attested_links_v1` capability and the
on-wire signature field carried by Phase 4's `route_announce_v3` frame
(see `docs/protocol/route_announce_v3.md` for the surrounding frame
contract; phase plan in
`docs/cluster-mesh/phase-4-compact-wire-signed.md` §3.2, overview §7.1).
The capability lets a destination identity attest to its own route
announcements with an Ed25519 signature so receivers can verify the
announcement without trusting any transit node along the path.

The capability ships in three substeps to keep wire-protocol changes
reviewable in isolation:

  - **13.2-A — Wire field + storage round-trip (this PR).** Defines
    `CapMeshAttestedLinksV1`. Threads the signature bytes opaquely
    through the receive path
    (`route_announce_v3.entries[].sig` → parallel sig slice →
    `RouteEntry.AttestedSig` → `UplinkClaim.AttestedSig`) and the
    emit path (`UplinkClaim.AttestedSig` → `AnnounceEntry.AttestedSig`
    → `route_announce_v3.entries[].sig`). No signing, no verification,
    no trust impact. The wire byte budget and forwarding contract are
    locked so 13.2-B / 13.2-C ship without further wire churn.
  - **13.2-B — Signer + verifier.** Adds the own-origin signer at
    emit time (Ed25519 over `RouteAnnounceV3Entry.CanonicalSigningBytes`,
    using the node's identity private key) and the receive-side
    verifier that consults the destination identity's public key via
    the knowledge store. Invalid signatures cause entry rejection;
    absent signatures stay Tier-2-permissive.
  - **13.2-C — Trust score bonus.** Wires the trust bonus
    (`scoreSignedBonus`) into `CompositeScore` so verified signed
    claims rank above unsigned ones, within-tier (the bonus never
    rescues a Bad/Questionable pair against a Good alternative —
    health-tier ordering wins). Anchor-mandatory enforcement is
    Phase 5 work, NOT part of Phase 4 13.2-C; see "Anchor-mandatory
    enforcement (Phase 5)" below for the future contract that
    requires `AttestedSigVerified=true` for entries reaching the
    receiver through `identity_record_v1` anchor publication.

### Wire field

The signature is a single optional field on each `route_announce_v3`
entry:

```
{
  "identity": "<dest_fp>",
  "hops":     <uint8>,
  "seq_no":   <uint64>,
  "extra":    {...},
  "sig":      "<base64 of Ed25519 signature, 64 raw bytes>"
}
```

Encoding is `base64.StdEncoding` (RFC 4648). `omitempty` on the
sender side keeps the JSON shape clean for Tier-2 unsigned entries —
no `sig` key appears when the local store has no signature for the
claim. A receiver that sees a malformed base64 payload treats the
slot as unsigned (debug log, no entry rejection); a verification
failure (13.2-B) will reject the entry whole, not just strip the sig.

The signed bytes are the deterministic byte string returned by
`RouteAnnounceV3Entry.CanonicalSigningBytes()` — the length-prefixed
concatenation `lenpfx(identity) || lenpfx(extra)` that the
wire-protocol contract pins in
`docs/protocol/route_announce_v3.md` ("Canonical signing bytes").

**Hops, epoch, AND seq_no are all deliberately excluded** from the
signed payload. All three fields vary per emitter:

- `hops` increments on every transit hop (receiver convention,
  `+1` on ingest), so a signature over `hops` could only be
  verified by the origin's direct peers.
- `epoch` is the SENDING node's local table-generation counter;
  the origin signs with their own epoch, but a transit node
  forwarding the entry stamps the wire frame with their own
  (different) epoch.
- `seq_no` on the wire is synthesised by the EMITTER via the
  outbound SeqNo helpers
  (`nextOutboundSeqLockedPerPeer` / `Broadcast` in
  `route_store.go`). Transit re-emit can advance the wire seq_no
  past the origin's native value (outboundMax+1 monotonicity
  guard) while forwarding `AttestedSig` verbatim, so including
  seq_no in the signed payload would make the origin's signature
  unverifiable after the first re-emit.

Multi-hop transit verification — which Phase 5 anchor publication
requires for verifiable DHT lookup — would be impossible with any
of these fields in the signed payload (a final receiver re-computing
canonical bytes from the last emitter's wire frame would never
match the origin's signature). Excluding all three lets the
signature attest to the stable properties of the identity's claim
(`identity`, `extra`) while leaving the per-emitter routing /
table-generation / sequencing metadata on the wire (carried for
receiver-side use, but not signed). This mirrors attestation
protocols such as BGP RPKI ROAs. Replay protection across epochs is
provided by per-(Identity, sender) SeqNo monotonicity (overview
§4.3), not by epoch in the signed payload.

This decision was finalised across three iterations: hops dropped
in 13.2-B, epoch dropped in the first P1 fix, seq_no dropped in
the second P1 fix — all supersede the original phase-4 draft
which had both fields in the canonical layout. The format is
byte-stable from the P1 fix onward so the signer and the verifier
(current and future versions) always agree on what was signed,
even after transit nodes have forwarded the entry through multiple
hops.

### Capability negotiation

`mesh_attested_links_v1` is additive and orthogonal to
`mesh_routing_v3`:

  - A pair that advertises BOTH negotiates signed v3 entries. The
    sender sets `sig` whenever its store has one; the verifier
    accepts/rejects per 13.2-B rules.
  - A pair that advertises `mesh_routing_v3` WITHOUT
    `mesh_attested_links_v1` on either side still uses the compact
    wire frame, but the `sig` field is treated as informational
    only — no verification, no rank penalty. This is the Tier-2
    "structural opt-in to compact frames without trust upgrade" path.
  - `mesh_attested_links_v1` is meaningful only when
    `mesh_routing_v3` is also negotiated: legacy v1/v2 wire frames
    carry no `sig` field, so the cap has no surface there.

**Production advertisement status.** The capability constant, the wire format, the storage round-trip, the verifier, and the signer are all shipped — but `localCapabilities` does NOT currently include `mesh_attested_links_v1`, even when `EnableMeshRoutingV3` is on. A Round-7 review surfaced that the production emit path (`route_store.AnnounceProjectionFor`) iterates the bucket map and never emits the local identity, because the self-route is synthesised in `Lookup` / `Snapshot` and is not stored. The signer (`signOwnOriginV3Entries`) therefore has no input to sign — every emitted v3 entry carries either a forwarded sig from another origin or no sig at all, and a node that advertised the cap would be promising a signed-announcement contract that no wire frame on its emit path actually delivers.

**Receive-side verifier is unreachable in production today.** Capabilities are negotiated as the intersection of locally-advertised and remotely-advertised sets (`peer_sessions.go::intersectCapabilities`). Because local advertise is OFF, a remote peer that advertises `mesh_attested_links_v1` still negotiates an EMPTY intersection for the cap against any current local node, so `peerSupportsAttestedLinks(session.address)` returns false and the verifier branch in `handleRouteAnnounceV3` is skipped — incoming `sig` bytes flow through storage as informational only (no `ed25519.Verify`, no entry drop on invalid, no trust-score bonus). The verifier code path therefore is exercised today ONLY by the synthetic unit-test fixtures in `routing_announce_v3_attest_test.go` that wire the cap directly onto a test session. The signer, the verifier, the `AttestedSigVerified` storage field, and the `scoreSignedBonus` trust-ladder are all kept wired so the day local re-enables the advertise — together with Phase 5 anchor publication, which adds a self-attestation entry stream (Identity == localIdentity, per-emitter SeqNo, anchor metadata in `Extra`) feeding the existing signer — the intersection becomes non-empty against any peer that also advertises the cap and the verifier engages without further wiring.

### Storage contract

The signature is stored verbatim per-`(Identity, Uplink)` claim:

  - `UplinkClaim.AttestedSig []byte` (Phase 1 pre-reserved this field
    in `internal/core/routing/uplink_claim.go`).
  - `UplinkClaim.AttestedSigVerified bool` records whether THIS
    receiver verified the signature against the destination
    identity's pubkey at ingest. Three-state ladder: `sig != nil &&
    verified == true` (highest trust, scoreSignedBonus applies),
    `sig != nil && verified == false` (sig present but pubkey
    unknown OR attested-links cap not negotiated with sender —
    bytes forwarded, no trust uplift), `sig == nil` (Tier-2
    unsigned, no trust uplift).
  - `RouteEntry.AttestedSig` / `AttestedSigVerified` carry the
    fields across the Table boundary so `Table.UpdateRoute(entry)`
    → `toUplinkClaim` preserves them, and `Lookup` /
    `AnnounceProjectionFor` / `AnnounceTo` synthesise back into
    `AnnounceEntry`.
  - `AnnounceEntry.AttestedSig` / `AttestedSigVerified` are the
    cross-package boundary the announce loop consumes;
    `buildRouteAnnounceV3Frame` encodes the sig to base64 on the
    wire.

Transit invariant: a node that re-announces a learned route on the
v3 wire emits the same `sig` bytes it received. The bytes never go
through a re-encoding hop, so a signature produced by the
destination identity remains verifiable after arbitrary transit
forwarding (the receiver reconstructs identical canonical bytes
from the entry's verbatim fields).

### Shipped behaviour

  - **Own-origin signing — DONE.** When `EnableMeshRoutingV3` is on,
    `signOwnOriginV3Entries` (in `node/routing_announce.go`)
    Ed25519-signs every own-origin entry with the local identity's
    private key before chunking. Pre-existing signatures are not
    overwritten (transit forwards verbatim).
  - **Receive-side verification — DONE.** When the sending session
    negotiated `mesh_attested_links_v1`,
    `verifyRouteAnnounceV3Sigs` runs ed25519 verification: valid
    signatures mark `AttestedSigVerified = true`;
    present-but-invalid (pubkey known, verify failed) drop the
    entry; absent or pubkey-unknown sig pass through with
    `AttestedSigVerified = false` (Tier-2 lenient). When the
    sending session did NOT negotiate the cap, the verifier is
    skipped entirely — sig bytes flow through with
    `AttestedSigVerified = false`, no entry drop on invalid.
  - **Trust-score bonus — DONE.** `CompositeScore` adds
    `scoreSignedBonus = 5.0` for claims with
    `AttestedSigVerified = true`, strictly within-tier (it never
    overrides the health/hops/source axes; documentation in
    `internal/core/routing/score.go`).
  - **Anchor-mandatory enforcement — Phase 5.** The check that
    every entry in an anchor-published `identity_record_v1` MUST
    carry a verified signature lands together with the
    `mesh_directory_v1` capability and is not part of Phase 4.

### Tests

  - `internal/core/routing/attested_sig_roundtrip_test.go` —
    `RouteEntry` ↔ `UplinkClaim` ↔ `AnnounceEntry` Sig preservation.
  - `internal/core/node/routing_announce_v3_sig_test.go` — wire
    encode/decode round-trip via `buildRouteAnnounceV3Frame` /
    `routeAnnounceV3EntriesToWire`; malformed-base64 graceful
    fallback; end-to-end storage round-trip via
    `handleRouteAnnounceV3`.

---

## Русский

Этот документ описывает capability `mesh_attested_links_v1` и
on-wire-поле подписи, передающееся в Phase 4 фрейме
`route_announce_v3` (см. `docs/protocol/route_announce_v3.md` для
контракта самого фрейма; phase-plan —
`docs/cluster-mesh/phase-4-compact-wire-signed.md` §3.2, overview
§7.1). Capability позволяет identity-назначению удостоверить
собственные route announcements Ed25519-подписью, чтобы получатели
могли верифицировать announcement без доверия к транзитным узлам.

Capability шипается тремя подшагами для изолированного ревью wire-
изменений:

  - **13.2-A — Wire-поле + storage round-trip (этот PR).** Определяет
    `CapMeshAttestedLinksV1`. Прокидывает байты подписи прозрачно
    через receive-путь
    (`route_announce_v3.entries[].sig` → параллельный sig-slice →
    `RouteEntry.AttestedSig` → `UplinkClaim.AttestedSig`) и через
    emit-путь (`UplinkClaim.AttestedSig` →
    `AnnounceEntry.AttestedSig` → `route_announce_v3.entries[].sig`).
    Без подписи, без верификации, без влияния на trust. Wire-бюджет
    и forwarding-контракт зафиксированы, чтобы 13.2-B / 13.2-C
    шипались без дополнительных wire-изменений.
  - **13.2-B — Signer + verifier.** Добавляет own-origin signer на
    emit (Ed25519 над `RouteAnnounceV3Entry.CanonicalSigningBytes`
    приватным ключом identity ноды) и receive-side verifier,
    обращающийся к knowledge-store за публичным ключом
    identity-назначения. Невалидные подписи отбрасывают entry;
    отсутствующие — Tier-2-permissive.
  - **13.2-C — Trust score bonus.** Подключает trust bonus
    (`scoreSignedBonus`) в `CompositeScore`, чтобы verified signed
    claims ранжировались выше unsigned, within-tier (бонус никогда
    не спасает Bad/Questionable пару против Good альтернативы —
    health-tier ordering побеждает). Anchor-mandatory enforcement —
    работа Phase 5, НЕ часть Phase 4 13.2-C; см. «Anchor-mandatory
    enforcement (Phase 5)» ниже для будущего контракта, требующего
    `AttestedSigVerified=true` для entries, доставленных через
    `identity_record_v1` anchor publication.

### Wire-поле

Подпись — единственное опциональное поле на каждом entry
`route_announce_v3`:

```
{
  "identity": "<dest_fp>",
  "hops":     <uint8>,
  "seq_no":   <uint64>,
  "extra":    {...},
  "sig":      "<base64 Ed25519-подписи, 64 raw байт>"
}
```

Кодировка — `base64.StdEncoding` (RFC 4648). `omitempty` на отправке
сохраняет JSON-форму чистой для Tier-2 unsigned entries — ключ `sig`
не появляется, когда у локального store нет подписи для claim.
Получатель, видящий malformed base64, обрабатывает слот как
unsigned (debug log, entry не отбрасывается); failure верификации
(13.2-B) отбрасывает entry целиком, а не просто срезает sig.

Подписанные байты — детерминированный byte-string, возвращаемый
`RouteAnnounceV3Entry.CanonicalSigningBytes()` — length-prefixed
конкатенация `lenpfx(identity) || lenpfx(extra)`, которую
фиксирует wire-протокол в
`docs/protocol/route_announce_v3.md` («Канонические байты для
подписи»).

**Hops, epoch И seq_no — все три исключены** из подписываемого
payload. Все три поля меняются per-emitter:

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

Multi-hop transit verification — необходимый для Phase 5 anchor
publication (verifiable DHT lookup) — был бы невозможен с любым из
этих полей в signed payload (финальный receiver, re-computing
canonical bytes из wire-frame последнего emitter'а, никогда не
получит match с подписью origin'а). Исключение всех трёх позволяет
подписи удостоверять stable properties claim'а identity (`identity`,
`extra`), оставляя per-emitter routing / table-generation /
sequencing metadata на wire (carry для receiver-side use, но не
signed). Тот же подход у attestation-протоколов вроде BGP RPKI
ROAs. Защита от replay через epochs обеспечивается per-(Identity,
sender) SeqNo-монотонностью (overview §4.3), не epoch в signed
payload.

Это решение финализировано в 13.2-B (hops) и refined в P1 fix
(epoch) — оба supersedes изначальный phase-4 draft, в котором оба
поля были в canonical layout. Формат byte-stable с P1 fix, чтобы
signer и verifier (текущие и future versions) всегда сходились на
том, что именно было подписано, даже после прохождения entry через
несколько transit-узлов.

### Negotiation capability

`mesh_attested_links_v1` additive и ortogонален `mesh_routing_v3`:

  - Пара, advertise обе → signed v3 entries. Отправитель ставит
    `sig` всегда, когда у store он есть; verifier accept/reject по
    правилам 13.2-B.
  - Пара, advertise `mesh_routing_v3` БЕЗ `mesh_attested_links_v1`
    у одной из сторон → используется compact wire frame, но поле
    `sig` трактуется как informational — без верификации, без rank-
    penalty. Это Tier-2 «structural opt-in к compact frames без
    trust-upgrade».
  - `mesh_attested_links_v1` имеет смысл только при negotiated
    `mesh_routing_v3`: legacy v1/v2 wire frames не несут `sig`-поле.

**Статус production advertise.** Капабилити-константа, wire-формат, storage round-trip, verifier и signer все отгружены — но `localCapabilities` сейчас НЕ включает `mesh_attested_links_v1`, даже когда `EnableMeshRoutingV3` включён. Round-7 ревью выявил, что production emit-путь (`route_store.AnnounceProjectionFor`) итерирует bucket-map и никогда не эмитит local identity, потому что self-route синтезируется в `Lookup` / `Snapshot` и не хранится. У signer'а (`signOwnOriginV3Entries`) поэтому нет входа для подписи — каждое эмитируемое v3-entry несёт либо forwarded sig другого origin'а, либо вообще без sig'а, и узел с advertise'ом cap'а пообещал бы signed-announcement-контракт, который ни один wire-кадр на его emit-пути на самом деле не доставляет.

**Receive-side verifier в production сегодня недостижим.** Capabilities negotiate'ятся как пересечение локально-advertise'нутых и удалённо-advertise'нутых наборов (`peer_sessions.go::intersectCapabilities`). Поскольку local advertise ВЫКЛЮЧЕН, удалённый peer, advertise'ящий `mesh_attested_links_v1`, всё равно negotiate'ит ПУСТОЕ пересечение по этой cap против любого текущего local-узла, поэтому `peerSupportsAttestedLinks(session.address)` возвращает false и ветка verifier'а в `handleRouteAnnounceV3` пропускается — incoming `sig`-байты текут через storage только как informational (никакого `ed25519.Verify`, никакого drop'а entry при invalid, никакого trust-score bonus). Verifier code path therefore exercised сегодня ТОЛЬКО synthetic unit-test фикстурами в `routing_announce_v3_attest_test.go`, которые wires cap напрямую на test session. Signer, verifier, storage-поле `AttestedSigVerified` и trust-ladder `scoreSignedBonus` остаются wired, чтобы в тот день когда local re-enable'ит advertise — вместе с Phase 5 anchor publication, которая добавляет self-attestation entry stream (Identity == localIdentity, per-emitter SeqNo, anchor metadata в `Extra`) питающий существующий signer — пересечение стало непустым против любого peer'а, тоже advertise'ящего cap, и verifier engaged без дополнительной wiring.

### Storage-контракт

Подпись хранится дословно per-`(Identity, Uplink)` claim:

  - `UplinkClaim.AttestedSig []byte` (Phase 1 предзарезервировал
    поле в `internal/core/routing/uplink_claim.go`).
  - `UplinkClaim.AttestedSigVerified bool` записывает, верифицировал
    ли ЭТОТ receiver подпись destination identity на ingest.
    Трёх-tier ladder: `sig != nil && verified == true` (highest
    trust, scoreSignedBonus применяется), `sig != nil && verified
    == false` (sig present, но pubkey unknown ИЛИ attested-links
    cap не negotiated с sender'ом — bytes форвардятся, без trust
    uplift), `sig == nil` (Tier-2 unsigned, без trust uplift).
  - `RouteEntry.AttestedSig` / `AttestedSigVerified` переносят поля
    через границу Table, чтобы `Table.UpdateRoute(entry)` →
    `toUplinkClaim` сохраняли их, а `Lookup` /
    `AnnounceProjectionFor` / `AnnounceTo` синтезировали обратно в
    `AnnounceEntry`.
  - `AnnounceEntry.AttestedSig` / `AttestedSigVerified` —
    cross-package boundary consume-точка announce loop;
    `buildRouteAnnounceV3Frame` кодирует sig в base64 на wire.

Transit-инвариант: узел, re-announce learned route на v3-проводе,
эмитит ТЕ ЖЕ `sig`-байты, которые получил. Байты никогда не идут
через re-encoding hop, поэтому подпись, произведённая identity-
назначением, остаётся верифицируемой после произвольного transit
forwarding (receiver реконструирует идентичные canonical bytes из
verbatim-полей entry).

### Shipped поведение

  - **Own-origin signing — DONE.** При включённом
    `EnableMeshRoutingV3` `signOwnOriginV3Entries` (в
    `node/routing_announce.go`) Ed25519-подписывает каждое
    own-origin entry приватным ключом локальной identity перед
    chunking. Pre-existing signatures не overwrite'ятся (transit
    форвардит verbatim).
  - **Receive-side verification — DONE.** Когда отправляющая сессия
    negotiated `mesh_attested_links_v1`,
    `verifyRouteAnnounceV3Sigs` запускает ed25519 verification:
    valid подписи помечают `AttestedSigVerified = true`;
    present-but-invalid (pubkey known, verify failed) дропают
    entry; absent или pubkey-unknown sig — passthrough с
    `AttestedSigVerified = false` (Tier-2 lenient). Когда сессия
    НЕ negotiated cap, verifier пропускается полностью — sig
    bytes форвардятся с `AttestedSigVerified = false`, никакого
    entry drop на invalid.
  - **Trust-score bonus — DONE.** `CompositeScore` добавляет
    `scoreSignedBonus = 5.0` для claims с
    `AttestedSigVerified = true`, строго within-tier (никогда не
    override'ит health/hops/source оси; документация в
    `internal/core/routing/score.go`).
  - **Anchor-mandatory enforcement — Phase 5.** Проверка, что
    каждое entry в anchor-published `identity_record_v1` ДОЛЖНО
    нести verified signature, приземляется вместе с
    `mesh_directory_v1` capability и не часть Phase 4.

### Тесты

  - `internal/core/routing/attested_sig_roundtrip_test.go` —
    `RouteEntry` ↔ `UplinkClaim` ↔ `AnnounceEntry` сохранение Sig.
  - `internal/core/node/routing_announce_v3_sig_test.go` — wire
    encode/decode round-trip через `buildRouteAnnounceV3Frame` /
    `routeAnnounceV3EntriesToWire`; graceful fallback на malformed
    base64; end-to-end storage round-trip через
    `handleRouteAnnounceV3`.

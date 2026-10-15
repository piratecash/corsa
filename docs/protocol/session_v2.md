# CORSA Secure Session v2

## English

### Status

**Nodes use v2.** From protocol version 31 two new nodes connect over v2, and a new node and an old one over v1, in either direction:

- **accepting:** a first byte `0x16` makes the connection v2: the handshake runs before the connection is registered, and the peer's `hello` is applied only once its proof verified. Any other first byte is a v1 connection, as before;
- **dialling** — every path that opens a connection to a node (the connection manager's long-lived sessions, the one-shot `syncPeer` recovery dial and the `sendNoticeToPeer` fallback): v2 first. A peer that answers with a v1 line (first byte `{`) is an old node: the address is marked as v1 for 24 hours and dialled again over v1. A TLS alert, other bytes, a timeout or a reset are v2 failures and never lead to v1.

### Downgrade protection

A v1 answer can be forged by whoever stands on the path, so a node remembers what proved v2 and refuses v1 for it — across restarts:

| What | Kept | Effect |
|---|---|---|
| an **identity** that proved itself over v2 (either direction) | for good | a v1 `hello` naming it is refused on accept, a v1 `welcome` naming it is refused on dial — before anything is signed; checked again at `auth_session` and right after an outbound v1 session is registered. A v1 connection naming it that was open before the proof is **closed at the proof**, and what such connections wrote into the routing table before it is forgotten (see below) |
| an **endpoint** this node dialled and reached over v2 | 30 days from the last v2 session on it (an ordinary binding) | a v1 answer from it, naming any identity, is a downgrade: the dial fails instead of falling back — until the binding expires |
| a **protected** endpoint binding | for good — never lowered or removed automatically | the same, without expiry. Only a dial with the intent "to X" that proved X, X a contact or a relation of this node, creates or raises one. **No dial path carries such an intent yet**, so no new protected binding is created today; protected bindings written by earlier builds — which protected an endpoint wherever a dial reached a contact — stay as they are |

Every v1 session refused because its identity proved v2 is counted in `fetchRouteSummary.secure_sessions.v1_refusals_identity_pinned`, on accept and on dial alike, and every v1 answer refused because its endpoint is bound to v2 in `fetchRouteSummary.secure_sessions.v1_refusals_endpoint_bound` ([rpc/routing.md](../rpc/routing.md)).

**A refusal on dial is not a failure of the address.** A v1 answer refused on dial — its `welcome` names a pinned identity, or its endpoint is bound to v2 — is this node's decision, and a man in the middle can provoke it at will by forging the answer. Charging the address for it would let him push the address out of rotation and keep v2 away from it. So the refusal counts nothing against the address: no failure, no score, no failure cooldown, no setup failure, no route quarantine, and the address is not published as disconnected. Its consequences are two backoffs of that address, both kept in memory and started over after a successful v2 session to it:

- **the hold of the fall back to v1**: 5 minutes after the first refusal, doubled after each further one, at most 6 hours. The refusal also forgets the address's v1 mark — the v1 answer behind it may have been forged by the same man in the middle — and no v1 mark is set while the hold runs, so every dial there starts with a v2 attempt; only the fall back to v1 after a v1 answer is withheld. The hold never delays a v2 attempt. Withheld fall backs are counted in `fetchRouteSummary.secure_sessions.v1_dials_held_by_refusal_backoff`;
- **the v2 retry backoff**: a v2 attempt that ended without v2 there — its v1 answer refused or its fall back held — makes the next dial wait 30 s, doubled per further such end, at most 30 min. The connection manager's own retry backoff still applies and the later of the two wins: a dial inside the v2 retry backoff is refused before any socket, and the address is not picked as a candidate meanwhile. This is pacing of the next v2 attempt, not the failure cooldown: it reads no failure count and writes nothing into the address's score, failures or setup failures, and it ends with the backoff. Dials it refused are counted in `fetchRouteSummary.secure_sessions.dials_held_by_v2_retry_backoff`.

A v1 dial that got no welcome at all — the far end hung up on the `hello`, as a v2 node that pinned this node does — also forgets the address's v1 mark, so the next dial tries v2 instead of repeating a v1 dial that cannot succeed.

The same applies when a v1 session is closed for the same reason after it was registered — its identity proved v2 between the dial and the registration, or the sweep at a proof (below) closes it: the session is withdrawn as this node's decision, nothing about it is published or charged, and the fall back to v1 at its address is held.

An ordinary binding that expired lets v1 back in on that endpoint — for an identity that is not pinned; the pin of the identity that proved v2 there still holds.

**At the moment of a proof** (`onIdentityProvenV2`, after the pin is on disk and before the v2 connection is registered) every v1 connection that names the identity is closed: such a connection proved nothing, and leaving it open would leave it a path to the identity and a voice on its routing plane — also after the identity's last v2 session closes, since the pin keeps refusing its reconnects. While an identity has a live v2 connection, a v1 connection naming it is refused too. Routing input a v1 connection wrote in the identity's name before the proof — claims, withdrawals with an arbitrary SeqNo, cooldowns, flaps, the v3 epoch — is forgotten locally at the same moment, without a wire withdrawal, unless a live v2 connection of the identity already vouches for its routes; the v2 session's connect-time full sync repopulates them (docs/protocol/network_security.md §13). An old device running under an identity that has proved v2 elsewhere is therefore cut off from this node — the same consequence the pin already had for its new connections.

A session counts only once the protection it earns is on disk: if the store cannot be written, the v2 session is not established — the peer reconnects, and the write is retried, including any earlier change that never reached the disk.

The state lives in `secure-sessions-<port>.json` next to the peers file (`CORSA_SECURE_SESSION_STORE_PATH` overrides it), written atomically and owner-only. A store file that exists but cannot be read makes every peer protected: v1 is refused, v2 keeps working, and the file is left for the operator. A protected binding is written with `protected_by` and `pool`; a protected record without them was written by an earlier build, when only a contact's binding was protected, and is read as given by its own identity in pool K. A record that contradicts itself — a pool on an ordinary binding, an unknown pool, a protected binding with only one of the two — makes the file unreadable, with the effect above. Temp files of interrupted saves older than an hour are removed when the store is loaded; younger ones may belong to a node on another port that shares the directory and is writing at that moment, and are left.

**Bounds of the endpoint bindings.** Ordinary bindings — 16 384. Protected bindings, apart from them — 32 768:

- 8 192 of the protected places are a reserve for the **contact pool (K)** only. The **relation pool (R)** never takes it, not even for a while: nothing protected is ever evicted, so a reserve place R took would never come back. R therefore holds at most 24 576; K may take any free place, so R may get fewer;
- one identity gives protection to at most 4 endpoints (`protected_by`);
- the pool is fixed when the binding is created or raised and is read back from the file, never derived again: removing a contact, or an identity moving between the classes, leaves its bindings in the pool they were counted in;
- the three bounds are checked and the binding is inserted in one step under the store's mutex, so concurrent sessions never pass a bound by even one binding.

Every dial path today is an address dial (the connection manager's candidate, the sender for `syncPeer`, a gossip target for the notice): it may reach whoever stands on the address, so it creates and renews only ordinary bindings, however the proven identity is pinned. A new owner of an endpoint takes the binding over with its tier — a protected one stays protected and stays counted for the identity that gave the protection — but never raises it: a protection asked for by a new owner is refused with `owner_changed`, which is not a bound and is not counted as one; only an identity that already was the endpoint's last proven identity raises an ordinary binding to protected.

At a bound nothing is removed or lowered: existing protected bindings and identity pins stay. The new protection is refused with a reason — `per_identity` (the identity already gives 4), `pool_r_full` (a relation, and the places R may take are taken), `store_full` (a contact, and every place is taken, the reserve included) — logged as `protected_binding_limit_reached` with the endpoint, the identity, the reason and what was recorded instead, and counted per reason in `fetchRouteSummary.secure_sessions` — once per binding: a renewal that meets the same bound again is refused again but neither logged nor counted again. The endpoint then gets an ordinary binding with its 30 days if the ordinary tier has room, and none otherwise; the session stands. The outcome is one of three, and they are never confused: protected; protection refused and an ordinary binding recorded; no binding. An operation that needs the endpoint protected fails with the typed refusal instead of passing an ordinary binding off as protected; the ordinary binding its v2 session earned is still recorded, since it records a completed mutual v2 handshake on an endpoint this node dialled. A file that holds more protected bindings than the bound is loaded whole; every new protection is refused until the count falls under the bound.

**Rolling back to an earlier build.** An earlier build reads the file — it ignores `protected_by` and `pool` and keeps `protected` — but it writes the file back without them, so after a return to this build every protected binding is read as a contact's (pool K) given by its last identity, and the earlier build bounds ordinary and protected bindings together at 16 384.

**Writes.** A change that creates, raises or re-owns a binding is written at once. A renewal of an ordinary binding is made in memory at once and written in a batch for the whole store, at most once a minute, by the node's 2-second maintenance tick, and once more when the node stops, after every session and handshake has ended: a crash loses at most a minute (plus one tick) of a binding's 30 days. A renewal of a protected binding is not written at all, since a protected binding never expires; its new last v2 time goes to disk with the next write. Concurrent saves of one state write it once.

**Size at the bounds.** Measured on a store filled to every bound with 253-character host names — the worst case: the file is ≈ 20.4 MiB (JSON, no checksum), the live structure ≈ 24 MiB, and the heap during a save ≈ 69 MiB — the live structure, a snapshot, and the file twice, because `encoding/json` builds the document in its own buffer and returns a copy. On disk a save holds the old file and one temp copy, never more: saves are serialised.

**A full pin store** — 20 000 identities. Existing protection is guaranteed and never dropped to make room; pinning a NEW identity is not promised. A v2 session of an identity that is not pinned yet, whose mandatory pin therefore cannot be stored, is **not established**: it fails with the explicit error `pin_store_full`, in either direction, and the dialler does **not** fall back to v1 in that attempt. Identities pinned earlier keep connecting as before, across reconnects and restarts. The refusal is logged (`secure_session_refused_pin_store_full`) and counted in `fetchRouteSummary.secure_sessions` ([rpc/routing.md](../rpc/routing.md)). An endpoint that gets no binding because the ordinary tier is full is logged; the session's identity pin still protects it. **The limit, stated plainly:** refusing the new v2 session does not by itself remove the v1 risk for an identity that is not pinned — until it has a pin, v1 connections naming it (an impostor's included) are not refused; the refusal only keeps an unprotected session from passing for a protected one.

**What v2 fixes.** The v1 proof signs `corsa-session-auth-v1|<challenge>|<own address>`. That names neither the verifier nor the connection, so a man in the middle M that holds a connection to X and one to B hands B's challenge to X, carries X's signature to B, and B takes M for X. Adding the verifier's identity to the signature would not help: M simply calls itself B. And even an honest v1 handshake leaves every later frame unprotected. v2 binds the identity proof to a protected channel instead.

**What v2 does not claim.** A node keeps accepting v1 from builds that do not speak v2 (legacy compatibility), so the v1 weakness remains for those connections. v2 protects a connection only when both ends use it.

### Transition from v1

The session mode of a build follows from its version constants alone (`sessionv2.ModeFor`, `config.ProtocolVersionSecureSession = 31`):

| Constants | Mode | Behaviour |
|---|---|---|
| `ProtocolVersion < 31` | `legacy_only` | v1 only — builds before v31 |
| `ProtocolVersion ≥ 31`, `MinimumProtocolVersion < 31` | `transition` — **this build** | v2 first; v1 only with a peer that answered with a v1 line and is not protected (see "Downgrade protection"); any other v2 failure does not fall back to v1 |
| `MinimumProtocolVersion ≥ 31` | `v2_only` | the listener closes what is not TLS, the dialer never probes and never goes legacy |

The downgrade to legacy is a temporary allowance of the transition. Raising `MinimumProtocolVersion` to 31 closes the relay hole for the whole network. It is closed by switching the legacy **path** off, not by checking a version number: the version in a v1 `hello` is unsigned, and a man in the middle would simply write 31 into it.

A build cannot select a mode its accept and dial paths do not implement: `TestTheConfiguredSessionModeIsWired` fails on such a version bump. `legacy_only` and `transition` are wired; `v2_only` waits for the legacy path to be removed.

#### Measuring the legacy share

When to raise `MinimumProtocolVersion` to 31 is decided by measurement, not by a date. Every node counts its authenticated sessions by kind — v2 or legacy — and by direction, served by `fetchRouteSummary` in `session_kinds` ([rpc/routing.md](../rpc/routing.md)). A session is counted once, when its peer's identity is authenticated: inbound when the accept-side handshake verifies, outbound when the ConnectionManager session is published (its setup succeeded and the pin let it through; a dialled session that fails setup, loses its slot during setup or is refused by the pin is not counted). The dialling side counts its peer sessions only, not the one-shot recovery and notice dials.

A legacy session is split by what the peer announced, which is unsigned and therefore a claim: below 31 (`hellos_below_secure_version` / `welcomes_below_secure_version`) it is an old peer; at or above 31 (`legacy_at_claimed_v2`) a secure handshake was expected and did not happen — a `corsa/2` ALPN stripped by DPI, or a man in the middle — and raising the minimum would cut it off despite the claim.

**The cumulative counts count sessions, not peers, and are gameable:** one peer opening many sessions runs them up. So `session_kinds` also carries `live`, a point-in-time gauge of the sessions live now by kind and direction with the DISTINCT identities behind each — bounded by the connection count, so a peer cannot inflate it by reconnecting. The decision rests on `live`, on `legacy_at_claimed_v2`, and on out-of-band data about which releases the network has taken — never on the cumulative counts alone. Nothing on the wire depends on any of this.

### Scheme

TLS 1.3 from the Go standard library carries the session. TLS provides the traffic keys of each direction, key confirmation (`Finished`), unique AEAD nonces and record integrity. Identity is proven separately, by an Ed25519 signature over the TLS exporter of this very connection. The TLS certificate proves nothing.

```
E    = TLS-Exporter("EXPORTER-corsa-session-v2", context = empty, 32)
P    = DOMAIN("corsa-session-v2-proof") ‖ role ‖ E      role: 0x01 dialer, 0x02 listener
DOMAIN(tag) = tag ‖ 0x00 ‖ uint16be(len(network)) ‖ network
sig  = Ed25519_identity(P)
frame: {"type":"session_proof","signature":"<86 characters of unpadded base64url>"}
```

A man in the middle runs two TLS sessions and therefore holds two different `E`. A proof made over one never verifies over the other. The network name in `DOMAIN` keeps a proof from one network valid on another. The role byte, taken from the local TLS side and never from a frame, keeps a proof from being reflected back to the end that made it.

### Handshake

```mermaid
sequenceDiagram
    participant D as D (dialer, TLS client)
    participant L as L (listener, TLS server)
    D->>L: ClientHello (ALPN corsa/2)
    L->>D: ServerHello … Certificate (cached, proves nothing) … Finished
    D->>L: Finished
    L->>D: [TLS] welcome v2
    L->>D: [TLS] session_proof (role 0x02, over E)
    Note over D: verify L, only then sign
    D->>L: [TLS] hello v2
    D->>L: [TLS] session_proof (role 0x01, over E)
    Note over L: verify D: session proven
```

*The v2 handshake: TLS 1.3, then the listener proves, then the dialer. Used by nodes from protocol version 31.*

The listener proves first. The dialer verifies it before it signs anything, so a dialer never hands a signature to a listener that has not proven itself.

The `hello` and `welcome` of v2 carry `address`, `pubkey`, `boxkey`, `boxsig` and `network`, and **no** `challenge` or `signature`. The sender fills the identity fields from its identity, never from caller input. Neither frame is signed. Their integrity comes from TLS and from the mandatory proof over `E`.

### Verification

The receiver closes the connection on the first failure:

1. the intro is the expected frame (`welcome` from the listener, `hello` from the dialer);
2. it has no v1 `challenge` or `signature`;
3. it has every identity field;
4. it is for this node's network;
5. its key passes `identity.ParsePublicKey`: wrong length, the 14 small-order encodings and a non-canonical `y` are refused. Without this check the neutral key `01 00…00` with the signature (R = neutral, S = 0) would verify for every message and forge the proof as well as the box binding;
6. the key's fingerprint equals `address`;
7. `boxkey` is bound by `boxsig` (`identity.VerifyBoxKeyBinding`);
8. the next frame is a strictly parsed `session_proof`: exactly `type` and `signature`, an 86-character unpadded base64url encoding of 64 bytes. Any other frame before the proof closes the connection;
9. the signature verifies over `P` built from **this** connection's `E` and the **peer's** role;
10. the peer is not this node;
11. when the caller dialled a named identity ("to X"), the proven identity is X. An address dial expects nobody and accepts whoever proved itself.

Nothing about the peer is applied anywhere before step 9 passes: the intro is held as a local value and handed out only inside the `ProvenSession`. `ProvenSession` has unexported fields, is returned only by `Dial` and `Accept`, and its zero value refuses every question (`ErrZeroSession`).

| Refusal | Meaning |
|---|---|
| `ErrNotV2` | not TLS 1.3 with ALPN `corsa/2` on both sides |
| `ErrIntro` | intro malformed, a field missing, wrong network, a key refused (wraps the `identity` error), the box key not bound |
| `ErrProofFrame` | the frame after the intro is not a strict `session_proof` |
| `ErrProofInvalid` | the proof does not verify for this session and this role |
| `ErrSelfConnection` | the peer is this node |
| `ErrPeerMismatch` | a dial "to X" reached somebody else |

All refusals are errors of the handshake. A v2 failure never falls back to v1 on its own.

### TLS parameters

| Parameter | Value |
|---|---|
| versions | TLS 1.3 only |
| ALPN | `corsa/2`, checked explicitly on both sides |
| server certificate | self-signed Ed25519, cached and rotated every hour, served through `GetCertificate`; never stored in `tls.Config.Certificates` |
| client certificate, SNI | none |
| verification of the server certificate | skipped by design: identity is proven by `session_proof`. Allowed in `sessionv2` only (guard test) |
| resumption, tickets, 0-RTT | off (`SessionTicketsDisabled`, no `ClientSessionCache`; Go has no 0-RTT) |
| `KeyLogWriter` | none |
| `DynamicRecordSizingDisabled` | on, so the record count below is an exact upper bound |

### Time limits

| Phase | Limit |
|---|---|
| TLS handshake complete | 5 s |
| the peer's intro and proof | 3 s |
| a cancelled context | stops the handshake at once |

### Key rotation

Go never initiates TLS `KeyUpdate`. Each end counts its own records as `writes + bytes / 16384`. At 2^24 the write is refused with `ErrRotationDue`, and the caller re-establishes the session. The count is never reset: when the peer requests `KeyUpdate`, Go also rotates our write key without telling us, so counting on is the conservative choice — at worst the reconnect comes early.

### Secrets

- **Certificate key.** It lives only in `CertificateSource`. `json.Marshal` and `json.Unmarshal` of the source fail with `identity.ErrSecretSerialization`. Every `fmt` verb, `%d` included, prints a redacted form. A guard test scans that output for the key in raw, base64, base64url, hex and decimal form.
- **Where TLS secrets may sit.** No other struct in the tree keeps a `tls.Certificate` or a `tls.Config` in a field (guard test).
- **Traffic keys and `E`.** Traffic keys stay inside `crypto/tls`. `E` is neither logged nor returned.

### Vectors

Listener seed `01…20`, dialer seed `21…40`, `E = 55…74`, network `gazeta-devnet`.

```
P_L (71 B) = 636f7273612d73657373696f6e2d76322d70726f6f6600000d67617a6574612d6465766e65740255565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071727374
sig L      = 74e805b8cf5ef320a547f1511ec1468f0694bd2a8719444db76aca4512adef006f8be1d5429b9fe913cb9d7a7e641208a9de09e3c480f19df2c8d9de831e680a
{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg"}
P_D (71 B) = 636f7273612d73657373696f6e2d76322d70726f6f6600000d67617a6574612d6465766e65740155565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071727374
sig D      = 914a3f9ae1cf2b98e50fd76ece8d65eb528a0d497b94e56deb1dd12c89cb612e9d8eff853a45fde89b34ec940b1450fac3059f8b09bf7dc29dcdae556d9a200f
{"type":"session_proof","signature":"kUo_muHPK5jlD9duzo1l61KKDUl7lOVt6x3RLInLYS6djv-FOkX96Js07JQLFFD6wwWfiwm_fcKdza5VbZogDw"}
```

Each of these must verify `false`:

- N1 — another `E`;
- N2 — the proof reflected to its maker;
- N3 — the role byte swapped;
- N4 — another key;
- N5 — a v1 `auth_session` signature presented as a proof;
- the same proof on another network.

### Tests

`internal/core/sessionv2` pins:

- **The vectors.**
- **The relay attack, both legs:**
  - M carries B's proof to X: X refuses and signs nothing;
  - M carries X's proof to B: B refuses. The same relay under v1 verifies, and the test shows it.
- **The dialer's refusals:**
  - forged small-order key;
  - a missing field;
  - v1 fields in a v2 intro;
  - another network;
  - a wrong frame type;
  - a frame before the proof;
  - reflection;
  - a proof over another exporter;
  - key-compromise impersonation;
  - self-connection;
  - an unexpected identity.
- **The transport:**
  - no ALPN on either side;
  - an altered record;
  - a replayed record;
  - the record budget;
  - the zero `ProvenSession`;
  - a cancelled handshake.
- **Guards:** TLS parameters, certificate caching and rotation, secret leakage.

---

## Русский

### Статус

**Узлы используют v2.** С версии протокола 31 два новых узла соединяются по v2, а со старым узлом — по v1, в обе стороны:

- **приём:** первый байт `0x16` — соединение v2: рукопожатие идёт до регистрации соединения, и `hello` собеседника применяется только после проверки его доказательства. Любой другой первый байт — соединение v1, как раньше;
- **дозвон** — все пути, открывающие соединение с узлом (долгоживущие сессии менеджера соединений, разовый дозвон восстановления `syncPeer`, запасной путь `sendNoticeToPeer`): сначала v2. Если собеседник ответил строкой v1 (первый байт `{`), это старый узел: адрес помечается как v1 на 24 часа, и дозвон повторяется по v1. TLS alert, другие байты, таймаут или сброс — неудачи v2, и к v1 они не ведут никогда.

### Защита от понижения

Ответ v1 может подделать тот, кто стоит на пути, поэтому узел помнит, что доказало v2, и отказывает таким в v1 — в том числе после перезапуска:

| Что | Хранится | Действие |
|---|---|---|
| **identity**, доказавшая себя по v2 (в любую сторону) | бессрочно | v1-`hello` с ней отвергается на приёме, v1-`welcome` с ней — на дозвоне, до того как что-либо подписано; повторная проверка — на `auth_session` и сразу после регистрации исходящей v1-сессии. v1-соединение с ней, открытое до доказательства, **закрывается в момент доказательства**, а то, что такие соединения записали в таблицу маршрутов раньше, забывается (см. ниже) |
| **адрес**, на котором наш дозвон получил v2 | 30 суток с последней сессии v2 на нём (обычная привязка) | ответ v1 с него, с любой identity, — попытка понижения: дозвон отказывает, а не откатывается — пока привязка не истекла |
| **защищённая** привязка адреса | бессрочно — автоматически не понижается и не удаляется | то же, без срока. Создаёт или повышает её только дозвон с намерением «к X», доказавший X, где X — контакт или отношение этого узла. **Ни один путь дозвона пока такого намерения не несёт**, поэтому новые защищённые привязки сегодня не создаются; защищённые привязки, записанные прежними сборками, — те защищали адрес везде, где дозвон доставал контакт, — остаются как есть |

Каждая сессия v1, отвергнутая потому, что её identity доказала v2, учитывается в `fetchRouteSummary.secure_sessions.v1_refusals_identity_pinned` — и на приёме, и при дозвоне, — а каждый ответ v1, отвергнутый потому, что его адрес привязан к v2, — в `fetchRouteSummary.secure_sessions.v1_refusals_endpoint_bound` ([rpc/routing.md](../rpc/routing.md)).

**Отказ на дозвоне — не неудача адреса.** Ответ v1, отвергнутый на дозвоне, — его `welcome` называет закреплённую identity, или его адрес привязан к v2, — решение этого узла, и посредник может вызвать его когда угодно, подделав ответ. Если бы за это штрафовался адрес, посредник выталкивал бы адрес из ротации и не подпускал бы к нему v2. Поэтому отказ ничего не ставит адресу в счёт: ни неудачи, ни снижения рейтинга, ни охлаждения за неудачи, ни неудачи установки сессии, ни карантина маршрутов, и адрес не публикуется как отключившийся. Следствия — два отступа этого адреса, оба в памяти, оба начинаются заново после успешной сессии v2 с ним:

- **задержка отката на v1**: 5 минут после первого отказа, удвоение после каждого следующего, не больше 6 часов. Отказ также забывает отметку v1 адреса — ответ v1, на котором она стоит, мог подделать тот же посредник, — и пока задержка действует, отметка v1 не ставится, поэтому каждый дозвон туда начинается с попытки v2; задерживается только откат на v1 после ответа v1. Попытку v2 задержка не откладывает никогда. Задержанные откаты учитываются в `fetchRouteSummary.secure_sessions.v1_dials_held_by_refusal_backoff`;
- **отступ повторов v2**: попытка v2, закончившаяся там без v2 — её ответ v1 отвергнут или откат задержан, — заставляет следующий дозвон ждать 30 с, удвоение на каждый следующий такой исход, не больше 30 мин. Собственный отступ повторов менеджера соединений действует по-прежнему, и побеждает больший из двух: дозвон внутри отступа повторов v2 отвергается до открытия сокета, а адрес на это время не выбирается кандидатом. Это темп следующей попытки v2, а не охлаждение за неудачи: он не читает счётчик неудач и ничего не пишет в рейтинг, неудачи или неудачи установки сессии адреса и кончается вместе с отступом. Отвергнутые им дозвоны учитываются в `fetchRouteSummary.secure_sessions.dials_held_by_v2_retry_backoff`.

v1-дозвон, не получивший `welcome` вовсе, — дальняя сторона оборвала соединение на `hello`, как делает v2-узел, закрепивший этот узел, — тоже забывает отметку v1 адреса, и следующий дозвон пробует v2, а не повторяет v1-дозвон, который не может удаться.

То же относится к v1-сессии, закрытой по той же причине уже после регистрации, — её identity доказала v2 между дозвоном и регистрацией, или её закрывает чистка в момент доказательства (ниже): сессия снимается как решение этого узла, ничего о ней не публикуется и никому не ставится в счёт, а откат на v1 на её адресе задерживается.

Истёкшая обычная привязка снова допускает v1 на этом адресе — для незакреплённой identity; закрепление identity, доказавшей там v2, продолжает действовать.

**В момент доказательства** (`onIdentityProvenV2`, после записи pin на диск и до регистрации v2-соединения) закрывается каждое v1-соединение, называющее эту identity: такое соединение ничего не доказало, и, оставшись открытым, оно осталось бы путём к identity и голосом в её маршрутной плоскости — и после закрытия её последней v2-сессии тоже, ведь pin продолжает отвергать его переподключения. Пока у identity есть живое v2-соединение, v1-соединение с ней тоже отвергается. Маршрутный вход, записанный v1-соединением от имени identity до доказательства, — claim-ы, отзывы с произвольным SeqNo, cooldown, flap, v3 epoch — в тот же момент забывается локально, без отзыва на проводе, если только живое v2-соединение identity уже не подтверждает её маршруты; их заново заполняет полная синхронизация v2-сессии при подключении (docs/protocol/network_security.md §13). Старое устройство под identity, доказавшей v2 в другом месте, поэтому отрезается от этого узла — то же следствие, что pin уже имел для его новых соединений.

Сессия считается установленной только после того, как заработанная ею защита записана на диск: если хранилище не записывается, сессия v2 не устанавливается — собеседник переподключается, и запись повторяется, включая любое прежнее изменение, не дошедшее до диска.

Состояние лежит в `secure-sessions-<port>.json` рядом с файлом пиров (переопределяется `CORSA_SECURE_SESSION_STORE_PATH`) и пишется атомарно, только для владельца. Файл, который есть, но не читается, делает защищёнными всех: v1 отвергается, v2 работает, а файл остаётся оператору. Защищённая привязка записывается с `protected_by` и `pool`; защищённая запись без них записана прежней сборкой, когда защищалась только привязка контакта, и читается как данная собственной identity в пуле K. Запись, противоречащая сама себе, — пул у обычной привязки, неизвестный пул, защищённая привязка только с одним из двух полей, — делает файл нечитаемым, с последствиями выше. Временные файлы прерванных сохранений старше часа удаляются при загрузке хранилища; более молодые могут принадлежать узлу на другом порту, который делит каталог и пишет прямо сейчас, и остаются.

**Пределы привязок адресов.** Обычных привязок — 16 384. Защищённых, отдельно от них, — 32 768:

- 8 192 защищённых места — резерв только для **пула контактов (K)**. **Пул отношений (R)** его не занимает, даже временно: защищённое никогда не вытесняется, и место резерва, занятое R, уже не вернулось бы. Поэтому у R не больше 24 576; K может занять любое свободное место, и тогда R достаётся меньше;
- одна identity даёт защиту не больше чем 4 адресам (`protected_by`);
- пул фиксируется при создании или повышении привязки и читается из файла, а не выводится заново: удаление контакта или переход identity между классами оставляют её привязки в том пуле, где они учтены;
- три предела проверяются и привязка вставляется одним шагом под мьютексом хранилища, поэтому конкурентные сессии не превышают ни одного предела ни на одну привязку.

Все пути дозвона сегодня адресные (кандидат менеджера соединений, отправитель трафика для `syncPeer`, цель рассылки для уведомления): такой дозвон может попасть на любого, кто стоит на адресе, поэтому создаёт и продлевает только обычные привязки, как бы ни была закреплена доказанная identity. Новый владелец адреса получает привязку вместе с её ярусом — защищённая остаётся защищённой и остаётся учтённой за identity, давшей защиту, — но никогда её не повышает: в защите, которую просит новый владелец, отказывается с `owner_changed`, это не предел и как предел не считается; обычную привязку до защищённой повышает только identity, которая уже была последней доказанной на этом адресе.

На пределе ничего не удаляется и не понижается: существующие защищённые привязки и закрепления identity сохраняются. Новой защите отказывается с причиной — `per_identity` (identity уже даёт 4), `pool_r_full` (отношение, а места, доступные R, заняты), `store_full` (контакт, а заняты все места, включая резерв), — отказ пишется в журнал как `protected_binding_limit_reached` с адресом, identity, причиной и тем, что записано вместо защиты, и считается по причинам в `fetchRouteSummary.secure_sessions` — один раз на привязку: продление, снова упирающееся в тот же предел, снова получает отказ, но повторно не пишется и не считается. Адрес тогда получает обычную привязку со сроком 30 суток, если в обычном ярусе есть место, а иначе никакой; сессия остаётся установленной. Исход — один из трёх, и они не смешиваются: защита установлена; в защите отказано, записана обычная привязка; привязки нет. Операция, которой нужен именно защищённый адрес, при отказе завершается типизированным отказом, а не выдаёт обычную привязку за защищённую; обычная привязка, заработанная её сессией v2, при этом записывается: она фиксирует завершённое взаимное рукопожатие v2 на адресе, который набрал этот узел. Файл, в котором защищённых привязок больше предела, загружается целиком; каждой новой защите отказывается, пока их число не опустится ниже предела.

**Откат на прежнюю сборку.** Прежняя сборка файл читает — `protected_by` и `pool` она пропускает, а `protected` сохраняет, — но записывает его обратно без них, поэтому после возврата на эту сборку каждая защищённая привязка читается как привязка контакта (пул K), данная её последней identity, а сама прежняя сборка ограничивает обычные и защищённые привязки вместе пределом 16 384.

**Запись.** Изменение, которое создаёт привязку, повышает её или меняет владельца, пишется сразу. Продление обычной привязки делается в памяти сразу, а пишется пакетом на всё хранилище не чаще раза в минуту — по 2-секундному служебному тику узла — и ещё раз при остановке узла, когда все сессии и рукопожатия уже завершены: авария теряет не больше минуты (плюс один тик) из 30 суток привязки. Продление защищённой привязки не пишется вовсе: защищённая привязка не истекает; её новое время последней сессии v2 уходит на диск со следующей записью. Конкурентные сохранения одного состояния пишут его один раз.

**Размер на пределах.** Измерено на хранилище, заполненном до всех пределов 253-символьными именами хостов, — худший случай: файл ≈ 20,4 МиБ (JSON, без контрольной суммы), живая структура ≈ 24 МиБ, куча во время сохранения ≈ 69 МиБ — живая структура, снимок и файл дважды, потому что `encoding/json` строит документ в собственном буфере и возвращает его копию. На диске при сохранении — старый файл и одна временная копия, не больше: сохранения упорядочены.

**Заполненное хранилище закреплений** — 20 000 identity. Сохранность существующей защиты гарантируется, ради места она не удаляется никогда; закрепление НОВОЙ identity не обещается. Сессия v2 ещё не закреплённой identity, чей обязательный pin поэтому не сохранить, **не устанавливается**: она завершается явной ошибкой `pin_store_full` в любом направлении, и дозвонившийся **не** откатывается на v1 в этой попытке. Ранее закреплённые identity подключаются как прежде — и после переподключения, и после перезапуска. Отказ пишется в журнал (`secure_session_refused_pin_store_full`) и считается в `fetchRouteSummary.secure_sessions` ([rpc/routing.md](../rpc/routing.md)). Адрес, не получивший привязки, потому что обычный ярус заполнен, отмечается в журнале; его защищает pin identity сессии. **Ограничение, прямо:** отказ новой v2-сессии сам по себе не устраняет риск v1 для незакреплённой identity — пока у неё нет pin, v1-соединения с ней (в том числе самозванца) не отвергаются; отказ лишь не выдаёт незащищённую сессию за защищённую.

**Что исправляет v2.** Доказательство v1 подписывает `corsa-session-auth-v1|<challenge>|<свой адрес>`. В нём не названы ни проверяющий, ни соединение. Поэтому посредник M с соединениями к X и к B передаёт challenge от B узлу X, несёт подпись X узлу B, и B принимает M за X. Добавить в подпись identity проверяющего не помогает: M просто назовётся B. К тому же даже честное рукопожатие v1 оставляет все последующие кадры незащищёнными. v2 вместо этого привязывает доказательство identity к защищённому каналу.

**Чего v2 не утверждает.** Узел продолжает принимать v1 от сборок, не умеющих v2 (legacy-совместимость), и для таких соединений слабость v1 остаётся. v2 защищает соединение, только когда его используют обе стороны.

### Переход с v1

Режим сессии сборки выводится только из её констант версий (`sessionv2.ModeFor`, `config.ProtocolVersionSecureSession = 31`):

| Константы | Режим | Поведение |
|---|---|---|
| `ProtocolVersion < 31` | `legacy_only` | только v1 — сборки до v31 |
| `ProtocolVersion ≥ 31`, `MinimumProtocolVersion < 31` | `transition` — **эта сборка** | сначала v2; v1 — только с собеседником, ответившим строкой v1 и не защищённым (см. «Защита от понижения»); другая неудача v2 на v1 не откатывается |
| `MinimumProtocolVersion ≥ 31` | `v2_only` | слушатель закрывает всё, что не TLS; дозвонившийся не делает пробу и не уходит в legacy |

Понижение до legacy — временное допущение переходного периода. Подъём `MinimumProtocolVersion` до 31 закрывает дыру с пересылкой для всей сети. Закрывает её выключение legacy-**пути**, а не проверка номера версии: версия в `hello` v1 не подписана, и посредник просто впишет туда 31.

Сборка не может выбрать режим, который её пути приёма и дозвона не реализуют: на таком подъёме версии падает `TestTheConfiguredSessionModeIsWired`. Реализованы `legacy_only` и `transition`; `v2_only` ждёт удаления legacy-пути.

#### Измерение доли legacy

Когда поднимать `MinimumProtocolVersion` до 31, решается по измерению, а не по календарю. Каждый узел считает свои аутентифицированные сессии по виду — v2 или legacy — и по направлению; их отдаёт `fetchRouteSummary` в разделе `session_kinds` ([rpc/routing.md](../rpc/routing.md)). Сессия учитывается один раз, когда identity пира аутентифицирована: входящая — когда проходит рукопожатие на приёме, исходящая — когда сессия менеджера соединений публикуется (её настройка прошла и pin её пропустил; исходящая сессия, чья настройка упала, чей слот вытеснен во время настройки или которую отверг pin, не учитывается). Дозванивающаяся сторона считает только свои сессии с пирами, без одноразовых recovery- и notice-дозвонов.

Legacy-сессия делится по тому, что пир объявил, а объявление не подписано и потому является заявлением: ниже 31 (`hellos_below_secure_version` / `welcomes_below_secure_version`) это старый пир; 31 и выше (`legacy_at_claimed_v2`) ожидалось защищённое рукопожатие, но его не было — ALPN `corsa/2`, срезанный DPI, или посредник, — и подъём минимума отрежет такую сессию, несмотря на заявление.

**Накопительные счётчики считают сессии, а не пиров, и накручиваются:** один пир, открывающий много сессий, их поднимает. Поэтому `session_kinds` несёт ещё и `live` — мгновенный срез живых сейчас сессий по виду и направлению с РАЗЛИЧНЫМИ identity за каждой, ограниченными числом соединений, так что переподключениями его не раздуть. Решение опирается на `live`, на `legacy_at_claimed_v2` и на внешние данные о том, какие релизы приняла сеть, — но не на накопительные счётчики сами по себе. Ничто на проводе от всего этого не зависит.

### Схема

Сессию несёт TLS 1.3 из стандартной библиотеки Go. TLS даёт ключи каждого направления, подтверждение ключей (`Finished`), уникальные AEAD-nonce и целостность записей. Identity доказывается отдельно — подписью Ed25519 над TLS-экспортёром именно этого соединения. Сертификат TLS ничего не доказывает.

```
E    = TLS-Exporter("EXPORTER-corsa-session-v2", context = пусто, 32)
P    = DOMAIN("corsa-session-v2-proof") ‖ role ‖ E      role: 0x01 дозвонившийся, 0x02 слушатель
DOMAIN(tag) = tag ‖ 0x00 ‖ uint16be(len(network)) ‖ network
sig  = Ed25519_identity(P)
кадр: {"type":"session_proof","signature":"<86 символов base64url без дополнения>"}
```

Посредник ведёт две TLS-сессии и поэтому держит два разных `E`. Доказательство над одним никогда не проверится над другим. Имя сети в `DOMAIN` не даёт доказательству одной сети действовать в другой. Байт роли берётся из локальной стороны TLS, а не из кадра, поэтому доказательство нельзя отразить обратно его автору.

### Рукопожатие

```mermaid
sequenceDiagram
    participant D as D (дозвонившийся, TLS-клиент)
    participant L as L (слушатель, TLS-сервер)
    D->>L: ClientHello (ALPN corsa/2)
    L->>D: ServerHello … Certificate (кешированный, ничего не доказывает) … Finished
    D->>L: Finished
    L->>D: [TLS] welcome v2
    L->>D: [TLS] session_proof (роль 0x02, над E)
    Note over D: проверить L, только потом подписывать
    D->>L: [TLS] hello v2
    D->>L: [TLS] session_proof (роль 0x01, над E)
    Note over L: проверить D: сессия доказана
```

*Рукопожатие v2: TLS 1.3, затем доказывает слушатель, затем дозвонившийся. Узлы используют его с версии протокола 31.*

Слушатель доказывает себя первым. Дозвонившийся проверяет его, прежде чем что-либо подписать, поэтому подпись никогда не уходит слушателю, который себя не доказал.

`hello` и `welcome` в v2 несут `address`, `pubkey`, `boxkey`, `boxsig` и `network` и **не** несут `challenge` и `signature`. Отправитель заполняет поля identity из своей identity, а не из данных вызывающего. Ни один из этих кадров не подписывается: их целостность дают TLS и обязательное доказательство над `E`.

### Проверка

Получатель закрывает соединение при первой же неудаче:

1. вступительный кадр — ожидаемый: `welcome` от слушателя, `hello` от дозвонившегося;
2. в нём нет полей v1 `challenge` и `signature`;
3. в нём есть все поля identity;
4. он для сети этого узла;
5. ключ проходит `identity.ParsePublicKey`: неверная длина, 14 кодировок малого порядка и неканонический `y` отвергаются. Без этой проверки нейтральный ключ `01 00…00` с подписью (R = нейтральный, S = 0) проходил бы проверку для любого сообщения и подделывал бы и доказательство, и привязку box-ключа;
6. отпечаток ключа равен `address`;
7. `boxkey` привязан подписью `boxsig` (`identity.VerifyBoxKeyBinding`);
8. следующий кадр — строго разобранный `session_proof`: ровно `type` и `signature`, 86 символов base64url без дополнения, 64 байта. Любой другой кадр до доказательства закрывает соединение;
9. подпись проверяется над `P` из `E` **этого** соединения и роли **собеседника**;
10. собеседник — не сам этот узел;
11. если вызывающий звонил по имени («к X»), доказана именно X. Адресный дозвон никого не ожидает и принимает того, кто себя доказал.

До прохождения шага 9 ничего о собеседнике никуда не применяется: вступительный кадр хранится локальным значением и выдаётся только внутри `ProvenSession`. У `ProvenSession` неэкспортируемые поля, её возвращают только `Dial` и `Accept`, а нулевое значение отказывает на любой вопрос (`ErrZeroSession`).

| Отказ | Смысл |
|---|---|
| `ErrNotV2` | не TLS 1.3 с ALPN `corsa/2` с обеих сторон |
| `ErrIntro` | вступительный кадр испорчен, нет поля, чужая сеть, ключ отвергнут (оборачивает ошибку `identity`), box-ключ не привязан |
| `ErrProofFrame` | кадр после вступительного — не строгий `session_proof` |
| `ErrProofInvalid` | доказательство не проверяется для этой сессии и этой роли |
| `ErrSelfConnection` | собеседник — этот же узел |
| `ErrPeerMismatch` | дозвон «к X» попал к кому-то другому |

Все отказы — ошибки рукопожатия. Неудача v2 сама по себе никогда не переходит на v1.

### Параметры TLS

| Параметр | Значение |
|---|---|
| версии | только TLS 1.3 |
| ALPN | `corsa/2`, явно проверяется обеими сторонами |
| сертификат сервера | самоподписанный Ed25519, кешируется и ротируется раз в час, отдаётся через `GetCertificate`; в `tls.Config.Certificates` не кладётся |
| клиентский сертификат, SNI | нет |
| проверка сертификата сервера | пропускается намеренно: identity доказывает `session_proof`. Разрешено только в `sessionv2` (тест-сторож) |
| возобновление, тикеты, 0-RTT | выключены (`SessionTicketsDisabled`, нет `ClientSessionCache`; в Go 0-RTT нет) |
| `KeyLogWriter` | нет |
| `DynamicRecordSizingDisabled` | включено, поэтому счёт записей ниже — точная верхняя оценка |

### Сроки

| Фаза | Срок |
|---|---|
| рукопожатие TLS завершено | 5 с |
| вступительный кадр и доказательство собеседника | 3 с |
| отменённый контекст | останавливает рукопожатие сразу |

### Ротация ключей

Go никогда не инициирует TLS `KeyUpdate`. Каждая сторона считает свои записи как `вызовы Write + байты / 16384`. На 2^24 запись отклоняется с `ErrRotationDue`, и вызывающий заново устанавливает сессию. Счётчик не сбрасывается: если собеседник запросит `KeyUpdate`, Go обновит и наш ключ записи, но не сообщит об этом. Продолжать счёт — консервативный выбор: в худшем случае переподключение произойдёт раньше нужного.

### Секреты

- **Ключ сертификата.** Живёт только в `CertificateSource`. `json.Marshal` и `json.Unmarshal` источника возвращают `identity.ErrSecretSerialization`. Любой глагол `fmt`, включая `%d`, печатает отредактированную форму. Тест-сторож ищет ключ в этом выводе в сыром виде, base64, base64url, hex и десятичной форме.
- **Где могут лежать секреты TLS.** Ни одна другая структура в дереве не хранит `tls.Certificate` или `tls.Config` в поле (тест-сторож).
- **Ключи трафика и `E`.** Ключи трафика остаются внутри `crypto/tls`. `E` не логируется и наружу не отдаётся.

### Векторы

Seed слушателя `01…20`, seed дозвонившегося `21…40`, `E = 55…74`, сеть `gazeta-devnet`.

```
P_L (71 B) = 636f7273612d73657373696f6e2d76322d70726f6f6600000d67617a6574612d6465766e65740255565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071727374
sig L      = 74e805b8cf5ef320a547f1511ec1468f0694bd2a8719444db76aca4512adef006f8be1d5429b9fe913cb9d7a7e641208a9de09e3c480f19df2c8d9de831e680a
{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg"}
P_D (71 B) = 636f7273612d73657373696f6e2d76322d70726f6f6600000d67617a6574612d6465766e65740155565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071727374
sig D      = 914a3f9ae1cf2b98e50fd76ece8d65eb528a0d497b94e56deb1dd12c89cb612e9d8eff853a45fde89b34ec940b1450fac3059f8b09bf7dc29dcdae556d9a200f
{"type":"session_proof","signature":"kUo_muHPK5jlD9duzo1l61KKDUl7lOVt6x3RLInLYS6djv-FOkX96Js07JQLFFD6wwWfiwm_fcKdza5VbZogDw"}
```


Каждый из этих случаев обязан давать `false`:

- N1 — другой `E`;
- N2 — доказательство, отражённое его автору;
- N3 — подменённый байт роли;
- N4 — чужой ключ;
- N5 — подпись v1 `auth_session` в роли доказательства;
- то же доказательство в другой сети.

### Тесты

`internal/core/sessionv2` закрепляет:

- **Векторы.**
- **Атаку пересылкой, обе ноги:**
  - M несёт доказательство B узлу X: X отказывает и ничего не подписывает;
  - M несёт доказательство X узлу B: B отказывает. Та же пересылка под v1 проходит проверку, и тест это показывает.
- **Отказы дозвонившегося:**
  - поддельный ключ малого порядка;
  - отсутствующее поле;
  - поля v1 во вступительном кадре v2;
  - чужая сеть;
  - неверный тип кадра;
  - кадр до доказательства;
  - отражение;
  - доказательство над другим экспортёром;
  - выдача себя за другого при скомпрометированном ключе;
  - самоподключение;
  - не та identity.
- **Транспорт:**
  - нет ALPN с любой стороны;
  - изменённая запись;
  - повторённая запись;
  - бюджет записей;
  - нулевая `ProvenSession`;
  - отменённое рукопожатие.
- **Сторожа:** параметры TLS, кеширование и ротация сертификата, утечка секретов.

# CORSA Secure Session v2

## English

### Status

**Nodes use v2.** From protocol version 31 two new nodes connect over v2, and a new node and an old one over v1, in either direction:

- **accepting:** a first byte `0x16` makes the connection v2: the handshake runs before the connection is registered, and the peer's `hello` is applied only once its proof verified. Any other first byte is a v1 connection, as before. A connection the listener refuses gets a TLS alert, not a silent close ("Listener refusals");
- **dialling** — every path that opens a connection to a node (the connection manager's long-lived sessions, the one-shot `syncPeer` recovery dial and the `sendNoticeToPeer` fallback): v2 first. A peer that answers with a v1 line (first byte `{`) is an old node: the address is marked as v1 for 24 hours and dialled again over v1. A TLS alert, other bytes, a timeout or a reset are v2 failures and never lead to v1.

### Downgrade protection

A v1 answer can be forged by whoever stands on the path, so a node remembers what proved v2 and refuses v1 for it — across restarts:

| What | Kept | Effect |
|---|---|---|
| an **identity** that proved itself over v2 (either direction) | for good | a v1 `hello` naming it is refused on accept, a v1 `welcome` naming it is refused on dial — before anything is signed; checked again at `auth_session` and right after an outbound v1 session is registered. A v1 connection naming it that was open before the proof is **closed at the proof**, and what such connections wrote into the routing table before it is forgotten (see below) |
| an **endpoint** this node dialled and reached over v2 | 30 days from the last v2 session on it (an ordinary binding) | a v1 answer from it, naming any identity, is a downgrade: the dial fails instead of falling back — until the binding expires |
| a **protected** endpoint binding | for good — never lowered or removed automatically | the same, without expiry. Only a dial with the intent "to X" that proved X, X a contact or a relation of this node, creates or raises one (a relation is not derived by any path yet, so today only a contact qualifies — see "Dial intent" below). **No dial path carries such an intent yet**, so no new protected binding is created today; protected bindings written by earlier builds — which protected an endpoint wherever a dial reached a contact — stay as they are. Only an operator lowers one: by a confirmed demotion of named endpoints ("Operator actions on endpoint bindings" below) |

Every v1 session refused because its identity proved v2 is counted in `fetchRouteSummary.secure_sessions.v1_refusals_identity_pinned`, on accept and on dial alike, and every v1 answer refused because its endpoint is bound to v2 in `fetchRouteSummary.secure_sessions.v1_refusals_endpoint_bound` ([rpc/routing.md](../rpc/routing.md)).

**The dialler refused on accept is told so.** A v1 `hello` naming a pinned identity — or its `auth_session`, when the identity proved v2 in between — is answered with `connection_notice{code=peer-banned, reason=peer-ban}` before the connection closes — a frame and a reason every release since protocol 28 understands ([errors.md](errors.md)). It carries no `until`, so the dialler applies its own default window (24 h on protocol 28–30 builds) and its dial gate stops offering this address — one it keeps a peer record for — until the window ends: an old dialler — or an old build still running under an identity that has proved v2 — does not turn the refusal into a storm of redials, each refused the same way. This node charges nothing for the refusal: no ban score, no peer ban, no record of the claimed identity. The dialler does pay: it does not dial this address for its whole default window, and upgrading it to a build that speaks v2 does not lift that before the window ends (this node can still dial it). `until` is left out on purpose: the dialler reads it against its own clock, and a short window is fragile when the two clocks disagree — it may expire at once or last far longer than meant — while the dialler's default is a window it already applies to such notices.

**A refusal on dial is not a failure of the address.** A v1 answer refused on dial — its `welcome` names a pinned identity, or its endpoint is bound to v2 — is this node's decision, and a man in the middle can provoke it at will by forging the answer. Charging the address for it would let him push the address out of rotation and keep v2 away from it. So the refusal counts nothing against the address: no failure, no score, no failure cooldown, no setup failure, no route quarantine, and the address is not published as disconnected. Its consequences are two backoffs of that address, both kept in memory and started over after a successful v2 session to it:

- **the hold of the fall back to v1**: 5 minutes after the first refusal, doubled after each further one, at most 6 hours. The refusal also forgets the address's v1 mark — the v1 answer behind it may have been forged by the same man in the middle — and no v1 mark is set while the hold runs, so every dial there starts with a v2 attempt; only the fall back to v1 after a v1 answer is withheld. The hold never delays a v2 attempt. Withheld fall backs are counted in `fetchRouteSummary.secure_sessions.v1_dials_held_by_refusal_backoff`;
- **the v2 retry backoff**: a v2 attempt that ended without v2 there — its v1 answer refused, its fall back held, or the listener refused it with its alert ("Listener refusals") — makes the next dial wait 30 s, doubled per further such end, at most 30 min; after a listener's refusal a random extra of up to half that wait is added, so the diallers one busy listener refused together do not come back together. The connection manager's own retry backoff still applies and the later of the two wins: a dial inside the v2 retry backoff is refused before any socket, and the address is not picked as a candidate meanwhile. This is pacing of the next v2 attempt, not the failure cooldown: it reads no failure count and writes nothing into the address's score, failures or setup failures, and it ends with the backoff. Dials it refused are counted in `fetchRouteSummary.secure_sessions.dials_held_by_v2_retry_backoff`.

A v1 dial that got no welcome at all — the far end hung up on the `hello`, as a v2 node that pinned this node does — also forgets the address's v1 mark, so the next dial tries v2 instead of repeating a v1 dial that cannot succeed.

The same applies when a v1 session is closed for the same reason after it was registered — its identity proved v2 between the dial and the registration, or the sweep at a proof (below) closes it: the session is withdrawn as this node's decision, nothing about it is published or charged, and the fall back to v1 at its address is held.

An ordinary binding that expired lets v1 back in on that endpoint — for an identity that is not pinned; the pin of the identity that proved v2 there still holds.

An ordinary binding past its term is removed by the periodic flush of the store (the 2-second maintenance tick, at most once a minute, and once more when the node stops) — not only when a full ordinary tier needs room — and also when a new binding of the same endpoint replaces it. Every removal is logged as `endpoint_binding_expired` with the endpoint, its last proven identity and its last v2 time, and is written with the next batch. A protected binding is never removed this way.

**At the moment of a proof** (`onIdentityProvenV2`, after the pin is on disk and before the v2 connection is registered) every v1 connection that names the identity is closed: such a connection proved nothing, and leaving it open would leave it a path to the identity and a voice on its routing plane — also after the identity's last v2 session closes, since the pin keeps refusing its reconnects. While an identity has a live v2 connection, a v1 connection naming it is refused too. Routing input a v1 connection wrote in the identity's name before the proof — claims, withdrawals with an arbitrary SeqNo, cooldowns, flaps, the v3 epoch — is forgotten locally at the same moment, without a wire withdrawal, unless a live v2 connection of the identity already vouches for its routes; the v2 session's connect-time full sync repopulates them (docs/protocol/network_security.md §13). An old device running under an identity that has proved v2 elsewhere is therefore cut off from this node — the same consequence the pin already had for its new connections.

A session counts only once the protection it earns is on disk: if the store cannot be written, the v2 session is not established — the peer reconnects, and the write is retried, including any earlier change that never reached the disk.

The state lives in `secure-sessions-<port>.json` next to the peers file (`CORSA_SECURE_SESSION_STORE_PATH` overrides it), written atomically and owner-only. A store file that exists but cannot be read makes every peer protected: v1 is refused, v2 keeps working, and the file is left for the operator. A protected binding is written with `protected_by` and `pool`; a protected record without them was written by an earlier build, when only a contact's binding was protected, and is read as given by its own identity in pool K. A record that contradicts itself — a pool on an ordinary binding, an unknown pool, a protected binding with only one of the two — makes the file unreadable, with the effect above. Temp files of interrupted saves older than an hour are removed when the store is loaded; younger ones may belong to a node on another port that shares the directory and is writing at that moment, and are left.

**Where the store lives must be private too.** The file is only as private as the directories above it: whoever can write into any of them can move the store's directory away and put their own in its place. When the store is loaded — once, before the node listens or dials — its location is audited:

- the file, if it exists, is not a symlink, is a regular file, and is owner-only as far as the audit can see: on POSIX owned by the node's effective user with mode `0600`, on Windows with a DACL whose every allow entry names the current user;
- on POSIX, every directory and symlink the path passes through, from the file's directory up to `/`, symlinks followed, is owned by the node's user or by root, and no directory is group- or world-writable unless it has the sticky bit. A sticky directory others may write (`/tmp`, `1777`) is accepted on the way only when the entry inside it belongs to the node's user — a data directory under `/tmp` passes, a root-owned one inside `/tmp` does not. A symlink on the way is not a violation by itself (macOS reaches every data directory through `/var → private/var`); it is judged by its owner and by the directory holding it. On Windows the ancestor directories are not judged yet, and neither is the file's owner — the owner of a file can always rewrite its DACL, so a file planted by another account with a DACL naming only this user passes.

**Limit: extended ACLs are not seen.** Only owner and mode bits are judged on POSIX. macOS (and Linux with POSIX ACLs) can grant others access through an extended ACL — read on the file, `add_file` or `delete_child` on a directory — that the mode does not show; the audit does not read ACLs, so a location that passes is safe as far as its mode bits go. On macOS the writer is affected too: a file created `0600` inherits the inheritable ACL entries of its directory, so in a directory carrying, for example, `everyone allow read,file_inherit` the store file is readable by others from the moment it is created. Keep the data directory free of inheritable ACL entries (`ls -le` shows them; `chmod -N` removes them).

A violation is logged once as a warning, `pin_store_unsafe`, with the store's file name, the component by its role (the file, the directory holding it, the directory n levels above it, or a link on the way), the violation code (`symlink`, `not_regular_file`, `file_not_owner_only`, `foreign_owner`, `writable_by_others`, `sticky_child_not_owned`), the owner and the mode — never the path — and sets `fetchRouteSummary.secure_sessions.pin_store_unsafe` ([rpc/routing.md](../rpc/routing.md)). A location that cannot be inspected is reported the same way with the code `unverifiable`. **The node keeps working**: the store loads and writes as before; refusing to start over an unsafe location is not this build's behaviour. A directory the node cannot write into at all — a root-owned `0755` data directory for a non-root node — is not an unsafe location; its first write fails with `pin_store_directory_not_writable`, naming the directory's owner and mode, and the session that needed the write is not established.

**Bounds of the endpoint bindings.** Ordinary bindings — 16 384. Protected bindings, apart from them — 32 768:

- 8 192 of the protected places are a reserve for the **contact pool (K)** only. The **relation pool (R)** never takes it, not even for a while: nothing protected is ever evicted, so a reserve place R took would never come back. R therefore holds at most 24 576; K may take any free place, so R may get fewer;
- one identity gives protection to at most 4 endpoints (`protected_by`);
- the pool is fixed when the binding is created or raised and is read back from the file, never derived again: removing a contact, or an identity moving between the classes, leaves its bindings in the pool they were counted in;
- the three bounds are checked and the binding is inserted in one step under the store's mutex, so concurrent sessions never pass a bound by even one binding.

**Endpoint keys and source groups.** A binding is keyed by the endpoint's canonical form — transport, host and port — so two spellings of one endpoint are one binding and a man in the middle cannot pick the spelling the binding does not cover. The host is parsed as an IP first and only otherwise read as a name:

| Host | Canonical form |
|---|---|
| IPv4 | decimal dotted form |
| IPv6 | RFC 5952 — lower case, zeros compressed; an IPv4-mapped address (`::ffff:a.b.c.d`) is its IPv4 form; the zone (`%eth0`) is part of the key only on a link-local or interface-local address, where the same address on another interface is another endpoint; on any other address it is dropped, or one host would have as many keys as there are strings to append |
| DNS name | lower case, no trailing dot, ASCII letters, digits, `-` and dots only; an internationalised name only in its punycode form (`xn--…`). The key is the name, not what it resolves to |
| onion | a v3 name (56 base32 characters), lower case, transport `tor` |
| i2p | a `.b32.i2p` name, lower case, transport `i2p` |
| port | decimal, 1–65535; one IP on two ports is two endpoints |

Every other host is `tcp`. A host that a resolver could read as an IPv4 address — a name whose **last** label is an IPv4 number: decimal or octal digits, or `0x` followed by hex digits only (`127.1`, `127.0.1`, `0x7f000001`, `017700000001`, `2130706433`); a `0x` label elsewhere in the name (`node.0x.org`, `0xnode.example.com`) is an ordinary name — a subdomain of an onion service (`www.<v3>.onion`), a v2 onion name, a non-ASCII name and any host longer than 253 bytes have **no canonical form**: a v2 session dialled there pins its identity and stands, but its endpoint gets no binding. Such a skip is logged as `endpoint_binding_skipped_not_canonical` with the address and the reason and counted in `fetchRouteSummary.secure_sessions.endpoint_bindings_skipped_not_canonical`.

The first three are **aliases** of a host that has a canonical form, and a binding of that host is not found under an alias without resolving it. A dial to an alias therefore **always requires v2**: a v1 answer there is refused like one from a bound endpoint. No legitimate configuration names a peer this way, so nothing that works today stops working. Any other address without a canonical form requires v2 only where the store file holds a record under that exact address (below).

Each binding keeps the **source group** it was created in:

- an IP endpoint — the group of its address: the IPv4 `/16`, the IPv6 `/32`; all of loopback (`127.0.0.0/8`, `::1`) is **one** group, since every inbound onion connection arrives from loopback and must not pass for many sources;
- a name dialled direct — the group of the address the dial actually reached; a name whose reached address is not known (a binding written before groups existed) — one shared group, `unresolved_name`;
- onion — the group `tor`, i2p — the group `i2p`.

**At most 64 new ordinary bindings per source group in any 24 hours.** A mutual v2 handshake costs an attacker with fresh keys nothing, and onion names cost nothing either; the bound keeps one block or one overlay from filling the ordinary tier. A name dial is one creation, whether it binds the name, its derived key (below) or both, and a refusal of the pair is counted once. Past the bound a new ordinary binding is not created: the session stands on its identity pin; the first refusal in a group is logged as `endpoint_binding_refused_source_group_quota` with the endpoint and the group, and every refusal is counted in `fetchRouteSummary.secure_sessions.general_refusals_source_group_quota`. Bindings that exist are never touched by it: their renewals and changes of owner go on, and a protection is not bounded by it. The quota counts creations, not live bindings, and the creation time is kept in the file, so a restart does not give a group a place back: after a restart every binding created within the day counts, a protected one too — the file does not tell one created ordinary and raised from one created protected — except a derived key created together with its name.

**Known gap: a full ordinary tier refuses instead of evicting.** The intended behaviour, once the 16 384 ordinary places are taken, is to evict the newest bindings of the most numerous source group, so that a flood displaces its own fresh bindings rather than everyone's new ones. This build does not evict: it removes only expired ordinary bindings and otherwise refuses the new one (`secure_session_endpoint_binding_refused_store_full`). The daily quota limits how fast any one group can fill the tier, but while the tier is full an honest new endpoint gets no binding either.

**The address behind a name.** When a name is dialled direct (not through the SOCKS5 proxy) and the session proves v2, the address the socket reached is bound too, as a **derived key** of the name (`derived_from`) in the same source group: the connection manager dials both forms of one host — the name from the configuration, the IP from peer exchange — and without it the IP form would be open to a downgrade after a restart. A derived key is always ordinary with the usual 30 days, even for a contact: the address behind a name is the provider's fact, not part of the identity. It is never raised — a dial "to X" on the IP form only renews it, and a protection asked for is refused with `derived_key`, which is not a bound and is not counted. Not covered: the other addresses of the name the node did not reach, the other address family, other names of the same host — each is bound when v2 is proven on it. A dial through the proxy reaches the proxy, not the peer, and derives nothing.

**The file stays format 1.** `transport`, `source_group`, `first_at` (creation time), `derived_from` and `demoted_at` (an operator's demotion, on an ordinary binding only — a protected record carrying it fails the load closed) are optional fields of a record. A record without them — written by an earlier build under the raw address it dialled — is read under the canonical key of that address, with the transport and the group of the address itself and no creation time (it counts against no group's day). Records that are spellings of one endpoint become one binding, field by field: the strongest protection — any over none, the contact pool over the relation pool, with its giver — so a merge never lowers protection or moves a contact's place into the relation pool; the identity and last v2 of the record with the latest last v2 (then the smaller stored address), the endpoint's last proven owner; the earliest creation time; the latest demotion, kept only while the merged binding is ordinary. The outcome does not depend on the order of the file, and the bounds' counts follow the merged binding. Each folded record is logged as `endpoint_binding_merged`, and the merged state is written with the next batch.

A record whose address has no canonical form is **kept as it was**, under that exact address, and logged at load as `endpoint_binding_kept_not_canonical`: dropping it would remove a protection automatically. It is for comparison only — a lookup of that exact address finds it — and is never renewed, raised, re-owned or created again. A protected one never expires and counts against the protected bounds and the contact reserve like any other; an ordinary one counts against the ordinary bound and expires 30 days after its last v2. Every write carries it back to the file. It is checked at load like any other record, except that its `transport`, which cannot be compared with the address, need only be a known one. Their number is `fetchRouteSummary.secure_sessions.non_canonical_bindings`. A record whose `transport` contradicts its address, an empty `source_group`, or a `derived_from` that is not an endpoint or sits on a protected binding makes the file unreadable, with the effect above. None of this is on the wire: bindings, keys and groups are this node's local state.

Every dial path today is an address dial (the connection manager's candidate, the sender for `syncPeer`, a gossip target for the notice): it may reach whoever stands on the address, so it creates and renews only ordinary bindings, however the proven identity is pinned. A new owner of an endpoint takes the binding over with its tier — a protected one stays protected and stays counted for the identity that gave the protection — but never raises it: a protection asked for by a new owner is refused with `owner_changed`, which is not a bound and is not counted as one; only an identity that already was the endpoint's last proven identity raises an ordinary binding to protected.

**Dial intent.** The dial carries a typed intent: an address dial, or a dial "to X" — never an empty identity standing for "nobody"; a dial path that states no intent at all is refused before any socket. Only the caller that knows X states it (sending to X, opening contact X); an endpoint binding, the address map or a pin never does, since none of them says who stands on an address. A dial "to X" differs from an address dial in two places, both on v2:

- **another identity proves itself** — the handshake expects X, so the dialler closes before it proves itself and the dial fails with `errEndpointOwnerMismatch`: that session is not a session with X. Nothing is recorded for it — no pin, no binding, the endpoint's binding (if any) stays exactly as it was — and the address is not charged: no failure, score, cooldown or setup failure, since the address may simply have changed hands. The mismatch is logged as `endpoint_owner_mismatch` with the endpoint, X, the identity proven instead and the dial's `conn_id`;
- **X proves itself** — the dial succeeds. On an endpoint last owned by another identity it takes the binding over like any dial (`endpoint_owner_changed`) without raising it; the next such dial may. When X is a contact and the endpoint either has no binding or is already X's, the binding is created or raised to protected in pool K, with X as `protected_by`, and logged as `endpoint_binding_protected` — within the bounds below; a refusal there leaves an ordinary binding. A dial "to X" for an X that is not a contact creates an ordinary binding, as an address dial does.

A v1 answer proves nothing, so on the v1 path the intent decides nothing in the transport: the identity a v1 `welcome` names is checked by whoever reads it. **No dial path makes a dial "to X" yet** — the connection manager, `syncPeer` and the notice dial all dial addresses — and a guard test keeps it so until a path is chosen, since protecting an endpoint for good is a decision visible to every node that later stands on it.

**A change of owner.** A dialled v2 session that proves an identity other than the endpoint's last one takes the binding over at once, and the change is logged as `endpoint_owner_changed` with the endpoint, the previous and the new identity, the connection (`conn_id`: on a connection-manager dial the same ID the session runs on; on a one-shot recovery or notice dial — `syncPeer`, `push_notice` — it names that dial, which registers no connection, so no other line carries it and the line is matched to the dial by the endpoint and the time) and the tier. The requirement of v2 is never weakened by it: the tier stays, a protected binding keeps its `protected_by` and `pool`, and an ordinary one's 30 days restart from this session, so its term is never shorter than before. The new owner inherits nothing of the previous one: the previous identity keeps its pin, which holds on every address; trust, routes, subscriptions and budgets are kept per identity, and the binding holds none of them; the protection stays counted for the identity that gave it, so it takes none of the new owner's quota and does not make the new owner trusted — it says only that this endpoint speaks v2. The change is one record replaced in one atomic write of the file, so a crash leaves either the previous owner or the new one, both requiring v2.

**What the journal shows, and what it does not.** `endpoint_owner_changed`, `endpoint_owner_mismatch`, `endpoint_binding_protected`, `endpoint_binding_expired`, `endpoint_binding_removed_by_operator` and `endpoint_binding_demoted_by_operator` pair an endpoint with identities — metadata about this node's neighbours. They stay in the node's local log: no RPC answer carries them, and `fetchRouteSummary.secure_sessions` reports only counts, never an endpoint or an identity. The one answer with pairs is the loopback-only `listEndpointBindings`, and it shows each endpoint's current state only; the answers of a removal and a demotion name endpoints, never an identity.

**Which records may hold a claimed identity.** A node keeps many records about other nodes by identity, and a v1 session only claims the identity it names. Each such record of the node is classified by what stands behind its identity: a v2 proof; the origin split (penalties and budgets keyed by the proven identity of a v2 session or by what the node observed about a v1 connection, never by the identity it names); an unproven claim (a v1 session's or unsigned content's — the record says how it goes when the identity proves v2, or that it does not); a signature or key fingerprint checked before the write; an identity the node named itself; or none. A guard test finds every such record — every map the node service keeps; every map in the node whose key or value is an identity or a node type carrying one, also through a named map type, a generic type instantiated with such a type, or an aliased import; and a hand-kept list of maps that key an identity by a plain string — and fails on one that is not classified, so a new record cannot be added without deciding whether it trusts a v1 claim. Outside its reach: a new map outside the node service keyed by an identity spelled as a plain string, an identity inside another package's type, containers other than maps, and the state of other components (the routing table, the datagram admission).

**The address → identity map.** The identity recorded for an address keeps its origin: proven over v2, or claimed (a v1 `hello` or `welcome`; peer exchange carries no identity). A claim never replaces what v2 recorded, and a claim of an identity that has already proved itself over v2 is not recorded at all: the check is made under the same lock as the write, so a proof that lands between a v1 path's pin check and its write does not let the claim back in. When an identity proves itself over v2, every address only a claim ties to it is forgotten — in memory, and in `peers.json` (`identity_origin` is written beside `identity`), so a restart does not bring it back — except an address v2 recorded for it or one bound to it by a v2 dial. An identity read from a `peers.json` without `identity_origin` (an earlier build) is taken as claimed.

At a bound nothing is removed or lowered: existing protected bindings and identity pins stay. The new protection is refused with a reason — `per_identity` (the identity already gives 4), `pool_r_full` (a relation, and the places R may take are taken), `store_full` (a contact, and every place is taken, the reserve included) — logged as `protected_binding_limit_reached` with the endpoint, the identity, the reason and what was recorded instead, and counted per reason in `fetchRouteSummary.secure_sessions` — once per binding: a renewal that meets the same bound again is refused again but neither logged nor counted again. The endpoint then gets an ordinary binding with its 30 days if the ordinary tier has room, and none otherwise; the session stands. The outcome is one of three, and they are never confused: protected; protection refused and an ordinary binding recorded; no binding. An operation that needs the endpoint protected fails with the typed refusal instead of passing an ordinary binding off as protected; the ordinary binding its v2 session earned is still recorded, since it records a completed mutual v2 handshake on an endpoint this node dialled. A file that holds more protected bindings than the bound is loaded whole; every new protection is refused until the count falls under the bound.

**Rolling back to an earlier build.** An earlier build reads the file — it ignores `protected_by` and `pool` and keeps `protected` — but it writes the file back without them, so after a return to this build every protected binding is read as a contact's (pool K) given by its last identity, and the earlier build bounds ordinary and protected bindings together at 16 384. It ignores `transport`, `source_group`, `first_at` and `derived_from` as well and writes the file back without them: after a return the groups come from the addresses again (a name's from the shared `unresolved_name`), the creation times are gone, so those bindings count against no group's day, and a derived key reads as an endpoint dialled by itself. It ignores `demoted_at` too: a binding demoted by an operator counts its term from its last v2 there, which for a former protected binding may already be past, so it may expire at once.

**Writes.** A change that creates, raises or re-owns a binding is written at once; the removal of an expired binding goes with the next batch. A renewal of an ordinary binding is made in memory at once and written in a batch for the whole store, at most once a minute, by the node's 2-second maintenance tick, and once more when the node stops, after every session and handshake has ended: a crash loses at most a minute (plus one tick) of a binding's 30 days. A renewal of a protected binding is not written at all, since a protected binding never expires; its new last v2 time goes to disk with the next write. Concurrent saves of one state write it once.

**The clock of the terms.** Terms of ordinary bindings, and every time the store writes, run on the **trusted time** of the process, not on the wall clock as it reads: the wall clock read at the start plus how far a clock that keeps running through sleep has moved since — `CLOCK_BOOTTIME` on Linux and Android, `CLOCK_MONOTONIC` on darwin (it runs through sleep there), `GetTickCount64` on Windows. Sleep moves both clocks alike and counts toward the 30 days in full. A wall clock that runs **more than one hour ahead** of the trusted time — a forged NTP answer, an operator's step — is a jump: the excess is not counted, the terms go on by the trusted time, and the jump is logged once as a `clock_jump_detected` warning with the store, the wall clock, the trusted time and how far ahead it ran, and counted in `fetchRouteSummary.secure_sessions.clock_jumps_detected` (`clock_jump_active` while it lasts). Without this, one forged NTP answer would end every ordinary binding at once and the endpoints would take v1 again from identities nobody pinned. Up to an hour ahead is an honest correction and is taken as it is. A wall clock set back is taken as it is too and is no jump; a renewal made meanwhile never moves `last_v2` back, so setting the clock back, renewing and returning it — a return is no jump — ends nothing early. A clock since boot that stops answering mid-run holds its last reading, so nothing ends early while it is gone; but an hour later the moving wall clock reads as a jump that lasts until the source answers again — a false `clock_jump_detected`, which fails toward terms that end later. Protected bindings and identity pins never expire and are not affected either way.

What is **not** covered:

- At the start there is nothing to hold the wall clock against, so a step made before or during a restart is taken as it is and may end ordinary bindings early — a known residue, as is a platform without a clock since boot (then `clock_jump_guarded` is false and `clock_jump_guard_unavailable_wall_clock_steps_end_ordinary_bindings_early` is logged at load).
- **A wall clock wrong at the start** — a device without a battery-backed clock that starts the node before NTP has synchronised, Δ behind. The honest correction that follows is more than an hour ahead of the trusted time, so it is a jump that never ends: `clock_jump_active` stays true for the life of the process, the process keeps running Δ behind the real time, and every time it stamps — `last_v2`, `first_at`, `demoted_at`, the pins — lags by Δ. After the next restart, on the right clock, those terms end Δ early, and at once for Δ of 30 days or more. The trusted time is not re-anchored automatically on purpose: re-anchoring would let an attacker step the clock forward and simply wait. **Diagnostic:** `clock_jump_active` that stays true long after a `clock_jump_detected` at start-up means the node started on a wrong clock — check the clock, and restart once it is right, so the next stamps are right; bindings stamped before that may end early. `last_v2` and the other times of a record found in the future at load — the clock was set back since — are brought to the load time, so a step back never stretches a term past its 30 days.

**Operator actions on endpoint bindings.** Three loopback-only RPC commands with authentication ([rpc/secure_session.md](../rpc/secure_session.md)) are the only way a binding leaves the store other than the expiry of an ordinary one. There is no bulk removal, automatic or by hand: every endpoint is named.

- `listEndpointBindings` — the live bindings a page at a time (at most 1 000), in key order, filtered by identity (the last one proven there, or the one whose protection it is) and by endpoint: the endpoint, its key form (canonical, or a record kept under an address without one), the transport, the source group, the tier — protected with its pool and `protected_by`, or general with its term — the last identity and the last v2. It is the one answer that pairs endpoints with identities, which is why it is loopback-only; it shows the current owner only, never an earlier one.
- `removeEndpointBinding` — removes one **ordinary** binding by its address, in any spelling of the endpoint. A protected binding is refused with `endpoint_binding_protected`: removal is not how a protection ends.
- `demoteEndpointBindings` — moves **selected** protected bindings to general, in two steps. Without `confirm` it is a dry run: it changes nothing and shows each endpoint, its pool, the term it would get and whether the pin of its last identity remains, the consequence — after 30 days without v2 on it, the endpoint accepts v1 again from an identity that is not pinned — and a `confirm_token`: a deterministic digest of exactly that selection with the protection each binding holds and the identity last proven on it — a guard against confirming the wrong set, not against an adversary, whom only the RPC's authentication keeps out. With the token the same selection is demoted; another selection, or the same one after a protection or an owner changed, is refused with `endpoint_binding_selection_changed`. At most 256 endpoints, each once; there is no "all" and no "all of an identity".

A demoted binding is ordinary from the confirmation on. Its term is 30 days from the later of the confirmation and the last v2 session on it — `demoted_at` in the record, because a protected binding's last v2 is not written on renewal, and a term counted from it alone could end at once — every v2 session extends it as usual, and it survives a restart. When it ends, the endpoint admits v1 again unless another reason to require v2 remains: the pin of the identity proven there keeps refusing v1 that names **that** identity, on any address. A demotion may leave the ordinary tier above its 16 384 — the bindings already held their places in the store — and new ordinary bindings are then refused until the tier falls back under its bound. Removing an identity's pin, which no command offers yet, would not remove the protected bindings it gave either.

An operator's change is written before it is made: the store as it would be after the change is written as one version of the file, and only once that write landed is the change made in memory. After a demotion whose write failed no binding is lowered, in memory or on disk. If a session changes a selected binding — its owner or its tier — while the file is written, none of the action is made in memory and the file is written again from memory in the same operation, so the disk never stays without a protection memory holds; the action answers `endpoint_binding_selection_changed`. Each removed or demoted endpoint is journalled — `endpoint_binding_removed_by_operator`; `endpoint_binding_demoted_by_operator` with the protection it lost (`previous_protected_by`, `pool`) and the new term — under the action's `operation_id`, which does not depend on any connection; a refused action is journalled as `endpoint_binding_operator_action_refused` with its reason. A node whose store file could not be read refuses all three (`endpoint_binding_store_unavailable`): nothing may be written over that file.

**Size at the bounds.** Measured on a store filled to every bound with 253-character host names, every record with its creation time and a group string of its own, every ordinary one a derived key naming another 253-character name, and the daily quota full — the worst case: the file is ≈ 28.7 MiB (JSON, no checksum), the live structure ≈ 32.8 MiB (above the ≈ 30 MiB estimated for this design), and the heap during a save ≈ 92 MiB — the live structure, a snapshot, and the file twice, because `encoding/json` builds the document in its own buffer and returns a copy. On disk a save holds the old file and one temp copy, never more: saves are serialised.

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
| a recovery dial (`syncPeer`) as a whole | 30 s: the dial (1.5 s), the TLS and proof phases above (8 s) and up to two reply drains of 10 s each |

### Listener refusals

The listener never closes a connection it refuses in silence. Its refusals before any session exists are:

- the per-IP connection rate (10 per 10 s);
- the per-IP cap on simultaneous connections (8);
- the shared connection budget (its inbound limit, total ceiling and outbound reserve);
- the blacklist;
- the inbound connection limit at registration;
- the bounds on inbound v2 handshakes below.

Each is answered with the same seven bytes, whatever the connection was going to speak:

```
15 03 03 00 02 02 50      TLS alert record: fatal, internal_error
```

- **A dialler of this build** reads exactly these seven bytes as a busy listener (any other alert stays an ordinary v2 failure). It is never a reason to fall back to v1 and sets no v1 mark, and it is not charged to the address — no failure, no score, no failure cooldown, no setup failure: the next v2 attempt there just waits for the v2 retry backoff, with jitter ("Downgrade protection").
- **An old dialler (v28–v30)** reads seven bytes without a newline and then the end of the connection. Its line reader sees EOF — what the silent close gave it. Nothing is charged on either side: the listener records no ban score, and the dialler records an ordinary failed dial and retries as before.
- No frame is written on these paths: the alert is the whole answer.

**Delivery.** Closing a socket with unread input makes the kernel send a reset, and a reset can discard the alert at the peer before it is read. So the listener:

1. writes the alert under a 100 ms write deadline;
2. half-closes (FIN);
3. reads what the peer already sent, at most 200 ms and 4 KiB — also on a socket that cannot half-close;
4. closes.

**The pool.** Alerts go out from a pool of at most 256 senders — goroutines, and the sockets they hold — of which one external source (an IPv4 address, or an IPv6 /64) holds at most 8, so one source cannot take the pool from everybody else. Loopback sources — the onion entry, many clients behind `127.0.0.1` — have no share of their own and are bounded by the pool alone. The accept loop and the connection handler take a sender without waiting. With none free for the source, the connection is closed at once without the alert: that tells the peer no less than a silent close. The accept loop never waits for a write or a drain. On shutdown the node waits for the senders in flight; each ends within its deadlines.

**Inbound v2 handshakes** are bounded in two steps, so that holding a slot costs the holder something:

1. **At the first byte `0x16`** — not at accept — a slot of the source: at most 4 at once per external source, where a source is an IPv4 address or an IPv6 /64 (one subscriber routinely holds a whole /64). The bound is not lower because many honest clients share one address behind CGNAT or a Tor exit. The whole ClientHello record must then arrive within **1 s**; a dialler that sends it slower is closed, without the alert — the deadline is a phase that ran out, not a refusal. A first record that cannot be a ClientHello — its type is not `0x16`, or its length is 0 or above 2^14 — is closed the same way, without the alert and before any slot of the node is taken: it is not TLS this listener speaks, and a refusal is no answer to it.
2. **Once the ClientHello record is whole**, a slot of the node: at most 64 at once, of which loopback sources (the onion entry, many clients behind `127.0.0.1`) hold at most 16, so that they cannot take the whole node bound from the clearnet entry.

Both slots are held until the handshake and the storing of the pin it earns are done, whatever the outcome. A socket that sends nothing holds no slot, and one that trickles its ClientHello holds only its own source's slot and only for 1 s, so a few addresses cannot close v2 to everybody. Loopback sources are not bounded per source. The per-source bound is switched off together with the other per-IP limits. A handshake over a bound gets the alert above.

A v2 connection refused at registration has already finished its TLS handshake, and a plaintext alert inside it is not something the dialler can read as a refusal: it is closed. The shared connection budget holds the same inbound limit from accept on, before the first byte, so this case stays a defence behind the budget.

The refusals are counted per bound, together with those that went out without the alert: `fetchRouteSummary`, section `listener_refusals` ([rpc/routing.md](../rpc/routing.md)).

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

- **приём:** первый байт `0x16` — соединение v2: рукопожатие идёт до регистрации соединения, и `hello` собеседника применяется только после проверки его доказательства. Любой другой первый байт — соединение v1, как раньше. Отвергнутое слушателем соединение получает TLS alert, а не молчаливое закрытие («Отказы слушателя»);
- **дозвон** — все пути, открывающие соединение с узлом (долгоживущие сессии менеджера соединений, разовый дозвон восстановления `syncPeer`, запасной путь `sendNoticeToPeer`): сначала v2. Если собеседник ответил строкой v1 (первый байт `{`), это старый узел: адрес помечается как v1 на 24 часа, и дозвон повторяется по v1. TLS alert, другие байты, таймаут или сброс — неудачи v2, и к v1 они не ведут никогда.

### Защита от понижения

Ответ v1 может подделать тот, кто стоит на пути, поэтому узел помнит, что доказало v2, и отказывает таким в v1 — в том числе после перезапуска:

| Что | Хранится | Действие |
|---|---|---|
| **identity**, доказавшая себя по v2 (в любую сторону) | бессрочно | v1-`hello` с ней отвергается на приёме, v1-`welcome` с ней — на дозвоне, до того как что-либо подписано; повторная проверка — на `auth_session` и сразу после регистрации исходящей v1-сессии. v1-соединение с ней, открытое до доказательства, **закрывается в момент доказательства**, а то, что такие соединения записали в таблицу маршрутов раньше, забывается (см. ниже) |
| **адрес**, на котором наш дозвон получил v2 | 30 суток с последней сессии v2 на нём (обычная привязка) | ответ v1 с него, с любой identity, — попытка понижения: дозвон отказывает, а не откатывается — пока привязка не истекла |
| **защищённая** привязка адреса | бессрочно — автоматически не понижается и не удаляется | то же, без срока. Создаёт или повышает её только дозвон с намерением «к X», доказавший X, где X — контакт или отношение этого узла (отношение пока не выводит ни один путь, поэтому сегодня подходит только контакт — см. «Намерение дозвона» ниже). **Ни один путь дозвона пока такого намерения не несёт**, поэтому новые защищённые привязки сегодня не создаются; защищённые привязки, записанные прежними сборками, — те защищали адрес везде, где дозвон доставал контакт, — остаются как есть. Понижает её только оператор: подтверждённым понижением названных адресов («Действия оператора над привязками адресов» ниже) |

Каждая сессия v1, отвергнутая потому, что её identity доказала v2, учитывается в `fetchRouteSummary.secure_sessions.v1_refusals_identity_pinned` — и на приёме, и при дозвоне, — а каждый ответ v1, отвергнутый потому, что его адрес привязан к v2, — в `fetchRouteSummary.secure_sessions.v1_refusals_endpoint_bound` ([rpc/routing.md](../rpc/routing.md)).

**Дозвонившемуся, отвергнутому на приёме, об этом сообщается.** На v1-`hello` с закреплённой identity — или на его `auth_session`, если identity доказала v2 в промежутке, — до закрытия соединения отправляется `connection_notice{code=peer-banned, reason=peer-ban}` — кадр и причина, которые понимает каждый выпуск начиная с протокола 28 ([errors.md](errors.md)). `until` в нём нет, поэтому дозвонившийся применяет собственное окно по умолчанию (24 ч у сборок протокола 28–30), и его гейт дозвона не предлагает этот адрес — если ведёт по нему запись пира — до конца окна: старый дозвонившийся — или старая сборка под identity, уже доказавшей v2, — не превращает отказ в шторм повторных дозвонов, каждый из которых отвергся бы так же. Этот узел за отказ ничего не ставит в счёт: ни ban score, ни бана пира, ни записи о названной identity. Платит дозвонившийся: он не дозванивается до этого адреса всё своё окно по умолчанию, и его обновление до сборки с v2 не снимает этого до конца окна (этот узел до него дозвониться может). `until` не задаётся намеренно: дозвонившийся читает его по своим часам, и короткое окно хрупко при расхождении часов — оно может истечь сразу или длиться намного дольше задуманного, — а окно по умолчанию дозвонившийся и так применяет к таким уведомлениям.

**Отказ на дозвоне — не неудача адреса.** Ответ v1, отвергнутый на дозвоне, — его `welcome` называет закреплённую identity, или его адрес привязан к v2, — решение этого узла, и посредник может вызвать его когда угодно, подделав ответ. Если бы за это штрафовался адрес, посредник выталкивал бы адрес из ротации и не подпускал бы к нему v2. Поэтому отказ ничего не ставит адресу в счёт: ни неудачи, ни снижения рейтинга, ни охлаждения за неудачи, ни неудачи установки сессии, ни карантина маршрутов, и адрес не публикуется как отключившийся. Следствия — два отступа этого адреса, оба в памяти, оба начинаются заново после успешной сессии v2 с ним:

- **задержка отката на v1**: 5 минут после первого отказа, удвоение после каждого следующего, не больше 6 часов. Отказ также забывает отметку v1 адреса — ответ v1, на котором она стоит, мог подделать тот же посредник, — и пока задержка действует, отметка v1 не ставится, поэтому каждый дозвон туда начинается с попытки v2; задерживается только откат на v1 после ответа v1. Попытку v2 задержка не откладывает никогда. Задержанные откаты учитываются в `fetchRouteSummary.secure_sessions.v1_dials_held_by_refusal_backoff`;
- **отступ повторов v2**: попытка v2, закончившаяся там без v2 — её ответ v1 отвергнут, откат задержан или слушатель отверг её своим alert («Отказы слушателя»), — заставляет следующий дозвон ждать 30 с, удвоение на каждый следующий такой исход, не больше 30 мин; после отказа слушателя добавляется случайная прибавка до половины этого ожидания, чтобы дозвонившиеся, которых один занятый слушатель отверг вместе, не вернулись вместе. Собственный отступ повторов менеджера соединений действует по-прежнему, и побеждает больший из двух: дозвон внутри отступа повторов v2 отвергается до открытия сокета, а адрес на это время не выбирается кандидатом. Это темп следующей попытки v2, а не охлаждение за неудачи: он не читает счётчик неудач и ничего не пишет в рейтинг, неудачи или неудачи установки сессии адреса и кончается вместе с отступом. Отвергнутые им дозвоны учитываются в `fetchRouteSummary.secure_sessions.dials_held_by_v2_retry_backoff`.

v1-дозвон, не получивший `welcome` вовсе, — дальняя сторона оборвала соединение на `hello`, как делает v2-узел, закрепивший этот узел, — тоже забывает отметку v1 адреса, и следующий дозвон пробует v2, а не повторяет v1-дозвон, который не может удаться.

То же относится к v1-сессии, закрытой по той же причине уже после регистрации, — её identity доказала v2 между дозвоном и регистрацией, или её закрывает чистка в момент доказательства (ниже): сессия снимается как решение этого узла, ничего о ней не публикуется и никому не ставится в счёт, а откат на v1 на её адресе задерживается.

Истёкшая обычная привязка снова допускает v1 на этом адресе — для незакреплённой identity; закрепление identity, доказавшей там v2, продолжает действовать.

Обычная привязка с истёкшим сроком удаляется периодическим сбросом хранилища (2-секундный служебный тик, не чаще раза в минуту, и ещё раз при остановке узла) — а не только когда заполненному обычному ярусу нужно место, — и ещё когда её заменяет новая привязка того же адреса. Каждое удаление пишется в журнал как `endpoint_binding_expired` с адресом, последней доказанной identity и временем последней сессии v2 и записывается на диск со следующим пакетом. Защищённая привязка так не удаляется никогда.

**В момент доказательства** (`onIdentityProvenV2`, после записи pin на диск и до регистрации v2-соединения) закрывается каждое v1-соединение, называющее эту identity: такое соединение ничего не доказало, и, оставшись открытым, оно осталось бы путём к identity и голосом в её маршрутной плоскости — и после закрытия её последней v2-сессии тоже, ведь pin продолжает отвергать его переподключения. Пока у identity есть живое v2-соединение, v1-соединение с ней тоже отвергается. Маршрутный вход, записанный v1-соединением от имени identity до доказательства, — claim-ы, отзывы с произвольным SeqNo, cooldown, flap, v3 epoch — в тот же момент забывается локально, без отзыва на проводе, если только живое v2-соединение identity уже не подтверждает её маршруты; их заново заполняет полная синхронизация v2-сессии при подключении (docs/protocol/network_security.md §13). Старое устройство под identity, доказавшей v2 в другом месте, поэтому отрезается от этого узла — то же следствие, что pin уже имел для его новых соединений.

Сессия считается установленной только после того, как заработанная ею защита записана на диск: если хранилище не записывается, сессия v2 не устанавливается — собеседник переподключается, и запись повторяется, включая любое прежнее изменение, не дошедшее до диска.

Состояние лежит в `secure-sessions-<port>.json` рядом с файлом пиров (переопределяется `CORSA_SECURE_SESSION_STORE_PATH`) и пишется атомарно, только для владельца. Файл, который есть, но не читается, делает защищёнными всех: v1 отвергается, v2 работает, а файл остаётся оператору. Защищённая привязка записывается с `protected_by` и `pool`; защищённая запись без них записана прежней сборкой, когда защищалась только привязка контакта, и читается как данная собственной identity в пуле K. Запись, противоречащая сама себе, — пул у обычной привязки, неизвестный пул, защищённая привязка только с одним из двух полей, — делает файл нечитаемым, с последствиями выше. Временные файлы прерванных сохранений старше часа удаляются при загрузке хранилища; более молодые могут принадлежать узлу на другом порту, который делит каталог и пишет прямо сейчас, и остаются.

**Место хранилища тоже должно быть закрытым.** Файл закрыт ровно настолько, насколько закрыты каталоги над ним: тот, кто может писать в любой из них, может убрать каталог хранилища и положить на его место свой. При загрузке хранилища — один раз, до слушателя и первого дозвона — его место проверяется:

- файл, если он есть, — не symlink, обычный файл и доступен только владельцу в той мере, в какой проверка это видит: на POSIX принадлежит эффективному пользователю узла и имеет режим `0600`, на Windows — DACL, где каждая разрешающая запись называет текущего пользователя;
- на POSIX каждый каталог и symlink, через которые проходит путь, от каталога файла до `/`, с переходом по ссылкам, принадлежит пользователю узла или root, и ни один каталог не доступен на запись группе или всем без sticky-бита. Sticky-каталог, куда пишут другие (`/tmp`, `1777`), допустим на пути, только если запись в нём принадлежит пользователю узла: каталог данных под `/tmp` проходит, принадлежащий root внутри `/tmp` — нет. Symlink на пути сам по себе не нарушение (macOS ведёт к любому каталогу данных через `/var → private/var`); он оценивается по владельцу и по каталогу, в котором лежит. На Windows каталоги-предки пока не проверяются, как и владелец файла: владелец всегда может переписать DACL, поэтому файл, подложенный другой учётной записью с DACL только на этого пользователя, проверку проходит.

**Ограничение: расширенные ACL не видны.** На POSIX оцениваются только владелец и биты режима. macOS (и Linux с POSIX ACL) может дать доступ другим через расширенный ACL — чтение файла, `add_file` или `delete_child` на каталоге, — которого режим не показывает; проверка ACL не читает, поэтому место, прошедшее её, безопасно лишь в части битов режима. На macOS это касается и писателя: файл, созданный с `0600`, наследует наследуемые записи ACL своего каталога, поэтому в каталоге с записью вроде `everyone allow read,file_inherit` файл хранилища читается другими с момента создания. Держите каталог данных без наследуемых записей ACL (`ls -le` их показывает, `chmod -N` снимает).

Нарушение один раз пишется в журнал предупреждением `pin_store_unsafe` с именем файла хранилища, компонентом по роли (файл, каталог файла, каталог на n уровней выше, ссылка на пути), кодом нарушения (`symlink`, `not_regular_file`, `file_not_owner_only`, `foreign_owner`, `writable_by_others`, `sticky_child_not_owned`), владельцем и режимом — без пути — и выставляет `fetchRouteSummary.secure_sessions.pin_store_unsafe` ([rpc/routing.md](../rpc/routing.md)). Место, которое не удалось проверить, сообщается так же, с кодом `unverifiable`. **Узел продолжает работать**: хранилище загружается и пишется как прежде; отказ старта из-за небезопасного места — не поведение этой сборки. Каталог, в который узел писать не может вовсе, — каталог данных root `0755` у узла не от root, — небезопасным местом не считается; его первая запись завершается ошибкой `pin_store_directory_not_writable` с владельцем и режимом каталога, и сессия, которой эта запись была нужна, не устанавливается.

**Пределы привязок адресов.** Обычных привязок — 16 384. Защищённых, отдельно от них, — 32 768:

- 8 192 защищённых места — резерв только для **пула контактов (K)**. **Пул отношений (R)** его не занимает, даже временно: защищённое никогда не вытесняется, и место резерва, занятое R, уже не вернулось бы. Поэтому у R не больше 24 576; K может занять любое свободное место, и тогда R достаётся меньше;
- одна identity даёт защиту не больше чем 4 адресам (`protected_by`);
- пул фиксируется при создании или повышении привязки и читается из файла, а не выводится заново: удаление контакта или переход identity между классами оставляют её привязки в том пуле, где они учтены;
- три предела проверяются и привязка вставляется одним шагом под мьютексом хранилища, поэтому конкурентные сессии не превышают ни одного предела ни на одну привязку.

**Ключи адресов и группы источника.** Привязка хранится по канонической форме адреса — транспорт, хост и порт, — поэтому два написания одного адреса — одна привязка, и посредник не может выбрать написание, которое привязка не покрывает. Хост сначала разбирается как IP и только иначе читается как имя:

| Хост | Каноническая форма |
|---|---|
| IPv4 | десятичная форма с точками |
| IPv6 | RFC 5952 — нижний регистр, сжатие нулей; адрес IPv4-mapped (`::ffff:a.b.c.d`) — его форма IPv4; зона (`%eth0`) входит в ключ только у адреса link-local или interface-local, где тот же адрес на другом интерфейсе — другой адрес; у любого другого адреса она отбрасывается, иначе у одного хоста было бы столько ключей, сколько строк можно дописать |
| DNS-имя | нижний регистр, без завершающей точки, только ASCII-буквы, цифры, `-` и точки; интернационализированное имя — только в punycode (`xn--…`). Ключ — имя, а не то, во что оно разрешается |
| onion | имя v3 (56 символов base32), нижний регистр, транспорт `tor` |
| i2p | имя `.b32.i2p`, нижний регистр, транспорт `i2p` |
| порт | десятичный, 1–65535; один IP на двух портах — два адреса |

Все прочие хосты — `tcp`. Хост, который резолвер может прочесть как адрес IPv4, — имя, **последняя** метка которого — число IPv4: десятичные или восьмеричные цифры либо `0x` и дальше только шестнадцатеричные цифры (`127.1`, `127.0.1`, `0x7f000001`, `017700000001`, `2130706433`); метка `0x` в другом месте имени (`node.0x.org`, `0xnode.example.com`) — обычное имя, — поддомен onion-сервиса (`www.<v3>.onion`), имя onion v2, не-ASCII имя и любой хост длиннее 253 байт **канонической формы не имеют**: сессия v2, набранная туда, закрепляет свою identity и остаётся установленной, но адрес привязки не получает. Такой пропуск пишется в журнал как `endpoint_binding_skipped_not_canonical` с адресом и причиной и считается в `fetchRouteSummary.secure_sessions.endpoint_bindings_skipped_not_canonical`.

Первые три — **псевдонимы** хоста, у которого каноническая форма есть, и привязку этого хоста по псевдониму без разрешения имени не найти. Поэтому дозвон по псевдониму **всегда требует v2**: ответ v1 там отвергается так же, как от привязанного адреса. Ни одна законная конфигурация не называет пира так, поэтому ничего работающего сегодня это не ломает. Прочие адреса без канонической формы требуют v2 только там, где в файле хранилища есть запись под этим точным адресом (ниже).

Каждая привязка хранит **группу источника**, в которой создана:

- адрес-IP — группа его адреса: `/16` для IPv4, `/32` для IPv6; весь loopback (`127.0.0.0/8`, `::1`) — **одна** группа: через loopback приходит весь входящий onion, и он не должен выдавать себя за много источников;
- имя, набранное напрямую, — группа адреса, до которого дозвон фактически дошёл; имя, чей достигнутый адрес неизвестен (привязка, записанная до появления групп), — одна общая группа `unresolved_name`;
- onion — группа `tor`, i2p — группа `i2p`.

**Не больше 64 новых обычных привязок на группу источника за любые 24 часа.** Взаимное рукопожатие v2 ничего не стоит атакующему со свежими ключами, имена onion тоже бесплатны; предел не даёт одному блоку или одной оверлейной сети заполнить обычный ярус. Дозвон по имени — одно создание, привязывает ли он имя, его производный ключ (ниже) или оба, и отказ паре считается один раз. Сверх предела новая обычная привязка не создаётся: сессия держится на pin своей identity; первый отказ в группе пишется в журнал как `endpoint_binding_refused_source_group_quota` с адресом и группой, а каждый отказ считается в `fetchRouteSummary.secure_sessions.general_refusals_source_group_quota`. Существующих привязок предел не касается: их продления и смены владельца идут как прежде, и защиту он не ограничивает. Квота считает создания, а не живые привязки, и время создания хранится в файле, поэтому перезапуск не возвращает группе места: после перезапуска учитывается каждая привязка, созданная за сутки, и защищённая тоже — файл не отличает созданную обычной и повышенную от созданной защищённой, — кроме производного ключа, созданного вместе со своим именем.

**Известное расхождение: заполненный обычный ярус отказывает, а не вытесняет.** Задуманное поведение после того, как 16 384 обычных места заняты, — вытеснять самые новые привязки самой многочисленной группы источника, чтобы поток вытеснял собственные свежие привязки, а не новые привязки всех. Эта сборка не вытесняет: она удаляет только истёкшие обычные привязки, а иначе отказывает новой (`secure_session_endpoint_binding_refused_store_full`). Суточная квота ограничивает, как быстро одна группа заполняет ярус, но пока ярус полон, честный новый адрес тоже не получает привязки.

**Адрес за именем.** Когда имя набрано напрямую (не через прокси SOCKS5) и сессия доказала v2, адрес, до которого дошёл сокет, тоже привязывается — как **производный ключ** имени (`derived_from`) в той же группе источника: менеджер соединений набирает обе формы одного хоста — имя из конфигурации, IP из обмена пирами, — и без него форма IP после перезапуска была бы открыта для понижения. Производный ключ всегда обычный, со сроком 30 суток, даже для контакта: адрес за именем — факт провайдера, а не часть identity. Он никогда не повышается: дозвон «к X» по форме IP только продлевает его, а в запрошенной защите отказывается с `derived_key` — это не предел и как предел не считается. Не покрываются: другие адреса имени, до которых узел не дошёл, другое семейство адресов, другие имена того же хоста — каждый привязывается сам, когда на нём доказан v2. Дозвон через прокси доходит до прокси, а не до собеседника, и производного ключа не даёт.

**Формат файла остаётся 1.** `transport`, `source_group`, `first_at` (время создания), `derived_from` и `demoted_at` (понижение оператором, только у обычной привязки — защищённая запись с ним закрывает загрузку отказом) — необязательные поля записи. Запись без них — записанная прежней сборкой под сырым адресом, который та набирала, — читается под каноническим ключом этого адреса, с транспортом и группой самого адреса и без времени создания (она не учитывается в сутках ни одной группы). Записи, являющиеся написаниями одного адреса, становятся одной привязкой, поле за полем: самая сильная защита — любая сильнее отсутствия, пул контактов сильнее пула отношений, вместе с её дающим, — поэтому слияние никогда не понижает защиту и не переносит место контакта в пул отношений; identity и последняя сессия v2 — от записи с самой поздней последней сессией v2 (затем с меньшим сохранённым адресом), последнего доказанного владельца адреса; самое раннее время создания; самое позднее понижение — только пока слитая привязка обычная. Исход не зависит от порядка в файле, а счётчики пределов следуют слитой привязке. Каждая влитая запись пишется в журнал как `endpoint_binding_merged`, а слитое состояние записывается со следующим пакетом.

Запись, адрес которой канонической формы не имеет, **сохраняется как была**, под этим точным адресом, и при загрузке пишется в журнал как `endpoint_binding_kept_not_canonical`: её отбрасывание было бы автоматическим снятием защиты. Она служит только для сравнения — запрос этого точного адреса её находит — и никогда не продлевается, не повышается, не меняет владельца и не создаётся заново. Защищённая не истекает и учитывается в пределах защищённых и резерве контактов, как любая другая; обычная учитывается в пределе обычных и истекает через 30 суток после последней сессии v2. Каждая запись файла несёт её обратно. При загрузке она проверяется, как любая другая запись, кроме `transport`: сравнить его с адресом нельзя, поэтому он должен быть лишь известным значением. Их число — `fetchRouteSummary.secure_sessions.non_canonical_bindings`. Запись, чей `transport` противоречит адресу, пустая `source_group` или `derived_from`, который не является адресом или стоит на защищённой привязке, делают файл нечитаемым, с последствиями выше. На провод ничего из этого не выходит: привязки, ключи и группы — локальное состояние узла.

Все пути дозвона сегодня адресные (кандидат менеджера соединений, отправитель трафика для `syncPeer`, цель рассылки для уведомления): такой дозвон может попасть на любого, кто стоит на адресе, поэтому создаёт и продлевает только обычные привязки, как бы ни была закреплена доказанная identity. Новый владелец адреса получает привязку вместе с её ярусом — защищённая остаётся защищённой и остаётся учтённой за identity, давшей защиту, — но никогда её не повышает: в защите, которую просит новый владелец, отказывается с `owner_changed`, это не предел и как предел не считается; обычную привязку до защищённой повышает только identity, которая уже была последней доказанной на этом адресе.

**Намерение дозвона.** Дозвон несёт типизированное намерение: адресный дозвон или дозвон «к X» — и никогда пустую identity в значении «никто»; путь дозвона, не заявивший намерения вовсе, отклоняется до открытия сокета. Заявляет его только вызывающий, который знает X (отправка X, открытие контакта X); привязка адреса, карта адресов и закрепление его не задают — ни одно из них не говорит, кто стоит на адресе. Дозвон «к X» отличается от адресного в двух местах, оба — на v2:

- **доказала себя другая identity** — рукопожатие ожидает X, поэтому дозвонившийся закрывает соединение до собственного доказательства, и дозвон завершается ошибкой `errEndpointOwnerMismatch`: такая сессия — не сессия с X. За неё ничего не записывается — ни закрепления, ни привязки, привязка адреса (если есть) остаётся ровно такой, какой была, — и адресу ничего не ставится в счёт: ни неудачи, ни рейтинга, ни паузы, ни setup failure, ведь адрес мог просто сменить владельца. Несовпадение пишется в журнал как `endpoint_owner_mismatch` с адресом, X, доказанной вместо неё identity и `conn_id` дозвона;
- **доказала себя X** — дозвон успешен. На адресе, последним владельцем которого была другая identity, он забирает привязку, как любой дозвон (`endpoint_owner_changed`), но не повышает её; это может сделать следующий такой дозвон. Если X — контакт, а у адреса либо нет привязки, либо она уже принадлежит X, привязка создаётся или повышается до защищённой в пуле K с `protected_by` = X и пишется в журнал как `endpoint_binding_protected` — в пределах, описанных ниже; отказ по пределу оставляет обычную привязку. Дозвон «к X», где X не контакт, создаёт обычную привязку, как адресный.

Ответ v1 ничего не доказывает, поэтому на пути v1 намерение в транспорте ничего не решает: identity, которую называет v1-`welcome`, проверяет тот, кто его читает. **Ни один путь дозвона пока не делает дозвон «к X»** — менеджер соединений, `syncPeer` и дозвон уведомления набирают адреса, — и тест-сторож держит это так, пока путь не выбран: бессрочная защита адреса — решение, видимое каждому узлу, который потом встанет на этот адрес.

**Смена владельца.** Исходящая сессия v2, доказавшая identity, отличную от последней на этом адресе, сразу забирает привязку, и смена пишется в журнал как `endpoint_owner_changed` с адресом, прежней и новой identity, соединением (`conn_id`: на дозвоне менеджера соединений — тот же ID, на котором идёт сессия; на одноразовом восстановительном дозвоне или дозвоне уведомления — `syncPeer`, `push_notice` — он называет сам дозвон, который соединения не регистрирует, поэтому другие строки его не несут и строка сопоставляется с дозвоном по адресу и времени) и ярусом. Требование v2 смена не ослабляет: ярус сохраняется, у защищённой привязки остаются её `protected_by` и `pool`, а 30 суток обычной отсчитываются заново от этой сессии, так что её срок не становится короче прежнего. Новый владелец ничего не наследует от прежнего: прежняя identity сохраняет своё закрепление, которое действует на любом адресе; доверие, маршруты, подписки и бюджеты хранятся по identity, и в привязке их нет; защита остаётся учтённой за identity, которая её дала, поэтому не тратит квоту нового владельца и не делает его доверенным — она говорит только, что на этом адресе говорят v2. Смена — замена одной записи в одной атомарной записи файла, поэтому авария оставляет либо прежнего владельца, либо нового, и оба требуют v2.

**Что видно в журнале и чего не видно.** `endpoint_owner_changed`, `endpoint_owner_mismatch`, `endpoint_binding_protected`, `endpoint_binding_expired`, `endpoint_binding_removed_by_operator` и `endpoint_binding_demoted_by_operator` связывают адрес с identity — это метаданные о соседях узла. Они остаются в локальном журнале узла: ни один ответ RPC их не несёт, а `fetchRouteSummary.secure_sessions` отдаёт только счётчики, без адресов и identity. Единственный ответ с такими парами — `listEndpointBindings`, только для loopback, и он показывает лишь текущее состояние каждого адреса; ответы снятия и понижения называют адреса, но не identity.

**Какие записи могут хранить заявленную identity.** Узел держит о других узлах много записей по identity, а сессия v1 лишь заявляет identity, которую называет. Каждая такая запись узла классифицирована по тому, что стоит за её identity: доказательство v2; разделение по происхождению (штрафы и бюджеты по доказанной identity сессии v2 или по тому, что узел сам наблюдал о соединении v1, и никогда — по названной им identity); недоказанная заявка (сессии v1 или неподписанного содержимого — у записи сказано, как она уходит, когда identity доказывает v2, или что не уходит); подпись или отпечаток ключа, проверенные до записи; identity, которую назвал сам узел; либо identity нет. Тест-сторож находит каждую такую запись — каждую map сервиса узла; каждую map узла, ключом или значением которой служит identity или тип узла, несущий её, в том числе через именованный тип map, обобщённый тип с таким аргументом или импорт под псевдонимом; и записи из ведомого вручную списка map, где identity — простая строка, — и падает на неклассифицированной, так что новую запись нельзя добавить, не решив, доверяет ли она заявке v1. Вне его досягаемости: новая map вне сервиса узла с identity простой строкой в ключе, identity внутри типа другого пакета, контейнеры, отличные от map, и состояние других компонентов (таблица маршрутизации, допуск датаграмм).

**Карта «адрес → identity».** Identity, записанная для адреса, хранит своё происхождение: доказана в v2 или заявлена (v1-`hello` или `welcome`; обмен peers identity не несёт). Заявка никогда не заменяет записанное v2, а заявка identity, уже доказавшей себя в v2, не записывается вовсе: проверка делается под тем же замком, что и запись, поэтому доказательство, попавшее между проверкой pin на пути v1 и записью, заявку не возвращает. Когда identity доказывает себя в v2, каждый адрес, который с ней связывает только заявка, забывается — в памяти и в `peers.json` (рядом с `identity` пишется `identity_origin`), поэтому рестарт его не возвращает, — кроме адреса, записанного для неё v2, и адреса, привязанного к ней дозвоном v2. Identity из `peers.json` без `identity_origin` (прежняя сборка) считается заявленной.

На пределе ничего не удаляется и не понижается: существующие защищённые привязки и закрепления identity сохраняются. Новой защите отказывается с причиной — `per_identity` (identity уже даёт 4), `pool_r_full` (отношение, а места, доступные R, заняты), `store_full` (контакт, а заняты все места, включая резерв), — отказ пишется в журнал как `protected_binding_limit_reached` с адресом, identity, причиной и тем, что записано вместо защиты, и считается по причинам в `fetchRouteSummary.secure_sessions` — один раз на привязку: продление, снова упирающееся в тот же предел, снова получает отказ, но повторно не пишется и не считается. Адрес тогда получает обычную привязку со сроком 30 суток, если в обычном ярусе есть место, а иначе никакой; сессия остаётся установленной. Исход — один из трёх, и они не смешиваются: защита установлена; в защите отказано, записана обычная привязка; привязки нет. Операция, которой нужен именно защищённый адрес, при отказе завершается типизированным отказом, а не выдаёт обычную привязку за защищённую; обычная привязка, заработанная её сессией v2, при этом записывается: она фиксирует завершённое взаимное рукопожатие v2 на адресе, который набрал этот узел. Файл, в котором защищённых привязок больше предела, загружается целиком; каждой новой защите отказывается, пока их число не опустится ниже предела.

**Откат на прежнюю сборку.** Прежняя сборка файл читает — `protected_by` и `pool` она пропускает, а `protected` сохраняет, — но записывает его обратно без них, поэтому после возврата на эту сборку каждая защищённая привязка читается как привязка контакта (пул K), данная её последней identity, а сама прежняя сборка ограничивает обычные и защищённые привязки вместе пределом 16 384. `transport`, `source_group`, `first_at` и `derived_from` она тоже пропускает и записывает файл без них: после возврата группы снова берутся из адресов (у имени — общая `unresolved_name`), времени создания нет, поэтому эти привязки не учитываются в сутках ни одной группы, а производный ключ читается как адрес, набранный сам по себе. `demoted_at` она тоже пропускает: привязка, пониженная оператором, отсчитывает там срок от последней сессии v2, которая у бывшей защищённой привязки может быть уже в прошлом, и может истечь сразу.

**Запись.** Изменение, которое создаёт привязку, повышает её или меняет владельца, пишется сразу; удаление истёкшей привязки уходит со следующим пакетом. Продление обычной привязки делается в памяти сразу, а пишется пакетом на всё хранилище не чаще раза в минуту — по 2-секундному служебному тику узла — и ещё раз при остановке узла, когда все сессии и рукопожатия уже завершены: авария теряет не больше минуты (плюс один тик) из 30 суток привязки. Продление защищённой привязки не пишется вовсе: защищённая привязка не истекает; её новое время последней сессии v2 уходит на диск со следующей записью. Конкурентные сохранения одного состояния пишут его один раз.

**Часы сроков.** Сроки обычных привязок и все времена, которые пишет хранилище, считаются по **доверенному времени** процесса, а не по стеночным часам как они есть: стеночное время на старте плюс то, на сколько с тех пор ушли часы, идущие во время сна, — `CLOCK_BOOTTIME` на Linux и Android, `CLOCK_MONOTONIC` на darwin (там он идёт во время сна), `GetTickCount64` на Windows. Сон двигает обои часы одинаково и засчитывается в 30 суток полностью. Стеночные часы, ушедшие **больше чем на час вперёд** от доверенного времени, — подделанный ответ NTP, перевод оператором — это скачок: избыток не засчитывается, сроки идут по доверенному времени, а скачок один раз пишется в журнал предупреждением `clock_jump_detected` с хранилищем, стеночным и доверенным временем и величиной ухода и учитывается в `fetchRouteSummary.secure_sessions.clock_jumps_detected` (`clock_jump_active`, пока он длится). Без этого один подделанный ответ NTP разом закончил бы все обычные привязки, и адреса снова приняли бы v1 от незакреплённых identity. Уход до часа — честная коррекция, она берётся как есть. Часы, переведённые назад, тоже берутся как есть и скачком не считаются; продление в это время никогда не сдвигает `last_v2` назад, поэтому перевод назад, продление и возврат часов — возврат скачком не считается — ничего досрочно не заканчивают. Часы с момента загрузки, переставшие отвечать посреди работы, держат последнее показание, поэтому пока их нет, ничего не кончается раньше; но через час идущие стеночные часы выглядят как скачок, который длится, пока источник снова не ответит, — ложный `clock_jump_detected`, ошибающийся в сторону более позднего конца сроков. Защищённые привязки и закрепления identity не истекают и не затрагиваются ни в одну сторону.

Что **не** покрыто:

- На старте стеночные часы не с чем сравнить, поэтому перевод до или во время рестарта берётся как есть и может досрочно закончить обычные привязки — известный остаток; то же на платформе без часов с момента загрузки (тогда `clock_jump_guarded` ложен, а при загрузке пишется `clock_jump_guard_unavailable_wall_clock_steps_end_ordinary_bindings_early`).
- **Стеночные часы, неверные на старте**, — устройство без часов с батарейкой, запустившее узел до синхронизации NTP, с отставанием Δ. Последующая честная коррекция больше чем на час впереди доверенного времени, поэтому это скачок, который не кончается: `clock_jump_active` остаётся истинным всю жизнь процесса, процесс так и идёт на Δ позади реального времени, и все времена, которые он ставит, — `last_v2`, `first_at`, `demoted_at`, закрепления — отстают на Δ. После следующего рестарта, уже на верных часах, эти сроки кончаются на Δ раньше, а при Δ от 30 суток — сразу. Доверенное время намеренно не переякоривается автоматически: переякорь позволил бы атакующему перевести часы вперёд и просто подождать. **Диагностика:** `clock_jump_active`, который долго остаётся истинным после `clock_jump_detected` на старте, означает, что узел стартовал на неверных часах, — проверьте часы и перезапустите узел, когда они верны, чтобы следующие времена были верными; привязки, отмеченные до этого, могут кончиться раньше. `last_v2` и другие времена записи, оказавшиеся при загрузке в будущем, — часы с тех пор перевели назад, — приводятся к моменту загрузки, поэтому перевод назад не растягивает срок сверх 30 суток.

**Действия оператора над привязками адресов.** Три команды RPC — только loopback, с аутентификацией ([rpc/secure_session.md](../rpc/secure_session.md)) — единственный путь, которым привязка покидает хранилище, кроме истечения обычной. Массового снятия нет — ни автоматического, ни ручного: каждый адрес называется явно.

- `listEndpointBindings` — действующие привязки постранично (не больше 1 000), в порядке ключей, с фильтром по identity (последней доказанной на адресе или той, чья это защита) и по адресу: адрес, форма ключа (канонический или запись, сохранённая под адресом без канонической формы), транспорт, группа источника, ярус — защищённый с пулом и `protected_by` или general со сроком, — последняя identity и последняя сессия v2. Это единственный ответ, который сопоставляет адреса с identity, поэтому он только для loopback; он показывает только текущего владельца, прежних — никогда.
- `removeEndpointBinding` — снимает одну **обычную** привязку по адресу, в любом написании адреса. Защищённая отклоняется с `endpoint_binding_protected`: защита заканчивается не снятием.
- `demoteEndpointBindings` — переводит **выбранные** защищённые привязки в general, в два шага. Без `confirm` это пробный прогон: ничего не меняет и показывает каждый адрес, его пул, срок, который он получит, и сохраняется ли закрепление его последней identity, последствие — через 30 суток без v2 на адресе он снова принимает v1 от незакреплённой identity — и `confirm_token`: детерминированный дайджест ровно этого набора с защитой каждой привязки и последней доказанной на ней identity — защита от подтверждения не того набора, а не от злоумышленника: его не пускает только аутентификация RPC. С токеном понижается тот же набор; другой набор или тот же после изменения защиты или владельца отклоняется с `endpoint_binding_selection_changed`. Не больше 256 адресов, каждый один раз; варианта «все» и «все по identity» нет.

Пониженная привязка обычная с момента подтверждения. Её срок — 30 суток от более позднего из подтверждения и последней сессии v2 на ней (`demoted_at` в записи: у защищённой привязки последняя сессия v2 при продлении не пишется, и срок, отсчитанный только от неё, мог бы закончиться сразу); каждая сессия v2 продлевает его как обычно, и срок переживает рестарт. Когда он заканчивается, адрес снова допускает v1, если не осталось другого основания требовать v2: закрепление доказанной там identity продолжает отклонять v1, называющий **эту** identity, на любом адресе. Понижение может оставить обычный ярус выше его 16 384 — привязки уже занимали свои места в хранилище, — и тогда новые обычные привязки отклоняются, пока ярус не опустится ниже предела. Снятие закрепления identity, которого пока не даёт ни одна команда, тоже не сняло бы данных ею защищённых привязок.

Изменение оператора записывается раньше, чем делается: хранилище в том виде, каким оно станет после изменения, пишется одной версией файла, и только после успешной записи изменение делается в памяти. После понижения, запись которого не удалась, не понижена ни одна привязка — ни в памяти, ни на диске. Если во время записи файла сессия изменила выбранную привязку — владельца или ярус, — действие в памяти не делается целиком, и файл в той же операции перезаписывается из памяти, поэтому диск не остаётся без защиты, которая есть в памяти; действие отвечает `endpoint_binding_selection_changed`. Каждый снятый или пониженный адрес пишется в журнал — `endpoint_binding_removed_by_operator`; `endpoint_binding_demoted_by_operator` с утраченной защитой (`previous_protected_by`, `pool`) и новым сроком — под `operation_id` действия, который не зависит ни от какого соединения; отклонённое действие пишется как `endpoint_binding_operator_action_refused` с причиной. Узел, файл хранилища которого не удалось прочитать, отклоняет все три (`endpoint_binding_store_unavailable`): поверх такого файла писать нельзя.

**Размер на пределах.** Измерено на хранилище, заполненном до всех пределов 253-символьными именами хостов, где у каждой записи есть время создания и собственная строка группы, каждая обычная — производный ключ с другим 253-символьным именем, а суточная квота заполнена, — худший случай: файл ≈ 28,7 МиБ (JSON, без контрольной суммы), живая структура ≈ 32,8 МиБ (больше оценки ≈ 30 МиБ для этой схемы), куча во время сохранения ≈ 92 МиБ — живая структура, снимок и файл дважды, потому что `encoding/json` строит документ в собственном буфере и возвращает его копию. На диске при сохранении — старый файл и одна временная копия, не больше: сохранения упорядочены.

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
| восстановительный дозвон (`syncPeer`) целиком | 30 с: дозвон (1,5 с), фазы TLS и доказательства выше (8 с) и до двух дренажей ответа по 10 с |

### Отказы слушателя

Слушатель никогда не закрывает отвергнутое соединение молча. Его отказы до появления сессии:

- частота соединений на IP (10 за 10 с);
- предел одновременных соединений на IP (8);
- общий бюджет соединений (его входящий лимит, общий потолок и резерв исходящих);
- чёрный список;
- входящий лимит при регистрации;
- пределы входящих рукопожатий v2 ниже.

Каждый отвечает одними и теми же семью байтами, на каком бы протоколе соединение ни собиралось говорить:

```
15 03 03 00 02 02 50      запись TLS alert: fatal, internal_error
```

- **Дозвонившийся этой сборки** читает ровно эти семь байт как занятого слушателя (любой другой alert остаётся обычной неудачей v2). К v1 это не ведёт никогда и отметку v1 не ставит, а адресу в счёт не идёт — ни неудачи, ни рейтинга, ни охлаждения за неудачи, ни неудачи установки сессии: следующая попытка v2 туда лишь ждёт отступа повторов v2 со случайной прибавкой («Защита от понижения»).
- **Старый дозвонившийся (v28–v30)** читает семь байт без перевода строки, а затем конец соединения. Его построчный ридер видит EOF — то же, что давало молчаливое закрытие. Ни одна сторона ничего не начисляет: слушатель не ставит очков бана, а дозвонившийся записывает обычный неудачный дозвон и повторяет его, как прежде.
- Ни одного фрейма на этих путях не пишется: alert — весь ответ.

**Доставка.** Если закрыть сокет с непрочитанными входящими данными, ядро шлёт сброс, а сброс может уничтожить alert у собеседника до того, как тот его прочтёт. Поэтому слушатель:

1. пишет alert с дедлайном записи 100 мс;
2. делает полузакрытие (FIN);
3. дочитывает то, что собеседник уже прислал, — не дольше 200 мс и не больше 4 КиБ, в том числе на сокете, который не умеет полузакрытие;
4. закрывает.

**Пул.** Alert отправляется из пула не более чем 256 отправителей — горутин и удерживаемых ими сокетов, — из которых один внешний источник (адрес IPv4 или /64 IPv6) держит не больше 8, чтобы один источник не отнял пул у всех остальных. У источников на loopback — onion-вход, много клиентов за `127.0.0.1` — своей доли нет, их ограничивает только пул. Цикл `accept` и обработчик соединения берут отправителя без ожидания. Если свободного для источника нет, соединение закрывается сразу без alert: собеседнику это говорит не меньше, чем молчаливое закрытие. Цикл `accept` никогда не ждёт записи или дочитки. При остановке узел дожидается отправителей в работе; каждый завершается в пределах своих дедлайнов.

**Входящие рукопожатия v2** ограничены в два шага, чтобы удержание слота чего-то стоило держащему:

1. **На первом байте `0x16`** — а не при `accept` — слот источника: не более 4 одновременно на внешний источник, где источник — адрес IPv4 или /64 IPv6 (одному абоненту обычно выдают целую /64). Предел не ниже, потому что за одним адресом CGNAT или Tor-exit много честных клиентов. Затем вся запись ClientHello должна прийти за **1 с**; дозвонившийся, который шлёт её медленнее, закрывается без alert — истёк срок фазы, это не отказ. Первая запись, которая не может быть ClientHello, — её тип не `0x16` или длина 0 либо больше 2^14, — закрывается так же, без alert и до того, как занят какой-либо слот узла: это не тот TLS, на котором говорит слушатель, и отказ на неё не ответ.
2. **Когда запись ClientHello целиком получена**, — слот узла: не более 64 одновременно, из которых источники на loopback (onion-вход, много клиентов за `127.0.0.1`) держат не больше 16, чтобы не отнять весь предел узла у clearnet-входа.

Оба слота держатся до конца рукопожатия и сохранения заработанного им pin, при любом исходе. Сокет, который ничего не шлёт, слота не занимает, а тот, что тянет ClientHello по байту, держит только слот своего источника и только 1 с, поэтому несколько адресов не закроют v2 для всех. Источники на loopback пределом на источник не ограничены. Предел на источник выключается вместе с остальными ограничениями по IP. Рукопожатие сверх предела получает alert выше.

Соединение v2, отвергнутое при регистрации, уже завершило рукопожатие TLS, и открытый alert внутри него дозвонившийся не прочтёт как отказ: оно закрывается. Общий бюджет соединений держит тот же входящий лимит начиная с `accept`, до первого байта, поэтому этот случай остаётся защитой за бюджетом.

Отказы считаются по ограничениям, вместе с ушедшими без alert: `fetchRouteSummary`, раздел `listener_refusals` ([rpc/routing.md](../rpc/routing.md)).

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

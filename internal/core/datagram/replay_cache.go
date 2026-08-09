package datagram

import (
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
)

// replay_cache.go holds the vocabulary of the layer's ONE memory: who an
// arrival is billed to, the handle of one reservation, and the outcome of each
// operation. The memory itself is BaseReplayCache (replay_base.go), which every
// caller addresses by its concrete type.
//
// It is ANTI-REPLAY and nothing else — RAM for the freshness window, not
// storage. The custody half — StoreIfAbsent, the lease cycle, Requeue,
// ReleasePayload — and the durable half — ReadPending, SettleRecovery,
// ProbeWritable — are gone rather than optional: a datagram belongs to the two
// ENDS of its path, so a memory that could take custody of one was a memory
// every relay on the way had to refuse in prose.
//
// Reference: docs/protocol/datagram.md §4.1.
//
// # No half-broken anti-replay
//
// The cache either REMEMBERS a key or it does not. There is no third answer,
// because there is no I/O to be uncertain about: every operation is arithmetic
// over a map, a heap and a per-owner list under one mutex, and it returns having
// either applied or refused. That is why none of the outcomes below has a
// `failed` variant and why the sentinels name only deterministic refusals.
//
// The durable-era vocabulary this replaced — an ambiguous write resolved by a
// read-back, a quarantine for the ambiguity that could not be resolved — is
// gone with the thing that produced it, and it cannot come back through a seam:
// there is no interface here to implement, so the only value the pipeline and
// the node can be handed is *BaseReplayCache, and a disk-backed or blocking
// memory is a compile error rather than a review comment.
//
// What the cache CAN still refuse is capacity, and that refusal is
// deterministic and observable: Reserve answers `rejected`
// (ErrReplayCacheCapacity) and the cache's own counters separate a refused
// neighbour from an evicted one (§5).
//
// # Optional values
//
// Outcomes are explicit variant types with a Kind/Outcome discriminator and
// payload accessors that return (value, ok). No outcome is encoded as a bool
// pair or a bare error, because "reserved" and "duplicate" and "rejected"
// are three different instructions to the caller, and `error != nil` cannot
// carry three.

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	// ErrStaleReservation marks an operation addressed by a reservation
	// token that is no longer the current one for its replay key. Release
	// treats it as a no-op (that is the whole point of the token, see the
	// ABA note on ReservationToken); every other operation reports it as
	// "definitely not applied".
	ErrStaleReservation = errors.New("datagram: stale reservation token")

	// ErrUnknownReplayKey marks an operation on a replay key the cache has
	// no record of.
	ErrUnknownReplayKey = errors.New("datagram: unknown replay key")

	// ErrReplayCacheCapacity marks a deterministic capacity or quota refusal.
	ErrReplayCacheCapacity = errors.New("datagram: replay cache capacity exhausted")
)

// ---------------------------------------------------------------------------
// Peers
// ---------------------------------------------------------------------------

// IngressPeer is where a frame entered this node from: a CHANNEL, what this node
// has been shown about the neighbour on the other end of it, and the BUDGET that
// arrival was billed to — or the node itself for a locally originated datagram
// (§4.1 — a local frame runs the same Reserve/Commit/Release cycle with
// incoming_peer = local).
//
// "Local" is a named state rather than a zero PeerIdentity: the zero value
// here means "not set", and a cache that quotas by neighbour must not merge
// every local frame into one anonymous bucket by accident.
//
// THE CHANNEL, THE IDENTITY AND THE OWNER ARE THREE FACTS, and the whole shape of
// this type follows from their being unequal in standing:
//
//   - the CHANNEL is this node's own socket and cannot be borrowed. It says WHERE
//     the frame came in, and it lives only as long as its connection;
//   - the IDENTITY is what the neighbour PRESENTS, and on a session this node
//     dialled it is a name the remote chose for itself. So the identity-shaped
//     accessor answers only where the identity was PROVEN (Identity), and the
//     claim leaves the layer only together with the level behind it
//     (PresentedIdentity). Foreign code — a type's handler, a type's
//     authorization hook — cannot be asked to remember which direction it is on;
//     the value it is handed has to say;
//   - the OWNER is WHOSE budget the arrival was billed to, which is the only one
//     of the three a per-neighbour QUOTA may be keyed on. It never leaves this
//     package — the bucket is the cache's own business — and is observable from
//     outside only as a count, and only from a test (OwnerLoadForTest). A channel dies
//     with its connection while a replay record lives up to replay_until, so a
//     bucket keyed on the channel is a bucket a reconnect renews — and a name is
//     borrowable, so a bucket keyed on the presented identity is a quota a
//     stranger spends on the node it names. This is the same split Upstream makes
//     on the reverse plane, for the same two reasons.
//
// None of the three is derivable from the others, which is why all three travel
// together instead of being re-derived at the far end.
type IngressPeer struct {
	channel   ChannelID
	billedTo  AdmissionKey
	identity  domain.PeerIdentity
	authority IngressAuthority
	local     bool
}

// LocalIngress is the incoming_peer of a datagram this node created. There is
// no channel — the frame crossed no socket to get here — and nothing to prove:
// the origin is this process. It is billed to nobody either, which is what keeps
// its bucket unshareable: our own frames are not attacker-generated and must not
// be evicted to make room for a neighbour's.
func LocalIngress() IngressPeer {
	return IngressPeer{channel: NoChannel(), local: true, authority: AuthorityProven}
}

// ProvenIngress is the incoming_peer of a datagram received on a channel whose
// neighbour PROVED its identity to this node: it signed a challenge this node
// generated with a key whose fingerprint is the identity it presents
// (connauth.VerifyAuthSession, on an accepted connection).
//
// The owner is not a parameter here and that is not an omission: in the PROVEN
// namespace the budget key IS the identity (AdmissionKeySpaceProvenIdentity), so
// a second argument would be a second opinion about one fact — and two opinions
// drift. It is the same derivation inboundFrame.authority already makes in the
// other direction: an arrival is proven exactly when its budget key equals the
// key this constructor mints.
func ProvenIngress(channel ChannelID, id domain.PeerIdentity) IngressPeer {
	return IngressPeer{
		channel:   channel,
		billedTo:  ProvenIdentityKey(id),
		identity:  id,
		authority: AuthorityProven,
	}
}

// ClaimedIngress is the incoming_peer of a datagram received on a channel whose
// neighbour proved NOTHING — a session this node dialled, where the welcome
// address is the remote's own claim. The channel is still exact; only the name
// is not.
//
// The owner IS a parameter here, and it is mandatory: on this direction the only
// defensible bucket is the host:port THIS node dialled (AdmissionKeySpace), which
// is nowhere in the rest of the value and cannot be recovered from a name the
// remote chose. The three arguments have pairwise distinct types, so the compiler
// checks the order.
func ClaimedIngress(channel ChannelID, owner AdmissionKey, id domain.PeerIdentity) IngressPeer {
	return IngressPeer{
		channel:   channel,
		billedTo:  owner,
		identity:  id,
		authority: AuthorityClaimed,
	}
}

// IsLocal reports whether the frame originated on this node.
func (p IngressPeer) IsLocal() bool { return p.local }

// IsZero reports whether the value was never set — neither local nor remote.
func (p IngressPeer) IsZero() bool { return !p.local && p.identity.IsZero() }

// Channel returns the transport channel the frame arrived on. The bool is false
// for a local origin and for a caller that recorded none, so a caller cannot
// pin a frame to "channel zero" by accident.
func (p IngressPeer) Channel() (ChannelID, bool) {
	if p.channel.IsZero() {
		return ChannelID{}, false
	}
	return p.channel, true
}

// owner returns the bucket this arrival's records are charged to.
//
// It has no companion bool, unlike Channel and Identity, and that is not a zero
// value slipping back in: there are exactly two forms an arrival can have —
// this node's own frame, or a neighbour's budget key, which ProvenIngress
// derives and ClaimedIngress demands — so a caller is never handed "no bucket"
// and never has to invent one. That is what keeps the single anonymous bucket
// ingressOwner exists to prevent without a producer at all.
//
// It is package-private because the bucket is an implementation detail of the
// cache's quota and eviction, and nothing outside this package groups by it. A
// caller supplies an IngressPeer and the cache bills it; how those counts are
// bucketed is not its business.
func (p IngressPeer) owner() ingressOwner {
	if p.local {
		return ingressOwner{local: true}
	}
	return ingressOwner{billed: p.billedTo}
}

// Identity returns the remote peer ONLY where this node has been shown that the
// neighbour really is that peer.
//
// The bool is false for the local origin, for the unset value AND for a
// neighbour that merely CLAIMS the name — the last of which is the point. Every
// caller of this accessor is code entitled to build a decision on who sent the
// frame, and on a dialled session there is no such fact; answering with the
// claim would let anybody willing to write a fingerprint into a welcome be
// admitted by a trust list. Code that wants the claim asks for it by name and
// gets its level with it (PresentedIdentity).
func (p IngressPeer) Identity() (domain.PeerIdentity, bool) {
	if p.local || p.identity.IsZero() || !p.authority.Proven() {
		return domain.PeerIdentity{}, false
	}
	return p.identity, true
}

// PresentedIdentity returns the name the neighbour PRESENTS together with the
// level of proof behind it. The identity is zero for a local origin and for the
// unset value.
//
// It is one accessor returning both because they are one fact: a name without
// its standing is exactly the value this round removed from the layer. A caller
// that ignores the level has to ignore it visibly, at the call site, in a line
// that names it.
func (p IngressPeer) PresentedIdentity() (domain.PeerIdentity, IngressAuthority) {
	if p.local {
		return domain.PeerIdentity{}, p.authority
	}
	return p.identity, p.authority
}

// String returns a log-friendly form. It carries the level, the channel and the
// OWNER because a log line naming only the identity is exactly the report that
// made the borrowed name invisible — and because a record refused or evicted by a
// quota is only actionable if the bucket it was charged to is readable.
func (p IngressPeer) String() string {
	switch {
	case p.local:
		return "local"
	case p.identity.IsZero():
		return "unset"
	default:
		return p.identity.String() + "/" + p.authority.String() +
			"@" + p.channel.String() + "#" + p.owner().String()
	}
}

// ingressOwner is WHOSE bucket the records of one arrival belong to: the value
// every per-neighbour quota and every fairness eviction inside the replay cache
// groups by.
//
// It is a type of its own rather than "the IngressPeer used as a map key", and
// that is the whole point. The peer value also carries the CHANNEL the frame
// arrived on, and a bucket keyed on the whole value is a bucket a neighbour opens
// one of per reconnect and one of per parallel session: it fills its share, dials
// again, and starts over — while at capacity its records, spread over N channels,
// read as N quiet neighbours, so the eviction takes from the honest peer that has
// only one. That is not a performance defect but an anti-replay one: the records
// evicted in its place are the memory that stops somebody else's frames from
// being replayed.
//
// So the bucket is what the receive path can actually defend about a neighbour
// ACROSS reconnects — the AdmissionKey the same arrival already pays its byte,
// frame and verification budget from — and one neighbour is one bucket on every
// limit the layer has. It is the exact counterpart of upstreamKey on the reverse
// plane (reverse_state.go).
//
// It is PACKAGE-PRIVATE, and the argument that once kept it exported was a
// circle: it was exported because an exported IngressPeer accessor returned it,
// and that accessor was exported because the type was. Neither had a consumer outside this
// package — the node constructs the cache and hands it to the pipeline, and
// every quota and eviction that groups by a bucket lives in replay_base.go.
// An export nobody imports is not an API, it is a promise the next change has
// to keep for nobody.
type ingressOwner struct {
	// billed is the budget key of an arrival somebody was billed for.
	billed AdmissionKey
	// local marks this node's own frames, whose bucket is shared with nobody.
	local bool
}

// ingressOwnerKind names WHICH form a bucket is.
//
// It exists so the "which field is non-zero" chain is written once and every
// reader of the form goes through it. Copies of a classification drift, and the
// direction they drift in here is a record filed under a bucket the next reader
// classifies differently.
type ingressOwnerKind uint8

const (
	// ingressOwnerUnset names NOBODY. It is the zero value and it is a named
	// state rather than an accident: an owner nothing set is the single
	// anonymous bucket every unbillable record on the node would evict each
	// other from, so the value has to be recognisable in order to be refused.
	ingressOwnerUnset ingressOwnerKind = iota
	// ingressOwnerLocal is this node's own frames, shared with nobody.
	ingressOwnerLocal
	// ingressOwnerBilled is an arrival somebody's budget was charged for, and
	// the bucket IS that budget key.
	ingressOwnerBilled
)

var ingressOwnerKindNames = map[ingressOwnerKind]string{
	ingressOwnerUnset:  "unset",
	ingressOwnerLocal:  "local",
	ingressOwnerBilled: "billed",
}

// String returns the form name used in logs and metrics.
func (k ingressOwnerKind) String() string { return enumName(ingressOwnerKindNames, k) }

// kind reports which form this bucket is.
func (o ingressOwner) kind() ingressOwnerKind {
	switch {
	case o.local:
		return ingressOwnerLocal
	case !o.billed.IsZero():
		return ingressOwnerBilled
	default:
		return ingressOwnerUnset
	}
}

// String renders the bucket for a log line: a `rejected` or `evicted` record is
// only actionable if the bucket it was charged to is readable.
func (o ingressOwner) String() string {
	switch o.kind() {
	case ingressOwnerLocal:
		return "local"
	case ingressOwnerBilled:
		return o.billed.String()
	default:
		return "unset"
	}
}

// compare orders buckets so the victim of an overflow is reproducible instead of
// map-iteration dependent. The LOCAL bucket is the LEAST key, which is what
// spares this node's own frame when the fairness eviction has to break a tie —
// the same direction upstreamOrderLess takes on the reverse plane.
func (o ingressOwner) compare(other ingressOwner) int {
	if o.local != other.local {
		if o.local {
			return -1
		}
		return 1
	}
	return o.billed.compare(other.billed)
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

// ReservationToken is `rsv`: the local handle of one reservation of one replay
// key. It never appears on the wire — it is neither the datagram
// UUID nor `retry` — and it exists to close two holes that a bare replay key
// leaves open (§4.1):
//
//   - read-back of a racing Reserve could not tell the cache's own
//     reservation from the competitor's reservation of the same key;
//   - a late Release(replay_key) of a finished reservation would cancel the
//     NEXT reservation of that key — textbook ABA. Release(rsv) with a stale
//     token is a no-op instead.
//
// The generation is a cache-wide monotonic sequence rather than a per-key
// counter: a per-key counter restarts at zero when the entry is deleted on
// Release, which would make the second reservation of a key carry the first
// one's token and re-open the ABA hole.
//
// One sequence is enough because no reservation outlives the process. The token
// used to carry a `boot` component beside it, to keep a DURABLE token minted
// before a restart from colliding with a fresh sequence — and durable stores are
// what this layer stopped having, so the only issuer left always passed zero.
type ReservationToken struct {
	key domain.ReplayKey
	seq uint64
}

// newReservationToken mints a token on the key the reservation was asked for.
//
// It is unexported because BaseReplayCache is the only issuer: the exported form
// existed for store implementations in other packages, and a stateless
// forwarder has none. A token is opaque to everyone else by construction rather
// than by agreement.
//
// That is also what makes "the token names the key Reserve was asked for" an
// invariant instead of a check: the only issuer mints it from Reserve's own key
// argument, three lines below the signature. Every later operation on the record
// — Commit and Release — is addressed by this token, so a token naming another
// key would move both onto a record the frame does not own; the layer used to
// re-verify that on every reservation, which was a guard against store
// implementations that can no longer exist.
func newReservationToken(key domain.ReplayKey, seq uint64) ReservationToken {
	return ReservationToken{key: key, seq: seq}
}

// ReplayKey returns the key this reservation is held on.
func (t ReservationToken) ReplayKey() domain.ReplayKey { return t.key }

// IsZero reports whether the token was never issued.
func (t ReservationToken) IsZero() bool { return t == ReservationToken{} }

// String returns a log-friendly form; the replay key is truncated because a
// full key in every log line buys nothing over its prefix.
func (t ReservationToken) String() string {
	return "rsv:" + shortKey(t.key) + ":" + formatUint(t.seq)
}

// ---------------------------------------------------------------------------
// Has
// ---------------------------------------------------------------------------

// HasOutcome enumerates the two answers of the early, non-reserving replay
// probe: the cache remembers this key, or it does not. There is no third
// answer, because an in-memory lookup has nothing to fail at — see "No
// half-broken anti-replay" at the top of this file.
type HasOutcome uint8

const (
	// HasMiss means the cache has no live record of this key.
	HasMiss HasOutcome = iota + 1
	// HasHit means the cache has seen this key.
	HasHit
)

// String returns the outcome name used in logs and metrics.
func (o HasOutcome) String() string {
	switch o {
	case HasMiss:
		return "miss"
	case HasHit:
		return "hit"
	default:
		return "invalid"
	}
}

// HasResult is the outcome of the early replay probe. It is a SNAPSHOT rather
// than a fact about the present: the record it was read from may be released by
// a concurrent instance of the same frame the moment after. It may therefore
// decide "is this worth going on with" and never "I still own the frame" (§4.1).
//
// A hit says the key is TAKEN and nothing more, and that is the whole memory
// the probe has to expose: a reserved key and a committed key are the same
// instruction to the receive path — drop the frame — and the verdict a node
// reached on the original is not something a duplicate may act on.
type HasResult struct {
	outcome HasOutcome
}

// hasMissResult reports that the key is unknown to the cache.
func hasMissResult() HasResult { return HasResult{outcome: HasMiss} }

// hasHitResult reports that the cache holds this key, whether it is still
// reserved by a concurrent instance of the same frame or already committed.
func hasHitResult() HasResult { return HasResult{outcome: HasHit} }

// Outcome returns the variant.
func (r HasResult) Outcome() HasOutcome { return r.outcome }

// ---------------------------------------------------------------------------
// Reserve
// ---------------------------------------------------------------------------

// ReserveOutcome enumerates the three answers of the atomic check-and-reserve
// that stands immediately before the first mutating operation (§4.1). Both
// negative answers are DETERMINISTIC — the key was taken, or there was no room
// for it — and neither leaves the caller guessing whether something landed.
type ReserveOutcome uint8

const (
	// ReserveReserved means this caller won the key and holds `rsv`.
	ReserveReserved ReserveOutcome = iota + 1
	// ReserveDuplicate means the key is already held or committed; the
	// frame is dropped silently.
	ReserveDuplicate
	// ReserveRejected means a deterministic capacity or neighbour-quota
	// refusal. Nothing was written.
	ReserveRejected
)

// String returns the outcome name used in logs and metrics.
func (o ReserveOutcome) String() string {
	switch o {
	case ReserveReserved:
		return "reserved"
	case ReserveDuplicate:
		return "duplicate"
	case ReserveRejected:
		return "rejected"
	default:
		return "invalid"
	}
}

// ReserveResult is the outcome of Reserve. A successful reservation takes
// the slot PHYSICALLY, which is what makes the later Commit incapable of
// failing for lack of room (§4.1).
type ReserveResult struct {
	err         error
	reservation ReservationToken
	outcome     ReserveOutcome
}

// reservedResult reports a won reservation addressed by rsv.
//
// This constructor and its two siblings are unexported — as are the ones of the
// other two result types — because the only producer of a result is the cache
// in this package. They were exported while a ReplayStore interface existed and
// an outside implementation had to be able to answer one; with the interface
// gone (see "What the memory is NOT" below) there is no such caller, and an
// exported constructor for a value nobody outside can be asked to produce is a
// second, unowned way to mint the layer's verdicts.
func reservedResult(rsv ReservationToken) ReserveResult {
	return ReserveResult{outcome: ReserveReserved, reservation: rsv}
}

// reserveDuplicateResult reports that the key is already taken.
func reserveDuplicateResult() ReserveResult {
	return ReserveResult{outcome: ReserveDuplicate}
}

// reserveRejectedResult reports a deterministic refusal (capacity, quota).
func reserveRejectedResult(err error) ReserveResult {
	return ReserveResult{outcome: ReserveRejected, err: err}
}

// Outcome returns the variant.
func (r ReserveResult) Outcome() ReserveOutcome { return r.outcome }

// Reservation returns the token of a won reservation.
func (r ReserveResult) Reservation() (ReservationToken, bool) {
	return r.reservation, r.outcome == ReserveReserved
}

// Err returns the refusal or failure cause, for logs only — the decision is
// made on the outcome, never on the error text.
func (r ReserveResult) Err() error { return r.err }

// ---------------------------------------------------------------------------
// ok | fail mutations
// ---------------------------------------------------------------------------

// MutationOutcome is the two-valued result shared by every mutation whose only
// question is "did it apply": Commit and Release.
type MutationOutcome uint8

const (
	// MutationApplied is `ok`.
	MutationApplied MutationOutcome = iota + 1
	// MutationNotApplied is `fail`, and it means the mutation definitely did
	// not land. It is not an I/O verdict: the cache refuses only a
	// token it cannot honour — one addressing a record that is gone, or a
	// stale one that a newer reservation of the same key has replaced.
	MutationNotApplied
)

// String returns the outcome name used in logs and metrics.
func (o MutationOutcome) String() string {
	switch o {
	case MutationApplied:
		return "ok"
	case MutationNotApplied:
		return "fail"
	default:
		return "invalid"
	}
}

// MutationResult is the `ok | fail` outcome. `fail` never means "maybe": see
// "No half-broken anti-replay" at the top of this file.
type MutationResult struct {
	err     error
	outcome MutationOutcome
}

// appliedResult reports a mutation that landed. The `Result` suffix is what
// keeps it from shadowing the `applied` locals the package already reads
// Commit into.
func appliedResult() MutationResult { return MutationResult{outcome: MutationApplied} }

// notAppliedResult reports a mutation that definitely did not land.
func notAppliedResult(err error) MutationResult {
	return MutationResult{outcome: MutationNotApplied, err: err}
}

// Outcome returns the variant.
func (r MutationResult) Outcome() MutationOutcome { return r.outcome }

// IsApplied reports whether the mutation landed.
func (r MutationResult) IsApplied() bool { return r.outcome == MutationApplied }

// Err returns the failure cause, for logs and for errors.Is checks against
// the sentinels in this file.
func (r MutationResult) Err() error { return r.err }

// ---------------------------------------------------------------------------
// What the memory is NOT
// ---------------------------------------------------------------------------
//
// There used to be a ReplayStore interface here, sealed with an unexported
// method so that only this package could implement it. The seal did not hold:
// an unexported method comes free with EMBEDDING, so a wrapper that embedded the
// interface, overrode Has/Reserve/Commit/Release and reached the pipeline
// through PipelineConfig was legal — and that wrapper could block, could touch a
// disk, and could answer an operation ambiguously. "No blocking and no disk"
// then rested on nobody writing one rather than on the type system.
//
// So the interface is gone and every caller names *BaseReplayCache. A different
// memory is no longer something to refuse in prose: it is a type that cannot be
// passed. Along with it went the OPTIONAL companion ExpiredRecordSweeper — the
// concrete type simply has SweepExpired, so there is nothing to type-assert for
// and no "a memory that keeps no expiring records" case to carry.
//
// The interface used to carry nine more methods than the four the cache has.
// Seven were CUSTODY — StoreIfAbsent, the four lease operations, Requeue and
// ReleasePayload — through which a store took a datagram and handed it back to
// the send path. Two more were the startup and runtime halves of DURABILITY:
// ReadPending, which reported a store's unfinished records after a restart, and
// ProbeWritable, which asked a degraded store whether its write path had come
// back. StoreIdentity went with them for a narrower reason: it existed so the
// layer could tell two entries of a profile-store map apart, and there is one
// memory.
//
// A durable store is not a missing feature here — it is the thing the layer
// stopped being. Repeating a frame belongs to the protocol that created it, at
// the two ENDS of its path, where the application state that decides "is this
// still worth sending" actually lives; a relay that could keep a frame is a
// relay every hop on the way has to be told, in prose, not to trust.

// shortKey renders the first bytes of a replay key for logs.
func shortKey(key domain.ReplayKey) string {
	full := key.String()
	if len(full) <= 12 {
		return full
	}
	return full[:12]
}

// formatUint renders a generation without pulling fmt into hot log paths.
func formatUint(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

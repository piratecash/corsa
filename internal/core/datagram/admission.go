package datagram

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// admission.go implements the TWO-STAGE per-neighbour admission of §4.1
// steps 1 and 8.
//
// Why two stages and not one. Stage one — bytes and frames per second —
// happens BEFORE any parsing, because a single budget checked after parsing
// would let a neighbour make the node decode 64 KiB and hash it before the
// limit ever fires. Stage two — one fixed-price token per signature check —
// happens immediately before ed25519.Verify and nowhere earlier, because
// before parsing it is unknown whether a verification will happen at all:
// request and response carry no signature by contract, and a routed frame
// may be sieved out by the early Has, by `ttl ≤ max_ttl` or by Validity. A
// frame refused on any of those paths must not have paid for a verification
// it never received.
//
// The two stages are therefore charged from two places, and both charge THIS
// controller with the SAME key. Stage one is called by the owner of the receive
// path, which is the only party above the refusals the conveyor never sees; the
// key it used travels down with the frame and the conveyor charges stage two on
// it. One neighbour, one bucket, two call sites — and neither of them derives
// the key from anything the neighbour wrote.
//
// Why ONE budget per neighbour and not one per class. §5 is explicit: the
// budget is per neighbour and the classes DIVIDE it. A per-class budget is
// self-defeating — the class is a field the sender writes, so a per-class
// budget is a budget the sender can double by alternating the field. Nothing
// in this file reads the class; that is not an omission, it is the rule.
//
// Why never `src`. On the direction where an identity IS proven, it is proven
// by the handshake before the first datagram arrives. `src` exists only in
// routed frames and means nothing until the signature is checked — and the
// signature check is itself charged to this budget, so trusting `src` to key
// the budget would let an attacker spend somebody else's tokens by writing
// their address into a frame that is about to fail verification. The same
// argument disqualifies the welcome address of an outbound session, which is a
// claim of exactly the same standing (see AdmissionKeySpaceDialedAddress).
//
// Why the key is a TYPE and not a domain.PeerIdentity. The two receive
// directions of a node prove different things about the neighbour, and the
// budget has to key on what is actually proven THERE (AdmissionKeySpace says
// which is which). A bare identity cannot express the difference, and the
// version that used one keyed the outbound direction on a string the REMOTE
// side chose — so an empty one left the whole session unbudgeted and a fresh
// one per reconnect handed back the whole burst. Two constructors and one
// unexported field mean the caller has to say which namespace it is in, and
// two namespaces can never collide on one bucket.
//
// Reference: docs/refactoring/datagram-transport.md §4.1, §5, §9.

// AdmissionKeySpace names WHICH KIND of neighbour identifier a key carries.
// It is part of the key, so a bucket belongs to exactly one namespace.
type AdmissionKeySpace uint8

const (
	// AdmissionKeySpaceUnset is the zero value and never a valid key. Admit
	// refuses it instead of opening a bucket for it: one shared bucket for
	// every unidentified caller is a budget any of them can drain for all the
	// others.
	AdmissionKeySpaceUnset AdmissionKeySpace = iota
	// AdmissionKeySpaceProvenIdentity is an identity the REMOTE side proved to
	// this node — it signed a challenge this node generated, with a key whose
	// fingerprint is that identity (connauth.VerifyAuthSession). Nobody else
	// can present it, so nobody else can spend its budget, and the same
	// neighbour reconnecting lands on the same bucket.
	AdmissionKeySpaceProvenIdentity
	// AdmissionKeySpaceDialedAddress is the host:port THIS node dialled. It is
	// the only trusted key of a direction where the remote proves nothing: on
	// an outbound session the challenge travels the other way, so everything
	// the welcome frame says about the peer — its address included — is the
	// peer's own claim. The dialled host:port is ours, it is fixed for the
	// lifetime of the session, and it is identical across reconnects to the
	// same peer, which is exactly what a rate limit needs. It is the same
	// argument the v12 wire contract already settled for the overlay key
	// (see applyWelcomeMetadata).
	AdmissionKeySpaceDialedAddress
)

var admissionKeySpaceNames = map[AdmissionKeySpace]string{
	AdmissionKeySpaceUnset:          "unset",
	AdmissionKeySpaceProvenIdentity: "proven_identity",
	AdmissionKeySpaceDialedAddress:  "dialed_address",
}

// String returns the log label of the namespace.
func (s AdmissionKeySpace) String() string {
	if name, ok := admissionKeySpaceNames[s]; ok {
		return name
	}
	return "unknown"
}

// AdmissionKey is WHO a per-neighbour budget is charged to.
//
// The fields are unexported and the only way in is one of the two
// constructors, because the whole value of the type is that a call site cannot
// produce a key without stating what it knows about the peer.
type AdmissionKey struct {
	address  domain.PeerAddress
	identity domain.PeerIdentity
	space    AdmissionKeySpace
}

// ProvenIdentityKey keys a budget on an identity the remote side PROVED.
// A zero identity yields the zero key: absence is modelled by the type, not by
// a bucket that stands for "somebody".
func ProvenIdentityKey(peer domain.PeerIdentity) AdmissionKey {
	if peer.IsZero() {
		return AdmissionKey{}
	}
	return AdmissionKey{space: AdmissionKeySpaceProvenIdentity, identity: peer}
}

// DialedAddressKey keys a budget on the host:port THIS node dialled.
// A blank address yields the zero key for the same reason.
func DialedAddressKey(address domain.PeerAddress) AdmissionKey {
	if strings.TrimSpace(string(address)) == "" {
		return AdmissionKey{}
	}
	return AdmissionKey{space: AdmissionKeySpaceDialedAddress, address: address}
}

// IsZero reports whether the key names nobody.
func (k AdmissionKey) IsZero() bool { return k.space == AdmissionKeySpaceUnset }

// Space reports which namespace the key lives in.
func (k AdmissionKey) Space() AdmissionKeySpace { return k.space }

// String renders the key for a log line, namespace first so two keys carrying
// the same text in different namespaces never read as one neighbour.
func (k AdmissionKey) String() string {
	switch k.space {
	case AdmissionKeySpaceProvenIdentity:
		return k.space.String() + ":" + k.identity.String()
	case AdmissionKeySpaceDialedAddress:
		return k.space.String() + ":" + string(k.address)
	default:
		return k.space.String()
	}
}

// compare orders keys so the eviction victim of a full map is reproducible
// instead of map-iteration dependent.
func (k AdmissionKey) compare(other AdmissionKey) int {
	switch {
	case k.space < other.space:
		return -1
	case k.space > other.space:
		return 1
	}
	if ordered := k.identity.Compare(other.identity); ordered != 0 {
		return ordered
	}
	return strings.Compare(string(k.address), string(other.address))
}

// AdmissionConfig wires the controller. Everything it needs is here, so a
// call site shows the whole policy at once (CLAUDE.md).
type AdmissionConfig struct {
	// Clock is the injectable time source, following the package
	// convention. Defaults to time.Now.
	Clock func() time.Time
	// Budget is the per-neighbour budget. Non-positive fields fall back to
	// the §5 starting values.
	Budget PeerBudget
}

// AdmissionStats is the lock-free counter snapshot of the controller, shaped
// for RPC diagnostics the same way routing.RouteCapStats is: monotonic
// counters, each read at its own Load point. Cross-field consistency is
// best-effort under concurrent traffic and exact once the traffic stops.
type AdmissionStats struct {
	// Admitted counts frames that passed stage one.
	Admitted uint64
	// RefusedBytes counts frames refused for lack of byte budget.
	RefusedBytes uint64
	// RefusedFrames counts frames refused for lack of frame budget.
	RefusedFrames uint64
	// AdmittedBytes counts the serialized bytes actually charged (§5:
	// counted on the wire form, including base64 and the auth block).
	AdmittedBytes uint64
	// VerifiesCharged counts tokens spent immediately before a signature
	// check.
	VerifiesCharged uint64
	// VerifiesRefused counts signature checks that never happened because
	// the neighbour's verification budget was empty.
	VerifiesRefused uint64
	// PeersEvicted counts buckets dropped to honour TrackedPeers while they
	// still owed tokens — the only path on which forgetting a neighbour
	// forgives debt, and therefore the only one worth watching.
	PeersEvicted uint64
	// RefusedUnkeyed counts charges refused because the caller could name no
	// neighbour at all — neither a proven identity nor an address this node
	// dialled.
	//
	// It is its own counter and not a flavour of RefusedBytes/RefusedFrames
	// because the two mean opposite things to whoever reads them: those are a
	// neighbour hitting its limit, which rises with load and is expected, while
	// any non-zero value here is a receive path that reached the budget without
	// knowing whose it was — a defect in the caller, not in the traffic.
	RefusedUnkeyed uint64
	// TrackedPeers is the current number of live buckets.
	TrackedPeers int
}

// peerBuckets is the whole budget of one neighbour: three token buckets that
// refill in real time, plus the moment the neighbour was last seen.
//
// Tokens are float64 because refill is continuous: an integer bucket refilled
// once per call would round every sub-token refill down to zero and turn a
// steady 60-frames-per-second neighbour into a blocked one.
type peerBuckets struct {
	lastRefill time.Time
	lastSeen   time.Time
	bytes      float64
	frames     float64
	verifies   float64
}

// PeerAdmission is the per-neighbour admission controller of §5.
//
// Locking contract: mu guards the bucket map and every bucket in it. The
// critical section is arithmetic only — no I/O, no callbacks — which is what
// lets the receive path of every session share one controller without the
// lock showing up in a profile.
type PeerAdmission struct {
	clock  func() time.Time
	budget PeerBudget

	peers map[AdmissionKey]*peerBuckets

	admitted        atomic.Uint64
	admittedBytes   atomic.Uint64
	refusedBytes    atomic.Uint64
	refusedFrames   atomic.Uint64
	verifiesCharged atomic.Uint64
	verifiesRefused atomic.Uint64
	peersEvicted    atomic.Uint64
	refusedUnkeyed  atomic.Uint64

	mu sync.Mutex
}

// Compile-time proof that the controller is the pipeline's verification budget.
//
// Stage one has no seam to satisfy: it is charged by the owner of the receive
// path, which calls Admit directly with the key that direction can defend
// (node/datagram_integration.go). Both stages are therefore the same
// controller and the same bucket by construction, which is the whole content of
// "the crypto budget is part of the neighbour's budget" (§5).
//
// There are no identity-typed adapters over the two entry points any more.
// They could only ever address the PROVEN namespace, which made them unusable
// on the outbound direction — and worse than unusable on stage two, where the
// pipeline did pass the outbound neighbour's own CLAIM into one and charged a
// stranger's bucket with it.
var _ cryptoBudget = (*PeerAdmission)(nil)

// NewPeerAdmission builds the controller. There is no error return: every
// field of the budget has a normative fallback, and refusing to start over a
// zero knob would be worse than starting with the documented default.
func NewPeerAdmission(cfg AdmissionConfig) *PeerAdmission {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &PeerAdmission{
		clock:  clock,
		budget: cfg.Budget.normalized(DefaultLimits().Peer),
		peers:  make(map[AdmissionKey]*peerBuckets),
	}
}

// Admit is stage one: charge frameBytes of the FULL wire line and one frame
// against the neighbour's budget, before the line is parsed.
//
// Both dimensions are charged atomically or not at all. A frame that fits the
// byte budget but not the frame budget must not silently eat bytes: a partial
// charge would let a flood of refused frames drain the budget of the frames
// that would have been admitted.
//
// The zero key is refused without touching the map. It means the caller could
// neither prove who sent the frame nor say which socket it dialled, and there
// is nobody to bill; opening a bucket for it would make one budget that every
// unidentified arrival on the node spends from.
func (a *PeerAdmission) Admit(key AdmissionKey, frameBytes int) bool {
	if key.IsZero() {
		a.refusedUnkeyed.Add(1)
		return false
	}
	if frameBytes < 0 {
		frameBytes = 0
	}
	now := a.clock()

	a.mu.Lock()
	buckets := a.bucketsLocked(key, now)
	a.refillLocked(buckets, now)

	switch {
	case buckets.frames < 1:
		a.mu.Unlock()
		a.refusedFrames.Add(1)
		return false
	case buckets.bytes < float64(frameBytes):
		a.mu.Unlock()
		a.refusedBytes.Add(1)
		return false
	}
	buckets.frames--
	buckets.bytes -= float64(frameBytes)
	a.mu.Unlock()

	a.admitted.Add(1)
	a.admittedBytes.Add(uint64(frameBytes))
	return true
}

// ChargeVerifyFor is stage two: one fixed-price token, taken immediately
// before ed25519.Verify. A false answer means the layer must refuse the frame
// WITHOUT verifying it — the whole point of the separate stage.
func (a *PeerAdmission) ChargeVerifyFor(key AdmissionKey) bool {
	if key.IsZero() {
		a.refusedUnkeyed.Add(1)
		return false
	}
	now := a.clock()

	a.mu.Lock()
	buckets := a.bucketsLocked(key, now)
	a.refillLocked(buckets, now)
	if buckets.verifies < 1 {
		a.mu.Unlock()
		a.verifiesRefused.Add(1)
		return false
	}
	buckets.verifies--
	a.mu.Unlock()

	a.verifiesCharged.Add(1)
	return true
}

// Forget releases a neighbour's buckets when its session ends — but ONLY when
// releasing them forgives nothing.
//
// The distinction is the whole point. bucketsLocked creates a bucket FULL,
// because the budget is a rate limit and the first frame of a freshly
// authenticated session must not be refused. Deleting a bucket that still owes
// its debt therefore hands the debt back, and the moment of the reconnect is
// chosen by the neighbour: tear the session down, dial again, and the whole
// ByteBurst, FrameBurst and VerifyBurst are new. The eviction path already
// treats that as something with a price and counts it (PeersEvicted); doing it
// unconditionally here would be the same gift, ungated and uncounted.
//
// So a bucket that is idle and completely refilled is dropped — it is
// byte-for-byte a fresh one — and any other bucket is simply left in place. It
// is not a leak: the map is bounded by TrackedPeers, and evictLocked drops
// exactly these leftovers as soon as they have refilled.
func (a *PeerAdmission) Forget(key AdmissionKey) {
	if key.IsZero() {
		return
	}
	now := a.clock()

	a.mu.Lock()
	defer a.mu.Unlock()
	buckets, known := a.peers[key]
	if !known || a.forgettableLocked(buckets, now) {
		delete(a.peers, key)
	}
}

// TrackedPeers returns the number of live buckets.
func (a *PeerAdmission) TrackedPeers() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.peers)
}

// Stats publishes the counters. The counters are atomic, so this takes the
// lock only for the bucket count.
func (a *PeerAdmission) Stats() AdmissionStats {
	return AdmissionStats{
		Admitted:        a.admitted.Load(),
		RefusedBytes:    a.refusedBytes.Load(),
		RefusedFrames:   a.refusedFrames.Load(),
		AdmittedBytes:   a.admittedBytes.Load(),
		VerifiesCharged: a.verifiesCharged.Load(),
		VerifiesRefused: a.verifiesRefused.Load(),
		PeersEvicted:    a.peersEvicted.Load(),
		RefusedUnkeyed:  a.refusedUnkeyed.Load(),
		TrackedPeers:    a.TrackedPeers(),
	}
}

// bucketsLocked returns the neighbour's buckets, creating them full.
//
// A new neighbour starts with a full bucket rather than an empty one because
// the budget is a rate limit, not an admission fee: the first frame of a
// freshly authenticated session must not be refused.
func (a *PeerAdmission) bucketsLocked(key AdmissionKey, now time.Time) *peerBuckets {
	if buckets, known := a.peers[key]; known {
		buckets.lastSeen = now
		return buckets
	}
	a.evictLocked(now)
	buckets := &peerBuckets{
		lastRefill: now,
		lastSeen:   now,
		bytes:      float64(a.budget.ByteBurst),
		frames:     float64(a.budget.FrameBurst),
		verifies:   float64(a.budget.VerifyBurst),
	}
	a.peers[key] = buckets
	return buckets
}

// refillLocked advances all three buckets to now. Time never runs backwards
// here: a clock that jumped back would otherwise DRAIN the buckets, turning a
// clock adjustment into a self-inflicted outage.
func (a *PeerAdmission) refillLocked(buckets *peerBuckets, now time.Time) {
	elapsed := now.Sub(buckets.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	buckets.lastRefill = now
	buckets.bytes = refill(buckets.bytes, elapsed*float64(a.budget.BytesPerSecond), float64(a.budget.ByteBurst))
	buckets.frames = refill(buckets.frames, elapsed*float64(a.budget.FramesPerSecond), float64(a.budget.FrameBurst))
	buckets.verifies = refill(buckets.verifies, elapsed*float64(a.budget.VerifiesPerSecond), float64(a.budget.VerifyBurst))
}

func refill(tokens, added, burst float64) float64 {
	tokens += added
	if tokens > burst {
		return burst
	}
	return tokens
}

// evictLocked keeps the map under TrackedPeers before a new bucket is added.
//
// It runs in two passes on purpose. The first drops buckets that are idle AND
// completely refilled: forgetting those forgives nothing, because a refilled
// bucket is byte-for-byte a fresh one. Only if that is not enough does the
// second pass drop the longest-idle bucket regardless of its debt — bounded
// memory beats exact accounting — and that path is counted, because it is the
// only one on which eviction hands a neighbour back budget it had spent.
func (a *PeerAdmission) evictLocked(now time.Time) {
	if len(a.peers) < a.budget.TrackedPeers {
		return
	}
	for key, buckets := range a.peers {
		if a.forgettableLocked(buckets, now) {
			delete(a.peers, key)
		}
	}
	if len(a.peers) < a.budget.TrackedPeers {
		return
	}
	victim, found := a.idlestLocked()
	if !found {
		return
	}
	delete(a.peers, victim)
	a.peersEvicted.Add(1)
}

// forgettableLocked reports whether dropping this bucket costs nothing.
func (a *PeerAdmission) forgettableLocked(buckets *peerBuckets, now time.Time) bool {
	if now.Sub(buckets.lastSeen) < a.budget.IdleRetention {
		return false
	}
	a.refillLocked(buckets, now)
	return buckets.bytes >= float64(a.budget.ByteBurst) &&
		buckets.frames >= float64(a.budget.FrameBurst) &&
		buckets.verifies >= float64(a.budget.VerifyBurst)
}

// idlestLocked picks the longest-idle bucket, with a deterministic tie-break
// so the victim of an overflow is reproducible instead of map-iteration
// dependent.
func (a *PeerAdmission) idlestLocked() (AdmissionKey, bool) {
	var (
		victim AdmissionKey
		oldest time.Time
		found  bool
	)
	for key, buckets := range a.peers {
		switch {
		case !found, buckets.lastSeen.Before(oldest):
			victim, oldest, found = key, buckets.lastSeen, true
		case buckets.lastSeen.Equal(oldest) && key.compare(victim) < 0:
			victim = key
		}
	}
	return victim, found
}

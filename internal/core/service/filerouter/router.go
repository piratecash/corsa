package filerouter

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

const fileCommandDefaultMaxTTL uint8 = 10

// NonceCache is the anti-replay interface required by Router. The node
// package provides the concrete implementation (nonceCache).
type NonceCache interface {
	Has(nonce string) bool
	TryAdd(nonce string) bool
}

// PeerRouteMeta is the per-peer metadata the file router needs to rank
// next-hop candidates. Bundling it into a single value forces the node
// layer to take peerMu once and hand back a consistent snapshot.
//
// Both fields describe the *same* connection — the one the live send
// path would actually try first for this peer. The node-side helper
// reads them from a shared candidate-selection helper, so the meta
// cannot drift away from what sendFrameToIdentity would do.
//
// ConnectedAt is the moment the chosen connection was established;
// older timestamps win the uptime tie-break in routeCandidateLess.
//
// ProtocolVersion is the *normalized ranking key*, NOT the raw
// handshake-reported value. The wire source depends on direction —
// outbound candidates carry welcome.Version, inbound candidates carry
// hello.Version (folded onto the inbound NetCore through ApplyOpts in
// the node layer) — but in either case the node layer applies an
// inflated-version defence before populating this field: a peer
// reporting a version higher than the local build's
// `config.ProtocolVersion` is either a benign staged-rollout case
// (operator upgraded the peer ahead of this node) or a deliberate
// traffic-capture attack, and the node-side helper caps the ranking
// value at the local protocol version. The cap collapses the inflated
// peer to the same primary-key tier as a legitimate v=local peer —
// neither wins the protocolVersion DESC sort on the inflation lie —
// while keeping the upgraded peer in the equal-version tier where the
// secondary keys (hops, uptime) decide. Earlier behaviour clamped to
// 0 instead, which solved the attack but starved every legitimate
// upgraded next-hop of file traffic.
//
// Together the two fields describe a single coherent socket — never
// an aggregate stitched across two — but the version key is a
// post-defence projection, not the wire-level reported value.
//
// RawProtocolVersion is the version the peer actually reported on the
// chosen connection BEFORE the inflated-version cap is applied. It is
// kept for audit logging and protocol-aware diagnostics that must see
// the truth, not the ranking projection. After the cap fix the two
// fields differ ONLY when the peer reported v > config.ProtocolVersion
// — in which case ProtocolVersion = config.ProtocolVersion and
// RawProtocolVersion is the actually-reported value. They are equal in
// the common case (peer reports a version this build understands).
//
// Callers that need the ranking key MUST read ProtocolVersion. Callers
// that need the wire-level reported value MUST read RawProtocolVersion.
type PeerRouteMeta struct {
	ConnectedAt        time.Time
	ProtocolVersion    domain.ProtocolVersion
	RawProtocolVersion domain.ProtocolVersion
}

// Router handles inbound FileCommandFrame processing at the node level.
// It validates cleartext headers, checks anti-replay, verifies sender
// signatures, and either delivers locally or forwards to the next hop.
//
// Design invariants:
//   - Transit nodes see only cleartext headers (SRC, DST, TTL, Time, Nonce, Signature).
//     The command type is inside the encrypted Payload — invisible to relays.
//   - Only full nodes relay file commands. Client nodes process frames addressed
//     to them but never forward to other destinations.
//   - File commands have no chatlog, no delivery receipts, no gossip fallback,
//     no pending queue. If no route exists, the frame is silently dropped.
//
// Two routing-table reads coexist deliberately. The hot transit path
// inside HandleInbound uses the cached snapshot (routeSnap) — a frame
// in flight has its own metadata and a ~1–1.5 s delay (the cached
// snapshot's coalescing floor plus a refresh tick) on a freshly
// added route is harmless. Locally-originated paths (SendFileCommand
// and the diagnostic ExplainRoute) read through the per-destination
// fresh oracle (routeLookup) so a route accepted right before the
// user-initiated send is visible immediately, without waiting for the
// cached snapshot's next dirty-flag publish (~1–1.5 s after the writer
// touched the table — routingSnapshotMinInterval's 1 s coalescing floor
// plus the next refresh tick). See "Two distinct staleness bounds" in
// docs/routing.md "Snapshot freshness" for the broader contract.
type Router struct {
	nonceCache                  NonceCache
	localID                     domain.PeerIdentity
	isFullNode                  func() bool
	routeSnap                   func() routing.Snapshot
	routeLookup                 func(dst domain.PeerIdentity) []routing.RouteEntry
	peerRouteMeta               func(domain.PeerIdentity) (PeerRouteMeta, bool)
	isAuthorizedForLocalDeliver func(domain.PeerIdentity) bool
	sessionSend                 func(dst domain.PeerIdentity, data []byte) bool
	localDeliver                func(frame protocol.FileCommandFrame)
}

// RouterConfig holds dependencies injected from Service into Router.
//
// Authenticity vs authorization is deliberately separated:
//
//   - Authenticity (data integrity) is self-contained in the wire frame.
//     Every FileCommandFrame carries SrcPubKey alongside SRC, and the
//     router checks identity.Fingerprint(SrcPubKey) == SRC plus the
//     Ed25519 signature against SrcPubKey. Any node — including a
//     transit relay that has never seen SRC before — can decide whether
//     a frame is forged without consulting any peer state.
//
//   - Authorization (whether to accept a frame for local delivery) is
//     a destination-side trust decision expressed by
//     IsAuthorizedForLocalDelivery. A frame with a perfectly valid
//     signature from an untrusted SRC must not deposit files into the
//     local inbox — that is policy, not data integrity.
//
// This is what lets two NAT-ed peers exchange files through any public
// relay: the relay verifies authenticity from the frame alone and
// forwards. Earlier versions conflated authenticity with authorization
// by sourcing pubkeys from the relay's own trust store, which made
// relay-through-stranger impossible.
type RouterConfig struct {
	NonceCache NonceCache
	LocalID    domain.PeerIdentity
	IsFullNode func() bool

	// RouteSnap returns the cached routing snapshot used by the hot
	// transit path inside HandleInbound. Bounded staleness ≤ the routing
	// snapshot's coalescing floor (1 s) plus the next refresh tick
	// (~1–1.5 s) is acceptable for in-flight
	// frames: their next-hop decision is bounded by the wire frame
	// metadata and a ~1–1.5 s delay on a freshly added route is
	// strictly better than blocking the routing writers (announce
	// loop, TickTTL, hop_ack confirmation) for a deep copy on every
	// transit-forward decision.
	RouteSnap func() routing.Snapshot

	// RouteLookup returns the fresh per-destination route slice for
	// locally-originated paths (SendFileCommand and ExplainRoute).
	// It is the same fresh path the node-level isPeerReachable uses
	// — a direct routing.Table.Lookup — and exists so a route
	// accepted right before a user-initiated file send becomes
	// visible immediately, without waiting for the cached snapshot's
	// next publish. Without this oracle the user would observe a
	// 0–1.5 s window (the routing snapshot's 1 s coalescing floor plus
	// the next refresh tick) where isPeerReachable reports the destination
	// as reachable (it reads the fresh table) but SendFileCommand
	// fails with "no route to <dst>" because collectRouteCandidates
	// reads the still-stale cached snapshot.
	//
	// May be nil — when unset, locally-originated paths fall back to
	// RouteSnap, recovering pre-fix behaviour for tests and any
	// caller that has not wired the oracle yet.
	RouteLookup func(dst domain.PeerIdentity) []routing.RouteEntry

	PeerRouteMeta func(domain.PeerIdentity) (PeerRouteMeta, bool)

	// IsAuthorizedForLocalDelivery is consulted only when DST == self.
	// It expresses the local trust-store policy: returning false means
	// "we do not accept files from this source", and the router silently
	// drops the frame even when authenticity (SrcPubKey + signature) is
	// fully verified. Implementations typically check the trust store.
	//
	// Authenticity is checked by the router itself from the wire frame —
	// callers do NOT supply a pubkey here. Keeping the authorization
	// boundary as a pure boolean prevents a future widening of authenticity
	// (e.g. accepting peers via mesh announcements) from accidentally
	// widening the local-delivery acceptance set.
	IsAuthorizedForLocalDelivery func(domain.PeerIdentity) bool

	SessionSend  func(dst domain.PeerIdentity, data []byte) bool
	LocalDeliver func(frame protocol.FileCommandFrame)
}

// NewRouter creates a Router with the provided dependencies.
func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		nonceCache:                  cfg.NonceCache,
		localID:                     cfg.LocalID,
		isFullNode:                  cfg.IsFullNode,
		routeSnap:                   cfg.RouteSnap,
		routeLookup:                 cfg.RouteLookup,
		peerRouteMeta:               cfg.PeerRouteMeta,
		isAuthorizedForLocalDeliver: cfg.IsAuthorizedForLocalDelivery,
		sessionSend:                 cfg.SessionSend,
		localDeliver:                cfg.LocalDeliver,
	}
}

// HandleInbound processes a received FileCommandFrame. It performs cleartext
// validation, signature verification, and anti-replay check, then routes
// the frame locally or forwards it.
//
// incomingPeer is the identity of the neighbor the frame was received from
// (the previous hop). It is used for split-horizon forwarding: the transit
// node MUST NOT choose a next-hop equal to the previous hop, otherwise a
// symmetric route would reflect the frame straight back where it came from
// and both sides would ping-pong the same frame until TTL expires — a
// waste of bandwidth and, on short TTLs, a failed delivery. Zero-value
// incomingPeer (empty identity) disables the exclusion and is used for
// locally-originated / test-injected frames.
//
// Processing pipeline (cheapest checks first for DDoS resistance):
//
// 1. Anti-replay: nonce cache lookup (Has only, no commit yet) — O(1).
// 2. Deliverability check: DST == self OR at least one viable route
// to DST exists in the cached snapshot. "Viable" applies the same
// TTL/withdrawn semantics as step 8 (current wall-clock against
// ExpiresAt, NextHop != self), so a frame whose only cached routes
// are wall-clock expired is rejected here, before any signature work
// and before any nonce commit.
// 3. Pre-mutation validation: TTL ≤ MaxTTL on the raw incoming value,
// freshness |now − Time| ≤ 5 min, and nonce binding
// SHA256(SRC||DST||MaxTTL||Time||Payload) == Nonce. MaxTTL is bound
// into the nonce so a malicious relay cannot inflate the hop budget
// without invalidating the signature chain. All three checks live in
// ValidateFileCommandFrame and run before any field is mutated, so a
// malicious relay cannot bypass them by inflating TTL past the
// ceiling and counting on a later decrement to slip it back under.
// 4. TTL decrement: apply hop budget after validation, drop on
// exhaustion (loop prevention).
// 5. Authenticity: decode SrcPubKey, recompute identity fingerprint
// against SRC, and ed25519_verify(SrcPubKey, Nonce, Signature).
// Self-contained — independent of any peer state on this node.
// 6. Local delivery (DST == self): IsAuthorizedForLocalDelivery first,
// then atomic nonce commit (TryAdd), then dispatch to
// FileTransferManager. Authorization gates the commit so an
// authentic-but-untrusted SRC cannot evict bounded-LRU entries.
// 7. Relay restriction: only full nodes forward; client nodes drop
// DST ≠ self. Runs BEFORE the relay TryAdd so a client node does
// not consume bounded-LRU slots for transit frames it would never
// forward — the symmetric defence to the auth-before-commit gate in
// the local-delivery branch.
// 8. Collect viable forwarding candidates with split-horizon applied
// (excludeVia == incomingPeer) and TTL evaluated against
// time.Now(). If the candidate set is empty — only routes pointing
// back at the previous hop, or every route wall-clock expired —
// drop without committing the nonce. This guards against the
// residual leak where step 2 saw a viable route but split-horizon
// erases it: the bounded LRU never absorbs an un-forwardable frame.
// 9. Relay path: atomic nonce commit (TryAdd) — exactly one goroutine
// proceeds per authentic transit frame, only after step 8 has
// proven the frame is forwardable.
// 10. Marshal and forward through the pre-collected candidates.
// Marshal runs after TryAdd so concurrent transit duplicates that
// lose the race do not pay the marshal cost.
func (r *Router) HandleInbound(raw json.RawMessage, incomingPeer domain.PeerIdentity) {
	var frame protocol.FileCommandFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		log.Debug().Err(err).Msg("file_router: unmarshal failed")
		return
	}

	now := time.Now()

	// 1. Anti-replay: check-only — do NOT insert yet. Inserting before
	// authenticity checks lets a malicious relay pre-poison the cache with
	// a nonce copied from a legitimate frame inside a malformed wrapper,
	// causing the real frame to be rejected as a replay.
	if r.nonceCache.Has(frame.Nonce) {
		log.Debug().Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: replay detected")
		return
	}

	// 2. Deliverability check: is the frame addressed to us, or can we
	// route it? The early gate uses the same TTL/withdrawn semantics as
	// the later candidate collection (current wall-clock against
	// ExpiresAt), so a frame whose only cached routes are wall-clock
	// expired is dropped HERE — before signature verification AND
	// before TryAdd. Without this, a stale-but-non-empty
	// snap.Routes[DST] would let the frame slip past the early gate,
	// pass through every authentication step, and only get dropped at
	// step 8 collectRouteCandidates AFTER the relay TryAdd had
	// committed the nonce — burning a slot in the bounded LRU for an
	// un-deliverable frame.
	isLocal := frame.DST == r.localID
	if !isLocal {
		if !r.hasViableRoute(frame.DST, now) {
			log.Debug().Str("dst", string(frame.DST)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: no viable route to destination, dropping")
			return
		}
	}

	// 3. Validate incoming frame before any mutation.
	// This includes TTL <= MaxTTL check on the raw incoming value,
	// freshness (clock drift), and nonce binding.
	// Validation MUST happen before DecrementTTL: a malicious relay could
	// inflate TTL to MaxTTL+1; decrementing first would reduce it to MaxTTL,
	// passing the TTL <= MaxTTL check inside ValidateFileCommandFrame.
	if err := protocol.ValidateFileCommandFrame(frame, now); err != nil {
		log.Debug().Err(err).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: validation failed")
		return
	}

	// 4. TTL decrement: apply hop budget after validation.
	decremented, err := frame.DecrementTTL()
	if err != nil {
		log.Debug().Str("dst", string(frame.DST)).Uint8("ttl", frame.TTL).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: TTL exhausted, dropping")
		return
	}
	frame = decremented

	// 5. Self-contained authenticity check. The frame carries SrcPubKey
	// alongside SRC; we verify identity↔pubkey binding by recomputing the
	// fingerprint, then verify the Ed25519 signature against the embedded
	// pubkey. This makes authenticity independent of any peer state on
	// this node — a transit relay that has never seen SRC before can
	// still decide whether the frame is forged.
	//
	// SrcPubKey is part of the permanent file_command authenticity
	// contract: a frame arriving without it is malformed, forged or
	// produced by a misconfigured peer. Drop is the correct outcome
	// regardless of the sender's protocol version.
	if frame.SrcPubKey == "" {
		log.Debug().
			Str("src", string(frame.SRC)).
			Str("nonce", noncePrefix(frame.Nonce)).
			Msg("file_router: missing src_pubkey")
		return
	}
	srcPubKey, err := base64.StdEncoding.DecodeString(frame.SrcPubKey)
	if err != nil || len(srcPubKey) != ed25519.PublicKeySize {
		log.Debug().
			Err(err).
			Str("src", string(frame.SRC)).
			Str("nonce", noncePrefix(frame.Nonce)).
			Int("src_pubkey_len", len(srcPubKey)).
			Msg("file_router: invalid src_pubkey encoding")
		return
	}
	if expected := identity.Fingerprint(ed25519.PublicKey(srcPubKey)); expected != string(frame.SRC) {
		log.Debug().
			Str("src", string(frame.SRC)).
			Str("expected_fingerprint", expected).
			Str("nonce", noncePrefix(frame.Nonce)).
			Msg("file_router: src_pubkey fingerprint does not match SRC")
		return
	}
	if err := protocol.VerifyFileCommandSignature(frame.Nonce, frame.Signature, ed25519.PublicKey(srcPubKey)); err != nil {
		log.Debug().Err(err).Str("src", string(frame.SRC)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: signature verification failed")
		return
	}

	// 6. Local delivery (DST == self): authorization MUST gate the
	// replay-cache commit. Authenticity by itself is cheap to produce
	// after the SrcPubKey change — any peer can sign a frame addressed
	// to us using its own identity — so committing the nonce before the
	// trust-store check would let an authentic-but-untrusted SRC burn
	// slots in the bounded LRU and evict legitimate nonces. The order
	// is therefore: authorization first, then TryAdd, then deliver.
	//
	// Concurrent deliveries of the same authorized frame still collapse
	// to a single localDeliver via TryAdd. The relay branch below keeps
	// the original authenticity-before-TryAdd order: an authenticated
	// frame in flight is part of the network's deduplication set
	// regardless of our local trust policy, and we must commit its
	// nonce so concurrent transit copies fold into one forward.
	if isLocal {
		if !r.isAuthorizedForLocalDeliver(frame.SRC) {
			log.Debug().
				Str("src", string(frame.SRC)).
				Str("nonce", noncePrefix(frame.Nonce)).
				Msg("file_router: SRC not authorized for local delivery")
			return
		}
		if !r.nonceCache.TryAdd(frame.Nonce) {
			log.Debug().Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: concurrent duplicate, nonce already committed")
			return
		}
		r.localDeliver(frame)
		return
	}

	// 7. Relay restriction: only full nodes forward. The check is here,
	// BEFORE the relay TryAdd, so that a client node holding a route
	// for DST cannot have its bounded LRU evicted by an attacker who
	// produces authentic transit frames at near-zero CPU cost. After
	// SrcPubKey self-contained authenticity, signing a valid frame is
	// cheap; without this gate every authenticated DST≠self frame
	// would commit a nonce on the client even though the client will
	// never forward it. The earlier Has check at step 1 still catches
	// genuine replays without committing anything, which is enough
	// dedupe for a non-forwarder.
	if !r.isFullNode() {
		log.Debug().Str("dst", string(frame.DST)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: client node, dropping non-local file command")
		return
	}

	// 8. Collect viable forwarding candidates BEFORE committing the
	// nonce. The early gate at step 2 already checked viability, but
	// it does NOT see the split-horizon constraint (excludeVia ==
	// incomingPeer): if our only viable route to DST happens to point
	// back at the peer that just handed us this frame, the candidate
	// set is empty here even though step 2 passed. Doing the
	// collection before TryAdd guarantees that we never burn a slot
	// in the bounded LRU for a frame the router cannot in fact
	// forward — closing the residual replay-cache leak that the
	// earlier-step TTL gate alone could not.
	//
	// collectRouteCandidates evaluates IsExpired against time.Now() and
	// applies split-horizon (excludeVia == incomingPeer). Concurrent
	// transit duplicates pay this collection work twice in the worst
	// case, but only one of them wins the TryAdd race below — far
	// better than poisoning the cache with un-deliverable frames.
	candidates := r.collectRouteCandidates(frame.DST, incomingPeer)
	if len(candidates) == 0 {
		log.Debug().
			Str("dst", string(frame.DST)).
			Str("exclude_via", string(incomingPeer)).
			Str("nonce", noncePrefix(frame.Nonce)).
			Msg("file_router: no viable forwarding candidate (split-horizon or all expired), dropping without nonce commit")
		return
	}

	// 9. Atomic anti-replay commit — only after we have a viable
	// candidate to forward through. TryAdd returns true if this
	// goroutine is the first to insert the nonce; concurrent transit
	// deliveries of the same valid frame (via multiple peers/transports)
	// lose the race and are dropped here. Using TryAdd instead of
	// separate Has+Add closes the TOCTOU window while still preventing
	// cache poisoning (forged frames never reach this point).
	if !r.nonceCache.TryAdd(frame.Nonce) {
		log.Debug().Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: concurrent duplicate, nonce already committed")
		return
	}

	// 10. Marshal and forward through the pre-collected candidates.
	// The frame is marshalled here, after TryAdd, so concurrent
	// duplicates that lose the race do not pay the marshal cost.
	data, err := protocol.MarshalFileCommandFrame(frame)
	if err != nil {
		log.Debug().Err(err).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: marshal failed")
		return
	}
	if r.trySendToCandidates(frame.DST, frame.Nonce, candidates, data) {
		log.Debug().
			Str("dst", string(frame.DST)).
			Uint8("ttl", frame.TTL).
			Str("nonce", noncePrefix(frame.Nonce)).
			Msg("file_router: forwarded")
		return
	}
	log.Debug().
		Str("dst", string(frame.DST)).
		Int("routes_tried", len(candidates)).
		Str("nonce", noncePrefix(frame.Nonce)).
		Msg("file_router: all routes exhausted, relay forward failed")
}

// hasViableRoute is the cheap preflight for HandleInbound's step 2
// deliverability gate. It checks ONLY structural liveness of the
// cached routing snapshot:
//
//   - dst has at least one entry in snap.Routes;
//   - the entry is not withdrawn (Hops < HopsInfinity);
//   - the entry is not wall-clock expired (IsExpired(now));
//   - the entry's NextHop is not the local identity (no self-loop).
//
// It deliberately does NOT replicate the full step-8 candidate
// selection. In particular it does not see:
//
//   - split-horizon (excludeVia == incomingPeer) — relay path-only
//     constraint that must be re-checked at step 8;
//   - live next-hop metadata (peerRouteMeta) — a route whose
//     NextHop has no PeerRouteMeta (peer dropped, no version known)
//     is still counted as viable here but discarded at step 8.
//
// Passing this gate therefore does NOT guarantee a non-empty
// candidate set at step 8; the relay path runs collectRouteCandidates
// again and is the authoritative drop point for split-horizon and
// next-hop-metadata cases. The role of hasViableRoute is narrower:
// reject frames whose only cached routes are structurally dead
// (every entry withdrawn / expired / self-loop) before signature
// work and before any nonce commit, so a flood of authentic frames
// addressed to peers with stale-cached dead routes cannot burn slots
// in the bounded LRU. Step 8's collect-before-TryAdd handles the
// remaining cases (split-horizon erasure, missing next-hop metadata)
// without committing the nonce either.
func (r *Router) hasViableRoute(dst domain.PeerIdentity, now time.Time) bool {
	snap := r.routeSnap()
	routes, ok := snap.Routes[dst]
	if !ok {
		return false
	}
	for i := range routes {
		re := &routes[i]
		if re.IsWithdrawn() || re.IsExpired(now) {
			continue
		}
		if re.NextHop == r.localID {
			continue
		}
		return true
	}
	return false
}

// routeCandidate is a single viable next-hop for a destination, used by
// collectRouteCandidates to deduplicate the route-selection logic shared
// between inbound forwarding and outbound sending.
//
// protocolVersion, rawProtocolVersion and connectedAt are filled from
// the same PeerRouteMeta snapshot so the sort keys describe a single
// session generation.
//
// protocolVersion is the ranking key. It is bounded above by
// config.ProtocolVersion: the node-side inflated-version defence
// (trustedFileRouteVersion) caps any reported value above local at
// config.ProtocolVersion so that a peer claiming "newer" cannot WIN the
// protocolVersion-DESC primary key on the lie alone. rawProtocolVersion
// mirrors the actually-reported value as audit/diagnostic data so
// operators can distinguish "legitimately newer (cap applied)" from
// "v=local" off the journal. The node-side helper logs the cap event
// whenever it fires (DEBUG for small gaps that look like normal staged
// rollouts, WARN for gaps large enough to be suspicious — see
// inflationWarnGap), so the original reported value is recoverable
// from the journal regardless of suspicion level.
type routeCandidate struct {
	nextHop            domain.PeerIdentity
	hops               int
	protocolVersion    domain.ProtocolVersion
	rawProtocolVersion domain.ProtocolVersion
	connectedAt        time.Time
}

type peerRouteMetaResult struct {
	meta     PeerRouteMeta
	ok       bool
	resolved bool
}

// collectRouteCandidates returns active, non-self routes to dst sorted by
// the contract documented above routeCandidateLess (protocolVersion DESC →
// hops ASC → connectedAt ASC → nextHop). Expired, withdrawn, and
// self-referencing entries are filtered out. Returns nil when no viable
// route exists.
//
// This variant reads the cached routing snapshot. It is the right call
// for the hot transit path inside HandleInbound (per-frame, bounded by
// wire metadata, a ~1–1.5 s delay on a freshly added route is harmless).
// Locally-originated paths must use collectFreshRouteCandidates instead
// — see that function and RouterConfig.RouteLookup for why.
//
// excludeVia removes routes whose NextHop matches the given identity.
// This implements split-horizon forwarding on the transit path: a frame
// received from neighbor X must never be forwarded back to X, otherwise
// the two nodes ping-pong the same frame until TTL expires. Pass the
// empty PeerIdentity to disable exclusion (locally-originated sends and
// tests).
func (r *Router) collectRouteCandidates(dst, excludeVia domain.PeerIdentity) []routeCandidate {
	snap := r.routeSnap()

	routes, ok := snap.Routes[dst]
	if !ok {
		return nil
	}

	return r.rankRouteCandidates(routes, dst, excludeVia)
}

// collectFreshRouteCandidates is the locally-originated counterpart of
// collectRouteCandidates: it reads the routing table through the
// per-destination fresh oracle (RouterConfig.RouteLookup), so a route
// accepted right before SendFileCommand or ExplainRoute is visible
// immediately. The cached snapshot path would only see this route after
// the next dirty-flag publish (~1–1.5 s after the writer touched the
// table — routingSnapshotMinInterval's 1 s coalescing floor plus the next
// refresh tick), which produces the user-visible regression: isPeerReachable
// already reports reachable (it queries the fresh table), but the send
// fails because the cached snapshot has not republished yet.
//
// When RouteLookup is nil — tests and any caller that has not wired the
// oracle — falls back to the cached path, preserving pre-fix behaviour.
//
// excludeVia is plumbed through for symmetry with collectRouteCandidates,
// but the locally-originated callers always pass empty (no previous hop
// to split-horizon against).
func (r *Router) collectFreshRouteCandidates(dst, excludeVia domain.PeerIdentity) []routeCandidate {
	if r.routeLookup == nil {
		return r.collectRouteCandidates(dst, excludeVia)
	}
	routes := r.routeLookup(dst)
	if len(routes) == 0 {
		return nil
	}
	return r.rankRouteCandidates(routes, dst, excludeVia)
}

// rankRouteCandidates is the shared filter+rank kernel behind both
// collectRouteCandidates (cached snapshot) and collectFreshRouteCandidates
// (per-destination Lookup). Splitting the data source from the ranking
// keeps the comparator/dedup contract identical across hot and cold
// paths — the transit path and the locally-originated path can never
// disagree about which next-hop is best for the same input set.
//
// `routes` is the raw RouteEntry slice for `dst` from whichever source
// the caller chose. The function applies the same TTL/withdrawn/self/
// excludeVia filters as before, deduplicates by NextHop using the same
// comparator, and finally sorts via routeCandidateLess.
func (r *Router) rankRouteCandidates(routes []routing.RouteEntry, dst, excludeVia domain.PeerIdentity) []routeCandidate {
	if len(routes) == 0 {
		return nil
	}

	// Evaluate TTL against the current wall clock, not snap.TakenAt.
	// The cached routing snapshot's TakenAt is the publish moment of
	// the last refresh tick; the dirty-flag publisher only re-publishes
	// when the routing table mutates, so a finite-TTL route that
	// silently aged out between two TickTTL passes (10s cadence)
	// would still report IsExpired(snap.TakenAt) == false and end up
	// in the candidates list. trySendToCandidates does not re-check
	// expiry, so the file router would happily forward through a
	// dead route. Comparing against time.Now() here closes that gap
	// without changing the wire-side behaviour: at worst we drop a
	// candidate that is in fact still alive (the next refresh will
	// republish), but we never send through a candidate that is
	// already dead by wall-clock time.
	now := time.Now()

	var candidates []routeCandidate
	byNextHop := make(map[domain.PeerIdentity]int)
	metaCache := make(map[domain.PeerIdentity]peerRouteMetaResult)
	for i := range routes {
		re := &routes[i]
		if re.IsWithdrawn() || re.IsExpired(now) {
			continue
		}
		if re.NextHop == r.localID {
			continue
		}
		if excludeVia != "" && re.NextHop == excludeVia {
			continue
		}
		var meta PeerRouteMeta
		if r.peerRouteMeta != nil {
			result := metaCache[re.NextHop]
			if !result.resolved {
				// peerRouteMeta may hit node-level session/health state, so memoize
				// it per next-hop within this selection pass. This keeps the
				// peerMu acquisition cost flat regardless of how many route entries
				// collapse to the same next-hop.
				result.meta, result.ok = r.peerRouteMeta(re.NextHop)
				result.resolved = true
				metaCache[re.NextHop] = result
			}
			if !result.ok {
				continue
			}
			meta = result.meta
		}
		candidate := routeCandidate{
			nextHop:            re.NextHop,
			hops:               re.Hops,
			protocolVersion:    meta.ProtocolVersion,
			rawProtocolVersion: meta.RawProtocolVersion,
			connectedAt:        meta.ConnectedAt,
		}
		if idx, exists := byNextHop[re.NextHop]; exists {
			if routeCandidateLess(candidate, candidates[idx]) {
				candidates[idx] = candidate
			}
			continue
		}
		byNextHop[re.NextHop] = len(candidates)
		candidates = append(candidates, candidate)
	}

	// Sort order, in priority:
	//  1. protocolVersion DESC — newer protocol wins. A peer that speaks a
	//     higher version unlocks features the older path may silently drop,
	//     so we route through it even at the cost of an extra hop.
	//  2. hops ASC — among equal-version peers, closest first. Shorter paths
	//     mean fewer relays handling the bytes and a smaller blast radius
	//     for any single relay misbehaviour.
	//  3. connectedAt ASC — older connectedAt means longer uptime; a session
	//     that has held up longer is empirically more stable than one we just
	//     dialed seconds ago.
	//  4. nextHop lexicographic — final deterministic tie-break so the
	//     selection is reproducible across reads of the same routing snapshot.
	//
	// Insertion sort is intentional here: candidate sets are tiny, stability
	// matters, and this keeps the hot path allocation-free.
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && routeCandidateLess(candidates[j], candidates[j-1]); j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}

	return candidates
}

func noncePrefix(nonce string) string {
	if len(nonce) <= 16 {
		return nonce
	}
	return nonce[:16]
}

// RoutePlanEntry is the public projection of routeCandidate, used by
// diagnostic surfaces that need to show *why* a particular next-hop
// would be chosen (RPC `explainFileRoute`, console, CLI, SDK).
//
// The fields mirror the comparator keys exactly so a caller can rebuild
// the ranking decision from the wire output. ConnectedAt is left as a
// zero time when the underlying meta lookup did not return a known
// timestamp — callers should render that as "unknown" rather than
// inventing an "uptime" of `now - 0`.
type RoutePlanEntry struct {
	NextHop         domain.PeerIdentity
	Hops            int
	ProtocolVersion domain.ProtocolVersion
	ConnectedAt     time.Time
}

// ExplainRoute returns the file router's ranked next-hop plan for dst
// as it would be evaluated by an origin-side SendFileCommand call —
// no split-horizon (excludeVia is empty), so every viable next-hop is
// included.
//
// The result is the same ordered slice the live send path would walk:
// element 0 is the route the router would actually try first, the
// remaining elements are the fall-back order. Returns nil when no
// usable next-hop exists.
//
// Direct-session branch: SendFileCommand calls r.sessionSend(dst, …)
// unconditionally before consulting the routing table, so when dst itself
// is reachable as a file-capable peer it is *the* best entry by
// construction — relay candidates only matter as fall-back. ExplainRoute
// mirrors this by promoting a synthetic direct candidate to the head of
// the plan when peerRouteMeta(dst) reports the destination as usable.
// The synthetic entry is deduplicated against any direct route the
// routing table happens to carry (NextHop == dst, hops == 1) so we never
// double-list the same path.
//
// This method is read-only — it never enqueues, dials, or mutates state.
// It exists to power diagnostics; the live send paths still go through
// SendFileCommand and the inline forwarding stage of HandleInbound.
func (r *Router) ExplainRoute(dst domain.PeerIdentity) []RoutePlanEntry {
	var plan []RoutePlanEntry

	// 1. Synthetic direct candidate. Mirrors SendFileCommand step 1
	// (direct session attempted first, regardless of route-table
	// ranking). hops is conventionally set to 1 because a direct send
	// is one network hop away — it lets renderers compare the direct
	// entry against relay entries on the same scale.
	directReachable := false
	if r.peerRouteMeta != nil {
		if meta, ok := r.peerRouteMeta(dst); ok {
			plan = append(plan, RoutePlanEntry{
				NextHop:         dst,
				Hops:            1,
				ProtocolVersion: meta.ProtocolVersion,
				ConnectedAt:     meta.ConnectedAt,
			})
			directReachable = true
		}
	}

	// 2. Route-table fallback. Skip the routing-table direct entry
	// (NextHop == dst) when we have already accounted for it via the
	// synthetic candidate above — listing it twice would mislead a
	// console reader into thinking there are two independent paths to
	// the same destination. ExplainRoute mirrors SendFileCommand so it
	// must read the same source — the fresh per-destination oracle —
	// otherwise an operator's diagnostic would disagree with the live
	// send during the cached snapshot's republish window.
	candidates := r.collectFreshRouteCandidates(dst, "")
	for _, c := range candidates {
		if directReachable && c.nextHop == dst {
			continue
		}
		plan = append(plan, RoutePlanEntry{
			NextHop:         c.nextHop,
			Hops:            c.hops,
			ProtocolVersion: c.protocolVersion,
			ConnectedAt:     c.connectedAt,
		})
	}

	if len(plan) == 0 {
		return nil
	}
	return plan
}

// routeCandidateLess implements the comparator used by collectRouteCandidates.
// See the comment above the insertion-sort loop for the canonical ordering;
// the keys here mirror that contract one-to-one. Keeping the keys in this
// single function (instead of inlining them at the call site) is what makes
// the dedup branch in collectRouteCandidates safe: choosing the "better" of
// two routeCandidate values for the same next-hop must use the same total
// order as the final sort, otherwise dedup and sort can disagree about which
// route is best.
func routeCandidateLess(a, b routeCandidate) bool {
	// 1. protocolVersion DESC — higher version wins.
	if a.protocolVersion != b.protocolVersion {
		return a.protocolVersion > b.protocolVersion
	}
	// 2. hops ASC — fewer hops wins.
	if a.hops != b.hops {
		return a.hops < b.hops
	}
	// 3. connectedAt ASC — older connectedAt (longer uptime) wins. Treat
	// zero timestamps as "unknown" and sort them after known ones, so a
	// peer with a real uptime always beats one we have no health data for.
	if a.connectedAt.IsZero() != b.connectedAt.IsZero() {
		return !a.connectedAt.IsZero()
	}
	if !a.connectedAt.Equal(b.connectedAt) {
		return a.connectedAt.Before(b.connectedAt)
	}
	// 4. nextHop lexicographic — final deterministic tie-break.
	return a.nextHop < b.nextHop
}

// trySendToCandidates iterates route candidates in order and sends data to
// the first reachable next-hop. Returns true if delivery succeeded.
func (r *Router) trySendToCandidates(dst domain.PeerIdentity, nonce string, candidates []routeCandidate, data []byte) bool {
	for _, c := range candidates {
		if r.sessionSend(c.nextHop, data) {
			return true
		}
		log.Debug().
			Str("dst", string(dst)).
			Str("next_hop", string(c.nextHop)).
			Str("nonce", noncePrefix(nonce)).
			Msg("file_router: next hop send failed, trying next route")
	}
	return false
}

// SendFileCommand constructs and sends a FileCommandFrame to the destination.
// Used by FileTransferManager to send chunk_request, chunk_response, etc.
//
// Delivery strategy (first success wins):
//  1. Direct session to dst with file_transfer_v1 capability.
//  2. Route table lookup: iterate all active routes to dst in the order
//     defined by routeCandidateLess (protocolVersion DESC → hops ASC →
//     connectedAt ASC → nextHop), skip self-routes (next_hop == localID)
//     AND skip next_hop == dst (already attempted in step 1 — same socket,
//     same enqueue, the second attempt would be a wasted call and would
//     also disagree with what ExplainRoute reports as the plan), try
//     sessionSend to each surviving next_hop until one succeeds.
//  3. If no route exists or all next-hops are unreachable — log warning,
//     return error.
func (r *Router) SendFileCommand(
	dst domain.PeerIdentity,
	recipientBoxKeyBase64 string,
	payload domain.FileCommandPayload,
	senderPrivateKey ed25519.PrivateKey,
	encryptFn func(string, domain.FileCommandPayload) (string, error),
) error {
	encryptedPayload, err := encryptFn(recipientBoxKeyBase64, payload)
	if err != nil {
		return fmt.Errorf("encrypt file command: %w", err)
	}

	// Default TTL 10 keeps file commands comfortably above the expected mesh
	// diameter while still bounding loops and retries on pathological paths.
	frame := protocol.NewFileCommandFrame(r.localID, dst, fileCommandDefaultMaxTTL, encryptedPayload, senderPrivateKey)

	data, err := protocol.MarshalFileCommandFrame(frame)
	if err != nil {
		return fmt.Errorf("marshal file command frame: %w", err)
	}

	// 1. Try direct session to destination.
	if r.sessionSend(dst, data) {
		log.Debug().Str("dst", string(dst)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: sent via direct session")
		return nil
	}

	log.Debug().Str("dst", string(dst)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: no direct session, trying route table")

	// 2. Route table fallback: collect active routes ranked by
	// routeCandidateLess (see its godoc for the canonical order).
	// No split-horizon here — this is a locally-originated send, there
	// is no previous hop to exclude. Uses the per-destination fresh
	// oracle (RouterConfig.RouteLookup) so a route accepted right
	// before this user-initiated send is visible immediately. The
	// cached snapshot used by the transit path would only see the
	// route after the next dirty-flag publish (~1–1.5 s — the 1 s
	// coalescing floor plus the next refresh tick), which would
	// reproduce the original "isPeerReachable says yes, send fails"
	// regression on the very next pipeline step.
	candidates := r.collectFreshRouteCandidates(dst, "")

	// Filter out routing-table entries whose next_hop == dst: the
	// direct sessionSend(dst, data) above already attempted that exact
	// socket and failed (otherwise we would have returned). Re-trying
	// it through trySendToCandidates would be a no-op duplicate and
	// would also diverge from what ExplainRoute renders as the plan
	// (which deduplicates the same way against the synthetic direct
	// candidate). The filter is a stable in-place compaction so we do
	// not allocate a second slice.
	filtered := candidates[:0]
	for i := range candidates {
		if candidates[i].nextHop == dst {
			continue
		}
		filtered = append(filtered, candidates[i])
	}
	candidates = filtered

	if len(candidates) == 0 {
		log.Warn().
			Str("dst", string(dst)).
			Str("nonce", noncePrefix(frame.Nonce)).
			Msg("file_router: no viable route to peer")
		return fmt.Errorf("no route to %s", dst)
	}

	if r.trySendToCandidates(dst, frame.Nonce, candidates, data) {
		return nil
	}

	log.Warn().
		Str("dst", string(dst)).
		Int("routes_tried", len(candidates)).
		Str("nonce", noncePrefix(frame.Nonce)).
		Msg("file_router: all routes exhausted, file command not delivered")
	return fmt.Errorf("all %d routes to %s failed", len(candidates), dst)
}

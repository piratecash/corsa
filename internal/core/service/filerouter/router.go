package filerouter

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
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
// `config.ProtocolVersion` cannot be speaking the protocol we run, so
// its claim is clamped to 0 here to push the candidate to the bottom
// of the protocolVersion DESC sort. Treating this field as raw
// handshake metadata would re-open the traffic-capture attack the
// clamp was added to mitigate. Use 0 as "ranking-untrusted" rather
// than "version unknown" — the node-side helper logs at WARN level
// when it clamps, so the original reported value is recoverable from
// the journal if you actually need it.
//
// Together the two fields describe a single coherent socket — never
// an aggregate stitched across two — but the version key is a
// post-defence projection, not the wire-level reported value.
type PeerRouteMeta struct {
	ConnectedAt     time.Time
	ProtocolVersion domain.ProtocolVersion
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
type Router struct {
	nonceCache    NonceCache
	localID       domain.PeerIdentity
	isFullNode    func() bool
	routeSnap     func() routing.Snapshot
	peerRouteMeta func(domain.PeerIdentity) (PeerRouteMeta, bool)
	peerPubKey    func(domain.PeerIdentity) (ed25519.PublicKey, bool)
	sessionSend   func(dst domain.PeerIdentity, data []byte) bool
	localDeliver  func(frame protocol.FileCommandFrame)
}

// RouterConfig holds dependencies injected from Service into Router.
type RouterConfig struct {
	NonceCache    NonceCache
	LocalID       domain.PeerIdentity
	IsFullNode    func() bool
	RouteSnap     func() routing.Snapshot
	PeerRouteMeta func(domain.PeerIdentity) (PeerRouteMeta, bool)
	PeerPubKey    func(domain.PeerIdentity) (ed25519.PublicKey, bool)
	SessionSend   func(dst domain.PeerIdentity, data []byte) bool
	LocalDeliver  func(frame protocol.FileCommandFrame)
}

// NewRouter creates a Router with the provided dependencies.
func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		nonceCache:    cfg.NonceCache,
		localID:       cfg.LocalID,
		isFullNode:    cfg.IsFullNode,
		routeSnap:     cfg.RouteSnap,
		peerRouteMeta: cfg.PeerRouteMeta,
		peerPubKey:    cfg.PeerPubKey,
		sessionSend:   cfg.SessionSend,
		localDeliver:  cfg.LocalDeliver,
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
//  1. Anti-replay: nonce cache lookup — O(1).
//  2. Deliverability check: DST == self OR route to DST exists — O(1).
//  3. TTL check: decrement TTL by 1, then drop if TTL == 0 (loop
//     prevention). The frame has exhausted its hop budget.
//  4. Freshness: |now − Time| ≤ 5 min.
//  5. Nonce binding: SHA256(SRC||DST||Time||Payload) must match Nonce.
//  6. Signature: ed25519_verify(SRC_pubkey, Nonce, Signature).
//  7. Atomic nonce commit (TryAdd): exactly one goroutine proceeds.
//  8. Local delivery (DST == self): decrypt payload, dispatch to
//     FileTransferManager.
//  9. Relay restriction: only full nodes forward; client nodes drop
//     DST ≠ self.
//  10. Capability-aware forwarding: select best route whose next-hop
//     has file_transfer_v1 AND is not incomingPeer. TTL already
//     decremented at step 3.
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

	// 2. Deliverability check: is the frame addressed to us, or can we route it?
	isLocal := frame.DST == r.localID
	if !isLocal {
		snap := r.routeSnap()
		if _, hasRoute := snap.Routes[frame.DST]; !hasRoute {
			log.Debug().Str("dst", string(frame.DST)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: no route to destination, dropping")
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

	// 5. Signature verification: need sender's public key.
	senderPubKey, ok := r.peerPubKey(frame.SRC)
	if !ok {
		log.Debug().Str("src", string(frame.SRC)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: unknown sender public key")
		return
	}
	if err := protocol.VerifyFileCommandSignature(frame.Nonce, frame.Signature, senderPubKey); err != nil {
		log.Debug().Err(err).Str("src", string(frame.SRC)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: signature verification failed")
		return
	}

	// 6. Atomic anti-replay commit — only after all authenticity checks
	// have passed. TryAdd returns true if this goroutine is the first to
	// insert the nonce; concurrent deliveries of the same valid frame
	// (via multiple peers/transports) lose the race and are dropped here.
	// Using TryAdd instead of separate Has+Add closes the TOCTOU window
	// while still preventing cache poisoning (forged frames never reach
	// this point).
	if !r.nonceCache.TryAdd(frame.Nonce) {
		log.Debug().Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: concurrent duplicate, nonce already committed")
		return
	}

	// 7. Local delivery: DST == self.
	if isLocal {
		r.localDeliver(frame)
		return
	}

	// 8. Relay restriction: only full nodes forward.
	if !r.isFullNode() {
		log.Debug().Str("dst", string(frame.DST)).Str("nonce", noncePrefix(frame.Nonce)).Msg("file_router: client node, dropping non-local file command")
		return
	}

	// 10. Capability-aware forwarding (TTL already decremented at step 3).
	// Pass incomingPeer so forwardToNextHop applies split-horizon and
	// never reflects the frame back to the neighbor that just delivered it.
	r.forwardToNextHop(frame, incomingPeer)
}

// routeCandidate is a single viable next-hop for a destination, used by
// collectRouteCandidates to deduplicate the route-selection logic shared
// between inbound forwarding and outbound sending.
//
// protocolVersion and connectedAt are filled from the same PeerRouteMeta
// snapshot so the two sort keys describe the same session generation.
// They are zero-value friendly: protocolVersion == 0 loses to any
// reported version under DESC ordering but does not disqualify the
// candidate — we still prefer to ship the file over an unfavoured peer
// rather than drop the frame.
//
// Note that protocolVersion == 0 carries two distinct semantics that
// the sort comparator treats identically but operators must read
// differently: (a) the peer truly has not negotiated a version yet
// (pre-handshake conn, or peer reports zero), or (b) the node-side
// inflated-version defence clamped a higher reported value to 0
// because it exceeded `config.ProtocolVersion` (see
// PeerRouteMeta.ProtocolVersion godoc). Future ranking changes that
// want to act on "unknown legacy peer" must consult the node-side
// WARN logs to disambiguate, not reuse the zero check here.
type routeCandidate struct {
	nextHop         domain.PeerIdentity
	hops            int
	protocolVersion domain.ProtocolVersion
	connectedAt     time.Time
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

	var candidates []routeCandidate
	byNextHop := make(map[domain.PeerIdentity]int)
	metaCache := make(map[domain.PeerIdentity]peerRouteMetaResult)
	for i := range routes {
		re := &routes[i]
		if re.IsWithdrawn() || re.IsExpired(snap.TakenAt) {
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
			nextHop:         re.NextHop,
			hops:            re.Hops,
			protocolVersion: meta.ProtocolVersion,
			connectedAt:     meta.ConnectedAt,
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
// SendFileCommand / forwardToNextHop.
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
	// the same destination.
	candidates := r.collectRouteCandidates(dst, "")
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

// forwardToNextHop collects all active routes to DST and tries each
// next-hop, in the order defined by routeCandidateLess, until one
// succeeds. TTL is already decremented at step 3 of the pipeline — this
// function only selects the route and sends.
//
// excludeVia is the previous-hop identity: the neighbor that handed us
// this frame. It is filtered out of the candidate set so the transit
// node cannot reflect the frame back where it came from (split-horizon).
// Empty identity disables the filter.
func (r *Router) forwardToNextHop(frame protocol.FileCommandFrame, excludeVia domain.PeerIdentity) {
	candidates := r.collectRouteCandidates(frame.DST, excludeVia)
	if len(candidates) == 0 {
		log.Debug().
			Str("dst", string(frame.DST)).
			Str("exclude_via", string(excludeVia)).
			Str("nonce", noncePrefix(frame.Nonce)).
			Msg("file_router: no active route to destination (after split-horizon)")
		return
	}

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
	// is no previous hop to exclude.
	candidates := r.collectRouteCandidates(dst, "")

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

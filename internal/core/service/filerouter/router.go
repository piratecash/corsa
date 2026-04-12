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
	nonceCache   NonceCache
	localID      domain.PeerIdentity
	isFullNode   func() bool
	routeSnap    func() routing.Snapshot
	peerUsableAt func(domain.PeerIdentity) (time.Time, bool)
	peerPubKey   func(domain.PeerIdentity) (ed25519.PublicKey, bool)
	sessionSend  func(dst domain.PeerIdentity, data []byte) bool
	localDeliver func(frame protocol.FileCommandFrame)
}

// RouterConfig holds dependencies injected from Service into Router.
type RouterConfig struct {
	NonceCache   NonceCache
	LocalID      domain.PeerIdentity
	IsFullNode   func() bool
	RouteSnap    func() routing.Snapshot
	PeerUsableAt func(domain.PeerIdentity) (time.Time, bool)
	PeerPubKey   func(domain.PeerIdentity) (ed25519.PublicKey, bool)
	SessionSend  func(dst domain.PeerIdentity, data []byte) bool
	LocalDeliver func(frame protocol.FileCommandFrame)
}

// NewRouter creates a Router with the provided dependencies.
func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		nonceCache:   cfg.NonceCache,
		localID:      cfg.LocalID,
		isFullNode:   cfg.IsFullNode,
		routeSnap:    cfg.RouteSnap,
		peerUsableAt: cfg.PeerUsableAt,
		peerPubKey:   cfg.PeerPubKey,
		sessionSend:  cfg.SessionSend,
		localDeliver: cfg.LocalDeliver,
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
type routeCandidate struct {
	nextHop     domain.PeerIdentity
	hops        int
	connectedAt time.Time
}

type peerUsableAtResult struct {
	connectedAt time.Time
	ok          bool
	resolved    bool
}

// collectRouteCandidates returns active, non-self routes to dst sorted by
// hops ascending. Expired, withdrawn, and self-referencing entries are
// filtered out. Returns nil when no viable route exists.
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
	usableCache := make(map[domain.PeerIdentity]peerUsableAtResult)
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
		connectedAt := time.Time{}
		if r.peerUsableAt != nil {
			result := usableCache[re.NextHop]
			if !result.resolved {
				// peerUsableAt may hit node-level session/health state, so memoize
				// it per next-hop within this selection pass. This keeps the
				// current interface simple while avoiding repeated lock+clock work
				// for multiple route entries that collapse to the same next-hop.
				result.connectedAt, result.ok = r.peerUsableAt(re.NextHop)
				result.resolved = true
				usableCache[re.NextHop] = result
			}
			if !result.ok {
				continue
			}
			connectedAt = result.connectedAt
		}
		candidate := routeCandidate{
			nextHop:     re.NextHop,
			hops:        re.Hops,
			connectedAt: connectedAt,
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

	// Sort by hops ascending — try closest routes first.
	// For equal hop count prefer the peer that has been connected longer
	// (older connectedAt). This makes tie-breaking deterministic and leans
	// toward the more stable link when path length is identical.
	// Insertion sort is intentional here: candidate sets are tiny, stable
	// ordering matters, and this keeps the hot path allocation-free.
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

func routeCandidateLess(a, b routeCandidate) bool {
	if a.hops != b.hops {
		return a.hops < b.hops
	}
	if a.connectedAt.IsZero() != b.connectedAt.IsZero() {
		return !a.connectedAt.IsZero()
	}
	if !a.connectedAt.Equal(b.connectedAt) {
		return a.connectedAt.Before(b.connectedAt)
	}
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

// forwardToNextHop collects all active routes to DST sorted by hops and
// tries each next-hop until one succeeds. TTL is already decremented at
// step 3 of the pipeline — this function only selects the route and sends.
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
//  2. Route table lookup: iterate all active routes to dst sorted by hops,
//     skip self-routes (next_hop == localID), try sessionSend to each
//     next_hop until one succeeds.
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

	// 2. Route table fallback: collect active routes sorted by hops.
	// No split-horizon here — this is a locally-originated send, there
	// is no previous hop to exclude.
	candidates := r.collectRouteCandidates(dst, "")
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

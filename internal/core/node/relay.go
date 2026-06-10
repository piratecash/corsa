package node

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const (
	// defaultMaxHops limits the maximum number of hops a relay message can
	// traverse. When hop_count >= max_hops, the message is dropped.
	defaultMaxHops = 10

	// relayStateTTLSeconds is the initial RemainingTTL for a relayForwardState
	// entry. Decremented every second by the cleanup ticker. When it reaches 0
	// the entry is removed. 180 seconds = 3 minutes.
	relayStateTTLSeconds = 180

	// defaultHopAckBudgetSeconds caps how long a forwarded relay_message
	// waits for its relay_hop_ack before the reputation primitive
	// (routing.Table.MarkHopFailure) is fired against the
	// (Recipient, ForwardedTo) pair. 10 seconds is the pessimistic
	// round-trip budget documented in
	// docs/cluster-mesh/phase-3-multipath-reputation.md §4.9 decision #3
	// — long enough to cover a busy 3-hop relay through a slow link,
	// short enough that a black-hole uplink converges on the
	// 5-failure × 2-min cooldown well inside the 122-second Phase 2
	// passive-Bad timeline.
	//
	// The constant is intentionally not operator-tunable: the Phase 3
	// plan §4.9 keeps reputation knobs out of the operator surface
	// until field telemetry proves a concrete need. Tests inject a
	// shorter budget by stamping HopAckRemainingTicks directly on the
	// relayForwardState before calling tickHopAckBudgets.
	defaultHopAckBudgetSeconds = 10

	// MaxFailoverRetries caps how many alternative-uplink retries the
	// Phase 3 PR 12.3 failover machinery fires for a single relayed
	// message ID. Default 1 — one retry through a second-best uplink
	// — gives the worst-case per-message latency a predictable
	// ceiling: original budget (10 s) + one retry budget (10 s) =
	// 20 s before gossip fallback / periodic retryRelayDeliveries
	// takes over. Higher values (2+) inflate end-to-end latency on a
	// blocked path without proportional delivery benefits — gossip
	// fallback already covers the tail. See Phase 3 §4.9 decision
	// #7 for the rationale.
	MaxFailoverRetries = 1
)

// relayForwardState stores per-message forwarding context on an intermediate
// node. Each node knows only its own previous_hop and forwarded_to —
// no single message reveals the full path.
type relayForwardState struct {
	MessageID            string
	PreviousHop          domain.PeerAddress   // who sent this relay to me (transport address)
	ReceiptForwardTo     domain.PeerAddress   // = PreviousHop (where to send receipt back)
	ForwardedTo          domain.PeerAddress   // who I forwarded to (for loop detection)
	AbandonedForwardedTo []domain.PeerAddress // old ForwardedTo values from reroutes (stale ack rejection)
	Recipient            domain.PeerIdentity  // final recipient identity (for hop_ack route confirmation)
	RouteOrigin          domain.PeerIdentity  // route origin from routing decision (retained for plumbing-stability; IGNORED by hop_ack promotion post-Phase-A — see routing_hop_ack.go)
	HopCount             int                  // incremented on each hop
	RemainingTTL         int                  // seconds until cleanup (decremented by ticker)

	// Phase 3 PR 12.2 — hop-ack budget tracking for the reputation
	// primitive (routing.Table.MarkHopFailure).
	//
	// HopAckRemainingTicks counts down each second alongside
	// RemainingTTL; it reaches zero (and fires the timeout callback
	// via onHopAckTimeout) when the relay_hop_ack does not arrive
	// inside defaultHopAckBudgetSeconds. Zero means "not tracking" —
	// final-recipient and stored-locally states never wait for a
	// downstream ack and must NOT fire MarkHopFailure spuriously.
	//
	// Initialised by sendRelayMessage / sendRelayMessageWithOrigin /
	// the intermediate-forward branch of handleRelayMessage to
	// defaultHopAckBudgetSeconds (10s). The re-route path that
	// future PR 12.3 introduces resets it back to budget AND clears
	// HopAckObserved so the second-best uplink gets a fresh deadline.
	HopAckRemainingTicks int

	// HopAckObserved flips to true on EITHER (a) handleRelayHopAck
	// processing an ack for this MessageID via
	// relayStateStore.markHopAckObserved, OR (b) the budget timer
	// firing onHopAckTimeout. Acts as the one-shot guard: once
	// observed, the ticker stops scanning this entry, so a single
	// hop-ack timeline produces at most one MarkHopFailure call and
	// at most one MarkHopAck-triggered MarkHopAckSuccess call.
	//
	// Late hop_ack arriving AFTER the ticker fired remains a valid
	// positive observation — handleRelayHopAck still calls
	// confirmRouteViaHopAck → Table.ConfirmHopAck →
	// applyHopAckSuccess, which clears the cooldown the timeout
	// armed. The Observed flag only suppresses repeated MarkHopFailure
	// fires, not the eventual recovery path.
	HopAckObserved bool

	// Phase 3 PR 12.3 — multi-path failover support.
	//
	// RetryAttempt counts how many alternative-uplink retries the
	// failover machinery has fired against this message ID. 0 means
	// the original forward has not yet been retried; the
	// MaxFailoverRetries gate (see relay.go's constant block) caps
	// the total. A bounded retry budget keeps the worst-case
	// per-message latency predictable: with MaxFailoverRetries=1 a
	// single hop-ack timeline at most doubles the relay latency
	// (one budget window + one retry window) before gossip fallback
	// takes over via the existing periodic retryRelayDeliveries
	// path.
	RetryAttempt int

	// FrameLine is the JSON-serialised protocol.Frame that was sent
	// on the wire for this MessageID. Set by the three call sites
	// that stamp a forward (sendRelayMessage,
	// sendRelayMessageWithOrigin, handleRelayMessage's intermediate
	// forward branch); left empty for final-recipient and
	// stored-locally entries, which have no downstream forward to
	// retry. onRelayHopAckTimeout parses this line back to a
	// protocol.Frame and re-sends it through the second-best
	// uplink on failover.
	//
	// Storing the wire bytes here trades persistence size (≤2 KiB
	// per entry in practice) for the ability to retry at any hop
	// in the relay chain — intermediate hops never persist the
	// payload through any other path, so without FrameLine the
	// failover machinery would be origin-only and intermediate-
	// hop hop-ack timeouts could never recover the message.
	FrameLine string
}

// relayStateStore is a concurrency-safe store for relay forwarding state.
// It is embedded in Service but has its own mutex to avoid contention with
// the main Service.mu on high-throughput relay paths.
//
// Capacity limits: the store enforces maxRelayStates (global) and
// maxRelayStatesPerPeer (per PreviousHop) to prevent unbounded growth
// under relay floods. When a limit is hit, new entries are rejected —
// the relay message is silently dropped (same as dedupe).
type relayStateStore struct {
	mu       sync.Mutex
	states   map[string]*relayForwardState // keyed by message ID
	perPeer  map[domain.PeerAddress]int    // PreviousHop → count of active states
	stopCh   chan struct{}
	onEvict  func() // called after TTL ticker removes expired entries; set by Service for persistence
	rejected int64  // counter for monitoring: entries rejected due to capacity

	// Phase 3 PR 12.2 — hop-ack timeout callback. Fired by ttlTicker
	// for each relayForwardState whose HopAckRemainingTicks elapses
	// without HopAckObserved being set. The Service implementation
	// (node.Service.onRelayHopAckTimeout in routing_relay.go)
	// resolves ForwardedTo to a peer identity and calls
	// routing.Table.MarkHopFailure(Recipient, forwardedIdentity).
	//
	// Set once by Service after newRelayStateStore. Cleared on store
	// shutdown only by goroutine exit (no explicit teardown needed).
	// Invoked OUTSIDE rs.mu so the callback's Table.MarkHopFailure
	// (which takes routing.Table.mu) cannot deadlock against any
	// concurrent rs.mu holder.
	onHopAckTimeout func(relayForwardState)
}

func newRelayStateStore() *relayStateStore {
	return &relayStateStore{
		states:  make(map[string]*relayForwardState),
		perPeer: make(map[domain.PeerAddress]int),
		stopCh:  make(chan struct{}),
	}
}

// start launches the TTL decrement ticker. Call stop() to terminate.
func (rs *relayStateStore) start() {
	go rs.ttlTicker()
}

// stop terminates the TTL ticker goroutine.
func (rs *relayStateStore) stop() {
	close(rs.stopCh)
}

// ttlTicker decrements RemainingTTL every second and removes expired entries.
// Uses a local ticker — no wall-clock comparison between nodes.
// When entries are evicted and an onEvict callback is set, it is called to
// trigger durable persistence of the updated relay state.
//
// Phase 3 PR 12.2 — the same 1-second tick also drives hop-ack budget
// expiry (tickHopAckBudgets). Both scans run under rs.mu in turn; the
// hop-ack-timeout callback is fired OUTSIDE rs.mu for each state that
// crossed its budget on this tick, so a callback that takes
// routing.Table.mu (the production path through
// node.Service.onRelayHopAckTimeout) cannot deadlock against any
// concurrent rs.mu holder.
func (rs *relayStateStore) ttlTicker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if rs.decrementTTLsAndReport() && rs.onEvict != nil {
				rs.onEvict()
			}
			if rs.onHopAckTimeout != nil {
				for _, state := range rs.tickHopAckBudgets() {
					rs.onHopAckTimeout(state)
				}
			}
		case <-rs.stopCh:
			return
		}
	}
}

func (rs *relayStateStore) decrementTTLs() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for id, state := range rs.states {
		state.RemainingTTL--
		if state.RemainingTTL <= 0 {
			if state.PreviousHop != "" {
				rs.perPeer[state.PreviousHop]--
				if rs.perPeer[state.PreviousHop] <= 0 {
					delete(rs.perPeer, state.PreviousHop)
				}
			}
			delete(rs.states, id)
		}
	}
}

// hasSeen returns true if a relayForwardState already exists for this message
// ID — the message has already been relayed through this node.
func (rs *relayStateStore) hasSeen(messageID string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	_, ok := rs.states[messageID]
	return ok
}

// tryReserve atomically checks whether messageID has been seen and, if not,
// inserts a placeholder entry. Returns true when the caller wins the
// reservation (first claim). This replaces the racy hasSeen+store pattern
// by combining both into a single critical section.
//
// previousHop identifies the transport address of the peer that sent the
// relay. It is used for per-peer quota enforcement. Pass "" for origin
// entries (the node that created the relay message).
//
// Capacity enforcement: returns false (same as dedupe) when the global
// limit (maxRelayStates) or per-peer limit (maxRelayStatesPerPeer) is
// reached. The caller treats this identically to a duplicate — the relay
// message is silently dropped.
func (rs *relayStateStore) tryReserve(messageID string, previousHop domain.PeerAddress) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.states[messageID]; ok {
		return false
	}
	// Enforce global capacity.
	if len(rs.states) >= maxRelayStates {
		rs.rejected++
		return false
	}
	// Enforce per-peer capacity.
	if previousHop != "" && rs.perPeer[previousHop] >= maxRelayStatesPerPeer {
		rs.rejected++
		return false
	}
	rs.states[messageID] = &relayForwardState{
		MessageID:    messageID,
		PreviousHop:  previousHop,
		RemainingTTL: relayStateTTLSeconds,
	}
	if previousHop != "" {
		rs.perPeer[previousHop]++
	}
	return true
}

// release removes a previously reserved messageID. Used on error paths
// (max hops, client node, store rejection) so the slot does not block
// a legitimate retry from a different hop.
func (rs *relayStateStore) release(messageID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if state, ok := rs.states[messageID]; ok {
		if state.PreviousHop != "" {
			rs.perPeer[state.PreviousHop]--
			if rs.perPeer[state.PreviousHop] <= 0 {
				delete(rs.perPeer, state.PreviousHop)
			}
		}
		delete(rs.states, messageID)
	}
}

// updateState overwrites the placeholder created by tryReserve with a
// fully populated relayForwardState.
func (rs *relayStateStore) updateState(state *relayForwardState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.states[state.MessageID] = state
}

// store saves forwarding state for a relayed message. Returns false if the
// message was already seen (dedupe) or the store is at capacity.
func (rs *relayStateStore) store(state *relayForwardState) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if existing, ok := rs.states[state.MessageID]; ok {
		// Idempotent upsert: refresh forwarding fields and TTL so that
		// pending queue retries after route churn use the current best
		// path instead of being rejected.
		// PreviousHop and ReceiptForwardTo are preserved from the original
		// store — they describe the upstream path and must not change.
		//
		// Both ForwardedTo and RouteOrigin are always updated. A resend
		// may use the same next-hop peer but a different route origin
		// (e.g. after the origin node re-announces with a new SeqNo),
		// so checking only ForwardedTo would miss same-peer reroutes.
		//
		// AbandonedForwardedTo accumulates all old ForwardedTo addresses
		// from previous reroutes. This lets the stale ack guard reject
		// late acks from any abandoned next-hop, not just the most recent
		// one (e.g. table via A → table via B → gossip via C: both A and
		// B are stale and must be rejected).
		if existing.ForwardedTo != state.ForwardedTo && existing.ForwardedTo != "" {
			existing.AbandonedForwardedTo = append(existing.AbandonedForwardedTo, existing.ForwardedTo)
			// Real reroute: the previous next-hop is now in
			// AbandonedForwardedTo, so the new forward gets a fresh
			// hop-ack budget. The caller is responsible for staging
			// HopAckRemainingTicks=defaultHopAckBudgetSeconds and
			// HopAckObserved=false on the inbound state struct; we
			// propagate verbatim here so re-routes (Phase 3 PR 12.3
			// failover) land the deadline they want. Without the
			// reset a stale HopAckObserved=true from the previous
			// attempt would silently swallow the second-best uplink's
			// timeout.
			existing.HopAckRemainingTicks = state.HopAckRemainingTicks
			existing.HopAckObserved = state.HopAckObserved
		}
		existing.ForwardedTo = state.ForwardedTo
		existing.RouteOrigin = state.RouteOrigin
		existing.RemainingTTL = state.RemainingTTL
		// Phase 3 PR 12.3 — FrameLine is the wire payload kept for
		// failover retry. Same-peer upserts (e.g. duplicate retry
		// stamping the same downstream peer) overwrite the field
		// verbatim because the latest frame line IS what we want to
		// retry from; the value is content-identical for unchanged
		// forwards, so this is a no-op in steady state. RetryAttempt
		// is NOT updated here — recordFailoverRetry owns the counter
		// and is the only path that bumps it; an organic re-store
		// (TTL refresh by handleRelayMessage on dedupe) must not
		// reset RetryAttempt back to zero.
		existing.FrameLine = state.FrameLine
		return true
	}
	if len(rs.states) >= maxRelayStates {
		rs.rejected++
		return false
	}
	if state.PreviousHop != "" && rs.perPeer[state.PreviousHop] >= maxRelayStatesPerPeer {
		rs.rejected++
		return false
	}
	rs.states[state.MessageID] = state
	if state.PreviousHop != "" {
		rs.perPeer[state.PreviousHop]++
	}
	return true
}

// lookupForwardedTo returns the address the message was forwarded to,
// or "" if unknown. Used by hop_ack processing to verify that the ack
// sender matches the current forwarding destination — a mismatch
// indicates a stale ack from a previous route and should be ignored.
func (rs *relayStateStore) lookupForwardedTo(messageID string) domain.PeerAddress {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.ForwardedTo
}

// lookupReceiptForwardTo returns the address to forward a receipt back to
// for the given message ID, or "" if unknown.
func (rs *relayStateStore) lookupReceiptForwardTo(messageID string) domain.PeerAddress {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.ReceiptForwardTo
}

// lookupRecipient returns the final recipient identity for a relayed message,
// or "" if the message ID is unknown. Used by hop_ack processing to confirm
// the routing table entry for the recipient.
func (rs *relayStateStore) lookupRecipient(messageID string) domain.PeerIdentity {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.Recipient
}

// lookupRouteOrigin returns the route origin for a relayed message, or ""
// if the message ID is unknown or origin was not recorded. The value is
// surfaced for plumbing-stability but IGNORED by hop_ack route promotion
// post-Phase-A: confirmRouteViaHopAck matches on (Identity, NextHop)
// only because the routing table no longer keys on Origin. See
// routing_hop_ack.go for the migration note.
func (rs *relayStateStore) lookupRouteOrigin(messageID string) domain.PeerIdentity {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.RouteOrigin
}

// lookupAbandonedForwardedTo returns all ForwardedTo addresses from previous
// reroutes, or nil if the message was never rerouted.
func (rs *relayStateStore) lookupAbandonedForwardedTo(messageID string) []domain.PeerAddress {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return nil
	}
	return state.AbandonedForwardedTo
}

// snapshot returns a copy of all relay forward states for persistence.
func (rs *relayStateStore) snapshot() []relayForwardState {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]relayForwardState, 0, len(rs.states))
	for _, state := range rs.states {
		out = append(out, *state)
	}
	return out
}

// restore loads relay forward states from persisted data. Rebuilds per-peer
// counters from the loaded entries.
//
// Phase 3 PR 12.2 — restored entries get HopAckObserved=true so the
// post-restart ticker NEVER fires onRelayHopAckTimeout for them. The
// pre-restart relay round-trip may have been acked during downtime
// and we have no way to correlate that ack to the persisted entry
// (handleRelayHopAck needs the live message ID match in the store).
// Marking them observed converts the worst case from "spurious
// MarkHopFailure on an actually-delivered message" (false-positive
// reputation hit) into "we lose this one hop-ack timeline signal"
// (best-effort survival). The relay state itself stays available for
// receipt-forwarding lookups via the normal TTL window.
func (rs *relayStateStore) restore(states []relayForwardState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for i := range states {
		s := states[i]
		if s.RemainingTTL > 0 {
			s.HopAckObserved = true
			rs.states[s.MessageID] = &s
			if s.PreviousHop != "" {
				rs.perPeer[s.PreviousHop]++
			}
		}
	}
}

// count returns the number of active relay forward states.
func (rs *relayStateStore) count() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.states)
}

// tickHopAckBudgets decrements HopAckRemainingTicks on every tracked
// state and returns a snapshot of the states whose budget elapsed on
// this tick (RemainingTicks reached zero AND HopAckObserved was still
// false). The Observed flag is flipped to true on every returned
// state inside this critical section so the very next tick cannot
// re-fire the same timeout.
//
// Tracking convention (initialised by the relay store/forward call
// sites — sendRelayMessage / sendRelayMessageWithOrigin /
// handleRelayMessage's intermediate forward branch):
//
//   - HopAckRemainingTicks > 0 → tracking; decremented each tick.
//   - HopAckRemainingTicks == 0 → not tracking; left alone. Final-
//     recipient and stored-locally entries fall into this branch and
//     never produce timeout callbacks.
//   - HopAckObserved == true → ack arrived OR timeout already fired;
//     left alone. handleRelayHopAck flips this on success via
//     markHopAckObserved; the timeout-fire branch below flips it
//     itself.
//
// Returns nil when nothing fired, mirroring decrementTTLsAndReport's
// "report only if work happened" shape. The ticker in ttlTicker
// iterates the return value and invokes onHopAckTimeout outside the
// mutex; see ttlTicker's doc-comment for the lock-ordering rationale.
//
// Lock contract: takes rs.mu in W mode for the scan and the
// HopAckObserved flip; callbacks fire after this returns, without
// holding rs.mu.
func (rs *relayStateStore) tickHopAckBudgets() []relayForwardState {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	var fired []relayForwardState
	for _, state := range rs.states {
		if state.HopAckObserved {
			continue
		}
		if state.HopAckRemainingTicks <= 0 {
			continue
		}
		state.HopAckRemainingTicks--
		if state.HopAckRemainingTicks == 0 {
			state.HopAckObserved = true
			fired = append(fired, *state)
		}
	}
	return fired
}

// markHopAckObserved flips HopAckObserved=true for the message ID and
// returns true when a state was actually present. No-op when the
// message is unknown (already evicted by TTL, or never recorded).
//
// Production caller: handleRelayHopAck, BEFORE confirmRouteViaHopAck,
// so a tick that lands between handleRelayHopAck entry and our
// confirmRouteViaHopAck → Table.ConfirmHopAck path cannot fire the
// MarkHopFailure callback on the same hop-ack timeline.
//
// Lock contract: takes rs.mu in W mode for the field flip. Disjoint
// from routing.Table.mu so the caller can safely take that mutex
// afterward without nesting concerns.
func (rs *relayStateStore) markHopAckObserved(messageID string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return false
	}
	state.HopAckObserved = true
	return true
}

// recordFailoverRetry atomically transitions a relayForwardState to its
// next failover attempt: the previous ForwardedTo moves into
// AbandonedForwardedTo (so late acks from the abandoned uplink are
// still rejected by handleRelayHopAck's stale-ack guard), the new
// downstream address takes its place, RetryAttempt increments, the
// hop-ack budget is re-armed (HopAckRemainingTicks =
// defaultHopAckBudgetSeconds, HopAckObserved=false), and any prior
// RouteOrigin metadata is cleared because failover does not carry the
// original routing-table origin (the new path may have come from a
// different uplink claim altogether).
//
// Returns true on success — the message ID was present and the
// transition landed. Returns false when the message ID is unknown
// (TTL eviction beat the retry attempt) so the caller can log and
// skip the wire send. Callers MUST call this AFTER the wire send
// succeeds: a "stamp before send" design would leave a stale-pointing
// state if the send failed and the next retry would skip the just-
// abandoned uplink for no reason.
//
// All field mutations happen under rs.mu in W mode; the helper does
// NOT take any other mutex. The Phase 3 PR 12.3 failover handler in
// routing_relay.go is the only production caller — see
// onRelayHopAckTimeout's doc-comment for the lock-ordering contract
// against routing.Table.mu (rs.mu released BEFORE Table reads).
func (rs *relayStateStore) recordFailoverRetry(messageID string, newAddress domain.PeerAddress) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return false
	}
	if state.ForwardedTo != "" && state.ForwardedTo != newAddress {
		state.AbandonedForwardedTo = append(state.AbandonedForwardedTo, state.ForwardedTo)
	}
	state.ForwardedTo = newAddress
	state.RouteOrigin = ""
	state.RetryAttempt++
	state.HopAckRemainingTicks = defaultHopAckBudgetSeconds
	state.HopAckObserved = false
	return true
}

// retryAttemptCountFor returns the current RetryAttempt counter for
// the message ID, plus a bool indicating whether the state was
// present. Used by onRelayHopAckTimeout to gate MaxFailoverRetries
// without taking the rs.mu twice (the value snapshot from
// ttlHopAckBudgets is already stale when the callback fires — a
// concurrent recordFailoverRetry could have advanced the counter in
// between). Returns (0, false) for unknown message IDs.
func (rs *relayStateStore) retryAttemptCountFor(messageID string) (int, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return 0, false
	}
	return state.RetryAttempt, true
}

// removed returns true if any entries were removed during the last
// decrementTTLs call. Used by the TTL ticker to trigger persistence
// only when state actually changed.
func (rs *relayStateStore) decrementTTLsAndReport() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	removed := false
	for id, state := range rs.states {
		state.RemainingTTL--
		if state.RemainingTTL <= 0 {
			if state.PreviousHop != "" {
				rs.perPeer[state.PreviousHop]--
				if rs.perPeer[state.PreviousHop] <= 0 {
					delete(rs.perPeer, state.PreviousHop)
				}
			}
			delete(rs.states, id)
			removed = true
		}
	}
	return removed
}

// persistRelayState is now a NO-OP, retained as a vestige of the removed
// queue-state disk persistence. It still calls s.queuePersist.MarkDirty(),
// but the persister's Run goroutine is no longer started, so MarkDirty only
// sets a flag with no consumer — nothing is snapshotted or written to disk.
// Relay state (paths, retry, receipts) is in-memory only and does NOT survive
// a restart; recovery is sender-side end-to-end retry (see
// docs/protocol/relay.md INV-8). The call sites are kept (and this helper with
// them) so the dormant mechanism can be re-enabled or cleanly deleted in a
// few releases — see the queue-persist deprecation notes in node/service.go.
func (s *Service) persistRelayState() {
	s.queuePersist.MarkDirty()
}

// handleRelayMessage processes an incoming relay_message frame and returns the
// semantic hop-ack status. The caller is responsible for delivering the ack to
// the sender — this function does NOT send an ack itself, because the delivery
// mechanism depends on context (direct conn write in dispatchNetworkFrame vs
// enqueuePeerFrame in dispatchPeerSessionFrame).
//
// Return values:
//   - "delivered" — this node is the final recipient, message accepted and stored locally
//   - "forwarded" — message relayed to the next hop
//   - "stored"    — no capable next hop, message stored locally for later delivery
//   - ""          — message was dropped (dedupe, max hops, client node, rejected by
//     storeIncomingMessage); no ack needed
//
// handleRelayMessage processes an incoming relay_message frame.
//
// syncSession is an optional outbound peer session that can be reused for
// on-demand sender-key sync (fetch_contacts). When the relay arrives on an
// inbound connection, the caller looks up the outbound session to the same
// peer and passes it here — this avoids opening a new TCP connection and
// handles NATed peers whose transport address is not redialable. When the
// relay arrives on an outbound session that may be inside a
// peerSessionRequest read loop, the caller passes nil to avoid a deadlock
// on the single-reader inboxCh.
func (s *Service) handleRelayMessage(senderAddress domain.PeerAddress, syncSession *peerSession, frame protocol.Frame) string {
	messageID := frame.ID
	recipient := frame.Recipient
	hopCount := frame.HopCount
	maxHops := frame.MaxHops
	originSender := frame.Address // original message sender

	if maxHops <= 0 {
		maxHops = defaultMaxHops
	}

	// Relay is a DM-class-only mechanism. Both data DMs ("dm") and
	// control DMs (TopicControlDM) follow point-to-point semantics and
	// share this transport; non-DM payloads are rejected to prevent
	// them from being stored via a code path with different lifecycle
	// semantics.
	if !protocol.IsDMTopic(frame.Topic) {
		log.Debug().
			Str("id", messageID).
			Str("topic", frame.Topic).
			Msg("relay_drop_non_dm_topic")
		return ""
	}

	// Am I the final recipient?
	if recipient == s.identity.Address {
		// Dedupe at final hop: if relay state already exists, this is a
		// duplicate relay_message. Drop silently — the valid reverse-path
		// state from the first delivery must not be overwritten or released.
		if !s.relayStates.tryReserve(messageID, senderAddress) {
			log.Info().
				Str("id", messageID).
				Str("from", originSender).
				Msg("relay_drop_duplicate_final_hop")
			return ""
		}
		log.Info().Str("id", messageID).Str("sender", originSender).Str("sync_from", string(senderAddress)).Msg("relay_final_hop_delivering")
		// Upgrade the placeholder reservation to full relay state BEFORE
		// delivery so the receipt reverse path is available when
		// storeIncomingMessage fires emitDeliveryReceipt in a goroutine.
		s.relayStates.updateState(&relayForwardState{
			MessageID:        messageID,
			PreviousHop:      senderAddress,
			ReceiptForwardTo: senderAddress,
			ForwardedTo:      "",
			Recipient:        domain.PeerIdentity(recipient),
			HopCount:         hopCount,
			RemainingTTL:     relayStateTTLSeconds,
		})
		if !s.deliverRelayedMessage(senderAddress, syncSession, frame) {
			s.relayStates.release(messageID)
			log.Warn().
				Str("id", messageID).
				Str("from", originSender).
				Msg("relay_deliver_rejected_locally")
			return ""
		}
		s.persistRelayState()
		log.Info().
			Str("id", messageID).
			Str("from", originSender).
			Int("hops", hopCount).
			Msg("relay_delivered_locally")
		return "delivered"
	}

	// Dedupe: atomically reserve this message_id. If another goroutine
	// already claimed it, drop silently. tryReserve inserts a placeholder
	// entry so no concurrent handler can pass this point for the same ID.
	if !s.relayStates.tryReserve(messageID, senderAddress) {
		log.Debug().
			Str("id", messageID).
			Str("from", originSender).
			Msg("relay_drop_duplicate")
		return ""
	}

	// Max hops exceeded? Release the reservation.
	if hopCount >= maxHops {
		s.relayStates.release(messageID)
		log.Debug().
			Str("id", messageID).
			Int("hop_count", hopCount).
			Int("max_hops", maxHops).
			Msg("relay_drop_max_hops")
		return ""
	}

	// Only forward if this node can relay (full node).
	// Client nodes may be senders or final recipients, but never
	// intermediate relay hops.
	if !s.CanForward() {
		s.relayStates.release(messageID)
		log.Debug().
			Str("id", messageID).
			Msg("relay_drop_client_node")
		return ""
	}

	newHopCount := hopCount + 1

	// Build the forwarded frame.
	forwardFrame := protocol.Frame{
		Type:        "relay_message",
		ID:          messageID,
		Address:     originSender,
		Recipient:   recipient,
		Topic:       frame.Topic,
		Body:        frame.Body,
		Flag:        frame.Flag,
		CreatedAt:   frame.CreatedAt,
		TTLSeconds:  frame.TTLSeconds,
		HopCount:    newHopCount,
		MaxHops:     maxHops,
		PreviousHop: string(s.identity.Address),
	}

	// Try direct peer first — is the recipient directly connected?
	// A recipient identity may have multiple session addresses (reconnects,
	// address changes), so we try all matching sessions until one succeeds.
	forwardedTo := s.tryForwardToDirectPeer(domain.PeerIdentity(recipient), forwardFrame)

	// Table-directed relay (Phase 1.2): if no direct peer, consult the
	// routing table for a next-hop. This enables multi-hop relay chains
	// A→B→D where B uses the table to find D even though D is not the
	// final recipient's direct peer on B. Exclude the sender (from
	// incoming frame.PreviousHop) to prevent sending the message back.
	var tableRouteOrigin domain.PeerIdentity
	if forwardedTo == "" {
		result := s.tryForwardViaRoutingTable(s.runCtx, domain.PeerIdentity(recipient), forwardFrame, domain.PeerIdentity(frame.PreviousHop))
		forwardedTo = result.Address
		tableRouteOrigin = result.RouteOrigin
	}

	// If no direct peer and no table route, gossip to capable peers.
	if forwardedTo == "" {
		forwardedTo = s.relayViaGossip(forwardFrame, senderAddress)
	}

	if forwardedTo == "" {
		// Control DMs (TopicControlDM) intentionally have no node-level
		// store-and-forward fallback — they are not persisted in
		// chatlog, not appended to s.topics, and not tracked in
		// relayRetry (see docs/dm-commands.md "Storage rules for
		// control DMs"). Calling deliverRelayedMessage here would
		// pass through storeIncomingMessage which returns stored=true
		// for the wire-dedup `seen` set yet leaves no recoverable
		// envelope behind, and we would falsely ack "stored" upstream
		// — the previous hop would consider relay successful while
		// the control envelope is in fact lost. The application-level
		// retry on the sender (DMRouter.pendingDelete in slice B) is
		// the canonical recovery for this case; signalling failure
		// upstream lets the sender's retry budget treat this attempt
		// as a miss and try again.
		if frame.Topic == protocol.TopicControlDM {
			s.relayStates.release(messageID)
			log.Info().
				Str("id", messageID).
				Str("recipient", recipient).
				Msg("relay_control_dropped_no_next_hop")
			return ""
		}

		// No capable relay next hop available. deliverRelayedMessage
		// calls storeIncomingMessage, which stores the message AND
		// runs the normal routing path (gossip + push). This is
		// intentional: gossip runs unconditionally (INV-3) and may
		// reach the recipient through non-relay peers. The "stored"
		// status tells the previous hop that relay forwarding did not
		// happen, but the message is not stuck — gossip handles it.
		if !s.deliverRelayedMessage(senderAddress, syncSession, frame) {
			s.relayStates.release(messageID)
			log.Warn().
				Str("id", messageID).
				Str("recipient", recipient).
				Msg("relay_store_rejected_locally")
			return ""
		}
		// Update the reservation placeholder with full state so the
		// receipt reverse path works correctly.
		s.relayStates.updateState(&relayForwardState{
			MessageID:        messageID,
			PreviousHop:      senderAddress,
			ReceiptForwardTo: senderAddress,
			ForwardedTo:      "", // stored locally, not forwarded
			Recipient:        domain.PeerIdentity(recipient),
			HopCount:         newHopCount,
			RemainingTTL:     relayStateTTLSeconds,
		})
		s.persistRelayState()
		log.Info().
			Str("id", messageID).
			Str("recipient", recipient).
			Msg("relay_stored_no_capable_peers")
		return "stored"
	}

	// Update the reservation placeholder with full forwarding state.
	// ReceiptForwardTo uses the transport address (senderAddress), not the
	// identity fingerprint from frame.PreviousHop. This is critical:
	// sessionHasCapability() and sendReceiptToPeer() look up sessions by
	// transport address, not by identity.
	//
	// Phase 3 PR 12.2 — arm the hop-ack budget. ForwardedTo is non-empty
	// here (we successfully picked a downstream peer above), so the
	// ticker should track this state for a missing relay_hop_ack and
	// fire onRelayHopAckTimeout if it does not arrive in time. The
	// budget counter is in 1-second ticks driven by the same TTL
	// ticker that decrements RemainingTTL.
	//
	// Phase 3 PR 12.3 — stash the serialised forwardFrame so the
	// failover path in onRelayHopAckTimeout can re-emit it through a
	// second-best uplink without losing the relay payload. Marshal
	// errors are surfaced as a structured log; the relay still
	// succeeded so we keep the state but skip retry-eligibility (an
	// empty FrameLine short-circuits the failover branch and the
	// reputation/MarkHopFailure path still runs).
	forwardLine, marshalErr := protocol.MarshalFrameLine(forwardFrame)
	if marshalErr != nil {
		forwardLine = ""
		log.Warn().
			Err(marshalErr).
			Str("id", messageID).
			Str("recipient", recipient).
			Msg("relay_forward_frame_marshal_failed_retry_disabled")
	}
	s.relayStates.updateState(&relayForwardState{
		MessageID:            messageID,
		PreviousHop:          senderAddress,
		ReceiptForwardTo:     senderAddress,
		ForwardedTo:          forwardedTo,
		Recipient:            domain.PeerIdentity(recipient),
		RouteOrigin:          tableRouteOrigin,
		HopCount:             newHopCount,
		RemainingTTL:         relayStateTTLSeconds,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            forwardLine,
	})
	s.persistRelayState()

	log.Info().
		Str("id", messageID).
		Str("from", originSender).
		Str("to", recipient).
		Str("forwarded_to", string(forwardedTo)).
		Int("hop_count", newHopCount).
		Msg("relay_forwarded")
	return "forwarded"
}

// deliverRelayedMessage converts a relay_message frame into a local message
// store operation. Reuses storeIncomingMessage to avoid duplicating
// deduplication, persistence, and UI notification logic.
//
// senderAddress is the transport address of the previous relay hop. When
// storeIncomingMessage rejects the DM with ErrCodeUnknownSenderKey, this
// function syncs keys from the previous hop (which likely knows the
// sender's keys, since it already processed the message) and retries —
// matching the existing push_message / request_inbox behavior.
//
// Returns true if the message was accepted (stored locally), false if it was
// rejected (parse error, invalid signature, unknown key after sync, etc.).
// The caller uses this to decide the hop-ack status: a rejected message must
// NOT be reported as "delivered" or "stored" to the previous hop.
// deliverRelayedMessage stores a relayed message locally. When the message
// sender's keys are unknown, it attempts on-demand key sync from the
// previous hop.
//
// syncSession is an optional outbound session to the relay peer that can be
// reused for fetch_contacts without opening a new TCP connection. Pass nil
// when the caller is inside a peerSessionRequest read loop on the same
// session (single-reader constraint on inboxCh would deadlock).
func (s *Service) deliverRelayedMessage(senderAddress domain.PeerAddress, syncSession *peerSession, frame protocol.Frame) bool {
	msg, err := incomingMessageFromFrame(protocol.Frame{
		ID:         frame.ID,
		Topic:      frame.Topic,
		Address:    frame.Address,
		Recipient:  frame.Recipient,
		Flag:       frame.Flag,
		CreatedAt:  frame.CreatedAt,
		TTLSeconds: frame.TTLSeconds,
		Body:       frame.Body,
	})
	if err != nil {
		log.Warn().Err(err).Str("id", frame.ID).Msg("relay_deliver_parse_error")
		return false
	}
	stored, _, errCode := s.storeIncomingMessage(msg, false)
	if !stored && errCode == protocol.ErrCodeUnknownSenderKey && senderAddress != "" {
		log.Info().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", string(senderAddress)).Msg("relay_key_sync_start")
		// Use s.runCtx as the owning lifecycle ctx for the narrow key sync.
		// Threading ctx through handleRelayMessage/deliverRelayedMessage is
		// out of scope for Step 5 (would touch 25+ call sites in tests); the
		// service-run context still provides cancellation on shutdown, which
		// is the property CLAUDE.md protects.
		imported := s.syncSenderKeys(s.runCtx, senderAddress, syncSession)
		if imported == 0 {
			log.Warn().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", string(senderAddress)).Msg("relay_key_sync_no_contacts_imported")
		}
		stored, _, errCode = s.storeIncomingMessage(msg, false)
		if stored {
			log.Info().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", string(senderAddress)).Int("imported", imported).Msg("relay_deliver_after_key_sync")
		} else {
			log.Warn().Str("id", frame.ID).Str("sender", frame.Address).Str("code", errCode).Int("imported", imported).Msg("relay_key_sync_retry_failed")
		}
	}
	if !stored && errCode != "" {
		log.Warn().Str("id", frame.ID).Str("code", errCode).Msg("relay_deliver_rejected")
		return false
	}
	// Duplicate (stored=false, errCode="") means the message was already
	// delivered earlier. Treat as success so the relay layer sends an ack
	// and the upstream stops retrying — otherwise duplicates cause an
	// infinite retry loop that wastes bandwidth.
	if !stored {
		log.Debug().Str("id", frame.ID).Msg("relay_deliver_duplicate_accepted")
		return true
	}
	return true
}

// sendRelayHopAck sends a relay_hop_ack frame back to the peer that forwarded
// the relay message to us.
func (s *Service) sendRelayHopAck(peerAddress domain.PeerAddress, messageID, status string) {
	ackFrame := protocol.Frame{
		Type:   "relay_hop_ack",
		ID:     messageID,
		Status: status,
	}
	s.enqueuePeerFrame(peerAddress, ackFrame)
}

// handleRelayHopAck processes an incoming relay_hop_ack frame. It
// flips HopAckObserved on the matching relayForwardState (suppressing
// the Phase 3 PR 12.2 timeout callback for the CURRENT downstream
// attempt) and — when the ack confirms delivery/forwarding — promotes
// the route through the responding peer via confirmRouteViaHopAck.
//
// Stale-ack ordering (Phase 3 review fix): the stale-sender guard runs
// BEFORE markHopAckObserved, not just before route confirmation. After a
// timeout-driven failover A → B, store() rotated ForwardedTo to B and
// pushed A onto AbandonedForwardedTo. A late ack from the superseded
// downstream A must NOT suppress B's budget timer — otherwise B's
// attempt never times out and never fails over. Marking observed only
// for an ack that matches the CURRENT attempt keeps the timer tied to
// the path we are actually waiting on. An unknown message ID
// (TTL eviction beat the ack) or a stored-locally message (no
// downstream) has no live timer, so both are early no-ops here.
//
// For an ack that IS current, markHopAckObserved still runs BEFORE
// confirmRouteViaHopAck so a tick landing between the two cannot fire
// onRelayHopAckTimeout for a message we just observed. The state
// mutation is strictly inside relayStateStore.mu, disjoint from
// routing.Table.mu — no nested-lock concerns when confirmRouteViaHopAck
// reaches Table.ConfirmHopAck afterward.
//
// A current late ack arriving AFTER the timeout fired (HopAckObserved
// already true) is still processed: confirmRouteViaHopAck reaches
// Table.ConfirmHopAck → applyHopAckSuccess, which clears CooldownUntil
// and resets ConsecutiveFailures (PR 12.1 contract). That is the
// documented recovery path — positive evidence overrides the black-hole
// signal.
func (s *Service) handleRelayHopAck(senderAddress domain.PeerAddress, frame protocol.Frame) {
	log.Debug().
		Str("id", frame.ID).
		Str("from", string(senderAddress)).
		Str("status", frame.Status).
		Msg("relay_hop_ack_received")

	forwardedTo := s.relayStates.lookupForwardedTo(frame.ID)
	if forwardedTo == "" {
		// Unknown message ID (TTL eviction beat the ack) OR a message we
		// stored locally without forwarding. Either way there is no
		// downstream budget timer to suppress and no next-hop to confirm.
		return
	}
	routeOrigin := s.relayStates.lookupRouteOrigin(frame.ID)
	ackIdentity := s.resolvePeerIdentity(senderAddress)

	// Stale-ack guard — applied to the SUPPRESSION decision, not only to
	// route confirmation. A stale ack must not reset the current
	// attempt's timer.
	//
	// Table-routed path (RouteOrigin set): after a reroute store() updates
	// ForwardedTo to the new next-hop. A late ack from the old next-hop
	// has a different identity than ForwardedTo. Comparison is
	// identity-based (not transport address) because a peer may have
	// several sessions (inbound + outbound) and the ack can arrive on any.
	//
	// Table→gossip fallback (RouteOrigin empty): store() clears RouteOrigin
	// when a table-routed message is retried via gossip, so the
	// table-routed stale check above no longer fires. AbandonedForwardedTo
	// accumulates every superseded next-hop across reroutes; reject acks
	// from any of them (covers chains like A → B → gossip(C)).
	if s.isStaleHopAckSender(frame.ID, ackIdentity, forwardedTo, routeOrigin) {
		return
	}

	// Ack relates to the current downstream attempt. Suppress its budget
	// timer before confirming the route.
	s.relayStates.markHopAckObserved(frame.ID)

	if frame.Status == "delivered" || frame.Status == "forwarded" {
		recipient := s.relayStates.lookupRecipient(frame.ID)
		if recipient == "" {
			return
		}
		s.confirmRouteViaHopAck(recipient, senderAddress, routeOrigin)
	}
}

// isStaleHopAckSender reports whether an incoming relay_hop_ack came from
// a downstream that has been superseded for this message, so the ack must
// neither suppress the current attempt's budget timer nor promote a
// route. See handleRelayHopAck for the table-routed vs table→gossip
// fallback rationale. ackIdentity may be "" when the sender cannot be
// resolved — in that case we cannot prove staleness and treat the ack as
// current (the existing behaviour; resolution races are rare and a
// false-current only costs one extra timer suppression).
func (s *Service) isStaleHopAckSender(messageID string, ackIdentity domain.PeerIdentity, forwardedTo domain.PeerAddress, routeOrigin domain.PeerIdentity) bool {
	if ackIdentity == "" {
		return false
	}
	if routeOrigin != "" {
		fwdIdentity := s.resolvePeerIdentity(forwardedTo)
		if fwdIdentity != "" && fwdIdentity != ackIdentity {
			log.Debug().
				Str("id", messageID).
				Str("ack_identity", string(ackIdentity)).
				Str("forwarded_identity", string(fwdIdentity)).
				Msg("relay_hop_ack_stale_sender_ignored")
			return true
		}
		return false
	}
	for _, abandoned := range s.relayStates.lookupAbandonedForwardedTo(messageID) {
		if s.resolvePeerIdentity(abandoned) == ackIdentity {
			log.Debug().
				Str("id", messageID).
				Str("ack_identity", string(ackIdentity)).
				Str("abandoned_forwarded_identity", string(ackIdentity)).
				Msg("relay_hop_ack_stale_prev_route_ignored")
			return true
		}
	}
	return false
}

// tryForwardToDirectPeer iterates all outbound sessions whose peerIdentity
// matches the recipient. For each match it checks mesh_relay_v1 capability
// and tries to enqueue the frame. Returns the address of the first session
// that accepted the frame, or "" if none did. This avoids the problem where
// a random non-capable or non-writable session shadows a healthy direct path.
func (s *Service) tryForwardToDirectPeer(recipient domain.PeerIdentity, frame protocol.Frame) domain.PeerAddress {
	s.peerMu.RLock()
	var candidates []domain.PeerAddress
	for address, session := range s.sessions {
		if session.peerIdentity == recipient {
			candidates = append(candidates, address)
		}
	}
	s.peerMu.RUnlock()

	for _, address := range candidates {
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if s.enqueuePeerFrame(address, frame) {
			return address
		}
	}
	return domain.PeerAddress("")
}

// relayViaGossip forwards a relay_message to the top-scored peers that
// have the mesh_relay_v1 capability. The senderAddress is excluded to
// avoid sending the message back to where it came from. Per-peer rate
// limiting is applied: if a target's token bucket is exhausted, the
// frame is silently skipped for that target.
//
// Target selection: routingTargetsForRecipient(frame.Recipient) rather
// than routingTargets() so the route-quarantine gate
// (routingTargetsForRecipient) excludes any peer in quarantine UNLESS
// they are the recipient themselves. Without this redirect the gossip
// fallback re-opened the very transit path the table-route gate just
// closed: TableRouter returns RelayNextHop=nil because the only
// transit was quarantined, but relayViaGossip then forwards the same
// relay_message back to the same quarantined full node as a gossip
// target, using P as transit for someone else's recipient.
func (s *Service) relayViaGossip(frame protocol.Frame, excludeAddress domain.PeerAddress) domain.PeerAddress {
	targets := s.routingTargetsForRecipient(string(frame.Recipient))
	var forwardedTo domain.PeerAddress
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) || address == excludeAddress {
			continue
		}
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if !s.relayLimiter.allow(address) {
			continue
		}
		frame.PreviousHop = s.identity.Address
		if s.enqueuePeerFrame(address, frame) {
			if forwardedTo == "" {
				forwardedTo = address
			}
		}
	}
	return forwardedTo
}

// handleRelayReceipt processes a delivery receipt that may need to be
// forwarded back along the relay chain via receipt_forward_to lookup.
// Returns true if the receipt was forwarded via the relay chain (the caller
// should NOT additionally gossip it). Returns false if no relay path was
// found or the relay hop failed — the caller is responsible for gossip
// fallback in that case.
func (s *Service) handleRelayReceipt(receipt protocol.DeliveryReceipt) bool {
	forwardTo := s.relayStates.lookupReceiptForwardTo(string(receipt.MessageID))
	if forwardTo == "" {
		return false
	}

	// Forward the receipt one hop back toward the original sender.
	if s.sessionHasCapability(forwardTo, domain.CapMeshRelayV1) {
		if s.sendReceiptToPeer(forwardTo, receipt) {
			log.Debug().
				Str("message_id", string(receipt.MessageID)).
				Str("forward_to", string(forwardTo)).
				Msg("relay_receipt_forwarded")
			return true
		}
		log.Debug().
			Str("message_id", string(receipt.MessageID)).
			Str("forward_to", string(forwardTo)).
			Msg("relay_receipt_send_failed")
		return false
	}

	log.Debug().
		Str("message_id", string(receipt.MessageID)).
		Str("forward_to", string(forwardTo)).
		Msg("relay_receipt_hop_unavailable")
	return false
}

// tryRelayToCapableFullNodes sends relay_message to full-node peers with
// mesh_relay_v1 capability. This is a fire-and-forget optimization on top
// of the gossip baseline — the caller MUST still run gossip unconditionally.
//
// Only full nodes are targeted because client nodes cannot forward relay
// messages (handleRelayMessage drops at CanForward check). Receivers
// dedupe via seen[messageID] if gossip arrives first.
func (s *Service) tryRelayToCapableFullNodes(msg protocol.Envelope, targets []domain.PeerAddress) {
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if s.peerIsClientNode(address) {
			continue
		}
		if !s.relayLimiter.allow(address) {
			continue
		}
		s.sendRelayMessage(address, msg)
	}
}

// relayStatusFrame returns diagnostic information about the relay subsystem
// for the fetch_relay_status RPC command.
func (s *Service) relayStatusFrame() protocol.Frame {
	activeStates := s.relayStates.count()
	capablePeers := s.countCapablePeers(domain.CapMeshRelayV1)

	return protocol.Frame{
		Type:   "relay_status",
		Status: "ok",
		Count:  activeStates,
		Limit:  capablePeers,
	}
}

// countCapablePeers returns the number of unique connected peers (both
// outbound sessions and inbound connections) that have the specified
// capability. A peer that appears in both maps is counted once.
//
// Activation gate: mirrors sendFrameToIdentity. An outbound session is
// inserted into s.sessions before markPeerConnected seeds s.health, and an
// inbound NetCore is registered before hello/auth populates identity and
// capabilities; during those windows the peer is not yet active. Require a
// present, Connected, non-stalled health entry so relay_status.limit does
// not include peers the rest of the PR treats as not yet active.
func (s *Service) countCapablePeers(cap domain.Capability) int {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	now := time.Now().UTC()
	seen := make(map[domain.PeerIdentity]struct{})

	isActive := func(address domain.PeerAddress) bool {
		health := s.health[s.resolveHealthAddress(address)]
		if health == nil || !health.Connected {
			return false
		}
		return s.computePeerStateAtLocked(health, now) != peerStateStalled
	}

	for _, session := range s.sessions {
		if session == nil || !isActive(session.address) {
			continue
		}
		for _, c := range session.capabilities {
			if c == cap {
				seen[session.peerIdentity] = struct{}{}
				break
			}
		}
	}

	// Outbound NetCores are already counted via s.sessions above;
	// skip them here so pre-activation outbound entries cannot
	// inflate the capable-peer count.
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if _, dup := seen[info.identity]; dup {
			return true
		}
		if !isActive(info.address) {
			return true
		}
		if info.HasCapability(cap) {
			seen[info.identity] = struct{}{}
		}
		return true
	})

	return len(seen)
}

// sendRelayMessage creates a relay_message frame from an envelope and sends
// it to the specified peer. Returns true if the frame was enqueued (live
// session) or queued (persistent fallback). Used when the routing decision
// indicates a relay-capable next hop.
func (s *Service) sendRelayMessage(address domain.PeerAddress, msg protocol.Envelope) bool {
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          string(msg.ID),
		Address:     msg.Sender,
		Recipient:   msg.Recipient,
		Topic:       msg.Topic,
		Flag:        string(msg.Flag),
		CreatedAt:   msg.CreatedAt.UTC().Format(time.RFC3339),
		TTLSeconds:  msg.TTLSeconds,
		Body:        string(msg.Payload),
		HopCount:    1,
		MaxHops:     defaultMaxHops,
		PreviousHop: s.identity.Address,
	}

	sent := s.enqueuePeerFrame(address, frame)
	if !sent {
		sent = s.queuePeerFrame(address, frame)
		if sent {
			log.Debug().
				Str("id", string(msg.ID)).
				Str("recipient", msg.Recipient).
				Str("peer", string(address)).
				Str("mode", "queued").
				Msg("relay_message_queued")
		}
	}

	if !sent {
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "dropped").
			Msg("relay_message_dropped")
		return false
	}

	// Store forwarding state for dedupe and receipt routing.
	// This is the origin node (hop_count=1), so ReceiptForwardTo is empty:
	// the receipt terminates here — there is no previous hop to forward to.
	// On intermediate nodes, handleRelayMessage stores the sender's transport
	// address as ReceiptForwardTo.
	//
	// Phase 3 PR 12.2 — origin path arms the hop-ack budget so the
	// reputation primitive learns about (Recipient, address) pairs
	// that never produce a hop_ack. See handleRelayMessage's
	// intermediate-forward branch for the matching arm on relayed
	// traffic.
	//
	// Phase 3 PR 12.3 — stash the serialised origin frame for
	// failover retry. Marshal errors disable retry but still let the
	// reputation/MarkHopFailure path fire on timeout.
	originLine, marshalErr := protocol.MarshalFrameLine(frame)
	if marshalErr != nil {
		originLine = ""
		log.Warn().
			Err(marshalErr).
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Msg("relay_origin_frame_marshal_failed_retry_disabled")
	}
	s.relayStates.store(&relayForwardState{
		MessageID:            string(msg.ID),
		PreviousHop:          "",
		ReceiptForwardTo:     "",
		ForwardedTo:          address,
		Recipient:            domain.PeerIdentity(msg.Recipient),
		HopCount:             1,
		RemainingTTL:         relayStateTTLSeconds,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            originLine,
	})
	s.persistRelayState()
	log.Debug().
		Str("id", string(msg.ID)).
		Str("recipient", msg.Recipient).
		Str("peer", string(address)).
		Str("mode", "session").
		Msg("relay_message_sent")
	return true
}

// sendRelayMessageWithOrigin is the table-directed variant of sendRelayMessage.
// It plumbs the route Origin into relayForwardState alongside the next-hop;
// this field is retained for caller-stability but is IGNORED for route
// promotion post-Phase-A — confirmRouteViaHopAck matches on (Identity,
// NextHop) only because the routing table no longer keys on Origin. The
// gossip path (sendRelayMessage) leaves RouteOrigin empty because gossip
// delivery is not scoped to a specific routing-table entry.
func (s *Service) sendRelayMessageWithOrigin(address domain.PeerAddress, msg protocol.Envelope, routeOrigin domain.PeerIdentity) bool {
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          string(msg.ID),
		Address:     msg.Sender,
		Recipient:   msg.Recipient,
		Topic:       msg.Topic,
		Flag:        string(msg.Flag),
		CreatedAt:   msg.CreatedAt.UTC().Format(time.RFC3339),
		TTLSeconds:  msg.TTLSeconds,
		Body:        string(msg.Payload),
		HopCount:    1,
		MaxHops:     defaultMaxHops,
		PreviousHop: s.identity.Address,
	}

	sent := s.enqueuePeerFrame(address, frame)
	if !sent {
		sent = s.queuePeerFrame(address, frame)
		if sent {
			log.Debug().
				Str("id", string(msg.ID)).
				Str("recipient", msg.Recipient).
				Str("peer", string(address)).
				Str("mode", "queued").
				Msg("relay_message_queued")
		}
	}

	if !sent {
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "dropped").
			Msg("relay_message_dropped")
		return false
	}

	// Phase 3 PR 12.2 — table-directed origin send arms the hop-ack
	// budget. Same rationale as sendRelayMessage / handleRelayMessage's
	// intermediate-forward branch.
	//
	// Phase 3 PR 12.3 — stash the serialised origin frame for
	// failover retry through alternative table routes if the
	// originally-chosen uplink times out without a hop_ack.
	originLine, marshalErr := protocol.MarshalFrameLine(frame)
	if marshalErr != nil {
		originLine = ""
		log.Warn().
			Err(marshalErr).
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Msg("relay_origin_frame_marshal_failed_retry_disabled")
	}
	stored := s.relayStates.store(&relayForwardState{
		MessageID:            string(msg.ID),
		PreviousHop:          "",
		ReceiptForwardTo:     "",
		ForwardedTo:          address,
		Recipient:            domain.PeerIdentity(msg.Recipient),
		RouteOrigin:          routeOrigin,
		HopCount:             1,
		RemainingTTL:         relayStateTTLSeconds,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            originLine,
	})
	if !stored {
		log.Warn().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Msg("relay_state_store_failed_outbound")
		return false
	}
	s.persistRelayState()
	log.Debug().
		Str("id", string(msg.ID)).
		Str("recipient", msg.Recipient).
		Str("peer", string(address)).
		Str("origin", string(routeOrigin)).
		Str("mode", "session").
		Msg("relay_message_sent_table_directed")
	return true
}

func (s *Service) gossipReceipt(receipt protocol.DeliveryReceipt) {
	defer crashlog.DeferRecover()
	for _, address := range s.routingTargetsForRecipient(receipt.Recipient) {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		s.dispatchGossipSend(func() { s.sendReceiptToPeer(address, receipt) })
	}
}

func (s *Service) sendReceiptToPeer(address domain.PeerAddress, receipt protocol.DeliveryReceipt) bool {
	defer crashlog.DeferRecover()
	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          string(receipt.MessageID),
		Address:     receipt.Sender,
		Recipient:   receipt.Recipient,
		Status:      receipt.Status,
		DeliveredAt: receipt.DeliveredAt.UTC().Format(time.RFC3339),
	}
	if s.enqueuePeerFrame(address, frame) {
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("peer", string(address)).Str("mode", "session").Str("status", receipt.Status).Msg("route_receipt_attempt")
		return true
	}
	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("peer", string(address)).Str("mode", "queued").Str("status", receipt.Status).Msg("route_receipt_attempt")
		return true
	}
	log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("peer", string(address)).Str("mode", "dropped").Str("status", receipt.Status).Msg("route_receipt_attempt")
	return false
}

func (s *Service) retryRelayDeliveries() {
	if !s.CanForward() {
		return
	}

	now := time.Now().UTC()
	for _, msg := range s.retryableRelayMessages(now) {
		attempts := s.noteRelayAttempt(relayMessageKey(msg.ID), now)
		decision := s.router.Route(msg)
		// Build the gossip-target string slice ONLY when debug logging is
		// enabled — it is purely for the log line, and at production log levels
		// this per-message allocation was pure churn on the 2s retry loop.
		if e := log.Debug(); e.Enabled() {
			targets := make([]string, len(decision.GossipTargets))
			for i, t := range decision.GossipTargets {
				targets[i] = string(t)
			}
			e.Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Int("attempts", attempts).Strs("gossip_targets", targets).Msg("relay_retry_message")
		}
		// Inline: executeGossipTargets only enqueues jobs on the bounded
		// gossip-dispatch pool now. The per-message goroutine that used
		// to live here was the retry loop's storm amplifier — N backlog
		// messages × fan-out targets in fresh goroutines every 2s tick
		// permanently ratcheted the runtime's goroutine free-list and
		// stack high-water mark (see gossip_dispatch.go header).
		s.executeGossipTargets(msg, decision.GossipTargets)
		// Table-directed relay (Phase 1.2): mirror the logic in
		// storeIncomingMessage — use the routing table when a next-hop
		// is known, fall back to blind gossip relay otherwise.
		if decision.RelayNextHop != nil {
			s.sendTableDirectedRelay(s.runCtx, msg, *decision.RelayNextHop, decision.RelayNextHopAddress, decision.RelayRouteOrigin, decision.RelayNextHopHops)
		} else {
			s.tryRelayToCapableFullNodes(msg, decision.GossipTargets)
		}
	}
	for _, receipt := range s.retryableRelayReceipts(now) {
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Int("attempts", s.noteRelayAttempt(relayReceiptKey(receipt), now)).Msg("relay_retry_receipt")
		if !s.handleRelayReceipt(receipt) {
			// Inline for the same reason as executeGossipTargets above:
			// the body is now a cheap enqueue loop on the dispatch pool.
			s.gossipReceipt(receipt)
		}
	}
}

func (s *Service) retryableRelayMessages(now time.Time) []protocol.Envelope {
	// Delivery-domain outer, gossip-domain inner per the canonical
	// deliveryMu → gossipMu order.  relayRetry and receipts live under
	// deliveryMu; topics["dm"] under gossipMu.  Snapshot topics["dm"] under
	// gossipMu.RLock inside the deliveryMu section so the two views stay
	// consistent with the current relayRetry state.
	log.Trace().Str("site", "retryableRelayMessages").Str("phase", "lock_wait").Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "retryableRelayMessages").Str("phase", "lock_held").Msg("delivery_mu_writer")

	s.gossipMu.RLock()
	// Reuse the per-Service scratch buffer instead of allocating a fresh copy
	// of the whole topics["dm"] slice every 2s cycle (see relayRetryScratch).
	// Safe: held under s.deliveryMu.Lock and never returned (out is separate).
	dm := s.topics["dm"]
	// Drop an oversized scratch left by a one-off retry spike so its backing
	// array — and the Envelopes/payloads it pins — is not retained forever.
	if cap(s.relayRetryScratch) > 2*len(dm)+64 {
		s.relayRetryScratch = nil
	}
	s.relayRetryScratch = append(s.relayRetryScratch[:0], dm...)
	// Release any Envelopes left in the backing array beyond the new length so
	// a shrunk topics["dm"] does not keep their (possibly large) payloads alive
	// until the next, larger, retry cycle.
	clear(s.relayRetryScratch[len(s.relayRetryScratch):cap(s.relayRetryScratch)])
	items := s.relayRetryScratch
	s.gossipMu.RUnlock()
	out := make([]protocol.Envelope, 0)
	beforeLen := len(s.relayRetry)
	for _, msg := range items {
		key := relayMessageKey(msg.ID)
		if msg.Recipient == "" || msg.Recipient == "*" || msg.Recipient == s.identity.Address {
			delete(s.relayRetry, key)
			continue
		}
		if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
			delete(s.relayRetry, key)
			continue
		}
		if s.hasReceiptForMessageLocked(msg.Sender, msg.ID) {
			delete(s.relayRetry, key)
			continue
		}
		if !shouldRetryRelayLocked(s.relayRetry, key, now) {
			continue
		}
		out = append(out, msg)
	}
	afterLen := len(s.relayRetry)
	if beforeLen != afterLen {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "retryableRelayMessages").Str("phase", "lock_released_dirty").Msg("delivery_mu_writer")
		s.queuePersist.MarkDirty()
		return out
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "retryableRelayMessages").Str("phase", "lock_released").Msg("delivery_mu_writer")
	return out
}

func (s *Service) retryableRelayReceipts(now time.Time) []protocol.DeliveryReceipt {
	log.Trace().Str("site", "retryableRelayReceipts").Str("phase", "lock_wait").Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "retryableRelayReceipts").Str("phase", "lock_held").Msg("delivery_mu_writer")

	out := make([]protocol.DeliveryReceipt, 0)
	beforeLen := len(s.relayRetry)
	for _, list := range s.receipts {
		for _, receipt := range list {
			key := relayReceiptKey(receipt)
			if receipt.Recipient == "" || receipt.Recipient == s.identity.Address {
				delete(s.relayRetry, key)
				continue
			}
			if !shouldRetryRelayLocked(s.relayRetry, key, now) {
				continue
			}
			out = append(out, receipt)
		}
	}
	afterLen := len(s.relayRetry)
	if beforeLen != afterLen {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "retryableRelayReceipts").Str("phase", "lock_released_dirty").Msg("delivery_mu_writer")
		s.queuePersist.MarkDirty()
		return out
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "retryableRelayReceipts").Str("phase", "lock_released").Msg("delivery_mu_writer")
	return out
}

func shouldRetryRelayLocked(items map[string]relayAttempt, key string, now time.Time) bool {
	state, ok := items[key]
	if !ok {
		return false
	}
	firstSeen := state.FirstSeen
	if firstSeen.IsZero() {
		firstSeen = now
	}
	if now.Sub(firstSeen) > relayRetryTTL {
		delete(items, key)
		return false
	}
	if state.LastAttempt.IsZero() {
		return true
	}
	return now.Sub(state.LastAttempt) >= relayRetryBackoff(state.Attempts)
}

func relayRetryBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 5 * time.Second
	}
	backoff := 5 * time.Second
	for i := 1; i < attempts; i++ {
		backoff *= 2
		if backoff >= 30*time.Second {
			return 30 * time.Second
		}
	}
	return backoff
}

func (s *Service) noteRelayAttempt(key string, now time.Time) int {
	log.Trace().Str("site", "noteRelayAttempt").Str("phase", "lock_wait").Str("key", key).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "noteRelayAttempt").Str("phase", "lock_held").Str("key", key).Msg("delivery_mu_writer")
	state := s.relayRetry[key]
	if state.FirstSeen.IsZero() {
		state.FirstSeen = now
	}
	state.LastAttempt = now
	state.Attempts++
	s.relayRetry[key] = state
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "noteRelayAttempt").Str("phase", "lock_released").Str("key", key).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
	return state.Attempts
}

func (s *Service) trackRelayMessage(msg protocol.Envelope) {
	// Control DMs (TopicControlDM) intentionally bypass the node-level
	// relayRetry tracker: their retry policy lives at the application
	// layer (DMRouter.pendingDelete with exponential backoff). That
	// retry queue is in-memory only in the current implementation —
	// see docs/dm-commands.md §"Acknowledgement and retry"; restart
	// abandons in-flight retries, and JSON persistence is a tracked
	// follow-up. Tracking control DMs here would create dead state
	// either way — retryableRelayMessages and queueStateSnapshotLocked
	// only consult topics["dm"], so a control envelope put into
	// relayRetry would never get retried and would only burn the
	// maxRelayRetryEntries quota until tombstone TTL.
	if msg.Topic != "dm" || msg.Recipient == "" || msg.Recipient == "*" {
		return
	}
	log.Trace().Str("site", "trackRelayMessage").Str("phase", "lock_wait").Str("msg_id", string(msg.ID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "trackRelayMessage").Str("phase", "lock_held").Str("msg_id", string(msg.ID)).Msg("delivery_mu_writer")
	key := relayMessageKey(msg.ID)
	state := s.relayRetry[key]
	if state.FirstSeen.IsZero() {
		// Enforce capacity: reject new entries when the retry map is full.
		if len(s.relayRetry) >= maxRelayRetryEntries {
			s.deliveryMu.Unlock()
			log.Trace().Str("site", "trackRelayMessage").Str("phase", "lock_released_full").Str("msg_id", string(msg.ID)).Msg("delivery_mu_writer")
			return
		}
		state.FirstSeen = time.Now().UTC()
		s.relayRetry[key] = state
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "trackRelayMessage").Str("phase", "lock_released_new").Str("msg_id", string(msg.ID)).Msg("delivery_mu_writer")
		s.queuePersist.MarkDirty()
		return
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "trackRelayMessage").Str("phase", "lock_released").Str("msg_id", string(msg.ID)).Msg("delivery_mu_writer")
}

func (s *Service) trackRelayReceipt(receipt protocol.DeliveryReceipt) {
	log.Trace().Str("site", "trackRelayReceipt").Str("phase", "lock_wait").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "trackRelayReceipt").Str("phase", "lock_held").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	key := relayReceiptKey(receipt)
	state := s.relayRetry[key]
	dirty := false
	if state.FirstSeen.IsZero() {
		// Enforce capacity: reject new entries when the retry map is full.
		// Deletions below (message key cleanup) still proceed.
		if len(s.relayRetry) >= maxRelayRetryEntries {
			// Still try to clean up the message key below before returning.
		} else {
			state.FirstSeen = time.Now().UTC()
			s.relayRetry[key] = state
			dirty = true
		}
	}
	if _, ok := s.relayRetry[relayMessageKey(receipt.MessageID)]; ok {
		delete(s.relayRetry, relayMessageKey(receipt.MessageID))
		dirty = true
	}
	if !dirty {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "trackRelayReceipt").Str("phase", "lock_released_clean").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
		return
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "trackRelayReceipt").Str("phase", "lock_released").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
}

func relayMessageKey(id protocol.MessageID) string {
	return "msg|" + string(id)
}

func relayReceiptKey(receipt protocol.DeliveryReceipt) string {
	return "receipt|" + receipt.Recipient + "|" + string(receipt.MessageID) + "|" + receipt.Status
}

// hasReceiptForMessageLocked reports whether s.receipts already contains a
// delivery receipt for (originalSender, messageID).  Caller MUST hold
// s.deliveryMu (RLock or Lock).
func (s *Service) hasReceiptForMessageLocked(originalSender string, messageID protocol.MessageID) bool {
	for _, receipt := range s.receipts[originalSender] {
		if receipt.MessageID == messageID {
			return true
		}
	}
	return false
}

func (s *Service) deleteBacklogMessageForRecipient(recipient string, messageID protocol.MessageID) int {
	// Cross-domain: deliveryMu OUTER (covers relayRetry), gossipMu INNER
	// (covers topics).  Canonical deliveryMu → gossipMu order.  The loop
	// mutates both sets in lockstep so the two must be held together;
	// nesting keeps the matching relayRetry entry and the corresponding
	// topics["dm"] envelope from diverging mid-iteration.
	log.Trace().Str("site", "deleteBacklogMessageForRecipient").Str("phase", "lock_wait").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "deleteBacklogMessageForRecipient").Str("phase", "lock_held").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.gossipMu.Lock()
	before := len(s.topics["dm"])
	filtered := s.topics["dm"][:0]
	for _, msg := range s.topics["dm"] {
		if msg.ID == messageID && msg.Recipient == recipient {
			delete(s.relayRetry, relayMessageKey(msg.ID))
			continue
		}
		filtered = append(filtered, msg)
	}
	if len(filtered) == 0 {
		delete(s.topics, "dm")
	} else {
		s.topics["dm"] = filtered
	}
	s.gossipMu.Unlock()
	removed := before - len(filtered)
	if removed <= 0 {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "deleteBacklogMessageForRecipient").Str("phase", "lock_released_empty").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
		return 0
	}
	log.Debug().Str("node", s.identity.Address).Str("recipient", recipient).Str("id", string(messageID)).Int("before", before).Int("after", len(filtered)).Int("removed", removed).Msg("deleteBacklogMessageForRecipient")
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "deleteBacklogMessageForRecipient").Str("phase", "lock_released").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
	return removed
}

func (s *Service) deleteBacklogReceiptForRecipient(recipient string, messageID protocol.MessageID, status string) int {
	log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_wait").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_held").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	list := s.receipts[recipient]
	if len(list) == 0 {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_released_empty").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
		return 0
	}
	filtered := list[:0]
	removed := 0
	for _, receipt := range list {
		if receipt.MessageID == messageID && receipt.Recipient == recipient && receipt.Status == status {
			delete(s.relayRetry, relayReceiptKey(receipt))
			removed++
			continue
		}
		filtered = append(filtered, receipt)
	}
	if len(filtered) == 0 {
		delete(s.receipts, recipient)
	} else {
		s.receipts[recipient] = filtered
	}
	if removed <= 0 {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_released_noop").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
		return 0
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_released").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
	return removed
}

// queueStateSnapshotLocked builds a full queue-state snapshot for disk
// persistence.  Precondition: caller holds s.deliveryMu.RLock — covers
// pending, orphaned, relayRetry, receipts, outbound.  The gossip slice
// (topics["dm"]) is snapshotted under a brief internal gossipMu.RLock in
// canonical deliveryMu → gossipMu order.
func (s *Service) queueStateSnapshotLocked() queueStateFile {
	pending := make(map[string][]pendingFrame, len(s.pending))
	for address, items := range s.pending {
		if len(items) == 0 {
			continue
		}
		frames := make([]pendingFrame, len(items))
		copy(frames, items)
		pending[string(address)] = frames
	}

	relayRetry := make(map[string]relayAttempt, len(s.relayRetry))
	for key, item := range s.relayRetry {
		relayRetry[key] = item
	}
	// topics["dm"] is a gossip-domain field — take gossipMu.RLock briefly
	// (nested inside the caller's s.deliveryMu.RLock, matching canonical
	// order).
	s.gossipMu.RLock()
	relayMessages := make([]protocol.Envelope, 0, len(s.topics["dm"]))
	for _, msg := range s.topics["dm"] {
		if _, ok := relayRetry[relayMessageKey(msg.ID)]; ok {
			relayMessages = append(relayMessages, msg)
		}
	}
	s.gossipMu.RUnlock()
	relayReceipts := make([]protocol.DeliveryReceipt, 0)
	for _, list := range s.receipts {
		for _, receipt := range list {
			if _, ok := relayRetry[relayReceiptKey(receipt)]; ok {
				relayReceipts = append(relayReceipts, receipt)
			}
		}
	}
	outbound := make(map[string]outboundDelivery, len(s.outbound))
	for key, item := range s.outbound {
		outbound[key] = item
	}

	orphaned := make(map[string][]pendingFrame, len(s.orphaned))
	for addr, items := range s.orphaned {
		orphaned[string(addr)] = append([]pendingFrame(nil), items...)
	}

	return queueStateFile{
		Version:            queueStateVersion,
		Pending:            pending,
		Orphaned:           orphaned,
		RelayRetry:         relayRetry,
		RelayMessages:      relayMessages,
		RelayReceipts:      relayReceipts,
		OutboundState:      outbound,
		RelayForwardStates: s.relayStates.snapshot(),
	}
}

func sanitizeRelayState(items map[string]relayAttempt, messages []protocol.Envelope, receipts []protocol.DeliveryReceipt) {
	valid := make(map[string]struct{}, len(messages)+len(receipts))
	for _, msg := range messages {
		valid[relayMessageKey(msg.ID)] = struct{}{}
	}
	for _, receipt := range receipts {
		valid[relayReceiptKey(receipt)] = struct{}{}
	}
	for key := range items {
		if _, ok := valid[key]; !ok {
			delete(items, key)
		}
	}
}

// noteOutboundQueuedLocked upserts an outbound entry for the frame into
// s.outbound with Status="queued". Caller MUST hold s.deliveryMu.Lock.
func (s *Service) noteOutboundQueuedLocked(frame protocol.Frame, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
		state.QueuedAt = time.Now().UTC()
	}
	state.Status = "queued"
	state.Error = errText
	s.outbound[frame.ID] = state
}

func (s *Service) markOutboundRetrying(frame protocol.Frame, queuedAt time.Time, retries int, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	log.Trace().Str("site", "markOutboundRetrying").Str("phase", "lock_wait").Str("msg_id", frame.ID).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "markOutboundRetrying").Str("phase", "lock_held").Str("msg_id", frame.ID).Msg("delivery_mu_writer")
	s.markOutboundRetryingLocked(frame, queuedAt, retries, errText)
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "markOutboundRetrying").Str("phase", "lock_released").Str("msg_id", frame.ID).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
}

// markOutboundRetryingLocked updates outbound state to "retrying" in memory.
// Caller MUST hold s.deliveryMu.Lock. The -Locked variant does not call the
// (now no-op) MarkDirty; drainPendingForIdentities collects these as deferred
// actions and applies them with the pending-queue return as one atomic
// in-memory update (queue-state is in-memory only — no disk persist).
func (s *Service) markOutboundRetryingLocked(frame protocol.Frame, queuedAt time.Time, retries int, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
	}
	if state.QueuedAt.IsZero() {
		state.QueuedAt = queuedAt
	}
	state.Status = "retrying"
	state.Retries = retries
	state.LastAttemptAt = time.Now().UTC()
	state.Error = errText
	s.outbound[frame.ID] = state
}

func (s *Service) markOutboundTerminal(frame protocol.Frame, status, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	log.Trace().Str("site", "markOutboundTerminal").Str("phase", "lock_wait").Str("msg_id", frame.ID).Str("status", status).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "markOutboundTerminal").Str("phase", "lock_held").Str("msg_id", frame.ID).Str("status", status).Msg("delivery_mu_writer")
	s.markOutboundTerminalLocked(frame, status, errText)
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "markOutboundTerminal").Str("phase", "lock_released").Str("msg_id", frame.ID).Str("status", status).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
}

// markOutboundTerminalLocked updates outbound state to a terminal status in
// memory. Caller MUST hold s.deliveryMu.Lock. The -Locked variant does not
// call the (now no-op) MarkDirty; drainPendingForIdentities collects these as
// deferred actions and applies them with the pending-queue return as one
// atomic in-memory update (queue-state is in-memory only — no disk persist).
func (s *Service) markOutboundTerminalLocked(frame protocol.Frame, status, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
		state.QueuedAt = time.Now().UTC()
	}
	state.Status = status
	state.LastAttemptAt = time.Now().UTC()
	if status == "failed" {
		state.Retries++
	}
	state.Error = errText
	s.outbound[frame.ID] = state
}

func (s *Service) clearOutboundQueued(messageID string) {
	if strings.TrimSpace(messageID) == "" {
		return
	}
	log.Trace().Str("site", "clearOutboundQueued").Str("phase", "lock_wait").Str("msg_id", messageID).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "clearOutboundQueued").Str("phase", "lock_held").Str("msg_id", messageID).Msg("delivery_mu_writer")
	if _, ok := s.outbound[messageID]; !ok {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "clearOutboundQueued").Str("phase", "lock_released_noop").Str("msg_id", messageID).Msg("delivery_mu_writer")
		return
	}
	delete(s.outbound, messageID)
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "clearOutboundQueued").Str("phase", "lock_released").Str("msg_id", messageID).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
}

// clearOutboundQueuedLocked removes outbound delivery state for a message in
// memory. Caller MUST hold s.deliveryMu.Lock. The -Locked variant does not
// call the (now no-op) MarkDirty; drainPendingForIdentities collects these as
// deferred actions and applies them with the pending-queue return as one
// atomic in-memory update (queue-state is in-memory only — no disk persist).
func (s *Service) clearOutboundQueuedLocked(messageID string) {
	if strings.TrimSpace(messageID) == "" {
		return
	}
	delete(s.outbound, messageID)
}

func (s *Service) clearRelayRetryForOutbound(frame protocol.Frame) {
	if frame.Type != "relay_delivery_receipt" || frame.ID == "" || frame.Recipient == "" || frame.Status == "" {
		return
	}

	key := relayReceiptKey(protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(frame.ID),
		Recipient: frame.Recipient,
		Status:    frame.Status,
	})

	log.Trace().Str("site", "clearRelayRetryForOutbound").Str("phase", "lock_wait").Str("key", key).Str("msg_id", frame.ID).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "clearRelayRetryForOutbound").Str("phase", "lock_held").Str("key", key).Str("msg_id", frame.ID).Msg("delivery_mu_writer")
	if _, ok := s.relayRetry[key]; !ok {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "clearRelayRetryForOutbound").Str("phase", "lock_released_noop").Str("key", key).Str("msg_id", frame.ID).Msg("delivery_mu_writer")
		return
	}
	delete(s.relayRetry, key)
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "clearRelayRetryForOutbound").Str("phase", "lock_released").Str("key", key).Str("msg_id", frame.ID).Msg("delivery_mu_writer")
	s.queuePersist.MarkDirty()
}

func (s *Service) emitDeliveryReceipt(msg incomingMessage) {
	defer crashlog.DeferRecover()
	receipt := protocol.DeliveryReceipt{
		MessageID:   msg.ID,
		Sender:      s.identity.Address,
		Recipient:   msg.Sender,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
	s.storeDeliveryReceipt(receipt)
}

func receiptFrame(receipt protocol.DeliveryReceipt) protocol.ReceiptFrame {
	return protocol.ReceiptFrame{
		MessageID:   string(receipt.MessageID),
		Sender:      receipt.Sender,
		Recipient:   receipt.Recipient,
		Status:      receipt.Status,
		DeliveredAt: receipt.DeliveredAt.UTC().Format(time.RFC3339),
	}
}

func receiptFromFrame(frame protocol.Frame) (protocol.DeliveryReceipt, error) {
	if strings.TrimSpace(frame.ID) == "" || strings.TrimSpace(frame.Address) == "" || strings.TrimSpace(frame.Recipient) == "" || strings.TrimSpace(frame.Status) == "" || strings.TrimSpace(frame.DeliveredAt) == "" {
		return protocol.DeliveryReceipt{}, fmt.Errorf("missing delivery receipt fields")
	}
	if frame.Status != protocol.ReceiptStatusDelivered && frame.Status != protocol.ReceiptStatusSeen {
		return protocol.DeliveryReceipt{}, fmt.Errorf("invalid delivery receipt status")
	}

	deliveredAt, err := time.Parse(time.RFC3339, frame.DeliveredAt)
	if err != nil {
		return protocol.DeliveryReceipt{}, err
	}

	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(frame.ID),
		Sender:      frame.Address,
		Recipient:   frame.Recipient,
		Status:      frame.Status,
		DeliveredAt: deliveredAt.UTC(),
	}, nil
}

func receiptFromReceiptFrame(frame protocol.ReceiptFrame) (protocol.DeliveryReceipt, error) {
	if frame.Status != protocol.ReceiptStatusDelivered && frame.Status != protocol.ReceiptStatusSeen {
		return protocol.DeliveryReceipt{}, fmt.Errorf("invalid delivery receipt status")
	}
	deliveredAt, err := time.Parse(time.RFC3339, frame.DeliveredAt)
	if err != nil {
		return protocol.DeliveryReceipt{}, err
	}
	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(frame.MessageID),
		Sender:      frame.Sender,
		Recipient:   frame.Recipient,
		Status:      frame.Status,
		DeliveredAt: deliveredAt.UTC(),
	}, nil
}

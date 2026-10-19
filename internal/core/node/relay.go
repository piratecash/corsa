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

// relayNextHopSelection names the path that picked the downstream of the
// attempt a relayForwardState tracks. It is recorded per attempt because
// the stale hop-ack guard judges an ack differently depending on it (see
// pinsAckIdentity).
type relayNextHopSelection uint8

const (
	// relayNextHopUnspecified is the zero value: a state built without the
	// field. It names no path; store and updateState log a forwarded state
	// that carries it (warnMissingNextHopSelection).
	relayNextHopUnspecified relayNextHopSelection = iota
	// relayNextHopNotForwarded — nothing went downstream: a reservation
	// placeholder, a final-hop delivery or a message stored locally.
	relayNextHopNotForwarded
	// relayNextHopDirectSession — the recipient itself held a session.
	relayNextHopDirectSession
	// relayNextHopRoutingTable — a routing-table decision named the next
	// hop: the intermediate table forward, a table-directed origin send and
	// a pending-queue drain through a newly learned route.
	relayNextHopRoutingTable
	// relayNextHopGossipFanOut — a blind fan-out to relay-capable peers.
	relayNextHopGossipFanOut
	// relayNextHopFailover — re-emitted through an alternative uplink after
	// the previous attempt's hop-ack budget expired.
	relayNextHopFailover
)

// pinsAckIdentity reports whether the attempt's next hop is the only
// identity whose hop_ack may count: an ack from anybody else is stale. Only
// a routing-table attempt is pinned. A direct-session attempt and a failover
// retry also hand the frame to one known identity, and could be pinned on
// the same grounds; they are not, to keep the stale-guard semantics these
// paths have always had (only an ack from an abandoned next hop is stale).
// Changing that is a behaviour change of its own, not part of naming the
// selection.
func (s relayNextHopSelection) pinsAckIdentity() bool {
	return s == relayNextHopRoutingTable
}

// String names the selection for logs.
func (s relayNextHopSelection) String() string {
	switch s {
	case relayNextHopUnspecified:
		return "unspecified"
	case relayNextHopNotForwarded:
		return "not_forwarded"
	case relayNextHopDirectSession:
		return "direct_session"
	case relayNextHopRoutingTable:
		return "routing_table"
	case relayNextHopGossipFanOut:
		return "gossip_fan_out"
	case relayNextHopFailover:
		return "failover"
	default:
		return "unknown"
	}
}

// relayForwardState stores per-message forwarding context on an intermediate
// node. Each node knows only its own previous_hop and forwarded_to —
// no single message reveals the full path.
type relayForwardState struct {
	MessageID        string
	PreviousHop      domain.PeerAddress // who sent this relay to me (transport address)
	ReceiptForwardTo domain.PeerAddress // = PreviousHop (where to send receipt back)
	ForwardedTo      domain.PeerAddress // who I forwarded to (for loop detection)
	// ForwardedVia is the connection the current attempt to ForwardedTo was
	// handed to, as it stood WHEN the attempt was made: its identity and its
	// penalty subject (proven only for a v2 session). The hop-ack timeout
	// charges this and never re-resolves the address, which may by then be
	// held by another connection. Zero when no connection held the address
	// (the frame was queued locally): such an attempt is charged to nobody.
	ForwardedVia         routingSender
	AbandonedForwardedTo []domain.PeerAddress  // old ForwardedTo values from reroutes (stale ack rejection)
	Recipient            domain.PeerIdentity   // final recipient identity (for hop_ack route confirmation)
	NextHopSelection     relayNextHopSelection // which path picked ForwardedTo; decides how the stale-ack guard judges an ack (isStaleHopAckSender)
	HopCount             int                   // incremented on each hop
	RemainingTTL         int                   // seconds until cleanup (decremented by ticker)

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
	// Initialised by sendRelayMessage / sendTableRoutedRelayMessage /
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
	// sendTableRoutedRelayMessage, handleRelayMessage's intermediate
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
	rejected int64 // counter for monitoring: entries rejected due to capacity

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
			rs.decrementTTLs()
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
		MessageID:        messageID,
		PreviousHop:      previousHop,
		NextHopSelection: relayNextHopNotForwarded,
		RemainingTTL:     relayStateTTLSeconds,
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

// warnMissingNextHopSelection logs a forwarded state whose caller did not
// say which path picked the next hop. The state is still stored — the
// forward happened — but the stale hop-ack guard can only treat it as an
// unpinned attempt, and the omission must be visible rather than silent.
func warnMissingNextHopSelection(state *relayForwardState) {
	if state.ForwardedTo == "" {
		return
	}
	switch state.NextHopSelection {
	case relayNextHopUnspecified, relayNextHopNotForwarded:
		log.Warn().
			Str("id", state.MessageID).
			Str("forwarded_to", string(state.ForwardedTo)).
			Str("next_hop_selection", state.NextHopSelection.String()).
			Msg("relay_state_next_hop_selection_missing")
	}
}

// updateState overwrites the placeholder created by tryReserve with a
// fully populated relayForwardState.
func (rs *relayStateStore) updateState(state *relayForwardState) {
	warnMissingNextHopSelection(state)
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.states[state.MessageID] = state
}

// store saves forwarding state for a relayed message. Returns false if the
// message was already seen (dedupe) or the store is at capacity.
func (rs *relayStateStore) store(state *relayForwardState) bool {
	warnMissingNextHopSelection(state)
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if existing, ok := rs.states[state.MessageID]; ok {
		// Idempotent upsert: refresh forwarding fields and TTL so that
		// pending queue retries after route churn use the current best
		// path instead of being rejected.
		// PreviousHop and ReceiptForwardTo are preserved from the original
		// store — they describe the upstream path and must not change.
		//
		// Both ForwardedTo and NextHopSelection are always updated. A
		// resend may reach the same next-hop peer through a different
		// path (a table route the first time, gossip fan-out the next),
		// and the stale-ack guard must judge an ack by the path of the
		// attempt that is actually outstanding.
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
		existing.ForwardedVia = state.ForwardedVia
		existing.NextHopSelection = state.NextHopSelection
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
		return domain.PeerIdentity{}
	}
	return state.Recipient
}

// lookupForwardAttempt returns the address and next-hop selection of the
// attempt currently outstanding for a relayed message, read together under
// one lock so a concurrent failover cannot pair one attempt's address with
// another attempt's selection. An unknown message ID yields "" and
// relayNextHopNotForwarded: nothing this node knows of went downstream.
func (rs *relayStateStore) lookupForwardAttempt(messageID string) (domain.PeerAddress, relayNextHopSelection) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return "", relayNextHopNotForwarded
	}
	return state.ForwardedTo, state.NextHopSelection
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
// sites — sendRelayMessage / sendTableRoutedRelayMessage /
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
// Returns nil when nothing fired. The ticker in ttlTicker iterates the
// return value and invokes onHopAckTimeout outside the mutex; see
// ttlTicker's doc-comment for the lock-ordering rationale.
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
	// Release the stashed wire bytes only when this call is the one
	// CANCELLING a live timer. Then the ack proves the next hop holds
	// the frame and the failover resend — FrameLine's only reader —
	// can never fire for this forward, so the store's memory bound
	// becomes "payload only while an ack is outstanding" instead of
	// "10 000 × payload for the whole TTL".
	//
	// HopAckObserved already true means the budget ELAPSED and
	// tickHopAckBudgets flipped it as it handed the state to
	// onRelayHopAckTimeout. A late ack for the abandoned uplink then
	// arrives while that failover is still running, and releasing here
	// would empty the field under it: the retry re-arms the budget
	// (recordFailoverRetry) and the next timeout would find nothing to
	// resend or gossip. The failover owns the payload from that point
	// and re-stamps what it sent; leave it alone.
	if !state.HopAckObserved {
		state.FrameLine = ""
	}
	state.HopAckObserved = true
	return true
}

// frameLineBytes sums the wire payload bytes stashed for failover
// across all tracked states. The walk is bounded by maxRelayStates and
// runs under rs.mu, which no Service domain mutex nests with; it is a
// diagnostic (resource breakdown), not a hot-path counter.
func (rs *relayStateStore) frameLineBytes() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	total := 0
	for _, state := range rs.states {
		total += len(state.FrameLine)
	}
	return total
}

// recordFailoverRetry atomically transitions a relayForwardState to its
// next failover attempt: the previous ForwardedTo moves into
// AbandonedForwardedTo (so late acks from the abandoned uplink are
// still rejected by handleRelayHopAck's stale-ack guard), the new
// downstream address takes its place, RetryAttempt increments, the
// hop-ack budget is re-armed (HopAckRemainingTicks =
// defaultHopAckBudgetSeconds, HopAckObserved=false), and the attempt is
// marked relayNextHopFailover: the stale-ack guard then rejects only acks
// from abandoned uplinks, as it always has for a failover retry.
//
// frameLine is the wire payload the caller just put on the new uplink,
// and it is re-stamped here because a re-armed budget means a further
// timeout can follow: the state must describe the forward that is
// actually outstanding. It also restores the field against a late ack
// for the abandoned uplink, which lands while this failover runs and
// would otherwise have released it (see markHopAckObserved). An empty
// frameLine leaves whatever the state holds — the caller had nothing to
// say about the payload.
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
func (rs *relayStateStore) recordFailoverRetry(messageID string, newAddress domain.PeerAddress, via routingSender, frameLine string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return false
	}
	if state.ForwardedTo != "" && state.ForwardedTo != newAddress {
		state.AbandonedForwardedTo = append(state.AbandonedForwardedTo, state.ForwardedTo)
	}
	if frameLine != "" {
		state.FrameLine = frameLine
	}
	state.ForwardedTo = newAddress
	state.ForwardedVia = via
	state.NextHopSelection = relayNextHopFailover
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

// handleRelayMessage processes an incoming relay_message frame and returns the
// semantic hop-ack status. The caller is responsible for delivering the ack to
// the sender — this function does NOT send an ack itself, because the delivery
// mechanism depends on context (direct conn write in dispatchNetworkFrame vs
// enqueuePeerFrame in dispatchPeerSessionFrame).
//
// Return values:
//   - "delivered" — this node is the final recipient, message accepted and stored locally
//   - "forwarded" — message relayed to the next hop
//   - "stored"    — no capable next hop, message buffered in-flight for forwarding retries
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
			NextHopSelection: relayNextHopNotForwarded,
			Recipient:        domain.PeerIdentityFromWire(recipient),
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

	// Origin echo on the FAST path: a relay_message whose ORIGIN sender is
	// THIS node is our own DM echoing back through the mesh (recipient != self
	// here — the local-delivery branch returned above). Re-forwarding it via
	// the transit contour re-injects a delivery the sender-owned engine may
	// have abandoned — the months-long zombie-DM storm. Re-send of our own DMs
	// is owned EXCLUSIVELY by the sender-owned engine (delivery_retry), so drop
	// here. Mirror of the originEcho guard in storeIncomingMessage; the fast
	// path bypasses that guard because it forwards before storeIncomingMessage.
	// No hop-ack (same as the max-hops drop).
	if s.identity != nil && originSender == s.identity.Address {
		s.relayStates.release(messageID)
		log.Debug().
			Str("node", s.identity.Address).
			Str("id", messageID).
			Str("recipient", recipient).
			Msg("relay_drop_origin_echo")
		return ""
	}

	// Frame-shape refusals on the FAST path (envelope_retention.go): the
	// direct/table/gossip forwards below run BEFORE storeIncomingMessage, so
	// a frame with a live next hop would otherwise bypass the admission
	// checks entirely. The AGE ceiling this gate used to apply is gone — a
	// relay does not refuse a DM for being old — and what is left refuses a
	// transit frame with no created_at, or one dated beyond clock skew into
	// the future. No hop-ack (same as the max-hops drop).
	if s.relayFrameAged(frame, originSender, recipient) {
		s.relayStates.release(messageID)
		log.Debug().
			Str("node", s.identity.Address).
			Str("id", messageID).
			Str("recipient", recipient).
			Msg("relay_drop_transit_age_ceiling")
		return ""
	}

	newHopCount := hopCount + 1

	// Build the forwarded frame. The origin sender's attached PUBLIC key
	// triple is copied through verbatim — stripping it here would break
	// first-contact verification on the final recipient and push it onto
	// the on-demand key sync. Verbatim copy, not re-validation: transit
	// hops stay crypto-free on the fast path, the recipient validates
	// before import (validateAttachedSenderPubKey /
	// importVerifiedSenderKeys). The ONE transit-side check is the cheap
	// length cap below: without it a hostile origin could stuff
	// megabytes into the key fields — bypassing the Body-size admission
	// cap — and have every hop amplify them across the mesh; oversized
	// fields can never validate on the recipient anyway, so they are
	// dropped here (the recipient falls back to the on-demand key sync).
	forwardCreatedAt, forwardTTL := legacyTransitWireStamp(frame.Topic, recipient, frame.CreatedAt, frame.TTLSeconds, time.Now().UTC())
	forwardFrame := protocol.Frame{
		Type:      "relay_message",
		ID:        messageID,
		Address:   originSender,
		Recipient: recipient,
		Topic:     frame.Topic,
		Body:      frame.Body,
		Flag:      frame.Flag,
		// TODO(transit-age-restamp-removal): forwarding somebody else's old
		// message needs the same treatment as sending our own — a pre-v30
		// node further along the route drops it otherwise, silently. Decided
		// HERE, once, so every onward copy (direct peer, table route, gossip,
		// the stashed failover line) carries the same date. The TTL is
		// re-derived with it so the author's deadline does not move. See
		// legacy_transit_restamp.go.
		CreatedAt:   forwardCreatedAt,
		TTLSeconds:  forwardTTL,
		HopCount:    newHopCount,
		MaxHops:     maxHops,
		PreviousHop: string(s.identity.Address),
	}
	if len(frame.PubKey) <= maxAttachedKeyFieldLen &&
		len(frame.BoxKey) <= maxAttachedKeyFieldLen &&
		len(frame.BoxSig) <= maxAttachedKeyFieldLen {
		forwardFrame.PubKey = frame.PubKey
		forwardFrame.BoxKey = frame.BoxKey
		forwardFrame.BoxSig = frame.BoxSig
	}
	// Backfill: the frame arrived keyless — an upstream hop rebuilt it
	// from a stored envelope whose sender its key maps had already
	// evicted, the fields were oversized and dropped above, or a hostile
	// hop stripped them. If THIS node happens to know the origin's keys
	// (e.g. the origin is a direct neighbour whose handshake we cached),
	// attach them so the downstream recipient still gets the
	// first-contact fast path instead of the on-demand sync.
	if forwardFrame.PubKey == "" {
		s.attachKnownSenderKeys(&forwardFrame, forwardFrame.Topic, originSender)
	}

	downstream := s.forwardRelayDownstream(domain.PeerIdentityFromWire(recipient), forwardFrame, domain.PeerIdentityFromWire(frame.PreviousHop), senderAddress)
	forwardedTo := downstream.Address

	if forwardedTo == "" {
		// Control DMs (TopicControlDM) intentionally have no node-level
		// in-flight buffering fallback — they are not persisted in
		// chatlog, not appended to s.topics, and not tracked in
		// relayRetry (see docs/dm-commands.md "Storage rules for
		// control DMs"). Calling deliverRelayedMessage here would
		// pass through storeIncomingMessage which returns stored=true
		// for the wire-dedup `seen` set yet leaves no recoverable
		// envelope behind, and we would falsely ack "stored" upstream
		// — the previous hop would consider relay successful while
		// the control envelope is in fact lost. The application-level
		// retry on the sender (the DMRouter delete intent) is
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
			NextHopSelection: relayNextHopNotForwarded,
			Recipient:        domain.PeerIdentityFromWire(recipient),
			HopCount:         newHopCount,
			RemainingTTL:     relayStateTTLSeconds,
		})
		log.Info().
			Str("id", messageID).
			Str("recipient", recipient).
			Msg("relay_stored_no_capable_peers")
		return "stored"
	}

	// Update the reservation placeholder with full forwarding state.
	// ReceiptForwardTo uses the transport address (senderAddress), not the
	// identity fingerprint from frame.PreviousHop. This is critical:
	// sendTargetHasCapability() and sendReceiptToPeer() look up sessions by
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
		ForwardedVia:         downstream.Via,
		Recipient:            domain.PeerIdentityFromWire(recipient),
		NextHopSelection:     downstream.Selection,
		HopCount:             newHopCount,
		RemainingTTL:         relayStateTTLSeconds,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            forwardLine,
	})

	log.Info().
		Str("id", messageID).
		Str("from", originSender).
		Str("to", recipient).
		Str("forwarded_to", string(forwardedTo)).
		Str("next_hop_selection", downstream.Selection.String()).
		Int("hop_count", newHopCount).
		Msg("relay_forwarded")
	return "forwarded"
}

// relayDownstreamAttempt is the downstream an intermediate hop handed a
// relay_message to. An empty Address means no path took the frame.
type relayDownstreamAttempt struct {
	Address   domain.PeerAddress
	Via       routingSender
	Selection relayNextHopSelection
}

// forwardRelayDownstream hands forwardFrame to the first path that takes
// it and reports which one did, in order of preference:
//
//   - a direct session to the recipient. A recipient identity may have
//     several session addresses (reconnects, address changes), so every
//     matching session is tried until one accepts;
//   - a routing-table next hop (Phase 1.2). This enables multi-hop chains
//     A→B→D where B finds D in its table even though D is not the final
//     recipient's direct peer on B. previousHop is excluded so the message
//     is never sent back to the hop it came from;
//   - gossip fan-out to relay-capable peers, excluding senderAddress.
func (s *Service) forwardRelayDownstream(recipient domain.PeerIdentity, forwardFrame protocol.Frame, previousHop domain.PeerIdentity, senderAddress domain.PeerAddress) relayDownstreamAttempt {
	if address, via := s.tryForwardToDirectPeer(recipient, forwardFrame); address != "" {
		return relayDownstreamAttempt{Address: address, Via: via, Selection: relayNextHopDirectSession}
	}
	if result := s.tryForwardViaRoutingTable(s.runCtx, recipient, forwardFrame, previousHop); result.Address != "" {
		return relayDownstreamAttempt{Address: result.Address, Via: result.Via, Selection: relayNextHopRoutingTable}
	}
	if address, via := s.relayViaGossip(forwardFrame, senderAddress); address != "" {
		return relayDownstreamAttempt{Address: address, Via: via, Selection: relayNextHopGossipFanOut}
	}
	return relayDownstreamAttempt{Selection: relayNextHopNotForwarded}
}

// deliverRelayedMessage converts a relay_message frame into a local message
// store operation. Reuses storeIncomingMessage to avoid duplicating
// deduplication, persistence, and UI notification logic.
//
// senderAddress is the transport address of the previous relay hop. When
// storeIncomingMessage rejects the DM with ErrCodeUnknownSenderKey (the
// frame arrived without the sender's self-certifying key triple and this
// node does not know the sender — see attachKnownSenderKeys for why a
// frame can still arrive keyless), this function SCHEDULES a
// background single-flight contact sync from the previous hop
// (triggerSenderKeySyncAsync) and rejects the current attempt; the
// previous hop's relay retry redelivers and succeeds once the keys are
// imported. The sync is never run inline here — this function executes on
// session dispatch loops, and a blocking recovery dial would let the
// socket reader overflow the bounded inboxCh and kill a healthy session.
//
// Returns true if the message was accepted (stored locally), false if it
// was rejected (parse error, invalid signature, unknown sender key, etc.).
// The caller uses this to decide the hop-ack status: a rejected message
// must NOT be reported as "delivered" or "stored" to the previous hop.
//
// syncSession is the live outbound session to the previous hop, when the
// caller has one (the inbound relay dispatch resolves it via
// activePeerSession; the outbound session dispatch passes its own
// session). The recovery pass uses it exclusively through the session
// OWNER (requestOwnedContactSync → serve-loop contactSyncCh) — never by
// reading inboxCh from the recovery goroutine, which would race the
// serve loop for frames. A previous hop connected ONLY inbound (no
// outbound session exists) has no recovery wire — see the honest-scope
// note on triggerSenderKeySyncAsync.
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
		// Attached PUBLIC sender keys: validated by
		// importAttachedSenderKeys before entering the knowledge maps —
		// this is what makes a first-contact relay DM verifiable without
		// the on-demand sync below.
		PubKey: frame.PubKey,
		BoxKey: frame.BoxKey,
		BoxSig: frame.BoxSig,
	})
	if err != nil {
		log.Warn().Err(err).Str("id", frame.ID).Msg("relay_deliver_parse_error")
		return false
	}
	// Ingress link (previous relay hop) for hop accounting + echo
	// suppression (transit_retention.go). The identity lookup covers
	// inbound-only previous hops whose route key ("inbound:<raw>")
	// address comparison cannot match.
	msg.Via = senderAddress
	msg.ViaIdentity = s.viaIdentityForAddress(senderAddress)
	// Gossip hop budget for a relay_message is derived UNCONDITIONALLY
	// from the relay chain's own HopCount/MaxHops — the top-level
	// `hops` field is deliberately NOT copied above and has no way
	// into the computation (see relayChainGossipBudget for the
	// injection-defense rationale and the MaxHops normalization
	// contract).
	msg.Hops = relayChainGossipBudget(frame.HopCount, frame.MaxHops)
	stored, _, errCode := s.storeIncomingMessage(msg, false)
	if !stored && errCode == protocol.ErrCodeUnknownSenderKey && senderAddress != "" {
		// Keyless frame for an unknown sender (a hop could not attach the
		// keys, dropped oversized ones or stripped them — see
		// attachKnownSenderKeys): schedule a BACKGROUND single-flight
		// contact sync against the previous hop and reject this
		// attempt. The sync must NOT run inline: deliverRelayedMessage
		// executes on session dispatch loops, and a synchronous
		// recovery dial (drains of the responder's post-auth backlog
		// take up to syncReplyDrainCap each) would stall the loop while
		// the socket reader fills the bounded inboxCh — overflow there
		// tears down a healthy session. The rejected message is
		// redelivered by the previous hop's relay retry (no hop-ack is
		// sent for a rejected delivery), which finds the imported keys
		// on the next attempt. syncSession (the live outbound session to
		// the previous hop, when the caller had one) is handed to the
		// recovery pass, which uses it ONLY through the session owner
		// (requestOwnedContactSync) — the wire that still reaches a
		// NATed / inbound-only previous hop a fresh dial cannot.
		log.Info().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", string(senderAddress)).Msg("relay_key_sync_scheduled")
		s.triggerSenderKeySyncAsync(senderAddress, msg.Sender, syncSession)
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
func (s *Service) handleRelayHopAck(senderAddress domain.PeerAddress, sender routingSender, frame protocol.Frame) {
	log.Debug().
		Str("id", frame.ID).
		Str("from", string(senderAddress)).
		Str("status", frame.Status).
		Msg("relay_hop_ack_received")

	forwardedTo, selection := s.relayStates.lookupForwardAttempt(frame.ID)
	if forwardedTo == "" {
		// Unknown message ID (TTL eviction beat the ack) OR a message we
		// stored locally without forwarding. Either way there is no
		// downstream budget timer to suppress and no next-hop to confirm.
		return
	}
	// sender is the connection the ack ARRIVED on, handed over by the
	// dispatcher. A connection that names no identity cannot vouch for any
	// attempt; resolving the address instead would credit whoever holds it
	// by now, which is exactly the misattribution this parameter removes.
	ackIdentity := sender.identity
	if ackIdentity.IsZero() {
		log.Debug().Str("id", frame.ID).Str("from", string(senderAddress)).Msg("relay_hop_ack_from_unnamed_connection_ignored")
		return
	}
	// An ack from a legacy connection merely naming an identity proven over
	// v2 neither suppresses the attempt's timer nor confirms a route in the
	// identity's name.
	release, admitted := s.admitRoutingInput(sender, "relay_hop_ack")
	if !admitted {
		return
	}
	defer release()

	// Stale-ack guard — applied to the SUPPRESSION decision, not only to
	// route confirmation. A stale ack must not reset the current
	// attempt's timer.
	//
	// Table-routed attempt (relayNextHopRoutingTable): the table named
	// one next hop, and after a reroute store() moves ForwardedTo to the
	// new one. An ack from any identity other than ForwardedTo's is stale.
	// Comparison is identity-based (not transport address) because a peer
	// may have several sessions (inbound + outbound) and the ack can
	// arrive on any.
	//
	// Any other attempt (direct session, gossip fan-out, failover): store()
	// and recordFailoverRetry overwrite the selection, so a table-routed
	// message retried another way no longer takes the branch above.
	// AbandonedForwardedTo accumulates every superseded next-hop across
	// reroutes; reject acks from any of them (covers chains like
	// A → B → gossip(C)).
	if s.isStaleHopAckSender(frame.ID, ackIdentity, forwardedTo, selection) {
		return
	}

	// Ack relates to the current downstream attempt. Suppress its budget
	// timer before confirming the route.
	s.relayStates.markHopAckObserved(frame.ID)

	if frame.Status == "delivered" || frame.Status == "forwarded" {
		recipient := s.relayStates.lookupRecipient(frame.ID)
		if recipient.IsZero() {
			return
		}
		s.confirmRouteViaHopAck(recipient, senderAddress)
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
func (s *Service) isStaleHopAckSender(messageID string, ackIdentity domain.PeerIdentity, forwardedTo domain.PeerAddress, selection relayNextHopSelection) bool {
	if ackIdentity.IsZero() {
		return false
	}
	if selection.pinsAckIdentity() {
		fwdIdentity := s.resolvePeerIdentity(forwardedTo)
		if !fwdIdentity.IsZero() && fwdIdentity != ackIdentity {
			log.Debug().
				Str("id", messageID).
				Str("ack_identity", ackIdentity.String()).
				Str("forwarded_identity", fwdIdentity.String()).
				Msg("relay_hop_ack_stale_sender_ignored")
			return true
		}
		return false
	}
	for _, abandoned := range s.relayStates.lookupAbandonedForwardedTo(messageID) {
		if s.resolvePeerIdentity(abandoned) == ackIdentity {
			log.Debug().
				Str("id", messageID).
				Str("ack_identity", ackIdentity.String()).
				Str("abandoned_forwarded_identity", ackIdentity.String()).
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
func (s *Service) tryForwardToDirectPeer(recipient domain.PeerIdentity, frame protocol.Frame) (domain.PeerAddress, routingSender) {
	s.peerMu.RLock()
	var candidates []domain.PeerAddress
	for address, session := range s.sessions {
		if session.peerIdentity == recipient {
			candidates = append(candidates, address)
		}
	}
	s.peerMu.RUnlock()

	for _, address := range candidates {
		if !s.sendTargetHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if via, queued := s.enqueuePeerSendItemVia(address, legacyPeerSendItem(frame)); queued {
			return address, via
		}
	}
	return domain.PeerAddress(""), routingSender{}
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
func (s *Service) relayViaGossip(frame protocol.Frame, excludeAddress domain.PeerAddress) (domain.PeerAddress, routingSender) {
	targets := s.routingTargetsForRecipient(string(frame.Recipient))
	var forwardedTo domain.PeerAddress
	var forwardedVia routingSender
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		// Canonical comparison (transit_retention.go): the previous
		// hop may appear in the target list under a fallback-port
		// dial alias — raw equality would echo the relay_message
		// straight back to it. Same ingress-suppression contract as
		// the push_message gossip filter and the table-directed path.
		if excludeAddress != "" && s.sameCanonicalAddress(address, excludeAddress) {
			continue
		}
		if !s.sendTargetHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if !s.relayLimiter.allow(address) {
			continue
		}
		frame.PreviousHop = s.identity.Address
		if via, queued := s.enqueuePeerSendItemVia(address, legacyPeerSendItem(frame)); queued {
			if forwardedTo == "" {
				forwardedTo = address
				forwardedVia = via
			}
		}
	}
	return forwardedTo, forwardedVia
}

// handleRelayReceipt processes a delivery receipt that may need to be
// forwarded back along the relay chain via receipt_forward_to lookup.
// Returns true if the receipt was forwarded via the relay chain (the caller
// should NOT additionally gossip it). Returns false if no relay path was
// found or the relay hop failed — the caller is responsible for gossip
// fallback in that case.
// relayHopForReceipt resolves the relay hop for a receipt. delivered/seen
// travel BACK toward the original message sender — the reverse path stored
// as ReceiptForwardTo (the hop the message arrived FROM). seen_ack travels
// in the ORIGINAL message direction, toward the seen-sender — the message's
// ForwardedTo hop; pushing it through the reverse path would bounce it back
// to the original sender.
func (s *Service) relayHopForReceipt(receipt protocol.DeliveryReceipt) domain.PeerAddress {
	if receipt.Status == protocol.ReceiptStatusSeenAck {
		return s.relayStates.lookupForwardedTo(string(receipt.MessageID))
	}
	return s.relayStates.lookupReceiptForwardTo(string(receipt.MessageID))
}

func (s *Service) handleRelayReceipt(receipt protocol.DeliveryReceipt) bool {
	forwardTo := s.relayHopForReceipt(receipt)
	if forwardTo == "" {
		return false
	}

	// Forward the receipt one hop along its direction: delivered/seen go
	// BACK toward the original sender, seen_ack goes FORWARD toward the
	// seen-sender (see relayHopForReceipt).
	if s.sendTargetHasCapability(forwardTo, domain.CapMeshRelayV1) {
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
// Returns whether at least one target ACCEPTED the frame. Every target
// already reports that individually; the aggregate is what lets a
// sender-owned delivery tell a fan-out that reached somebody from one
// where every candidate was skipped or refused.
// Returns the BEST outcome across the targets: a frame that reached a
// writer beats one whose fate is unknown, which beats an outright refusal.
// The middle value is not decoration — collapsing it onto "refused" makes
// the caller record "reached nobody" for a message a peer may be holding.
func (s *Service) tryRelayToCapableFullNodes(msg protocol.Envelope, targets []domain.PeerAddress, dispatchedAt time.Time) relaySendOutcome {
	best := relaySendRefused
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		if !s.sendTargetHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if s.peerIsClientNode(address) {
			continue
		}
		if !s.relayLimiter.allow(address) {
			continue
		}
		switch outcome := s.sendRelayMessage(address, msg, dispatchedAt); {
		case outcome.leftTheNode():
			best = relaySendHandedToPeer
		case outcome == relaySendFateUnknown && best == relaySendRefused:
			best = relaySendFateUnknown
		}
	}
	return best
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

// relaySendOutcome says how far a relay frame actually got. The
// distinction between the last two is invisible to transit forwarding —
// which only asks "is this handled, or must I try elsewhere?" — but it is
// everything to a sender-owned delivery: a frame parked in OUR OWN pending
// ring has not left this machine, and calling that "sent" is how a stale
// route used to turn into a delivered-looking message.
type relaySendOutcome uint8

const (
	// relaySendRefused: nobody took the frame, not even our own ring.
	relaySendRefused relaySendOutcome = iota
	// relaySendQueuedLocally: parked in this node's pending ring, to be
	// flushed if that peer ever comes back. Still here.
	relaySendQueuedLocally
	// relaySendQueuedForSession: handed to a live peer SESSION, whose
	// writer loop confirms the delivery itself once NetCore accepts the
	// frame. Not a confirmation here: the session's queue is in-process
	// and its remainder is discarded when the session closes.
	relaySendQueuedForSession
	// relaySendHandedToPeer: written straight to a connection's writer,
	// which is as far as this node can see.
	relaySendHandedToPeer
	// relaySendFateUnknown: a writer answered in a way that cannot place
	// the frame — a sync flush that ended on a timeout or a cancelled
	// context, a writer already tearing down. The bytes may have left.
	//
	// It exists because collapsing this onto "refused" is a LIE IN THE
	// EXPENSIVE DIRECTION: the caller then records "reached nobody", and a
	// later deletion skips a peer who is holding the message. Refused says
	// the frame is provably still ours; this says nobody can tell, and the
	// only honest response is to write nothing down and try again.
	relaySendFateUnknown
)

// handled reports whether the frame needs no further attempt from the
// caller — the question transit forwarding asks.
func (o relaySendOutcome) handled() bool {
	return o != relaySendRefused && o != relaySendFateUnknown
}

// leftTheNode reports whether the frame reached a writer, which is the only
// outcome a sender-owned delivery may count as sent HERE.
//
// A session enqueue is deliberately not one: it is an in-process queue, and
// the session's writer loop confirms the delivery for itself when NetCore
// accepts the frame. Counting it here would call a message sent that a
// session teardown could still discard, and would also double-count the
// confirmation the writer makes.
func (o relaySendOutcome) leftTheNode() bool { return o == relaySendHandedToPeer }

// sendRelayMessage creates a relay_message frame from an envelope and sends
// it to the specified peer. Used when the routing decision indicates a
// relay-capable next hop.
func (s *Service) sendRelayMessage(address domain.PeerAddress, msg protocol.Envelope, dispatchedAt time.Time) relaySendOutcome {
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
	// Attach the sender's self-certifying PUBLIC key triple so a
	// first-contact recipient can verify without a network round-trip.
	// For own envelopes msg.Sender == s.identity and the triple comes
	// from the self contact seeded at NewService (box key absent under
	// the relay-only DM opt-out — signing key only, by design).
	s.attachKnownSenderKeys(&frame, msg.Topic, msg.Sender)

	outcome := relaySendRefused
	via, queued := s.enqueuePeerSendItemVia(address, deliveryPeerSendItem(frame, msg, dispatchedAt))
	if queued {
		outcome = relaySendQueuedForSession
	} else if s.queuePeerFrame(address, frame) {
		outcome = relaySendQueuedLocally
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "queued").
			Msg("relay_message_queued")
	}

	if !outcome.handled() {
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "dropped").
			Msg("relay_message_dropped")
		return relaySendRefused
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
		ForwardedVia:         via,
		Recipient:            domain.PeerIdentityFromWire(msg.Recipient),
		NextHopSelection:     relayNextHopGossipFanOut,
		HopCount:             1,
		RemainingTTL:         relayStateTTLSeconds,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            originLine,
	})
	log.Debug().
		Str("id", string(msg.ID)).
		Str("recipient", msg.Recipient).
		Str("peer", string(address)).
		Str("mode", "session").
		Msg("relay_message_sent")
	return outcome
}

// sendTableRoutedRelayMessage is the table-directed variant of
// sendRelayMessage: the address is the next hop a routing-table decision
// picked, so the stored relayForwardState is marked
// relayNextHopRoutingTable and the stale-ack guard accepts a hop_ack only
// from that next hop's identity. The fan-out path (sendRelayMessage) marks
// its attempts relayNextHopGossipFanOut because no table entry chose them.
func (s *Service) sendTableRoutedRelayMessage(address domain.PeerAddress, msg protocol.Envelope, dispatchedAt time.Time) relaySendOutcome {
	return s.sendTableRoutedRelayMessageWithR2a(address, msg, dispatchedAt, nil)
}

func (s *Service) sendTableRoutedRelayMessageWithR2a(address domain.PeerAddress, msg protocol.Envelope, dispatchedAt time.Time, origin *r2aDirectedDispatch) relaySendOutcome {
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
	// Sender-key attachment — same contract as sendRelayMessage.
	s.attachKnownSenderKeys(&frame, msg.Topic, msg.Sender)

	outcome := relaySendRefused
	item := deliveryPeerSendItem(frame, msg, dispatchedAt)
	session := s.snapshotR2aDirectedSession(origin)
	candidate := s.captureR2aFirstDirectedItem(origin, session, item)
	var via routingSender
	var queued bool
	if candidate != nil {
		item.writeAck = candidate.ack
		queued = s.enqueueSessionSendItem(session, item)
		s.r2aRecordOffer(candidate, queued)
		if queued {
			via = sessionRoutingSender(session)
		}
	} else {
		via, queued = s.enqueuePeerSendItemVia(address, item)
	}
	if queued {
		outcome = relaySendQueuedForSession
	} else if s.queuePeerFrame(address, frame) {
		outcome = relaySendQueuedLocally
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "queued").
			Msg("relay_message_queued")
	}

	if !outcome.handled() {
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "dropped").
			Msg("relay_message_dropped")
		return relaySendRefused
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
		ForwardedVia:         via,
		Recipient:            domain.PeerIdentityFromWire(msg.Recipient),
		NextHopSelection:     relayNextHopRoutingTable,
		HopCount:             1,
		RemainingTTL:         relayStateTTLSeconds,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            originLine,
	})
	if !stored {
		// Same rule as the inbound path: the send has already happened
		// and its outcome is what it is. A full relay-state table costs
		// this forward its hop-ack tracking, not its existence.
		log.Warn().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Msg("relay_state_store_failed_outbound_hop_ack_disabled")
	}
	log.Debug().
		Str("id", string(msg.ID)).
		Str("recipient", msg.Recipient).
		Str("peer", string(address)).
		Str("mode", "session").
		Msg("relay_message_sent_table_directed")
	return outcome
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
		attempts, tracked := s.noteRelayAttempt(relayMessageKey(msg.ID), now)
		if !tracked {
			// Removed between the snapshot and now — the message is no
			// longer ours to retry.
			continue
		}
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
		// Retries obey the same propagation gates as first admission
		// (hop budget + ingress suppression, transit_retention.go): a
		// retry is a re-emission of the SAME hop, not a new one, so
		// the stored Hops value is reused, not decremented again.
		// gossipTargetsForRelay drops the table next-hop from the fan-out
		// (it gets the directed relay_message below), then applies the
		// hop/ingress/ack/K-of-N gates — so the retry tick no longer emits a
		// duplicate push_message+relay_message to the same peer (the repeated
		// double-send visible in the capture).
		gossipTargets := s.gossipTargetsForRelay(msg, decision)
		// Transit: not ours, so no delivery to answer for. The zero ref
		// keeps this path exactly as fire-and-forget as it was.
		s.executeGossipTargets(msg, gossipTargets, deliveryDispatchRef{})
		// Table-directed relay (Phase 1.2): mirror the logic in
		// storeIncomingMessage — use the routing table when a next-hop
		// is known, fall back to blind gossip relay otherwise.
		if decision.RelayNextHop != nil {
			s.sendTableDirectedRelay(s.runCtx, msg, *decision.RelayNextHop, decision.RelayNextHopAddress, decision.RelayNextHopHops, time.Now().UTC())
		} else {
			s.tryRelayToCapableFullNodes(msg, gossipTargets, time.Now().UTC())
		}
	}
	for _, receipt := range s.retryableRelayReceipts(now) {
		attempts, tracked := s.noteRelayAttempt(relayReceiptKey(receipt), now)
		if !tracked {
			continue
		}
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Int("attempts", attempts).Msg("relay_retry_receipt")
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
	for _, msg := range items {
		key := relayMessageKey(msg.ID)
		// The same predicate the admission commit arms by, so an envelope
		// the commit would not arm can never be retried from here, whatever
		// put an entry under its id. See envelopeEntersRelayRetry.
		if !envelopeEntersRelayRetry(msg, s.identity.Address) {
			delete(s.relayRetry, key)
			continue
		}
		if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
			delete(s.relayRetry, key)
			continue
		}
		if s.hasReceiptForMessageLocked(messageIdentityOf(msg)) {
			delete(s.relayRetry, key)
			continue
		}
		if !shouldRetryRelayLocked(s.relayRetry, key, now) {
			continue
		}
		out = append(out, msg)
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
	for _, list := range s.receipts {
		for _, receipt := range list {
			key := relayReceiptKey(receipt)
			if !receiptEntersRelayRetry(receipt, s.identity.Address) {
				delete(s.relayRetry, key)
				continue
			}
			if !shouldRetryRelayLocked(s.relayRetry, key, now) {
				continue
			}
			out = append(out, receipt)
		}
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

// noteRelayAttempt records one retry attempt on an entry that is still
// tracked and returns the attempt count. The bool is false when the
// entry is gone: the retry loop works from a snapshot taken under
// deliveryMu and released before this call, so in between a delivery
// receipt can remove the entry, and ack_delete, a transit-cap eviction or
// the expiry sweep can remove the message together with its entry.
// Re-creating the entry for a removed message made it unreachable for
// every reaper (they all walk the live backlog, which no longer holds the
// message), so it stayed until process exit and burned the
// maxRelayRetryEntries quota. Absent entry → no write, and the caller
// skips the send, because the work it was about to do is no longer owed.
func (s *Service) noteRelayAttempt(key string, now time.Time) (int, bool) {
	log.Trace().Str("site", "noteRelayAttempt").Str("phase", "lock_wait").Str("key", key).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "noteRelayAttempt").Str("phase", "lock_held").Str("key", key).Msg("delivery_mu_writer")
	state, tracked := s.relayRetry[key]
	if tracked {
		state.LastAttempt = now
		state.Attempts++
		s.relayRetry[key] = state
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "noteRelayAttempt").Str("phase", "lock_released").Str("key", key).Msg("delivery_mu_writer")
	return state.Attempts, tracked
}

// envelopeEntersRelayRetry reports whether a BACKLOGGED envelope is one this
// node relays onward on a retry schedule: a data DM between two OTHER
// identities. The admission commit arms by it and retryableRelayMessages
// reaps by it, so the two cannot disagree on which envelopes own a `msg|`
// entry.
//
// Excluded, each for its own reason:
//   - control DMs (TopicControlDM): their retry policy lives at the
//     application layer (the DMRouter delete intent, with exponential
//     backoff — see docs/dm-commands.md §"Acknowledgement and retry"), and
//     they never enter s.topics, so an entry for one would be visited by no
//     walk and would only burn the maxRelayRetryEntries quota;
//   - broadcast and addressless envelopes, and DMs addressed to this node,
//     which have arrived and are not forwarded;
//   - DMs this node authored. They are owned by the sender-owned delivery
//     engine (delivery_retry.go: registerAwaitingDelivered →
//     dispatchEnvelopeRetry), which paces itself on a capped backoff, emits
//     only into a route that exists and ends on the recipient's receipt, the
//     author's withdrawal or the message's own TTL. This contour is for
//     TRANSIT forwarding only (relay.md INV-1/INV-2: "relay is
//     forwarding-only — offline delivery is the sender's job") and re-emits
//     BLINDLY; arming our own message here re-gossiped it forever, every mesh
//     echo re-armed the relayRetryTTL window, and that was the source of the
//     multi-day DM spam.
func envelopeEntersRelayRetry(envelope protocol.Envelope, localAddress string) bool {
	return envelope.Topic == "dm" &&
		envelope.Recipient != "" && envelope.Recipient != "*" && envelope.Recipient != localAddress &&
		envelope.Sender != localAddress
}

// armRelayMessageRetryLocked starts the relay retry of a transit DM that the
// caller has just appended to s.topics["dm"]. Caller MUST hold s.deliveryMu
// (write) and s.gossipMu (write), in canonical order, in the same section as
// the append.
//
// It is called only from storeIncomingMessage's admission commit, never as a
// step of its own, because a `msg|` entry is reaped only through its
// envelope: retryableRelayMessages (and its TTL) walks s.topics["dm"], and
// ack_delete, the transit cap and the in-flight window remove the entry in
// the same section that removes the envelope. Arming in a later
// section left a window in which any of them could take the envelope; the
// entry was then created for an envelope no walk would visit. It held a
// maxRelayRetryEntries slot and made the ack_delete suppression gate
// (recordRelayDeliveredWhileRelayed) treat the id as relayed — admitting
// records for it — until a receipt for the id or (as cancels then did) a
// withdrawal of an own message with the same id happened to delete it.
//
// It writes a fresh entry without looking for an existing one. That is safe
// because the append it accompanies is never a duplicate (the caller's exact-ID
// scan returned first), so an entry already under this id would have no
// envelope behind it — itself the orphan this invariant rules out — and
// replacing it loses nothing live.
//
// A full map leaves the envelope unarmed rather than evicting a live entry.
// It is still backlogged, so subscribers and fetch_inbox still get it and its
// first send still goes out from the admission; what it loses is this node's
// onward re-sends, and the sender's own end-to-end retry covers those.
func (s *Service) armRelayMessageRetryLocked(envelope protocol.Envelope) {
	if len(s.relayRetry) >= maxRelayRetryEntries {
		log.Debug().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).
			Msg("relay retry not armed for transit message: relayRetry is full")
		return
	}
	s.relayRetry[relayMessageKey(envelope.ID)] = relayAttempt{FirstSeen: envelope.StoredAt}
}

// receiptEntersRelayRetry reports whether a BACKLOGGED receipt is one this
// node relays onward on a retry schedule: a transit receipt for some other
// identity, as opposed to one addressed to this node, which has arrived.
// (seen_ack never reaches the backlog, so it needs no clause here.) The
// commit arms by it and retryableRelayReceipts reaps by it, so the two cannot
// disagree on which receipts own an entry.
func receiptEntersRelayRetry(receipt protocol.DeliveryReceipt, localAddress string) bool {
	return receipt.Recipient != "" && receipt.Recipient != localAddress
}

// armRelayReceiptRetryLocked starts the relay retry of a transit receipt
// that the caller has just appended to s.receipts. Caller MUST hold
// s.deliveryMu (write).
//
// It is called only from finishReceipt's commit, never as a step of its
// own, because a `receipt|` entry is reaped almost only through its
// receipt: retryableRelayReceipts (and its TTL), the offline-recipient drop,
// ack_delete and the per-recipient cap all walk s.receipts. The one reaper
// that goes by key, clearRelayRetryForOutbound, runs only in an OUTBOUND
// peer session's serve loop, when a relay_delivery_receipt frame for it is
// accepted into that session's NetCore writer queue (SendOK). Arming in a
// later section left a window in which the recipient's last subscriber
// could go and take the backlog with it; the entry was then created for a
// receipt no walk would visit. Unless the first send happened to get such a
// frame accepted by an outbound session's writer queue, it held a
// maxRelayRetryEntries slot until the process exited — that is, when there
// was no route and no gossip target, when every send was refused or dropped
// (an oversize frame included), or when the frames went out over inbound
// connections only.
//
// A full map leaves the receipt unarmed rather than evicting a live entry.
// It is still backlogged, so fetch_delivery_receipts serves it to the
// subscriber, and it still gets its first send from distributeReceipt;
// what it loses is this node's onward re-sends. End-to-end recovery then
// depends on the status. A lost delivered is recovered by the message
// sender's own delivery retry: while it has no receipt it re-sends the
// message, and the recipient answers the duplicate with a fresh delivered
// receipt (resendDeliveryReceipt). A lost seen is recovered by its author:
// the seen-sender re-sends the seen receipt on its own schedule until the
// matching seen_ack arrives (the seen-retry pass in delivery_retry.go).
func (s *Service) armRelayReceiptRetryLocked(receipt protocol.DeliveryReceipt, now time.Time) {
	if len(s.relayRetry) >= maxRelayRetryEntries {
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).
			Msg("relay retry not armed for transit receipt: relayRetry is full")
		return
	}
	s.relayRetry[relayReceiptKey(receipt)] = relayAttempt{FirstSeen: now}
}

// dropConfirmedRelayMessageRetryLocked ends the relay retry of the transit DM
// a just-committed receipt confirms. Caller MUST hold s.deliveryMu (write)
// and no gossip-domain lock: s.topics["dm"] is read under a nested
// s.gossipMu.RLock (deliveryMu → gossipMu, canonical order).
//
// The `msg|` key is the id alone, and the id is unsigned, so the key cannot
// say whose message it is — only the envelope that owns it can. The
// admission's exact-id dedup keeps at most one envelope per id in
// s.topics["dm"], so the entry goes only when that envelope IS the confirmed
// message. Dropping by key let a receipt for this node's own message, or for
// a pair of identities the transit DM is not between, end that DM's onward
// re-sends.
//
// It is dropped here rather than left to retryableRelayMessages, which stops
// at a backlogged receipt too, because the receipt does not stay backlogged:
// the original sender's ack_delete, the per-recipient cap and the offline
// drop all take it, and the walk would then resume re-sending a message its
// recipient has already confirmed.
func (s *Service) dropConfirmedRelayMessageRetryLocked(message messageIdentity) {
	key := relayMessageKey(message.MessageID)
	if _, armed := s.relayRetry[key]; !armed {
		return
	}
	s.gossipMu.RLock()
	owner, backlogged := s.backlogMessageUnderIDLocked(message.MessageID)
	s.gossipMu.RUnlock()
	if !backlogged {
		// An entry with no envelope behind it breaks the invariant the
		// admission commit and every removal path keep (an entry exists only
		// while its envelope is backlogged). It is owned by no live message:
		// retryableRelayMessages walks s.topics["dm"] and nothing walks the
		// map itself, so left here it would hold a maxRelayRetryEntries slot
		// until exit and keep the ack_delete gate treating the id as relayed.
		// There is nothing to relay under it, and a later transit DM with the
		// id re-arms at its own admission, so it is reaped — together with
		// its ack_delete suppression set, which only envelope-removal paths
		// forget and which would otherwise hand stale suppression to the next
		// transit DM under the id. Forgotten in this hold (relayDeliveredMu is
		// a leaf, allowed innermost under deliveryMu), so no ack_delete record
		// can land between the reap and the forget.
		log.Warn().Str("message_id", string(message.MessageID)).
			Str("confirmed_sender", message.Sender).Str("confirmed_recipient", message.Recipient).
			Msg("relay_retry_entry_without_backlogged_envelope")
		delete(s.relayRetry, key)
		s.forgetRelayDelivered([]protocol.MessageID{message.MessageID})
		return
	}
	if owner == message {
		delete(s.relayRetry, key)
		return
	}
	// An armed entry whose envelope is some other message: an id collision
	// or a forged receipt. Logged so a relay that keeps running after a
	// receipt can be traced to the mismatch that kept it.
	log.Debug().Str("message_id", string(message.MessageID)).
		Str("confirmed_sender", message.Sender).Str("confirmed_recipient", message.Recipient).
		Str("envelope_sender", owner.Sender).Str("envelope_recipient", owner.Recipient).
		Msg("relay_retry_kept_receipt_confirms_another_message")
}

// backlogMessageUnderIDLocked returns the identity of the dm backlog's
// envelope under id; the admission's exact-id dedup keeps at most one.
// Caller MUST hold s.gossipMu (RLock or Lock; reads s.topics).
func (s *Service) backlogMessageUnderIDLocked(id protocol.MessageID) (messageIdentity, bool) {
	for _, envelope := range s.topics["dm"] {
		if envelope.ID == id {
			return messageIdentityOf(envelope), true
		}
	}
	return messageIdentity{}, false
}

func relayMessageKey(id protocol.MessageID) string {
	return "msg|" + string(id)
}

// relayFrameAged reports whether an incoming relay_message must not be
// forwarded on account of its timestamp. For the DM classes that is no
// longer about AGE — the ceiling is gone — but about the frame being
// unusable: no created_at at all, or one dated beyond clock skew into the
// future. Broadcast, which keeps a real ceiling, is still age-checked here.
//
// handleRelayMessage forwards on the FAST path (direct / table / gossip)
// BEFORE any call into storeIncomingMessage, so the admission checks are
// bypassed for any frame with a live next hop — this gate re-applies them.
// It parses the wire CreatedAt (an unparseable value is treated as zero) and
// defers to the shared envelopePropagationAged gate, so the semantics match
// the store-path emit exactly.
func (s *Service) relayFrameAged(frame protocol.Frame, sender, recipient string) bool {
	created, err := time.Parse(time.RFC3339, strings.TrimSpace(frame.CreatedAt))
	if err != nil {
		created = time.Time{} // unparseable → zero → handled by the DM-class rule
	}
	return s.envelopePropagationAged(frame.Topic, sender, recipient, created.UTC(), time.Now().UTC())
}

// hasReceiptForMessageLocked reports whether s.receipts already contains a
// receipt that confirms this very message. A receipt under the same id that
// some other identity made, or that is addressed to someone else, is about a
// different message: the id is unsigned, and taking it as confirmation would
// end this message's relay (or skip its abandonment) on another's word.
//
// Only backlogged receipts are consulted, so a seen_ack never counts here:
// it is not backlogged (finishReceipt). Its effect is the immediate drop in
// its own commit, which ends the relay of a message backlogged at that
// moment. A copy of the message admitted after it is relayed until a
// delivered/seen receipt or its TTL — accepted, because a seen_ack follows
// the recipient's seen receipt, which ends that relay through this walk
// wherever it is backlogged, and keeping a second, ack-only record to cover
// the leftover would be state with no reaper of its own.
// Caller MUST hold s.deliveryMu (RLock or Lock).
func (s *Service) hasReceiptForMessageLocked(message messageIdentity) bool {
	for _, receipt := range s.receipts[message.Sender] {
		if confirmed, ok := confirmedMessageOf(receipt); ok && confirmed == message {
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
	messages := s.topics["dm"]
	before := len(messages)
	filtered := messages[:0]
	for _, msg := range messages {
		if msg.ID == messageID && msg.Recipient == recipient {
			delete(s.relayRetry, relayMessageKey(msg.ID))
			continue
		}
		filtered = append(filtered, msg)
	}
	// Release the delivered Envelope left in the shared backing array
	// so its payload is not pinned until a future append overwrites
	// the tail (same hygiene as the retention sweep).
	clear(messages[len(filtered):])
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
	// The envelope left s.topics — drop its ack_delete suppression set too
	// (relay_delivered.go). Leaf lock, taken after both domain mutexes are
	// released.
	s.forgetRelayDelivered([]protocol.MessageID{messageID})
	return removed
}

// deleteBacklogReceiptForRecipient drops the acked receipt, and ONLY it.
//
// The identity is the whole point: an ack names one receipt, and the
// backlog can hold two with the same recipient, message and status when
// somebody forged one — that is precisely the case where deleting "all
// that look alike" destroys the genuine receipt and its retry entry, and
// the sender's message sits at `sent` until the message itself is
// re-sent. The forgery is rejected at the end node, but the ack for it
// is still legitimate traffic, and this is what it must not sweep up.
//
// An ack from a peer older than ProtocolVersionReceiptSenderAck cannot
// name the author. Then the rule is "delete only if unambiguous": one
// candidate is deleted, several are all KEPT. Keeping costs a duplicate
// push the peer discards; deleting the wrong one costs the delivery.
func (s *Service) deleteBacklogReceiptForRecipient(target receiptIdentity) int {
	recipient, messageID := target.Recipient, target.MessageID
	log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_wait").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_held").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	list := s.receipts[recipient]
	if len(list) == 0 {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "deleteBacklogReceiptForRecipient").Str("phase", "lock_released_empty").Str("recipient", recipient).Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
		return 0
	}
	if !target.senderKnown() {
		candidates := 0
		for _, receipt := range list {
			if target.matches(receipt) {
				candidates++
			}
		}
		if candidates > 1 {
			s.deliveryMu.Unlock()
			log.Warn().Str("recipient", recipient).Str("msg_id", string(messageID)).Str("status", target.Status).Int("candidates", candidates).
				Msg("ack_delete kept every receipt: the acking peer cannot name which one it holds and more than one matches")
			return 0
		}
	}
	filtered := list[:0]
	removed := 0
	for _, receipt := range list {
		if target.matches(receipt) {
			delete(s.relayRetry, relayReceiptKey(receipt))
			removed++
			continue
		}
		filtered = append(filtered, receipt)
	}
	// Zero the tail left in the shared backing array so the removed receipts'
	// string fields are not pinned until a future append overwrites them (same
	// hygiene as deleteBacklogMessageForRecipient above).
	clear(list[len(filtered):])
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
	return removed
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
}

// markOutboundRetryingLocked updates outbound state to "retrying" in memory.
// Caller MUST hold s.deliveryMu.Lock. drainPendingForIdentities collects these
// as deferred actions and applies them with the pending-queue return as one
// atomic in-memory update.
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
}

// markOutboundTerminalLocked updates outbound state to a terminal status in
// memory. Caller MUST hold s.deliveryMu.Lock. drainPendingForIdentities
// collects these as deferred actions and applies them with the pending-queue
// return as one atomic in-memory update.
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

// sweepTerminalOutbound reclaims s.outbound entries stuck in a terminal
// status ("expired"/"failed") for longer than outboundTerminalRetention.
// Live statuses (queued/retrying) are cleared by receipt arrival, channel
// send, ring eviction or recipient removal; terminal ones reach none of
// those paths and would otherwise accumulate one per undeliverable
// message for the life of the process. Runs from bootstrapLoop.
func (s *Service) sweepTerminalOutbound(now time.Time) {
	log.Trace().Str("site", "sweepTerminalOutbound").Str("phase", "lock_wait").Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "sweepTerminalOutbound").Str("phase", "lock_held").Msg("delivery_mu_writer")
	defer func() {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "sweepTerminalOutbound").Str("phase", "lock_released").Msg("delivery_mu_writer")
	}()

	if !s.lastOutboundTerminalSweep.IsZero() && now.Sub(s.lastOutboundTerminalSweep) < outboundTerminalSweepInterval {
		return
	}
	s.lastOutboundTerminalSweep = now

	removed := 0
	for id, ob := range s.outbound {
		if ob.Status != "expired" && ob.Status != "failed" {
			continue
		}
		staleSince := ob.LastAttemptAt
		if staleSince.IsZero() {
			staleSince = ob.QueuedAt
		}
		if staleSince.IsZero() || now.Sub(staleSince) > outboundTerminalRetention {
			delete(s.outbound, id)
			removed++
		}
	}
	if removed > 0 {
		log.Debug().Int("removed", removed).Msg("outbound_terminal_entries_swept")
	}
}

// clearConfirmedOutboundLocked drops the outbound status entry of the own
// message a receipt confirms. Caller MUST hold s.deliveryMu (write).
//
// s.outbound holds only messages this node authored but keys them by the
// bare id, so an entry under the id is the confirmed message only when this
// node is its author and the entry's recipient is its addressee. A receipt
// for anything else under the id — a transit DM this node relays, or a claim
// by an identity the message was not sent to — must leave the entry: its
// status is what fetch_pending_messages shows, and CancelOutgoingDeliveriesTo
// learns the id from it.
func (s *Service) clearConfirmedOutboundLocked(message messageIdentity) {
	entry, tracked := s.outbound[string(message.MessageID)]
	if !tracked || s.ownMessageIdentity(message.MessageID, entry.Recipient) != message {
		return
	}
	delete(s.outbound, string(message.MessageID))
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
}

// clearOutboundQueuedLocked removes outbound delivery state for a message in
// memory. Caller MUST hold s.deliveryMu.Lock. drainPendingForIdentities
// collects these as deferred actions and applies them with the pending-queue
// return as one atomic in-memory update.
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

	// frame.Address is the receipt's author (receiptFromFrame reads it
	// into DeliveryReceipt.Sender), so the identity is complete here —
	// this key used to omit it and cleared the retry of whichever
	// same-looking receipt happened to own the entry.
	if strings.TrimSpace(frame.Address) == "" {
		return
	}
	key := identityFromRelayFrame(frame).retryKey()

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
}

func (s *Service) emitDeliveryReceipt(msg incomingMessage) {
	defer crashlog.DeferRecover()
	s.storeDeliveryReceipt(deliveredReceiptFor(s.identity.Address, msg))
}

// resendDeliveryReceipt redistributes the delivered receipt for a DM that
// arrived as a DUPLICATE. The first arrival already recorded the receipt
// locally (seenReceipts / receipts), so storeDeliveryReceipt would dedupe a
// second emit into a complete no-op — but a duplicate arrival means the
// sender most likely never received the original receipt and is retrying
// end-to-end. Push the receipt outward again without touching local
// receipt state.
func (s *Service) resendDeliveryReceipt(msg incomingMessage) {
	defer crashlog.DeferRecover()
	receipt := deliveredReceiptFor(s.identity.Address, msg)
	log.Info().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Msg("resending delivered receipt for duplicate DM")
	s.distributeReceipt(receipt)
}

// distributeReceipt pushes a receipt outward — the relay hop first
// (direction per relayHopForReceipt: reverse for delivered/seen, forward
// for seen_ack), gossip fallback, plus live subscribers — without touching
// local receipt state. Mirrors the distribution block at the end of
// storeDeliveryReceipt and is shared by the duplicate re-send paths and
// the delivery retry scheduler.
func (s *Service) distributeReceipt(receipt protocol.DeliveryReceipt) {
	if !s.handleRelayReceipt(receipt) {
		s.goBackground(func() { s.gossipReceipt(receipt) })
	}
	s.goBackground(func() { s.pushReceiptToSubscribers(receipt) })
}

// deliveredReceiptFor builds the delivered receipt this node owes the
// remote sender of msg.
func deliveredReceiptFor(localAddress string, msg incomingMessage) protocol.DeliveryReceipt {
	return protocol.DeliveryReceipt{
		MessageID:   msg.ID,
		Sender:      localAddress,
		Recipient:   msg.Sender,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
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

// localReceiptFrame is receiptFrame for the backlog this node serves to its
// OWN client, and the only builder that fills ObservedAt.
//
// Separate from receiptFrame rather than a flag on it, because the two answer
// different questions and only one of them crosses the wire: a relayed
// receipt carries the author's claim and nothing of ours, while the backlog
// the desktop reads has to agree with the live event it may have missed —
// and the live event carries our admission time.
func localReceiptFrame(receipt protocol.DeliveryReceipt) protocol.ReceiptFrame {
	frame := receiptFrame(receipt)
	if !receipt.ObservedAt.IsZero() {
		frame.ObservedAt = receipt.ObservedAt.UTC().Format(time.RFC3339)
	}
	return frame
}

// receiptDisplayTime is when to say the message was delivered: what this
// node saw, falling back to the sender's claim for a receipt that predates
// the local stamp (a restart's reseed, or a fixture).
func receiptDisplayTime(receipt protocol.DeliveryReceipt) time.Time {
	if !receipt.ObservedAt.IsZero() {
		return receipt.ObservedAt
	}
	return receipt.DeliveredAt
}

func receiptFromFrame(frame protocol.Frame) (protocol.DeliveryReceipt, error) {
	if strings.TrimSpace(frame.ID) == "" || strings.TrimSpace(frame.Address) == "" || strings.TrimSpace(frame.Recipient) == "" || strings.TrimSpace(frame.Status) == "" || strings.TrimSpace(frame.DeliveredAt) == "" {
		return protocol.DeliveryReceipt{}, fmt.Errorf("missing delivery receipt fields")
	}
	if !protocol.IsValidReceiptStatus(frame.Status) {
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
	if !protocol.IsValidReceiptStatus(frame.Status) {
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

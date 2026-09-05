package datagram

import (
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// reverse_state.go implements the two-phase reverse state of §4.2 — the only
// state a transit node keeps for the unsigned request/response planes.
//
// Two properties drive every decision in this file:
//
//   - the record is created in TWO PHASES. "Create it after a successful
//     enqueue" contains a race: the moment the frame is published the writer
//     may already send it and receive a fast answer, before the map entry
//     exists, and the answer would be dropped as unaddressed. So the slot is
//     taken (pending) once the candidates are chosen and before publication,
//     and the chosen downstream is written into it before publication too;
//   - the record has exactly ONE answer slot, so nothing may claim it before
//     the answer has been validated: the first piece of garbage would eat the
//     slot and the real answer would be refused as "already claimed". Hence
//     the order read-only checks → probe budget → CAS pending→claimed →
//     enqueue, and hence the probe budget, which is reserved atomically
//     BEFORE the expensive check.
//
// The routed plane never touches anything here, and nothing here ever touches
// the routed replay cache (§4.1).
//
// Reference: docs/refactoring/datagram-transport.md §4.1, §4.2, §5.

// ---------------------------------------------------------------------------
// Label — the key of the reverse state
// ---------------------------------------------------------------------------

// Label is the one-shot 20-byte tag an initiator puts in `request.src` and
// gets echoed back in `response.dst` (§2.1.1). It is the key of the reverse
// state and it is NOT an address.
//
// It is a distinct type from domain.PeerIdentity on purpose. On the wire the
// two share a shape, and that similarity is exactly the trap: a label is not
// authenticated, does not belong to anybody, and must never be compared with
// a local identity, used as a routing destination or handed to an
// authorization hook as "the sender". Making it a separate type turns each of
// those mistakes into a compile error instead of a review comment.
type Label struct {
	raw domain.PeerIdentity
}

// NewLabel reads a label out of the header field that carries it.
func NewLabel(raw domain.PeerIdentity) Label { return Label{raw: raw} }

// Raw returns the 20 bytes to put back on the wire. Named Raw rather than
// Identity so a call site that means "the peer" cannot reach it by accident.
func (l Label) Raw() domain.PeerIdentity { return l.raw }

// IsZero reports whether the label is unset.
func (l Label) IsZero() bool { return l.raw.IsZero() }

// String renders the label for logs and metrics.
func (l Label) String() string { return l.raw.String() }

// ---------------------------------------------------------------------------
// Upstream — the return path AND the quota owner, as two values
// ---------------------------------------------------------------------------

// Upstream answers TWO questions about a forwarded request, and the whole shape
// of this type is that they are two questions and not one:
//
//   - WHERE DOES THE ANSWER GO — the CHANNEL the request arrived on, or the
//     LOCAL marker for a request this node originated;
//   - WHOSE SLOT IS THIS — the AdmissionKey the arrival was billed to, which is
//     what the per-upstream cap and the fairness eviction of §5 group by
//     (upstreamKey).
//
// Neither is derivable from the other, and each earlier answer to both at once
// lost one half:
//
//   - the NAME answered neither. It is what the neighbour presents, and on a
//     session this node dialled it is a name the remote chose for itself, so a
//     stranger's records landed in the named node's quota bucket and "return the
//     answer to whoever asked" resolved, through the identity map, to "hand it to
//     whoever the asker NAMED";
//   - the CHANNEL answers the first correctly and the second wrongly. A channel
//     lives until the connection closes while a record lives up to
//     reverse_state_ttl, so a neighbour fills its 64 slots, reconnects with a
//     fresh ConnID and starts again — and once the GLOBAL cap binds, its records
//     spread over N channels read as N lightly loaded upstreams, so the fairness
//     eviction takes from the honest neighbour that has only one.
//
// The AdmissionKey is what a quota has to be against, because it is what the
// receive path can actually defend about a neighbour across reconnects: a proven
// identity, or the host:port THIS node dialled (AdmissionKeySpace). It is the key
// the same arrival already pays its byte, frame and verification budget from, so
// one neighbour is one bucket on every limit the layer has.
//
// peer travels alongside as a NAME — for the log line, and for the writer's own
// diagnostics through OutboundFrame.Peer. It is part of neither answer.
//
// `local` is a marker and never this node's own address (§4.2): an answer to
// our own request goes to a resolver inside the process, not into a session,
// and encoding that as "the peer happens to be me" would mix the transit and
// the local path in every comparison. It has no owner either — our own requests
// are not billed to a neighbour — which is what keeps its bucket unshareable.
type Upstream struct {
	owner   AdmissionKey
	channel ChannelID
	peer    domain.PeerIdentity
	local   bool
}

// upstreamKey is what the per-upstream accounting of §5 groups by: the OWNER of
// the quota, with the local marker as its own bucket.
//
// It is a type of its own rather than "the Upstream value used as a map key"
// because the value also carries the return path and the presented name, and a
// key including either would let one neighbour open a bucket per channel it
// reconnects on or per name it feels like presenting.
type upstreamKey struct {
	owner AdmissionKey
	local bool
}

// LocalUpstream marks a request created on this node.
func LocalUpstream() Upstream { return Upstream{local: true} }

// ChannelUpstream states all three facts of a transited arrival: the channel the
// answer must leave over, the budget key the arrival was billed to and therefore
// owns the slot, and the name the neighbour on that channel presents.
//
// The three are positional and not an opts struct precisely because all three
// are mandatory: an opts struct makes each of them forgettable, and a forgotten
// owner is a zero AdmissionKey — one bucket shared by every arrival that forgot.
// Their types are pairwise distinct, so the compiler checks the order.
func ChannelUpstream(channel ChannelID, owner AdmissionKey, peer domain.PeerIdentity) Upstream {
	return Upstream{channel: channel, owner: owner, peer: peer}
}

// IsLocal reports whether the answer belongs to this node.
func (u Upstream) IsLocal() bool { return u.local }

// IsZero reports whether the upstream was never set. A remote upstream needs
// BOTH of its answers: with no CHANNEL there is nowhere to return the answer to,
// and with no OWNER there is no bucket to charge the slot to — and a zero key
// would be one bucket every unbillable arrival on the node reserves from, which
// is the shape PeerAdmission.Admit refuses for the same reason.
func (u Upstream) IsZero() bool {
	return !u.local && (u.channel.IsZero() || u.owner.IsZero())
}

// Channel returns the channel the answer must leave over. The bool is false for
// the local marker, so a caller cannot pin an answer to "channel zero".
func (u Upstream) Channel() (ChannelID, bool) {
	if u.local || u.channel.IsZero() {
		return ChannelID{}, false
	}
	return u.channel, true
}

// Owner returns WHOSE per-upstream quota this record occupies. The bool is false
// for the local marker, which is billed to nobody.
//
// It is not a route and must not be used as one: an AdmissionKey may be a
// host:port this node dialled, which names a peer to bill and never a socket to
// write to. The answer is addressed by Channel above.
func (u Upstream) Owner() (AdmissionKey, bool) {
	if u.local || u.owner.IsZero() {
		return AdmissionKey{}, false
	}
	return u.owner, true
}

// Peer returns the NAME of the neighbour the answer is owed to. The bool is
// false for the local marker.
//
// It is not an address and must not be used as one: the answer is addressed by
// Channel above. It is not a quota either: the slot belongs to Owner. The name is
// what a log line and the writer's diagnostics show, and on a dialled session it
// is the neighbour's own claim.
func (u Upstream) Peer() (domain.PeerIdentity, bool) {
	if u.local {
		return domain.PeerIdentity{}, false
	}
	return u.peer, true
}

// key returns the accounting bucket of this upstream.
func (u Upstream) key() upstreamKey {
	if u.local {
		return upstreamKey{local: true}
	}
	return upstreamKey{owner: u.owner}
}

// String renders the upstream for logs. The owner is part of the line because a
// `capped` or `evicted` record is only actionable if the bucket it was charged to
// is readable — the channel alone names a socket that may already be gone.
func (u Upstream) String() string {
	if u.local {
		return "local"
	}
	return u.peer.String() + "@" + u.channel.String() + "#" + u.owner.String()
}

// ---------------------------------------------------------------------------
// Downstream — the channel a request was forwarded over
// ---------------------------------------------------------------------------

// Downstream is the CHANNEL a forwarded request left over, and therefore the
// only place an answer to it may come from.
//
// It is a channel for exactly the reason Upstream is one, and having it be an
// identity was the last place on this plane where a NAME still decided
// something. An answer used to be admitted by comparing the stored hop's
// identity with the name the answering neighbour PRESENTS — and on a session
// this node dialled that name is the remote's own claim, so any session willing
// to write the expected fingerprint into its welcome could take the single
// unsigned answer slot of an exchange it had nothing to do with. A fingerprint
// is public; a socket cannot be borrowed.
//
// peer travels alongside as a NAME — for the log line and for the writer's own
// diagnostics through OutboundFrame.Peer — and is never what an answer is judged
// by. That split is why the name is a field of this type rather than the type
// itself: one value, one meaning per accessor.
//
// It carries NO quota owner, unlike Upstream, and the asymmetry is the honest
// one: §5 bounds how many records a neighbour may make this node HOLD, and that
// is an inbound quantity. Nothing is metered per egress, so an AdmissionKey here
// would be a field with no reader — and there is nothing to put in it either,
// since an admission key is what a RECEIVE path proved about a neighbour and the
// scheduler's candidate is an outbound connection nobody billed.
type Downstream struct {
	channel ChannelID
	peer    domain.PeerIdentity
}

// ChannelDownstream names the channel the request was pinned to, together with
// the name of the neighbour on the far end of it.
func ChannelDownstream(channel ChannelID, peer domain.PeerIdentity) Downstream {
	return Downstream{channel: channel, peer: peer}
}

// IsZero reports whether the value names no channel — whatever name it carries.
// A downstream without a channel is not a downstream: there is nothing an answer
// could be checked against.
func (d Downstream) IsZero() bool { return d.channel.IsZero() }

// Channel returns the channel an answer must arrive on.
//
// It has no companion bool, and that is not a zero-value signal slipping back
// in: a Downstream reachable through ReverseRecord.Downstream always carries a
// channel, because FixDownstream refuses one that does not. The guard lives at
// the single place a record can be WRITTEN instead of at every place one is
// read, which is what makes "both ends of the record are channels" checkable
// rather than remembered.
func (d Downstream) Channel() ChannelID { return d.channel }

// Peer returns the NAME of the neighbour the request was handed to. It is a log
// label and must not be used to judge an answer.
func (d Downstream) Peer() domain.PeerIdentity { return d.peer }

// String renders the downstream for logs.
func (d Downstream) String() string {
	return d.peer.String() + "@" + d.channel.String()
}

// ---------------------------------------------------------------------------
// Window arithmetic (§4.2)
// ---------------------------------------------------------------------------

// reverseTargetBudget is the processing allowance at the target inside the
// reverse_state_ttl formula of §4.2. It is the one term of that formula with
// no constant of its own in domain, so it is named here rather than folded
// into a literal total.
const reverseTargetBudget = 10 * time.Second

// ReverseStateWindow derives reverse_state_ttl from the §4.2 formula instead
// of hard-coding 240 seconds:
//
//	reverse_state_ttl = 2 × defaultMaxHops × (queue_residence + write_grace)
//	                    of the control class + target_budget
//	                  = 2 × 10 × (5 + 5) s + 10 s = 210 s → rounded up to 240 s
//
// The record has to survive a full round trip: up to defaultMaxHops hops out
// and as many back, each hop costing at most its queue residence plus the
// write grace — the write itself is part of the hop budget, because a write
// that has started outlives send_until. Rounding up to whole minutes is the
// last step of the formula and is what turns 210 into the 240 s of
// domain.ReverseStateTTL, which reverse_state_test.go pins byte for byte.
func ReverseStateWindow() time.Duration {
	residence, err := domain.QueueResidence(domain.DatagramClassControl)
	if err != nil {
		return domain.ReverseStateTTL
	}
	grace, err := domain.WriteGrace(domain.DatagramClassControl)
	if err != nil {
		return domain.ReverseStateTTL
	}
	hop := residence + grace
	raw := 2*time.Duration(domain.DatagramDefaultMaxHops)*hop + reverseTargetBudget
	return roundUpToMinute(raw)
}

// roundUpToMinute is the rounding step of the formula. It is written out
// rather than applied by hand so a future change of the hop budget produces a
// derived window instead of a stale literal.
func roundUpToMinute(d time.Duration) time.Duration {
	if remainder := d % time.Minute; remainder != 0 {
		return d + time.Minute - remainder
	}
	return d
}

// ResponseSendDeadline is the send deadline of an answer, computed LOCALLY:
// moment_of_arrival + queue_residence(control) (§4.2). There is no wire field
// for it and there cannot be one — the node forming the answer does not know
// anybody else's expires_at, and the target's handler, which is the only thing
// that answers, creates no reverse record at all.
//
// The value must be the same 5 seconds the reverse window is computed from:
// take thirty here and a ten-hop round trip would need more than 1200 s while
// the record lives 240 s, so the answer would arrive at state that has
// already expired.
func ResponseSendDeadline(receivedAt time.Time) time.Time {
	residence, err := domain.QueueResidence(domain.DatagramClassControl)
	if err != nil {
		return receivedAt
	}
	return receivedAt.Add(residence)
}

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

// ReverseSlotState is the two-state life of one record.
type ReverseSlotState uint8

const (
	// ReverseSlotUnset is the zero value.
	ReverseSlotUnset ReverseSlotState = iota
	// ReverseSlotPending means the request was forwarded and the single
	// answer slot is still free.
	ReverseSlotPending
	// ReverseSlotClaimed means an answer has taken the slot. A second answer
	// is refused, and the record stays claimed until expires_at even if the
	// enqueue failed: no second chance is granted, the initiator retries with
	// a fresh label.
	ReverseSlotClaimed
)

var reverseSlotStateNames = map[ReverseSlotState]string{
	ReverseSlotUnset:   "unset",
	ReverseSlotPending: "pending",
	ReverseSlotClaimed: "claimed",
}

// String returns the metric label of the state.
func (s ReverseSlotState) String() string { return enumName(reverseSlotStateNames, s) }

// ReverseRecord is the read-only view of one reverse-state entry — the value
// the response fork judges an answer against (§4.2).
//
// It is a value copy with unexported fields and no setters, which is how "a
// reader mutates nothing" is enforced rather than requested: every mutation
// goes back through ReverseTable carrying the record it was decided on, so a
// stale generation is refused instead of applied.
type ReverseRecord struct {
	expiresAt  time.Time
	label      Label
	dst        domain.PeerIdentity
	dtype      domain.DType
	upstream   Upstream
	downstream Downstream
	probesLeft int
	state      ReverseSlotState
	generation uint64
}

// Label returns the key of the record — the label from `request.src`.
func (r ReverseRecord) Label() Label { return r.label }

// Dst returns the destination of the REQUEST, which a lawful answer must
// carry in its `src` (§2.1.1).
func (r ReverseRecord) Dst() domain.PeerIdentity { return r.dst }

// DType returns the dtype of the REQUEST. It is stored because without it a
// formally valid answer of ANOTHER protocol whose type this node happens to
// know would take the single claimed slot of somebody else's exchange (§4.2).
func (r ReverseRecord) DType() domain.DType { return r.dtype }

// Upstream returns where the answer has to go.
func (r ReverseRecord) Upstream() Upstream { return r.upstream }

// Downstream returns the CHANNEL the request was forwarded over — the only
// channel an answer is accepted from. The bool is false before the first
// candidate has been fixed.
//
// Both ends of a record are now channels, which is what closes the asymmetry the
// previous round left open here. The upstream is an arrival, so its channel was
// always at hand; the downstream is an egress, and the channel it names is one
// the LAYER chose before publishing rather than one it learned afterwards — the
// emitter's answer comes too late for phase 3 and, behind the class queue of §5,
// never comes at all (see Pipeline.requestPublisher).
func (r ReverseRecord) Downstream() (Downstream, bool) {
	if r.downstream.IsZero() {
		return Downstream{}, false
	}
	return r.downstream, true
}

// State returns pending or claimed.
func (r ReverseRecord) State() ReverseSlotState { return r.state }

// ProbesLeft returns the remaining budget of REFUSED answers.
func (r ReverseRecord) ProbesLeft() int { return r.probesLeft }

// ExpiresAt returns the end of the record's life.
func (r ReverseRecord) ExpiresAt() time.Time { return r.expiresAt }

type reverseEntry struct {
	expiresAt  time.Time
	label      Label
	dst        domain.PeerIdentity
	dtype      domain.DType
	upstream   Upstream
	downstream Downstream
	probesUsed int
	state      ReverseSlotState
	generation uint64
}

func (e *reverseEntry) view(budget int) ReverseRecord {
	return ReverseRecord{
		expiresAt:  e.expiresAt,
		label:      e.label,
		dst:        e.dst,
		downstream: e.downstream,
		dtype:      e.dtype,
		upstream:   e.upstream,
		probesLeft: budget - e.probesUsed,
		state:      e.state,
		generation: e.generation,
	}
}

// ---------------------------------------------------------------------------
// Handles
// ---------------------------------------------------------------------------

// ReverseSlot is the handle of a slot this caller took. It carries the
// generation of the entry, so a late rollback from an abandoned request
// cannot free the slot a FRESH request has meanwhile taken under the same
// label — the same ABA guard the replay reservation token implements.
type ReverseSlot struct {
	label      Label
	generation uint64
}

// Label returns the key the slot was taken under.
func (s ReverseSlot) Label() Label { return s.label }

// IsZero reports whether the handle is unset.
func (s ReverseSlot) IsZero() bool { return s.label.IsZero() }

// ProbeTicket is the handle of one reserved probe: the right to run the
// expensive validation of ONE candidate answer.
type ProbeTicket struct {
	label      Label
	generation uint64
	held       bool
}

// IsZero reports whether the ticket is unset.
func (t ProbeTicket) IsZero() bool { return !t.held }

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

// ReverseReserveOutcome is the verdict of taking a slot.
type ReverseReserveOutcome uint8

const (
	// ReverseReserveUnset is the zero value.
	ReverseReserveUnset ReverseReserveOutcome = iota
	// ReverseSlotReserved means the slot is now pending and owned by the
	// caller.
	ReverseSlotReserved
	// ReverseSlotBusy means the label already has a record. The existing one
	// is NOT overwritten: a repeated — possibly looped — request would
	// otherwise re-point downstream and the answer to the first forward would
	// lose its way back. No ban either, because a loop can be honest.
	ReverseSlotBusy
	// ReverseSlotCapped means the global or per-upstream cap refused the
	// record and nothing could be evicted fairly.
	ReverseSlotCapped
)

var reverseReserveOutcomeNames = map[ReverseReserveOutcome]string{
	ReverseReserveUnset: "unset",
	ReverseSlotReserved: "reserved",
	ReverseSlotBusy:     "busy",
	ReverseSlotCapped:   "capped",
}

// String returns the metric label of the outcome.
func (o ReverseReserveOutcome) String() string { return enumName(reverseReserveOutcomeNames, o) }

// ReverseReserveResult pairs the verdict with the slot handle.
type ReverseReserveResult struct {
	slot    ReverseSlot
	outcome ReverseReserveOutcome
}

// Outcome reports the verdict.
func (r ReverseReserveResult) Outcome() ReverseReserveOutcome { return r.outcome }

// Slot returns the handle. The bool is false for every refusal, so a caller
// cannot publish a frame under a slot it does not own.
func (r ReverseReserveResult) Slot() (ReverseSlot, bool) {
	if r.outcome != ReverseSlotReserved {
		return ReverseSlot{}, false
	}
	return r.slot, true
}

// ReverseProbeOutcome is the verdict of reserving one probe.
//
// It is an enumeration rather than a bool because there are THREE answers and
// only one of them is "granted". A refusal for lack of budget and a refusal
// because the validated record is no longer the record under that label are
// different events: the first names a live record that will pay for no more
// expensive work, the second an answer to an exchange that has already ended
// and whose budget was never touched. A caller handed one bool would report
// both as "budget exhausted" and tell the operator that a record ran out of
// probes nobody ever spent.
type ReverseProbeOutcome uint8

const (
	// ReverseProbeUnset is the zero value.
	ReverseProbeUnset ReverseProbeOutcome = iota
	// ReverseProbeGranted means one unit was taken and the ticket is held.
	ReverseProbeGranted
	// ReverseProbeExhausted means the record has no budget left. It stays
	// pending until expires_at — it is only the cryptographic work that
	// nobody pays for any more.
	ReverseProbeExhausted
	// ReverseProbeStale means the record the caller validated is gone:
	// rolled back, completed, expired, or replaced by a FRESH exchange under
	// the same label. Nothing was spent and nothing may be spent, because the
	// budget under that label now belongs to somebody else's exchange.
	ReverseProbeStale
)

var reverseProbeOutcomeNames = map[ReverseProbeOutcome]string{
	ReverseProbeUnset:     "unset",
	ReverseProbeGranted:   "granted",
	ReverseProbeExhausted: "exhausted",
	ReverseProbeStale:     "stale",
}

// String returns the metric label of the outcome.
func (o ReverseProbeOutcome) String() string { return enumName(reverseProbeOutcomeNames, o) }

// ReverseEvent names what happened to the reverse state, for the metric sink.
type ReverseEvent uint8

const (
	// ReverseEventUnset is the zero value.
	ReverseEventUnset ReverseEvent = iota
	// ReverseEventReserved counts a new pending record.
	ReverseEventReserved
	// ReverseEventBusy counts a label whose slot was already taken.
	ReverseEventBusy
	// ReverseEventCapped counts a record refused by the caps of §5.
	ReverseEventCapped
	// ReverseEventEvicted counts a record dropped to make room, fairly.
	ReverseEventEvicted
	// ReverseEventExpired counts a record that reached expires_at.
	ReverseEventExpired
	// ReverseEventDownstreamFixed counts a downstream written before the
	// frame is published.
	ReverseEventDownstreamFixed
	// ReverseEventRolledBack counts a record released because the candidate
	// list ran out.
	ReverseEventRolledBack
	// ReverseEventProbeSpent counts one reserved probe.
	ReverseEventProbeSpent
	// ReverseEventProbeExhausted counts an answer refused for lack of budget.
	ReverseEventProbeExhausted
	// ReverseEventClaimed counts a successful CAS pending → claimed.
	ReverseEventClaimed
	// ReverseEventClaimRefused counts a CAS that lost — a second answer.
	ReverseEventClaimRefused
	// ReverseEventCompleted counts a record freed after the answer was
	// successfully enqueued.
	ReverseEventCompleted
)

var reverseEventNames = map[ReverseEvent]string{
	ReverseEventUnset:           "unset",
	ReverseEventReserved:        "reserved",
	ReverseEventBusy:            "busy",
	ReverseEventCapped:          "capped",
	ReverseEventEvicted:         "evicted",
	ReverseEventExpired:         "expired",
	ReverseEventDownstreamFixed: "downstream_fixed",
	ReverseEventRolledBack:      "rolled_back",
	ReverseEventProbeSpent:      "probe_spent",
	ReverseEventProbeExhausted:  "probe_exhausted",
	ReverseEventClaimed:         "claimed",
	ReverseEventClaimRefused:    "claim_refused",
	ReverseEventCompleted:       "completed",
}

// String returns the metric label of the event.
func (e ReverseEvent) String() string { return enumName(reverseEventNames, e) }

// ---------------------------------------------------------------------------
// Narrow seams to M8
// ---------------------------------------------------------------------------

// reverseLimits is the WHOLE view the reverse table needs of §5: two numbers.
// M8 owns their tuning and their telemetry; the table only asks. Keeping the
// seam this narrow is what stops the limits package and the state machine
// from growing a shared idea of "how full is full".
type reverseLimits interface {
	// ReverseStateCaps returns the global cap and the per-upstream cap of
	// reverse records. Non-positive values mean "use the layer default".
	ReverseStateCaps() (global, perUpstream int)
}

// reverseMetrics is the counting seam. It is separate from the pipeline's
// metricsSink because the table is usable — and tested — on its own.
type reverseMetrics interface {
	ObserveReverseState(event ReverseEvent)
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

const (
	// DefaultReverseProbeBudget is the starting budget of REFUSED answers per
	// record (§4.2).
	DefaultReverseProbeBudget = 4

	// defaultReverseGlobalCap and defaultReversePerUpstreamCap are the
	// starting caps of §5 until M8 supplies its own.
	defaultReverseGlobalCap      = 4096
	defaultReversePerUpstreamCap = 64
)

// ReverseTableConfig wires the table.
type ReverseTableConfig struct {
	// Clock is the injectable time source, following the package convention.
	Clock func() time.Time
	// Limits is the optional §5 seam; nil means the layer defaults.
	Limits reverseLimits
	// Metrics is the optional counting seam.
	Metrics reverseMetrics
	// Probes is the starting probe budget; zero means the default.
	Probes int
}

// ReverseTable is the bounded store of reverse-state records.
//
// Locking contract: mu guards the whole map. Nothing external is called while
// it is held — the pipeline publishes frames and reaches handlers strictly
// outside these methods, which is what lets a fast answer
// arrive re-entrantly on the very same goroutine that is still publishing the
// request (§4.2, phase 3).
type ReverseTable struct {
	clock   func() time.Time
	limits  reverseLimits
	metrics reverseMetrics

	entries map[Label]*reverseEntry
	// byUpstream is the per-upstream record count, maintained INCREMENTALLY and
	// keyed by the QUOTA OWNER (upstreamKey).
	//
	// It replaces three full passes over the table on every forwarded request
	// — the sweep, the per-upstream load and the busiest-upstream tally, the
	// last of which allocated a map sized for the whole table. The request
	// plane is unsigned and cheap for an attacker to generate, so a per-frame
	// cost linear in the table size is a lever: only the per-peer frame budget
	// stood between a flood and O(n) work per frame.
	//
	// The key is neither the presented identity nor the channel, for the two
	// reasons Upstream states: a name is borrowable, so an identity-keyed bucket
	// is a quota a stranger spends on the node it names — and a channel dies with
	// its connection while a record outlives it, so a channel-keyed bucket is a
	// quota a reconnect renews.
	byUpstream map[upstreamKey]int

	// localRefusals counts capped refusals of THIS NODE's own requests, keyed
	// by the dtype that was turned away.
	//
	// It exists because "the shared quota is full" is not an answer anyone can
	// act on. Every locally originated request exchange — identity resolution,
	// the liveness probe, and whatever is added next — shares ONE bucket of
	// PerUpstreamCap slots, and a full local bucket REFUSES rather than
	// evicting. So one busy subsystem can stop another from ever starting a
	// lookup without exceeding a single limit of its own, and the only trace
	// of it was a counter that mixes local refusals with transit ones. The
	// question worth measuring is not how much was taken but WHO WAS TURNED
	// AWAY.
	//
	// Only the local bucket is attributed. A transit refusal already has its
	// own drop reason, and its dtype arrives from the wire — keying a map on it
	// would let a stranger grow this node's memory one invented type name at a
	// time. Local dtypes come from this build's own senders, so the key space
	// is ours; localRefusalDTypeCap is the backstop for the day that stops
	// being true.
	localRefusals map[domain.DType]uint64

	probes     int
	generation uint64

	// pending buffers the events of one critical section. CLAUDE.md forbids
	// holding a domain mutex across a callback into an external component, and
	// reverseMetrics is exactly that — an injected sink M9 supplies. A sink
	// that reads the table back (a "current depth" gauge beside the counter)
	// would otherwise self-deadlock on a non-reentrant mutex, which is a hang
	// rather than a panic.
	pending []ReverseEvent

	mu sync.Mutex
}

// NewReverseTable builds the table.
func NewReverseTable(cfg ReverseTableConfig) *ReverseTable {
	table := &ReverseTable{
		clock:      cfg.Clock,
		limits:     normaliseOptional(cfg.Limits),
		metrics:    normaliseOptional(cfg.Metrics),
		entries:    make(map[Label]*reverseEntry),
		byUpstream: make(map[upstreamKey]int),
		probes:     cfg.Probes,
	}
	if table.clock == nil {
		table.clock = time.Now
	}
	if table.probes <= 0 {
		table.probes = DefaultReverseProbeBudget
	}
	return table
}

// ReverseReserveOpts describes the record to create.
type ReverseReserveOpts struct {
	// ReceivedAt is the moment the request arrived (or was created locally);
	// expires_at is derived from it and nothing else.
	ReceivedAt time.Time
	// Label is the key: the one-shot tag from request.src.
	Label Label
	// Dst is the destination of the request, checked against the src of a
	// future answer.
	Dst domain.PeerIdentity
	// DType is the dtype of the REQUEST, kept for the pairing check.
	DType domain.DType
	// Upstream is where the answer must go.
	Upstream Upstream
}

// Reserve takes the slot of one label in state pending — phase 1 of §4.2.
//
// It runs only where a request is actually PUT ON THE WIRE — a transit forward
// or a locally created request, after the candidates are chosen: a request this
// node answers from its own handler, or drops, needs no state at all. An
// occupied slot is a plain refusal with no overwrite and no ban.
func (t *ReverseTable) Reserve(opts ReverseReserveOpts) ReverseReserveResult {
	if opts.Label.IsZero() || opts.Upstream.IsZero() {
		return ReverseReserveResult{outcome: ReverseReserveUnset}
	}
	t.mu.Lock()
	defer t.unlockAndPublish()

	now := opts.ReceivedAt
	if now.IsZero() {
		now = t.clock()
	}

	// The label is expired LAZILY rather than by a table-wide sweep: the sweep
	// is the expensive part, and it is needed only when a cap actually binds
	// (see makeRoomLocked). A ticker-driven Sweep of the owner keeps the table
	// tidy in the meantime, and every read path expires what it touches.
	if _, taken := t.liveLocked(opts.Label, now); taken {
		t.observeLocked(ReverseEventBusy)
		return ReverseReserveResult{outcome: ReverseSlotBusy}
	}
	if !t.makeRoomLocked(opts.Upstream, now) {
		t.observeLocked(ReverseEventCapped)
		t.recordRefusalLocked(opts.Upstream, opts.DType)
		return ReverseReserveResult{outcome: ReverseSlotCapped}
	}

	t.generation++
	entry := &reverseEntry{
		expiresAt:  now.Add(ReverseStateWindow()),
		label:      opts.Label,
		dst:        opts.Dst,
		dtype:      opts.DType,
		upstream:   opts.Upstream,
		state:      ReverseSlotPending,
		generation: t.generation,
	}
	t.insertLocked(entry)
	t.observeLocked(ReverseEventReserved)
	return ReverseReserveResult{
		outcome: ReverseSlotReserved,
		slot:    ReverseSlot{label: opts.Label, generation: entry.generation},
	}
}

// FixDownstream writes the CHANNEL the chosen candidate will be published over
// into the slot — phase 3 of §4.2, performed BEFORE the frame is handed to the
// writer so an answer cannot physically outrun the record.
//
// It is also phase 4: an enqueue failure means the frame never left towards
// that candidate, so the caller calls it again with the next one.
//
// A downstream naming no channel is REFUSED rather than stored. Storing one
// would leave the record in the state this round exists to remove: "forwarded"
// answered true while "over which channel" had no answer, so the only thing left
// to judge an arriving answer by would be the name the answering neighbour
// presents.
func (t *ReverseTable) FixDownstream(slot ReverseSlot, downstream Downstream) bool {
	if downstream.IsZero() {
		return false
	}
	t.mu.Lock()
	defer t.unlockAndPublish()
	entry, ok := t.ownedLocked(slot)
	if !ok {
		return false
	}
	entry.downstream = downstream
	t.observeLocked(ReverseEventDownstreamFixed)
	return true
}

// Rollback frees the record entirely — the "candidates ran out" branch of
// §4.2 phase 4. The generation guard makes a late rollback of an abandoned
// request a no-op instead of an eviction of somebody else's fresh record.
func (t *ReverseTable) Rollback(slot ReverseSlot) bool {
	t.mu.Lock()
	defer t.unlockAndPublish()
	entry, ok := t.ownedLocked(slot)
	if !ok {
		return false
	}
	t.removeLocked(entry)
	t.observeLocked(ReverseEventRolledBack)
	return true
}

// Lookup returns the live record of a label. A record past expires_at is not
// live, and neither is a missing one: both answer false, because "expired"
// and "never existed" lead to the same drop.
func (t *ReverseTable) Lookup(label Label) (ReverseRecord, bool) {
	t.mu.Lock()
	defer t.unlockAndPublish()
	entry, ok := t.liveLocked(label, t.clock())
	if !ok {
		return ReverseRecord{}, false
	}
	return entry.view(t.probes), true
}

// ReserveProbe is the atomic increment-and-test of §4.2: every candidate
// answer takes one unit of budget, and only the one that won a non-zero
// remainder enters the expensive check.
//
// Atomicity is the whole point. Without it several forged answers would each
// see a free budget and all of them would reach the expensive check — the limit
// would protect exactly the case that was already safe.
//
// It charges the RECORD the caller validated, not whatever holds the label
// now, and refuses a stale generation — the same ABA guard Rollback, Complete
// and Claim already had. The label is chosen by whoever sends the request, so
// between the Lookup that produced the record and this call the entry may have
// been rolled back, completed or expired and REPLACED by a fresh exchange. A
// budget taken by label alone is then taken from the new exchange for an answer
// belonging to the old one, and enough such answers leave a live exchange
// unable to pay for its own genuine reply.
func (t *ReverseTable) ReserveProbe(record ReverseRecord) (ProbeTicket, ReverseProbeOutcome) {
	t.mu.Lock()
	defer t.unlockAndPublish()
	entry, ok := t.liveLocked(record.label, t.clock())
	if !ok || entry.generation != record.generation {
		return ProbeTicket{}, ReverseProbeStale
	}
	if entry.probesUsed >= t.probes {
		// Exhaustion does NOT free the slot: the record stays pending until
		// expires_at, it is only the cryptographic work that nobody pays for
		// any more.
		t.observeLocked(ReverseEventProbeExhausted)
		return ProbeTicket{}, ReverseProbeExhausted
	}
	entry.probesUsed++
	t.observeLocked(ReverseEventProbeSpent)
	ticket := ProbeTicket{label: record.label, generation: entry.generation, held: true}
	return ticket, ReverseProbeGranted
}

// RefundProbe returns the unit taken by an attempt that turned out to be
// legitimate: only REFUSED attempts spend budget, a successful `forward`
// followed by a claim does not (§4.2).
func (t *ReverseTable) RefundProbe(ticket ProbeTicket) {
	if ticket.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.unlockAndPublish()
	entry, ok := t.entries[ticket.label]
	if !ok || entry.generation != ticket.generation || entry.probesUsed == 0 {
		return
	}
	entry.probesUsed--
}

// Claim is the CAS pending → claimed. It runs only after every refusable step
// has let the answer through — the read-only transport invariants, the probe
// budget and, on the local branch, the dtype pairing: a drop at any earlier
// step leaves the record pending, and the real answer can still arrive.
//
// It takes the RECORD the caller validated, not a bare label, and refuses a
// stale generation — the same ABA guard Rollback and Complete already had. The
// label is chosen by whoever sends the request, so between the Lookup that
// produced the record and this call the entry may have been rolled back and
// replaced by a FRESH exchange under the same label. Claiming by label alone
// would hand the new exchange's single answer slot to an answer whose
// read-only invariants — downstream, subject, dtype pairing — were checked
// against the old copy.
func (t *ReverseTable) Claim(record ReverseRecord) (ReverseSlot, bool) {
	t.mu.Lock()
	defer t.unlockAndPublish()
	entry, ok := t.liveLocked(record.label, t.clock())
	if !ok || entry.generation != record.generation || entry.state != ReverseSlotPending {
		t.observeLocked(ReverseEventClaimRefused)
		return ReverseSlot{}, false
	}
	entry.state = ReverseSlotClaimed
	t.observeLocked(ReverseEventClaimed)
	return ReverseSlot{label: record.label, generation: entry.generation}, true
}

// Complete frees the record after the answer REACHED its upstream — the ONLY
// release path of a claimed record (§4.2). That is a successful enqueue on a
// network upstream and a resolver that accepted the answer on a local one:
// both are the mutating step of their branch, and both end the exchange.
//
// The failure of either leaves the record claimed until expires_at on purpose:
// the answer is lost, the initiator retries with a fresh label, and no second
// chance is granted, or repeats could hammer the upstream for free.
func (t *ReverseTable) Complete(slot ReverseSlot) bool {
	t.mu.Lock()
	defer t.unlockAndPublish()
	entry, ok := t.ownedLocked(slot)
	if !ok {
		return false
	}
	t.removeLocked(entry)
	t.observeLocked(ReverseEventCompleted)
	return true
}

// Sweep drops every expired record and reports how many went. It is exported
// because the owner of the table runs it on a ticker; the read paths sweep
// lazily as well, so a table nobody sweeps still never answers with a dead
// record.
func (t *ReverseTable) Sweep() int {
	t.mu.Lock()
	defer t.unlockAndPublish()
	return t.sweepLocked(t.clock())
}

// ---------------------------------------------------------------------------
// Internals — every helper below requires t.mu
// ---------------------------------------------------------------------------

// liveLocked resolves a label to a non-expired entry.
func (t *ReverseTable) liveLocked(label Label, now time.Time) (*reverseEntry, bool) {
	entry, ok := t.entries[label]
	if !ok {
		return nil, false
	}
	if now.After(entry.expiresAt) {
		t.removeLocked(entry)
		t.observeLocked(ReverseEventExpired)
		return nil, false
	}
	return entry, true
}

// ownedLocked resolves a slot handle, refusing a stale generation.
func (t *ReverseTable) ownedLocked(slot ReverseSlot) (*reverseEntry, bool) {
	if slot.IsZero() {
		return nil, false
	}
	entry, ok := t.entries[slot.label]
	if !ok || entry.generation != slot.generation {
		return nil, false
	}
	return entry, true
}

func (t *ReverseTable) sweepLocked(now time.Time) int {
	removed := 0
	for _, entry := range t.entries {
		if now.After(entry.expiresAt) {
			t.removeLocked(entry)
			t.observeLocked(ReverseEventExpired)
			removed++
		}
	}
	return removed
}

// insertLocked and removeLocked are the ONLY ways an entry enters or leaves
// the table, so the per-upstream tally cannot drift from the map it summarises.
func (t *ReverseTable) insertLocked(entry *reverseEntry) {
	t.entries[entry.label] = entry
	t.byUpstream[entry.upstream.key()]++
}

func (t *ReverseTable) removeLocked(entry *reverseEntry) {
	delete(t.entries, entry.label)
	key := entry.upstream.key()
	if remaining := t.byUpstream[key] - 1; remaining > 0 {
		t.byUpstream[key] = remaining
		return
	}
	// The empty bucket is deleted rather than left at zero: byUpstream is keyed
	// by an admission key, which is a value an attacker mints one of per dialled
	// address, and keeping zero entries would make it the unbounded map the
	// record table is careful not to be.
	delete(t.byUpstream, key)
}

func (t *ReverseTable) caps() (global, perUpstream int) {
	global, perUpstream = defaultReverseGlobalCap, defaultReversePerUpstreamCap
	if t.limits == nil {
		return global, perUpstream
	}
	configuredGlobal, configuredPerUpstream := t.limits.ReverseStateCaps()
	if configuredGlobal > 0 {
		global = configuredGlobal
	}
	if configuredPerUpstream > 0 {
		perUpstream = configuredPerUpstream
	}
	return global, perUpstream
}

// makeRoomLocked enforces both caps of §5 and evicts by FAIRNESS: when the
// table is full, the victim is the oldest record of the QUOTA OWNER holding the
// most slots. A plain "oldest overall" would let one noisy neighbour push out
// everybody else's exchanges, which is the opposite of what the cap is for — and
// so would an owner counted once per channel, since a neighbour spreading its
// records over N sessions would then read as N quiet upstreams.
func (t *ReverseTable) makeRoomLocked(upstream Upstream, now time.Time) bool {
	global, perUpstream := t.caps()
	if t.loadLocked(upstream) >= perUpstream || len(t.entries) >= global {
		// A cap binds, so the ONE full pass this call may afford is worth
		// making: expired records are exactly the room a fair eviction should
		// not have to take from a live exchange.
		t.sweepLocked(now)
	}
	if t.loadLocked(upstream) >= perUpstream {
		return false
	}
	if len(t.entries) < global {
		return true
	}
	busiest, ok := t.busiestUpstreamLocked()
	if !ok {
		return false
	}
	return t.evictOldestOfLocked(busiest)
}

// loadLocked is an O(1) read of the incremental tally.
func (t *ReverseTable) loadLocked(upstream Upstream) int {
	return t.byUpstream[upstream.key()]
}

// busiestUpstreamLocked picks the upstream bucket holding the most records, with
// a deterministic tie-break so two nodes under the same load evict the same
// record and a test is not flaky.
//
// It walks the per-upstream tally, not the records, and allocates nothing: the
// tally has one entry per QUOTA OWNER with live state, never one per record.
//
// The tie keeps the GREATEST key under upstreamOrderLess, which is the same
// direction BaseReplayCache.noisiestOwnerLocked takes with ingressOwner.compare —
// and since the local marker is the least key in both orders, a tie never victimises
// this node's OWN exchange. It read the other way here and contradicted the rule
// its own eviction is for: local records are not attacker-generated, cannot be
// replayed from anywhere else, and are exactly what "fair" must protect.
func (t *ReverseTable) busiestUpstreamLocked() (upstreamKey, bool) {
	var (
		busiest upstreamKey
		best    int
		found   bool
	)
	for upstream, load := range t.byUpstream {
		switch {
		case !found, load > best:
			busiest, best, found = upstream, load, true
		case load == best && upstreamOrderLess(busiest, upstream):
			busiest = upstream
		}
	}
	return busiest, found
}

// evictOldestOfLocked drops the oldest record charged to one bucket. The walk
// spans EVERY channel of that owner, which is what makes the victim the noisy
// neighbour's own record rather than whichever of its sessions happens to be
// least loaded.
func (t *ReverseTable) evictOldestOfLocked(owner upstreamKey) bool {
	var (
		victim *reverseEntry
		found  bool
	)
	for _, entry := range t.entries {
		if entry.upstream.key() != owner {
			continue
		}
		if !found || entry.expiresAt.Before(victim.expiresAt) ||
			(entry.expiresAt.Equal(victim.expiresAt) && entry.generation < victim.generation) {
			victim, found = entry, true
		}
	}
	if !found {
		return false
	}
	t.removeLocked(victim)
	t.observeLocked(ReverseEventEvicted)
	return true
}

// observeLocked records an event for publication AFTER the mutex is released.
// It is the only way the table counts anything.
func (t *ReverseTable) observeLocked(event ReverseEvent) {
	if t.metrics == nil {
		return
	}
	t.pending = append(t.pending, event)
}

// unlockAndPublish releases the mutex and only then hands the buffered events
// to the sink. Every exported method defers exactly this instead of
// `defer t.mu.Unlock()`, so the "nothing external is called under t.mu"
// contract is visible at each call site rather than promised in a comment.
func (t *ReverseTable) unlockAndPublish() {
	pending := t.pending
	t.pending = nil
	t.mu.Unlock()

	for _, event := range pending {
		t.metrics.ObserveReverseState(event)
	}
}

// sameUpstream reports whether two upstreams share one accounting bucket. The
// local marker is its own bucket: our own requests must not be evicted to make
// room for a neighbour's, nor the other way round.
//
// It compares QUOTA OWNERS and nothing else. Two arrivals of one neighbour over
// two channels — a reconnect, or two parallel sessions — are ONE upstream, which
// is what stops a reconnect from renewing the quota; two neighbours presenting
// one name are two, because the name is not what anybody is billed by.
func sameUpstream(a, b Upstream) bool { return a.key() == b.key() }

// upstreamOrderLess is the deterministic tie-break of the fairness eviction, and
// the LEAST key is the one a tie spares (busiestUpstreamLocked). The local marker
// is least, so this node's own exchange survives a tie.
func upstreamOrderLess(a, b upstreamKey) bool {
	if a.local != b.local {
		return a.local
	}
	return a.owner.compare(b.owner) < 0
}

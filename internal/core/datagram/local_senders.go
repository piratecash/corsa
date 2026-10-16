package datagram

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// local_senders.go attributes locally originated sends to the subsystem that
// made them (docs/rpc/datagram.md, `local_senders`).
//
// The reverse table already counts refusals of the shared local request quota,
// but only by dtype — and two subsystems ask with the same dtype. A refusal of
// get_identity therefore could not say whether the resolver or the presence
// prober had filled the quota, or which of them was cut off by it, which is the
// one question a shared quota has to be able to answer. The sender is the
// missing dimension, and it is a closed enum: a counter key never comes from a
// frame, and a new subsystem that sends has to be named here to send at all.
//
// The count is taken in exactly one place, Pipeline.sendLocal, once per call,
// from the outcome the call returned. Transit has no sender and is not counted
// here; bytes are not counted here either — they belong to the transport
// totals.

// LocalSender names the subsystem of this node that originated a datagram.
type LocalSender uint8

const (
	// LocalSenderUnset is the zero value and never a sender: SendLocal refuses
	// it as `failed` and counts the attempt on the `unattributed` row.
	LocalSenderUnset LocalSender = iota
	// LocalSenderIdentityResolver resolves an unknown identity on demand
	// (get_identity requests).
	LocalSenderIdentityResolver
	// LocalSenderPresenceProber probes known contacts for presence — also with
	// get_identity requests, which is why dtype alone cannot attribute them.
	LocalSenderPresenceProber
	// LocalSenderIdentityDiscovery pushes this node's identity to a new
	// session (routed push_identity).
	LocalSenderIdentityDiscovery
	// LocalSenderDMControl sends direct-message control frames (routed
	// dm_control).
	LocalSenderDMControl

	// localSenderBound is one past the last sender; new senders go above it.
	localSenderBound
)

// localSendersUnattributed is the row an unset or unknown sender is counted on.
const localSendersUnattributed = "unattributed"

var localSenderNames = map[LocalSender]string{
	LocalSenderUnset:             "unset",
	LocalSenderIdentityResolver:  "identity_resolver",
	LocalSenderPresenceProber:    "presence_prober",
	LocalSenderIdentityDiscovery: "identity_discovery",
	LocalSenderDMControl:         "dm_control",
}

// ErrLocalSenderUnset is the error of a SendLocal call that named no sender.
var ErrLocalSenderUnset = errors.New("datagram: local send names no sender")

// String returns the stable metric label of the sender.
func (s LocalSender) String() string {
	if name, ok := localSenderNames[s]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", s)
}

// Valid reports whether the value names a real sender.
func (s LocalSender) Valid() bool {
	_, named := localSenderNames[s]
	return named && s > LocalSenderUnset && s < localSenderBound
}

// statsName is the row the sender is counted on.
func (s LocalSender) statsName() string {
	if !s.Valid() {
		return localSendersUnattributed
	}
	return s.String()
}

// slot is the counter slot of the sender; every non-sender shares slot zero,
// the unattributed row.
func (s LocalSender) slot() LocalSender {
	if !s.Valid() {
		return LocalSenderUnset
	}
	return s
}

// LocalSenders returns every real sender in declaration order.
func LocalSenders() []LocalSender {
	all := make([]LocalSender, 0, int(localSenderBound)-1)
	for sender := LocalSenderUnset + 1; sender < localSenderBound; sender++ {
		all = append(all, sender)
	}
	return all
}

// rejectionCause says which branch of the local or forwarding path produced a
// SendRejected outcome. It never leaves the package: callers keep reading
// Rejection(), and the cause exists so the attribution can split `rejected`
// without re-deriving the branch from an error string.
type rejectionCause uint8

const (
	// rejectionCauseUnset is never set on purpose: a rejection carrying it
	// falls into no cause, the counters stop adding up, and
	// TestEveryRejectionNamesItsCause refuses the literal that built it.
	rejectionCauseUnset rejectionCause = iota
	// rejectedByGate is an admission-gate verdict — the outcome
	// observeSendRefusal counts in SendRefusals.
	rejectedByGate
	// rejectedByQuota is the reverse table refusing for its caps.
	rejectedByQuota
	// rejectedBySlotBusy is the reverse table finding the label taken.
	rejectedBySlotBusy
	// rejectedByExpiry is a routed frame whose send window is already behind.
	rejectedByExpiry
	// rejectedByReplay is the anti-replay cache refusing the key.
	rejectedByReplay
	// rejectedByMode is a mode a caller cannot originate (response).
	rejectedByMode
	// rejectedByReverseState is a reverse reservation refused for any other
	// reason than busy or capped.
	rejectedByReverseState
)

// reverseRefusalCause maps a refused reverse reservation onto its cause.
func reverseRefusalCause(outcome ReverseReserveOutcome) rejectionCause {
	switch outcome {
	case ReverseSlotCapped:
		return rejectedByQuota
	case ReverseSlotBusy:
		return rejectedBySlotBusy
	default:
		return rejectedByReverseState
	}
}

// LocalSendCounts is one sender's row. The first level is the outcome kind,
// the second splits `rejected` by cause:
//
//	attempts == queued + no_route + failed + rejected
//	rejected == gate_refused + quota_capped + slot_busy + expired + other_rejected
//
// Fields are read one at a time, like MetricsSnapshot: under live sends two
// fields may be one increment apart, and at rest the identities are exact.
type LocalSendCounts struct {
	// Attempts is every SendLocal call.
	Attempts uint64 `json:"attempts"`
	// Queued counts SendQueued.
	Queued uint64 `json:"queued"`
	// NoRoute counts SendNoRoute — not a gate verdict, and not in SendRefusals.
	NoRoute uint64 `json:"no_route"`
	// Failed counts SendFailed, an unattributed call included.
	Failed uint64 `json:"failed"`
	// Rejected counts SendRejected; the next five split it.
	Rejected uint64 `json:"rejected"`
	// GateRefused is an admission-gate verdict, the same event SendRefusals
	// counts.
	GateRefused uint64 `json:"gate_refused"`
	// QuotaCapped is the shared local request quota refusing — the same event
	// the reverse table's LocalRefusals counts by dtype.
	QuotaCapped uint64 `json:"quota_capped"`
	// SlotBusy is a request whose label already has a reverse record.
	SlotBusy uint64 `json:"slot_busy"`
	// Expired is a routed frame whose send window had already closed.
	Expired uint64 `json:"expired"`
	// OtherRejected is every other refusal: a response the caller tried to
	// originate, an anti-replay refusal of the frame's key.
	OtherRejected uint64 `json:"other_rejected"`
}

// LocalSendersSnapshot is the published `local_senders` block. Every sender
// and the unattributed row are always present.
type LocalSendersSnapshot struct {
	// BySender maps a sender label (or "unattributed") to its row.
	BySender map[string]LocalSendCounts `json:"by_sender"`
	// StartedAt is when the counters began accumulating — the Metrics period.
	StartedAt *time.Time `json:"started_at"`
	// ReadAt is when the counters were read.
	ReadAt *time.Time `json:"read_at"`
}

// localSendCounters is one sender's atomics.
type localSendCounters struct {
	attempts      atomic.Uint64
	queued        atomic.Uint64
	noRoute       atomic.Uint64
	failed        atomic.Uint64
	rejected      atomic.Uint64
	gateRefused   atomic.Uint64
	quotaCapped   atomic.Uint64
	slotBusy      atomic.Uint64
	expired       atomic.Uint64
	otherRejected atomic.Uint64
}

// record charges one finished call.
func (c *localSendCounters) record(outcome SendOutcome) {
	c.attempts.Add(1)
	switch outcome.Kind() {
	case SendQueued:
		c.queued.Add(1)
	case SendNoRoute:
		c.noRoute.Add(1)
	case SendRejected:
		c.rejected.Add(1)
		c.recordRejection(outcome.cause)
	default:
		c.failed.Add(1)
	}
}

// recordRejection charges the cause of a rejection.
func (c *localSendCounters) recordRejection(cause rejectionCause) {
	switch cause {
	case rejectionCauseUnset:
		// Charged nowhere, on purpose: the causes then stop adding up to
		// `rejected`, which is the visible symptom of a branch that forgot to
		// say why. Filing it under other_rejected would hide exactly that.
	case rejectedByGate:
		c.gateRefused.Add(1)
	case rejectedByQuota:
		c.quotaCapped.Add(1)
	case rejectedBySlotBusy:
		c.slotBusy.Add(1)
	case rejectedByExpiry:
		c.expired.Add(1)
	case rejectedByReplay, rejectedByMode, rejectedByReverseState:
		c.otherRejected.Add(1)
	}
}

func (c *localSendCounters) read() LocalSendCounts {
	return LocalSendCounts{
		Attempts:      c.attempts.Load(),
		Queued:        c.queued.Load(),
		NoRoute:       c.noRoute.Load(),
		Failed:        c.failed.Load(),
		Rejected:      c.rejected.Load(),
		GateRefused:   c.gateRefused.Load(),
		QuotaCapped:   c.quotaCapped.Load(),
		SlotBusy:      c.slotBusy.Load(),
		Expired:       c.expired.Load(),
		OtherRejected: c.otherRejected.Load(),
	}
}

// ObserveLocalSend counts one SendLocal call by the sender that made it.
func (m *Metrics) ObserveLocalSend(sender LocalSender, outcome SendOutcome) {
	if m == nil {
		return
	}
	m.localSends[sender.slot()].record(outcome)
}

// LocalSendersSnapshot publishes the per-sender rows. Lock-free.
func (m *Metrics) LocalSendersSnapshot() LocalSendersSnapshot {
	snapshot := LocalSendersSnapshot{BySender: make(map[string]LocalSendCounts, localSenderBound)}
	if m == nil {
		return snapshot
	}
	for slot := LocalSenderUnset; slot < localSenderBound; slot++ {
		snapshot.BySender[slot.statsName()] = m.localSends[slot].read()
	}
	snapshot.StartedAt = m.startedAtStamp()
	readAt := time.Now().UTC()
	snapshot.ReadAt = &readAt
	return snapshot
}

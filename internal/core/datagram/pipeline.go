package datagram

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// pipeline.go is the inbound conveyor of §4.1: the common part shared by all
// three modes, the vocabulary of its outcomes, the narrow seams to the
// limits and metrics of §5, and the locally originated send path — which runs
// the SAME Reserve / Commit / Release cycle with incoming_peer = local.
//
// The order of the steps is the contract, not an implementation detail. Two
// invariants are the reason it is written out step by step:
//
//   - the replay key is committed only after the frame is proven AUTHENTIC
//     and DELIVERABLE;
//   - a node that has not declared itself a transit spends not one line of
//     state on somebody else's frames.
//
// Reference: docs/protocol/datagram.md §4.1, §4.1.1, §4.2, §4.4, §5, §7.

// ---------------------------------------------------------------------------
// Narrow seams to M8 (limits, budgets, metrics)
// ---------------------------------------------------------------------------

// cryptoBudget is the SECOND stage of the two-stage admission of §5: a
// fixed-price token per signature verification, charged IMMEDIATELY BEFORE
// ed25519.Verify and never earlier (§4.1 step 8).
//
// STAGE ONE IS NOT DECLARED HERE, and its absence is the wiring rule rather
// than an omission. Bytes and frames are charged on the RAW line before
// anything is decoded, which puts the charge above every refusal that reads the
// frame — and above two refusals the conveyor never sees at all: a connection
// that never negotiated the plane, and a line past MaxFrameLine. Only the owner
// of the socket stands above both of them, so
// the owner charges stage one and the conveyor does not. Two owners of one step
// would either double-charge every honest frame or leave the cheapest verdict
// on the node free (see node/datagram_integration.go, handleDatagramFrame).
//
// The key is the SAME AdmissionKey stage one was charged on, and it arrives
// with the frame (InboundOpts.BudgetKey) instead of being derived from the
// neighbour here. Deriving it was the defect: on an outbound session the
// neighbour's identity is its own claim — the challenge of that handshake
// travels the other way — so a stage-two charge keyed on it burned the tokens
// of whatever node the sender named, and reset itself on every reconnect.
type cryptoBudget interface {
	// ChargeVerifyFor takes one verification token from the budget of the
	// neighbour the frame was billed to. false means: refuse WITHOUT verifying.
	ChargeVerifyFor(key AdmissionKey) bool
}

// metricsSink counts what the pipeline decided. Everything the layer drops is
// dropped SILENTLY on the wire, so the metric is the only place a drop is
// observable at all.
type metricsSink interface {
	// ObserveInbound counts one processed inbound frame. reason is
	// DropReasonUnset for every non-drop outcome.
	ObserveInbound(mode domain.DatagramMode, outcome InboundOutcome, reason DropReason)
	// ObserveDrop counts a frame dropped OUTSIDE the inbound conveyor — a
	// frame the writer refused after the class queue released it. It moves
	// the reason breakdown only: the frame was already counted once as an
	// inbound outcome, or was never inbound at all.
	ObserveDrop(reason DropReason)
	// ObserveUnknownDType counts a frame addressed to this node whose dtype
	// is not in the registry: a silent drop, a live connection, no ban (§2).
	ObserveUnknownDType(dtype domain.DType)
	// ObserveReverseState counts one reverse-state transition (§4.2). The
	// same method satisfies reverseMetrics, so one M8 type serves both.
	ObserveReverseState(event ReverseEvent)
}

// OutboundFrame is everything the writer needs about one frame the layer is
// handing over, and nothing it does not.
//
// It is a struct rather than four arguments because two of its fields exist
// solely so the consumer does not have to recompute what the layer already
// knows:
//
//   - Line is the serialized wire line INCLUDING its newline. The layer has to
//     produce it anyway — the class queue accounts in exactly these bytes, and
//     so does the neighbour's inbound budget (§2.3, §5) — and handing it over
//     is what stops the netcore adapter from serializing the same frame a
//     second time;
//
//   - Class and SendUntil are what netcore.OutboundWrite is assembled from.
//     There is no wire field for the deadline and there cannot be one: for
//     routed frames it comes from the timing rule of §3.3 after the layer's
//     clamps, and for request/response it is computed locally as
//     arrival + queue_residence(control), because the node forming an answer
//     does not know anybody else's expires_at.
//
//   - Channel is the CHANNEL CONSTRAINT of §4.2, and it is the one field an
//     emitter may not treat as a hint. A zero value means "the scheduler chose a
//     neighbour; pick any of its gated sockets", which is the routed plane. A
//     SET value means "the only lawful place for this frame is that channel":
//     an emitter that cannot reach it MUST refuse the frame rather than fall
//     back to the identity map. Resolving through the identity map is precisely
//     how "back to whoever asked" became "to whoever the asker NAMED" — and on a
//     session this node dialled, the asker chooses that name (see channel.go).
//
//     Three kinds of frame carry a channel, and each of them owns a leg of one
//     exchange: an answer produced here, a `response` travelling to a record's
//     upstream, and — since the reverse record stores the channel a question left
//     over — a forwarded `request`. The last one is the pin that makes the
//     record's downstream defensible: without it the layer would have to accept
//     an answer from any socket of whatever peer answered to the stored name.
type OutboundFrame struct {
	// SendUntil is the deadline the writer re-checks immediately before the
	// socket write. Zero means "no deadline".
	SendUntil time.Time
	// Peer is the neighbour the frame is handed to. For a frame carrying a
	// Channel it is the neighbour's NAME and not its address: the channel
	// decides where the bytes go.
	Peer domain.PeerIdentity
	// Frame is the datagram itself.
	Frame protocol.DatagramFrame
	// Line is the serialized wire line, newline included.
	Line []byte
	// Channel pins the frame to one channel; zero leaves the socket to the
	// transport.
	Channel ChannelID
	// Class picks the write grace and the queue lane (§5).
	Class domain.DatagramClass
}

// Bytes is the serialized size — the ONE quantity §5 accounts in, so a caller
// cannot pick a different one by accident.
func (o OutboundFrame) Bytes() int { return len(o.Line) }

// newOutboundFrame serializes once, at the single boundary where the layer
// hands a frame over.
func newOutboundFrame(
	out egress,
	frame protocol.DatagramFrame,
	sendUntil time.Time,
) (OutboundFrame, error) {
	line, err := protocol.MarshalDatagramFrameLine(frame)
	if err != nil {
		return OutboundFrame{}, fmt.Errorf("datagram: serialize the outbound frame: %w", err)
	}
	return OutboundFrame{
		SendUntil: sendUntil,
		Peer:      out.peer,
		Frame:     frame,
		Line:      []byte(line),
		Channel:   out.channel,
		Class:     frame.Class,
	}, nil
}

// FrameEmitter hands a frame to a neighbour's write queue.
//
// The bool is "accepted by the queue", not "written to the socket": queued
// means queued, and the writer may still drop the frame on send_until. Nothing
// is owed back either way — the layer keeps no per-send record to close, and
// the ticket that goes down with the frame travels one way and brings nothing
// back (netcore/write_ticket.go).
//
// A false is not proof that nothing was written. It is built from a netcore
// status, and that status is exact only for a refusal at the door; a frame
// written just before a LATER frame killed the link is refused after the fact
// (docs/protocol/network_core.md, "Tracked sends"). The emitter answers false
// and its caller tries the next connection of the same peer, so the cost is a
// duplicate the receiving side drops, never a frame nobody sent.
type FrameEmitter interface {
	EmitTo(ctx context.Context, out OutboundFrame) bool
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

// InboundOutcome is what the pipeline did with one inbound frame.
type InboundOutcome uint8

const (
	// InboundOutcomeUnset is the zero value.
	InboundOutcomeUnset InboundOutcome = iota
	// InboundDelivered means a local handler accepted the frame.
	InboundDelivered
	// InboundForwarded means the frame was queued towards a next hop.
	InboundForwarded
	// InboundDropped means the frame ended here; Reason says why.
	InboundDropped
	// InboundAnswered means the target's handler produced an answer and it was
	// queued to the neighbour the request came from.
	InboundAnswered
)

var inboundOutcomeNames = map[InboundOutcome]string{
	InboundOutcomeUnset: "unset",
	InboundDelivered:    "delivered",
	InboundForwarded:    "forwarded",
	InboundDropped:      "dropped",
	InboundAnswered:     "answered",
}

// String returns the metric label of the outcome.
func (o InboundOutcome) String() string { return enumName(inboundOutcomeNames, o) }

// DropReason names WHY a frame was dropped. Everything the layer refuses is
// refused silently on the wire, so this enumeration is the whole observable
// surface of the pipeline's decisions.
type DropReason uint8

const (
	// DropReasonUnset is the zero value: no drop happened.
	DropReasonUnset DropReason = iota

	// --- common part, steps 1..3 and the sender proof ---

	// DropAdmission is the byte or frame-rate budget of the neighbour,
	// charged BEFORE any parsing — and therefore charged by the OWNER of the
	// receive path rather than by this conveyor, which only ever sees a line
	// somebody already paid for. The reason lives in this enumeration anyway,
	// because §10's ledger is one ledger: a drop counted under a name of its
	// own somewhere else could not be added to the rest.
	DropAdmission
	// DropFrameTooLarge is the MaxFrameLine gate of §2.3, counted on the raw
	// line including its newline and applied BEFORE the parser.
	//
	// It is its own reason and not a flavour of `malformed` because the two
	// are produced by different gates and only one of them is even reachable
	// on the wide peer-session reader. Sharing a counter made the gate
	// untestable: an oversize line with the gate removed simply reached the
	// parser, which refused it and incremented the same counter (§10, "dropped
	// by reason").
	DropFrameTooLarge
	// DropMalformed is a strict-parser refusal: duplicate key, unknown
	// field, out-of-bounds value, matrix violation, oversized payload.
	DropMalformed
	// DropUnknownHeaderVersion is a header version this build does not
	// implement: dropped WITHOUT forwarding and without ban.
	DropUnknownHeaderVersion
	// DropTTLExhausted is a RAW incoming ttl of zero.
	DropTTLExhausted

	// --- routed plane ---

	// DropTranscript is a frame whose transcript could not be built.
	DropTranscript
	// DropReplayDuplicate is a key the replay cache has already seen.
	DropReplayDuplicate
	// DropUndeliverable is the early deliverability sieve of step 7.
	DropUndeliverable
	// DropTTLBudget is `ttl > auth.max_ttl` on the RAW value.
	DropTTLBudget
	// DropNotYetValid is a frame from the future.
	DropNotYetValid
	// DropStale is a frame past the validity THIS node admitted it under,
	// which is bounded by the base anti-replay window: a node forwards nothing
	// it can no longer recognise as a repeat (ComputeDeadlines).
	DropStale
	// DropSendWindowExpired is a frame still inside its validity interval
	// whose clamped send_until is already behind now (DeadlinesExpired).
	//
	// It is kept apart from DropStale because the two say different things
	// about the SENDER's clock: `stale` is a frame this node may no longer
	// carry at all, while this one is a frame that arrived alive and simply
	// has no room left for the write §3.3 reserves for it. It is also kept
	// apart from DropForwardFailed, which is where such a frame used to be
	// counted: that reason means backpressure at the next hop, and reading a
	// timing refusal as a full queue sends an operator after the wrong fault.
	DropSendWindowExpired
	// DropCryptoBudget is the verification budget of the neighbour.
	DropCryptoBudget
	// DropFingerprint is Fingerprint(pubkey) != src. Ban-worthy.
	DropFingerprint
	// DropSignature is a signature that does not verify. Ban-worthy.
	DropSignature
	// DropUnknownDType is a frame addressed here whose type this node does
	// not implement: silent, live connection, no ban, no replay slot.
	DropUnknownDType
	// DropModeNotAllowedForType is a type refusing this mode — the gate that
	// makes mode demotion (§3.6) harmless.
	DropModeNotAllowedForType
	// DropClassNotAllowedForType is a type refusing this traffic class.
	DropClassNotAllowedForType
	// DropUnauthorized is a `reject` from the authorization hook: silent, and
	// WITHOUT committing the replay key.
	DropUnauthorized
	// DropTransitGate is a frame for somebody else arriving at a node that
	// does not advertise mesh_datagram_transit_v1.
	DropTransitGate
	// DropNoCandidates is an empty candidate set.
	DropNoCandidates
	// DropReserveDuplicate is a racing second instance of one frame.
	DropReserveDuplicate
	// DropReserveRejected is a deterministic capacity or quota refusal: the
	// anti-replay cache is full and this neighbour is the one charged for it
	// (§5). It is the ONLY way a reservation can be refused other than by a
	// duplicate — the cache is in memory and cannot fail halfway.
	DropReserveRejected
	// DropForwardFailed is every candidate refusing the frame.
	DropForwardFailed
	// DropHandlerRejected is a permanent refusal by the handler.
	DropHandlerRejected
	// DropHandlerFailed is a retryable fault of the handler.
	DropHandlerFailed

	// --- request / response planes ---

	// DropReverseSlotBusy is a label whose slot is already taken: no
	// overwrite, no ban — a repeat may be an honest loop.
	DropReverseSlotBusy
	// DropReverseSlotCapped is a reverse record refused by the caps of §5.
	DropReverseSlotCapped
	// DropReverseUnknownLabel is a response with no live record.
	DropReverseUnknownLabel
	// DropReverseWrongDownstream is a response from a neighbour the request
	// was never forwarded to.
	DropReverseWrongDownstream
	// DropReverseSubjectMismatch is `response.src` disagreeing with the
	// stored `request.dst`.
	DropReverseSubjectMismatch
	// DropReverseNotPending is a second answer to an already claimed record.
	DropReverseNotPending
	// DropReversePairing is a locally KNOWN answer type that does not
	// declare the stored request dtype among the requests it answers.
	DropReversePairing
	// DropReverseProbeExhausted is an answer that lost the atomic
	// increment-and-test of the probe budget.
	DropReverseProbeExhausted
	// DropReverseRecordStale is an answer whose validated record stopped being
	// the record under its label before the probe was reserved: rolled back,
	// completed, expired, or replaced by a fresh exchange. It is its own
	// reason because the two neighbouring ones would both misinform — nothing
	// was spent, so reverse_probe_exhausted would accuse a live record of
	// running out of a budget nobody touched, and the loss happened BEFORE the
	// expensive validation, which is exactly what reverse_claim_lost is read as
	// having paid for.
	DropReverseRecordStale
	// DropReverseClaimLost is a CAS pending → claimed that lost a race.
	DropReverseClaimLost
	// DropAnswerNotDelivered is an answer the writer queue refused.
	DropAnswerNotDelivered
	// DropRequestNoAnswer is a request whose handler did not accept: a
	// silent drop with NO answer, because answering on a refusal would
	// disguise it as success.
	DropRequestNoAnswer
	// DropUnsupportedMode is a mode this entry point does not serve.
	DropUnsupportedMode
	// DropWriterRefused is a frame the writer turned away AFTER the class
	// queue had already released it (§5). The queue never puts such a frame
	// back — already-queued frames are not re-ordered or resurrected — so
	// without this counter the frame vanished from every counter the node
	// publishes, which §10 forbids ("dropped by reason").
	DropWriterRefused
	// DropWriterPanicked is a frame lost the same way and at the same place,
	// to a writer that CRASHED instead of answering. It is its own reason
	// because the two mean opposite things to whoever reads the numbers:
	// writer_refused is ordinary backpressure and rises with load, while
	// writer_panicked is a defect in the adapter and any non-zero value is a
	// crash report waiting in the log — folded together, the second would be
	// invisible inside the first (§10).
	DropWriterPanicked
	// DropPlaneNotNegotiated is a datagram that arrived on a connection whose
	// handshake never established mesh_datagram_v1 — the peer did not advertise
	// it, or this node did not, since the negotiated set is the intersection of
	// the two.
	//
	// It is refused ABOVE the conveyor, by the owner of the receive path, which
	// is why it is counted through ObserveDrop and not as an inbound outcome:
	// nothing decided on the frame, so it must not move Observed. It is a fact
	// about the HANDSHAKE of that connection and about nothing in the frame,
	// which is why no drop reason read from the envelope can stand in for it.
	//
	// "Silent" in §2 is a statement about the WIRE — no error frame, no
	// tear-down, no ban score — and never about the operator: a neighbour off the
	// plane pushing frames at line rate has to be distinguishable from ordinary
	// load, and a Debug line is not a ledger.
	DropPlaneNotNegotiated

	// --- proof of the sender ---

	// DropUnprovenSender is a frame whose LOCAL delivery would reach a type that
	// DECLARED it requires a proven neighbour (SenderProofPolicy), arriving on a
	// direction where nothing about the neighbour has been proven — a session
	// THIS node dialled, where the welcome's address is the remote's own claim.
	//
	// It is its own reason and not a flavour of `unauthorized`, because the two
	// answer different questions and an operator acts on them differently:
	// `unauthorized` is a hook that looked at a KNOWN sender and said no, which
	// is a trust-list matter, while this one is the LAYER refusing to let the
	// hook look at a name the sender chose for itself, which is a matter of
	// which direction the handshake proved. Folded into one counter, a tightened
	// trust list and a peer borrowing somebody's fingerprint would be
	// indistinguishable.
	//
	// It is also why the refusal is not "hand the hook a zero identity": that
	// would be reported as `unauthorized` and read as the hook's own verdict,
	// and a hook whose trust list happens to be empty would look identical to a
	// hook that was never allowed to decide.
	//
	// No ban. Naming yourself in your own welcome is what the handshake asks
	// for, and §4.4 reserves punishment for the stable header and auth.
	DropUnprovenSender
)

var dropReasonNames = map[DropReason]string{
	DropReasonUnset:            "none",
	DropAdmission:              "admission",
	DropFrameTooLarge:          "frame_too_large",
	DropMalformed:              "malformed",
	DropUnknownHeaderVersion:   "unknown_header_version",
	DropTTLExhausted:           "ttl_exhausted",
	DropTranscript:             "transcript",
	DropReplayDuplicate:        "replay_duplicate",
	DropUndeliverable:          "undeliverable",
	DropTTLBudget:              "ttl_budget",
	DropNotYetValid:            "not_yet_valid",
	DropStale:                  "stale",
	DropSendWindowExpired:      "send_window_expired",
	DropCryptoBudget:           "crypto_budget",
	DropFingerprint:            "fingerprint_mismatch",
	DropSignature:              "invalid_signature",
	DropUnknownDType:           "unknown_dtype",
	DropModeNotAllowedForType:  "mode_not_allowed_for_type",
	DropClassNotAllowedForType: "class_not_allowed_for_type",
	DropUnauthorized:           "unauthorized",
	DropTransitGate:            "transit_gate",
	DropNoCandidates:           "no_candidates",
	DropReserveDuplicate:       "reserve_duplicate",
	DropReserveRejected:        "reserve_rejected",
	DropForwardFailed:          "forward_failed",
	DropHandlerRejected:        "handler_rejected",
	DropHandlerFailed:          "handler_failed",
	DropReverseSlotBusy:        "reverse_slot_busy",
	DropReverseSlotCapped:      "reverse_slot_capped",
	DropReverseUnknownLabel:    "reverse_unknown_label",
	DropReverseWrongDownstream: "reverse_wrong_downstream",
	DropReverseSubjectMismatch: "reverse_subject_mismatch",
	DropReverseNotPending:      "reverse_not_pending",
	DropReversePairing:         "reverse_pairing",
	DropReverseProbeExhausted:  "reverse_probe_exhausted",
	DropReverseRecordStale:     "reverse_record_stale",
	DropReverseClaimLost:       "reverse_claim_lost",
	DropAnswerNotDelivered:     "answer_not_delivered",
	DropRequestNoAnswer:        "request_no_answer",
	DropUnsupportedMode:        "unsupported_mode",
	DropWriterRefused:          "writer_refused",
	DropWriterPanicked:         "writer_panicked",
	DropPlaneNotNegotiated:     "plane_not_negotiated",
	DropUnprovenSender:         "unproven_sender",
}

// String returns the metric label of the reason.
func (r DropReason) String() string { return enumName(dropReasonNames, r) }

// InboundResult is what HandleInbound reports.
//
// Ban is a separate field rather than a class of drop reasons because §4.4 is
// explicit about where punishment belongs: violations of the STABLE HEADER
// and of auth — a fingerprint mismatch, a forged signature, a field out of
// bounds — which every datagram transit is obliged to check. An unknown dtype and a
// refused authorization are never ban-worthy, because
// the layer itself allows an honest node to relay a type it cannot read.
type InboundResult struct {
	err     error
	outcome InboundOutcome
	reason  DropReason
	ban     bool
}

// Outcome reports what happened to the frame.
func (r InboundResult) Outcome() InboundOutcome { return r.outcome }

// Reason returns the drop reason, DropReasonUnset for a non-drop.
func (r InboundResult) Reason() DropReason { return r.reason }

// Dropped reports whether the frame was dropped.
func (r InboundResult) Dropped() bool { return r.outcome == InboundDropped }

// BanWorthy reports whether the neighbour should be charged ban points.
func (r InboundResult) BanWorthy() bool { return r.ban }

// Err returns the cause behind the decision, for logs.
func (r InboundResult) Err() error { return r.err }

func dropped(reason DropReason, err error) InboundResult {
	return InboundResult{outcome: InboundDropped, reason: reason, err: err}
}

func droppedWithBan(reason DropReason, err error) InboundResult {
	return InboundResult{outcome: InboundDropped, reason: reason, err: err, ban: true}
}

func handled(outcome InboundOutcome) InboundResult {
	return InboundResult{outcome: outcome}
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

// PipelineConfig wires the pipeline. Everything the layer needs from the node
// arrives here through interfaces (see doc.go): the package must not import
// internal/core/node, and a hidden global would make the ordering contract
// untestable.
type PipelineConfig struct {
	// Clock is the injectable time source, following the package convention.
	Clock func() time.Time
	// Types is the ENDPOINT registry of §7: the handlers and authorizers of the
	// types this node implements. It decides local delivery and nothing about
	// transit.
	Types *TypeRegistry
	// ReplayCache is the anti-replay memory of the routed plane: RAM for the
	// freshness window, never storage. It is the ONLY state a frame occupies on
	// this node, and it is the same memory whether the frame is in transit or
	// addressed here — a relay accepts and forwards, and anti-replay is all it
	// may keep.
	//
	// The field names the CONCRETE cache, so a blocking or disk-backed memory is
	// not something to refuse in prose but a value that cannot be passed (see
	// "What the memory is NOT" in replay_cache.go).
	ReplayCache *BaseReplayCache
	// Reverse is the request/response reverse state (§4.2).
	Reverse *ReverseTable
	// Scheduler ranks and filters next-hop candidates (§4.3).
	Scheduler *Scheduler
	// Emitter publishes a frame to a neighbour together with its deadline.
	Emitter FrameEmitter
	// Queue is the weighted class queue of §5. When it is supplied the layer
	// puts it BETWEEN the conveyor and Emitter, which is the only placement
	// that makes "bulk keeps a guaranteed share under a constant control
	// stream" true of the real path rather than of a type nobody produces
	// into. The owner drains it — Pipeline.OutboundQueue().Run or Drain — for
	// the same reason it owns every other schedule in the layer.
	//
	// Optional: without it the conveyor publishes straight to Emitter, which
	// is what a unit test that reasons about one frame wants.
	Queue *WeightedQueue
	// Crypto is the VERIFICATION budget of §5 — stage two, and the only stage
	// the conveyor owns. Optional; nil charges nothing, which is the shape a
	// unit test reasoning about one frame wants.
	//
	// There is no companion field for stage one. It is charged by the owner of
	// the receive path, above this layer, because it has to stand above refusals
	// this layer never sees (cryptoBudget says which). A field here would be a
	// second owner of one step: wired, it double-charges; unwired, it is a seam
	// with no consumer.
	Crypto cryptoBudget
	// Metrics counts decisions. Optional.
	Metrics metricsSink
	// Advertised is the set THIS node advertises. It serves the transit gate
	// of §4.1 step 11 — one source of truth, so a node cannot relay while
	// telling the network it does not.
	Advertised AdvertisedCapabilities
	// Network is the network id bound into every transcript (§3.2).
	Network domain.NetworkID
	// LocalID is this node's address: the whole definition of dst == self.
	LocalID domain.PeerIdentity
}

// Pipeline processes inbound datagrams and places locally created ones.
//
// It holds NO mutable state of its own. Everything a frame touches belongs to a
// component with its own synchronisation — the replay cache and the reverse
// table — so a frame is processed end to end on the goroutine that received it,
// and a fast answer arriving re-entrantly during publication is served without a
// single lock being held across a handler or a socket write.
type Pipeline struct {
	clock      func() time.Time
	types      *TypeRegistry
	replay     *BaseReplayCache
	reverse    *ReverseTable
	scheduler  *Scheduler
	emitter    FrameEmitter
	outbound   *ClassQueueEmitter
	crypto     cryptoBudget
	metrics    metricsSink
	advertised AdvertisedCapabilities
	network    domain.NetworkID
	localID    domain.PeerIdentity
}

// NewPipeline validates the wiring. A missing dependency is refused at
// construction instead of nil-checked per call: a pipeline that silently
// skipped the reverse table would look like a routing bug months later.
func NewPipeline(cfg PipelineConfig) (*Pipeline, error) {
	required := []struct {
		name   string
		absent bool
	}{
		{"a local identity", cfg.LocalID.IsZero()},
		{"a network id", cfg.Network == ""},
		// isNilValue on every field whose type is an INTERFACE, never a bare
		// `== nil`: a TYPED nil satisfies an interface, so
		// `Emitter: (*myEmitter)(nil)` passed this table and failed on the first
		// frame instead. The rule is the seam's, not the field's — one function,
		// every accepted interface. ReplayCache is not one of them: it is a
		// concrete pointer, where `== nil` is the exact question.
		{"a type registry", isNilValue(cfg.Types)},
		{"a replay cache", cfg.ReplayCache == nil},
		{"a reverse state table", isNilValue(cfg.Reverse)},
		{"a scheduler", isNilValue(cfg.Scheduler)},
		{"a frame emitter", isNilValue(cfg.Emitter)},
	}
	for _, dependency := range required {
		if dependency.absent {
			return nil, fmt.Errorf("datagram: pipeline requires %s", dependency.name)
		}
	}
	if _, err := domain.ParseNetworkID(cfg.Network.String()); err != nil {
		return nil, err
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	emitter := cfg.Emitter
	var outbound *ClassQueueEmitter
	if cfg.Queue != nil {
		queued, err := NewClassQueueEmitter(ClassQueueEmitterConfig{
			Queue:   cfg.Queue,
			Out:     cfg.Emitter,
			Metrics: normaliseOptional(cfg.Metrics),
		})
		if err != nil {
			return nil, err
		}
		emitter, outbound = queued, queued
	}
	// OPTIONAL SEAMS ARE NORMALISED TO ABSENT, not refused.
	//
	// One rule for all of them, because the alternative was per-field and
	// therefore per-field-forgotten. A required dependency is refused at
	// construction — the build cannot work without it and a named error beats a
	// nil panic. An OPTIONAL one has a defined meaning for "not supplied", so a
	// typed nil is normalised to exactly that: it is the same statement made
	// clumsily, and refusing it would turn a harmless wiring habit into a build
	// failure while a bare `!= nil` would turn it into a call on nothing.
	//
	// Normalising also puts the decision in ONE place. Every reader downstream
	// tests the field against nil, and it is those readers that were wrong when
	// a typed nil got past here.
	crypto := normaliseOptional(cfg.Crypto)
	metrics := normaliseOptional(cfg.Metrics)

	return &Pipeline{
		clock:      clock,
		types:      cfg.Types,
		replay:     cfg.ReplayCache,
		reverse:    cfg.Reverse,
		scheduler:  cfg.Scheduler,
		emitter:    emitter,
		outbound:   outbound,
		crypto:     crypto,
		metrics:    metrics,
		advertised: cfg.Advertised,
		network:    cfg.Network,
		localID:    cfg.LocalID,
	}, nil
}

// ReverseState returns the reverse table, so the owner can sweep it on a
// ticker and a test can inspect it.
func (p *Pipeline) ReverseState() *ReverseTable { return p.reverse }

// OutboundQueue returns the class queue of §5 when the layer is wired with
// one. The bool is false for a pipeline that publishes straight to the writer.
//
// The owner drives it — Run in a goroutine, or Drain on its own schedule —
// because the layer starts no goroutine of its own (CLAUDE.md: a goroutine is
// not started "just in case").
func (p *Pipeline) OutboundQueue() (*ClassQueueEmitter, bool) {
	return p.outbound, p.outbound != nil
}

// ---------------------------------------------------------------------------
// Inbound — the common part (steps 1..3 and the sender proof)
// ---------------------------------------------------------------------------

// InboundOpts is one arriving frame.
type InboundOpts struct {
	// ReceivedAt is the moment of arrival. Everything derived from it —
	// expires_at of a reverse record, the answer's send deadline — is derived
	// from THIS value, so a test can pin the whole round trip to one instant.
	// The zero value means "now".
	ReceivedAt time.Time
	// Line is the raw wire line as the reader saw it, INCLUDING its newline.
	// The pipeline parses it itself, on these exact bytes: §3.4 requires the
	// strict parser to see what arrived, and the caller has already charged
	// stage one on the same quantity (§4.1 step 1).
	Line []byte
	// Peer is the identity the neighbour PRESENTS, and nothing more.
	//
	// It used to be described as "the label the conveyor keys its
	// channel-relative state on", and that description was wrong in the way that
	// mattered: a PeerIdentity is a node, not a socket. On an accepted
	// connection the presented identity is proven; on a session this node
	// dialled it is the peer's own claim — the challenge of that handshake
	// travels the other way — so keying reverse records, quotas and return
	// addresses on it let a neighbour that wrote somebody else's fingerprint
	// into its welcome share that node's state and pull that node's answers.
	// Channel-relative state keys on Channel below; this value is only ever a
	// NAME, and it leaves the layer with its level attached
	// (IngressPeer.PresentedIdentity).
	Peer domain.PeerIdentity
	// Channel is the transport channel the line arrived on, and it is REQUIRED.
	//
	// It is what every channel-relative decision keys on — the upstream of a
	// reverse record and its per-upstream quota, and the return path of an
	// answer — because those ask "which SOCKET is this", a question no name can
	// answer. A receive path that cannot produce one has nothing to key that
	// state on and its arrivals are refused, which is the closed direction: the
	// alternative is a conveyor that silently falls back to the identity map
	// exactly where the identity is a claim.
	Channel ChannelID
	// BudgetKey is WHO PAYS, and it is REQUIRED. It is the same key the caller
	// charged stage one on, carried down so stage two lands in the same bucket
	// — the whole reason it travels with the frame instead of being derived
	// from Peer here.
	//
	// Deriving it was the defect this field replaces: on an outbound session
	// Peer is a fingerprint the neighbour chose, so a verification charged
	// against it spent the tokens of whichever node the sender named, gave that
	// node's real traffic a crypto-budget refusal it never earned, and started
	// over on every reconnect.
	BudgetKey AdmissionKey
}

// HandleInbound runs the conveyor of §4.1 over one arriving frame.
//
// Step 1 — admission by bytes and frames, before any parsing — has ALREADY run
// when this is called: it belongs to the owner of the receive path, which is
// the only party standing above the refusals this function never sees
// (cryptoBudget states the argument). What arrives here is a line somebody has
// paid for, plus the key they paid with.
//
// Steps 2..3 are common to all three modes and are performed here, in this
// order and no other:
//
//  2. strict parsing and the mode matrix;
//  3. `ttl == 0` on the RAW value.
//
// Then the proof of the SENDER, which §4.1 gives no number of its own because
// §4.1 numbers the routed fork from 5 onwards: a frame that would end at a
// local handler whose type asks WHO SENT IT is refused when the direction
// proved nothing about the neighbour (senderProofGate). It stands above the
// fork because all three planes end at the same §7 hook.
//
// Then the frame forks by mode, because one common conveyor is not enough:
// a response has no route to its dst and must not have one, and a request may
// not be discarded for undeliverability before its own local handler has had
// the chance to answer it.
func (p *Pipeline) HandleInbound(ctx context.Context, in InboundOpts) InboundResult {
	if in.Peer.IsZero() {
		return p.observe(domain.DatagramMode(""), dropped(DropMalformed, errInboundNoPeer))
	}
	if in.BudgetKey.IsZero() {
		// A caller that cannot name who pays cannot have charged stage one
		// either, and the conveyor must not invent a bucket for it: the zero key
		// would be one budget every unbillable arrival on the node spends from,
		// and the crypto stage below would silently run unmetered.
		return p.observe(domain.DatagramMode(""), dropped(DropMalformed, errInboundNoBudgetKey))
	}
	if in.Channel.IsZero() {
		// A caller that cannot name the channel has handed the conveyor nothing
		// to key channel-relative state on, and the only value left to fall back
		// to is the presented identity — which is the defect this field exists to
		// remove. Refusing is the closed direction and, like the two refusals
		// above, it is a wiring fault of the caller rather than peer misbehaviour.
		return p.observe(domain.DatagramMode(""), dropped(DropMalformed, errInboundNoChannel))
	}
	frame, err := protocol.ParseDatagramFrameLine(string(in.Line))
	if err != nil {
		return p.observe(domain.DatagramMode(""), parseRefusal(err))
	}
	if result, refused := p.commonGates(frame); refused {
		return p.observe(frame.Mode, result)
	}

	received := in.ReceivedAt
	if received.IsZero() {
		received = p.clock()
	}
	arrival := inboundFrame{
		frame:      frame,
		peer:       in.Peer,
		channel:    in.Channel,
		budgetKey:  in.BudgetKey,
		receivedAt: received,
	}

	// The proof of the SENDER, before the mode fork, because all three forks end
	// at the same authorization hook and a gate written once cannot be forgotten
	// by a fourth entry point.
	if result, refused := p.senderProofGate(arrival); refused {
		return p.observe(frame.Mode, result)
	}

	switch frame.Mode {
	case domain.DatagramModeRouted:
		return p.observe(frame.Mode, p.handleRouted(ctx, arrival))
	case domain.DatagramModeRequest:
		return p.observe(frame.Mode, p.handleRequest(ctx, arrival))
	case domain.DatagramModeResponse:
		return p.observe(frame.Mode, p.handleResponse(ctx, arrival))
	default:
		// Unreachable through the parser, which enforces the closed matrix.
		return p.observe(frame.Mode, dropped(DropUnsupportedMode, nil))
	}
}

// The two refusals HandleInbound makes on its own arguments, before the line is
// even looked at. They are sentinels rather than inline messages because both
// are wiring faults of the CALLER — neither can be produced by a peer — and a
// caller's test has to be able to tell them from the parser's own refusal,
// which shares the drop reason and, unlike these, is ban-worthy.
var (
	// "presented" and not "authenticated": the conveyor requires a name to put
	// in front of the hooks and the logs, which both directions can produce.
	// Whether that name was ever proven is a separate question with a separate
	// answer (authority) and a separate refusal (DropUnprovenSender), and WHERE
	// the frame came from is a third one with a field of its own (Channel).
	errInboundNoPeer      = errors.New("datagram: inbound frame without a presented peer")
	errInboundNoBudgetKey = errors.New("datagram: inbound frame without an admission key")
	errInboundNoChannel   = errors.New("datagram: inbound frame without a transport channel")
)

// inboundFrame is the parsed arrival, carried through the three mode
// handlers so none of them re-derives the moment of arrival — or the channel it
// came in on, or the neighbour a budget is charged to.
//
// peer, channel and budgetKey are three fields because they are three facts, and
// the routed plane needs all of them at once: peer is the NAME the hooks are
// shown, channel is the socket every channel-relative decision keys on,
// budgetKey is who pays for the signature check. On an accepted connection the
// first and the third name the same neighbour; on an outbound session only the
// second and the third are defensible.
//
// A FOURTH fact — whether anybody proved that peer really is that node — is
// DERIVED from the two rather than stored beside them (authority). It is a
// derivation and not a field on purpose: a stored flag would be a second
// opinion about the same question, and the first time one receive path set it
// and another forgot, the two would disagree with nothing to notice it.
type inboundFrame struct {
	receivedAt time.Time
	frame      protocol.DatagramFrame
	peer       domain.PeerIdentity
	channel    ChannelID
	budgetKey  AdmissionKey
}

// ingress is the previous hop as the hooks, the router and the replay cache see
// it: the CHANNEL the frame arrived on, the name the neighbour presents, the
// level of proof behind that name, and the BUDGET KEY the arrival was billed to —
// all four in one value, so nothing downstream can read the name without the
// level being one accessor away, and nothing has to re-derive who pays.
//
// The budget key travels because the replay cache's per-neighbour quota may be
// keyed on nothing else (ingressOwner): the channel dies with its connection while
// a record outlives it, so a bucket keyed on the channel is one a reconnect
// renews. On the proven branch the key is the identity by definition, which is why
// only the claimed constructor is handed one.
//
// The level is not passed in; it is the derivation below, so the one place that
// can answer "was anything proven" is the one place that answers it.
func (f inboundFrame) ingress() IngressPeer {
	if f.authority().Proven() {
		return ProvenIngress(f.channel, f.peer)
	}
	return ClaimedIngress(f.channel, f.budgetKey, f.peer)
}

// authority is the ONE derivation of "has anybody proved who this neighbour
// is", and it reads the ADMISSION KEY rather than a flag of its own.
//
// The key already carries the fact. Its namespace is exactly the two
// directions' standing claims about the peer — proven identity or dialled
// host:port (AdmissionKeySpace) — and that discriminator was introduced
// precisely so a call site has to say what it knows. A second field beside it
// would be a second opinion about one question, and two opinions drift.
//
// The test is EQUALITY with the key the proven constructor would have produced
// for this peer, not merely the namespace. A caller that pairs a proven key
// with a different claimed identity has contradicted itself, and the only safe
// direction to resolve a contradiction in is the closed one: such an arrival is
// read as claiming, never as proving. That also makes the rule checkable in one
// expression instead of an invariant nobody runs.
func (f inboundFrame) authority() IngressAuthority {
	if f.budgetKey == ProvenIdentityKey(f.peer) {
		return AuthorityProven
	}
	return AuthorityClaimed
}

// errUnprovenSender is the cause behind DropUnprovenSender, so a caller's log
// carries WHY without matching on the drop reason alone.
var errUnprovenSender = errors.New(
	"datagram: the type requires a proven neighbour and this direction proved nothing about it")

// senderProofGate refuses, on a direction where the neighbour proved nothing,
// exactly the frames whose local delivery would go to a type that DECLARED it
// needs a proven neighbour.
//
// # What is refused and what is not
//
// The gate refuses a DECLARED REQUIREMENT, not a direction and not a hook. It
// used to read "the type registered an Authorizer" as the requirement, and that
// inference was wrong in both directions: §7 describes a sender authenticated by
// a signature INSIDE the payload, whose Authorizer never touches the neighbour's
// name and which has to keep working on every session this node dialled, while a
// type with no Authorizer at all may perfectly well build its HANDLER on who the
// neighbour is. So the requirement is a field of the registration
// (SenderProofPolicy) and the gate reads exactly that.
//
// Refusing the whole direction instead would take the request/response plane
// off every session this node dialled — most of a client node's traffic — to
// close a hole that only opens where a type says it depends on the answer.
//
// # Why here, above the fork
//
// All three planes end at the same §7 seam, so one gate covers them and a fourth
// entry point cannot be added without it. It also stands before anti-replay and
// before the verification budget, which is strictly better than refusing at the
// hook: a frame the layer will not authorize spends no crypto token and takes
// no reservation slot, exactly as §7 requires of `reject`.
//
// The bool reports a refusal.
func (p *Pipeline) senderProofGate(arrival inboundFrame) (InboundResult, bool) {
	if !p.deliversToATypeThatNeedsAProvenNeighbour(arrival) {
		return InboundResult{}, false
	}
	return dropped(DropUnprovenSender, errUnprovenSender), true
}

// deliversToATypeThatNeedsAProvenNeighbour is the gate's whole condition, kept
// apart from the verdict so the three facts it joins read as one sentence.
//
// This is the ONE place outside local delivery that touches the type registry,
// and it is not the coupling a stateless forwarder forbids: deliversHere is a
// CONJUNCT of the same sentence, so no frame this node is merely relaying can be
// refused by it, whatever this node did or did not register. The registry read
// stands first only because it is a lock-free map lookup while deliversHere may
// take the reverse state — the cheap filter runs first, and its answer alone
// decides nothing.
func (p *Pipeline) deliversToATypeThatNeedsAProvenNeighbour(arrival inboundFrame) bool {
	if arrival.authority().Proven() {
		return false
	}
	// An unknown dtype declared nothing and has no seam to mislead, and it is
	// refused one step later with its own reason and its own metric (§7).
	// Answering here would move that refusal onto this counter.
	entry, known := p.types.Lookup(arrival.frame.DType)
	if !known || !entry.RequiresProvenPeer() {
		return false
	}
	return p.deliversHere(arrival.frame)
}

// deliversHere answers "would this frame end at a handler of THIS node", which
// is the only place the authorization hook runs. A frame in transit never
// reaches §7 at all, and refusing it would turn a hardening rule into a routing
// outage for every type that declares a hook.
func (p *Pipeline) deliversHere(frame protocol.DatagramFrame) bool {
	addressed, mapped := localDeliveryTargets[frame.Mode]
	if !mapped {
		return false
	}
	return addressed(p, frame)
}

// localDeliveryTargets is "is this node the terminal" per mode. It is a table
// rather than a switch because the two answers are genuinely different
// questions — a routed frame and a request name their destination in `dst`,
// while a response names a LABEL and its terminal is decided by the reverse
// record's upstream — and a mode added later without a row here is read as
// "not delivered locally", which is the fail-open direction the gate must not
// take silently. The map is total over the closed matrix the parser enforces.
var localDeliveryTargets = map[domain.DatagramMode]func(*Pipeline, protocol.DatagramFrame) bool{
	domain.DatagramModeRouted:   (*Pipeline).addressedToSelf,
	domain.DatagramModeRequest:  (*Pipeline).addressedToSelf,
	domain.DatagramModeResponse: (*Pipeline).answersALocalExchange,
}

// addressedToSelf is the dst comparison of the two modes whose dst is an
// address.
func (p *Pipeline) addressedToSelf(frame protocol.DatagramFrame) bool { return p.isSelf(frame.Dst) }

// answersALocalExchange reports whether a response would be consumed here: a
// live record under its echoed label whose upstream is the local marker.
//
// Every failure to decide answers false, and that is safe rather than
// fail-open: a response the fork will not deliver locally is refused there, by
// the fork's own reasons — an unknown label, a stale record, a mismatched
// subject — none of which this gate should pre-empt with a reason of its own.
//
// The lookup is read-only and runs only on the cold path the caller already
// narrowed to: a claimed ingress carrying a type that declared it requires a
// proven neighbour.
func (p *Pipeline) answersALocalExchange(frame protocol.DatagramFrame) bool {
	header, err := NewDeliveryHeader(frame)
	if err != nil {
		return false
	}
	label, labelled := header.Label()
	if !labelled {
		return false
	}
	record, live := p.reverse.Lookup(label)
	if !live {
		return false
	}
	return record.Upstream().IsLocal()
}

// commonGates runs step 3. The bool reports a refusal.
//
// It is one gate and no longer two: the self-gate over `req_caps` stood here,
// and it went with the field. A node no longer judges a frame by names the
// SENDER put in the envelope — the only thing it may refuse for is its own
// role, which the peer that chose it as a next hop has already checked.
func (p *Pipeline) commonGates(frame protocol.DatagramFrame) (InboundResult, bool) {
	// On the RAW value: clamping first would resurrect the frame.
	if TTLExhausted(frame.TTL) {
		return dropped(DropTTLExhausted, nil), true
	}
	return InboundResult{}, false
}

// parseRefusal maps a strict-parser error onto a drop, deciding the one thing
// the caller cannot: whether the neighbour earned ban points.
//
// TWO refusals are not ban-worthy, and they are the two a neighbour can be
// handed by somebody else:
//
//   - an unknown header VERSION is the extension mechanism working as designed
//     (§2): a v3 frame crossing a v2 node must cost its sender nothing;
//   - a line past MaxFrameLine is a §2.3 size verdict about the LINE, not a
//     statement the sender made about the frame. The neighbour that relayed it
//     is not its author, and nothing in the envelope obliges it to have measured
//     the frame the way this node does, so the size rule is a silent drop under
//     its own reason and never a punishment.
//
// Everything else the parser refuses is a violation of the stable header, which
// every datagram node is obliged to check, and §4.4 puts exactly those in the
// ban-worthy set.
func parseRefusal(err error) InboundResult {
	switch {
	case errors.Is(err, protocol.ErrDatagramUnknownVersion):
		return dropped(DropUnknownHeaderVersion, err)
	case errors.Is(err, protocol.ErrFrameTooLarge):
		return dropped(DropFrameTooLarge, err)
	default:
		return droppedWithBan(DropMalformed, err)
	}
}

// chargeVerify is stage two, charged to the key the frame arrived with. It
// takes the key rather than the arrival so the one call site reads as the
// charge it is, and so no future call site can reach for arrival.peer instead.
func (p *Pipeline) chargeVerify(key AdmissionKey) bool {
	if p.crypto == nil {
		return true
	}
	return p.crypto.ChargeVerifyFor(key)
}

func (p *Pipeline) observe(mode domain.DatagramMode, result InboundResult) InboundResult {
	if p.metrics != nil {
		p.metrics.ObserveInbound(mode, result.outcome, result.reason)
	}
	return result
}

func (p *Pipeline) observeUnknownDType(dtype domain.DType) {
	if p.metrics != nil {
		p.metrics.ObserveUnknownDType(dtype)
	}
}

// emit is the ONE place the layer hands a frame over, so the serialization, the
// class and the deadline cannot diverge between the call sites. It reports
// false for both a refused queue and a frame that could not be serialized:
// neither one left the node, and §4.3 treats both as a local failure of this
// hop.
func (p *Pipeline) emit(
	ctx context.Context,
	out egress,
	frame protocol.DatagramFrame,
	sendUntil time.Time,
) bool {
	peer := out.peer
	outbound, err := newOutboundFrame(out, frame, sendUntil)
	if err != nil {
		log.Warn().
			Err(err).
			Str("peer", peer.String()).
			Str("channel", out.channel.String()).
			Str("dtype", frame.DType.String()).
			Msg("datagram: an outbound frame could not be serialized")
		return false
	}
	// A panic in the writer becomes `false` — "the frame was not taken" — which
	// is the writer's own documented failure value and the answer every caller
	// already has a path for. Nothing is owed back: the layer keeps no per-send
	// record that a missing terminal could strand.
	site := hookSite{hook: "EmitTo", peer: peer, dtype: frame.DType}
	return guardHook(site, false, func() bool {
		return p.emitter.EmitTo(ctx, outbound)
	})
}

// isSelf reports whether the frame is addressed to this node. It is a plain
// address comparison and is never asked of a response, whose dst is a label.
func (p *Pipeline) isSelf(id domain.PeerIdentity) bool { return id == p.localID }

// transitAllowed is the transit gate of §4.1 step 11. It is not a formality:
// the capability filter stops an HONEST neighbour from picking an
// endpoint-only client as a relay, but not a hostile one from handing it a
// frame directly. Without this gate a client with a non-empty routing table
// would start forwarding other people's frames and committing replay keys —
// and after self-contained authentication, signing a valid frame costs an
// attacker almost nothing, so its bounded storage would be evicted for free.
func (p *Pipeline) transitAllowed() bool {
	return p.advertised.Has(CapabilityDatagramTransitV1)
}

// ---------------------------------------------------------------------------
// Candidates
// ---------------------------------------------------------------------------

// selectFor asks the scheduler for the ordered next hops of a frame, WITHOUT
// touching any state, and hands back the FULL verdict — the list AND the policy
// refusal behind an empty list.
//
// Returning the whole verdict rather than just the slice is the point: the
// vocabulary of §4.3 (`no_route`, `rejected(unsupported_dtype)`,
// `rejected(missing_capability)`, `failed`) lives in that verdict, and a
// caller that threw it away could only ever answer `no_route` — which is
// exactly the refusal §4.3 forbids collapsing, because a policy refusal must
// stop a retry that `no_route` would encourage.
//
// It is called before the reservation on purpose: an empty candidate set must
// not consume a reservation slot, and §4.1 lists "no candidates" among the
// branches that happen BEFORE Reserve and therefore never call Release —
// releasing there would be worse than useless, since the key is one per frame
// and the release would strip a reservation a concurrent instance is holding.
func (p *Pipeline) selectFor(ctx context.Context, job sendJob) candidateSelection {
	return p.scheduler.selectFor(ctx, job)
}

// deliverableCandidates answers the read-only "is there anybody to hand it to"
// question of the early sieve. It never rotates the explore counter: the sieve
// publishes nothing, and §4.3 makes the rotation a property of a send.
func (p *Pipeline) deliverableCandidates(
	ctx context.Context,
	frame protocol.DatagramFrame,
	incoming IngressPeer,
) []RouteCandidate {
	selection := p.selectFor(ctx, sendJob{
		frame:    frame,
		incoming: incoming,
		avoid:    NoAvoidedNextHop(),
		readOnly: true,
	})
	if !selection.publishable() {
		return nil
	}
	return selection.candidates
}

// deliverable is the early sieve of §4.1 step 7: dst == self, or at least one
// viable candidate. A frame with nowhere to go must not be paid for with a
// signature verification.
//
// It has no exceptions left. The one that survived the durable cut belonged to a
// profile that could name a next hop the routing table does not know; a
// stateless forwarder has no such memory, so every frame it carries is one the
// ordinary route can place.
func (p *Pipeline) deliverable(ctx context.Context, arrival inboundFrame) bool {
	if p.isSelf(arrival.frame.Dst) {
		return true
	}
	return len(p.deliverableCandidates(ctx, arrival.frame, arrival.ingress())) > 0
}

// ---------------------------------------------------------------------------
// Locally originated sends
// ---------------------------------------------------------------------------

// LocalSendOpts is one frame created on this node.
type LocalSendOpts struct {
	// Frame is the datagram, already signed when the mode requires it.
	Frame protocol.DatagramFrame
	// Avoid is the optional avoid_next_hop exclusion (§4.3). It never
	// reaches the wire.
	Avoid AvoidedNextHop
	// FirstHop is the caller's first-hop preference — the guard set of
	// docs/protocol/presence.md §4.2. It never reaches the wire, and it never
	// removes a candidate: see PreferredFirstHops.
	FirstHop PreferredFirstHops
}

// SendLocal places a frame this node created.
//
// The point of the method is that a local frame is NOT a special case: it runs
// the same Reserve / Commit / Release cycle and the same candidate walk as a
// transited one. Without the reservation on the outgoing path two parallel
// sends of one signed frame would record different next hops, and a copy
// returning through a loop would not be recognised as our own repeat.
//
// The one thing that IS different is the ttl: the origin does not decrement
// (§4.1.1 rule 4), so the first hop receives the full budget.
func (p *Pipeline) SendLocal(ctx context.Context, opts LocalSendOpts) SendOutcome {
	return p.sendLocal(ctx, opts)
}

// sendLocal is the single local-send body.
//
// THERE IS NO RECOVERY BARRIER LEFT TO CHECK. It refused every local send until
// a startup pass over durable stores had finished, and with no durable store
// there is no pass and no state a restart could leave unfinished: the layer's
// whole memory is an in-memory cache that starts empty and is correct from the
// first frame.
func (p *Pipeline) sendLocal(ctx context.Context, opts LocalSendOpts) SendOutcome {
	switch opts.Frame.Mode {
	case domain.DatagramModeRouted:
		return p.sendLocalRouted(ctx, opts)
	case domain.DatagramModeRequest:
		return p.sendLocalRequest(ctx, opts)
	default:
		// A response is never created by a caller: it is produced by the
		// pipeline itself, out of reverse state that only the pipeline holds.
		return localRefusal()
	}
}

// sendLocalRouted runs the origin half of the forwarding cycle for the signed
// plane: the same timing rule, the same reservation and the same candidate walk
// a transited frame gets.
func (p *Pipeline) sendLocalRouted(ctx context.Context, opts LocalSendOpts) SendOutcome {
	frame := opts.Frame
	header, err := NewHeader(frame)
	if err != nil {
		return failedOutcome(true, err)
	}
	now := p.clock()
	decision := ComputeDeadlines(header, now)
	deadlines, ok := decision.Deadlines()
	if !ok {
		return failedOutcome(true, fmt.Errorf("datagram: local send refused by deadlines: %s", decision.Outcome()))
	}
	if decision.Outcome() == DeadlinesExpired {
		// The clamped send window is already behind now: nothing is enqueued
		// at all, exactly as §2.2 requires.
		return localRefusal()
	}
	transcript, err := protocol.BuildDatagramTranscript(frame, p.network)
	if err != nil {
		return failedOutcome(true, err)
	}

	return p.forwardRouted(ctx, forwardPlan{
		frame:     frame,
		key:       protocol.DatagramReplayKey(transcript),
		incoming:  LocalIngress(),
		deadlines: deadlines,
		avoid:     opts.Avoid,
		firstHop:  opts.FirstHop,
	})
}

// localRefusal is a policy refusal of a locally created frame. `rejected`
// means "repeating without changed conditions is pointless", which is exactly
// what a frame the layer will not carry deserves.
func localRefusal() SendOutcome {
	return SendOutcome{kind: SendRejected, local: true}
}

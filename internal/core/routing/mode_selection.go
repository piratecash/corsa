package routing

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Mode-selection telemetry: WHICH wire format this node chose for a peer,
// and WHY.
//
// The choice already happened on every announce before this file existed —
// it was simply invisible. classifyDeltaMode picked v3/v2/v1, sendFullAnnounce
// picked v3-or-legacy, and the only trace was a per-cycle atomic.Int32 that
// lived for one cycle and reached a Debug log line. An operator asking "what
// fraction of my neighbours forces me down to the legacy frame, and what are
// they missing" had nothing to read on a running node, which is the question
// a capability rollout is made of.
//
// THREE FINITE DIMENSIONS, and each answers a different question:
//
//   - AnnounceOperation — WHICH send made the choice. A delta and a full sync
//     are not interchangeable: the full-sync ladder has no v2 rung at all
//     (there is no v2 full frame; the legacy announce_routes IS the v1 full),
//     so mixing them into one "mode" histogram would invent a v2 full sync
//     that the wire has never carried.
//   - AnnounceMode — WHAT was chosen.
//   - ModeReason — WHY that and not something higher.
//
// ONE DECISION INCREMENTS EXACTLY ONE COUNTER. This is the property that makes
// the numbers addable: the sum over every (operation, mode, reason) triple is
// the number of send decisions, not the number of things that happened to be
// true about them. The per-cycle counters in announceToAllPeers do NOT have
// this property and were never meant to — a v3 baseline downgrade advances
// v3DowngradedNoBaseline AND then deltaV1, which is two increments describing
// one decision. They stay as they are (their consumer is the cycle log line);
// the cumulative counter here records the FINAL mode with the reason that
// produced it.
//
// The reason comes from the branch that actually decided, never from a second
// derivation of the same inputs. Re-deriving "why" next to "what" is how the
// two drift apart, and a telemetry field that contradicts the behaviour it
// describes is worse than no field.
//
// CARDINALITY IS BOUNDED BY THE RELEASE, not by what peers send. Every label is
// a constant from this package; no peer identity, address, error text or
// version number becomes a map key. The same rule already forced
// ObserveUnknownDType to count only a total (datagram/metrics.go) and capped
// localRefusals at localRefusalDTypeCap — a neighbour-controlled string as the
// key of a map this node grows is a memory defect wearing a metric's clothes.
//
// Reference: docs/refactoring/dht/05-rollout-metrics.md.

// AnnounceOperation names the send that performed a mode selection.
type AnnounceOperation uint8

const (
	// AnnounceOperationDelta is an incremental announce (the cursor path in
	// AnnounceLoop.announceToAllPeers). Ladder: v3 → v2 → v1.
	AnnounceOperationDelta AnnounceOperation = iota
	// AnnounceOperationFullSync is a full snapshot send — forced-full,
	// first-sync or connect-time. Ladder: v3 → legacy (v1). There is no v2
	// full frame on the wire, so AnnounceModeV2 never appears here.
	AnnounceOperationFullSync

	announceOperationSlots
)

var announceOperationNames = [announceOperationSlots]string{
	AnnounceOperationDelta:    "delta",
	AnnounceOperationFullSync: "full_sync",
}

// String returns the metric label of the operation.
func (o AnnounceOperation) String() string {
	if o >= announceOperationSlots {
		return "unknown"
	}
	return announceOperationNames[o]
}

// AnnounceMode names the wire format a selection settled on.
type AnnounceMode uint8

const (
	// AnnounceModeV1 is the legacy frame — announce_routes for a full sync,
	// the legacy incremental for a delta. It is the floor of both ladders and
	// the fallback every downgrade lands on.
	AnnounceModeV1 AnnounceMode = iota
	// AnnounceModeV2 is routes_update. Delta only.
	AnnounceModeV2
	// AnnounceModeV3 is route_announce_v3.
	AnnounceModeV3
	// AnnounceModeDivergence is not a wire format: it records that the two
	// capability snapshots for one peer disagreed, which sends the cycle down
	// the legacy path AND marks the peer for re-sync. It is a mode of its own
	// because "we chose v1 because the peer is old" and "we chose v1 because
	// our own two views of the peer disagree" call for different actions.
	AnnounceModeDivergence

	announceModeSlots
)

var announceModeNames = [announceModeSlots]string{
	AnnounceModeV1:         "v1",
	AnnounceModeV2:         "v2",
	AnnounceModeV3:         "v3",
	AnnounceModeDivergence: "divergence",
}

// String returns the metric label of the mode.
func (m AnnounceMode) String() string {
	if m >= announceModeSlots {
		return "unknown"
	}
	return announceModeNames[m]
}

// ModeReason names why a selection landed where it did.
//
// Three classes live in this one enum, and telling them apart is the whole
// point of the breakdown — they call for different actions:
//
//   - NEGOTIATED: no downgrade at all. This is the denominator of any
//     "fraction degraded" figure, which is why it is counted rather than
//     inferred from a total that nobody records.
//   - PEER INCOMPATIBILITY (ModeReasonMissing*): the neighbour did not
//     advertise something this build needs. Acting on it means waiting for
//     that neighbour to update — it is the rollout signal.
//   - LOCAL PROTOCOL STATE (ModeReasonNoWireBaseline*): the neighbour is
//     perfectly capable and we still sent the older frame, because the newer
//     one needs a baseline this session has not put on the wire yet. Counting
//     this as incompatibility would inflate the rollout signal with our own
//     bookkeeping and send an operator hunting for old peers that do not
//     exist.
type ModeReason uint8

const (
	// ModeReasonNegotiated means the highest mode this build can emit for the
	// operation was chosen. No downgrade happened.
	ModeReasonNegotiated ModeReason = iota
	// ModeReasonMissingRoutingV1 — the peer did not advertise mesh_routing_v1.
	ModeReasonMissingRoutingV1
	// ModeReasonMissingRoutingV2 — the peer did not advertise mesh_routing_v2.
	ModeReasonMissingRoutingV2
	// ModeReasonMissingRoutingV3 — the peer did not advertise mesh_routing_v3.
	ModeReasonMissingRoutingV3
	// ModeReasonMissingRelayV1 — the peer did not advertise mesh_relay_v1,
	// which both the v2 and v3 triplets require because the send-side dispatch
	// gates on it.
	ModeReasonMissingRelayV1
	// ModeReasonNoWireBaselineV2 — capabilities allowed v2, but no legacy
	// baseline has gone out on this session yet, so the peer's v2 receive gate
	// would refuse the frame. LOCAL state, not the peer's fault.
	ModeReasonNoWireBaselineV2
	// ModeReasonNoWireBaselineV3 — same for the v3 kind="full" baseline.
	ModeReasonNoWireBaselineV3
	// ModeReasonCapabilityDivergence — the announce state and the cycle target
	// disagreed about the peer's capabilities.
	ModeReasonCapabilityDivergence
	// ModeReasonUnknown is a REAL code, not a spare bucket: it is recorded when
	// the decision had no capability snapshot to reason about (no live
	// transport at selection time). It is never treated as "probably benign" —
	// a rising unknown means the telemetry stopped explaining the behaviour,
	// and that is itself the finding.
	ModeReasonUnknown
	// ModeReasonLocalRoutingV1Disabled … ModeReasonLocalRelayV1Disabled mean
	// THIS node does not advertise the capability, so the ladder was capped
	// here and the neighbour was never asked.
	//
	// These four exist because of a defect this telemetry had on its first
	// day: the capability set a send reasons about is the NEGOTIATED one — the
	// intersection of ours and theirs — so a locally disabled capability is
	// absent from it for EVERY neighbour, and the metric reported a fleet of
	// un-upgraded peers when the truth was one line of local configuration. An
	// operator acting on that would chase their whole network instead of
	// reading their own config.
	ModeReasonLocalRoutingV1Disabled
	ModeReasonLocalRoutingV2Disabled
	ModeReasonLocalRoutingV3Disabled
	ModeReasonLocalRelayV1Disabled

	modeReasonSlots
)

var modeReasonNames = [modeReasonSlots]string{
	ModeReasonNegotiated:             "negotiated",
	ModeReasonMissingRoutingV1:       "missing_mesh_routing_v1",
	ModeReasonMissingRoutingV2:       "missing_mesh_routing_v2",
	ModeReasonMissingRoutingV3:       "missing_mesh_routing_v3",
	ModeReasonMissingRelayV1:         "missing_mesh_relay_v1",
	ModeReasonNoWireBaselineV2:       "no_wire_baseline_v2",
	ModeReasonNoWireBaselineV3:       "no_wire_baseline_v3",
	ModeReasonCapabilityDivergence:   "capability_divergence",
	ModeReasonUnknown:                "unknown",
	ModeReasonLocalRoutingV1Disabled: "local_mesh_routing_v1_disabled",
	ModeReasonLocalRoutingV2Disabled: "local_mesh_routing_v2_disabled",
	ModeReasonLocalRoutingV3Disabled: "local_mesh_routing_v3_disabled",
	ModeReasonLocalRelayV1Disabled:   "local_mesh_relay_v1_disabled",
}

// String returns the metric label of the reason.
func (r ModeReason) String() string {
	if r >= modeReasonSlots {
		return ModeReasonUnknown.String()
	}
	return modeReasonNames[r]
}

// IsPeerIncompatibility reports whether the reason is "the NEIGHBOUR lacks a
// capability" as opposed to a local limitation, local protocol state, or no
// downgrade at all.
//
// Exported because the distinction is the point of the metric and must not be
// re-implemented by every reader as a list of string prefixes: a reason added
// later would silently fall outside such a list, and the fraction it feeds
// would be quietly wrong instead of loudly missing.
func (r ModeReason) IsPeerIncompatibility() bool {
	switch r {
	case ModeReasonMissingRoutingV1,
		ModeReasonMissingRoutingV2,
		ModeReasonMissingRoutingV3,
		ModeReasonMissingRelayV1:
		return true
	default:
		return false
	}
}

// IsLocalLimitation reports whether the downgrade was OURS: either a
// capability this node does not advertise, or a wire baseline this session has
// not sent yet.
//
// Kept apart from IsPeerIncompatibility because the actions are opposite —
// one is "wait for the fleet", the other is "read your own configuration" —
// and a reader that lumps them together will spend the difference chasing
// neighbours that are already up to date.
func (r ModeReason) IsLocalLimitation() bool {
	switch r {
	case ModeReasonLocalRoutingV1Disabled,
		ModeReasonLocalRoutingV2Disabled,
		ModeReasonLocalRoutingV3Disabled,
		ModeReasonLocalRelayV1Disabled,
		ModeReasonNoWireBaselineV2,
		ModeReasonNoWireBaselineV3:
		return true
	default:
		return false
	}
}

// ModeDecision is one (operation, mode, reason) triple with its count.
type ModeDecision struct {
	Operation AnnounceOperation
	Mode      AnnounceMode
	Reason    ModeReason
	Count     uint64
}

// ModeSelectionStats is the read-only projection of the counters.
//
// StartedAt is the beginning of the accumulation period: the counters are
// in-memory and reset to zero on restart, so a raw cumulative number means
// nothing without it. Decisions omits zero triples and is ordered
// deterministically (operation, then mode, then reason) so two consecutive
// reads are diffable field by field.
type ModeSelectionStats struct {
	StartedAt time.Time
	// ReadAt is when these counters were LOADED.
	//
	// It exists because the response that carries them also carries
	// `snapshot_at`, which belongs to the cached routing snapshot and does not
	// move while the table is unchanged. Two answers could therefore show the
	// same `snapshot_at` and different counts, and any rate computed from that
	// pair would be wrong by however long the table stood still. The period
	// these numbers cover is StartedAt → ReadAt, and nothing else.
	ReadAt    time.Time
	Decisions []ModeDecision
}

// ModeSelectionCounters accumulates mode selections for the lifetime of the
// process.
//
// Lock-free by construction: every counter is an atomic in a fixed-size array
// indexed by the enums, so recording costs one atomic add on the announce path
// and reading takes no mutex at all. This matters because the reader is
// fetchRouteSummary, which today touches no domain mutex of the Service
// (docs/locking.md); a metric that made the hot RPC read acquire peerMu would
// pay for observability with the thing being observed.
//
// A nil *ModeSelectionCounters is a valid no-op recorder. That is deliberate:
// the AnnounceLoop is constructed in tests without telemetry, and requiring
// every one of them to pass a counter set would make the option mandatory in
// name only — the first test to forget it would panic on a path that has
// nothing to do with what it is testing.
type ModeSelectionCounters struct {
	startedAt time.Time
	counts    [announceOperationSlots][announceModeSlots][modeReasonSlots]atomic.Uint64
}

// NewModeSelectionCounters returns counters whose accumulation period starts at
// startedAt.
func NewModeSelectionCounters(startedAt time.Time) *ModeSelectionCounters {
	return &ModeSelectionCounters{startedAt: startedAt}
}

// Record adds one decision. Out-of-range values are folded into the unknown
// reason rather than dropped: a decision that happened is data even when the
// caller mislabelled it, and silently discarding it would make the totals stop
// adding up — which is the one property this counter exists to have.
func (c *ModeSelectionCounters) Record(op AnnounceOperation, mode AnnounceMode, reason ModeReason) {
	if c == nil {
		return
	}
	if op >= announceOperationSlots || mode >= announceModeSlots {
		return
	}
	if reason >= modeReasonSlots {
		reason = ModeReasonUnknown
	}
	c.counts[op][mode][reason].Add(1)
}

// Snapshot projects the counters. Safe on a nil receiver, where it reports an
// empty period.
func (c *ModeSelectionCounters) Snapshot() ModeSelectionStats {
	if c == nil {
		return ModeSelectionStats{}
	}
	// Stamped BEFORE the loads, so the window a reader computes can only be
	// slightly too wide — never too narrow, which would over-state a rate.
	stats := ModeSelectionStats{StartedAt: c.startedAt, ReadAt: time.Now().UTC()}
	for op := AnnounceOperation(0); op < announceOperationSlots; op++ {
		for mode := AnnounceMode(0); mode < announceModeSlots; mode++ {
			for reason := ModeReason(0); reason < modeReasonSlots; reason++ {
				count := c.counts[op][mode][reason].Load()
				if count == 0 {
					continue
				}
				stats.Decisions = append(stats.Decisions, ModeDecision{
					Operation: op,
					Mode:      mode,
					Reason:    reason,
					Count:     count,
				})
			}
		}
	}
	sort.Slice(stats.Decisions, func(i, j int) bool {
		a, b := stats.Decisions[i], stats.Decisions[j]
		if a.Operation != b.Operation {
			return a.Operation < b.Operation
		}
		if a.Mode != b.Mode {
			return a.Mode < b.Mode
		}
		return a.Reason < b.Reason
	})
	return stats
}

// ClassifyFullSyncMode picks the wire format for a FULL snapshot send and says
// why.
//
// The full-sync ladder is two rungs, not three: v3 when the peer advertises the
// complete triplet, legacy announce_routes otherwise. There is no v2 full
// frame — routes_update cannot bootstrap a peer — which is why the v2
// capability plays no part here and AnnounceModeV2 is unreachable from this
// function.
//
// The bool result is the decision sendFullAnnounce and sendConnectTimeFullSync
// already made through PeerSupportsV3; returning it alongside the reason keeps
// ONE branch deciding both, so the telemetry cannot describe a choice the code
// did not make.
func ClassifyFullSyncMode(local, negotiated []PeerCapability) (AnnounceMode, ModeReason) {
	if PeerSupportsV3(negotiated) {
		return AnnounceModeV3, ModeReasonNegotiated
	}
	return AnnounceModeV1, missingFromTriplet(local, negotiated, domain.CapMeshRoutingV3)
}

// missingFromTriplet names the FIRST capability of the triplet the peer did not
// advertise, checked in a fixed order: v1, then the generation cap, then relay.
//
// Deterministic order matters more than which order is chosen. A peer missing
// two of the three would otherwise be attributed differently depending on
// iteration accidents, and two runs of the same fleet would produce two
// different pictures of the same problem. Only ONE reason is returned because
// only one counter may be incremented per decision; the others are not lost
// information worth a second counter — a peer missing v1 is legacy-only, and
// what else it lacks changes nothing about what we send it.
func missingFromTriplet(local, negotiated []PeerCapability, generation domain.Capability) ModeReason {
	for _, capability := range [...]domain.Capability{domain.CapMeshRoutingV1, generation, domain.CapMeshRelayV1} {
		if capabilitiesContain(negotiated, capability) {
			continue
		}
		return attributeMissing(local, capability)
	}
	return ModeReasonUnknown
}

// attributeMissing says WHOSE absence capped the ladder.
//
// The negotiated set is the INTERSECTION of what this node advertises and what
// the neighbour did, so a capability missing from it has exactly two possible
// owners — and they are the whole point of the distinction:
//
//   - it is not in OUR advertised set: the neighbour was never asked, and the
//     answer is local configuration. Reporting this as a missing neighbour
//     capability is how a metric sends an operator to upgrade a fleet that is
//     already up to date;
//   - it IS in ours: then the intersection dropped it because the neighbour
//     did not advertise it. That is the rollout signal.
//
// local nil is treated as "unknown local set" rather than "advertises
// nothing": a caller that could not read its own advertisement must not have
// every downgrade attributed to itself.
func attributeMissing(local []PeerCapability, capability domain.Capability) ModeReason {
	if local != nil && !capabilitiesContain(local, capability) {
		switch capability {
		case domain.CapMeshRoutingV1:
			return ModeReasonLocalRoutingV1Disabled
		case domain.CapMeshRoutingV2:
			return ModeReasonLocalRoutingV2Disabled
		case domain.CapMeshRoutingV3:
			return ModeReasonLocalRoutingV3Disabled
		case domain.CapMeshRelayV1:
			return ModeReasonLocalRelayV1Disabled
		default:
			return ModeReasonUnknown
		}
	}
	switch capability {
	case domain.CapMeshRoutingV1:
		return ModeReasonMissingRoutingV1
	case domain.CapMeshRoutingV2:
		return ModeReasonMissingRoutingV2
	case domain.CapMeshRoutingV3:
		return ModeReasonMissingRoutingV3
	case domain.CapMeshRelayV1:
		return ModeReasonMissingRelayV1
	default:
		return ModeReasonUnknown
	}
}

// capabilitiesContain reports whether caps contains target.
func capabilitiesContain(caps []PeerCapability, target domain.Capability) bool {
	for _, c := range caps {
		if c == target {
			return true
		}
	}
	return false
}

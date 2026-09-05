package datagram

import (
	"github.com/piratecash/corsa/internal/core/domain"
)

// usage.go reports what the datagram plane is holding, and closes the one
// blind spot the plane had: the reverse-state table published nothing at all —
// not its depth, not the occupancy of the quota every locally originated
// request competes for, not who that quota turned away.
//
// Everything here is a len or a counter read. Nothing walks a record set: the
// queue and the replay cache already maintain their own depths, and the
// reverse table keeps a per-owner tally precisely so a decision about a full
// bucket never has to count one.
//
// Reference: docs/refactoring/dht/13-measurements.md §2, §4.

// localRefusalDTypeCap bounds the attribution map of local refusals.
//
// The keys are this build's own dtypes, so the map cannot grow past the number
// of request types the node implements — a handful. The cap is not sized for
// that: it is there because "the keys are ours" is a property of today's
// callers rather than of the type, and an accounting map that could be grown
// by a future caller is the shape several of this tree's leaks had.
const localRefusalDTypeCap = 64

// Per-entry costs, resolved once at initialisation. Key plus value, excluding
// what they point at — see domain.ResourceGauge for what that omits.
var (
	reverseEntryBytes  = domain.SizeOfAll(Label{}, reverseEntry{})
	replayEntryBytes   = domain.SizeOfAll(domain.ReplayKey{}, baseEntry{})
	admissionPeerBytes = domain.SizeOfAll(AdmissionKey{}, peerBuckets{})
	exploreEntryBytes  = domain.SizeOfAll(exploreKey{}, exploreCounter{})
)

// CollectUsage assembles what the plane holds.
//
// Every component is optional for the same reason CollectDiagnostics makes
// them optional: a node that wired no queue should report an empty queue, not
// refuse to answer. A diagnostic that fails when a subsystem is absent is a
// diagnostic nobody calls.
//
// The queue reports BYTES rather than a count, and it is the one gauge here
// that is not a floor: a class queue tracks the exact byte weight of what it
// holds, because that weight is what its caps are expressed in. It is
// presented as a single-entry gauge so the shape stays uniform.
func CollectUsage(
	queue *WeightedQueue,
	replay *BaseReplayCache,
	reverse *ReverseTable,
	admission *PeerAdmission,
	scheduler *Scheduler,
) domain.SubsystemUsage {
	var gauges []domain.ResourceGauge

	if queue != nil {
		stats := queue.Stats()
		gauges = append(gauges, domain.NewResourceGauge("queued_bytes", queueBytes(stats), 1))
	}
	if replay != nil {
		gauges = append(gauges, domain.NewResourceGauge("replay_records", replay.Len(), replayEntryBytes))
	}
	if reverse != nil {
		gauges = append(gauges,
			domain.NewResourceGauge("reverse_records", reverse.Len(), reverseEntryBytes),
			// SATURATION, not memory, and the distinction is load-bearing: the
			// local slots are a SUBSET of the records above, so charging bytes
			// for them again would report the same records twice and make a
			// "floor" larger than the truth it claims to sit under.
			//
			// It is here because the resource it describes is the scarcest one
			// the plane has: a few dozen slots held for four minutes each,
			// shared by every subsystem that asks a question. Read it against
			// limits.reverse.per_upstream_cap.
			domain.NewSaturationGauge("reverse_local_slots", reverse.LocalSlots()),
		)
	}
	if admission != nil {
		gauges = append(gauges, domain.NewResourceGauge("admission_peers", admission.Stats().TrackedPeers, admissionPeerBytes))
	}
	if scheduler != nil {
		gauges = append(gauges, domain.NewResourceGauge("explore_counters", scheduler.exploreCounters(), exploreEntryBytes))
	}
	return domain.NewSubsystemUsage(domain.ResourceSubsystemDatagram, gauges...)
}

// queueBytes totals what the class queues hold right now.
//
// Both lanes are named explicitly rather than summed from a slice because
// QueueStats reports them as separate fields; a lane added later must be added
// here too, and failing to compile is the wrong outcome only if the omission
// would otherwise be silent — which is exactly what would happen if this
// summed a subset it did not name.
func queueBytes(stats QueueStats) int {
	return stats.ControlBytes + stats.BulkBytes
}

// exploreCounters reports how many destination/type pairs the explore rotation
// is tracking. Bounded by the rotation's own LRU; reported because the bound
// is a configured number and an operator sizing it needs to see it bind.
func (s *Scheduler) exploreCounters() int {
	if s == nil || s.rotator == nil {
		return 0
	}
	return s.rotator.len()
}

// len reports the rotation's tracked pair count.
func (r *exploreRotator) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// Len is the number of reverse-state records held at this instant, expired
// ones that nobody has swept included.
//
// It used to exist only in the test build, which is why the reverse table was
// the one component of the plane whose depth an operator could not see — and
// the one whose overflow refuses rather than evicts.
func (t *ReverseTable) Len() int {
	t.mu.Lock()
	defer t.unlockAndPublish()
	return len(t.entries)
}

// LocalSlots is how many of the shared local-request slots are occupied.
//
// Occupancy alone does not say whether the quota bound — LocalRefusals does —
// but a refusal count without an occupancy reads the same at one slot in use
// and at the ceiling.
func (t *ReverseTable) LocalSlots() int {
	t.mu.Lock()
	defer t.unlockAndPublish()
	return t.loadLocked(LocalUpstream())
}

// LocalRefusals returns, per dtype, how many of this node's own request
// exchanges the shared local quota has turned away. The map is a copy.
//
// This is the "who was refused" half of the isolation question. Its
// counterpart, "how much was taken", is LocalSlots, and neither is enough
// alone: a full bucket is normal while nobody else wants a slot, and a single
// refusal matters if the subsystem it refused is how contacts get resolved.
func (t *ReverseTable) LocalRefusals() map[domain.DType]uint64 {
	t.mu.Lock()
	defer t.unlockAndPublish()
	refusals := make(map[domain.DType]uint64, len(t.localRefusals))
	for dtype, count := range t.localRefusals {
		refusals[dtype] = count
	}
	return refusals
}

// recordRefusalLocked attributes one capped refusal to the caller that asked.
//
// Transit refusals are deliberately not recorded here: their dtype comes off
// the wire, and this map's safety rests on its keys being ours. A dtype the
// map has never seen is dropped once the cap is reached rather than evicting
// an existing key — losing the tail of an attribution is a smaller harm than
// losing the counts that were already accumulating, and reaching the cap is
// itself the signal that the assumption above no longer holds.
//
// The caller must hold t.mu.
func (t *ReverseTable) recordRefusalLocked(upstream Upstream, dtype domain.DType) {
	if !upstream.key().local {
		return
	}
	if t.localRefusals == nil {
		t.localRefusals = make(map[domain.DType]uint64, 4)
	}
	if _, tracked := t.localRefusals[dtype]; !tracked && len(t.localRefusals) >= localRefusalDTypeCap {
		return
	}
	t.localRefusals[dtype]++
}

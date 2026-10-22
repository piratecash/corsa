package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// The read has three explicit scopes: last immutable owner publications,
// actual NetCore operational item custody, and the one active scheduler batch.
// They are independent samples, never admission or a simultaneous heap census.
type ownedFindNodeResourceView struct {
	Owner                                                                                  findnode.ResourceSnapshot
	ForwardItems, AnswerItems, ItemBytes, CellBytes, PayloadBytes, NamedRoundEstimateBytes uint64
	Batch                                                                                  findnode.BatchResourceShape
	LocalQueueSlots, LocalQueueCapacity                                                    int
}

func (s *Service) ownedFindNodeResourceView() ownedFindNodeResourceView {
	var result ownedFindNodeResourceView
	if s == nil {
		return result
	}
	if c := s.bridgeCapture; c != nil {
		result.Owner = c.intake.ResourceSnapshot()
		if c.roundLoop != nil {
			if batch := c.roundLoop.batch.Load(); batch != nil {
				result.Batch = *batch
			}
		}
		if c.localLoop != nil {
			result.LocalQueueSlots = len(c.localLoop.slots)
			result.LocalQueueCapacity = cap(c.localLoop.slots)
		}
	}
	// Original owner counters include detached cores and cancelled-but-queued
	// items. Current ConnID/registry membership never defines their lifetime.
	held := result.Owner.Writer
	result.ForwardItems = held.ForwardItems
	result.AnswerItems = held.AnswerItems
	result.CellBytes = held.CellBytes
	result.PayloadBytes = held.PayloadBytes
	result.ItemBytes = (held.ForwardItems + held.AnswerItems) * netcore.OwnedQueueItemBytes()
	result.NamedRoundEstimateBytes = held.NamedRoundEstimateBytes
	return result
}
func ownedResourceInt(value uint64) int {
	maximum := uint64(^uint(0) >> 1)
	if value > maximum {
		return int(maximum)
	}
	return int(value)
}
func (s *Service) ownedFindNodeUsage() domain.SubsystemUsage {
	v := s.ownedFindNodeResourceView()
	o := v.Owner
	fixed := 0
	if o.Enabled {
		fixed = 1
	}
	local := 0
	if o.LocalEnabled {
		local = 1
	}
	coordinated := o.Local.AliasPending + o.Local.Consumed + o.Local.Unsignable
	return domain.NewSubsystemUsage(domain.ResourceSubsystemOverlay,
		domain.NewResourceGauge("owner_fixed_storage", fixed, o.FixedBytes),
		domain.NewResourceGauge("published_pending_rows", o.Pending, o.PendingEntryBytes),
		domain.NewResourceGauge("forward_calendars", o.Calendars, o.CalendarBytes),
		domain.NewResourceGauge("local_fixed_storage", local, o.LocalFixedBytes),
		domain.NewResourceGauge("target_cache_entries", o.Local.Cache.Entries, o.CacheEntryBytes),
		domain.NewResourceGauge("target_recipient_refs", o.Local.Cache.SessionReferences, o.RecipientBytes),
		domain.NewResourceGauge("coordinated_signing", coordinated, o.CoordinatedBytes),
		domain.NewResourceGauge("signing_slots", o.Local.CalendarPending, o.SigningSlotBytes),
		domain.NewResourceGauge("owned_queue_item_bytes", ownedResourceInt(v.ItemBytes), 1),
		domain.NewResourceGauge("owned_queue_cell_bytes", ownedResourceInt(v.CellBytes), 1),
		domain.NewResourceGauge("owned_queue_payload_bytes", ownedResourceInt(v.PayloadBytes), 1),
		domain.NewSaturationGauge("owned_forward_items", ownedResourceInt(v.ForwardItems)),
		domain.NewSaturationGauge("owned_answer_items", ownedResourceInt(v.AnswerItems)),
		domain.NewSaturationGauge("scheduler_batch_work_refs", ownedResourceInt(v.Batch.Works)),
		domain.NewSaturationGauge("local_queue_and_signing_slots", v.LocalQueueSlots))
}
func (s *Service) ownedFindNodeDiagnostics() map[string]any {
	v := s.ownedFindNodeResourceView()
	o := v.Owner
	return map[string]any{
		"installed": o.Enabled, "sample_kind": "independent published owner and operational custody samples",
		"intake": map[string]any{"sample_elapsed_ns": int64(o.SampleAt), "round": o.Round, "generation": o.Generation, "pending": o.Pending, "provisional": o.Provisional, "reserved": o.Reserved, "dispatching": o.Dispatching, "awaiting_answer": o.Awaiting, "terminal_dispatch": o.Terminal, "calendars": o.Calendars, "group_records": o.GroupRecords, "target_names": o.TargetNames, "aliases": o.Aliases,
			"counters": map[string]any{"offered": o.Intake.Offered, "refused": o.Intake.Refused, "replaced": o.Intake.Replaced, "committed": o.Intake.Committed, "taken": o.Intake.Taken, "answered": o.Intake.Answered, "assumption_violations": o.Intake.Violations, "saturated": o.Intake.Saturated}},
		"local":                  map[string]any{"installed": o.LocalEnabled, "sample_elapsed_ns": int64(o.Local.SampleAt), "sequence": o.Local.Sequence, "sequence_saturated": o.Local.SequenceSaturated, "cache": o.Local.Cache, "counters": o.Local.Local, "alias_pending": o.Local.AliasPending, "consumed": o.Local.Consumed, "unsignable": o.Local.Unsignable, "calendar_pending": o.Local.CalendarPending, "node_slots": v.LocalQueueSlots, "node_capacity": v.LocalQueueCapacity},
		"writer_custody":         map[string]any{"forward_items": v.ForwardItems, "answer_items": v.AnswerItems, "item_bytes": v.ItemBytes, "item_bytes_scope": "graph-bearing operational wrappers only; empty detached wrappers may remain until CoreGC and are excluded, no reclamation claim", "cell_bytes": v.CellBytes, "payload_bytes": v.PayloadBytes, "sample_kind": "independent atomic loads; counts and bytes may transiently differ, not a coherent tuple or heap census", "scope": "preoffer producer until nonoffer return, or sole queue/active writer until actual final use; logical Cancel/Finish do not release"},
		"retained_named_storage": map[string]any{"writer_unshared_round_estimate_bytes": v.NamedRoundEstimateBytes, "scheduler_batch_bytes": v.Batch.BatchBytes, "scheduler_work_refs": v.Batch.Works, "scheduler_round_estimate_bytes": v.Batch.NamedRoundEstimateBytes, "included_in_floor": false, "sharing": "per-work named cut/claim/execution/plan/receipt estimate repeats shared graphs; distinct rounds retain separate arrays; batch and writer scopes may overlap", "scope": "structural named storage only, no allocator/map/GC/TLS/session-pointee census; T5 actual retained graph measurement remains open"},
	}
}

package connbudget

import "time"

// TotalObservationKey is a frozen caller reference, not provenance or a load
// witness. The original source validates tokens only when Total is enabled.
type TotalObservationKey struct {
	Anchor                            string
	Epoch, ClockDomain, Configuration [16]byte
}
type TotalObservationCut struct {
	Available bool
	Rows      int
	Sequence  uint64
	At        time.Duration
	Full      bool
}
type TotalObservationTransition struct {
	Sequence     uint64
	Full         bool
	Count        int
	Lower, Upper time.Duration
	Done, Valid  bool
}

// Every original raw snapshot field is copied; Config includes all five
// actual immutable limits. PastOnly never certifies the present through a gap.
// These are local duration bounds, not UTC or a daily/benign-load assessment.
type TotalObservationSnapshot struct {
	Status, Reason string
	Key            TotalObservationKey
	Config         Config
	CoverageStart  time.Duration
	Cut            TotalObservationCut
	Rows           []TotalObservationTransition
	PastOnly       bool
}

// TotalObservation has no setter, installer, worker or second budget owner.
type TotalObservation struct{ budget *Budget }

// NewWithTotalObservation delegates once to the existing private constructor.
// Capacity is an explicit caller allocation request, not a selected deployment
// limit or proof that arbitrary allocation fits. Total0 remains DISABLED before
// token/capacity/clock checks, without row allocation or observation clock calls.
// Invalid Config preserves New's original error; enabled source failures keep
// the real admission budget with UNKNOWN evidence.
func NewWithTotalObservation(cfg Config, key TotalObservationKey, capacity int, clock func() (time.Duration, error)) (*Budget, *TotalObservation, error) {
	b, err := newObservedBudget(cfg, totalSourceKey{anchor: key.Anchor, epoch: key.Epoch, clockDomain: key.ClockDomain, configuration: key.Configuration}, capacity, clock)
	if err != nil {
		return nil, nil, err
	}
	return b, &TotalObservation{budget: b}, nil
}
func (o *TotalObservation) Snapshot() TotalObservationSnapshot {
	var budget *Budget
	if o != nil {
		budget = o.budget
	}
	v := budget.fullSnapshot()
	result := TotalObservationSnapshot{Status: v.status, Reason: v.reason, Key: TotalObservationKey{Anchor: v.key.anchor, Epoch: v.key.epoch, ClockDomain: v.key.clockDomain, Configuration: v.key.configuration}, Config: v.cfg, CoverageStart: v.coverageStart, Cut: TotalObservationCut{Available: v.cut.available, Rows: v.cut.rows, Sequence: v.cut.sequence, At: v.cut.at, Full: v.cut.full}, PastOnly: v.pastOnly}
	if v.rows != nil {
		result.Rows = make([]TotalObservationTransition, len(v.rows))
		for i, row := range v.rows {
			result.Rows[i] = TotalObservationTransition{Sequence: row.sequence, Full: row.full, Count: row.count, Lower: row.lower, Upper: row.upper, Done: row.done, Valid: row.valid}
		}
	}
	return result
}

package node

import (
	"crypto/rand"
	"errors"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const maxPendingObservationCount = int64(^uint64(0) >> 1)

// pendingQueueObservation is owned entirely by Service.deliveryMu. It retains
// constant state, not a lossy event stream. Normal roots do not install it.
// readDuration is immutable from construction, before the source is attached
// and before Service.Run; it must be a validated boot/monotonic duration source,
// not trustedclock.Clock.Now (which deliberately holds a stale read on errors).
type pendingQueueObservation struct {
	id             [16]byte
	readDuration   func() (time.Duration, error)
	last           time.Duration
	haveLast       bool
	baseline       domain.PendingQueueTimeBounds
	count          int64
	countValid     bool
	durationValid  bool
	reason         string
	sequence       uint64
	open           bool
	begin          time.Duration
	delta          int64
	runStart       domain.PendingQueueTimeBounds
	runStartCount  int64
	runHasIncrease bool
	hasGrowingRun  bool
	longest        domain.PendingQueueDurationBounds
}

// newPendingQueueObservation constructs the private candidate while the caller
// owns deliveryMu, around a stable actual-frame baseline. There is no runtime
// installer, clock setter, public RPC, worker or ordinary-root activation.
func newPendingQueueObservation(count int64, readDuration func() (time.Duration, error)) (*pendingQueueObservation, error) {
	if count < 0 || readDuration == nil {
		return nil, errors.New("pending observation: invalid baseline or duration source")
	}
	o := &pendingQueueObservation{readDuration: readDuration, count: count, countValid: true, durationValid: true, runStartCount: count}
	if _, err := rand.Read(o.id[:]); err != nil {
		return nil, err
	}
	lo, ok := o.read()
	if !ok {
		return nil, errors.New("pending observation: initial duration unavailable")
	}
	hi, ok := o.read()
	if !ok {
		return nil, errors.New("pending observation: initial duration invalid")
	}
	o.baseline = domain.PendingQueueTimeBounds{Earliest: lo, Latest: hi}
	o.runStart = o.baseline
	return o, nil
}

func (o *pendingQueueObservation) unknown(reason string) {
	if o.reason == "" {
		o.reason = reason
	}
}

func (o *pendingQueueObservation) read() (time.Duration, bool) {
	now, err := o.readDuration()
	if err != nil || now < 0 {
		o.durationValid = false
		o.unknown("duration_read_failed")
		return 0, false
	}
	if o.haveLast && now < o.last {
		o.durationValid = false
		o.unknown("duration_regressed")
		return 0, false
	}
	o.last, o.haveLast = now, true
	return now, true
}

func (s *Service) beginPendingQueueObservationLocked() {
	o := s.pendingObservation
	if o == nil {
		return
	}
	if o.open {
		o.countValid = false
		o.unknown("unclosed_owner_transaction")
	}
	o.open, o.delta = true, 0
	o.begin, _ = o.read()
}

func (s *Service) stagePendingQueueObservationLocked(delta int64) {
	o := s.pendingObservation
	if o == nil || delta == 0 {
		return
	}
	if !o.open {
		o.countValid = false
		o.unknown("mutation_without_owner_boundary")
		return
	}
	if (delta > 0 && o.delta > maxPendingObservationCount-delta) ||
		(delta < 0 && o.delta < -maxPendingObservationCount-1-delta) {
		o.countValid = false
		o.unknown("count_overflow")
		return
	}
	o.delta += delta
}

func pendingRunDuration(start, end domain.PendingQueueTimeBounds) domain.PendingQueueDurationBounds {
	var d domain.PendingQueueDurationBounds
	if end.Earliest > start.Latest {
		d.Lower = end.Earliest - start.Latest
	}
	if end.Latest > start.Earliest {
		d.Upper = end.Latest - start.Earliest
	}
	return d
}

func extendPendingRun(longest domain.PendingQueueDurationBounds, start, end domain.PendingQueueTimeBounds) domain.PendingQueueDurationBounds {
	d := pendingRunDuration(start, end)
	if d.Lower > longest.Lower {
		longest.Lower = d.Lower
	}
	if d.Upper > longest.Upper {
		longest.Upper = d.Upper
	}
	return longest
}

func (s *Service) commitPendingQueueObservationLocked() {
	o := s.pendingObservation
	if o == nil {
		return
	}
	if !o.open {
		o.countValid = false
		o.unknown("commit_without_owner_boundary")
		return
	}
	end, _ := o.read()
	o.open = false
	if o.sequence == ^uint64(0) {
		o.countValid = false
		o.unknown("sequence_overflow")
	} else {
		o.sequence++
	}
	if (o.delta > 0 && o.count > maxPendingObservationCount-o.delta) ||
		(o.delta < 0 && o.delta < -o.count) {
		o.countValid = false
		o.unknown("count_overflow_or_underflow")
		return
	}
	o.count += o.delta
	boundary := domain.PendingQueueTimeBounds{Earliest: o.begin, Latest: end}
	if o.delta < 0 {
		if o.runHasIncrease {
			o.hasGrowingRun = true
			o.longest = extendPendingRun(o.longest, o.runStart, boundary)
		}
		o.runStart, o.runStartCount, o.runHasIncrease = boundary, o.count, false
	} else if o.delta > 0 {
		o.runHasIncrease = true
	}
	// delta==0 is one atomic unchanged-total commit: pretrim+append and
	// in-place payload replacement neither reset the run nor claim growth.
}

// PendingQueueObservation takes an atomic end boundary and validates the slot
// count against the actual visible queues. Existing AggregateStatus retains its
// statusMu-only read path. This method does not register any RPC or collector.
func (s *Service) PendingQueueObservation() domain.PendingQueueObservationSnapshot {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	o := s.pendingObservation
	if o == nil {
		return domain.PendingQueueObservationSnapshot{UnknownReason: "source_unavailable"}
	}
	lo, _ := o.read()
	var actual int64
	for _, frames := range s.pending {
		n := int64(len(frames))
		if actual > maxPendingObservationCount-n {
			o.countValid = false
			o.unknown("count_overflow")
			break
		}
		actual += n
	}
	hi, _ := o.read()
	if o.open {
		o.countValid = false
		o.unknown("unclosed_owner_transaction")
	}
	if actual != o.count {
		o.countValid = false
		o.unknown("actual_count_mismatch")
	}
	end := domain.PendingQueueTimeBounds{Earliest: lo, Latest: hi}
	longest, growing := o.longest, o.hasGrowingRun
	if o.runHasIncrease {
		growing = true
		longest = extendPendingRun(longest, o.runStart, end)
	}
	return domain.PendingQueueObservationSnapshot{
		Available: true, SchemaVersion: 1, SourceID: o.id,
		Sequence: o.sequence, Count: o.count, CountValid: o.countValid,
		CoverageValid: o.countValid && o.durationValid && o.reason == "",
		DurationValid: o.durationValid, UnknownReason: o.reason,
		Baseline: o.baseline, End: end, RunStart: o.runStart,
		RunStartCount: o.runStartCount, RunHasIncrease: o.runHasIncrease,
		HasGrowingRun: growing, LongestGrowingRun: longest,
	}
}

package node

import (
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

// Owner/publisher are immutable before callbacks. installed/coverageStart are
// initialized exactly once under owner.mu+W; every reader holds owner.mu.
type ownedDegreeStatisticProducer struct {
	owner         *degreeOwner
	publisher     findnodebridge.Publisher
	coverageStart findnodebridge.OwnedInstant
	installed     bool
}

type ownedDegreeProjectionState uint8

const (
	ownedDegreeUnavailable ownedDegreeProjectionState = iota
	ownedDegreeFault
	ownedDegreeComplete
)

// The builder calls this after ALL application waits. No later constructor
// application wait follows the actual SampleRuntimePair installation point.
func (s *Service) installOwnedDegreeStatistic() bool {
	p := s.ownedDegreeStatisticProducer
	if p == nil || p.owner == nil || p.owner != s.degree {
		return false
	}
	o := p.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.installed || o.poisoned || p.publisher == (findnodebridge.Publisher{}) || p.publisher != o.bridgePublisher {
		return false
	}
	publication := p.publisher.BeginClockedPublication()
	point, _, err := publication.SampleRuntimePair()
	valid := err == nil && point.Elapsed() >= o.last
	if valid {
		o.last = point.Elapsed()
		p.coverageStart = point
		p.installed = true
	} else {
		o.poisoned = true
	}
	publication.Complete()
	return valid
}

func (s *Service) captureOwnedOverlayDWinForFlush() (overlayDegreeStatistic, error) {
	p := s.ownedDegreeStatisticProducer
	if p == nil || p.owner == nil || p.owner != s.degree {
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	o := p.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if !p.installed || o.poisoned || p.publisher == (findnodebridge.Publisher{}) || p.publisher != o.bridgePublisher {
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	publication := p.publisher.BeginClockedPublication()
	point, wall, err := publication.SampleRuntimePair()
	if err != nil || point.Elapsed() < o.last {
		o.poisoned = true
		publication.Complete()
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	o.last = point.Elapsed()
	if !point.NotBefore(p.coverageStart) {
		o.poisoned = true
		publication.Complete()
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	if !degreeStatisticCoverageReady(p.coverageStart.Elapsed(), point.Elapsed()) || !validDegreeStatisticWall(wall) {
		publication.Complete()
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	_, _, window, state := projectOwnedDegreeStatisticLocked(o, point.Elapsed())
	if state != ownedDegreeComplete {
		if state == ownedDegreeFault {
			o.poisoned = true
		}
		publication.Complete()
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	result := overlayDegreeStatistic{value: uint8(window), at: wall, valid: true}
	publication.Complete()
	return result, nil
}

// Statistics wrapper over the shared pure fixed-workspace basis. Both actual
// callers own degree.mu at a guarded same-domain q; their scope publishes any
// fault. This wrapper and the basis write only local arrays/counters.
func projectOwnedDegreeStatisticLocked(o *degreeOwner, q time.Duration) (int, int, int, ownedDegreeProjectionState) {
	rows, used, state := projectOwnedDegreeBasisLocked(o, q)
	if state != ownedDegreeComplete {
		return 0, 0, 0, state
	}
	outgoing, incoming := 0, 0
	for i := 0; i < used; i++ {
		row := rows[i]
		if q-row.last >= degreeRecent {
			continue
		}
		if row.key.outgoing {
			outgoing++
		} else {
			incoming++
		}
	}
	return outgoing, incoming, outgoing + incoming, ownedDegreeComplete
}

// Both callers own the exact degree mutex. Pure fixed64/65 workspace only;
// no history/owner mutation, new clock, provider or callback is introduced.
func projectOwnedDegreeBasisLocked(o *degreeOwner, q time.Duration) ([degreeHistoryLimit]degreeHistoryRow, int, ownedDegreeProjectionState) {
	if o == nil || o.poisoned || o.unknown != 0 {
		return [degreeHistoryLimit]degreeHistoryRow{}, 0, ownedDegreeUnavailable
	}
	if o.bridgePublisher == (findnodebridge.Publisher{}) || q < 0 || q == time.Duration(math.MaxInt64) || q < o.last || o.token == nil || o.profile.MaxActive <= 0 || len(o.active) != o.profile.MaxActive || o.historyN < 0 || o.historyN > degreeHistoryLimit {
		return [degreeHistoryLimit]degreeHistoryRow{}, 0, ownedDegreeFault
	}
	rows, used := o.history, o.historyN
	for i := 0; i < used; i++ {
		if !validDegreeStatisticKey(rows[i].key) || rows[i].last < 0 || rows[i].last > q {
			return [degreeHistoryLimit]degreeHistoryRow{}, 0, ownedDegreeFault
		}
		group := 0
		for j := 0; j < i; j++ {
			if rows[j].key == rows[i].key {
				return [degreeHistoryLimit]degreeHistoryRow{}, 0, ownedDegreeFault
			}
			if !rows[i].key.outgoing && !rows[j].key.outgoing && rows[j].key.group == rows[i].key.group {
				group++
			}
		}
		if group >= 2 {
			return [degreeHistoryLimit]degreeHistoryRow{}, 0, ownedDegreeFault
		}
	}
	for i := range o.active {
		row := &o.active[i]
		if row.life == nil {
			if row.witness != nil {
				return [degreeHistoryLimit]degreeHistoryRow{}, 0, ownedDegreeFault
			}
			continue
		}
		w := row.witness
		if w == nil || w.origin != row.life || w.owner != o || w.closed || w.untracked || w.slot != i || w.generation == 0 || w.generation != row.generation || row.life.token != o.token || !validDegreeStatisticKey(w.key) {
			return [degreeHistoryLimit]degreeHistoryRow{}, 0, ownedDegreeFault
		}
		insertDegreeStatisticKey(&rows, &used, w.key, q)
	}
	return rows, used, ownedDegreeComplete
}

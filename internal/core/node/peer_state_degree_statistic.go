package node

import (
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Immutable, constructor-only statistical binding. No admission authority,
// previous-run keys, callback, worker or late attachment port is retained.
type degreeStatisticProducer struct {
	owner         *degreeOwner
	coverageStart time.Duration
}

// Unused statistics-only root. The called degree root fixes the concrete
// production datagram constructor; no builder or elapsed clock escapes here.
// Its ordinary construction graph opens no session before returning. Owner
// installation happens first; construction time is excluded from coverage.
func newDegreeStatisticCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile degreeProfile) (*Service, error) {
	s, err := newDegreeService(cfg, id, bus, profile, nil)
	if err != nil {
		return s, err
	}
	o := s.degree
	if o == nil || o.token == nil {
		return s, errDegreeUnavailable
	}
	o.mu.Lock()
	start, _, valid := sampleDegreeStatisticClockLocked(o)
	if valid {
		s.degreeStatisticProducer = &degreeStatisticProducer{owner: o, coverageStart: start}
	}
	o.mu.Unlock()
	if !valid {
		return s, errDegreeUnavailable
	}
	return s, nil
}

// Requires the exact owner's mutex. This is the only statistic clock source:
// ONE original monotonic+wall sample after lock acquisition, never a callback
// or a caller Time. Record HWM BEFORE coverage/unknown/projection refusal.
func sampleDegreeStatisticClockLocked(o *degreeOwner) (time.Duration, time.Time, bool) {
	if o == nil || o.poisoned || o.token == nil || o.clock.fixture != nil || o.clock.origin.IsZero() {
		return 0, time.Time{}, false
	}
	sample := time.Now()
	q := sample.Sub(o.clock.origin)
	if q < 0 || q == time.Duration(math.MaxInt64) || q < o.last {
		o.poisoned = true
		return 0, time.Time{}, false
	}
	o.last = q
	wall := sample.UTC()
	if !validDegreeStatisticWall(wall) {
		return 0, time.Time{}, false
	}
	return q, wall, true
}

func validDegreeStatisticWall(at time.Time) bool {
	return !at.IsZero() && at.Year() >= 0 && at.Year() <= 9999
}

// Pure arithmetic, not a runtime clock or an elapsed-time fixture port.
func degreeStatisticCoverageReady(start, q time.Duration) bool {
	return start >= 0 && q >= start && q < time.Duration(math.MaxInt64) && q-start >= degreeRecent
}

// Optional observation only. The producer's immutable owner must still be
// the Service owner. No common lock, provider, peer/IP/intake lock or IO here.
func (s *Service) captureOverlayDWinForFlush() (overlayDegreeStatistic, error) {
	if s == nil || s.degreeStatisticProducer == nil {
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	p := s.degreeStatisticProducer
	o := p.owner
	if o == nil || o != s.degree {
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	q, wall, valid := sampleDegreeStatisticClockLocked(o)
	if !valid || !degreeStatisticCoverageReady(p.coverageStart, q) {
		return overlayDegreeStatistic{}, errDegreeUnavailable
	}
	_, _, window, err := projectDegreeStatisticLocked(o, q)
	if err != nil {
		return overlayDegreeStatistic{}, err
	}
	return overlayDegreeStatistic{value: uint8(window), at: wall, valid: true}, nil
}

func validDegreeStatisticKey(key degreeKey) bool {
	return !key.identity.IsZero() && (key.outgoing && key.group == "" || !key.outgoing && key.group != "")
}

// Pure fixed64+65 newest-first basis update. This edits local workspace only;
// it never mutates degreeOwner.history/historyN, calls degreeSnapshot, invents
// keys from the loaded scalar, or returns a truncated active enumeration.
func insertDegreeStatisticKey(rows *[degreeHistoryLimit]degreeHistoryRow, used *int, key degreeKey, at time.Duration) {
	var trial [degreeHistoryLimit + 1]degreeHistoryRow
	n := 0
	for i := 0; i < *used; i++ {
		row := rows[i]
		if at-row.last >= degreeRecent || row.key == key {
			continue
		}
		trial[n] = row
		n++
	}
	trial[n] = degreeHistoryRow{key: key, last: at}
	n++
	for i := 1; i < n; i++ {
		row := trial[i]
		j := i
		for j > 0 && (row.last > trial[j-1].last || row.last == trial[j-1].last && degreeKeyLess(row.key, trial[j-1].key)) {
			trial[j] = trial[j-1]
			j--
		}
		trial[j] = row
	}
	*rows = [degreeHistoryLimit]degreeHistoryRow{}
	count := 0
	for i := 0; i < n && count < degreeHistoryLimit; i++ {
		row := trial[i]
		if !row.key.outgoing {
			group := 0
			for j := 0; j < count; j++ {
				if !rows[j].key.outgoing && rows[j].key.group == row.key.group {
					group++
				}
			}
			if group >= 2 {
				continue
			}
		}
		rows[count] = row
		count++
	}
	*used = count
}

// Caller holds o.mu at its actual sampled q. All witness fields examined here
// are immutable or owned by degree.mu; common evidence/ended/peer are not read.
// Unknown coverage never becomes zero. Invalid stored state poisons/refuses.
func projectDegreeStatisticLocked(o *degreeOwner, q time.Duration) (int, int, int, error) {
	if o == nil || o.poisoned || q < 0 || q == time.Duration(math.MaxInt64) || q < o.last || o.unknown != 0 {
		return 0, 0, 0, errDegreeUnavailable
	}
	if o.token == nil || o.profile.MaxActive <= 0 || len(o.active) != o.profile.MaxActive || o.historyN < 0 || o.historyN > degreeHistoryLimit {
		o.poisoned = true
		return 0, 0, 0, errDegreeUnavailable
	}
	rows, used := o.history, o.historyN
	for i := 0; i < used; i++ {
		if !validDegreeStatisticKey(rows[i].key) || rows[i].last < 0 || rows[i].last > q {
			o.poisoned = true
			return 0, 0, 0, errDegreeUnavailable
		}
		group := 0
		for j := 0; j < i; j++ {
			if rows[j].key == rows[i].key {
				o.poisoned = true
				return 0, 0, 0, errDegreeUnavailable
			}
			if !rows[i].key.outgoing && !rows[j].key.outgoing && rows[j].key.group == rows[i].key.group {
				group++
			}
		}
		if group >= 2 {
			o.poisoned = true
			return 0, 0, 0, errDegreeUnavailable
		}
	}
	for i := range o.active {
		row := &o.active[i]
		if row.life == nil {
			if row.witness != nil {
				o.poisoned = true
				return 0, 0, 0, errDegreeUnavailable
			}
			continue
		}
		w := row.witness
		if w == nil || w.owner != o || w.closed || w.untracked || w.slot != i || w.generation == 0 || w.generation != row.generation || row.life.token != o.token || !validDegreeStatisticKey(w.key) {
			o.poisoned = true
			return 0, 0, 0, errDegreeUnavailable
		}
		insertDegreeStatisticKey(&rows, &used, w.key, q)
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
	return outgoing, incoming, outgoing + incoming, nil
}

// Selection is wall validation only; it never refreshes an observation's at.
// A complete genuine zero replaces a nonzero prior; unavailable keeps prior.
func selectOverlayDegreeStatistic(prior, candidate overlayDegreeStatistic, now time.Time) overlayDegreeStatistic {
	if validDegreeStatisticWall(candidate.at) {
		if fresh := freshOverlayDegreeStatistic(candidate, now); fresh.valid {
			return fresh
		}
	}
	return prior
}

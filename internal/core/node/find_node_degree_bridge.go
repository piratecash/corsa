package node

import (
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

type bridgeDegreeProjection struct {
	at                         findnodebridge.OwnedInstant
	outgoing, incoming, window int
	degreeValidBefore          findnodebridge.OwnedBoundary
}

// Last Node-side blocking capture: expected degree.mu, then the original
// ObservationClock. Fault publication occurs only after that clock is released.
func (s *Service) captureBridgeDegreeProjection() (bridgeDegreeProjection, error) {
	if s == nil || s.bridgeCapture == nil {
		return bridgeDegreeProjection{}, errDegreeUnavailable
	}
	c := s.bridgeCapture
	t := c.terminal
	if t == nil || t != s.bridgeTerminal || t.owner == nil || t.owner != s.degree || t.token == nil || t.token != s.proofToken || t.owner.token != t.token || t.owner.bridgePublisher != t.publisher {
		return bridgeDegreeProjection{}, errDegreeUnavailable
	}
	o := t.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.poisoned {
		return bridgeDegreeProjection{}, errDegreeUnavailable
	}
	clock, err := c.observer.ReserveClock()
	if err == nil {
		point, sampleErr := clock.Sample()
		if sampleErr == nil && point.Elapsed() >= o.last {
			o.last = point.Elapsed()
			rows, used, state := projectOwnedDegreeBasisLocked(o, point.Elapsed())
			clock.Release()
			if state == ownedDegreeUnavailable {
				return bridgeDegreeProjection{}, errDegreeUnavailable
			}
			if state == ownedDegreeComplete {
				outgoing, incoming := 0, 0
				for i := 0; i < used; i++ {
					row := rows[i]
					if point.Elapsed()-row.last >= degreeRecent {
						continue
					}
					if row.key.outgoing {
						outgoing++
					} else {
						incoming++
					}
				}
				bound, err := ownedDegreeProjectionFrontier(point, rows, used)
				if err != nil || !bound.Contains(point) {
					return bridgeDegreeProjection{}, errDegreeUnavailable
				}
				return bridgeDegreeProjection{at: point, outgoing: outgoing, incoming: incoming, window: outgoing + incoming, degreeValidBefore: bound}, nil
			}
		} else {
			clock.Release()
		}
	}
	// No observation clock survives to W; never choose an owner from a witness.
	publication := t.publisher.BeginPublication()
	o.poisoned = true
	publication.Complete()
	return bridgeDegreeProjection{}, errDegreeUnavailable
}

// Source-derived exclusive frontier. MAX is the leaf's exclusive domain end,
// never an observed instant or a clamped sample. All workspace is copied.
func ownedDegreeProjectionFrontier(at findnodebridge.OwnedInstant, rows [degreeHistoryLimit]degreeHistoryRow, used int) (findnodebridge.OwnedBoundary, error) {
	q := at.Elapsed()
	if q < 0 || q == time.Duration(math.MaxInt64) || used < 0 || used > degreeHistoryLimit {
		return findnodebridge.OwnedBoundary{}, errDegreeUnavailable
	}
	delta := time.Duration(math.MaxInt64) - q
	for i := 0; i < used; i++ {
		row := rows[i]
		if row.last < 0 || row.last > q {
			return findnodebridge.OwnedBoundary{}, errDegreeUnavailable
		}
		age := q - row.last
		if age >= degreeRecent {
			continue
		}
		remaining := degreeRecent - age
		if remaining < delta {
			delta = remaining
		}
	}
	return at.BoundaryAfter(delta)
}

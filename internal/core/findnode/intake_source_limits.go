package findnode

import (
	"math"

	"github.com/piratecash/corsa/internal/core/domain"
)

const candidateSourceTargets = 128

// Nonzero size is essential: separately allocated tokens have distinct identity.
// The immutable token does not retain its coordinator or a transport.
type candidateSourceOwnerToken struct{ _ byte }
type candidateSourceWindows struct {
	owner *candidateSourceOwnerToken
	rows  [candidateSourceTargets]candidateSourceTarget
}
type candidateSourceTarget struct {
	target            domain.PeerIdentity
	projected, actual candidatePairHistory
	expires           Instant
	generation        uint64
	refs              int
}
type candidateSourceRef struct {
	windows    *candidateSourceWindows
	index      uint8
	generation uint64
}
type candidateSourcePlan struct {
	windows *candidateSourceWindows
	index   uint8
	row     candidateSourceTarget
	floor   Instant
	fresh   bool
}

// prepareSources takes only a transient snapshot under the existing locks.
// First companions are allocated after releasing those locks; nothing is bound
// until all joint trials succeed. A racing new provisional may safely refuse.
func (o *intakeCoordinator) prepareSources() map[Session]*candidateSourceWindows {
	o.mu.Lock()
	o.pending.mu.Lock()
	sources := make([]Session, 0, len(o.pending.bySource))
	for _, entry := range o.pending.bySource {
		if entry.intake.phase == candidateProvisional && entry.source.state.sourceWindows.Load() == nil {
			sources = append(sources, entry.source)
		}
	}
	o.pending.mu.Unlock()
	o.mu.Unlock()
	prepared := make(map[Session]*candidateSourceWindows, len(sources))
	for _, source := range sources {
		prepared[source] = &candidateSourceWindows{owner: o.sourceOwner}
	}
	return prepared
}

// planSourceLocked reads mutable rows only after checking immutable ownership.
// Pending aliases are existing rows, not a second queue or completed registry.
func (o *intakeCoordinator) planSourceLocked(entry *pendingEntry, now Instant, prepared *candidateSourceWindows) (candidateSourcePlan, error) {
	windows := entry.source.state.sourceWindows.Load()
	fresh := windows == nil
	if fresh {
		windows = prepared
	}
	if windows == nil || windows.owner != o.sourceOwner {
		return candidateSourcePlan{}, ErrStaleWork
	}
	index := -1
	for i := range windows.rows {
		if windows.rows[i].target == entry.request.Target {
			index = i
			break
		}
	}
	if index < 0 {
		for i := range windows.rows {
			row := windows.rows[i]
			if row.target.IsZero() || (row.refs == 0 && now >= row.expires && row.projected.candidatePairExpired(now) && row.actual.candidatePairExpired(now)) {
				if row.generation != math.MaxUint64 {
					index = i
					break
				}
			}
		}
		if index < 0 {
			return candidateSourcePlan{}, ErrCapacity
		}
	}
	row := windows.rows[index]
	reusable := row.target.IsZero() || (row.refs == 0 && now >= row.expires && row.projected.candidatePairExpired(now) && row.actual.candidatePairExpired(now))
	if reusable {
		if row.generation == math.MaxUint64 {
			return candidateSourcePlan{}, ErrCapacity
		}
		row = candidateSourceTarget{target: entry.request.Target, generation: row.generation + 1}
	}
	if row.refs < 0 || row.refs == math.MaxInt {
		return candidateSourcePlan{}, ErrCapacity
	}
	_, projectedFloor, err := row.projected.candidatePairProject(now)
	if err != nil {
		return candidateSourcePlan{}, err
	}
	horizon, aliases := now, 0
	for _, pending := range o.pending.bySource {
		if pending.source == entry.source && pending.request.Target == entry.request.Target && pending.intake.alias != nil {
			existing, err := o.sourceRowLocked(pending)
			if err != nil || existing != &windows.rows[index] {
				return candidateSourcePlan{}, ErrStaleWork
			}
			if pending.intake.phase != candidateReserved || pending.intake.latest < now {
				return candidateSourcePlan{}, ErrInvalid
			}
			aliases++
			horizon = max(horizon, pending.intake.latest)
		}
	}
	actual := row.actual
	for range aliases {
		actual, _, err = actual.candidatePairProject(horizon)
		if err != nil {
			return candidateSourcePlan{}, err
		}
	}
	_, actualFloor, err := actual.candidatePairProject(horizon)
	if err != nil {
		return candidateSourcePlan{}, err
	}
	return candidateSourcePlan{windows: windows, index: uint8(index), row: row, floor: max(projectedFloor, actualFloor), fresh: fresh}, nil
}

// finalizeSourceLocked runs at the FINAL joint slot, before any shared effect.
func (p candidateSourcePlan) finalizeSourceLocked(at Instant) (candidateSourcePlan, error) {
	projected, err := p.row.projected.candidatePairTrial(at)
	if err != nil {
		return candidateSourcePlan{}, err
	}
	expiry, err := at.plus(candidateGroupWindow)
	if err != nil {
		return candidateSourcePlan{}, err
	}
	p.row.projected = projected
	p.row.expires = max(p.row.expires, expiry)
	p.row.refs++ // capacity was checked before this local trial assignment
	return p, nil
}

// sourceRowLocked requires owner and pending locks. It validates exact Session,
// target and generation before allowing a detached handle to touch row state.
func (o *intakeCoordinator) sourceRowLocked(entry *pendingEntry) (*candidateSourceTarget, error) {
	ref := entry.intake.source
	if ref.windows == nil || int(ref.index) >= len(ref.windows.rows) || ref.windows.owner != o.sourceOwner || entry.source.state.sourceWindows.Load() != ref.windows {
		return nil, ErrStaleWork
	}
	row := &ref.windows.rows[ref.index]
	if row.target != entry.request.Target || row.generation != ref.generation || row.refs <= 0 {
		return nil, ErrStaleWork
	}
	return row, nil
}

func (o *intakeCoordinator) releaseSourceLocked(entry *pendingEntry) {
	if row, err := o.sourceRowLocked(entry); err == nil {
		row.refs--
	}
	entry.intake.source = candidateSourceRef{}
}

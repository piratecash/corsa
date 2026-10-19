package findnode

import (
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const candidateNamedGroups = 256
const candidateGroupCells = candidateNamedGroups + 1
const candidateMaxRecords = 3840 + 16*candidateGroupCells
const candidateGroupWindow = Instant(10 * time.Minute)

type candidateRecordKey struct {
	cell   int
	target domain.PeerIdentity
}
type candidateGroupRecord struct {
	expires Instant
	refs    int
}
type candidateGroupPlan struct {
	group   domain.SourceGroup
	cell    int
	name    bool
	key     candidateRecordKey
	expires Instant
}

// candidateGroups belongs exclusively to intakeCoordinator.mu. Overflow never
// retains a physical group name. References are companions of existing rows.
type candidateGroups struct {
	named      map[domain.SourceGroup]int
	records    map[candidateRecordKey]candidateGroupRecord
	generation uint64
	lastUse    Instant
	used       bool
}

func newCandidateGroups() candidateGroups {
	return candidateGroups{named: make(map[domain.SourceGroup]int), records: make(map[candidateRecordKey]candidateGroupRecord), generation: 1}
}
func (g *candidateGroups) prospective(group domain.SourceGroup) int {
	if cell, ok := g.named[group]; ok {
		return cell
	}
	if len(g.named) < candidateNamedGroups {
		return len(g.named)
	}
	return candidateNamedGroups
}
func (g *candidateGroups) orderCell(group domain.SourceGroup) int {
	if cell, ok := g.named[group]; ok {
		return cell
	}
	return candidateNamedGroups
}
func (g *candidateGroups) plan(group domain.SourceGroup, target domain.PeerIdentity, slot Instant, cap int) (candidateGroupPlan, error) {
	if group == "" || target.IsZero() || slot < 0 || cap < 512 || cap > 3840 {
		return candidateGroupPlan{}, ErrInvalid
	}
	expiry, err := slot.plus(candidateGroupWindow)
	if err != nil {
		return candidateGroupPlan{}, err
	}
	cell := g.prospective(group)
	key := candidateRecordKey{cell, target}
	active := [candidateGroupCells]bool{}
	occupancy := 0
	for record := range g.records {
		active[record.cell] = true
		if record.cell == cell {
			occupancy++
		}
	}
	active[cell] = true
	k := 0
	for _, present := range active {
		if present {
			k++
		}
	}
	old, exists := g.records[key]
	if !exists && (occupancy >= max(16, cap/k) || len(g.records) >= cap+16*candidateGroupCells) {
		return candidateGroupPlan{}, ErrCapacity
	}
	_, named := g.named[group]
	return candidateGroupPlan{group: group, cell: cell, name: !named && cell < candidateNamedGroups, key: key, expires: max(expiry, old.expires)}, nil
}
func (g *candidateGroups) commit(plan candidateGroupPlan, now Instant) {
	if plan.name {
		g.named[plan.group] = plan.cell
	}
	record := g.records[plan.key]
	record.expires = plan.expires
	record.refs++
	g.records[plan.key] = record
	g.lastUse = now
	g.used = true
}
func (g *candidateGroups) release(key candidateRecordKey) {
	record := g.records[key]
	record.refs--
	g.records[key] = record
}
func (g *candidateGroups) finish(key candidateRecordKey, at Instant) error {
	expiry, err := at.plus(candidateGroupWindow)
	if err != nil {
		return err
	}
	record, ok := g.records[key]
	if !ok {
		return ErrStaleWork
	}
	record.expires = max(record.expires, expiry)
	g.records[key] = record
	g.lastUse = at
	g.used = true
	return nil
}
func (g *candidateGroups) expire(now Instant) {
	for key, record := range g.records {
		if record.refs == 0 && now >= record.expires {
			delete(g.records, key)
		}
	}
}
func (g *candidateGroups) reset(now Instant, rows int) bool {
	if rows != 0 || len(g.records) != 0 || g.generation == math.MaxUint64 {
		return false
	}
	if g.used && (now < g.lastUse || now-g.lastUse < candidateGroupWindow) {
		return false
	}
	clear(g.named)
	g.generation++
	g.used = false
	return true
}

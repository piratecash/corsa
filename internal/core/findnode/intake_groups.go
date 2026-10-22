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
type candidateGroupSlot struct {
	key    candidateRecordKey
	record candidateGroupRecord
}
type candidateGroups struct {
	named       [candidateNamedGroups]domain.SourceGroup
	namedCount  int
	records     [candidateMaxRecords]candidateGroupSlot
	recordCount int
	generation  uint64
	lastUse     Instant
	used        bool
}

func newCandidateGroups() candidateGroups { return candidateGroups{generation: 1} }
func (g *candidateGroups) prospective(group domain.SourceGroup) int {
	for cell := 0; cell < g.namedCount; cell++ {
		if g.named[cell] == group {
			return cell
		}
	}
	if g.namedCount < candidateNamedGroups {
		return g.namedCount
	}
	return candidateNamedGroups
}
func (g *candidateGroups) orderCell(group domain.SourceGroup) int {
	for cell := 0; cell < g.namedCount; cell++ {
		if g.named[cell] == group {
			return cell
		}
	}
	return candidateNamedGroups
}
func (g *candidateGroups) recordIndex(key candidateRecordKey) int {
	for index := 0; index < g.recordCount; index++ {
		if g.records[index].key == key {
			return index
		}
	}
	return -1
}
func (g *candidateGroups) record(key candidateRecordKey) (candidateGroupRecord, bool) {
	index := g.recordIndex(key)
	if index < 0 {
		return candidateGroupRecord{}, false
	}
	return g.records[index].record, true
}
func (g *candidateGroups) counts() (names, records int) { return g.namedCount, g.recordCount }
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
	for index := 0; index < g.recordCount; index++ {
		record := g.records[index].key
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
	old, exists := g.record(key)
	if !exists && (occupancy >= max(16, cap/k) || g.recordCount >= cap+16*candidateGroupCells) {
		return candidateGroupPlan{}, ErrCapacity
	}
	return candidateGroupPlan{group: group, cell: cell, name: cell == g.namedCount && cell < candidateNamedGroups, key: key, expires: max(expiry, old.expires)}, nil
}

// Valid plans are produced under the SAME uninterrupted coordinator ownership.
// Space/max-ref checks precede the original source CAS; this suffix cannot fail.
func (g *candidateGroups) commit(plan candidateGroupPlan, now Instant) {
	if plan.name {
		g.named[plan.cell] = plan.group
		g.namedCount++
	}
	index := g.recordIndex(plan.key)
	if index < 0 {
		index = g.recordCount
		g.records[index].key = plan.key
		g.recordCount++
	}
	g.records[index].record.expires = plan.expires
	g.records[index].record.refs++
	g.lastUse = now
	g.used = true
}
func (g *candidateGroups) release(key candidateRecordKey) {
	index := g.recordIndex(key)
	// A real caller owns a current reference; a corrupt missing key never aliases
	// another physical slot or creates the old map's unreachable ghost record.
	if index < 0 {
		return
	}
	g.records[index].record.refs--
}
func (g *candidateGroups) finish(key candidateRecordKey, at Instant) error {
	expiry, err := at.plus(candidateGroupWindow)
	if err != nil {
		return err
	}
	index := g.recordIndex(key)
	if index < 0 {
		return ErrStaleWork
	}
	g.records[index].record.expires = max(g.records[index].record.expires, expiry)
	g.lastUse = at
	g.used = true
	return nil
}
func (g *candidateGroups) expire(now Instant) {
	for index := 0; index < g.recordCount; {
		record := g.records[index].record
		if record.refs == 0 && now >= record.expires {
			last := g.recordCount - 1
			g.records[index] = g.records[last]
			g.records[last] = candidateGroupSlot{}
			g.recordCount--
			continue
		}
		index++
	}
}
func (g *candidateGroups) reset(now Instant, rows int) bool {
	if rows != 0 || g.recordCount != 0 || g.generation == math.MaxUint64 {
		return false
	}
	if g.used && (now < g.lastUse || now-g.lastUse < candidateGroupWindow) {
		return false
	}
	clear(g.named[:g.namedCount])
	g.namedCount = 0
	g.generation++
	g.used = false
	return true
}

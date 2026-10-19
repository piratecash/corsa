package node

import (
	"bytes"
	"errors"
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const (
	secureSessionJournalFormat       = 3
	maxInboundPins                   = 4096
	ninGroupCapacity                 = 4096
	ninEventsPerGroup                = 64
	ninWindow                        = uint64(24 * time.Hour)
	ninKnown                   uint8 = 1
	ninUnknown                 uint8 = 2
)

var errNinJournal = errors.New("inbound quota journal invalid or unavailable")
var errNinMetadataRequiresJournal = errors.New("inbound metadata requires paired journal admission")

type ninKey [5]byte

// Row is exactly520 bytes; neither pins nor handshake attempts own an event array.
type ninRow struct {
	key      ninKey
	count    uint8
	reserved [2]byte
	ticks    [ninEventsPerGroup]uint64
}
type ninStamp struct {
	domain     [16]byte
	generation uint64
	phase      uint8
	tick       uint64
}

func (a ninStamp) sameContent(b ninStamp) bool {
	return a.domain == b.domain && a.generation == b.generation && a.phase == b.phase
}

type ninJournalImage struct {
	domain           [16]byte
	generation, tick uint64
	phase            uint8
	rows             []ninRow
}

func (i *ninJournalImage) stamp() ninStamp {
	if i == nil {
		return ninStamp{}
	}
	return ninStamp{domain: i.domain, generation: i.generation, phase: i.phase, tick: i.tick}
}

// All fields belong to store.mu. The arena is lazy; empty Unknown stores do
// not allocate2MiB. No row pointer or mutable arena escapes its owner.
type ninJournal struct {
	domain             [16]byte
	generation, tick   uint64
	phase              uint8
	arena              *[ninGroupCapacity]ninRow
	rows               int
	anchored           bool
	last               time.Duration
	clockReason        domain.IdentityPinInboundQuotaReason
	written, confirmed ninStamp
	checkpointAttempt  *time.Duration
}

func (j *ninJournal) stamp() ninStamp {
	return ninStamp{domain: j.domain, generation: j.generation, phase: j.phase, tick: j.tick}
}
func (j *ninJournal) pending() bool {
	return !j.stamp().sameContent(j.written) || j.tick > j.written.tick
}
func (j *ninJournal) strictConfirmed() bool { return j.stamp().sameContent(j.confirmed) }
func (j *ninJournal) row(index int) ninRow {
	if j.arena == nil {
		return ninRow{}
	}
	return j.arena[index]
}
func pruneNinRow(row ninRow, now uint64) ninRow {
	if now < ninWindow {
		return row
	}
	n := 0
	for i := 0; i < int(row.count); i++ {
		if row.ticks[i] > now-ninWindow {
			row.ticks[n] = row.ticks[i]
			n++
		}
	}
	for i := n; i < ninEventsPerGroup; i++ {
		row.ticks[i] = 0
	}
	row.count = uint8(n)
	return row
}
func (j *ninJournal) prune() {
	if j.arena == nil {
		return
	}
	n := 1
	j.arena[0] = pruneNinRow(j.arena[0], j.tick)
	for i := 1; i < j.rows; i++ {
		row := pruneNinRow(j.arena[i], j.tick)
		if row.count != 0 {
			j.arena[n] = row
			n++
		}
	}
	for i := n; i < j.rows; i++ {
		j.arena[i] = ninRow{}
	}
	j.rows = n
}
func (j *ninJournal) image() *ninJournalImage {
	j.prune()
	rows := make([]ninRow, j.rows)
	if j.arena != nil {
		copy(rows, j.arena[:j.rows])
	}
	return &ninJournalImage{domain: j.domain, generation: j.generation, tick: j.tick, phase: j.phase, rows: rows}
}
func ninJournalFromImage(image *ninJournalImage) *ninJournal {
	j := &ninJournal{domain: image.domain, generation: image.generation, tick: image.tick, phase: image.phase, rows: len(image.rows)}
	if len(image.rows) > 1 || image.rows[0].count != 0 {
		j.arena = new([ninGroupCapacity]ninRow)
		copy(j.arena[:], image.rows)
	}
	j.written = j.stamp()
	return j
}

// sampleNinQuotaLocked requires store.mu. The elapsed source is an immutable
// leaf (SinceBoot), never a wall clock or callback into Service/store.
func (s *secureSessionStore) sampleNinQuotaLocked() {
	j := s.ninJournal
	if j == nil {
		return
	}
	if j.clockReason == domain.InboundQuotaClockFault || j.clockReason == domain.InboundQuotaExhausted {
		return
	}
	if s.ninElapsed == nil {
		j.clockReason = domain.InboundQuotaClockUnavailable
		return
	}
	reading, err := s.ninElapsed()
	if err != nil {
		j.clockReason = domain.InboundQuotaClockUnavailable
		return
	}
	if reading < 0 || (j.anchored && reading < j.last) {
		j.clockReason = domain.InboundQuotaClockFault
		return
	}
	j.clockReason = domain.InboundQuotaReady
	if !j.anchored {
		j.last = reading
		j.anchored = true
		return
	}
	delta := uint64(reading - j.last)
	if delta > math.MaxUint64-j.tick {
		j.clockReason = domain.InboundQuotaExhausted
		return
	}
	next := j.tick + delta
	if j.phase == ninUnknown && next >= ninWindow {
		if j.generation == math.MaxUint64 || s.changes == math.MaxUint64 {
			j.clockReason = domain.InboundQuotaExhausted
			return
		}
		j.phase = ninKnown
		j.generation++
		s.changes++
	}
	j.tick = next
	j.last = reading
}

// ninProgressPendingLocked requires store.mu.
func (s *secureSessionStore) ninProgressPendingLocked() bool {
	return s.ninJournal != nil && s.ninJournal.pending()
}
func (s *secureSessionStore) ninCheckpointDueLocked() bool {
	j := s.ninJournal
	return j != nil && (j.checkpointAttempt == nil || j.last-*j.checkpointAttempt >= renewalFlushEvery)
}

// recordNinSnapshotWrittenLocked requires store.mu. Coverage is captured, never
// the live tick; a newer suffix/content state remains pending. Capture only this
// fixed stamp before I/O: retaining the full main DTO here would retain its large
// maps through the write/GC boundary. Ordinary writes
// do not grant strict authority or K2 completion.
func (s *secureSessionStore) recordNinSnapshotWrittenLocked(captured ninStamp, strict bool) {
	j := s.ninJournal
	if j == nil || captured.domain == ([16]byte{}) {
		return
	}
	if !j.stamp().sameContent(captured) {
		return
	}
	if !j.written.sameContent(captured) || captured.tick > j.written.tick {
		j.written = captured
	}
	if strict {
		j.confirmed = captured
	}
}
func (s *secureSessionStore) inboundAllocationsLocked() (int, error) {
	n := s.inboundAllocations
	if s.pinRecovery != nil && s.pinRecovery.permit != nil {
		b := s.pinRecovery.permit.Binding
		if _, present := s.identities[b.Identity]; !present {
			record, err := b.Backup.Restore(b.Identity, b.PreviousPinnedAt)
			if err != nil {
				return n, err
			}
			if record.Metadata.Inbound.State != domain.IdentityPinAllocationAbsent {
				n++
			}
		}
	}
	return n, nil
}

// inboundQuotaStatus returns fixed-size values only; there is no public producer.
func (s *secureSessionStore) inboundQuotaStatus(group domain.SourceGroup) domain.IdentityPinInboundQuotaStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sampleNinQuotaLocked()
	status := domain.IdentityPinInboundQuotaStatus{Reason: domain.InboundQuotaNotInstalled}
	if s.unreadable || (s.pinRecovery != nil && s.pinRecovery.fatal) {
		status.Reason = domain.InboundQuotaUnavailable
		return status
	}
	n, err := s.inboundAllocationsLocked()
	status.Allocations = n
	status.ConfirmedAllocations = s.inboundConfirmed
	status.OverCapacity = n > maxInboundPins || err != nil
	j := s.ninJournal
	if j == nil {
		return status
	}
	j.prune()
	status.Groups = j.rows
	for i := 0; i < j.rows; i++ {
		status.Events += int(pruneNinRow(j.row(i), j.tick).count)
	}
	status.Reason = j.reason()
	if j.clockReason == domain.InboundQuotaReady && j.phase == ninUnknown && j.tick < ninWindow {
		remaining := time.Duration(ninWindow - j.tick)
		status.RemainingObserved = &remaining
	}
	if err != nil {
		status.Reason = domain.InboundQuotaUnavailable
		status.RemainingObserved = nil
		return status
	}
	if n >= maxInboundPins {
		status.Reason = domain.InboundQuotaClassFull
		status.RemainingObserved = nil
	}
	if status.Reason != domain.InboundQuotaReady {
		return status
	}
	key, keyErr := ninKeyOf(group)
	if keyErr != nil {
		status.Reason = domain.InboundQuotaInvalidGroup
		return status
	}
	index, found := j.find(key)
	if found {
		row := pruneNinRow(j.row(index), j.tick)
		if row.count == ninEventsPerGroup {
			status.Reason = domain.InboundQuotaGroupFull
			remaining := time.Duration(ninWindow - (j.tick - row.ticks[0]))
			status.RemainingObserved = &remaining
		}
	} else if j.rows == ninGroupCapacity {
		status.Reason = domain.InboundQuotaHistoryFull
		// A row is reusable only after its LAST retained event expires.
		var earliest uint64 = math.MaxUint64
		for i := 1; i < j.rows; i++ {
			row := j.row(i)
			age := j.tick - row.ticks[row.count-1]
			remaining := ninWindow - age
			if remaining < earliest {
				earliest = remaining
			}
		}
		remaining := time.Duration(earliest)
		status.RemainingObserved = &remaining
	}
	return status
}
func (j *ninJournal) reason() domain.IdentityPinInboundQuotaReason {
	if j.clockReason != domain.InboundQuotaReady {
		return j.clockReason
	}
	if j.phase == ninUnknown {
		return domain.InboundQuotaHistoryUnknown
	}
	if !j.strictConfirmed() {
		return domain.InboundQuotaUnconfirmed
	}
	return domain.InboundQuotaReady
}
func (j *ninJournal) find(key ninKey) (int, bool) {
	lo, hi := 0, j.rows
	for lo < hi {
		mid := (lo + hi) / 2
		row := j.row(mid)
		if bytes.Compare(row.key[:], key[:]) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, lo < j.rows && j.row(lo).key == key
}

func (e identityPinEntry) inboundMetadataConfirmed() bool {
	return e.protectionRevision != 0 && e.metadata != nil && e.classMetadata().Inbound.State == domain.IdentityPinAllocationEligible && e.metadata.confirmedRevision == e.protectionRevision
}

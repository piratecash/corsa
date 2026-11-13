package node

import (
	"crypto/rand"
	"sort"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// MaxWriterQueueHistory bounds retained registered writers for one diagnostic
// service lifetime, including retired writers. Exhaustion invalidates coverage.
const MaxWriterQueueHistory = 256

type WriterQueueLifecycleRow struct {
	ConnID    domain.ConnID
	Direction domain.PeerDirection
	BirthAt   time.Duration
	Retired   bool
	Queue     netcore.WriterQueueSnapshot
}

// WriterQueueLifecycleSnapshot covers the registered NetCore population.
// Each queue has its own cut; membership is held stable while reading them.
type WriterQueueLifecycleSnapshot struct {
	Instance      [16]byte
	Registered    int
	Rows          []WriterQueueLifecycleRow
	UnknownReason string
}

type writerQueueBirth struct {
	instance [16]byte
	at       time.Duration
}

// All mutable state is owned by Service.peerMu. Retired rows retain values
// only; they cannot pin an underlying writer, buffer or socket.
type writerQueueLifecycle struct {
	instance   [16]byte
	capacity   int
	registered int
	retired    []WriterQueueLifecycleRow
	reason     string
}

func newWriterQueueLifecycle(capacity int) *writerQueueLifecycle {
	o := &writerQueueLifecycle{capacity: capacity}
	if _, err := rand.Read(o.instance[:]); err != nil {
		o.fail("source_identity_unavailable")
	}
	return o
}

func (o *writerQueueLifecycle) fail(reason string) {
	if o != nil && o.reason == "" {
		o.reason = reason
	}
}

func (s *Service) recordWriterQueueBirthLocked(entry *connEntry, registryCollision bool) {
	o := s.writerQueueLifecycle
	if o == nil || o.reason != "" {
		return
	}
	if registryCollision || entry.core.ConnID() == 0 {
		o.fail("registry_identity_collision")
		return
	}
	if o.registered == o.capacity {
		o.fail("history_capacity_exhausted")
		return
	}
	cut := entry.core.WriterQueueObservation()
	if cut.UnknownReason != "" || cut.Instance == ([16]byte{}) || cut.Depth != 0 || cut.Enqueued != 0 || cut.WriterExited {
		o.fail("invalid_writer_birth")
		return
	}
	entry.writerQueueBirth = &writerQueueBirth{instance: cut.Instance, at: cut.At}
	o.registered++
}

func writerQueueLifecycleRow(id domain.ConnID, entry *connEntry) (WriterQueueLifecycleRow, bool) {
	if entry == nil || entry.core == nil || entry.writerQueueBirth == nil || entry.core.ConnID() != id {
		return WriterQueueLifecycleRow{}, false
	}
	cut := entry.core.WriterQueueObservation()
	birth := entry.writerQueueBirth
	if cut.UnknownReason != "" || cut.Instance != birth.instance || cut.At < birth.at {
		return WriterQueueLifecycleRow{}, false
	}
	return WriterQueueLifecycleRow{ConnID: id, Direction: coreToPeerDirection(entry.core.Dir()), BirthAt: birth.at, Queue: cut}, true
}

func (s *Service) recordWriterQueueRetirementLocked(id domain.ConnID, entry *connEntry) {
	o := s.writerQueueLifecycle
	if o == nil || o.reason != "" {
		return
	}
	row, ok := writerQueueLifecycleRow(id, entry)
	if !ok || !row.Queue.WriterExited || row.Queue.Depth != 0 || len(o.retired) >= o.registered {
		o.fail("writer_final_cut_unavailable")
		return
	}
	row.Retired = true
	o.retired = append(o.retired, row)
}

func (s *Service) writerQueueLifecycleSnapshot() WriterQueueLifecycleSnapshot {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	o := s.writerQueueLifecycle
	if o == nil {
		return WriterQueueLifecycleSnapshot{UnknownReason: "source_unavailable"}
	}
	result := WriterQueueLifecycleSnapshot{Instance: o.instance, Registered: o.registered}
	if o.reason == "" && len(s.conns)+len(o.retired) != o.registered {
		o.fail("registry_coverage_mismatch")
	}
	if o.reason == "" {
		result.Rows = make([]WriterQueueLifecycleRow, 0, o.registered)
		result.Rows = append(result.Rows, o.retired...)
		for id, entry := range s.conns {
			row, ok := writerQueueLifecycleRow(id, entry)
			if !ok {
				o.fail("writer_cut_unavailable")
				break
			}
			result.Rows = append(result.Rows, row)
		}
	}
	result.UnknownReason = o.reason
	if result.UnknownReason != "" {
		result.Rows = nil
		return result
	}
	sort.Slice(result.Rows, func(i, j int) bool { return result.Rows[i].ConnID < result.Rows[j].ConnID })
	for i := 1; i < len(result.Rows); i++ {
		if result.Rows[i-1].ConnID == result.Rows[i].ConnID {
			o.fail("registry_identity_reused")
			result.UnknownReason, result.Rows = o.reason, nil
			break
		}
	}
	return result
}

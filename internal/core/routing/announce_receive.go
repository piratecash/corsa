package routing

import "sync"

// AnnounceReceiveEvidence belongs to the actual authenticated connection. No
// identity-level lifecycle event can establish or clear another connection's
// evidence. AnnouncePeerState implements it only for explicit registry fixtures.
type AnnounceReceiveEvidence interface {
	ReceiveReady() bool
	MarkBaselineReceived()
	HasReceivedBaseline() bool
	ObserveV3Epoch(uint64) V3EpochVerdict
}

// AnnounceReceiveState is a bounded receive-only connection state. Its leaf
// mutex is never held during callbacks, I/O, or acquisition of Service locks.
// The zero value is closed/uninitialised; only the constructor opens a state.
type AnnounceReceiveState struct {
	mu       sync.Mutex
	active   bool
	baseline bool
	epoch    uint64
	epochSet bool
}

func NewAnnounceReceiveState() *AnnounceReceiveState { return &AnnounceReceiveState{active: true} }
func (s *AnnounceReceiveState) ReceiveReady() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}
func (s *AnnounceReceiveState) MarkBaselineReceived() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active {
		s.baseline = true
	}
}
func (s *AnnounceReceiveState) HasReceivedBaseline() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active && s.baseline
}
func (s *AnnounceReceiveState) ObserveV3Epoch(incoming uint64) V3EpochVerdict {
	if s == nil {
		return V3EpochStale
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return V3EpochStale
	}
	return observeV3ReceiveEpoch(incoming, &s.epoch, &s.epochSet, &s.baseline)
}
func (s *AnnounceReceiveState) KnownV3Epoch() (uint64, bool) {
	if s == nil {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.epochSet
}
func (s *AnnounceReceiveState) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = false
	s.baseline = false
}

// Both registry compatibility state and connection-owned state use exactly
// the same transition. The caller holds its own mutex, no other lock is taken.
func observeV3ReceiveEpoch(incoming uint64, epoch *uint64, known *bool, baseline *bool) V3EpochVerdict {
	if !*known {
		*epoch = incoming
		*known = true
		return V3EpochApply
	}
	if incoming < *epoch {
		return V3EpochStale
	}
	if incoming > *epoch {
		*epoch = incoming
		*baseline = false
		return V3EpochReset
	}
	return V3EpochApply
}

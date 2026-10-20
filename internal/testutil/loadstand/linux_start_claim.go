package loadstand

import (
	"sync"
	"time"
)

type linuxStartPhase uint8

const (
	linuxStartPending linuxStartPhase = iota + 1
	linuxStartClaimed
	linuxStartPublished
	linuxStartDenied
	linuxStartFailed
)

type linuxStartObservation struct {
	phase    linuxStartPhase
	stopped  bool
	stopID   LinuxRequestID
	deadline time.Time
	defect   bool
}

// linuxStartClaim is one helper-local launch owner, not remote cancellation
// authority. Its mutex never spans Start, signals, Wait, preflight or I/O.
type linuxStartClaim struct {
	mu      sync.Mutex
	state   linuxStartObservation
	process *Process
}

func newLinuxStartClaim() *linuxStartClaim {
	return &linuxStartClaim{state: linuxStartObservation{phase: linuxStartPending}}
}
func (s *linuxStartClaim) claim() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.phase != linuxStartPending {
		return false
	}
	if s.state.stopped {
		s.state.phase = linuxStartDenied
		return false
	}
	s.state.phase = linuxStartClaimed
	return true
}
func (s *linuxStartClaim) observeStop(id LinuxRequestID, deadline time.Time, defect bool) {
	s.mu.Lock()
	if id != 0 && s.state.stopID == 0 {
		s.state.stopID = id
	} else if id != 0 && id != s.state.stopID {
		defect = true
	}
	s.state.stopped = true
	s.state.defect = s.state.defect || defect
	if s.state.deadline.IsZero() || deadline.Before(s.state.deadline) {
		s.state.deadline = deadline
	}
	process, stopAt := s.process, s.state.deadline
	s.mu.Unlock()
	if process != nil {
		process.terminal.submit(StopLifetime, false, stopAt, false)
	}
}
func (s *linuxStartClaim) publish(process *Process) error {
	s.mu.Lock()
	if process == nil || s.state.phase != linuxStartClaimed {
		s.mu.Unlock()
		return ErrLinuxOwnerProtocol
	}
	s.process = process
	s.state.phase = linuxStartPublished
	stopped, stopAt := s.state.stopped, s.state.deadline
	s.mu.Unlock()
	if stopped {
		process.terminal.submit(StopLifetime, false, stopAt, false)
	}
	return nil
}
func (s *linuxStartClaim) failed() {
	s.mu.Lock()
	if s.state.phase == linuxStartClaimed {
		s.state.phase = linuxStartFailed
	}
	s.mu.Unlock()
}
func (s *linuxStartClaim) snapshot() linuxStartObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

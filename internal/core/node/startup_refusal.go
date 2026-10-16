package node

import "fmt"

// StartupRefusal is why this node must not start, or nil: a connection
// budget that cannot be honoured, or pins the node could not keep or enforce
// — an unsafe, lost or unreadable pin store, a secure session that cannot be
// built, a v1-only build over a pin store (*PinStoreRefusalError, errors.Is
// domain.ErrPinStoreRefused). All are found by NewService and change no more
// after it.
//
// Run returns it before it opens the node's listener. The process starting
// the node asks it as well, before listeners of its own — the RPC server,
// pprof — so a node that is not going to run is never reachable at all.
//
// The pin store refusal names paths, for the operator on this machine; it is
// returned to the process that starts the node and is never part of an RPC
// answer.
func (s *Service) StartupRefusal() error {
	// A budget that cannot be honoured stops the node. Starting with a
	// silently disabled ceiling would be the worst of the two outcomes: the
	// operator asked for a limit, and the node would run without one.
	if s.connBudgetErr != nil {
		return fmt.Errorf("connection budget: %w", s.connBudgetErr)
	}
	if s.secureSessions == nil {
		return nil
	}
	return s.secureSessions.startupRefusal()
}

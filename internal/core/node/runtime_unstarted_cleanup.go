package node

import (
	"context"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Normal Runtime construction, still zero-origin. The cleanup belongs to the
// constructor's ORIGINAL Service and is used only before Run has started.
// No Service method or RPC/wire operation exposes background admission control.
func NewServiceForRuntime(cfg config.Node, id *identity.Identity, bus *ebus.Bus) (*Service, context.CancelFunc) {
	s := NewService(cfg, id, bus)
	return s, newUnstartedRuntimeCleanup(s, s.runCancel)
}

// No new lifecycle/gate: close the existing original admission before a caller
// waits and releases stores. Never invoke while Run is executing; Run must
// retain admission for teardown continuations until its own LAST defer.
func newUnstartedRuntimeCleanup(s *Service, originalCancel context.CancelFunc) context.CancelFunc {
	return func() {
		originalCancel()
		s.closeBackgroundAdmission()
	}
}

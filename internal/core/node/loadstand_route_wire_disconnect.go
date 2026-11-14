package node

import (
	"context"
	"time"

	"github.com/piratecash/corsa/internal/core/netcore"
)

// DisconnectLoadstandRouteWireSession closes the sole original registered
// test session. It is available only to an explicitly scoped FullNode with
// full receipts, and has no RPC entry. Selection consumes a one-shot permit;
// close outside peerMu because the ordinary teardown needs that lock.
func (s *Service) DisconnectLoadstandRouteWireSession(ctx context.Context, at time.Duration) error {
	core, err := s.selectLoadstandRouteWireDisconnect(ctx, at)
	if err != nil {
		return err
	}
	core.Close()
	return nil
}

func (s *Service) selectLoadstandRouteWireDisconnect(ctx context.Context, at time.Duration) (*netcore.NetCore, error) {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	source, obs := s.k3Registered, s.routeWireObserver()
	if source == nil || source.routeWireScope == ([32]byte{}) || source.fault != "" || source.loadstandDisconnected ||
		obs == nil || obs.window == nil || obs.fullBudget <= 0 || at <= obs.window.begin || at >= obs.window.windowEnd ||
		source.length != 1 || len(source.owners) != 1 || source.history[0].Clock.After >= obs.window.begin {
		return nil, errRouteWireIncomplete
	}
	now, ok := source.readClock()
	if !ok || now < at || now >= obs.window.windowEnd {
		return nil, errRouteWireIncomplete
	}
	for _, owner := range source.owners {
		core := owner.binding.core
		if core == nil || core.LoadstandSessionID() == ([32]byte{}) || !core.AcceptingFrames() {
			return nil, errRouteWireIncomplete
		}
		source.loadstandDisconnected = true
		return core, nil
	}
	return nil, errRouteWireIncomplete
}

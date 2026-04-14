package node

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// network_bridge.go introduces the networkBridge adapter — the mapping
// from the netcore.Network interface to the *Service surface. The name is
// intentionally unexported: external packages receive only the Network
// interface value returned from Service.Network().
//
// After PR 10.6 the transitional net.Conn-first working call sites that
// motivated the bridge have been migrated to ConnID-first signatures
// (writeJSONFrameByID, enqueueFrameByID, dispatchNetworkFrame already
// ConnID-first via core lookup). The bridge now calls those ConnID-first
// methods directly. The only remaining net.Conn-first surface is the
// permanent carve-out in conn_registry.go (registerInboundConnLocked,
// attachOutboundCoreLocked, unregisterConnLocked, connEntryForLocked) plus
// the socket-infra helper enableTCPKeepAlive.
//
// The bridge itself holds no state beyond a back-reference to *Service.
// Every method acquires the necessary locks exactly as the equivalent
// direct call site would, using the same -Locked helpers from
// conn_registry.go (coreForIDLocked, forEachConnLocked). No extra
// synchronisation primitive is introduced.
//
// SendStatus-to-error translation is performed here via
// netcore.SendStatusToError — this is the single hop where the internal
// enum of *NetCore is converted into the public sentinel error contract
// of Network. Callers of the bridge never see SendStatus.

// networkBridge implements netcore.Network backed by *Service. It is an
// implementation detail — external packages receive only the Network
// interface value returned from Service.Network().
type networkBridge struct {
	svc *Service
}

// compile-time assertion: bridge satisfies the transport contract.
var _ netcore.Network = (*networkBridge)(nil)

// Network returns the transport-level API surface of the Service.
//
// Default path: the returned value is backed by a bridge adapter and shares
// no state with other callers — invoking Network() multiple times is cheap
// and safe; every call returns an independent bridge instance over the same
// Service. Consumers must not assume pointer identity across calls.
//
// Override path: when NewServiceWithNetwork has pinned a caller-supplied
// netcore.Network (see s.networkOverride), that value is returned verbatim
// on every call. This is the single injection seam used by tests — the
// override pointer is intentionally reused across calls so that test
// backends (internal/core/netcore/netcoretest) can observe Service frames
// as a single conversation. Pointer identity is still not a documented
// guarantee of the API and callers must not rely on it.
func (s *Service) Network() netcore.Network {
	if s.networkOverride != nil {
		return s.networkOverride
	}
	return &networkBridge{svc: s}
}

// coreForID resolves id to the owning *netcore.NetCore under s.mu.RLock.
// Returns nil if id is not registered. The method releases the lock
// before returning; callers operate on NetCore outside of s.mu, which is
// the existing convention across service.go send paths.
func (b *networkBridge) coreForID(id domain.ConnID) *netcore.NetCore {
	b.svc.mu.RLock()
	defer b.svc.mu.RUnlock()
	return b.svc.coreForIDLocked(id)
}

// SendFrame enqueues frame asynchronously on id's writer queue. Honours
// ctx: if ctx is already cancelled on entry, returns ctx.Err() without
// touching the transport (no partial-send race). Otherwise delegates to
// NetCore.SendRaw and maps the returned SendStatus to a public sentinel.
func (b *networkBridge) SendFrame(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	core := b.coreForID(id)
	if core == nil {
		return netcore.ErrUnknownConn
	}
	status := core.SendRaw(frame)
	return netcore.SendStatusToError(status)
}

// SendFrameSync enqueues frame and blocks until the writer flushes it.
// Honours ctx on entry identically to SendFrame. The underlying
// SendRawSync has its own internal deadline (syncFlushTimeout) which
// bounds the wait; ctx is checked up-front so a pre-cancelled request
// fails fast without consuming a queue slot.
func (b *networkBridge) SendFrameSync(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	core := b.coreForID(id)
	if core == nil {
		return netcore.ErrUnknownConn
	}
	status := core.SendRawSync(frame)
	return netcore.SendStatusToError(status)
}

// Enumerate iterates registered connections matching dir. If ctx is
// already cancelled on entry, no iteration starts. The walk acquires
// s.mu.RLock for the full duration of the iteration — matching the
// existing forEach*Locked convention — so fn must not call back into
// Service methods that take s.mu (deadlock). Callers that need to act
// on a ConnID outside the lock should collect ConnIDs into a local
// slice inside fn and process them after Enumerate returns.
func (b *networkBridge) Enumerate(ctx context.Context, dir netcore.Direction, fn func(domain.ConnID) bool) {
	if ctx.Err() != nil {
		return
	}
	b.svc.mu.RLock()
	defer b.svc.mu.RUnlock()
	b.svc.forEachConnLocked(func(id domain.ConnID, core *netcore.NetCore) bool {
		if core.Dir() != dir {
			return true
		}
		return fn(id)
	})
}

// Close triggers graceful shutdown of the connection by id. The actual
// teardown protocol lives on *NetCore.Close(): it closes the raw socket,
// closes sendCh and waits for the writer goroutine to drain. Registry
// eviction is a separate concern owned by the lifecycle helpers
// (unregisterConnLocked) and is not performed here — Close() signals
// the transport to stop, registry cleanup follows the existing teardown
// path on the owning Service method.
func (b *networkBridge) Close(ctx context.Context, id domain.ConnID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	core := b.coreForID(id)
	if core == nil {
		return netcore.ErrUnknownConn
	}
	core.Close()
	return nil
}

// RemoteAddr returns the peer address string for id, or "" if id is not
// registered. The lookup holds s.mu.RLock briefly and then reads the
// address from NetCore, whose own accessor is safe for concurrent use.
func (b *networkBridge) RemoteAddr(id domain.ConnID) string {
	core := b.coreForID(id)
	if core == nil {
		return ""
	}
	return core.RemoteAddr()
}

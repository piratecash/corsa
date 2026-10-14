package node

import (
	"context"
	"errors"

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
// attachOutboundCoreLocked, unregisterConnLocked) plus the socket-infra
// helper enableTCPKeepAlive.
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

// ErrNetworkOverrideOwnsNoConnections is the refusal a Service built by
// NewServiceWithNetwork answers whenever it is asked to own a real
// connection: to serve (Run), to register an accepted socket
// (registerInboundConn) or to attach a dialled one (attachOutboundNetCore).
var ErrNetworkOverrideOwnsNoConnections = errors.New("node: a Service with a Network override owns no real connections")

// refuseRealConnectionUnderNetworkOverride answers
// ErrNetworkOverrideOwnsNoConnections on a Service whose Network is
// overridden, and nil otherwise.
//
// A core that registerInboundConn or attachOutboundNetCore inserts into
// s.conns is closed through Service.Network(): the inbound teardown
// (unregisterInboundConn) and the shutdown drain (closeAllInboundConns) can
// reach it no other way. An override does not hold the registry's cores, so
// a real connection registered under one could not be closed by the paths
// that own its teardown — its Close is answered ErrUnknownConn while the
// entry is removed anyway, and the drain enumerates the override and misses
// it, leaving the socket and the writer goroutine open and Run waiting on
// connWg for the read timeout. The override exists to drive protocol logic
// over an in-memory transport, so owning no real connection costs it
// nothing; refusing at both insertion points keeps every core they insert
// reachable by the Network that closes it.
//
// Outside that invariant, on purpose: test fixtures that seed s.conns
// directly with setTestConnEntryLocked. Their cores are built by
// newTestNetCore and closed by its test cleanup, not by any Network, and no
// production teardown path runs on them.
//
// The read takes no lock because networkOverride is set only by a struct
// literal key while the Service is being built — newService, behind
// NewServiceWithNetwork, or a test fixture that builds the Service as a
// literal — before it exists for any other goroutine, so every later read is
// ordered after that write by whatever hands the Service out.
// TestNetworkOverrideIsSetOnlyAtConstruction fails on any other write. A
// late write would also break the refusal itself: it is taken at
// registration, so an override installed afterwards would receive the Close
// of a connection registered under the bridge.
func (s *Service) refuseRealConnectionUnderNetworkOverride() error {
	if s.networkOverride != nil {
		return ErrNetworkOverrideOwnsNoConnections
	}
	return nil
}

// Network returns the transport-level API surface of the Service.
//
// Default path: the returned value is backed by a bridge adapter and shares
// no state with other callers — invoking Network() multiple times is cheap
// and safe; every call returns an independent bridge instance over the same
// Service. Consumers must not assume pointer identity across calls.
//
// Override path: when the Service was built with a caller-supplied
// netcore.Network (NewServiceWithNetwork; see s.networkOverride), that value
// is returned verbatim
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

// coreForID resolves id to the owning *netcore.NetCore under s.peerMu.RLock.
// Returns nil if id is not registered. The method releases the lock
// before returning; callers operate on NetCore outside of s.peerMu, which
// is the existing convention across service.go send paths.
func (b *networkBridge) coreForID(id domain.ConnID) *netcore.NetCore {
	b.svc.peerMu.RLock()
	defer b.svc.peerMu.RUnlock()
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

// SendFrameSync enqueues frame and blocks until one of: the writer flushes
// it, the caller ctx is cancelled (including mid-flight), or the
// per-connection sync deadline (syncFlushTimeout) elapses. Honours the
// Network.SendFrameSync contract end-to-end: a request-scoped timeout set
// by the caller interrupts the flush wait instead of being silently
// upgraded to the 5 s NetCore deadline.
//
// Routing through SendRawSyncCtx is the only way to preserve ctx mid-flight:
// SendRawSync (the non-ctx twin) waits solely on its internal timer and would
// ignore ctx cancellation once the frame has been enqueued. When the
// returned status is SendCtxCancelled, ctx.Err() is returned verbatim so
// callers can discriminate context.Canceled from context.DeadlineExceeded
// via errors.Is — the defensive ErrSendCtxCancelled mapping exists for
// any future caller that bypasses this bridge path.
func (b *networkBridge) SendFrameSync(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	core := b.coreForID(id)
	if core == nil {
		return netcore.ErrUnknownConn
	}
	return ctxSendStatusToError(ctx, core.SendRawSyncCtx(ctx, frame))
}

// SendFrameBlocking enqueues frame on id's writer queue, waiting for a slot
// while the queue is full until ctx ends, and returns once the frame is
// queued — it does not wait for the flush. Delegates to
// NetCore.SendRawBlockingCtx, whose wait resumes on the writer's own receive
// from the queue, so room is noticed the moment it appears and nothing polls.
// SendFrame and SendFrameSync are untouched: on them a full queue is still
// ErrSendBufferFull, the slow-peer eviction signal.
func (b *networkBridge) SendFrameBlocking(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	core := b.coreForID(id)
	if core == nil {
		return netcore.ErrUnknownConn
	}
	return ctxSendStatusToError(ctx, core.SendRawBlockingCtx(ctx, frame))
}

// ctxSendStatusToError maps the status of a ctx-aware NetCore send to the
// Network contract. SendCtxCancelled becomes ctx.Err() verbatim, so callers
// can tell context.Canceled from context.DeadlineExceeded via errors.Is;
// every other status goes through netcore.SendStatusToError.
func ctxSendStatusToError(ctx context.Context, status netcore.SendStatus) error {
	if status != netcore.SendCtxCancelled {
		return netcore.SendStatusToError(status)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Defensive: core reported ctx-cancel but ctx reports no error —
	// surface the typed sentinel rather than nil so the caller never
	// mistakes "ctx said cancelled mid-flight" for a successful send.
	return netcore.ErrSendCtxCancelled
}

// Enumerate iterates registered connections matching dir. If ctx is
// already cancelled on entry, no iteration starts. The walk acquires
// s.peerMu.RLock for the full duration of the iteration — matching the
// existing forEach*Locked convention — so fn must not call back into
// Service methods that take s.peerMu (deadlock). Callers that need to act
// on a ConnID outside the lock should collect ConnIDs into a local
// slice inside fn and process them after Enumerate returns.
func (b *networkBridge) Enumerate(ctx context.Context, dir netcore.Direction, fn func(domain.ConnID) bool) {
	if ctx.Err() != nil {
		return
	}
	b.svc.peerMu.RLock()
	defer b.svc.peerMu.RUnlock()
	b.svc.forEachConnLocked(func(info connInfo) bool {
		if info.dir != dir {
			return true
		}
		return fn(info.id)
	})
}

// Close triggers graceful shutdown of the connection by id. The actual
// teardown protocol lives on *NetCore.Close(): it shuts the send gate, closes
// the raw socket, signals the writer and waits for it to release the whole
// queue residue. The queue itself is NEVER closed as a Go channel — see
// docs/protocol/network_core.md, "Queue ownership". Registry
// eviction is a separate concern owned by the lifecycle helpers
// (unregisterConnLocked) and is not performed here — Close() signals
// the transport to stop, registry cleanup follows the existing teardown
// path on the owning Service method.
//
// Unlike SendFrame and Enumerate, Close does not consult ctx: releasing a
// connection is not cancellable work (see netcore.Network.Close). Its callers
// are teardown and eviction paths that have already decided the connection
// must go, and many of them run exactly when the lifecycle ctx has ended. A
// skipped close there is a leak, not a cancellation: the inbound teardown
// removes the registry entry right after this call, and once it is gone
// nothing can reach the core, whose writer returns only on Close.
func (b *networkBridge) Close(_ context.Context, id domain.ConnID) error {
	core := b.coreForID(id)
	if core == nil {
		return netcore.ErrUnknownConn
	}
	core.Close()
	return nil
}

// RemoteAddr returns the peer address string for id, or "" if id is not
// registered. The lookup holds s.peerMu.RLock briefly and then reads the
// address from NetCore, whose own accessor is safe for concurrent use.
func (b *networkBridge) RemoteAddr(id domain.ConnID) string {
	core := b.coreForID(id)
	if core == nil {
		return ""
	}
	return core.RemoteAddr()
}

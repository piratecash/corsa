package netcore

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Network is the transport-level API exposed by the netcore package to
// consumers (primarily node.Service). It is intentionally narrow: only
// frame I/O, connection enumeration, shutdown and a lightweight address
// accessor. Auth, tracking, reachability, ban score, inbox routing and
// any other policy concern remain outside this interface — they live on
// node.Service surface and are not owned by the transport boundary.
//
// Identity keys.
//   - The public key of Network is domain.ConnID. It is the transport-layer
//     identity: stable for the lifetime of a single socket, no longer.
//   - domain.PeerIdentity (the business / routing identity that survives
//     reconnects) does not appear in this interface. Resolution between
//     PeerIdentity and ConnID is a concern of the Service layer, not of
//     the transport.
//
// Partial-failure model.
//   - SendFrame / SendFrameSync return plain error. Partial-failure
//     outcomes (buffer full, writer goroutine already exited, sync flush
//     timeout, channel closed during teardown, marshal error) are
//     expressed via typed sentinel errors declared below and discriminated
//     through errors.Is. The lower-level SendStatus enum remains an
//     implementation detail of *NetCore and is mapped to sentinels at the
//     bridge boundary.
//
// Lifecycle carve-out.
//   - Accept / Register / Unregister are not methods of Network. Those
//     operations stay net.Conn-first inside node/conn_registry.go —
//     registerInboundConnLocked, attachOutboundCoreLocked and
//     unregisterConnLocked are the intentional entry boundary for raw
//     sockets and are the only code that binds a net.Conn to a ConnID.
//     Network is the working API for frames on already-registered
//     connections, not a factory for them.
//   - The permanent net.Conn-first surface inside node/ is the set of
//     functions whose signature is dictated by structural role
//     (pre-registration IP policy, lifecycle binding, external interface
//     pinning net.Conn) and therefore will not migrate to ConnID. The
//     authoritative enumeration of that permanent set, together with the
//     separately classified transitional net.Conn-first bridges that are
//     still present on the current branch but expected to shrink, lives
//     in the block-comment at the top of node/conn_registry.go. That
//     comment is the normative source of truth.
//
// Transitional bridge (NetCore.Conn()).
//   - NetCore.Conn() returns the underlying net.Conn and is a known
//     transitional bridge for write wrappers and a small set of address /
//     iteration sites that have not yet migrated to ConnID-first surface.
//     It is not part of the permanent carve-out described in
//     node/conn_registry.go. New call sites of NetCore.Conn() outside the
//     existing locations require explicit justification at review.
type Network interface {
	// SendFrame asynchronously enqueues frame for writing to id. It returns
	// nil when the frame has been accepted by the writer queue. On partial
	// failure it returns one of the Err* sentinels below; the caller uses
	// errors.Is to select recovery (slow-peer eviction, teardown ack, etc.).
	// Returns ErrUnknownConn if id is not registered.
	SendFrame(ctx context.Context, id domain.ConnID, frame []byte) error

	// SendFrameSync enqueues frame and blocks until the writer goroutine
	// flushes it, ctx is cancelled, or the per-connection sync deadline
	// elapses. Applied on paths that must not proceed before the bytes
	// have left the process (auth handshake, critical control frames).
	// The sentinel set is the same as SendFrame plus ctx.Err() propagation.
	SendFrameSync(ctx context.Context, id domain.ConnID, frame []byte) error

	// Enumerate walks the registered connections matching dir and calls fn
	// with each ConnID. The callback is transport-level only: PeerIdentity
	// and net.Conn are deliberately hidden. Sites that need the peer
	// address request it via RemoteAddr(id) outside the callback.
	// fn returning false stops iteration. Enumerate respects ctx: it does
	// not start a new iteration if ctx is already cancelled.
	Enumerate(ctx context.Context, dir Direction, fn func(domain.ConnID) bool)

	// Close initiates graceful shutdown of the connection identified by id
	// by delegating to *NetCore.Close (rawConn.Close → close(sendCh) →
	// wait for the writer goroutine to drain). It is idempotent. Returns
	// ErrUnknownConn if id is not registered.
	Close(ctx context.Context, id domain.ConnID) error

	// RemoteAddr is a lightweight accessor for logging and ban tracking.
	// Returns the empty string if id is not registered — callers treat the
	// zero value as "unknown", not as an error.
	RemoteAddr(id domain.ConnID) string
}

// Sentinel errors for the Network interface. Every partial-failure outcome
// previously expressed as a SendStatus enum value maps to exactly one
// sentinel; mapping is performed at the bridge boundary (SendStatusToError).
// Callers discriminate via errors.Is, never by string comparison.
var (
	// ErrUnknownConn — id is not present in the connection registry.
	// Either the connection was never registered, or it has already been
	// unregistered by a teardown path. Callers typically drop the frame
	// silently and log at debug level.
	ErrUnknownConn = errors.New("netcore: unknown connection")

	// ErrSendBufferFull — the writer queue is saturated; the remote peer
	// is not draining. Slow-peer eviction signal for the async path.
	ErrSendBufferFull = errors.New("netcore: send buffer full")

	// ErrSendWriterDone — the writer goroutine has already exited. The
	// connection is being torn down; callers must not attempt recovery.
	ErrSendWriterDone = errors.New("netcore: writer goroutine exited")

	// ErrSendTimeout — sync flush deadline elapsed before the writer
	// acknowledged the frame. Treated as peer-level unresponsiveness on
	// sync paths.
	ErrSendTimeout = errors.New("netcore: sync flush timeout")

	// ErrSendChanClosed — sendCh was closed while the send was in
	// progress. The connection is shutting down; orderly teardown owns
	// the socket. Callers must not Close() the connection in response.
	ErrSendChanClosed = errors.New("netcore: send channel closed")

	// ErrSendMarshalError — the caller-supplied frame could not be
	// serialised. Indicates a bug in the caller, not a transport fault.
	ErrSendMarshalError = errors.New("netcore: frame marshal error")

	// ErrSendInvalidStatus — defensive sentinel for SendStatusInvalid
	// (zero value). Should never appear in correct code; its presence
	// indicates an uninitialised status reached the bridge boundary.
	ErrSendInvalidStatus = errors.New("netcore: invalid send status")

	// ErrSendCtxCancelled — defensive sentinel for SendCtxCancelled. The
	// bridge boundary (networkBridge.SendFrameSync) intercepts this status
	// and returns the caller's ctx.Err() verbatim, preserving the
	// context.Canceled / context.DeadlineExceeded distinction. This
	// sentinel exists so that any future caller bypassing the bridge still
	// receives a typed error instead of a silent mis-mapping.
	ErrSendCtxCancelled = errors.New("netcore: caller ctx cancelled")
)

// SendStatusToError maps a SendStatus to the corresponding public sentinel
// error. Defined in the netcore package so that the mapping lives next to
// both producer (SendStatus enum) and consumer (Network sentinels).
// SendOK maps to nil. SendStatusInvalid maps to ErrSendInvalidStatus so
// the zero value cannot be silently treated as success.
func SendStatusToError(s SendStatus) error {
	switch s {
	case SendOK:
		return nil
	case SendBufferFull:
		return ErrSendBufferFull
	case SendWriterDone:
		return ErrSendWriterDone
	case SendTimeout:
		return ErrSendTimeout
	case SendChanClosed:
		return ErrSendChanClosed
	case SendMarshalError:
		return ErrSendMarshalError
	case SendCtxCancelled:
		return ErrSendCtxCancelled
	case SendStatusInvalid:
		return ErrSendInvalidStatus
	default:
		return ErrSendInvalidStatus
	}
}

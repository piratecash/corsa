package node

// network_consumer.go holds the first Service-internal consumer of the
// netcore.Network interface returned by Service.Network().
//
// Every other outbound write inside Service currently flows through the
// ConnID-first helpers (writeJSONFrameByID / enqueueFrameByID) or the
// session-scoped writeSessionFrame carve-out. Both paths resolve
// *netcore.NetCore from s.conns and call pc.SendRaw directly, bypassing
// the Network() injection seam — which means a test harness wired via
// NewServiceWithNetwork (e.g. netcoretest.Backend) cannot observe those
// frames, because the override is never consulted on the write path.
//
// sendFrameViaNetwork and sendFrameViaNetworkSync exist so production
// reply paths route through s.Network().SendFrame / .SendFrameSync: when
// the Service is constructed with a caller-supplied Network, the backend
// receives the bytes and protocol logic becomes testable without a TCP
// socket. Both the async and sync reply layers of dispatchNetworkFrame
// are fully routed through these helpers, along with the inbound-direct /
// subscriber / gossip write call-sites.
//
// sendFrameBytesViaNetwork and sendFrameBytesViaNetworkSync are the
// raw-bytes companions of the Frame-API helpers. They take
// pre-marshalled []byte and have NO marshal-fallback path — every drop
// class (unregistered / buffer-full / writer-done / chan-closed /
// timeout) surfaces through the return. Strict callers
// (writeFrameToInbound, writePushFrame) marshal themselves so
// they can distinguish a caller-side encode bug from a transport drop.
//
// writeSessionFrame and the handleConn / handleCommand top-of-loop
// error replies still resolve *netcore.NetCore through the legacy
// helpers — expanding the migration to those call-sites is out of
// scope for this file.

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// sendFrameViaNetwork marshals frame and enqueues it asynchronously through
// the netcore.Network surface for id. Contract: nil iff the writer
// accepted the bytes; every non-success outcome returns a non-nil error so
// strict callers can classify (success / unregistered / drop) via
// errors.Is. Fire-and-forget callers stay on `_ = s.sendFrameViaNetwork(...)`
// and lose nothing.
//
//   - netcore.ErrUnknownConn — connection not registered in the active
//     Network (either the live s.conns registry or a test backend). This
//     is the single-writer-invariant violation that writeJSONFrameByID
//     reports as ErrUnregisteredWrite; mapped to the same sentinel so
//     existing call-site acknowledgment patterns keep working.
//   - netcore.ErrSendBufferFull — writer queue saturated. Slow-peer
//     eviction is triggered by delegating to network.Close(ctx, id) —
//     the Network-level equivalent of pc.Close() used inside
//     enqueueFrameByID. The sentinel is RETURNED to the caller:
//     strict callers like writePushFrame need to know that the frame
//     did not reach the peer so they can clean up subscriber state.
//     Eviction-as-side-effect is unchanged.
//   - netcore.ErrSendWriterDone / ErrSendChanClosed — connection is
//     already being torn down; the sentinel is RETURNED so strict
//     callers see "frame did not reach the peer". No additional Close
//     is issued (teardown owns the socket).
//   - context cancellation sentinels (context.Canceled /
//     context.DeadlineExceeded) propagate verbatim; the caller decides
//     whether to log them.
//   - marshal / invalid-status sentinels indicate a caller bug and are
//     returned without eviction so the error is visible in the log.
//
// Marshal failure of the input frame is handled inside the helper via the
// fallback path (an ErrCodeEncodeFailed frame is sent to the peer); a
// caller-side encode bug is therefore NOT distinguishable through the
// return value. Strict callers that must classify marshal failure (e.g.
// writeFrameToInbound, writePushFrame) MUST marshal themselves
// and use sendFrameBytesViaNetwork instead — the raw-bytes helper has no
// fallback and surfaces every drop class through the return.
//
// ctx is the caller-provided operation context. The Service constructor
// initialises s.runCtx to context.Background() as a boundary default and
// Run(ctx) replaces it with the real cancellable lifecycle ctx, so every
// call-site inside Service already has a non-nil ctx to pass here — this
// helper does not fabricate a context of its own.
//
// The return is error (not bool) so callers must acknowledge the outcome
// explicitly. Fire-and-forget call-sites use
// `_ = s.sendFrameViaNetwork(...)`.
func (s *Service) sendFrameViaNetwork(ctx context.Context, id domain.ConnID, frame protocol.Frame) error {
	network := s.Network()
	addr := network.RemoteAddr(id)

	line, marshalErr := protocol.MarshalFrameLine(frame)
	if marshalErr != nil {
		// Mirror writeJSONFrameByID's marshal-fallback: try to deliver a
		// structured error frame to the peer so the other side sees
		// ErrCodeEncodeFailed instead of silence. The fallback itself
		// routes through the Network surface — never drop back to a
		// legacy helper here, otherwise the "single Network() consumer"
		// invariant this file establishes regresses.
		fallback, _ := json.Marshal(protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeEncodeFailed,
			Error: marshalErr.Error(),
		})
		data := append(fallback, '\n')
		res := classifyNetworkSendResult(network.SendFrame(ctx, id, data))
		emitProtocolTrace(addr, frame, res)
		if res == enqueueUnregistered {
			logUnregisteredWrite(addr, frame, "sendFrameViaNetwork.marshal_fallback")
			return ErrUnregisteredWrite
		}
		return nil
	}

	sendErr := network.SendFrame(ctx, id, []byte(line))
	res := classifyNetworkSendResult(sendErr)
	emitProtocolTrace(addr, frame, res)

	switch {
	case sendErr == nil:
		return nil
	case errors.Is(sendErr, netcore.ErrUnknownConn):
		logUnregisteredWrite(addr, frame, "sendFrameViaNetwork")
		return ErrUnregisteredWrite
	case errors.Is(sendErr, netcore.ErrSendBufferFull):
		// Slow-peer eviction — same operational response as the legacy
		// enqueueFrameByID branch for SendBufferFull. Close goes through
		// the Network surface so the seam is preserved; the bridge maps
		// this to NetCore.Close() which closes the raw socket and
		// sendCh exactly like the legacy pc.Close() call.
		// The sentinel is returned: strict callers need to see the drop.
		// Fire-and-forget callers swallow it under `_`.
		_ = network.Close(ctx, id)
		return sendErr
	case errors.Is(sendErr, netcore.ErrSendWriterDone),
		errors.Is(sendErr, netcore.ErrSendChanClosed):
		// Connection already tearing down — drop the frame, no Close
		// (teardown owns the socket). Sentinel is returned so strict
		// callers can react to closing-writer states.
		return sendErr
	case errors.Is(sendErr, context.Canceled),
		errors.Is(sendErr, context.DeadlineExceeded):
		// Propagate ctx errors verbatim; caller logs them.
		return sendErr
	default:
		// Marshal / invalid-status / unknown sentinel — caller bug path.
		// Do not evict the connection: the fault is local, not a slow
		// peer. Returning the error surfaces it in caller's log.
		return sendErr
	}
}

// sendFrameViaNetworkSync is the sync-path symmetric of sendFrameViaNetwork.
// It marshals frame and delegates to network.SendFrameSync, which blocks
// until the writer has handed the bytes to the socket (or a sentinel is
// returned). Contract: nil iff the writer accepted the bytes; every
// non-success outcome returns a non-nil error so strict callers can
// classify (success / unregistered / drop) via errors.Is.
//
//   - netcore.ErrUnknownConn → single-writer-invariant violation; mapped to
//     ErrUnregisteredWrite so call-site diagnostic surface matches the
//     legacy writeJSONFrameSyncByID return.
//   - netcore.ErrSendBufferFull → slow-peer eviction. Unlike the async
//     bridge.SendFrame path where pc.Close() is a side-effect of the
//     legacy helper, bridge.SendFrameSync does NOT auto-close the core on
//     buffer-full — this helper must request eviction explicitly through
//     network.Close(ctx, id) to preserve the legacy pc.Close() semantics
//     from enqueueFrameSyncByID. The sentinel is RETURNED so strict
//     callers see the drop; eviction-as-side-effect is unchanged.
//   - netcore.ErrSendTimeout → sync flush timeout. Same eviction
//     rationale: the legacy path calls pc.Close() on SendTimeout, and the
//     bridge does not; we restore the close here. Sentinel is RETURNED.
//   - netcore.ErrSendWriterDone / ErrSendChanClosed → connection already
//     tearing down; sentinel is RETURNED, no Close (teardown owns the
//     socket).
//   - context.Canceled / context.DeadlineExceeded → propagated verbatim;
//     caller decides whether to log them.
//   - marshal / invalid-status / unknown sentinel → caller bug path;
//     returned without eviction so the fault surfaces in the caller log.
//
// Marshal failure of the input frame is handled inside the helper via the
// fallback path (an ErrCodeEncodeFailed frame is sent to the peer); a
// caller-side encode bug is therefore NOT distinguishable through the
// return value. Strict callers that must classify marshal failure (e.g.
// writeFrameToInbound) MUST marshal themselves and use
// sendFrameBytesViaNetworkSync instead — the raw-bytes helper has no
// fallback and surfaces every drop class through the return.
//
// ctx semantics are identical to sendFrameViaNetwork: callers inside
// Service pass s.runCtx, which the constructor seeds with
// context.Background() and Run(ctx) replaces with the real lifecycle ctx.
// This helper does not fabricate a context of its own.
//
// The return is error (not bool). Fire-and-forget call-sites use
// `_ = s.sendFrameViaNetworkSync(...)`.
func (s *Service) sendFrameViaNetworkSync(ctx context.Context, id domain.ConnID, frame protocol.Frame) error {
	network := s.Network()
	addr := network.RemoteAddr(id)

	line, marshalErr := protocol.MarshalFrameLine(frame)
	if marshalErr != nil {
		// Mirror writeJSONFrameSyncByID's marshal-fallback path: attempt
		// to deliver a structured encode-failed frame so the peer sees a
		// reason code before the socket closes. Fallback must still route
		// through the Network surface — never drop to a legacy helper.
		fallback, _ := json.Marshal(protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeEncodeFailed,
			Error: marshalErr.Error(),
		})
		data := append(fallback, '\n')
		res := classifyNetworkSendResult(network.SendFrameSync(ctx, id, data))
		emitProtocolTrace(addr, frame, res)
		if res == enqueueUnregistered {
			logUnregisteredWrite(addr, frame, "sendFrameViaNetworkSync.marshal_fallback")
			return ErrUnregisteredWrite
		}
		return nil
	}

	sendErr := network.SendFrameSync(ctx, id, []byte(line))
	res := classifyNetworkSendResult(sendErr)
	emitProtocolTrace(addr, frame, res)

	switch {
	case sendErr == nil:
		return nil
	case errors.Is(sendErr, netcore.ErrUnknownConn):
		logUnregisteredWrite(addr, frame, "sendFrameViaNetworkSync")
		return ErrUnregisteredWrite
	case errors.Is(sendErr, netcore.ErrSendBufferFull):
		// Slow-peer eviction — restore legacy enqueueFrameSyncByID
		// semantics (pc.Close on SendBufferFull) via the Network surface.
		// bridge.SendFrameSync does not auto-close, so the Close call is
		// mandatory here, not optional. Sentinel returned to caller.
		log.Warn().Str("addr", addr).Msg("send buffer full, disconnecting slow peer")
		_ = network.Close(ctx, id)
		return sendErr
	case errors.Is(sendErr, netcore.ErrSendTimeout):
		// Sync flush timeout — restore legacy pc.Close on SendTimeout.
		// Same rationale as SendBufferFull above: bridge does not
		// auto-close, we must. Sentinel returned to caller.
		log.Warn().Str("addr", addr).Msg("sync flush timeout, disconnecting peer")
		_ = network.Close(ctx, id)
		return sendErr
	case errors.Is(sendErr, netcore.ErrSendWriterDone),
		errors.Is(sendErr, netcore.ErrSendChanClosed):
		// Connection already tearing down — sentinel returned to caller,
		// no Close (teardown owns the socket).
		return sendErr
	case errors.Is(sendErr, context.Canceled),
		errors.Is(sendErr, context.DeadlineExceeded):
		return sendErr
	default:
		// Marshal / invalid-status / unknown sentinel — caller bug path.
		return sendErr
	}
}

// sendFrameBytesViaNetwork is the raw-bytes companion of sendFrameViaNetwork.
// It accepts pre-marshalled wire bytes and routes them through
// network.SendFrame; unlike the Frame-API helper, it has NO marshal-fallback
// path because there is nothing to marshal — the caller already owns
// serialisation. This makes it the right primitive for strict callers that
// must distinguish a caller-side encode bug from a transport drop:
//
//   - writeFrameToInbound — marshals first, logs frame_inbound_marshal_failed
//     locally on encode error, then calls this helper only with valid bytes;
//     classifies the return into 3-state sent/unregistered/dropped.
//   - writePushFrame — marshals first, leaves the subscriber alone
//     on encode error (legacy split: caller-side bug ≠ transport drop),
//     then calls this helper and removes the subscriber on any non-nil
//     return (transport-level "did not reach the peer").
//
// Outcome tree is identical to sendFrameViaNetwork's full-propagation
// contract: nil iff the writer accepted the bytes; ErrUnknownConn →
// ErrUnregisteredWrite mapping; ErrSendBufferFull triggers
// network.Close(ctx, id) and returns the sentinel; ErrSendWriterDone /
// ErrSendChanClosed return the sentinel without Close (teardown owns the
// socket); ctx errors and unknown sentinels propagate verbatim.
//
// Like sendFrameViaNetwork, the ctx argument is the caller-provided
// operation context; this helper does not fabricate a context of its own.
func (s *Service) sendFrameBytesViaNetwork(ctx context.Context, id domain.ConnID, data []byte) error {
	network := s.Network()
	addr := network.RemoteAddr(id)

	sendErr := network.SendFrame(ctx, id, data)

	switch {
	case sendErr == nil:
		return nil
	case errors.Is(sendErr, netcore.ErrUnknownConn):
		logUnregisteredWriteRaw(addr, "sendFrameBytesViaNetwork")
		return ErrUnregisteredWrite
	case errors.Is(sendErr, netcore.ErrSendBufferFull):
		// Slow-peer eviction — same operational response as the Frame-API
		// helper. Sentinel returned so the strict caller can react.
		_ = network.Close(ctx, id)
		return sendErr
	case errors.Is(sendErr, netcore.ErrSendWriterDone),
		errors.Is(sendErr, netcore.ErrSendChanClosed):
		// Connection already tearing down — sentinel returned, no Close.
		return sendErr
	case errors.Is(sendErr, context.Canceled),
		errors.Is(sendErr, context.DeadlineExceeded):
		return sendErr
	default:
		// Unknown sentinel — surfaced to caller for logging.
		return sendErr
	}
}

// sendFrameBytesViaNetworkSync is the sync-path raw-bytes companion of
// sendFrameViaNetworkSync. It blocks until the writer has handed
// the bytes to the socket (or a sentinel is returned) and has NO marshal-
// fallback. Strict callers like writeFrameToInbound use it for fail-fast
// inbound-direct delivery where the 3-state outcome (sent / unregistered /
// dropped) drives a downstream bool return — caller-side marshal must be
// kept so frame_inbound_marshal_failed stays distinct from
// frame_inbound_dropped.
//
// Outcome tree mirrors sendFrameViaNetworkSync's full-propagation contract,
// including the explicit network.Close(ctx, id) on
// ErrSendBufferFull and ErrSendTimeout (bridge.SendFrameSync does not
// auto-close, so eviction is mandatory here). Sentinel is returned in all
// non-success cases.
func (s *Service) sendFrameBytesViaNetworkSync(ctx context.Context, id domain.ConnID, data []byte) error {
	network := s.Network()
	addr := network.RemoteAddr(id)

	sendErr := network.SendFrameSync(ctx, id, data)

	switch {
	case sendErr == nil:
		return nil
	case errors.Is(sendErr, netcore.ErrUnknownConn):
		logUnregisteredWriteRaw(addr, "sendFrameBytesViaNetworkSync")
		return ErrUnregisteredWrite
	case errors.Is(sendErr, netcore.ErrSendBufferFull):
		log.Warn().Str("addr", addr).Msg("send buffer full, disconnecting slow peer")
		_ = network.Close(ctx, id)
		return sendErr
	case errors.Is(sendErr, netcore.ErrSendTimeout):
		log.Warn().Str("addr", addr).Msg("sync flush timeout, disconnecting peer")
		_ = network.Close(ctx, id)
		return sendErr
	case errors.Is(sendErr, netcore.ErrSendWriterDone),
		errors.Is(sendErr, netcore.ErrSendChanClosed):
		return sendErr
	case errors.Is(sendErr, context.Canceled),
		errors.Is(sendErr, context.DeadlineExceeded):
		return sendErr
	default:
		return sendErr
	}
}

// logUnregisteredWriteRaw is the raw-bytes counterpart of
// logUnregisteredWrite — the latter takes a protocol.Frame to log
// frame.Type, which the raw-bytes helpers do not have available. The
// diagnostic line keeps the same severity (Error) and key set sans
// command, so log aggregation can correlate single-writer-invariant
// violations regardless of whether the caller went through the Frame or
// the bytes API.
func logUnregisteredWriteRaw(addr, origin string) {
	log.Error().
		Str("origin", origin).
		Str("addr", addr).
		Msg("unregistered_write: conn missing NetCore — single-writer invariant violation, frame dropped")
}

// classifyNetworkSendResult maps a netcore.Network send outcome to the
// enqueueResult enum used by emitProtocolTrace. The mapping mirrors
// enqueueFrameByID so protocol_trace log lines stay comparable between
// the legacy path and the Network()-routed path — operators reading
// protocol_trace must not see a new send_outcome vocabulary for the same
// transport-level events.
func classifyNetworkSendResult(err error) enqueueResult {
	switch {
	case err == nil:
		return enqueueSent
	case errors.Is(err, netcore.ErrUnknownConn):
		return enqueueUnregistered
	default:
		// Every other sentinel (buffer-full, writer-done, chan-closed,
		// sync-timeout, marshal-error, invalid-status, ctx.Err) is an
		// operational drop from the observability point of view —
		// distinct from "never delivered because conn not registered".
		return enqueueDropped
	}
}

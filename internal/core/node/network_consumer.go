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
// sendFrameViaNetwork exists so at least one production reply path routes
// through s.Network().SendFrame: when the Service is constructed with a
// caller-supplied Network, the backend receives the bytes and protocol
// logic becomes testable without a TCP socket. Today the only migrated
// call-site is the inbound ping → pong reply in dispatchNetworkFrame;
// every other Service write path still resolves *netcore.NetCore from
// s.conns and calls pc.SendRaw through the legacy helpers. Expanding
// the migration to more call-sites is deliberately out of scope in
// this file.

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// sendFrameViaNetwork marshals frame and enqueues it asynchronously through
// the netcore.Network surface for id. Outcome semantics are deliberately
// aligned with enqueueFrameByID / writeJSONFrameByID so a caller migrating
// from the legacy helpers observes the same outcome tree:
//
//   - netcore.ErrUnknownConn — connection not registered in the active
//     Network (either the live s.conns registry or a test backend). This
//     is the single-writer-invariant violation that writeJSONFrameByID
//     reports as ErrUnregisteredWrite; mapped to the same sentinel so
//     existing call-site acknowledgment patterns keep working.
//   - netcore.ErrSendBufferFull — writer queue saturated. Slow-peer
//     eviction is triggered by delegating to network.Close(ctx, id) —
//     the Network-level equivalent of pc.Close() used inside
//     enqueueFrameByID. The frame is dropped (nil returned) because the
//     operational condition is handled, not an architectural error the
//     caller must react to.
//   - netcore.ErrSendWriterDone / ErrSendChanClosed — connection is
//     already being torn down; the frame is dropped, no additional Close
//     is issued (teardown owns the socket).
//   - context cancellation sentinels (context.Canceled /
//     context.DeadlineExceeded) propagate verbatim; the caller decides
//     whether to log them.
//   - marshal / invalid-status sentinels indicate a caller bug and are
//     returned without eviction so the error is visible in the log.
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
		_ = network.Close(ctx, id)
		return nil
	case errors.Is(sendErr, netcore.ErrSendWriterDone),
		errors.Is(sendErr, netcore.ErrSendChanClosed):
		// Connection already tearing down — drop the frame, no Close.
		return nil
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

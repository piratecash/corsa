package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// LocalRPCClient is the transport layer used by the service package to issue
// command frames against the embedded node. It replaces the low-level
// transport portion of the former DesktopClient.
//
// LocalRequestFrame / LocalRequestFrameCtx route through the in-process
// *node.Service.HandleLocalFrame. This path never reaches the TCP wire and is
// the only channel for local command dispatch — TCP fallback was removed
// because it bypassed RPC CommandTable and RBAC.
//
// It opens no connection to a remote peer. The raw v1 session it once dialled
// for an on-demand message sync went around the node's dial path — the SOCKS5
// proxy, the shared connection budget, the v2-first session with its pin and
// binding checks — and had no caller left, so it was removed rather than
// rebuilt; anything that needs a peer asks the embedded node.
//
// LocalRPCClient carries no mutable state and is safe for concurrent use.
type LocalRPCClient struct {
	info      AppInfo
	localNode *node.Service
}

// NewLocalRPCClient wires an LocalRPCClient from the AppInfo snapshot and
// the embedded node. A nil *node.Service is allowed: LocalAddress still
// answers, while LocalRequestFrame returns an explicit error.
func NewLocalRPCClient(info AppInfo, localNode *node.Service) *LocalRPCClient {
	return &LocalRPCClient{info: info, localNode: localNode}
}

// LocalNode exposes the embedded node pointer for sub-services that need
// direct access (file transfer, routing snapshot). It may return nil in
// standalone-RPC mode.
func (c *LocalRPCClient) LocalNode() *node.Service { return c.localNode }

// LocalAddress returns the effective loopback address for the configured
// listener. Used when the caller needs to skip self-dialling during peer
// sync.
func (c *LocalRPCClient) LocalAddress() string {
	listen := c.info.NodeConfig().ListenAddress
	if strings.HasPrefix(listen, ":") {
		return "127.0.0.1" + listen
	}
	return listen
}

// LocalRequestFrame dispatches a frame synchronously through the embedded
// node using a background context. Thin wrapper around LocalRequestFrameCtx
// for the common case where the caller does not have a context yet.
//
// Prefer LocalRequestFrameCtx in production paths — background context
// cannot be cancelled, so a stuck handler will block the caller.
func (c *LocalRPCClient) LocalRequestFrame(request protocol.Frame) (protocol.Frame, error) {
	return c.LocalRequestFrameCtx(context.Background(), request)
}

// LocalRequestFrameCtx dispatches a command frame through the embedded node.
//
// ctx is checked ONCE, before dispatch. That is the only moment at which
// cancellation can still prevent something: HandleLocalFrame is synchronous
// and cannot be interrupted, so by the time it returns its effect has already
// happened.
//
// It used to be checked afterwards as well, and that was worse than useless
// for a frame that changes state. A send_message whose context ended mid-call
// had already stored the message and queued it for delivery, yet the reply —
// carrying the message ID the node generated — was thrown away and the caller
// got context.Canceled. Retrying then produced a SECOND message with a new ID,
// so a cancellation that prevented nothing caused a duplicate. Handing back
// what actually happened lets the caller decide; the cancellation is still
// visible through its own context.
//
// TCP fallback to the data port is intentionally unavailable: local command
// dispatch must go through the in-process embedded node exclusively.
func (c *LocalRPCClient) LocalRequestFrameCtx(ctx context.Context, request protocol.Frame) (protocol.Frame, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Frame{}, err
	}

	if c.localNode == nil {
		return protocol.Frame{}, fmt.Errorf("localRequestFrame: embedded node is required, TCP fallback removed")
	}

	frame := c.localNode.HandleLocalFrame(request)
	if frame.Type == "error" {
		if frame.Code != "" {
			return protocol.Frame{}, protocol.ErrorFromCode(frame.Code)
		}
		if frame.Error != "" {
			return protocol.Frame{}, fmt.Errorf("%s", frame.Error)
		}
		return protocol.Frame{}, protocol.ErrProtocol
	}
	return frame, nil
}

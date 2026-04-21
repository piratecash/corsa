package service

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// LocalRPCClient is the transport layer used by the service package to issue
// command frames against the embedded node (in-process) or a remote peer
// over TCP. It replaces the low-level transport portion of the former
// DesktopClient.
//
// Two dispatch modes coexist:
//
//   - LocalRequestFrame / LocalRequestFrameCtx route through the in-process
//     *node.Service.HandleLocalFrame. This path never reaches the TCP wire
//     and is the only legitimate channel for local command dispatch — TCP
//     fallback was removed because it bypassed RPC CommandTable and RBAC.
//
//   - OpenSessionAt / RequestFrame dial a remote peer's data port to issue
//     legitimate P2P operations (ping, message sync). They perform a full
//     hello + optional session-auth handshake so the remote node treats
//     this client as an authenticated desktop peer.
//
// LocalRPCClient carries no mutable state and is safe for concurrent use.
type LocalRPCClient struct {
	info      AppInfo
	localNode *node.Service
}

// NewLocalRPCClient wires an LocalRPCClient from the AppInfo snapshot and
// the embedded node. A nil *node.Service is allowed — callers that only
// need the remote-TCP path (OpenSessionAt) still get a functional client,
// while LocalRequestFrame will return an explicit error.
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
// ctx.Err() is checked before and after HandleLocalFrame as a best-effort
// cancellation check — HandleLocalFrame itself is synchronous and cannot be
// interrupted mid-execution, so a stuck handler will block until it returns
// regardless of ctx cancellation.
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
	if err := ctx.Err(); err != nil {
		return protocol.Frame{}, err
	}
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

// OpenSessionAt dials a remote peer's TCP data port, issues a hello frame
// signed with the local identity, and optionally completes a session-auth
// challenge. On success the caller receives an open connection plus a
// buffered reader for subsequent RequestFrame calls.
func (c *LocalRPCClient) OpenSessionAt(ctx context.Context, address, clientKind string) (net.Conn, *bufio.Reader, protocol.Frame, error) {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, protocol.Frame{}, err
	}

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)

	id := c.info.Identity()
	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        clientKind,
		ClientVersion: strings.ReplaceAll(c.info.Version(), " ", "-"),
		ClientBuild:   config.ClientBuild,
		Address:       id.Address,
		PubKey:        identity.PublicKeyBase64(id.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(id),
	})
	if err != nil {
		_ = conn.Close()
		return nil, nil, protocol.Frame{}, err
	}
	if _, err := io.WriteString(conn, line); err != nil {
		_ = conn.Close()
		return nil, nil, protocol.Frame{}, err
	}
	welcome, err := readJSONFrame(reader)
	if err != nil {
		_ = conn.Close()
		return nil, nil, protocol.Frame{}, err
	}
	if welcome.Type == "error" {
		_ = conn.Close()
		if welcome.Code != "" {
			return nil, nil, protocol.Frame{}, protocol.ErrorFromCode(welcome.Code)
		}
		return nil, nil, protocol.Frame{}, protocol.ErrProtocol
	}
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   id.Address,
			Signature: identity.SignPayload(id, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+id.Address)),
		})
		if err != nil {
			_ = conn.Close()
			return nil, nil, protocol.Frame{}, err
		}
		if _, err := io.WriteString(conn, authLine); err != nil {
			_ = conn.Close()
			return nil, nil, protocol.Frame{}, err
		}
		authReply, err := readJSONFrame(reader)
		if err != nil {
			_ = conn.Close()
			return nil, nil, protocol.Frame{}, err
		}
		if authReply.Type == "error" {
			_ = conn.Close()
			if authReply.Code != "" {
				return nil, nil, protocol.Frame{}, protocol.ErrorFromCode(authReply.Code)
			}
			return nil, nil, protocol.Frame{}, protocol.ErrProtocol
		}
	}

	return conn, reader, welcome, nil
}

// RequestFrame sends a frame over an already-established remote session and
// parses the next reply line. The caller owns the connection lifecycle.
func (c *LocalRPCClient) RequestFrame(conn net.Conn, reader *bufio.Reader, request protocol.Frame) (protocol.Frame, error) {
	line, err := protocol.MarshalFrameLine(request)
	if err != nil {
		return protocol.Frame{}, err
	}
	if _, err := io.WriteString(conn, line); err != nil {
		return protocol.Frame{}, err
	}
	return readJSONFrame(reader)
}

// readJSONFrame parses a single framed JSON line from the session reader.
// Shared helper used by the remote-TCP path only — LocalRequestFrame does
// not hit the wire.
func readJSONFrame(reader *bufio.Reader) (protocol.Frame, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return protocol.Frame{}, err
	}

	frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
	if err != nil {
		return protocol.Frame{}, err
	}
	if frame.Type == "error" {
		if frame.Code != "" {
			return protocol.Frame{}, protocol.ErrorFromCode(frame.Code)
		}
		return protocol.Frame{}, protocol.ErrProtocol
	}
	return frame, nil
}

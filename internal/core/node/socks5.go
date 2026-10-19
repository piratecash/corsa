package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

var (
	// errOverlayWithoutProxy: an overlay address (.onion, .b32.i2p) and no
	// SOCKS5 proxy to reach it through. An overlay name has no direct form,
	// so the dial ends here, before any socket.
	errOverlayWithoutProxy = errors.New("no SOCKS5 proxy configured for an overlay address")
	// errProxyUnavailable: the SOCKS5 proxy itself could not be reached. The
	// dial ends with it; it never falls back to a direct connection, which
	// would put the node's own IP and a local lookup of the name on the wire.
	errProxyUnavailable = errors.New("SOCKS5 proxy unavailable")
	// errInvalidOverlayAddress: a name in an overlay TLD that is not a valid
	// overlay name.
	errInvalidOverlayAddress = errors.New("invalid overlay address")
)

// tcpDialFunc opens one TCP socket to address. It is the primitive under
// dialPeer — the direct dial and the connection to the SOCKS5 proxy both go
// through it, so a test that replaces it sees every socket the node's peer
// dials open, and every name they would resolve.
type tcpDialFunc func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error)

// dialTCPDirect is the production tcpDialFunc.
func dialTCPDirect(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, "tcp", address)
}

// dialPeer is the one place the node opens a TCP socket to a peer: a name in
// an overlay TLD (.onion, .i2p) goes only through the SOCKS5 proxy configured
// in cfg.ProxyAddress, or is refused — never dialled or resolved directly;
// everything else is dialled directly. Every outbound path reaches it
// — the connection manager through dialPeerRawForCM, the auxiliary dials
// (syncPeer, the notice fallback, session dials) through dialPeerWithBudget.
// ctx bounds the whole dial, the proxy handshake included.
func (s *Service) dialPeer(ctx context.Context, address domain.PeerAddress, timeout time.Duration) (conn net.Conn, dialErr error) {
	defer func() {
		attempt, _ := ctx.Value(addressDialAttemptKey{}).(*addressDialAttempt)
		if attempt != nil && !errors.Is(dialErr, errProxyUnavailable) && !errors.Is(dialErr, errOverlayWithoutProxy) && !errors.Is(dialErr, errInvalidOverlayAddress) {
			attempt.performed = true
		}
	}()
	host, port, ok := splitHostPort(string(address))
	if !ok {
		return nil, fmt.Errorf("%w: %s", errInvalidOverlayAddress, address)
	}

	overlayTarget, overlay, err := overlayDialTarget(host, port)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, address)
	}
	if !overlay {
		return s.peerTCPDial(ctx, string(address), timeout)
	}
	proxy := s.cfg.ProxyAddress
	if proxy == "" {
		return nil, fmt.Errorf("%w: %s", errOverlayWithoutProxy, address)
	}
	return dialSOCKS5(ctx, s.peerTCPDial, proxy, overlayTarget, timeout)
}

// overlayTLDs are the top-level domains that exist only inside an overlay
// network. A name under one of them has no meaning to the public DNS, and
// asking it leaks the name to the local resolver.
var overlayTLDs = map[string]struct{}{
	"onion": {},
	"i2p":   {},
}

// overlayDialTarget decides by the TLD alone whether host is an overlay
// name — case-insensitively and with the trailing dots of a fully qualified
// name removed — so a name the overlay validator rejects cannot slip into
// the direct dial and the local resolver. An overlay name that is not valid
// (isOnionAddress, isI2PAddress) is errInvalidOverlayAddress; a valid one is
// returned in the canonical form handed to the proxy.
func overlayDialTarget(host, port string) (target string, overlay bool, err error) {
	name := strings.ToLower(strings.TrimRight(host, "."))
	if _, ok := overlayTLDs[name[strings.LastIndex(name, ".")+1:]]; !ok {
		return "", false, nil
	}
	if !isOnionAddress(name) && !isI2PAddress(name) {
		return "", true, errInvalidOverlayAddress
	}
	return net.JoinHostPort(name, port), true, nil
}

// dialSOCKS5 establishes a TCP connection through a SOCKS5 proxy
// (RFC 1928) using the CONNECT command with no authentication.
// The target address is sent as a domain name (ATYP 0x03), which is
// what Tor expects for .onion resolution: the name is resolved by the
// proxy, never on this machine.
func dialSOCKS5(ctx context.Context, dial tcpDialFunc, proxyAddr, targetAddr string, timeout time.Duration) (net.Conn, error) {
	host, portStr, ok := splitHostPort(targetAddr)
	if !ok {
		return nil, fmt.Errorf("socks5: invalid target address: %s", targetAddr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("socks5: invalid target port: %s", portStr)
	}
	if len(host) > 255 {
		return nil, errors.New("socks5: hostname too long")
	}

	conn, err := dial(ctx, proxyAddr, timeout)
	if err != nil {
		return nil, fmt.Errorf("socks5: %w at %s: %w", errProxyUnavailable, proxyAddr, err)
	}
	if err := negotiateSOCKS5(ctx, conn, host, port, time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// socks5Stream is what the proxy handshake needs of the socket: the bytes
// and a deadline. It stays narrower than net.Conn on purpose — the handshake
// runs before the socket is handed to the managed transport, and nothing in
// it may outlive the exchange.
type socks5Stream interface {
	io.ReadWriter
	SetDeadline(t time.Time) error
}

// negotiateSOCKS5 runs the proxy handshake on conn until deadline or until
// ctx ends, whichever is first. The handshake reads block on the socket, not
// on ctx, so a cancelled ctx moves the socket's deadline into the past: the
// blocked read returns at once and the caller closes the socket, instead of
// both the socket and its unit of the connection budget being held until the
// timeout.
func negotiateSOCKS5(ctx context.Context, conn socks5Stream, host string, port int, deadline time.Time) error {
	_ = conn.SetDeadline(deadline)
	stopCancelWatch := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	err := exchangeSOCKS5Connect(conn, host, port)
	// stopCancelWatch reports false once the watch has fired: ctx ended
	// during the handshake and the deadline may already be in the past, so
	// the connection is not handed out even if the exchange completed.
	if !stopCancelWatch() {
		return fmt.Errorf("socks5: handshake: %w", context.Cause(ctx))
	}
	if err != nil {
		return err
	}
	// Clear the deadline; the caller will set its own.
	_ = conn.SetDeadline(time.Time{})
	return nil
}

// exchangeSOCKS5Connect writes the greeting and the CONNECT request for
// host:port and reads both replies, the bound address included, so the next
// byte on conn is the peer's.
func exchangeSOCKS5Connect(conn io.ReadWriter, host string, port int) error {
	// Greeting: VER=5, NMETHODS=1, METHOD=0x00 (no auth)
	// netcore-migration: §5.3.2 sub-NetCore exception. SOCKS5 handshake runs
	// against the dialed socket before any NetCore is constructed around it,
	// so the managed send path does not exist yet at this point.
	// scripts/enforce-netcore-boundary.sh pins the number of raw writes in
	// this file (two: this greeting and the CONNECT below).
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return fmt.Errorf("socks5: send greeting: %w", err)
	}

	// Server choice: VER=5, METHOD
	var greeting [2]byte
	if _, err := io.ReadFull(conn, greeting[:]); err != nil {
		return fmt.Errorf("socks5: read greeting reply: %w", err)
	}
	if greeting[0] != 0x05 || greeting[1] != 0x00 {
		return fmt.Errorf("socks5: unsupported auth method %d", greeting[1])
	}

	// CONNECT request: VER=5, CMD=1(connect), RSV=0, ATYP=3(domain)
	hostBytes := []byte(host)
	req := make([]byte, 0, 7+len(hostBytes))
	req = append(req, 0x05, 0x01, 0x00, 0x03, byte(len(hostBytes)))
	req = append(req, hostBytes...)
	req = append(req, byte(port>>8), byte(port&0xff))
	// netcore-migration: §5.3.2 sub-NetCore exception. Same rationale as the
	// greeting write above — SOCKS5 CONNECT request is part of proxy
	// handshake before the socket enters the managed transport layer.
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("socks5: send connect: %w", err)
	}

	// Reply: VER, REP, RSV, ATYP, BND.ADDR, BND.PORT
	var reply [4]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return fmt.Errorf("socks5: read connect reply: %w", err)
	}
	if reply[0] != 0x05 {
		return fmt.Errorf("socks5: unexpected version %d", reply[0])
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("socks5: connect failed with code %d", reply[1])
	}
	return skipSOCKS5BoundAddress(conn, reply[3])
}

// socks5BoundAddressLengths is the size of BND.ADDR per address type; a
// domain (0x03) carries its own length byte instead.
var socks5BoundAddressLengths = map[byte]int{
	0x01: 4,  // IPv4
	0x04: 16, // IPv6
}

// skipSOCKS5BoundAddress reads the bound address and port of the CONNECT
// reply. They are of no use to the caller, but left unread they would be
// taken for the first bytes the peer sent.
func skipSOCKS5BoundAddress(conn io.Reader, addressType byte) error {
	length, fixed := socks5BoundAddressLengths[addressType]
	if !fixed {
		if addressType != 0x03 {
			return fmt.Errorf("socks5: unknown bound address type %d", addressType)
		}
		var domainLength [1]byte
		if _, err := io.ReadFull(conn, domainLength[:]); err != nil {
			return fmt.Errorf("socks5: read bind domain length: %w", err)
		}
		length = int(domainLength[0])
	}
	skip := make([]byte, length+2)
	if _, err := io.ReadFull(conn, skip); err != nil {
		return fmt.Errorf("socks5: read bound address: %w", err)
	}
	return nil
}

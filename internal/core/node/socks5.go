package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// dialPeer connects to the given address, routing overlay addresses (.onion,
// .b32.i2p) through the SOCKS5 proxy configured in cfg.ProxyAddress.
func (s *Service) dialPeer(ctx context.Context, address domain.PeerAddress, timeout time.Duration) (net.Conn, error) {
	host, _, ok := splitHostPort(string(address))
	if !ok {
		return nil, fmt.Errorf("invalid peer address: %s", address)
	}

	if isOnionAddress(host) || isI2PAddress(host) {
		proxy := s.cfg.ProxyAddress
		if proxy == "" {
			return nil, fmt.Errorf("no SOCKS5 proxy configured for overlay address %s", address)
		}
		return dialSOCKS5(ctx, proxy, string(address), timeout)
	}

	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, "tcp", string(address))
}

// dialSOCKS5 establishes a TCP connection through a SOCKS5 proxy
// (RFC 1928) using the CONNECT command with no authentication.
// The target address is sent as a domain name (ATYP 0x03), which is
// what Tor expects for .onion resolution.
func dialSOCKS5(ctx context.Context, proxyAddr, targetAddr string, timeout time.Duration) (net.Conn, error) {
	host, portStr, ok := splitHostPort(targetAddr)
	if !ok {
		return nil, fmt.Errorf("socks5: invalid target address: %s", targetAddr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("socks5: invalid target port: %s", portStr)
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("socks5: connect to proxy %s: %w", proxyAddr, err)
	}

	deadline := time.Now().Add(timeout)
	_ = conn.SetDeadline(deadline)

	// Greeting: VER=5, NMETHODS=1, METHOD=0x00 (no auth)
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("socks5: send greeting: %w", err)
	}

	// Server choice: VER=5, METHOD
	var greeting [2]byte
	if _, err := io.ReadFull(conn, greeting[:]); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("socks5: read greeting reply: %w", err)
	}
	if greeting[0] != 0x05 || greeting[1] != 0x00 {
		_ = conn.Close()
		return nil, fmt.Errorf("socks5: unsupported auth method %d", greeting[1])
	}

	// CONNECT request: VER=5, CMD=1(connect), RSV=0, ATYP=3(domain)
	hostBytes := []byte(host)
	if len(hostBytes) > 255 {
		_ = conn.Close()
		return nil, errors.New("socks5: hostname too long")
	}
	req := make([]byte, 0, 7+len(hostBytes))
	req = append(req, 0x05, 0x01, 0x00, 0x03, byte(len(hostBytes)))
	req = append(req, hostBytes...)
	req = append(req, byte(port>>8), byte(port&0xff))
	if _, err := conn.Write(req); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("socks5: send connect: %w", err)
	}

	// Reply: VER, REP, RSV, ATYP, BND.ADDR, BND.PORT
	var reply [4]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("socks5: read connect reply: %w", err)
	}
	if reply[0] != 0x05 {
		_ = conn.Close()
		return nil, fmt.Errorf("socks5: unexpected version %d", reply[0])
	}
	if reply[1] != 0x00 {
		_ = conn.Close()
		return nil, fmt.Errorf("socks5: connect failed with code %d", reply[1])
	}

	// Drain the bound address to complete the handshake.
	switch reply[3] {
	case 0x01: // IPv4
		var skip [4 + 2]byte
		_, _ = io.ReadFull(conn, skip[:])
	case 0x04: // IPv6
		var skip [16 + 2]byte
		_, _ = io.ReadFull(conn, skip[:])
	case 0x03: // Domain
		var dlen [1]byte
		if _, err := io.ReadFull(conn, dlen[:]); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("socks5: read bind domain length: %w", err)
		}
		skip := make([]byte, int(dlen[0])+2)
		_, _ = io.ReadFull(conn, skip)
	default:
		// Unknown ATYP — try reading 6 bytes (IPv4+port) as fallback.
		var skip [6]byte
		_, _ = io.ReadFull(conn, skip[:])
	}

	// Clear the deadline; the caller will set its own.
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

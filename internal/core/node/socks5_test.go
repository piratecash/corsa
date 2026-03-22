package node

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/config"
)

// testV3Onion is a valid 56-char base32 onion host for use in SOCKS5/dial tests.
var testV3Onion = strings.Repeat("a", 56) + ".onion"

// mockSOCKS5Server accepts a single SOCKS5 CONNECT and then echoes back
// whatever the client writes. It provides a minimal RFC 1928 implementation
// sufficient for testing dialSOCKS5.
func mockSOCKS5Server(t *testing.T, listener net.Listener, wantHost string, wantPort int) {
	t.Helper()
	conn, err := listener.Accept()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Read greeting: VER, NMETHODS, METHODS...
	greeting := make([]byte, 3)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		t.Errorf("socks5 mock: read greeting: %v", err)
		return
	}
	if greeting[0] != 0x05 {
		t.Errorf("socks5 mock: unexpected version %d", greeting[0])
		return
	}

	// Reply: VER=5, METHOD=0 (no auth)
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		t.Errorf("socks5 mock: write greeting reply: %v", err)
		return
	}

	// Read CONNECT request: VER, CMD, RSV, ATYP, ...
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Errorf("socks5 mock: read connect header: %v", err)
		return
	}
	if header[1] != 0x01 {
		t.Errorf("socks5 mock: expected CMD=1, got %d", header[1])
		return
	}
	if header[3] != 0x03 {
		t.Errorf("socks5 mock: expected ATYP=3 (domain), got %d", header[3])
		return
	}

	// Domain length + domain + port
	dlenBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, dlenBuf); err != nil {
		t.Errorf("socks5 mock: read domain length: %v", err)
		return
	}
	domainAndPort := make([]byte, int(dlenBuf[0])+2)
	if _, err := io.ReadFull(conn, domainAndPort); err != nil {
		t.Errorf("socks5 mock: read domain+port: %v", err)
		return
	}
	domain := string(domainAndPort[:dlenBuf[0]])
	port := int(domainAndPort[dlenBuf[0]])<<8 | int(domainAndPort[dlenBuf[0]+1])

	if domain != wantHost {
		t.Errorf("socks5 mock: expected host %q, got %q", wantHost, domain)
	}
	if port != wantPort {
		t.Errorf("socks5 mock: expected port %d, got %d", wantPort, port)
	}

	// Reply: success with 0.0.0.0:0 bound address
	reply := []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	if _, err := conn.Write(reply); err != nil {
		t.Errorf("socks5 mock: write connect reply: %v", err)
		return
	}

	// Echo loop
	buf := make([]byte, 256)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		if _, err := conn.Write(buf[:n]); err != nil {
			return
		}
	}
}

func TestDialSOCKS5ConnectsAndRelaysData(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	go mockSOCKS5Server(t, listener, testV3Onion, 64646)

	ctx := context.Background()
	conn, err := dialSOCKS5(ctx, listener.Addr().String(), testV3Onion+":64646", 3*time.Second)
	if err != nil {
		t.Fatalf("dialSOCKS5: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Write and read-back through the echo mock.
	msg := []byte("hello onion")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "hello onion" {
		t.Fatalf("unexpected echo: %q", buf)
	}
}

func TestDialSOCKS5RejectsInvalidTarget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, err := dialSOCKS5(ctx, "127.0.0.1:1", "no-port", 1*time.Second)
	if err == nil {
		t.Fatal("expected error for invalid target address")
	}
}

func TestDialSOCKS5FailsWhenProxyUnreachable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, err := dialSOCKS5(ctx, "127.0.0.1:1", testV3Onion+":64646", 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for unreachable proxy")
	}
}

func TestDialPeerRoutesOnionThroughSOCKS5(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	go mockSOCKS5Server(t, listener, testV3Onion, 64646)

	svc := &Service{
		cfg: config.Node{
			ProxyAddress: listener.Addr().String(),
		},
	}

	ctx := context.Background()
	conn, err := svc.dialPeer(ctx, testV3Onion+":64646", 3*time.Second)
	if err != nil {
		t.Fatalf("dialPeer .onion: %v", err)
	}
	_ = conn.Close()
}

func TestDialPeerRejectsOnionWithoutProxy(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg: config.Node{
			ProxyAddress: "",
		},
	}

	ctx := context.Background()
	_, err := svc.dialPeer(ctx, testV3Onion+":64646", 1*time.Second)
	if err == nil {
		t.Fatal("expected error for .onion without proxy")
	}
}

func TestDialPeerUsesDirectTCPForRegularAddress(t *testing.T) {
	t.Parallel()

	// Set up a simple TCP echo server.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 128)
		n, _ := conn.Read(buf)
		_, _ = conn.Write(buf[:n])
	}()

	svc := &Service{
		cfg: config.Node{
			ProxyAddress: "127.0.0.1:1", // proxy set but shouldn't be used
		},
	}

	ctx := context.Background()
	conn, err := svc.dialPeer(ctx, listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dialPeer direct: %v", err)
	}
	defer func() { _ = conn.Close() }()

	msg := []byte("ping")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("unexpected echo: %q", buf)
	}
}

func TestDialPeerRejectsInvalidAddress(t *testing.T) {
	t.Parallel()

	svc := &Service{cfg: config.Node{}}

	ctx := context.Background()
	_, err := svc.dialPeer(ctx, "no-port-here", 1*time.Second)
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

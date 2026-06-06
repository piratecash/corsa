package debugserver

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestStart_EmptyAddrIsNoop(t *testing.T) {
	t.Parallel()
	shutdown, err := Start("")
	if err != nil {
		t.Fatalf("empty addr must be a no-op, got err: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown must be non-nil even for the no-op case")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("no-op shutdown must succeed, got: %v", err)
	}
}

func TestStart_RejectsNonLoopbackPreflight(t *testing.T) {
	t.Parallel()
	// These are rejected BEFORE any socket is opened (preflight): an
	// empty host binds all interfaces, IP literals are checked directly.
	cases := []string{
		"0.0.0.0:6060",      // all interfaces
		":6060",             // empty host = all interfaces
		"8.8.8.8:6060",      // public IP
		"192.168.1.10:6060", // LAN IP (not loopback)
	}
	for _, addr := range cases {
		shutdown, err := Start(addr)
		if err == nil {
			t.Errorf("Start(%q) must reject a non-loopback bind, got nil error", addr)
		}
		if shutdown != nil {
			_ = shutdown(context.Background())
		}
	}
}

// fakeListener is a net.Listener stub whose Addr() returns a
// caller-chosen address — used to drive verifyBoundLoopback past the
// resolver (which cannot be coerced to map a name off-loopback in CI).
type fakeListener struct{ addr net.Addr }

func (f fakeListener) Accept() (net.Conn, error) { return nil, errors.New("not implemented") }
func (f fakeListener) Close() error              { return nil }
func (f fakeListener) Addr() net.Addr            { return f.addr }

// TestVerifyBoundLoopback pins the AUTHORITATIVE post-bind guard
// directly — the resolver-independent check that catches a hostname
// (e.g. "localhost") which the system mapped onto a non-loopback
// interface. preflightLoopback cannot see that case (it trusts the
// hostname pre-bind), so the guard is the real protection and must be
// tested on its own, not through Start (where an IP literal would be
// short-circuited by preflight first).
func TestVerifyBoundLoopback(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		ip     net.IP
		reject bool
	}{
		{"ipv4 loopback", net.IPv4(127, 0, 0, 1), false},
		{"ipv6 loopback", net.IPv6loopback, false},
		{"public ipv4", net.IPv4(8, 8, 8, 8), true},
		{"lan ipv4", net.IPv4(192, 168, 1, 10), true},
		{"unspecified ipv4", net.IPv4zero, true}, // 0.0.0.0 = all interfaces
	}
	for _, c := range cases {
		ln := fakeListener{addr: &net.TCPAddr{IP: c.ip, Port: 6060}}
		err := verifyBoundLoopback(ln)
		if c.reject && err == nil {
			t.Errorf("%s (%s): expected rejection, got nil", c.name, c.ip)
		}
		if !c.reject && err != nil {
			t.Errorf("%s (%s): expected accept, got %v", c.name, c.ip, err)
		}
	}

	// Non-TCP address is rejected (defensive — Start always binds TCP).
	if err := verifyBoundLoopback(fakeListener{addr: &net.UnixAddr{Name: "/tmp/x", Net: "unix"}}); err == nil {
		t.Error("non-TCP listener address must be rejected")
	}
}

func TestStart_LoopbackAcceptsAndServes(t *testing.T) {
	t.Parallel()
	// Port 0 → OS picks a free loopback port, so the test never collides
	// with a real listener. "localhost" is included to prove the
	// post-bind guard accepts a hostname that DOES resolve to loopback.
	for _, addr := range []string{"127.0.0.1:0", "localhost:0", "[::1]:0"} {
		shutdown, err := Start(addr)
		if err != nil {
			t.Fatalf("Start(%q) on loopback must succeed, got: %v", addr, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := shutdown(ctx); err != nil {
			t.Fatalf("Start(%q) shutdown must be clean, got: %v", addr, err)
		}
		cancel()
	}
}

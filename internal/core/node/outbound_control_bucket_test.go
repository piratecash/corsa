package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// outbound_control_bucket_test.go pins who drops an outbound session's
// control-frame bucket (cmdLimiter key "outbound:<connID>"): the goroutine
// that dispatched the session's frames, on every way the session ends. Each
// test charges the bucket the way production does and asserts nothing of it
// is left once the session's goroutine is done.

// commandLimiterBucketCount reports how many buckets the limiter holds. The
// legacy dial mints its connID internally, so its test cannot name the key.
func commandLimiterBucketCount(rl *commandRateLimiter) int {
	rl.buckets.mu.Lock()
	defer rl.buckets.mu.Unlock()
	return len(rl.buckets.buckets)
}

// legacyResyncThenRefusalScript is the remote half of a legacy dial. It
// completes the handshake and answers the first sync request with a
// request_resync — which the dialler charges to the session's control-frame
// bucket — followed by a ping. The dialler handles both on one goroutine, in
// order, so its pong proves the request_resync was dispatched: the script
// counts the buckets at that moment, while the session is still alive, then
// sends the error that ends the sync and waits for the dialler to close the
// socket, so the test resumes only after openPeerSession is done with it.
type legacyResyncThenRefusalScript struct {
	welcome string
	authOK  string
	resync  string
	ping    string
	refusal string
	// bucketsNow counts the dialler's command-limiter buckets.
	bucketsNow func() int
}

func (script legacyResyncThenRefusalScript) run(ln net.Listener) (bucketsWhileAlive int, err error) {
	conn, err := ln.Accept()
	if err != nil {
		return 0, fmt.Errorf("accept: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		return 0, fmt.Errorf("set deadline: %w", err)
	}
	reader := bufio.NewReader(conn)
	steps := []struct {
		expectLine string
		write      string
	}{
		{expectLine: "hello", write: script.welcome},
		{expectLine: "auth_session", write: script.authOK},
		{expectLine: "the first sync request", write: script.resync + script.ping},
	}
	for _, step := range steps {
		if _, err := reader.ReadString('\n'); err != nil {
			return 0, fmt.Errorf("read %s: %w", step.expectLine, err)
		}
		if _, err := conn.Write([]byte(step.write)); err != nil {
			return 0, fmt.Errorf("write after %s: %w", step.expectLine, err)
		}
	}
	// Lines the dialler writes before the pong are not ours to judge; the
	// connection deadline bounds the wait.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return 0, fmt.Errorf("read until the pong: %w", err)
		}
		if strings.Contains(line, `"pong"`) {
			break
		}
	}
	bucketsWhileAlive = script.bucketsNow()
	if _, err := conn.Write([]byte(script.refusal)); err != nil {
		return 0, fmt.Errorf("write refusal: %w", err)
	}
	return bucketsWhileAlive, awaitDiallerClose(reader)
}

// awaitDiallerClose reads until the dialler has closed the connection. A close
// arrives as a FIN (io.EOF) or, when unread bytes were left in the dialler's
// buffer, as an RST (ECONNRESET); both mean the dialler is done.
func awaitDiallerClose(reader *bufio.Reader) error {
	for {
		_, err := reader.ReadString('\n')
		switch {
		case err == nil:
			continue
		case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed), errors.Is(err, syscall.ECONNRESET):
			return nil
		default:
			return fmt.Errorf("wait for the dialler to close: %w", err)
		}
	}
}

func mustFrameLine(t *testing.T, frame protocol.Frame) string {
	t.Helper()
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("marshal %s: %v", frame.Type, err)
	}
	return line
}

// A legacy session ends inside openPeerSession, on an error as here or on a
// cancelled context through the same return, and the bucket its frames
// charged ends with it. The drop belongs to the goroutine that dispatched the
// session's frames and is keyed by the session's own connID, one owner per
// path, the same rule the CM path follows; it used to sit in two places
// outside that goroutine, which reached the connID only through the
// session-map entry.
func TestOpenPeerSession_DropsTheControlFrameBucketItsSessionCharged(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	asLegacyOnlyDialler(svc)
	remote, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	script := legacyResyncThenRefusalScript{
		welcome: mustFrameLine(t, protocol.Frame{
			Type:                   "welcome",
			Version:                config.ProtocolVersion,
			MinimumProtocolVersion: config.MinimumProtocolVersion,
			Address:                remote.Address,
			Challenge:              "corsa-control-bucket-legacy",
			// mesh_routing_v2 is what admits request_resync to the
			// control-frame branch that charges the bucket.
			Capabilities: []string{string(domain.CapMeshRoutingV1), string(domain.CapMeshRoutingV2)},
		}),
		authOK:     mustFrameLine(t, protocol.Frame{Type: "auth_ok"}),
		resync:     mustFrameLine(t, protocol.Frame{Type: "request_resync"}),
		ping:       mustFrameLine(t, protocol.Frame{Type: "ping"}),
		refusal:    mustFrameLine(t, protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand}),
		bucketsNow: func() int { return commandLimiterBucketCount(svc.cmdLimiter) },
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	type scriptResult struct {
		bucketsWhileAlive int
		err               error
	}
	scriptDone := make(chan scriptResult, 1)
	go func() {
		buckets, err := script.run(ln)
		scriptDone <- scriptResult{bucketsWhileAlive: buckets, err: err}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := svc.openPeerSession(ctx, domain.PeerAddress(ln.Addr().String())); err == nil {
		t.Fatal("the scripted peer refuses the sync: openPeerSession must surface the error")
	}
	result := <-scriptDone
	if result.err != nil {
		t.Fatalf("scripted peer: %v", result.err)
	}
	if result.bucketsWhileAlive != 1 {
		t.Fatalf("precondition: the live session holds %d command buckets, want 1 — the request_resync it dispatched must have charged its bucket", result.bucketsWhileAlive)
	}

	if got := commandLimiterBucketCount(svc.cmdLimiter); got != 0 {
		t.Fatalf("command limiter holds %d buckets after openPeerSession returned, want 0: the session's control-frame bucket outlived it", got)
	}
}

// A CM session whose setup failed is not published, so no session-map
// removal ever sees it; frames dispatched during setup may already have
// charged its bucket, and the failure path drops it.
func TestFailCMSessionSetup_DropsTheControlFrameBucket(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const addr = domain.PeerAddress("10.0.0.8:64646")
	session := &peerSession{address: addr, connID: 4343}
	bucketKey := outboundControlFrameLimitKey(session.connID)

	svc.outboundControlFrameAllowed(session)
	if !commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("precondition: a setup-time control frame charges the bucket")
	}
	svc.failCMSessionSetup(session, addr, 1, io.ErrUnexpectedEOF)

	if commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("the control-frame bucket of a session whose setup failed outlived it")
	}
}

// The same for a session the ConnectionManager evicted while its setup ran.
func TestAbortLocallyEvictedCMSession_DropsTheControlFrameBucket(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const addr = domain.PeerAddress("10.0.0.9:64646")
	session := &peerSession{address: addr, connID: 4444}
	bucketKey := outboundControlFrameLimitKey(session.connID)

	svc.outboundControlFrameAllowed(session)
	if !commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("precondition: a setup-time control frame charges the bucket")
	}
	svc.abortLocallyEvictedCMSession(session, addr, nil)

	if commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("the control-frame bucket of a session evicted during setup outlived it")
	}
}

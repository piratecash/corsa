package node

import (
	"bufio"
	"context"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/transport"
)

// newSyncPeerTestService creates a minimal Service for testing syncPeer().
// It populates all maps that syncPeer touches to avoid nil-pointer panics.
func newSyncPeerTestService(aggregateStatus domain.NetworkStatus) *Service {
	id := &identity.Identity{
		Address: "test-node-address",
	}
	return &Service{
		identity:      id,
		cfg:           config.Node{AllowPrivatePeers: true},
		runCtx:        context.Background(),
		health:        make(map[domain.PeerAddress]*peerHealth),
		pending:       make(map[domain.PeerAddress][]pendingFrame),
		peers:         []transport.Peer{},
		known:         make(map[string]struct{}),
		boxKeys:       make(map[string]string),
		pubKeys:       make(map[string]string),
		boxSigs:       make(map[string]string),
		peerTypes:     make(map[domain.PeerAddress]domain.NodeType),
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerVersions:  make(map[domain.PeerAddress]string),
		peerBuilds:    make(map[domain.PeerAddress]int),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		dialOrigin:    make(map[domain.PeerAddress]domain.PeerAddress),
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
		observedAddrs: make(map[domain.PeerIdentity]string),
		aggregateStatus: domain.AggregateStatusSnapshot{
			Status: aggregateStatus,
		},
	}
}

// syncPeerMockServer accepts a single TCP connection and simulates a minimal
// peer that responds to the syncPeer() handshake. It sends a welcome frame
// (no auth challenge), then responds to get_peers and fetch_contacts.
//
// Returns the ordered list of frame types received from the client.
func syncPeerMockServer(t *testing.T, ln net.Listener, respondPeers []string) []string {
	t.Helper()
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("mock server accept failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	var received []string

	// Read hello from client.
	helloLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("mock server: failed to read hello: %v", err)
	}
	helloFrame, err := protocol.ParseFrameLine(strings.TrimSpace(helloLine))
	if err != nil {
		t.Fatalf("mock server: failed to parse hello: %v", err)
	}
	received = append(received, helloFrame.Type)

	// Send welcome (no challenge — no auth required).
	welcome, _ := protocol.MarshalFrameLine(protocol.Frame{
		Type: "welcome",
	})
	if _, err := conn.Write([]byte(welcome)); err != nil {
		t.Fatalf("mock server: failed to write welcome: %v", err)
	}

	// Read and respond to subsequent requests until client closes.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		frame, err := protocol.ParseFrameLine(line)
		if err != nil {
			t.Logf("mock server: parse error: %v (line: %q)", err, line)
			continue
		}
		received = append(received, frame.Type)

		switch frame.Type {
		case "get_peers":
			resp, _ := protocol.MarshalFrameLine(protocol.Frame{
				Type:  "peers",
				Peers: respondPeers,
			})
			_, _ = conn.Write([]byte(resp))
		case "fetch_contacts":
			resp, _ := protocol.MarshalFrameLine(protocol.Frame{
				Type:     "contacts",
				Contacts: []protocol.ContactFrame{},
			})
			_, _ = conn.Write([]byte(resp))
		}
	}

	return received
}

// TestSyncPeer_RequestPeersTrue_SendsGetPeers verifies that when the caller
// passes requestPeers=true, syncPeer sends get_peers followed by
// fetch_contacts and adds discovered peer addresses. This is the path a
// future bootstrap/recovery caller would take; forced-refresh sender-key
// recovery paths pass false (see Step 5) and are covered separately.
func TestSyncPeer_RequestPeersTrue_SendsGetPeers(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	discoveredPeers := []string{"10.0.0.100:9000", "10.0.0.101:9000"}

	receivedCh := make(chan []string, 1)
	go func() {
		receivedCh <- syncPeerMockServer(t, ln, discoveredPeers)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	imported := svc.syncPeer(ctx, peerAddr, true)

	received := <-receivedCh

	// Expect: hello, get_peers, fetch_contacts.
	expectedTypes := []string{"hello", "get_peers", "fetch_contacts"}
	if len(received) != len(expectedTypes) {
		t.Fatalf("expected %d requests %v, got %d: %v",
			len(expectedTypes), expectedTypes, len(received), received)
	}
	for i, exp := range expectedTypes {
		if received[i] != exp {
			t.Errorf("request[%d]: expected %q, got %q", i, exp, received[i])
		}
	}

	// Verify peers were added via addPeerAddress.
	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != len(discoveredPeers) {
		t.Errorf("expected %d peers added, got %d", len(discoveredPeers), peerCount)
	}

	// imported reflects contacts (none from our mock), not peers.
	if imported != 0 {
		t.Errorf("expected 0 imported contacts (mock returns none), got %d", imported)
	}
}

// TestSyncPeer_RequestPeersFalse_SkipsGetPeers verifies that when the caller
// passes requestPeers=false, syncPeer skips get_peers but still sends
// fetch_contacts. This is the contract relied on by forced-refresh
// sender-key recovery paths (Step 5), where peer exchange must never be
// triggered regardless of aggregate status.
func TestSyncPeer_RequestPeersFalse_SkipsGetPeers(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Aggregate status deliberately set to Offline to prove that the
	// requestPeers=false override wins over the "not healthy" signal that
	// shouldRequestPeers() would otherwise return.
	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	receivedCh := make(chan []string, 1)
	go func() {
		receivedCh <- syncPeerMockServer(t, ln, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	imported := svc.syncPeer(ctx, peerAddr, false)

	received := <-receivedCh

	// Expect: hello, fetch_contacts — NO get_peers.
	expectedTypes := []string{"hello", "fetch_contacts"}
	if len(received) != len(expectedTypes) {
		t.Fatalf("expected %d requests %v, got %d: %v",
			len(expectedTypes), expectedTypes, len(received), received)
	}
	for i, exp := range expectedTypes {
		if received[i] != exp {
			t.Errorf("request[%d]: expected %q, got %q", i, exp, received[i])
		}
	}

	// Verify no peers were added (get_peers was skipped).
	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 0 {
		t.Errorf("expected 0 peers (get_peers skipped), got %d", peerCount)
	}

	// imported should still be 0 (mock returns empty contacts).
	if imported != 0 {
		t.Errorf("expected 0 imported contacts, got %d", imported)
	}
}

// TestSyncPeer_SkipGetPeers_FetchContactsStillWorks is a regression test
// ensuring that skipping get_peers (requestPeers=false) does not abort
// syncPeer() before fetch_contacts. The legacy code had an `else { return 0 }`
// branch on MarshalFrameLine error that would prematurely exit; with the skip
// path wrapping the entire get_peers block, that branch is only reachable
// when get_peers is actually attempted.
func TestSyncPeer_SkipGetPeers_FetchContactsStillWorks(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	svc := newSyncPeerTestService(domain.NetworkStatusHealthy)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	receivedCh := make(chan []string, 1)
	go func() {
		receivedCh <- syncPeerMockServer(t, ln, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	svc.syncPeer(ctx, peerAddr, false)

	received := <-receivedCh

	// The critical assertion: fetch_contacts must appear even though
	// get_peers was skipped.
	hasFetchContacts := false
	for _, r := range received {
		if r == "fetch_contacts" {
			hasFetchContacts = true
		}
	}
	if !hasFetchContacts {
		t.Fatalf("fetch_contacts was not sent after policy skip of get_peers; received: %v", received)
	}
}

// TestSyncPeer_SkipGetPeers_NoAddPeerAddress verifies that when the caller
// requests requestPeers=false, addPeerAddress is NOT called. The peer set
// should continue to grow only through announce_peer and other discovery
// mechanisms. This is a key invariant for forced-refresh sender-key
// recovery paths (Step 5).
func TestSyncPeer_SkipGetPeers_NoAddPeerAddress(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Aggregate status set to Offline to prove the requestPeers=false
	// override wins: even a "not healthy" node must not leak peer exchange
	// out of a sender-key recovery dial.
	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	receivedCh := make(chan []string, 1)
	go func() {
		// Even though the mock server is ready to respond with peers,
		// the client should never send get_peers.
		receivedCh <- syncPeerMockServer(t, ln, []string{"10.0.0.200:9000"})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	svc.syncPeer(ctx, peerAddr, false)

	<-receivedCh

	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 0 {
		t.Errorf("expected 0 peers (get_peers was skipped), got %d", peerCount)
	}
}

// TestSyncPeer_PolicyReturnCount verifies that syncPeer returns the correct
// imported contact count regardless of the requestPeers decision. The return
// value must always reflect contacts imported via fetch_contacts, not peers
// discovered via get_peers.
func TestSyncPeer_PolicyReturnCount(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	receivedCh := make(chan []string, 1)
	go func() {
		receivedCh <- syncPeerMockServer(t, ln, []string{"10.0.0.50:9000"})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	imported := svc.syncPeer(ctx, peerAddr, true)
	<-receivedCh

	// Mock returns empty contacts, so imported should be 0.
	// The key assertion: return value is about contacts, not peers.
	if imported != 0 {
		t.Errorf("expected imported=0 (no contacts from mock), got %d", imported)
	}
}

// TestSyncSenderKeys_FreshDialFallback_SkipsGetPeers is the core Step 5
// regression test. It verifies that the fresh-dial fallback in
// syncSenderKeys never triggers peer exchange, even when the aggregate
// status would otherwise allow it (NetworkStatusOffline). Sender-key
// recovery is a narrow contact/key sync; escalating it into eager peer
// exchange on a private failure was the historical behaviour in Step 5
// removes.
//
// The test also verifies that the call site does not call addPeerAddress,
// so the peer set is not polluted by the recovery path.
func TestSyncSenderKeys_FreshDialFallback_SkipsGetPeers(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Aggregate status = Offline proves that the narrow override wins over
	// the shouldRequestPeers() signal that would otherwise say "yes".
	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	receivedCh := make(chan []string, 1)
	go func() {
		// If the fallback path leaked get_peers, the mock would answer
		// with these peers and the test would see them in svc.peers.
		receivedCh <- syncPeerMockServer(t, ln, []string{"10.0.0.250:9000"})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// nil syncSession forces the fresh-dial fallback branch.
	_ = svc.syncSenderKeys(ctx, peerAddr, nil)

	received := <-receivedCh

	// Expect: hello, fetch_contacts — NO get_peers.
	expectedTypes := []string{"hello", "fetch_contacts"}
	if len(received) != len(expectedTypes) {
		t.Fatalf("expected %d requests %v, got %d: %v",
			len(expectedTypes), expectedTypes, len(received), received)
	}
	for i, exp := range expectedTypes {
		if received[i] != exp {
			t.Errorf("request[%d]: expected %q, got %q", i, exp, received[i])
		}
	}

	// Regression: addPeerAddress must not be called — the recovery path
	// is narrow contact/key sync, not a discovery event.
	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 0 {
		t.Errorf("expected 0 peers (recovery path must not discover), got %d", peerCount)
	}
}

// TestSyncSenderKeys_FreshDialFallback_RespectsCtxCancel is a lifecycle
// regression: when the owning ctx is already cancelled, the fallback
// fresh-dial must bail out promptly instead of using a locally-created
// context.Background() that would ignore shutdown. This enforces the
// Step 5 rule against synthesising root contexts in business logic
// (CLAUDE.md: context.Context is passed as the first argument).
func TestSyncSenderKeys_FreshDialFallback_RespectsCtxCancel(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	// Pre-cancelled ctx: dialPeer must observe cancellation and abort.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	imported := svc.syncSenderKeys(ctx, peerAddr, nil)
	elapsed := time.Since(start)

	if imported != 0 {
		t.Errorf("expected imported=0 on cancelled ctx, got %d", imported)
	}
	// The dial must fail fast; syncHandshakeTimeout is measured in seconds,
	// so anything close to it would indicate the ctx was swallowed.
	if elapsed >= syncHandshakeTimeout {
		t.Errorf("expected prompt cancellation, took %v (timeout=%v)",
			elapsed, syncHandshakeTimeout)
	}
}

// TestSyncPeer_BootstrapNetCoreDoesNotLeakWriterGoroutine verifies the
// single-writer invariant for the migrated syncPeer path: each invocation
// creates a bootstrap NetCore with a writer goroutine, and pc.Close() must
// drain/terminate that goroutine before returning. A leak would manifest as
// goroutine count growing linearly with the iteration count.
//
// This is the regression lock on the syncPeer bootstrap path: if a future
// refactor replaces pc.Close() with a raw conn.Close() or forgets the
// defer, sendCh never closes, writerLoop never exits, and this test
// catches it deterministically.
func TestSyncPeer_BootstrapNetCoreDoesNotLeakWriterGoroutine(t *testing.T) {
	// Intentionally not t.Parallel — goroutine counting is a global signal.

	const iterations = 20

	// Settle any goroutines left over from previous tests in this package.
	for i := 0; i < 5; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	baseline := runtime.NumGoroutine()

	for i := 0; i < iterations; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("iter %d: failed to start listener: %v", i, err)
		}

		svc := newSyncPeerTestService(domain.NetworkStatusOffline)
		peerAddr := domain.PeerAddress(ln.Addr().String())

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = syncPeerMockServer(t, ln, nil)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		svc.syncPeer(ctx, peerAddr, false)
		cancel()

		_ = ln.Close()
		<-done
	}

	// Allow any in-flight writerLoop goroutines to observe close(sendCh)
	// and exit. pc.Close() already waits on writerDone, so this is a
	// belt-and-braces settle window against accept goroutines etc.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	final := runtime.NumGoroutine()
	// Small tolerance for unrelated runtime goroutines (GC, netpoll noise).
	if final > baseline+5 {
		t.Errorf("goroutine leak: baseline=%d final=%d iterations=%d (delta=%d)",
			baseline, final, iterations, final-baseline)
	}
}

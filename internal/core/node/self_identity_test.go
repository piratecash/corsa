package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestInboundHello_SelfIdentity_EmitsPeerBannedNotice reproduces the
// self-loopback defence at the inbound hello path: when a remote hello
// carries the local node's own Ed25519 address we must answer with a
// connection_notice{peer-banned, reason=self-identity} AND return
// accepted=false so handleConn tears down the inbound connection
// through the Network wrapper (never a raw conn.Close).
//
// Without the guard the same hello would proceed into connauth.PrepareAuth
// and either (a) fail the box-key binding check and emit a generic
// invalid-auth-signature error, or (b) on a crafted but structurally
// valid hello slip through and contaminate rememberConnPeerAddr with
// our own identity keyed by the reflected socket address. Either
// outcome leads to a reconnect loop against the local endpoint; the
// self-identity notice is the only wire form that steers the dialler
// into the address cooldown.
func TestInboundHello_SelfIdentity_EmitsPeerBannedNotice(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(9500)
	backend.Register(connID, netcore.Inbound, "10.0.0.42:64646")

	// Craft a hello whose Address field matches the local identity. The
	// PubKey / BoxKey / BoxSig values are filler — the guard runs BEFORE
	// connauth.PrepareAuth so the cryptographic box-key binding is never
	// checked on this path. That is deliberate: a valid binding is
	// impossible when the remote is actually us (we would need our own
	// private key on the remote side), so gating the guard on a crypto
	// pre-check would make it unreachable in production.
	//
	// Listen is populated because the guard is gated on it: subscribers
	// (local RPC clients using the node identity for self-subscription)
	// never advertise Listen, and the guard must not break that flow.
	// Every genuine self-loopback path surfaces Listen because it
	// originates from our own outbound dialler, which always populates
	// the field; this test reproduces that wire shape.
	helloFrame := protocol.Frame{
		Type:    "hello",
		Version: config.ProtocolVersion,
		Client:  "node",
		Address: svc.identity.Address,
		Listen:  "198.51.100.7:64646",
		PubKey:  "filler-pubkey",
		BoxKey:  "filler-boxkey",
		BoxSig:  "filler-boxsig",
	}
	helloLine, err := protocol.MarshalFrameLine(helloFrame)
	if err != nil {
		t.Fatalf("MarshalFrameLine hello: %v", err)
	}

	if ok := svc.dispatchNetworkFrame(connID, helloLine); ok {
		t.Fatalf("dispatchNetworkFrame(self-identity hello) returned true; expected accepted=false so handleConn tears down the socket via the Network wrapper")
	}

	select {
	case data, open := <-backend.Outbound(connID):
		if !open {
			t.Fatal("backend.Outbound(connID) closed before self-identity notice arrived")
		}
		frame, err := parseFrameLineForTest(data)
		if err != nil {
			t.Fatalf("parse notice frame: %v (raw=%q)", err, data)
		}
		if frame.Type != protocol.FrameTypeConnectionNotice {
			t.Fatalf("expected %q, got type=%q (raw=%q)", protocol.FrameTypeConnectionNotice, frame.Type, data)
		}
		if frame.Code != protocol.ErrCodePeerBanned {
			t.Fatalf("expected code=%q, got %q", protocol.ErrCodePeerBanned, frame.Code)
		}
		details, _, err := protocol.ParsePeerBannedDetails(frame.Details)
		if err != nil {
			t.Fatalf("parse peer-banned details: %v (raw=%s)", err, string(frame.Details))
		}
		if details.Reason != protocol.PeerBannedReasonSelfIdentity {
			t.Fatalf("expected reason=%q, got %q", protocol.PeerBannedReasonSelfIdentity, details.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for self-identity notice on backend.Outbound(connID)")
	}
}

// TestLearnIdentityFromWelcome_SelfIdentity_SkipsIngest asserts that the
// defence-in-depth guard inside learnIdentityFromWelcome drops a welcome
// whose Address matches the local identity. Without the guard, the peer
// caches (knownIdentities, boxKeys, pubKeys, boxSigs) would ingest our
// own key material and promotePeerAddress would attach our listen
// address to our own peer list — creating a spurious self-loop dial
// candidate on every future bootstrap tick.
func TestLearnIdentityFromWelcome_SelfIdentity_SkipsIngest(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	selfAddr := svc.identity.Address
	welcome := protocol.Frame{
		Type:     "welcome",
		Version:  config.ProtocolVersion,
		Client:   "node",
		Address:  selfAddr,
		Listen:   "198.51.100.7:64646",
		Listener: "1",
		PubKey:   "filler-pubkey",
		BoxKey:   "filler-boxkey",
		BoxSig:   "filler-boxsig",
	}

	// Service constructor seeds its own identity caches (known, boxKeys,
	// pubKeys, boxSigs) so the node can sign its own advertisements and
	// verify self-references in gossip. We cannot assert "map is absent":
	// we must assert "guarded call did not MUTATE the pre-existing
	// entries with the filler values from the foreign welcome frame".
	// Snapshot the pre-state and verify equality after the call. Any
	// ingest would either overwrite a field with "filler-*" or insert
	// a new listen binding, both caught by the comparison.
	// s.known, s.boxKeys, s.pubKeys, s.boxSigs all live under s.knowledgeMu.
	svc.knowledgeMu.RLock()
	_, knownHad := svc.known[selfAddr]
	boxKeysBefore, boxKeysHad := svc.boxKeys[selfAddr]
	pubKeysBefore, pubKeysHad := svc.pubKeys[selfAddr]
	boxSigsBefore, boxSigsHad := svc.boxSigs[selfAddr]
	svc.knowledgeMu.RUnlock()

	svc.learnIdentityFromWelcome(welcome)

	svc.knowledgeMu.RLock()
	defer svc.knowledgeMu.RUnlock()

	// known is map[string]struct{} — only presence can change under the
	// guarded call. A flipped presence bit is the tell-tale of ingest.
	_, knownHasNow := svc.known[selfAddr]
	if knownHasNow != knownHad {
		t.Fatalf("known[selfAddr] presence flipped by guarded learnIdentityFromWelcome: before=%v, after=%v", knownHad, knownHasNow)
	}

	boxKeysAfter, boxKeysHasNow := svc.boxKeys[selfAddr]
	if boxKeysHasNow != boxKeysHad {
		t.Fatalf("boxKeys[selfAddr] presence flipped: before=%v, after=%v", boxKeysHad, boxKeysHasNow)
	}
	if boxKeysHad && boxKeysBefore != boxKeysAfter {
		t.Fatalf("boxKeys[selfAddr] mutated: before=%q, after=%q", boxKeysBefore, boxKeysAfter)
	}
	if boxKeysHasNow && boxKeysAfter == "filler-boxkey" {
		t.Fatal("boxKeys[selfAddr] overwritten with welcome filler material; guard failed")
	}

	pubKeysAfter, pubKeysHasNow := svc.pubKeys[selfAddr]
	if pubKeysHasNow != pubKeysHad {
		t.Fatalf("pubKeys[selfAddr] presence flipped: before=%v, after=%v", pubKeysHad, pubKeysHasNow)
	}
	if pubKeysHad && pubKeysBefore != pubKeysAfter {
		t.Fatalf("pubKeys[selfAddr] mutated: before=%q, after=%q", pubKeysBefore, pubKeysAfter)
	}
	if pubKeysHasNow && pubKeysAfter == "filler-pubkey" {
		t.Fatal("pubKeys[selfAddr] overwritten with welcome filler material; guard failed")
	}

	boxSigsAfter, boxSigsHasNow := svc.boxSigs[selfAddr]
	if boxSigsHasNow != boxSigsHad {
		t.Fatalf("boxSigs[selfAddr] presence flipped: before=%v, after=%v", boxSigsHad, boxSigsHasNow)
	}
	if boxSigsHad && boxSigsBefore != boxSigsAfter {
		t.Fatalf("boxSigs[selfAddr] mutated: before=%q, after=%q", boxSigsBefore, boxSigsAfter)
	}
	if boxSigsHasNow && boxSigsAfter == "filler-boxsig" {
		t.Fatal("boxSigs[selfAddr] overwritten with welcome filler material; guard failed")
	}
}

// TestOnCMDialFailed_SelfIdentityError_AppliesCooldown asserts that a
// structured selfIdentityError surfaced by openPeerSessionForCM is
// converted into an address-level cooldown by onCMDialFailed. Without
// this path the error would fall through to markPeerDisconnected, which
// applies a small transient penalty and leaves the address ready to be
// redialled immediately — directly contradicting the whole point of
// detecting the self-loopback.
func TestOnCMDialFailed_SelfIdentityError_AppliesCooldown(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	addr := domain.PeerAddress("198.51.100.7:64646")
	selfErr := &selfIdentityError{
		Address:       addr,
		PeerListen:    addr,
		LocalIdentity: domain.PeerIdentity(svc.identity.Address),
	}

	// Sanity on error discrimination: both errors.As(structured) and
	// errors.Is(sentinel) must succeed so the penalty path cannot be
	// bypassed by the fmt.Errorf("%w: ...") wrapping pattern other call
	// sites already use.
	var casted *selfIdentityError
	if !errors.As(selfErr, &casted) {
		t.Fatal("errors.As(selfIdentityError) failed")
	}
	if !errors.Is(selfErr, errSelfIdentity) {
		t.Fatal("errors.Is(errSelfIdentity) failed — Unwrap() missing from selfIdentityError")
	}

	svc.onCMDialFailed(addr, selfErr, false)

	svc.peerMu.RLock()
	health, ok := svc.health[addr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("onCMDialFailed did not create a peerHealth entry for %s", addr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatalf("BannedUntil not set after self-identity failure; expected non-zero cooldown")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("health.LastErrorCode = %q, want %q", got, want)
	}
	if health.LastError == "" {
		t.Fatal("health.LastError is empty; expected selfIdentityError message")
	}
	minExpected := peerBanSelfIdentity - time.Minute
	if time.Until(health.BannedUntil) < minExpected {
		t.Fatalf("BannedUntil too short: got %v until, want ≥ %v", time.Until(health.BannedUntil), minExpected)
	}
}

// TestOnCMDialFailed_WireSentinelSelfIdentity_AppliesCooldown covers the
// sibling arrival shape: a connection_notice{peer-banned,
// reason=self-identity} frame from the responder must decode through
// protocol.NoticeErrorFromFrame into the ErrSelfIdentity sentinel, and
// the resulting error — when handed to onCMDialFailed — must trigger the
// 24h self-identity cooldown (not the generic ban-respect path).
//
// The test exercises the real decode path end-to-end rather than
// short-circuiting with a pre-constructed sentinel: a direct pass of
// protocol.ErrSelfIdentity into onCMDialFailed would still pass even if
// peerSessionRequest accidentally reverted to ErrorFromCode (which
// collapses the reason into ErrPeerBanned), masking the very regression
// this test is meant to pin. By marshalling the real wire shape and
// letting NoticeErrorFromFrame resolve it, we catch decode-layer
// regressions in addition to dispatch-layer regressions.
func TestOnCMDialFailed_WireSentinelSelfIdentity_AppliesCooldown(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	// Craft the exact wire frame the responder emits for the self-loopback
	// case: peer-banned code with a details.reason=self-identity payload.
	// Until is zero so the dialler picks its local default — this test
	// pins reason-aware dispatch, not expiration parsing.
	details, err := protocol.MarshalPeerBannedDetails(time.Time{}, protocol.PeerBannedReasonSelfIdentity)
	if err != nil {
		t.Fatalf("MarshalPeerBannedDetails: %v", err)
	}
	notice := protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Details: details,
	}

	// Real decode path: this is the single line the production call site
	// (peerSessionRequest) runs when a notice arrives. If anyone reverts
	// it to ErrorFromCode(frame.Code), the returned error would no longer
	// satisfy errors.Is(ErrSelfIdentity) and onCMDialFailed would miss
	// the self-identity arm — the test guards that contract explicitly.
	decoded := protocol.NoticeErrorFromFrame(notice)
	if !errors.Is(decoded, protocol.ErrSelfIdentity) {
		t.Fatalf("NoticeErrorFromFrame(peer-banned, self-identity) = %v; want errors.Is(ErrSelfIdentity)", decoded)
	}

	addr := domain.PeerAddress("203.0.113.5:64646")
	svc.onCMDialFailed(addr, decoded, false)

	svc.peerMu.RLock()
	health, ok := svc.health[addr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("onCMDialFailed did not create a peerHealth entry for %s", addr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatal("BannedUntil not set after wire-sentinel self-identity; cooldown missing")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("health.LastErrorCode = %q, want %q", got, want)
	}
	if minExpected := peerBanSelfIdentity - time.Minute; time.Until(health.BannedUntil) < minExpected {
		t.Fatalf("BannedUntil too short: got %v until, want ≥ %v", time.Until(health.BannedUntil), minExpected)
	}
}

// TestIsSelfIdentity_EdgeCases pins the monotone behaviour of the helper:
// empty input is NOT self (so upstream call sites can run the empty
// guard independently without double-negation), and the comparison is
// exact-match on the Ed25519 overlay address.
func TestIsSelfIdentity_EdgeCases(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	if svc.isSelfIdentity("") {
		t.Error("isSelfIdentity(\"\") returned true; expected false so upstream guards stay monotone")
	}
	if !svc.isSelfIdentity(domain.PeerIdentity(svc.identity.Address)) {
		t.Error("isSelfIdentity(local identity) returned false; expected true")
	}
	if svc.isSelfIdentity(domain.PeerIdentity("CoRsAsome0therAddress1111111111111111")) {
		t.Error("isSelfIdentity(foreign address) returned true")
	}
}

// TestSyncPeer_SelfIdentityWelcome_AppliesCooldown pins the contract that
// the syncPeer self-identity branch persists an address-level cooldown
// through applySelfIdentityCooldown. Without this, the fresh-dial
// recovery callers (syncSenderKeys, the unknown-sender recovery in
// handleInboundPushMessage) would race back onto the same self-looping
// address on every tick — detecting the collision but taking no
// preventive action, which defeats the whole point of catching it.
//
// The fix routes the branch through the same cooldown path used by the
// managed outbound dial (openPeerSessionForCM → onCMDialFailed). This
// test reproduces the NAT-reflection shape: the dialled endpoint answers
// with a welcome carrying our own Ed25519 address. Post-dial, the
// peerHealth entry for the dialled address must have BannedUntil set
// (~24h into the future) and LastErrorCode == ErrCodeSelfIdentity so
// the dial gate suppresses further attempts for the ban window.
func TestSyncPeer_SelfIdentityWelcome_AppliesCooldown(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	svc := newSyncPeerTestService(domain.NetworkStatusHealthy)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	// Mock peer: read hello, answer with a welcome whose Address matches
	// the local identity. The Listen field is populated so the log line
	// renders the full diagnostic triple — the guard triggers on the
	// identity equality, not on Listen presence (syncPeer is always a
	// P2P dial, never a subscriber probe, so the Listen gate that lives
	// on the inbound hello path does not apply here).
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		welcome, _ := protocol.MarshalFrameLine(protocol.Frame{
			Type:    "welcome",
			Address: svc.identity.Address,
			Listen:  string(peerAddr),
		})
		_, _ = conn.Write([]byte(welcome))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if got := svc.syncPeer(ctx, peerAddr, true); got != 0 {
		t.Fatalf("syncPeer(self-identity welcome) imported %d; expected 0 on early abort", got)
	}
	<-done

	svc.peerMu.RLock()
	health, ok := svc.health[peerAddr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("applySelfIdentityCooldown was not reached: no peerHealth entry for %s", peerAddr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatal("BannedUntil not set — syncPeer self-identity branch bypassed the cooldown (the bug this test pins)")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("health.LastErrorCode = %q, want %q", got, want)
	}
	if health.LastError == "" {
		t.Fatal("health.LastError is empty; applySelfIdentityCooldown must stamp the structured error message")
	}
	if minExpected := peerBanSelfIdentity - time.Minute; time.Until(health.BannedUntil) < minExpected {
		t.Fatalf("BannedUntil too short: got %v until, want ≥ %v", time.Until(health.BannedUntil), minExpected)
	}
}

// TestTryApplySelfIdentityCooldown_StructuredError_Hit pins the helper's
// primary contract: a concrete *selfIdentityError must be recognised
// through errors.As even when wrapped (fmt.Errorf("%w")) by an
// upstream caller, and the returned bool must be true so the calling
// dispatcher skips the generic disconnect penalty.
func TestTryApplySelfIdentityCooldown_StructuredError_Hit(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	addr := domain.PeerAddress("198.51.100.11:64646")
	selfErr := &selfIdentityError{
		Address:       addr,
		PeerListen:    addr,
		LocalIdentity: domain.PeerIdentity(svc.identity.Address),
	}
	// Wrap with fmt.Errorf("%w: ...") to prove the helper uses errors.As
	// (which unwraps), not a concrete type-assertion. openPeerSession
	// returns the concrete type today, but any future refactor that
	// adds contextual wrapping upstream must not break this contract.
	wrapped := fmt.Errorf("handshake: %w", selfErr)

	if !svc.tryApplySelfIdentityCooldown(addr, wrapped) {
		t.Fatal("tryApplySelfIdentityCooldown(structured, wrapped) returned false; expected true")
	}

	svc.peerMu.RLock()
	health, ok := svc.health[addr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("no peerHealth entry for %s after hit", addr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatal("BannedUntil not set after structured-error hit")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("LastErrorCode = %q, want %q", got, want)
	}
}

// TestTryApplySelfIdentityCooldown_WireSentinel_Hit covers the other
// recognised shape: the bare protocol.ErrSelfIdentity sentinel that
// peerSessionRequest / NoticeErrorFromFrame surface when the remote
// told us via connection_notice{peer-banned, reason=self-identity}.
// The helper must synthesise the structured record (Address from the
// dial target, LocalIdentity from Service) so applySelfIdentityCooldown
// sees the same shape regardless of whether the collision was detected
// locally or reported remotely.
func TestTryApplySelfIdentityCooldown_WireSentinel_Hit(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	addr := domain.PeerAddress("203.0.113.13:64646")
	// Wrap the sentinel to prove errors.Is (which unwraps) is what the
	// helper calls — direct-equality comparison would break here.
	wrapped := fmt.Errorf("peer session request: %w", protocol.ErrSelfIdentity)

	if !svc.tryApplySelfIdentityCooldown(addr, wrapped) {
		t.Fatal("tryApplySelfIdentityCooldown(sentinel, wrapped) returned false; expected true")
	}

	svc.peerMu.RLock()
	health, ok := svc.health[addr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("no peerHealth entry for %s after hit", addr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatal("BannedUntil not set after wire-sentinel hit")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("LastErrorCode = %q, want %q", got, want)
	}
}

// TestTryApplySelfIdentityCooldown_UnrelatedError_Miss pins the
// negative case: an error that is neither a *selfIdentityError nor
// wraps protocol.ErrSelfIdentity must return false AND must NOT write
// any health state. If this leaks a false positive, every generic dial
// failure would collapse into a 24h cooldown, which would silently
// wipe out the entire peer list on a transient outage.
func TestTryApplySelfIdentityCooldown_UnrelatedError_Miss(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	addr := domain.PeerAddress("192.0.2.17:64646")
	unrelated := errors.New("connection refused")

	if svc.tryApplySelfIdentityCooldown(addr, unrelated) {
		t.Fatal("tryApplySelfIdentityCooldown(unrelated) returned true; expected false")
	}

	svc.peerMu.RLock()
	_, ok := svc.health[addr]
	svc.peerMu.RUnlock()
	if ok {
		t.Fatal("peerHealth entry created for unrelated error; helper must leave state untouched on miss")
	}
}

// TestTryApplySelfIdentityCooldown_NilError_Miss guards the nil-input
// degenerate case — errors.As / errors.Is both return false on nil, so
// the helper must return false without touching state. A regression
// here would be a crash on the nil path (nil.Error() inside
// applySelfIdentityCooldown), since the structured record would not
// be reached.
func TestTryApplySelfIdentityCooldown_NilError_Miss(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	addr := domain.PeerAddress("192.0.2.23:64646")

	if svc.tryApplySelfIdentityCooldown(addr, nil) {
		t.Fatal("tryApplySelfIdentityCooldown(nil) returned true; expected false")
	}

	svc.peerMu.RLock()
	_, ok := svc.health[addr]
	svc.peerMu.RUnlock()
	if ok {
		t.Fatal("peerHealth entry created for nil error; helper must leave state untouched on miss")
	}
}

// TestRunPeerSession_SelfIdentity_AppliesCooldownAndExits pins the
// contract that the legacy outbound runPeerSession / openPeerSession
// pair routes the self-loopback failure through
// applySelfIdentityCooldown and exits the retry loop instead of
// downgrading to markPeerDisconnected + a 2s retry. Without this
// branch the managed CM path and syncPeer path converge on the 24h
// BannedUntil cooldown while runPeerSession keeps churning against
// the self-alias forever — the exact regression the bug report
// describes.
//
// The mock peer welcomes us with our own Ed25519 address, which is
// the NAT-reflection / peer-exchange-echo shape that the host:port
// guards (isSelfAddress, isSelfDialIP) cannot catch. Post-run:
//   - the function must return BEFORE the 2s retry-sleep floor
//     (proves the short-circuit fires — not just the cooldown);
//   - the listener must have accepted exactly ONE connection
//     (proves no retry reopened a fresh dial against itself);
//   - health.BannedUntil must be stamped (~24h into the future);
//   - health.LastErrorCode must equal ErrCodeSelfIdentity so the
//     dial gate suppresses further attempts for the ban window.
func TestRunPeerSession_SelfIdentity_AppliesCooldownAndExits(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Mock peer: accept connections, read hello, answer with a welcome
	// whose Address matches the local identity. A retry loop would try
	// to accept a second connection; the acceptCount assertion pins
	// that path closed. Listen is populated so the structured log
	// emits the full diagnostic triple — the guard triggers on
	// identity equality, not on Listen presence.
	//
	// acceptCount is observed AFTER runPeerSession returns; using
	// atomic.Int32 (not a channel) avoids the close-while-producer-
	// is-writing race — the accept goroutine exits only after
	// ln.Close() is called by t.Cleanup, which happens after the test
	// body drops all references.
	peerAddr := domain.PeerAddress(ln.Addr().String())
	var acceptCount atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				// ln.Close() is the expected exit path — t.Cleanup
				// closes the listener at test teardown.
				return
			}
			acceptCount.Add(1)
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				reader := bufio.NewReader(conn)
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				welcome, err := protocol.MarshalFrameLine(protocol.Frame{
					Type:    "welcome",
					Version: config.ProtocolVersion,
					Address: svc.identity.Address,
					Listen:  string(peerAddr),
				})
				if err != nil {
					return
				}
				_, _ = conn.Write([]byte(welcome))
			}(c)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(runDone)
		svc.runPeerSession(ctx, peerAddr)
	}()

	// The short-circuit must fire BEFORE the 2s retry sleep. A generous
	// 1.5s ceiling proves the function returned directly from the
	// tryApplySelfIdentityCooldown branch — not after one pass through
	// the retry backoff (which would also pass the BannedUntil check
	// below if the cooldown eventually lands, hiding the regression).
	select {
	case <-runDone:
		if elapsed := time.Since(start); elapsed >= 1500*time.Millisecond {
			t.Fatalf("runPeerSession returned after %v; must short-circuit before the 2s retry-sleep floor", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runPeerSession did not return within 5s — self-identity short-circuit missing, retry loop still active")
	}

	// Snapshot the accept count now that runPeerSession has returned.
	// A retry would have redialled the listener and bumped the counter
	// above 1 — the wire-level footprint of the regression described
	// in the comment above.
	if got := acceptCount.Load(); got != 1 {
		t.Fatalf("listener saw %d accepts; want exactly 1 (retry loop still churning against self-alias)", got)
	}

	svc.peerMu.RLock()
	health, ok := svc.health[peerAddr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("applySelfIdentityCooldown was not reached: no peerHealth entry for %s", peerAddr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatal("BannedUntil not set — runPeerSession self-identity branch bypassed the cooldown (the bug this test pins)")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("health.LastErrorCode = %q, want %q", got, want)
	}
	if health.LastError == "" {
		t.Fatal("health.LastError is empty; applySelfIdentityCooldown must stamp the structured error message")
	}
	if minExpected := peerBanSelfIdentity - time.Minute; time.Until(health.BannedUntil) < minExpected {
		t.Fatalf("BannedUntil too short: got %v until, want ≥ %v", time.Until(health.BannedUntil), minExpected)
	}
}

// TestApplySelfIdentityCooldown_NoAccumulation_WithinBanWindow pins the
// documented "no accumulation" contract (peerBanSelfIdentity comment in
// peer_state.go and onCMDialFailed comment): the address-level Score
// penalty and ConsecutiveFailures increment must land exactly ONCE
// while the ban window is still active, regardless of how many
// outbound paths (onCMDialFailed, runPeerSession, syncPeer,
// sendNoticeToPeer) resurface the same self-alias. The older
// implementation called the penalty arithmetic unconditionally, so
// every re-entry drove ConsecutiveFailures upward unboundedly and
// re-clamped Score at the floor on every hit. Either would blend
// self-identity into the generic failure-backoff machinery even
// though the classification is binary evidence.
//
// The test invokes applySelfIdentityCooldown twice with no other
// state transition between calls. First call must drop Score by
// peerScoreOldProtocol and set ConsecutiveFailures to 1. Second call
// must keep both unchanged. BannedUntil may be refreshed (the
// function still updates current-status fields), but the accumulators
// stay pinned to the first-observation value.
func TestApplySelfIdentityCooldown_NoAccumulation_WithinBanWindow(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	addr := domain.PeerAddress("198.51.100.31:64646")
	selfErr := &selfIdentityError{
		Address:       addr,
		PeerListen:    addr,
		LocalIdentity: domain.PeerIdentity(svc.identity.Address),
	}

	svc.applySelfIdentityCooldown(addr, selfErr)

	svc.peerMu.RLock()
	firstScore := svc.health[addr].Score
	firstFailures := svc.health[addr].ConsecutiveFailures
	firstBannedUntil := svc.health[addr].BannedUntil
	svc.peerMu.RUnlock()

	if firstFailures != 1 {
		t.Fatalf("after first observation ConsecutiveFailures = %d, want 1", firstFailures)
	}
	// Score should have dropped by peerScoreOldProtocol, clamped at
	// peerScoreMin. From a zero baseline 0 + -50 == -50 which is
	// exactly peerScoreMin — pre-clamp and post-clamp coincide.
	if firstScore != peerScoreMin {
		t.Fatalf("after first observation Score = %d, want clamped floor %d (start 0 + %d)", firstScore, peerScoreMin, peerScoreOldProtocol)
	}

	// Second call — SAME address, within the same ban window, no
	// intervening state change. The accumulators must stay put.
	svc.applySelfIdentityCooldown(addr, selfErr)

	svc.peerMu.RLock()
	secondScore := svc.health[addr].Score
	secondFailures := svc.health[addr].ConsecutiveFailures
	secondBannedUntil := svc.health[addr].BannedUntil
	svc.peerMu.RUnlock()

	if secondFailures != firstFailures {
		t.Fatalf("ConsecutiveFailures accumulated across self-identity hits: first=%d second=%d (contract: no accumulation)", firstFailures, secondFailures)
	}
	if secondScore != firstScore {
		t.Fatalf("Score accumulated across self-identity hits: first=%d second=%d (contract: no accumulation)", firstScore, secondScore)
	}
	// BannedUntil is allowed to refresh forward (current-status field)
	// but MUST NOT drift backward — the ban window only extends.
	if secondBannedUntil.Before(firstBannedUntil) {
		t.Fatalf("BannedUntil regressed: first=%v second=%v", firstBannedUntil, secondBannedUntil)
	}
}

// TestApplySelfIdentityCooldown_ReAppliesAfterBanExpired covers the
// complementary edge of the first-observation gate: when the prior
// self-identity ban has already expired (the address was dormant long
// enough to drop off the dial gate) a fresh collision IS a new
// observation and must re-apply the penalty. Without this branch the
// peer would stay at the same Score/ConsecutiveFailures pinned 24h+
// ago even after a real recovery-then-recollision cycle, which would
// let the dial gate promote it back into the candidate set with a
// stale bottom-of-list Score that no longer reflects the current
// evidence.
//
// The test seeds the health record with a prior self-identity ban
// whose window has already lapsed, then calls applySelfIdentityCooldown
// and asserts ConsecutiveFailures moved from the seeded value to
// seeded+1 (so the fresh observation did count), proving the gate
// does not incorrectly suppress the accumulator on the expired-ban
// path.
func TestApplySelfIdentityCooldown_ReAppliesAfterBanExpired(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	addr := domain.PeerAddress("203.0.113.47:64646")
	selfErr := &selfIdentityError{
		Address:       addr,
		PeerListen:    addr,
		LocalIdentity: domain.PeerIdentity(svc.identity.Address),
	}

	// Seed: pretend we applied a self-identity cooldown ages ago and
	// the ban window already expired. Direct map write under the lock
	// is the simplest way to reach this state deterministically — the
	// test is in the same package, so the private field is visible.
	priorFailures := 3
	priorScore := -10
	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{
		Address:             addr,
		LastErrorCode:       protocol.ErrCodeSelfIdentity,
		BannedUntil:         time.Now().UTC().Add(-time.Hour), // already expired
		ConsecutiveFailures: priorFailures,
		Score:               priorScore,
	}
	svc.peerMu.Unlock()

	svc.applySelfIdentityCooldown(addr, selfErr)

	svc.peerMu.RLock()
	gotFailures := svc.health[addr].ConsecutiveFailures
	gotScore := svc.health[addr].Score
	gotBannedUntil := svc.health[addr].BannedUntil
	svc.peerMu.RUnlock()

	if gotFailures != priorFailures+1 {
		t.Fatalf("ConsecutiveFailures after expired-ban re-collision = %d, want %d (seeded %d + 1 fresh observation)", gotFailures, priorFailures+1, priorFailures)
	}
	// Score should have fallen by peerScoreOldProtocol, clamped at
	// peerScoreMin. priorScore=-10 plus -50 = -60 which clamps to -50.
	wantScore := clampScore(priorScore + peerScoreOldProtocol)
	if gotScore != wantScore {
		t.Fatalf("Score after expired-ban re-collision = %d, want %d", gotScore, wantScore)
	}
	if !gotBannedUntil.After(time.Now().UTC()) {
		t.Fatalf("BannedUntil after expired-ban re-collision = %v, want future (ban refreshed)", gotBannedUntil)
	}
}

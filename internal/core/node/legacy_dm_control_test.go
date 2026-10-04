package node

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// legacy_dm_control_test.go pins the dm_control half of N1
// (docs/refactoring/n1-legacy-residual.md §3): a connection that does not
// declare the dm_control dtype says so about ITSELF, for as long as it lives.
// It holds the batch that would have left over it; it does not forbid
// reactions to the identity for an hour, and it does not stop the next pass
// from leaving over a candidate that can take them.

// DC-A: the gate refused the type once — the connection the send would have
// used declared no dm_control. The next pass, once a candidate that takes the
// type is there, sends: no identity-wide block was left behind, and no new
// session had to clear one.
func TestADTypeRefusalHoldsOnlyTheBatchItCameFrom(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)

	refused := true
	attempts := 0
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		attempts++
		if refused {
			return dmControlDispatch{kind: datagram.SendRejected, rejection: datagram.RejectionUnsupportedDType, summary: "rejected"}
		}
		return dmControlDispatch{kind: datagram.SendQueued, summary: "queued"}
	}
	facts := manyOutgoingFacts(peer, 3)
	if err := sender.queueReactions(peer, facts); err != nil {
		t.Fatalf("queue: %v", err)
	}
	sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))
	if attempts != 1 {
		t.Fatalf("precondition: one frame met the refusal, got %d attempts", attempts)
	}
	if got := len(queuedFor(sender, peer).entries); got != len(facts) {
		t.Fatalf("%d of %d facts came back after the refusal", got, len(facts))
	}

	// A candidate that takes the type is now what the send would use — no
	// session event for the identity in between.
	refused = false
	attempts = 0
	sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor+dmControlRetryDelay+time.Second))
	if attempts == 0 {
		t.Fatal("reactions to the identity stayed blocked after the refusing candidate was gone: the refusal outlived the connection it was about")
	}
	if queuedPeers(sender) != 0 {
		t.Fatalf("facts stayed queued after the capable candidate took them: %#v", queuedFor(sender, peer))
	}
}

// DC-D: a send to X tries X's v2 connection before a legacy one naming X,
// whichever direction each runs in and however old each is.
func TestSendSelectionPrefersTheProvenConnection(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	peer, _ := sessionv2test.NewProvenPeer(t)
	x := peer.Identity
	caps := []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshRelayV1}

	legacy := legacyOutboundSession(legacyAttackerAddress, x)
	legacy.capabilities = caps
	old := time.Now().Add(-time.Hour)
	svc.peerMu.Lock()
	svc.sessions[legacy.address] = legacy
	svc.health[legacy.address] = &peerHealth{Connected: true, LastConnectedAt: old}
	svc.peerMu.Unlock()

	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	overlay := domain.PeerAddress("10.0.0.21:64646")
	core := netcore.New(netcore.ConnID(77), &fakeConn{Conn: local, remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.21"), Port: 40021}},
		netcore.Inbound, netcore.Options{Address: overlay, Identity: x, Caps: caps})
	t.Cleanup(core.Close)
	core.SetAuth(provenInboundAuth(t, peer))
	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(local, &connEntry{core: core})
	svc.health[overlay] = &peerHealth{Connected: true, LastConnectedAt: time.Now()}
	conns := svc.peerSendableConnectionsLocked(x, domain.CapMeshDatagramV1, time.Now())
	svc.peerMu.Unlock()

	if len(conns) != 2 {
		t.Fatalf("%d sendable connections, want 2", len(conns))
	}
	if conns[0].outbound != nil {
		t.Fatal("the legacy session naming X was tried before X's own v2 connection")
	}
}

// legacyAuthFor is the auth state of an accepted v1 connection naming id.
func legacyAuthFor(id domain.PeerIdentity) *connauth.State {
	return &connauth.State{Verified: true, Hello: protocol.Frame{Address: id.String()}}
}

// reactionsFixture registers connections of one identity with chosen
// declarations, proof and health, and asks the node what it knows.
type reactionsFixture struct {
	t   *testing.T
	svc *Service
	n   uint64
}

func newReactionsFixture(t *testing.T) *reactionsFixture {
	t.Helper()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	return &reactionsFixture{t: t, svc: svc}
}

// accepted registers an accepted connection naming id; auth decides whether it
// is proven, healthy whether a send would use it, takesDMControl what it
// declared. It returns a closer that unregisters it, as a teardown would.
func (f *reactionsFixture) accepted(id domain.PeerIdentity, auth *connauth.State, healthy, takesDMControl bool) func() {
	f.t.Helper()
	f.n++
	types := []domain.DType{domain.DTypeGetIdentity}
	if takesDMControl {
		types = append(types, domain.DTypeDMControl)
	}
	declarations := netcore.HandshakeDeclarations{DeclaredDTypes: domain.ExplicitDTypes(types)}
	local, remote := net.Pipe()
	f.t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	overlay := domain.PeerAddress(net.JoinHostPort("10.0.1."+itoaForTest(f.n), "64646"))
	core := netcore.New(netcore.ConnID(800+f.n), &fakeConn{Conn: local, remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.1." + itoaForTest(f.n)), Port: 40000}},
		netcore.Inbound, netcore.Options{
			Address:      overlay,
			Identity:     id,
			Caps:         []domain.Capability{domain.CapMeshDatagramV1},
			Declarations: &declarations,
		})
	f.t.Cleanup(core.Close)
	core.SetAuth(auth)
	f.svc.peerMu.Lock()
	f.svc.setTestConnEntryLocked(local, &connEntry{core: core})
	f.svc.health[overlay] = &peerHealth{Connected: healthy, LastConnectedAt: time.Now()}
	f.svc.peerMu.Unlock()
	return func() {
		f.svc.peerMu.Lock()
		delete(f.svc.health, overlay)
		f.svc.peerMu.Unlock()
	}
}

func itoaForTest(n uint64) string { return strconv.FormatUint(n, 10) }

// DC-1: X's v2 connection declares dm_control and a legacy connection naming X
// does not; both are live. X can take reactions.
func TestReactionsSupportReadsTheV2Connection(t *testing.T) {
	f := newReactionsFixture(t)
	peer, _ := sessionv2test.NewProvenPeer(t)
	pinIdentity(t, f.svc, peer.Identity)
	f.accepted(peer.Identity, legacyAuthFor(peer.Identity), true, false)
	f.accepted(peer.Identity, provenInboundAuth(t, peer), true, true)
	if got := f.svc.ReactionsSupportOf(peer.Identity); got != domain.ReactionsSupportDeclared {
		t.Fatalf("support = %s, want declared", got)
	}
}

// DC-2: X is pinned to v2 and its v2 connection is momentarily not sendable;
// a healthy legacy connection naming X declares no dm_control. That
// connection proves nothing about X, so X is not "unable to receive
// reactions" — nothing is known now.
func TestALegacyConnectionCannotMakeAV2IdentityUnableToTakeReactions(t *testing.T) {
	f := newReactionsFixture(t)
	peer, _ := sessionv2test.NewProvenPeer(t)
	pinIdentity(t, f.svc, peer.Identity)
	f.accepted(peer.Identity, provenInboundAuth(t, peer), false, true)
	f.accepted(peer.Identity, legacyAuthFor(peer.Identity), true, false)
	if got := f.svc.ReactionsSupportOf(peer.Identity); got != domain.ReactionsSupportUnknown {
		t.Fatalf("support = %s, want unknown: a legacy connection naming a v2 identity decided it", got)
	}
}

// DC-3 (legacy-only identity): its only live connection declares no
// dm_control — confirmed absent, for as long as that connection lives. Once it
// is gone nothing is known; a new connection that declares the type is read
// at once, with no hour-long belief in between.
func TestALegacyOnlyIdentitysSupportFollowsItsLiveConnection(t *testing.T) {
	f := newReactionsFixture(t)
	x := idPeerB
	closeOld := f.accepted(x, legacyAuthFor(x), true, false)
	if got := f.svc.ReactionsSupportOf(x); got != domain.ReactionsSupportAbsent {
		t.Fatalf("support = %s, want absent while the connection that declared nothing lives", got)
	}
	closeOld()
	if got := f.svc.ReactionsSupportOf(x); got != domain.ReactionsSupportUnknown {
		t.Fatalf("support = %s after the connection closed, want unknown", got)
	}
	f.accepted(x, legacyAuthFor(x), true, true)
	if got := f.svc.ReactionsSupportOf(x); got != domain.ReactionsSupportDeclared {
		t.Fatalf("support = %s with a connection that declares the type, want declared", got)
	}
}

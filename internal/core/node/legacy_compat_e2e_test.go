package node

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// legacy_compat_e2e_test.go runs a node of this build against an emulated
// released build that speaks only v1 (asOldNode behind legacyVersionProxy,
// which rewrites the old peer's announced version to 28, 29 or 30) and pins
// the compatibility the transition promises: the session is legacy, and
// legacy delivery is not truncated — a DM crosses in both directions, so the
// route to each node and the frames over it work. It also pins that reaching
// an old listener over v1 costs the address nothing beyond the contract.
//
// THE OLD PEER IS AN EMULATION, NOT A RELEASED BINARY: it is this build in
// v1-only mode (asOldNode) with the proxy rewriting its announced version. The
// cross-binary run against the real v26–v30 releases is a separate task with
// its own isolation; these tests check what
// this build does with a peer that behaves and announces like an old one.

// legacyPair is a node of this build (new) wired to an emulated old node
// (old) through two proxies: newDialsOld carries the new node's dials to the
// old listener (its welcome rewritten to the old version), oldDialsNew
// carries the old node's dials to the new listener (its hello rewritten).
type legacyPair struct {
	new, old        *Service
	newToOldAddress domain.PeerAddress
	oldToNewAddress domain.PeerAddress
}

// startLegacyPair brings up the two nodes and the two proxies for one old
// version. Both nodes are full nodes so either can host a DM recipient and
// relay.
func startLegacyPair(t *testing.T, version int) legacyPair {
	t.Helper()
	newAddress := freeAddress(t)
	oldAddress := freeAddress(t)

	newDialsOld := startLegacyVersionProxy(t, oldAddress, oldPeerListens, version)
	oldDialsNew := startLegacyVersionProxy(t, newAddress, oldPeerDials, version)

	oldNode, stopOld := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  oldAddress,
		BootstrapPeers: []string{oldDialsNew.address()},
		Type:           config.NodeTypeFull,
	}, asOldNode)
	t.Cleanup(stopOld)

	newNode, stopNew := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  newAddress,
		BootstrapPeers: []string{newDialsOld.address()},
		Type:           config.NodeTypeFull,
	}, nil)
	t.Cleanup(stopNew)

	return legacyPair{
		new:             newNode,
		old:             oldNode,
		newToOldAddress: newDialsOld.peerAddress(),
		oldToNewAddress: oldDialsNew.peerAddress(),
	}
}

// deliverDM encrypts a DM from `from` to `to` and injects it into `from`
// through the local API, then waits for `to` to store it. It returns once the
// message is in the recipient's inbox or the deadline passes.
func deliverDM(t *testing.T, from, to *Service, id, body string) {
	t.Helper()
	ciphertext, err := directmsg.EncryptForParticipants(
		from.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(to.Address()),
			BoxKeyBase64: identity.BoxPublicKeyBase64(to.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: body},
	)
	if err != nil {
		t.Fatalf("encrypt DM %s: %v", id, err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	stored := from.HandleLocalFrame(sendMessageFrame("dm", id, from.Address(), to.Address(), "sender-delete", ts, 0, ciphertext))
	if stored.Type != "message_stored" {
		t.Fatalf("send %s: %#v", id, stored)
	}
	waitForConditionMsg(t, 20*time.Second, "DM "+id+" never reached the recipient", func() bool {
		reply := to.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		if reply.Type != "messages" {
			return false
		}
		for _, msg := range reply.Messages {
			if msg.ID == id {
				return true
			}
		}
		return false
	})
}

// A node of this build and an old peer form a legacy session in both
// directions and deliver a DM each way, for versions 28, 29 and 30: an old
// peer dialling a new node, and the delivery of a new node dialling an old
// peer. A probe hello without an identity on the same connection is not
// implemented in the node, so a full v1 hello is used instead and that probe
// stays uncovered.
func TestANodeAndAnOldPeerDeliverDMsBothWays(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			pair := startLegacyPair(t, version)

			waitForConditionMsg(t, 20*time.Second, "no legacy session between the node and the old peer", func() bool {
				_, newV1 := authenticatedInbound(pair.new)
				_, oldV1 := authenticatedInbound(pair.old)
				return newV1 > 0 && oldV1 > 0
			})
			if v2, _ := authenticatedInbound(pair.new); v2 != 0 {
				t.Fatalf("v%d: %d v2 sessions with a node that cannot speak v2", version, v2)
			}

			// Delivery is not truncated: a DM crosses each way.
			deliverDM(t, pair.new, pair.old, "dm-new-to-old", "to-the-old-peer")
			deliverDM(t, pair.old, pair.new, "dm-old-to-new", "to-the-new-node")

			kinds := pair.new.SessionKindStats()
			if kinds.InboundLegacy == 0 || kinds.OutboundLegacy == 0 {
				t.Fatalf("v%d: session kinds = %+v, want a legacy session each way", version, kinds)
			}
			if kinds.InboundV2 != 0 || kinds.OutboundV2 != 0 {
				t.Fatalf("v%d: a v2 session was counted with an old peer: %+v", version, kinds)
			}
			if kinds.HellosBelowSecureVersion == 0 || kinds.WelcomesBelowSecureVersion == 0 {
				t.Fatalf("v%d: the old peer's announced version was not seen as pre-v2: %+v", version, kinds)
			}
		})
	}
}

// Reaching an old listener over v1 — the v2 ClientHello answered with
// invalid-json (first byte `{`), then the fall back — costs the dialled
// address nothing: it is an old node, not a failure. This is the `{` answer
// to the ClientHello and the no-penalty half of a new node dialling an old one,
// for versions 28, 29 and 30. (Silence and RST in place of the answer are in
// the test below; the no-penalty accounting itself — 100 refusals through the
// CM, v2 not delayed, repeated refusals of a pinned identity — is pinned in
// legacy_refusal_test.go.)
func TestReachingAnOldListenerOverV1DoesNotPenaliseTheAddress(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			oldAddress := freeAddress(t)
			newDialsOld := startLegacyVersionProxy(t, oldAddress, oldPeerListens, version)
			_, stopOld := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  oldAddress,
				BootstrapPeers: []string{},
				Type:           config.NodeTypeFull,
			}, withoutOwnDials(asOldNode))
			defer stopOld()
			dialer, stopDialer := startQuietNode(t, nil)
			defer stopDialer()
			target := newDialsOld.peerAddress()

			if established, err := dialPastTransport(t, dialer, target); !established {
				t.Fatalf("v%d: the node did not reach the old listener over v1: %v", version, err)
			}

			// A successful legacy session with an old node leaves the address
			// as a healthy peer: no failure counted, no cooldown, no setup
			// failure. The dial penalises nothing because it ended in a
			// session, and the v2 attempt before it ended in ErrPeerSpeaksV1,
			// which is never charged.
			dialer.peerMu.RLock()
			health := dialer.health[target]
			setup := dialer.setupFailures[target]
			dialer.peerMu.RUnlock()
			if setup != nil && setup.Consecutive > 0 {
				t.Fatalf("v%d: reaching an old listener was counted as a setup failure: %+v", version, *setup)
			}
			if health != nil {
				if health.ConsecutiveFailures != 0 {
					t.Fatalf("v%d: ConsecutiveFailures = %d, want 0", version, health.ConsecutiveFailures)
				}
				if health.Score < 0 {
					t.Fatalf("v%d: Score = %d, want it not lowered", version, health.Score)
				}
				if !health.BannedUntil.IsZero() {
					t.Fatalf("v%d: the address was put into a cooldown until %v", version, health.BannedUntil)
				}
			}
			// The address answered v1: the next dial to it may start with v1,
			// and no v2 binding was created.
			if dialer.secureSessions.store.endpointRequiresV2(target) {
				t.Fatalf("v%d: an old listener left a v2 binding", version)
			}
		})
	}
}

// Silence and RST in place of an answer: a listener that accepts the TCP
// connection but never answers the v2 ClientHello, or resets it, is a plain v2
// failure — not a v1 answer. The node does not fall back to v1 (only the `{`
// first byte means "old node"), so it leaves no v1 mark and opens no legacy
// session; it also binds the address to nothing. A man in the middle cannot
// turn silence into a downgrade.
func TestReachingASilentOrResettingListenerLeavesNoV1Mark(t *testing.T) {
	t.Parallel()
	cases := map[string]func(net.Conn){
		"silence": func(conn net.Conn) { /* hold it open, answer nothing */ },
		"rst": func(conn net.Conn) {
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetLinger(0)
			}
			_ = conn.Close()
		},
	}
	for name, handler := range cases {
		name, handler := name, handler
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			address := serveRawTCP(t, handler)
			dialer, stopDialer := startQuietNode(t, nil)
			defer stopDialer()

			// The dial reaches the socket and fails the v2 handshake; it is not
			// a v1 answer, so dialPastTransport returns a non-transport error.
			if established, _ := dialPastTransport(t, dialer, address); established {
				t.Fatalf("%s: a session was established with a listener that never spoke v2", name)
			}
			if _, v1 := marksOf(dialer); v1 != 0 {
				t.Fatalf("%s: a v1 mark was set for a listener that only %s", name, name)
			}
			if dialer.secureSessions.store.endpointRequiresV2(address) {
				t.Fatalf("%s: the failed v2 attempt bound the address to v2", name)
			}
		})
	}
}

// serveRawTCP accepts connections on a loopback port and hands each to
// handler. It is a bare socket, not a node: the caller uses it to see how the
// dialler reacts to a peer that is not an old node answering with a v1 line.
func serveRawTCP(t *testing.T, handler func(net.Conn)) domain.PeerAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var held sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held.Lock()
			conns = append(conns, conn)
			held.Unlock()
			go handler(conn)
		}
	}()
	t.Cleanup(func() {
		held.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		held.Unlock()
	})
	return domain.PeerAddress(listener.Addr().String())
}

// A mixed network delivers across an emulated old relay: a DM from a foreign
// sender, injected at a new node, reaches the new node that hosts the
// recipient by travelling through an old (v1-only) node in the middle. The old
// node's version is rewritten to a released one on every leg, so both new
// nodes run legacy sessions with it. It runs for the oldest supported version
// only.
func TestMixedNetworkChainDeliversThroughAnOldRelay(t *testing.T) {
	t.Parallel()
	const version = config.MinimumProtocolVersion

	senderAddr := freeAddress(t) // the two new nodes
	recipientAddr := freeAddress(t)
	oldAddr := freeAddress(t) // the old relay in the middle

	// Each new node dials the old relay through an oldPeerListens proxy (sees
	// its welcome as v`version`); the old relay dials each new node back
	// through an oldPeerDials proxy (its hello rewritten), so a session exists
	// both ways on each leg and the relay can forward either direction.
	senderDialsOld := startLegacyVersionProxy(t, oldAddr, oldPeerListens, version)
	recipientDialsOld := startLegacyVersionProxy(t, oldAddr, oldPeerListens, version)
	oldDialsSender := startLegacyVersionProxy(t, senderAddr, oldPeerDials, version)
	oldDialsRecipient := startLegacyVersionProxy(t, recipientAddr, oldPeerDials, version)

	oldRelay, stopOld := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  oldAddr,
		BootstrapPeers: []string{oldDialsSender.address(), oldDialsRecipient.address()},
		Type:           config.NodeTypeFull,
	}, asOldNode)
	defer stopOld()

	senderNode, stopSender := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  senderAddr,
		BootstrapPeers: []string{senderDialsOld.address()},
		Type:           config.NodeTypeFull,
	}, nil)
	defer stopSender()

	recipientNode, stopRecipient := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  recipientAddr,
		BootstrapPeers: []string{recipientDialsOld.address()},
		Type:           config.NodeTypeFull,
	}, nil)
	defer stopRecipient()

	foreignSender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	for _, svc := range []*Service{senderNode, oldRelay, recipientNode} {
		registerContact(t, svc, foreignSender)
	}

	waitForConditionMsg(t, 20*time.Second, "the chain never connected through the old relay", func() bool {
		return len(senderNode.Peers()) >= 1 && len(recipientNode.Peers()) >= 1 && len(oldRelay.Peers()) >= 2
	})

	recipientID := domain.PeerIdentityFromWire(recipientNode.Address())
	assertNoDirectContact(t, senderNode, recipientID, "before the DM")

	ciphertext, err := directmsg.EncryptForParticipants(
		foreignSender,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(recipientNode.Address()),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientNode.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "through-the-old-relay"},
	)
	if err != nil {
		t.Fatalf("encrypt DM: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	stored := senderNode.HandleLocalFrame(sendMessageFrame("dm", "chain-dm", foreignSender.Address, recipientNode.Address(), string(protocol.MessageFlagImmutable), ts, 0, ciphertext))
	if stored.Type != "message_stored" {
		t.Fatalf("inject at the sender node: %#v", stored)
	}

	waitForConditionMsg(t, 25*time.Second, "the DM never reached the recipient through the old relay", func() bool {
		reply := recipientNode.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		if reply.Type != "messages" {
			return false
		}
		for _, msg := range reply.Messages {
			if msg.ID == "chain-dm" {
				return true
			}
		}
		return false
	})
	// The delivery proves the old relay carried the DM only if the two new
	// nodes never met: a direct session, either way, would carry it past the
	// relay and the test would pass without the old node doing anything.
	assertNoDirectContact(t, senderNode, recipientID, "after the DM")
}

// assertNoDirectContact fails when svc holds a session or a connection, in
// either direction, with the peer whose identity is id.
func assertNoDirectContact(t *testing.T, svc *Service, id domain.PeerIdentity, when string) {
	t.Helper()
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	for address, session := range svc.sessions {
		if session != nil && session.peerIdentity == id {
			t.Fatalf("%s: the sender holds a session with the recipient at %s, so the chain bypassed the old relay", when, address)
		}
	}
	for connID, entry := range svc.conns {
		if entry != nil && entry.core != nil && entry.core.Identity() == id {
			t.Fatalf("%s: the sender holds connection %d with the recipient, so the chain bypassed the old relay", when, connID)
		}
	}
}

func versionName(version int) string {
	return "v" + strconv.Itoa(version)
}

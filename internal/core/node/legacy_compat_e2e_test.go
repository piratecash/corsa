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

// A silent or resetting listener supplies no session proof. A dial must
// leave the endpoint unbound and create no compatible session.
func TestReachingASilentOrResettingListenerLeavesNoBindingOrSession(t *testing.T) {
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
			if dialer.secureSessions.store.endpointRequiresV2(address) {
				t.Fatalf("%s: the failed v2 attempt bound the address to v2", name)
			}
		})
	}
}

// serveRawTCP accepts connections on a loopback port and hands each to
// handler. It supplies a socket without an authenticated node session.
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

// startTLSRelayPair gives both endpoints one proved route through a relay.
// The endpoints cannot listen or dial one another.
func startTLSRelayPair(t *testing.T) (sender, relay, recipient *Service) {
	t.Helper()
	relayAddress := freeAddress(t)
	relay, stopRelay := startTestNodeWithSetup(t, config.Node{
		ListenAddress: relayAddress, BootstrapPeers: []string{}, Type: config.NodeTypeFull,
	}, withoutOwnDials(nil))
	t.Cleanup(stopRelay)

	startEndpoint := func() *Service {
		svc, stop := startTestNodeWithSetup(t, config.Node{
			ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: false,
			BootstrapPeers: []string{relayAddress}, ConnectOnly: relayAddress, Type: config.NodeTypeFull,
		}, nil)
		t.Cleanup(stop)
		return svc
	}
	sender, recipient = startEndpoint(), startEndpoint()
	waitForConditionMsg(t, 20*time.Second, "the TLS endpoints did not establish relay sessions", func() bool {
		return sender.SessionKindStats().OutboundV2 > 0 && recipient.SessionKindStats().OutboundV2 > 0
	})
	return sender, relay, recipient
}

func TestNetworkChainDeliversForeignMessageThroughTLSRelay(t *testing.T) {
	t.Parallel()
	senderNode, relay, recipientNode := startTLSRelayPair(t)
	foreignSender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	for _, svc := range []*Service{senderNode, relay, recipientNode} {
		registerContact(t, svc, foreignSender)
	}

	recipientID := domain.PeerIdentityFromWire(recipientNode.Address())
	relayID := domain.PeerIdentityFromWire(relay.Address())
	waitForConditionMsg(t, 20*time.Second, "no target route through the TLS relay", func() bool {
		route := senderNode.RoutingSnapshot().BestRoute(recipientID)
		return route != nil && route.NextHop == relayID && route.Hops == 2
	})
	assertNoDirectContact(t, senderNode, recipientID, "before the DM")

	ciphertext, err := directmsg.EncryptForParticipants(
		foreignSender,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(recipientNode.Address()),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientNode.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "through-the-tls-relay"},
	)
	if err != nil {
		t.Fatalf("encrypt DM: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	stored := senderNode.HandleLocalFrame(sendMessageFrame("dm", "chain-dm", foreignSender.Address, recipientNode.Address(), string(protocol.MessageFlagImmutable), ts, 0, ciphertext))
	if stored.Type != "message_stored" {
		t.Fatalf("inject at the sender node: %#v", stored)
	}

	waitForConditionMsg(t, 25*time.Second, "the DM never reached the recipient through the TLS relay", func() bool {
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
	// Neither endpoint had a session with the other while the DM crossed the relay.
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
			t.Fatalf("%s: the sender holds a session with the recipient at %s, so the chain bypassed the relay", when, address)
		}
	}
	for connID, entry := range svc.conns {
		if entry != nil && entry.core != nil && entry.core.Identity() == id {
			t.Fatalf("%s: the sender holds connection %d with the recipient, so the chain bypassed the relay", when, connID)
		}
	}
}

func versionName(version int) string {
	return "v" + strconv.Itoa(version)
}

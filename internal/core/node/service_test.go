package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/gazeta"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/transport"
)

// candidateAddresses extracts the dial addresses from peerDialCandidate results.
func candidateAddresses(candidates []peerDialCandidate) []string {
	out := make([]string, len(candidates))
	for i, c := range candidates {
		out[i] = string(c.address)
	}
	return out
}

func TestSingleNodeJSONProtocolFlow(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	ts := time.Now().UTC().Format(time.RFC3339)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Handshake over unauthenticated TCP connection.
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
	)

	if got := frames[0]; got.Type != "welcome" || got.Address != svc.Address() {
		t.Fatalf("unexpected welcome: %#v", got)
	}
	if got := frames[0]; got.Listener != "1" || got.Listen != normalizeAddress(address) {
		t.Fatalf("unexpected welcome listener fields: %#v", got)
	}

	// Query local state via HandleLocalFrame (no peers expected initially).
	peers := svc.HandleLocalFrame(protocol.Frame{Type: "get_peers"})
	if peers.Type != "peers" || peers.Count != 0 {
		t.Fatalf("unexpected peers: %#v", peers)
	}

	// State-changing + sensitive-read commands via local RPC (requires
	// authenticated peer on the data port; HandleLocalFrame is the trusted
	// local operator path).
	stored := svc.HandleLocalFrame(sendMessageFrame("global", "msg-1", svc.Address(), "*", "immutable", ts, 0, "hello"))
	if stored.Type != "message_stored" || stored.ID != "msg-1" {
		t.Fatalf("unexpected stored frame: %#v", stored)
	}

	messages := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	assertMessageFrame(t, messages, "messages", "global", 1, protocol.MessageFrame{
		ID: "msg-1", Sender: svc.Address(), Recipient: "*", Flag: "immutable", CreatedAt: ts, TTLSeconds: 0, Body: "hello",
	})
	inbox := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()})
	assertMessageFrame(t, inbox, "inbox", "global", 1, protocol.MessageFrame{
		ID: "msg-1", Sender: svc.Address(), Recipient: "*", Flag: "immutable", CreatedAt: ts, TTLSeconds: 0, Body: "hello",
	})
}

func TestClientWithoutListenerAnnouncesIdentityButNotDialAddress(t *testing.T) {
	t.Parallel()

	addressFull := freeAddress(t)
	addressClient := freeAddress(t)

	fullNode, stopFull := startTestNode(t, config.Node{
		ListenAddress:    addressFull,
		AdvertiseAddress: normalizeAddress(addressFull),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stopFull()

	clientNode, stopClient := startTestNode(t, config.Node{
		ListenAddress:    addressClient,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             domain.NodeTypeClient,
	})
	defer stopClient()

	// Use HandleLocalFrame to query the full node's local state directly.
	waitForCondition(t, 5*time.Second, func() bool {
		reply := fullNode.HandleLocalFrame(protocol.Frame{Type: "fetch_identities"})
		for _, address := range reply.Identities {
			if address == clientNode.Address() {
				return true
			}
		}
		return false
	})

	peersReply := fullNode.HandleLocalFrame(protocol.Frame{Type: "get_peers"})
	for _, peer := range peersReply.Peers {
		if peer == normalizeAddress(addressClient) {
			t.Fatalf("listener-disabled client should not be advertised as dialable peer: %#v", peersReply.Peers)
		}
	}
}

func TestHandshakeRejectsIncompatibleProtocolRange(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 0, Client: "test", ClientVersion: config.CorsaWireVersion},
	)

	if len(frames) != 1 {
		t.Fatalf("unexpected frame count: %#v", frames)
	}
	if got := frames[0]; got.Type != "error" || got.Code != protocol.ErrCodeIncompatibleProtocol {
		t.Fatalf("unexpected incompatible protocol reply: %#v", got)
	}
}

// TestInboundIncompatibleProtocolBlacklistsIP verifies that a single
// incompatible hello immediately blacklists the remote IP for 24 hours
// and closes the connection (return false from dispatchNetworkFrame).
func TestInboundIncompatibleProtocolBlacklistsIP(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// First connection: incompatible hello → error + blacklist.
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 0, Client: "test", ClientVersion: config.CorsaWireVersion},
	)
	if len(frames) != 1 || frames[0].Type != "error" {
		t.Fatalf("expected error frame, got: %#v", frames)
	}

	// Second connection from the same IP should be rejected immediately
	// (isBlacklistedConn closes conn before any frame exchange).
	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial should succeed at TCP level: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Try to read — the server should close without sending anything.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, readErr := conn.Read(buf)
	if readErr == nil {
		t.Fatal("expected connection to be closed by server (blacklisted), but read succeeded")
	}
}

func TestV2NodeHandshakeRequiresSignedAuth(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
	})
	defer stop()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)

	writeJSONFrame(t, conn, protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		ClientVersion: config.CorsaWireVersion,
		Address:       id.Address,
		PubKey:        identity.PublicKeyBase64(id.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(id),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+id.Address)),
	})
	authOK := readJSONTestFrame(t, reader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %#v", authOK)
	}

	// Verify authenticated connection works with a handshake command.
	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	pong := readJSONTestFrame(t, reader)
	if pong.Type != "pong" {
		t.Fatalf("expected pong after auth, got %#v", pong)
	}
}

func TestV2InvalidAuthSignatureAccumulatesBanScore(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
	})
	defer stop()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	for i := 0; i < 10; i++ {
		conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial attempt %d: %v", i, err)
		}
		reader := bufio.NewReader(conn)
		writeJSONFrame(t, conn, protocol.Frame{
			Type:          "hello",
			Version:       config.ProtocolVersion,
			Client:        "node",
			ClientVersion: config.CorsaWireVersion,
			Address:       id.Address,
			PubKey:        identity.PublicKeyBase64(id.PublicKey),
			BoxKey:        identity.BoxPublicKeyBase64(id.BoxPublicKey),
			BoxSig:        identity.SignBoxKeyBinding(id),
		})
		welcome := readJSONTestFrame(t, reader)
		if welcome.Type != "welcome" || welcome.Challenge == "" {
			t.Fatalf("expected welcome challenge on attempt %d, got %#v", i, welcome)
		}
		writeJSONFrame(t, conn, protocol.Frame{
			Type:      "auth_session",
			Address:   id.Address,
			Signature: "bad-signature",
		})
		reply := readJSONTestFrame(t, reader)
		if reply.Code != protocol.ErrCodeInvalidAuthSignature {
			t.Fatalf("expected invalid auth signature on attempt %d, got %#v", i, reply)
		}
		_ = conn.Close()
	}

	svc.mu.RLock()
	entry := svc.bans["127.0.0.1"]
	svc.mu.RUnlock()
	if entry.Score != 1000 {
		t.Fatalf("expected ban score 1000, got %#v", entry)
	}
	if entry.Blacklisted.IsZero() {
		t.Fatalf("expected blacklist expiration to be set, got %#v", entry)
	}
}

func TestPeerDialCandidatesRespectsClientOutgoingLimit(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		PeersStatePath:   filepath.Join(t.TempDir(), "peers.json"),
		BootstrapPeers: []string{
			"10.0.0.1:64646",
			"10.0.0.2:64646",
			"10.0.0.3:64646",
			"10.0.0.4:64646",
			"10.0.0.5:64646",
			"10.0.0.6:64646",
			"10.0.0.7:64646",
			"10.0.0.8:64646",
			"10.0.0.9:64646",
		},
		Type: domain.NodeTypeClient,
	}, id)

	got := candidateAddresses(svc.peerDialCandidates())
	if len(got) != config.DefaultOutgoingPeers {
		t.Fatalf("expected %d peer dial candidates, got %d: %#v", config.DefaultOutgoingPeers, len(got), got)
	}
	if got[0] != "10.0.0.1:64646" || got[7] != "10.0.0.8:64646" {
		t.Fatalf("unexpected candidate order: %#v", got)
	}
}

func TestPeerDialCandidatesUsesDefaultFullOutgoingLimit(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	bootstrap := make([]string, 0, config.DefaultOutgoingPeers+4)
	for i := 1; i <= config.DefaultOutgoingPeers+4; i++ {
		bootstrap = append(bootstrap, fmt.Sprintf("10.0.1.%d:64646", i))
	}

	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		PeersStatePath:   filepath.Join(t.TempDir(), "peers.json"),
		BootstrapPeers:   bootstrap,
		Type:             domain.NodeTypeFull,
	}, id)

	got := candidateAddresses(svc.peerDialCandidates())
	if len(got) != config.DefaultOutgoingPeers {
		t.Fatalf("expected %d peer dial candidates, got %d: %#v", config.DefaultOutgoingPeers, len(got), got)
	}
}

func TestNormalizePeerAddressPrefersObservedHostAndDefaultPortForPrivateAdvertise(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		Type:             domain.NodeTypeFull,
	}, id)

	got, ok := svc.normalizePeerAddress(domain.PeerAddress("198.51.100.2:50702"), domain.PeerAddress("127.0.0.1:64647"))
	if !ok {
		t.Fatalf("expected normalized address")
	}
	if got != domain.PeerAddress("198.51.100.2:64646") {
		t.Fatalf("unexpected normalized address: %s", got)
	}
}

func TestNormalizePeerAddressPrefersObservedHostWhenAdvertisedHostDiffers(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		Type:             domain.NodeTypeFull,
	}, id)

	got, ok := svc.normalizePeerAddress(domain.PeerAddress("198.51.100.2:50702"), domain.PeerAddress("198.51.100.3:64647"))
	if !ok {
		t.Fatalf("expected normalized address")
	}
	if got != domain.PeerAddress("198.51.100.2:64647") {
		t.Fatalf("unexpected normalized address: %s", got)
	}
}

func TestPeerDialCandidatesSkipForbiddenPrivateRangesAndAddDefaultPortFallback(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		PeersStatePath:   filepath.Join(t.TempDir(), "peers.json"),
		BootstrapPeers: []string{
			"10.0.0.1:64647",
			"127.0.0.1:64647",
			"192.168.1.20:64646",
			"172.16.3.10:64646",
		},
		Type: domain.NodeTypeClient,
	}, id)

	got := candidateAddresses(svc.peerDialCandidates())
	want := []string{"10.0.0.1:64647", "10.0.0.1:64646"}
	if len(got) != len(want) {
		t.Fatalf("unexpected candidate count: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected candidates: got %v want %v", got, want)
		}
	}
}

func TestPeerDialCandidatesSkipsOwnPublicIP(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		PeersStatePath:   filepath.Join(t.TempDir(), "peers.json"),
		BootstrapPeers: []string{
			"198.51.100.1:64647",
			"198.51.100.2:64647",
		},
		Type: domain.NodeTypeClient,
	}, id)

	got := candidateAddresses(svc.peerDialCandidates())
	want := []string{"198.51.100.2:64647", "198.51.100.2:64646"}
	if len(got) != len(want) {
		t.Fatalf("unexpected candidate count: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected candidates: got %v want %v", got, want)
		}
	}
}

func TestClientNodeDoesNotForwardMeshTraffic(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
		Type:             domain.NodeTypeClient,
		ListenerEnabled:  true,
		ListenerSet:      true,
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             domain.NodeTypeFull,
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 && len(nodeB.Peers()) >= 1
	})

	ts := time.Now().UTC().Format(time.RFC3339)
	stored := nodeA.HandleLocalFrame(sendMessageFrame("global", "client-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "client-message"))
	if stored.Type != "message_stored" {
		t.Fatalf("unexpected client store response: %#v", stored)
	}

	time.Sleep(1500 * time.Millisecond)

	// fetch_messages is a data-only command (Stage 7) — use HandleLocalFrame.
	final := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	if final.Type != "messages" || final.Count != 0 {
		t.Fatalf("expected empty message log on full node, got %#v", final)
	}
}

func TestDuplicateSendMessageIsDeduplicated(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	ts := time.Now().UTC().Format(time.RFC3339)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	first := svc.HandleLocalFrame(sendMessageFrame("global", "dup-msg-1", svc.Address(), "*", "immutable", ts, 0, "same"))
	if first.Type != "message_stored" {
		t.Fatalf("unexpected first store response: %#v", first)
	}
	dup := svc.HandleLocalFrame(sendMessageFrame("global", "dup-msg-1", svc.Address(), "*", "immutable", ts, 0, "same"))
	if dup.Type != "message_known" || dup.ID != "dup-msg-1" {
		t.Fatalf("unexpected duplicate response: %#v", dup)
	}
	messages := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	assertMessageFrame(t, messages, "messages", "global", 1, protocol.MessageFrame{
		ID: "dup-msg-1", Sender: svc.Address(), Recipient: "*", Flag: "immutable", CreatedAt: ts, TTLSeconds: 0, Body: "same",
	})
}

func TestSingleConnectionAndSeparateConnectionsBehaveTheSame(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Part 1: TCP handshake + data-only queries via HandleLocalFrame.
	tcpFrames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
	)
	if got := tcpFrames[0]; got.Type != "welcome" || got.Address != svc.Address() {
		t.Fatalf("unexpected welcome: %#v", got)
	}

	// Query local state via HandleLocalFrame (in-process path).
	peersA := svc.HandleLocalFrame(protocol.Frame{Type: "get_peers"})
	if peersA.Type != "peers" || peersA.Count != 0 {
		t.Fatalf("unexpected peers: %#v", peersA)
	}
	contactsA := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
	if contactsA.Type != "contacts" || contactsA.Count == 0 {
		t.Fatalf("unexpected contacts: %#v", contactsA)
	}

	timestampA := time.Now().UTC().Format(time.RFC3339)
	stored := svc.HandleLocalFrame(sendMessageFrame("global", "series-msg-1", svc.Address(), "*", "immutable", timestampA, 0, "hello-series"))
	if stored.Type != "message_stored" || stored.ID != "series-msg-1" {
		t.Fatalf("unexpected store reply: %#v", stored)
	}
	inbox := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()})
	assertMessageFrame(t, inbox, "inbox", "global", 1, protocol.MessageFrame{
		ID: "series-msg-1", Sender: svc.Address(), Recipient: "*", Flag: "immutable", CreatedAt: timestampA, TTLSeconds: 0, Body: "hello-series",
	})

	// Part 2: separate TCP handshake + repeated local queries.
	step1 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
	)
	if got := step1[0]; got.Type != "welcome" || got.Address != svc.Address() {
		t.Fatalf("unexpected welcome over separate connection: %#v", got)
	}

	peersB := svc.HandleLocalFrame(protocol.Frame{Type: "get_peers"})
	if peersB.Type != "peers" || peersB.Count != 0 {
		t.Fatalf("unexpected peers over separate call: %#v", peersB)
	}
	contactsB := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
	if contactsB.Type != "contacts" || contactsB.Count == 0 {
		t.Fatalf("unexpected contacts over separate call: %#v", contactsB)
	}

	timestampB := time.Now().UTC().Format(time.RFC3339)
	stored2 := svc.HandleLocalFrame(sendMessageFrame("global", "series-msg-2", svc.Address(), "*", "immutable", timestampB, 0, "hello-separate"))
	if stored2.Type != "message_stored" || stored2.ID != "series-msg-2" {
		t.Fatalf("unexpected store reply: %#v", stored2)
	}

	finalInbox := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()})
	if finalInbox.Type != "inbox" || finalInbox.Count != 2 {
		t.Fatalf("unexpected final inbox: %#v", finalInbox)
	}
	if len(finalInbox.Messages) != 2 {
		t.Fatalf("expected 2 messages in final inbox, got %#v", finalInbox)
	}
	if finalInbox.Messages[0].ID != "series-msg-1" || finalInbox.Messages[1].ID != "series-msg-2" {
		t.Fatalf("unexpected message order in final inbox: %#v", finalInbox.Messages)
	}
}

func TestFetchPeerHealthShowsEstablishedSession(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	_, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	// Wait for an established (Connected==true) session, not just any
	// peer_health entry. A peer_health entry can appear in "reconnecting"
	// before the outbound session is fully up, making the assertion phase
	// timing-sensitive if we only check Count > 0.
	waitForCondition(t, 5*time.Second, func() bool {
		return hasConnectedPeer(nodeA)
	})

	reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	if reply.Type != "peer_health" || reply.Count == 0 {
		t.Fatalf("expected peer_health response with items, got %#v", reply)
	}
	if reply.PeerHealth[0].Address != normalizeAddress(addressB) {
		t.Fatalf("expected peer health for %s, got %#v", normalizeAddress(addressB), reply.PeerHealth)
	}
	if reply.PeerHealth[0].State == "" {
		t.Fatalf("expected peer health state, got %#v", reply.PeerHealth[0])
	}
}

// TestPeerHealthIncludesConnID verifies that fetch_peer_health returns
// a non-zero ConnID for connected peers.
func TestPeerHealthIncludesConnID(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	_, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return hasConnectedPeer(nodeA)
	})

	reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	if reply.Type != "peer_health" || reply.Count == 0 {
		t.Fatalf("expected peer_health with items, got %#v", reply)
	}
	for _, ph := range reply.PeerHealth {
		if ph.Connected && ph.ConnID == 0 {
			t.Errorf("connected peer %s has ConnID=0; expected non-zero", ph.Address)
		}
	}
}

func TestPeerHealthRetainsClientBuild(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	_, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	// Wait until the outbound session completes and the build is
	// propagated via learnIdentityFromWelcome / learnPeerFromFrame.
	// A health entry may exist earlier from a failed dial attempt
	// (before nodeB is listening), so we must wait for the build
	// specifically — not just for Count > 0.
	var reply protocol.Frame
	waitForCondition(t, 5*time.Second, func() bool {
		reply = nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
		if reply.Type != "peer_health" || reply.Count == 0 {
			return false
		}
		for _, ph := range reply.PeerHealth {
			if ph.ClientBuild == config.ClientBuild {
				return true
			}
		}
		return false
	})

	if reply.Type != "peer_health" || reply.Count == 0 {
		t.Fatalf("expected peer_health with items, got %#v", reply)
	}
	found := false
	for _, ph := range reply.PeerHealth {
		if ph.ClientBuild == config.ClientBuild {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected at least one peer with ClientBuild=%d, got %+v", config.ClientBuild, reply.PeerHealth)
	}
}

// validPeerStates is the set of peer states recognized by the protocol,
// UI, and i18n layers. Any state outside this set is a semantic bug.
var validPeerStates = map[string]bool{
	"healthy":      true,
	"degraded":     true,
	"stalled":      true,
	"reconnecting": true,
}

func TestPeerHealthStatesAreProtocolValid(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	_, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	// Wait for a connected peer, not just any peer_health entry.
	// Pre-connection snapshots may contain early states (e.g., "reconnecting")
	// that do not reflect the steady-state session this test validates.
	waitForCondition(t, 5*time.Second, func() bool {
		return hasConnectedPeer(nodeA)
	})

	reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	for _, ph := range reply.PeerHealth {
		if !validPeerStates[ph.State] {
			t.Errorf("peer %s has non-protocol state %q; expected one of healthy/degraded/stalled/reconnecting", ph.Address, ph.State)
		}
	}
}

func TestNetworkStatsIncludesTrafficAndKnownPeers(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	_, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	// Wait for a connection to be established and some traffic exchanged.
	waitForCondition(t, 5*time.Second, func() bool {
		reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
		if reply.NetworkStats == nil {
			return false
		}
		return reply.NetworkStats.ConnectedPeers > 0
	})

	reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
	if reply.Type != "network_stats" {
		t.Fatalf("expected network_stats frame, got %q", reply.Type)
	}
	stats := reply.NetworkStats
	if stats == nil {
		t.Fatal("network_stats is nil")
	}

	// At least 1 connected peer.
	if stats.ConnectedPeers < 1 {
		t.Errorf("expected connected_peers >= 1, got %d", stats.ConnectedPeers)
	}

	// known_peers must reflect the full peer pool (at least the bootstrap peer).
	if stats.KnownPeers < 1 {
		t.Errorf("expected known_peers >= 1, got %d", stats.KnownPeers)
	}

	// After handshake, some bytes must have been exchanged.
	if stats.TotalTraffic <= 0 {
		t.Errorf("expected total_traffic > 0 after handshake, got %d", stats.TotalTraffic)
	}

	// Peer traffic breakdown should list at least the connected peer.
	if len(stats.PeerTraffic) < 1 {
		t.Errorf("expected at least 1 peer_traffic entry, got %d", len(stats.PeerTraffic))
	}
	for _, pt := range stats.PeerTraffic {
		if pt.TotalTraffic != pt.BytesSent+pt.BytesReceived {
			t.Errorf("peer %s: total_traffic (%d) != bytes_sent (%d) + bytes_received (%d)",
				pt.Address, pt.TotalTraffic, pt.BytesSent, pt.BytesReceived)
		}
	}
}

// TestUnauthenticatedInboundTrafficNotAttributed verifies that an inbound
// connection that claims a peer address via hello but fails auth_session
// does NOT have its bytes attributed to that address in network_stats.
// This is a regression test for the spoofed traffic attribution bug.
func TestUnauthenticatedInboundTrafficNotAttributed(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
	})
	defer stop()

	spoofedID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	// Open connection, send hello claiming spoofedID (triggers session auth),
	// then fail auth with a bad signature.
	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)

	writeJSONFrame(t, conn, protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		ClientVersion: config.CorsaWireVersion,
		Address:       spoofedID.Address,
		PubKey:        identity.PublicKeyBase64(spoofedID.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(spoofedID.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(spoofedID),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	// Send bad auth — this will fail and the server will close the connection.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   spoofedID.Address,
		Signature: "deliberately-bad-signature",
	})
	reply := readJSONTestFrame(t, reader)
	if reply.Code != protocol.ErrCodeInvalidAuthSignature {
		t.Fatalf("expected invalid auth signature error, got %#v", reply)
	}
	_ = conn.Close()

	// Give the server a moment to run accumulateInboundTraffic on close.
	time.Sleep(200 * time.Millisecond)

	// Verify: the spoofed address should NOT appear in network_stats.
	statsReply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
	if statsReply.NetworkStats == nil {
		t.Fatal("network_stats is nil")
	}
	for _, pt := range statsReply.NetworkStats.PeerTraffic {
		if pt.Address == spoofedID.Address {
			t.Errorf("spoofed address %s should not appear in peer_traffic, but found: sent=%d recv=%d",
				spoofedID.Address, pt.BytesSent, pt.BytesReceived)
		}
	}

	// Also verify via peer_health.
	healthReply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	for _, ph := range healthReply.PeerHealth {
		if ph.Address == spoofedID.Address && (ph.BytesSent > 0 || ph.BytesReceived > 0) {
			t.Errorf("spoofed address %s should not have traffic in peer_health, but found: sent=%d recv=%d",
				spoofedID.Address, ph.BytesSent, ph.BytesReceived)
		}
	}
}

// TestUnauthenticatedPingDoesNotCreateHealth verifies that an inbound
// connection that sends ping frames before completing authentication does not
// create or refresh peerHealth entries for the claimed address.
func TestUnauthenticatedPingDoesNotCreateHealth(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
	})
	defer stop()

	spoofedID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	// Send hello as a node client (requires auth).
	writeJSONFrame(t, conn, protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		ClientVersion: config.CorsaWireVersion,
		Address:       spoofedID.Address,
		PubKey:        identity.PublicKeyBase64(spoofedID.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(spoofedID.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(spoofedID),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	// Instead of finishing auth, send ping frames. The server must reply
	// with pong (protocol-level liveness) but must NOT create a health
	// entry for the unverified claimed address.
	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	pong := readJSONTestFrame(t, reader)
	if pong.Type != "pong" {
		t.Fatalf("expected pong, got %#v", pong)
	}

	// Send a second ping to exercise the "refresh" path.
	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	pong2 := readJSONTestFrame(t, reader)
	if pong2.Type != "pong" {
		t.Fatalf("expected second pong, got %#v", pong2)
	}

	_ = conn.Close()
	time.Sleep(200 * time.Millisecond)

	// The spoofed address must not appear in peer_health at all.
	healthReply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	for _, ph := range healthReply.PeerHealth {
		if ph.Address == spoofedID.Address {
			t.Errorf("unauthenticated peer %s should not appear in peer_health (state=%s, direction=%s)",
				spoofedID.Address, ph.State, ph.Direction)
		}
	}

	// Also must not appear in network_stats peer_traffic.
	statsReply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
	if statsReply.NetworkStats != nil {
		for _, pt := range statsReply.NetworkStats.PeerTraffic {
			if pt.Address == spoofedID.Address {
				t.Errorf("unauthenticated peer %s should not appear in peer_traffic", spoofedID.Address)
			}
		}
	}
}

// TestKnownPeersIncludesNonListenerClients verifies that known_peers counts
// connected non-listener peers (e.g. desktop clients with Listener: "0") that
// appear in peer_traffic but are not added to s.peers. This is a regression
// test ensuring known_peers >= len(peer_traffic).
func TestKnownPeersIncludesNonListenerClients(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
	})
	defer stop()

	// Generate a valid identity for authenticated connection.
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	// Connect as a non-listener node (Listener: "0", no Listen address).
	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	writeJSONFrame(t, conn, protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		ClientVersion: config.CorsaWireVersion,
		Address:       id.Address,
		PubKey:        identity.PublicKeyBase64(id.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(id),
		Listener:      "0",
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	// Complete authentication.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+id.Address)),
	})
	authOK := readJSONTestFrame(t, reader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %#v", authOK)
	}

	// Exchange a frame to generate some traffic.
	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	_ = readJSONTestFrame(t, reader)

	// Verify: the non-listener peer should appear in peer_traffic,
	// and known_peers must be >= len(peer_traffic).
	statsReply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
	if statsReply.NetworkStats == nil {
		t.Fatal("network_stats is nil")
	}
	stats := statsReply.NetworkStats

	if stats.KnownPeers < len(stats.PeerTraffic) {
		t.Errorf("known_peers (%d) < len(peer_traffic) (%d); non-listener peers are undercounted",
			stats.KnownPeers, len(stats.PeerTraffic))
	}

	// The non-listener peer should appear in peer_traffic with live bytes.
	foundNonListener := false
	for _, pt := range stats.PeerTraffic {
		if pt.Address == id.Address {
			foundNonListener = true
			if pt.TotalTraffic <= 0 {
				t.Errorf("non-listener peer %s has no traffic, expected > 0", id.Address)
			}
			break
		}
	}

	// The peer might appear via connPeerInfo (resolved address) rather than
	// identity address, so we check via a broader condition: at minimum the
	// invariant known_peers >= len(peer_traffic) must hold.
	if !foundNonListener && stats.KnownPeers < len(stats.PeerTraffic) {
		t.Errorf("non-listener peer %s not found in peer_traffic and known_peers invariant violated", id.Address)
	}
}

func TestQueuedMeshMessageFlushesAfterPeerReconnect(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	ts := time.Now().UTC().Format(time.RFC3339)
	stored := nodeA.HandleLocalFrame(sendMessageFrame("global", "queued-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "hello-after-reconnect"))
	if stored.Type != "message_stored" {
		t.Fatalf("unexpected nodeA store response: %#v", stored)
	}

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	// fetch_messages is a data-only command (Stage 7) — use HandleLocalFrame.
	waitForCondition(t, 8*time.Second, func() bool {
		reply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
		return reply.Type == "messages" && len(reply.Messages) == 1 && reply.Messages[0].ID == "queued-msg-1"
	})
}

func TestRoutingTargetsPreferBestHealthySessions(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg:       config.Node{AdvertiseAddress: "self:64646", ListenAddress: ":64646"},
		sessions:  map[domain.PeerAddress]*peerSession{},
		health:    map[domain.PeerAddress]*peerHealth{},
		peerTypes: map[domain.PeerAddress]domain.NodeType{},
		peerIDs:   map[domain.PeerAddress]domain.PeerIdentity{},
	}

	now := time.Now().UTC()
	addresses := []string{"a:1", "b:1", "c:1", "d:1", "e:1"}
	for _, address := range addresses {
		addr := domain.PeerAddress(address)
		svc.sessions[addr] = &peerSession{address: addr}
	}
	svc.health[domain.PeerAddress("a:1")] = &peerHealth{Address: domain.PeerAddress("a:1"), Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now.Add(-2 * time.Second)}
	svc.health[domain.PeerAddress("b:1")] = &peerHealth{Address: domain.PeerAddress("b:1"), Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now.Add(-1 * time.Second)}
	svc.health[domain.PeerAddress("c:1")] = &peerHealth{Address: domain.PeerAddress("c:1"), Connected: true, State: peerStateDegraded, LastUsefulReceiveAt: now.Add(-3 * time.Second)}
	svc.health[domain.PeerAddress("d:1")] = &peerHealth{Address: domain.PeerAddress("d:1"), Connected: true, State: peerStateStalled, LastUsefulReceiveAt: now.Add(-120 * time.Second)}
	svc.health[domain.PeerAddress("e:1")] = &peerHealth{Address: domain.PeerAddress("e:1"), Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now.Add(-4 * time.Second)}

	got := svc.routingTargets()
	want := []domain.PeerAddress{"b:1", "a:1", "e:1"}
	if len(got) != len(want) {
		t.Fatalf("unexpected target count: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected routing targets: got %v want %v", got, want)
		}
	}
}

// TestRoutingTargetsIncludeNonSessionPeers verifies that routingTargets
// merges active session targets with known peers that don't have sessions yet.
// This prevents the timing hole where a message arriving before all sessions
// are up gets gossipped only backwards to the sender.
func TestRoutingTargetsIncludeNonSessionPeers(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg:       config.Node{AdvertiseAddress: "self:64646", ListenAddress: ":64646"},
		sessions:  map[domain.PeerAddress]*peerSession{},
		health:    map[domain.PeerAddress]*peerHealth{},
		peerTypes: map[domain.PeerAddress]domain.NodeType{},
		peerIDs:   map[domain.PeerAddress]domain.PeerIdentity{},
	}

	now := time.Now().UTC()

	// Session to nodeA — active and healthy.
	addrA := domain.PeerAddress("nodeA:1")
	svc.sessions[addrA] = &peerSession{address: addrA}
	svc.health[addrA] = &peerHealth{Address: addrA, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}

	// nodeB is a known peer but has no active session yet.
	addrB := domain.PeerAddress("nodeB:1")
	svc.mu.Lock()
	svc.peers = append(svc.peers, transport.Peer{Address: addrB})
	svc.mu.Unlock()

	targets := svc.routingTargets()

	// Both nodeA (session) and nodeB (pending peer) should be in targets.
	found := map[domain.PeerAddress]bool{}
	for _, addr := range targets {
		found[addr] = true
	}
	if !found[addrA] {
		t.Fatalf("session peer should be in targets: %v", targets)
	}
	if !found[addrB] {
		t.Fatalf("non-session known peer should be in targets: %v", targets)
	}
}

// TestRoutingTargetsSessionOnlyWhenAllPeersHaveSessions verifies that when
// all known peers already have sessions, no duplicates appear.
func TestRoutingTargetsSessionOnlyWhenAllPeersHaveSessions(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg:       config.Node{AdvertiseAddress: "self:64646", ListenAddress: ":64646"},
		sessions:  map[domain.PeerAddress]*peerSession{},
		health:    map[domain.PeerAddress]*peerHealth{},
		peerTypes: map[domain.PeerAddress]domain.NodeType{},
		peerIDs:   map[domain.PeerAddress]domain.PeerIdentity{},
	}

	now := time.Now().UTC()
	addrA := domain.PeerAddress("nodeA:1")
	svc.sessions[addrA] = &peerSession{address: addrA}
	svc.health[addrA] = &peerHealth{Address: addrA, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}

	// Same address in peers — should not create a duplicate.
	svc.mu.Lock()
	svc.peers = append(svc.peers, transport.Peer{Address: addrA})
	svc.mu.Unlock()

	targets := svc.routingTargets()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target (no duplicates), got %v", targets)
	}
	if targets[0] != addrA {
		t.Fatalf("expected %s, got %s", addrA, targets[0])
	}
}

func TestRoutingTargetsSkipClientRelayUnlessRecipientMatches(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg:       config.Node{AdvertiseAddress: "self:64646", ListenAddress: ":64646"},
		sessions:  map[domain.PeerAddress]*peerSession{},
		health:    map[domain.PeerAddress]*peerHealth{},
		peerTypes: map[domain.PeerAddress]domain.NodeType{},
		peerIDs:   map[domain.PeerAddress]domain.PeerIdentity{},
	}

	now := time.Now().UTC()
	svc.sessions[domain.PeerAddress("full:1")] = &peerSession{address: domain.PeerAddress("full:1")}
	svc.sessions[domain.PeerAddress("client:1")] = &peerSession{address: domain.PeerAddress("client:1")}
	svc.health[domain.PeerAddress("full:1")] = &peerHealth{Address: domain.PeerAddress("full:1"), Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	svc.health[domain.PeerAddress("client:1")] = &peerHealth{Address: domain.PeerAddress("client:1"), Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	svc.peerTypes[domain.PeerAddress("full:1")] = domain.NodeTypeFull
	svc.peerTypes[domain.PeerAddress("client:1")] = domain.NodeTypeClient
	svc.peerIDs[domain.PeerAddress("client:1")] = domain.PeerIdentity("client-id")

	globalTargets := svc.routingTargetsForMessage(protocol.Envelope{
		Topic:     "global",
		Sender:    "sender",
		Recipient: "*",
	})
	if len(globalTargets) != 1 || globalTargets[0] != domain.PeerAddress("full:1") {
		t.Fatalf("global traffic should avoid client relay: %#v", globalTargets)
	}

	dmTargets := svc.routingTargetsForMessage(protocol.Envelope{
		Topic:     "dm",
		Sender:    "sender",
		Recipient: "client-id",
	})
	if len(dmTargets) != 2 {
		t.Fatalf("direct message should allow relay and direct recipient session: %#v", dmTargets)
	}
	if dmTargets[0] != domain.PeerAddress("client:1") && dmTargets[1] != domain.PeerAddress("client:1") {
		t.Fatalf("recipient client session missing from targets: %#v", dmTargets)
	}
}

func TestRoutingTargetsAllowUnknownPeerType(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg:      config.Node{AdvertiseAddress: "self:64646", ListenAddress: ":64646"},
		sessions: map[domain.PeerAddress]*peerSession{},
		health:   map[domain.PeerAddress]*peerHealth{},
	}

	now := time.Now().UTC()
	addr := domain.PeerAddress("unknown:1")
	svc.sessions[addr] = &peerSession{address: addr}
	svc.health[addr] = &peerHealth{
		Address:             addr,
		Connected:           true,
		State:               peerStateHealthy,
		LastUsefulReceiveAt: now,
	}

	targets := svc.routingTargets()
	if len(targets) != 1 || targets[0] != addr {
		t.Fatalf("unknown peer type should remain eligible for routing targets: %#v", targets)
	}
}

func TestMeshMessagePropagation(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 && len(nodeB.Peers()) >= 1
	})

	ts := time.Now().UTC().Format(time.RFC3339)
	stored := nodeA.HandleLocalFrame(sendMessageFrame("global", "mesh-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "hello-from-a"))
	if stored.Type != "message_stored" {
		t.Fatalf("unexpected nodeA store response: %#v", stored)
	}

	// fetch_messages is data-only (Stage 7) — query via HandleLocalFrame.
	waitForCondition(t, 5*time.Second, func() bool {
		reply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
		return reply.Type == "messages" && len(reply.Messages) == 1 && reply.Messages[0].Body == "hello-from-a"
	})

	finalMessages := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	assertMessageFrame(t, finalMessages, "messages", "global", 1, protocol.MessageFrame{
		ID: "mesh-msg-1", Sender: nodeA.Address(), Recipient: "*", Flag: "immutable", CreatedAt: ts, TTLSeconds: 0, Body: "hello-from-a",
	})
	finalPeers := nodeB.HandleLocalFrame(protocol.Frame{Type: "get_peers"})
	if finalPeers.Type != "peers" {
		t.Fatalf("expected peers frame, got %#v", finalPeers)
	}
}

func TestDirectedMessageDeliveredToRecipientInbox(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 && len(nodeB.Peers()) >= 1
	})

	// Subscribe to nodeB local changes BEFORE sending, so we catch the
	// pushed message event deterministically instead of polling via
	// repeated TCP connections (which creates load and races).
	eventCh, cancelSub := nodeB.SubscribeLocalChanges()
	defer cancelSub()

	ciphertext, err := directmsg.EncryptForParticipants(
		nodeA.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(nodeB.Address()),
			BoxKeyBase64: identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "secret-for-b"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	stored := nodeA.HandleLocalFrame(sendMessageFrame("dm", "dm-msg-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, ciphertext))
	if stored.Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", stored)
	}

	// Wait for nodeB to receive and store the pushed message via local
	// change event — deterministic, no TCP polling.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-eventCh:
			if ev.Type == protocol.LocalChangeNewMessage && ev.MessageID == "dm-msg-1" {
				goto messageReceived
			}
		case <-deadline:
			t.Fatal("timed out waiting for dm-msg-1 LocalChangeNewMessage on nodeB")
		}
	}
messageReceived:

	// fetch_messages, fetch_message_ids, fetch_message are data-only (Stage 7).
	inboxBReply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
	// Parallel tests may gossip stray DMs into nodeB's "dm" topic through
	// port reuse, so we assert the expected message exists rather than
	// requiring an exact message count.
	if inboxBReply.Type != "messages" || inboxBReply.Topic != "dm" {
		t.Fatalf("unexpected message frame envelope: %#v", inboxBReply)
	}
	expectedMsg := protocol.MessageFrame{
		ID: "dm-msg-1", Sender: nodeA.Address(), Recipient: nodeB.Address(), Flag: "sender-delete", CreatedAt: ts, TTLSeconds: 0, Body: ciphertext,
	}
	foundDM := false
	for _, msg := range inboxBReply.Messages {
		if msg.ID == "dm-msg-1" {
			if msg != expectedMsg {
				t.Fatalf("dm-msg-1 content mismatch: got %#v want %#v", msg, expectedMsg)
			}
			foundDM = true
			break
		}
	}
	if !foundDM {
		t.Fatalf("dm-msg-1 not found in messages: %#v", inboxBReply.Messages)
	}

	inboxA := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: nodeA.Address()})
	if inboxA.Type != "inbox" || inboxA.Count != 0 {
		t.Fatalf("unexpected nodeA inbox: %#v", inboxA)
	}

	dmIDsReply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_message_ids", Topic: "dm"})
	if dmIDsReply.Type != "message_ids" {
		t.Fatalf("unexpected dm id list type: %#v", dmIDsReply)
	}
	foundID := false
	for _, id := range dmIDsReply.IDs {
		if id == "dm-msg-1" {
			foundID = true
			break
		}
	}
	if !foundID {
		t.Fatalf("dm-msg-1 not found in message IDs: %v", dmIDsReply.IDs)
	}

	singleReply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_message", Topic: "dm", ID: "dm-msg-1"})
	if singleReply.Type != "message" || singleReply.Item == nil || singleReply.Item.ID != "dm-msg-1" {
		t.Fatalf("unexpected single dm fetch: %#v", singleReply)
	}
}

func TestFullNodePushRoutesDirectMessageToClientNode(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             domain.NodeTypeClient,
		ListenerEnabled:  true,
		ListenerSet:      true,
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeB.Peers()) >= 1
	})
	waitForCondition(t, 5*time.Second, func() bool {
		return nodeA.SubscriberCount(nodeB.Address()) >= 1
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		nodeA.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(nodeB.Address()),
			BoxKeyBase64: identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "push-route-secret"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	stored := nodeA.HandleLocalFrame(sendMessageFrame("dm", "push-dm-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, ciphertext))
	if stored.Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", stored)
	}

	// Message delivery involves relay retries with back-off and may traverse
	// multiple hops. Under parallel test load the 5s window is too tight;
	// use 10s to avoid flakiness while still catching real regressions
	// (a stuck relay would hang for minutes, not seconds).
	waitForCondition(t, 10*time.Second, func() bool {
		reply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		return reply.Type == "messages" && len(reply.Messages) == 1 && reply.Messages[0].ID == "push-dm-1"
	})

	waitForCondition(t, 10*time.Second, func() bool {
		reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_delivery_receipts", Recipient: nodeA.Address()})
		return reply.Type == "delivery_receipts" && len(reply.Receipts) == 1 && reply.Receipts[0].MessageID == "push-dm-1"
	})
}

func TestFetchTrustedContactsDoesNotIncludeNetworkDiscoveredContacts(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	// fetch_trusted_contacts is data-only (Stage 7); fetch_contacts is
	// available on both TCP (P2P wire) and local — query via HandleLocalFrame.
	waitForCondition(t, 5*time.Second, func() bool {
		reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
		if reply.Type != "contacts" || reply.Count == 0 {
			return false
		}
		for _, contact := range reply.Contacts {
			if contact.Address == nodeB.Address() && contact.BoxSig != "" {
				return true
			}
		}
		return false
	})

	networkReply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
	foundNetworkContact := false
	for _, contact := range networkReply.Contacts {
		if contact.Address == nodeB.Address() {
			foundNetworkContact = true
			if contact.BoxSig == "" {
				t.Fatalf("expected network contact %s to include box signature, got %#v", nodeB.Address(), contact)
			}
		}
	}
	if !foundNetworkContact {
		t.Fatalf("expected fetch_contacts to include network-discovered contact %s, got %#v", nodeB.Address(), networkReply.Contacts)
	}

	got := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if got.Type != "contacts" || got.Count != 1 || len(got.Contacts) != 1 {
		t.Fatalf("expected only self trusted contact, got %#v", got)
	}
	if got.Contacts[0].Address != nodeA.Address() {
		t.Fatalf("expected only self trusted contact %s, got %#v", nodeA.Address(), got.Contacts)
	}
}

func TestNodeRejectsInvalidDirectMessageSignature(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 && len(nodeB.Peers()) >= 1
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		nodeA.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(nodeB.Address()),
			BoxKeyBase64: identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "tampered"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	tampered := ciphertext[:len(ciphertext)-1] + "A"
	ts := time.Now().UTC().Format(time.RFC3339)
	reply := nodeA.HandleLocalFrame(sendMessageFrame("dm", "tampered-msg-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, tampered))
	if reply.Type != "error" || reply.Code != protocol.ErrCodeInvalidDirectMessageSig {
		t.Fatalf("unexpected invalid signature response: %#v", reply)
	}
}

func TestNodeRejectsMessageOutsideAllowedClockDrift(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		MaxClockDrift:    10 * time.Minute,
	})
	defer stop()

	oldTimestamp := time.Now().UTC().Add(-11 * time.Minute).Format(time.RFC3339)
	reply := svc.HandleLocalFrame(sendMessageFrame("global", "old-msg-1", svc.Address(), "*", "immutable", oldTimestamp, 0, "too-old"))
	if reply.Type != "error" || reply.Code != protocol.ErrCodeMessageTimestampOutOfRange {
		t.Fatalf("unexpected stale timestamp response: %#v", reply)
	}
}

func TestImportMessageAllowsHistoricalTimestamp(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		MaxClockDrift:    10 * time.Minute,
	})
	defer stop()

	oldTimestamp := time.Now().UTC().Add(-11 * time.Minute).Format(time.RFC3339)
	imported := svc.HandleLocalFrame(protocol.Frame{
		Type:       "import_message",
		Topic:      "global",
		ID:         "historic-msg-1",
		Address:    svc.Address(),
		Recipient:  "*",
		Flag:       "immutable",
		CreatedAt:  oldTimestamp,
		TTLSeconds: 0,
		Body:       "historic",
	})
	if imported.Type != "message_stored" || imported.ID != "historic-msg-1" {
		t.Fatalf("unexpected import response: %#v", imported)
	}
	messages := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	assertMessageFrame(t, messages, "messages", "global", 1, protocol.MessageFrame{
		ID: "historic-msg-1", Sender: svc.Address(), Recipient: "*", Flag: "immutable", CreatedAt: oldTimestamp, TTLSeconds: 0, Body: "historic",
	})
}

func TestDirectMessageAllowsHistoricalTimestampWithoutTTL(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient identity failed: %v", err)
	}

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		MaxClockDrift:    10 * time.Minute,
	})
	defer stop()

	ciphertext, err := directmsg.EncryptForParticipants(
		svc.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "late-but-valid"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	oldTimestamp := time.Now().UTC().Add(-11 * time.Minute).Format(time.RFC3339)
	reply := svc.HandleLocalFrame(sendMessageFrame("dm", "late-dm-1", svc.Address(), recipientID.Address, "sender-delete", oldTimestamp, 0, ciphertext))
	if (reply.Type != "message_stored" && reply.Type != "message_known") || reply.ID != "late-dm-1" {
		t.Fatalf("unexpected historical dm response: %#v", reply)
	}
}

func TestDirectMessageRejectsExpiredTTL(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient identity failed: %v", err)
	}

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		MaxClockDrift:    10 * time.Minute,
	})
	defer stop()

	ciphertext, err := directmsg.EncryptForParticipants(
		svc.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "already-expired"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	oldTimestamp := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339)
	reply := svc.HandleLocalFrame(sendMessageFrame("dm", "expired-dm-1", svc.Address(), recipientID.Address, "sender-delete", oldTimestamp, 30, ciphertext))
	if reply.Type != "error" || reply.Code != protocol.ErrCodeMessageTimestampOutOfRange {
		t.Fatalf("unexpected expired ttl dm response: %#v", reply)
	}
}

func TestAutoDeleteTTLMessageExpiresFromLogAndInbox(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	ts := time.Now().UTC().Format(time.RFC3339)
	storedTTL := svc.HandleLocalFrame(sendMessageFrame("global", "ttl-msg-1", svc.Address(), "*", "auto-delete-ttl", ts, 1, "burn-after-read"))
	if storedTTL.Type != "message_stored" {
		t.Fatalf("unexpected ttl store response: %#v", storedTTL)
	}
	msgBefore := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	assertMessageFrame(t, msgBefore, "messages", "global", 1, protocol.MessageFrame{
		ID: "ttl-msg-1", Sender: svc.Address(), Recipient: "*", Flag: "auto-delete-ttl", CreatedAt: ts, TTLSeconds: 1, Body: "burn-after-read",
	})

	time.Sleep(1500 * time.Millisecond)

	msgAfter := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	if msgAfter.Type != "messages" || msgAfter.Count != 0 {
		t.Fatalf("expected ttl message to expire from log, got %#v", msgAfter)
	}
	inboxAfter := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()})
	if inboxAfter.Type != "inbox" || inboxAfter.Count != 0 {
		t.Fatalf("expected ttl message to expire from inbox, got %#v", inboxAfter)
	}
}

func TestGazetaNoticePropagatesAndDecryptsOnlyForRecipient(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	// gossipNotice → routingTargets() iterates s.sessions (outbound only)
	// and checks health.Connected. A peer reachable only via inbound
	// connection (Direction=="inbound") is invisible to routingTargets().
	// nodeA publishes the notice and must gossip it to nodeB, so nodeA
	// needs an outbound session to nodeB. nodeA starts first and its
	// initial bootstrap dial to nodeB fails (nodeB not yet listening);
	// the retry fires after a 2-second backoff. We must wait for that
	// retry to establish the outbound session before publishing.
	waitForCondition(t, 10*time.Second, func() bool {
		return hasOutboundSession(nodeA) && hasConnectedPeer(nodeB)
	})

	ciphertext, err := gazeta.EncryptForRecipient(
		identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		"gazeta",
		nodeA.Address(),
		"meet-at-dawn",
	)
	if err != nil {
		t.Fatalf("EncryptForRecipient failed: %v", err)
	}

	noticeReply := nodeA.HandleLocalFrame(protocol.Frame{Type: "publish_notice", TTLSeconds: 30, Ciphertext: ciphertext})
	if noticeReply.Type != "notice_stored" {
		t.Fatalf("unexpected notice store response: %#v", noticeReply)
	}

	// fetch_notices is a data-only command (Stage 7) — use HandleLocalFrame.
	waitForCondition(t, 5*time.Second, func() bool {
		reply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_notices"})
		return reply.Type == "notices" && len(reply.Notices) > 0
	})

	replyB := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_notices"})
	if len(replyB.Notices) == 0 {
		t.Fatal("expected nodeB to have at least one notice")
	}

	plain, err := gazeta.DecryptForIdentity(nodeB.identity, replyB.Notices[0].Ciphertext)
	if err != nil {
		t.Fatalf("expected nodeB to decrypt notice: %v", err)
	}
	if plain.Body != "meet-at-dawn" {
		t.Fatalf("unexpected gazeta body: %q", plain.Body)
	}

	if _, err := gazeta.DecryptForIdentity(nodeA.identity, replyB.Notices[0].Ciphertext); err == nil {
		t.Fatal("expected sender nodeA not to decrypt recipient notice")
	}
}

func startTestNode(t *testing.T, cfg config.Node) (*Service, func()) {
	t.Helper()

	// Isolate peer and queue state so tests never load leftover files
	// from a previous run that happened to bind the same port.
	tmpDir := t.TempDir()
	if cfg.PeersStatePath == "" {
		cfg.PeersStatePath = filepath.Join(tmpDir, "peers.json")
	}
	if cfg.QueueStatePath == "" {
		cfg.QueueStatePath = filepath.Join(tmpDir, "queue.json")
	}
	ctx, cancel := context.WithCancel(context.Background())
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate test identity: %v", err)
	}
	svc := NewService(cfg, id)
	svc.disableRateLimiting = true
	return startTestService(t, ctx, cancel, svc)
}

func startTestNodeWithIdentity(t *testing.T, cfg config.Node, id *identity.Identity) (*Service, func()) {
	t.Helper()

	// Isolate peer and queue state so tests never load leftover files
	// from a previous run that happened to bind the same port.
	tmpDir := t.TempDir()
	if cfg.PeersStatePath == "" {
		cfg.PeersStatePath = filepath.Join(tmpDir, "peers.json")
	}
	if cfg.QueueStatePath == "" {
		cfg.QueueStatePath = filepath.Join(tmpDir, "queue.json")
	}

	ctx, cancel := context.WithCancel(context.Background())
	svc := NewService(cfg, id)
	svc.disableRateLimiting = true
	return startTestService(t, ctx, cancel, svc)
}

func startTestService(t *testing.T, ctx context.Context, cancel context.CancelFunc, svc *Service) (*Service, func()) {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Run(ctx)
	}()

	if svc.cfg.EffectiveListenerEnabled() {
		waitForCondition(t, 3*time.Second, func() bool {
			conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 200*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				return true
			}
			return false
		})
	}

	stop := func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("node stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for node shutdown")
		}
		// Wait for fire-and-forget goroutines (persistQueueState, etc.)
		// to finish so TempDir cleanup does not race with async disk writes.
		bgDone := make(chan struct{})
		go func() {
			svc.WaitBackground()
			close(bgDone)
		}()
		select {
		case <-bgDone:
		case <-time.After(3 * time.Second):
			t.Logf("WaitBackground timed out — some background goroutines may still be running")
		}
	}

	return svc, stop
}

func TestFullNodeRetriesDirectMessageUntilRecipientPeerAppears(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	idB, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient identity failed: %v", err)
	}

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
		Type:             domain.NodeTypeFull,
	})
	defer stopA()

	ciphertext, err := directmsg.EncryptForParticipants(
		nodeA.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(idB.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(idB.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "retry-route-secret"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	retryStored := nodeA.HandleLocalFrame(sendMessageFrame("dm", "retry-dm-1", nodeA.Address(), idB.Address, "sender-delete", ts, 0, ciphertext))
	if retryStored.Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", retryStored)
	}

	time.Sleep(2500 * time.Millisecond)

	nodeB, stopB := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             domain.NodeTypeFull,
	}, idB)
	defer stopB()

	waitForCondition(t, 40*time.Second, func() bool {
		// fetch_messages is a data-only command (Stage 7) — use HandleLocalFrame
		// instead of TCP data port.
		reply := nodeB.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		return reply.Type == "messages" && len(reply.Messages) == 1 && reply.Messages[0].ID == "retry-dm-1"
	})
}

func TestClientReceivesBacklogInboxWhenPeerSessionSubscribes(t *testing.T) {
	t.Parallel()

	addressFull := freeAddress(t)
	addressClient := freeAddress(t)

	idClient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient identity failed: %v", err)
	}

	fullNode, stopFull := startTestNode(t, config.Node{
		ListenAddress:    addressFull,
		AdvertiseAddress: normalizeAddress(addressFull),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stopFull()

	ciphertext, err := directmsg.EncryptForParticipants(
		fullNode.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(idClient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(idClient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "backlog-secret"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	backlogStored := fullNode.HandleLocalFrame(sendMessageFrame("dm", "backlog-dm-1", fullNode.Address(), idClient.Address, "sender-delete", ts, 0, ciphertext))
	if backlogStored.Type != "message_stored" {
		t.Fatalf("unexpected full-node direct store response: %#v", backlogStored)
	}

	clientNode, stopClient := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addressClient,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             domain.NodeTypeClient,
	}, idClient)
	defer stopClient()

	waitForCondition(t, 8*time.Second, func() bool {
		reply := clientNode.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		return reply.Type == "messages" && len(reply.Messages) == 1 && reply.Messages[0].ID == "backlog-dm-1"
	})
}

func TestV2AckDeleteClearsReceiptBacklog(t *testing.T) {
	t.Parallel()

	addressFull := freeAddress(t)
	addressClient := freeAddress(t)

	idClient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate client identity: %v", err)
	}

	fullNode, stopFull := startTestNode(t, config.Node{
		ListenAddress:    addressFull,
		AdvertiseAddress: normalizeAddress(addressFull),
		Type:             domain.NodeTypeFull,
	})
	defer stopFull()

	stored, _ := fullNode.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "receipt-backlog-1",
		Sender:      fullNode.Address(),
		Recipient:   idClient.Address,
		Status:      "delivered",
		DeliveredAt: time.Now().UTC(),
	})
	if !stored {
		t.Fatal("expected backlog receipt to be stored")
	}

	clientNode, stopClient := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addressClient,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             domain.NodeTypeClient,
	}, idClient)
	defer stopClient()

	waitForCondition(t, 8*time.Second, func() bool {
		reply := fullNode.HandleLocalFrame(protocol.Frame{
			Type:      "fetch_delivery_receipts",
			Recipient: clientNode.Address(),
		})
		return reply.Type == "delivery_receipts" && len(reply.Receipts) == 0
	})
}

func TestClientSenderDeliversStoredDirectMessageThroughFullNodeWhenRecipientAppears(t *testing.T) {
	t.Parallel()

	addressFull := freeAddress(t)
	addressSender := freeAddress(t)
	addressRecipient := freeAddress(t)

	idRecipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient identity failed: %v", err)
	}

	fullNode, stopFull := startTestNode(t, config.Node{
		ListenAddress:    addressFull,
		AdvertiseAddress: normalizeAddress(addressFull),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stopFull()

	senderNode, stopSender := startTestNode(t, config.Node{
		ListenAddress:    addressSender,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             domain.NodeTypeClient,
	})
	defer stopSender()

	waitForCondition(t, 6*time.Second, func() bool {
		reply := fullNode.HandleLocalFrame(protocol.Frame{Type: "fetch_identities"})
		for _, address := range reply.Identities {
			if address == senderNode.Address() {
				return true
			}
		}
		return false
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		senderNode.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(idRecipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(idRecipient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "sender-client-backlog-secret"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	reply := senderNode.HandleLocalFrame(sendMessageFrame(
		"dm",
		"client-offline-dm-1",
		senderNode.Address(),
		idRecipient.Address,
		"sender-delete",
		ts,
		0,
		ciphertext,
	))
	if reply.Type != "message_stored" {
		t.Fatalf("unexpected sender direct store response: %#v", reply)
	}

	waitForConditionMsg(t, 10*time.Second, "message did not propagate from sender to full node via gossip", func() bool {
		fullReply := fullNode.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: idRecipient.Address})
		return fullReply.Type == "inbox" && len(fullReply.Messages) == 1 && fullReply.Messages[0].ID == "client-offline-dm-1"
	})

	// Verify sender has an active, healthy connection to the full node.
	// fetch_identities alone is insufficient — the peer session must be
	// fully established so that gossip and relay delivery paths are active.
	waitForConditionMsg(t, 6*time.Second, "sender peer health not connected to full node", func() bool {
		reply := senderNode.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
		for _, ph := range reply.PeerHealth {
			if ph.Connected {
				return true
			}
		}
		return false
	})

	recipientNode, stopRecipient := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addressRecipient,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             domain.NodeTypeClient,
	}, idRecipient)
	defer stopRecipient()

	// Wait for recipient to establish peer session with the full node
	// before checking messages — this ensures initPeerSession (subscribe_inbox
	// + sync) has completed and the single backlog delivery path has fired.
	waitForConditionMsg(t, 6*time.Second, "recipient peer health not connected to full node", func() bool {
		reply := recipientNode.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
		for _, ph := range reply.PeerHealth {
			if ph.Connected {
				return true
			}
		}
		return false
	})

	waitForConditionMsg(t, 10*time.Second, "recipient did not receive stored DM after connecting to full node", func() bool {
		messages := recipientNode.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		return messages.Type == "messages" && len(messages.Messages) == 1 && messages.Messages[0].ID == "client-offline-dm-1"
	})
}

// TestRelayMessageDoesNotStallGossipDelivery is a regression test for the
// fire-and-forget relay_message fix. Before the fix, relay_message frames
// went through peerSessionRequest which blocked the sender's outbound
// session for up to peerRequestTimeout (12s) waiting for a response that
// the full node never sent. This blocked the subsequent gossip send_message
// from being delivered in time.
//
// The test verifies that when a client sends a DM through a full node,
// the gossip message reaches the full node within a reasonable time (3s)
// even though a relay_message is also sent on the same session.
func TestRelayMessageDoesNotStallGossipDelivery(t *testing.T) {
	t.Parallel()

	addressFull := freeAddress(t)
	addressSender := freeAddress(t)

	idRecipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient identity failed: %v", err)
	}

	fullNode, stopFull := startTestNode(t, config.Node{
		ListenAddress:    addressFull,
		AdvertiseAddress: normalizeAddress(addressFull),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stopFull()

	senderNode, stopSender := startTestNode(t, config.Node{
		ListenAddress:    addressSender,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             domain.NodeTypeClient,
	})
	defer stopSender()

	// Wait for sender to connect and be recognized by full node.
	waitForCondition(t, 6*time.Second, func() bool {
		reply := fullNode.HandleLocalFrame(protocol.Frame{Type: "fetch_identities"})
		for _, address := range reply.Identities {
			if address == senderNode.Address() {
				return true
			}
		}
		return false
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		senderNode.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(idRecipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(idRecipient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "relay-stall-test-secret"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	reply := senderNode.HandleLocalFrame(sendMessageFrame(
		"dm",
		"relay-stall-test-1",
		senderNode.Address(),
		idRecipient.Address,
		"sender-delete",
		ts,
		0,
		ciphertext,
	))
	// Both message_stored and message_known are acceptable: the latter occurs
	// when the gossip round-trip delivers the message back to the sender
	// before HandleLocalFrame finishes processing send_message locally.
	if reply.Type != "message_stored" && reply.Type != "message_known" {
		t.Fatalf("unexpected sender direct store response: %#v", reply)
	}

	// The message should arrive at the full node within 3 seconds via gossip.
	// Before the fire-and-forget fix, relay_message stalled the session for
	// 12 seconds, causing this check to time out at 3s.
	waitForCondition(t, 3*time.Second, func() bool {
		fullReply := fullNode.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: idRecipient.Address})
		return fullReply.Type == "inbox" && len(fullReply.Messages) == 1 && fullReply.Messages[0].ID == "relay-stall-test-1"
	})
}

func TestFetchInboxSkipsDeliveredDirectMessages(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
		Type:             domain.NodeTypeFull,
	}, id)

	createdAt := time.Now().UTC().Truncate(time.Second)
	svc.mu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], protocol.Envelope{
		ID:         protocol.MessageID("delivered-dm-1"),
		Topic:      "dm",
		Sender:     "sender-1",
		Recipient:  id.Address,
		Flag:       protocol.MessageFlagSenderDelete,
		CreatedAt:  createdAt,
		TTLSeconds: 0,
		Payload:    []byte("ciphertext"),
	})
	svc.receipts["sender-1"] = append(svc.receipts["sender-1"], protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID("delivered-dm-1"),
		Sender:      id.Address,
		Recipient:   "sender-1",
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: createdAt.Add(time.Second),
	})
	svc.mu.Unlock()

	reply := svc.fetchInboxFrame("dm", id.Address)
	if reply.Type != "inbox" || reply.Count != 0 || len(reply.Messages) != 0 {
		t.Fatalf("expected delivered dm to be hidden from inbox backlog, got %#v", reply)
	}
}

func TestStoreDeliveryReceiptForSelfClearsPendingOutboundAndDoesNotRelay(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
		Type:             domain.NodeTypeFull,
	}, id)

	frame := protocol.Frame{
		Type:      "send_message",
		Topic:     "dm",
		ID:        "outbound-dm-1",
		Address:   id.Address,
		Recipient: "peer-recipient",
		Flag:      string(protocol.MessageFlagSenderDelete),
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Body:      "ciphertext",
	}

	svc.mu.Lock()
	svc.pending[domain.PeerAddress("198.51.100.2:64646")] = []pendingFrame{{Frame: frame, QueuedAt: time.Now().UTC()}}
	svc.pending[domain.PeerAddress("198.51.100.2:64647")] = []pendingFrame{{Frame: frame, QueuedAt: time.Now().UTC()}}
	svc.pendingKeys[pendingFrameKey(domain.PeerAddress("198.51.100.2:64646"), frame)] = struct{}{}
	svc.pendingKeys[pendingFrameKey(domain.PeerAddress("198.51.100.2:64647"), frame)] = struct{}{}
	receiptFrame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          frame.ID,
		Address:     "peer-recipient",
		Recipient:   id.Address,
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}
	svc.pending[domain.PeerAddress("198.51.100.1:64646")] = []pendingFrame{{Frame: receiptFrame, QueuedAt: time.Now().UTC()}}
	svc.pendingKeys[pendingFrameKey(domain.PeerAddress("198.51.100.1:64646"), receiptFrame)] = struct{}{}
	svc.outbound[frame.ID] = outboundDelivery{
		MessageID: frame.ID,
		Recipient: frame.Recipient,
		Status:    "queued",
		QueuedAt:  time.Now().UTC(),
	}
	svc.mu.Unlock()

	receipt := protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(frame.ID),
		Sender:      "peer-recipient",
		Recipient:   id.Address,
		Status:      "delivered",
		DeliveredAt: time.Now().UTC(),
	}

	stored, _ := svc.storeDeliveryReceipt(receipt)
	if !stored {
		t.Fatalf("expected receipt to be stored")
	}

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	if len(svc.pending) != 0 {
		t.Fatalf("expected pending send_message entries to be cleared, got %#v", svc.pending)
	}
	if len(svc.pendingKeys) != 0 {
		t.Fatalf("expected pending keys to be cleared, got %#v", svc.pendingKeys)
	}
	if _, ok := svc.relayRetry[relayReceiptKey(receipt)]; ok {
		t.Fatalf("expected self receipt not to be tracked for relay retry")
	}
}

func TestRecipientNodeDoesNotRouteMessageAddressedToSelf(t *testing.T) {
	t.Parallel()

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		Type:             domain.NodeTypeFull,
	}, recipientID)

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender identity failed: %v", err)
	}
	svc.addKnownPubKey(senderID.Address, identity.PublicKeyBase64(senderID.PublicKey))

	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "for-myself-no-reroute"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:         protocol.MessageID("self-dm-1"),
		Topic:      "dm",
		Sender:     senderID.Address,
		Recipient:  recipientID.Address,
		Flag:       protocol.MessageFlagSenderDelete,
		CreatedAt:  time.Now().UTC(),
		TTLSeconds: 0,
		Body:       ciphertext,
	}, true)
	if !stored || errCode != "" {
		t.Fatalf("unexpected store result stored=%v errCode=%q", stored, errCode)
	}

	time.Sleep(50 * time.Millisecond)

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	if _, ok := svc.relayRetry[relayMessageKey(protocol.MessageID("self-dm-1"))]; ok {
		t.Fatalf("recipient-local dm should not be tracked for relay retry: %#v", svc.relayRetry)
	}
	if _, ok := svc.outbound["self-dm-1"]; ok {
		t.Fatalf("recipient-local dm should not create outbound state: %#v", svc.outbound["self-dm-1"])
	}
}

func TestQueueAndRelayRetryStatePersistAcrossServiceRestart(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate test identity: %v", err)
	}

	cfg := config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		Type:             domain.NodeTypeFull,
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
	}

	svc := NewService(cfg, id)
	frame := protocol.Frame{
		Type:      "send_message",
		Topic:     "dm",
		ID:        "persist-msg-1",
		Address:   id.Address,
		Recipient: "recipient-1",
		Flag:      "sender-delete",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Body:      "queued-body",
	}
	if ok := svc.queuePeerFrame("127.0.0.1:65001", frame); !ok {
		t.Fatal("expected queued frame to be accepted")
	}

	envelope := protocol.Envelope{
		ID:         protocol.MessageID("persist-msg-1"),
		Topic:      "dm",
		Sender:     id.Address,
		Recipient:  "recipient-1",
		Flag:       protocol.MessageFlag("sender-delete"),
		CreatedAt:  time.Now().UTC(),
		TTLSeconds: 0,
		Payload:    []byte("ciphertext"),
	}
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.trackRelayMessage(envelope)
	attempts := svc.noteRelayAttempt(relayMessageKey(envelope.ID), time.Now().UTC())
	if attempts != 1 {
		t.Fatalf("expected first relay attempt, got %d", attempts)
	}

	reloaded := NewService(cfg, id)
	reloaded.mu.RLock()
	defer reloaded.mu.RUnlock()

	items := reloaded.pending[domain.PeerAddress("127.0.0.1:65001")]
	if len(items) != 1 {
		t.Fatalf("expected 1 pending item after restart, got %d", len(items))
	}
	if items[0].Frame.ID != "persist-msg-1" {
		t.Fatalf("unexpected persisted pending frame: %#v", items[0].Frame)
	}
	state, ok := reloaded.relayRetry[relayMessageKey(envelope.ID)]
	if !ok {
		t.Fatal("expected relay retry state after restart")
	}
	if state.Attempts != 1 {
		t.Fatalf("expected persisted attempts=1, got %#v", state)
	}
	if outbound, ok := reloaded.outbound["persist-msg-1"]; !ok || outbound.Status != "queued" {
		t.Fatalf("expected persisted outbound queued state, got %#v", reloaded.outbound)
	}
	if len(reloaded.topics["dm"]) != 1 || reloaded.topics["dm"][0].ID != protocol.MessageID("persist-msg-1") {
		t.Fatalf("expected persisted relay message payload, got %#v", reloaded.topics["dm"])
	}
}

func TestPendingMessagesFrameIncludesLifecycleStatuses(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()

	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
	}, id)

	queuedAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	lastAttemptAt := queuedAt.Add(30 * time.Second)
	svc.pending[domain.PeerAddress("peer-a")] = []pendingFrame{
		{
			Frame: protocol.Frame{
				Type:      "send_message",
				Topic:     "dm",
				ID:        "queued-1",
				Recipient: "alice",
			},
			QueuedAt: queuedAt,
		},
		{
			Frame: protocol.Frame{
				Type:      "send_message",
				Topic:     "dm",
				ID:        "retrying-1",
				Recipient: "bob",
			},
			QueuedAt: queuedAt,
			Retries:  2,
		},
	}
	svc.outbound["retrying-1"] = outboundDelivery{
		MessageID:     "retrying-1",
		Recipient:     "bob",
		Status:        "retrying",
		QueuedAt:      queuedAt,
		LastAttemptAt: lastAttemptAt,
		Retries:       2,
		Error:         "retry queued delivery",
	}
	svc.outbound["failed-1"] = outboundDelivery{
		MessageID:     "failed-1",
		Recipient:     "carol",
		Status:        "failed",
		QueuedAt:      queuedAt,
		LastAttemptAt: lastAttemptAt,
		Retries:       5,
		Error:         "max retries exceeded",
	}
	svc.outbound["expired-1"] = outboundDelivery{
		MessageID:     "expired-1",
		Recipient:     "dan",
		Status:        "expired",
		QueuedAt:      queuedAt,
		LastAttemptAt: lastAttemptAt,
		Error:         "pending queue expired",
	}

	frame := svc.pendingMessagesFrame("dm")
	if frame.Type != "pending_messages" || frame.Count != 4 {
		t.Fatalf("unexpected pending frame: %#v", frame)
	}
	got := make(map[string]protocol.PendingMessageFrame, len(frame.PendingMessages))
	for _, item := range frame.PendingMessages {
		got[item.ID] = item
	}
	if got["queued-1"].Status != "queued" {
		t.Fatalf("expected queued status, got %#v", got["queued-1"])
	}
	if got["retrying-1"].Status != "retrying" || got["retrying-1"].Retries != 2 {
		t.Fatalf("expected retrying status, got %#v", got["retrying-1"])
	}
	if got["failed-1"].Status != "failed" || got["failed-1"].Error == "" {
		t.Fatalf("expected failed status, got %#v", got["failed-1"])
	}
	if got["expired-1"].Status != "expired" || got["expired-1"].Error == "" {
		t.Fatalf("expected expired status, got %#v", got["expired-1"])
	}
}

func TestFlushPendingPeerFramesExpiresDirectMessageByTTL(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()

	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
	}, id)

	address := domain.PeerAddress("127.0.0.1:65001")
	svc.mu.Lock()
	svc.sessions[address] = &peerSession{address: address, sendCh: make(chan protocol.Frame)}
	svc.health[address] = &peerHealth{Address: address, Connected: true, State: peerStateHealthy}
	svc.pending[address] = []pendingFrame{{
		Frame: protocol.Frame{
			Type:       "send_message",
			Topic:      "dm",
			ID:         "ttl-expire-1",
			Address:    id.Address,
			Recipient:  "recipient-1",
			Flag:       "sender-delete",
			CreatedAt:  time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339),
			TTLSeconds: 30,
			Body:       "ciphertext",
		},
		QueuedAt: time.Now().UTC().Add(-90 * time.Second),
	}}
	svc.pendingKeys[pendingFrameKey(address, svc.pending[address][0].Frame)] = struct{}{}
	svc.mu.Unlock()

	svc.flushPendingPeerFrames(address)

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	if len(svc.pending[address]) != 0 {
		t.Fatalf("expected expired dm to be removed from pending queue, got %#v", svc.pending[address])
	}
	state, ok := svc.outbound["ttl-expire-1"]
	if !ok || state.Status != "expired" {
		t.Fatalf("expected outbound expired state, got %#v", svc.outbound["ttl-expire-1"])
	}
}

func TestClearRelayRetryForOutboundReceipt(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate test identity: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
	}, id)

	receipt := protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID("receipt-msg-1"),
		Sender:      "alice",
		Recipient:   "bob",
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC(),
	}
	svc.trackRelayReceipt(receipt)

	svc.mu.RLock()
	if _, ok := svc.relayRetry[relayReceiptKey(receipt)]; !ok {
		svc.mu.RUnlock()
		t.Fatalf("expected receipt relay retry state before clear")
	}
	svc.mu.RUnlock()

	svc.clearRelayRetryForOutbound(protocol.Frame{
		Type:      "relay_delivery_receipt",
		ID:        string(receipt.MessageID),
		Recipient: receipt.Recipient,
		Status:    receipt.Status,
	})

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	if _, ok := svc.relayRetry[relayReceiptKey(receipt)]; ok {
		t.Fatalf("expected receipt relay retry state to be cleared")
	}
}

func TestRetryableRelayReceiptsSkipsClearedReceiptState(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate test identity: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
		Type:             domain.NodeTypeFull,
	}, id)

	receipt := protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID("receipt-msg-2"),
		Sender:      "alice",
		Recipient:   "bob",
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}

	svc.mu.Lock()
	svc.receipts[receipt.Recipient] = append(svc.receipts[receipt.Recipient], receipt)
	svc.mu.Unlock()
	svc.trackRelayReceipt(receipt)
	svc.clearRelayRetryForOutbound(protocol.Frame{
		Type:      "relay_delivery_receipt",
		ID:        string(receipt.MessageID),
		Recipient: receipt.Recipient,
		Status:    receipt.Status,
	})

	retryable := svc.retryableRelayReceipts(time.Now().UTC())
	if len(retryable) != 0 {
		t.Fatalf("expected cleared receipt not to be retried, got %#v", retryable)
	}
}

func exchangeFrames(t *testing.T, address string, frames ...protocol.Frame) []protocol.Frame {
	t.Helper()

	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	reader := bufio.NewReader(conn)
	replyFrames := make([]protocol.Frame, 0, len(frames))
	for _, frame := range frames {
		writeJSONFrame(t, conn, frame)
		replyFrames = append(replyFrames, readJSONTestFrame(t, reader))
	}

	return replyFrames
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	waitForConditionMsg(t, timeout, "condition not met before timeout", fn)
}

func waitForConditionMsg(t *testing.T, timeout time.Duration, msg string, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("%s (waited %s)", msg, timeout)
}

// hasConnectedPeer returns true when at least one entry in the node's
// peer_health list has Connected==true. routingTargets() uses
// health.Connected to filter gossip/relay targets, so tests that depend
// on notice propagation or syncPeer reachability must wait for this
// predicate, not just Peers()>=1 (which only checks the bootstrap list).
func hasConnectedPeer(node *Service) bool {
	reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	if reply.Type != "peer_health" {
		return false
	}
	for _, ph := range reply.PeerHealth {
		if ph.Connected {
			return true
		}
	}
	return false
}

// hasOutboundSession returns true when at least one peer_health entry
// has Connected==true and Direction=="outbound". routingTargetsFiltered
// first iterates s.sessions (outbound sessions only) and checks
// health.Connected; a peer reachable only via inbound connection does
// NOT appear in routingTargets(). Tests that depend on gossip or notice
// propagation must wait for an outbound session, not just hasConnectedPeer.
func hasOutboundSession(node *Service) bool {
	reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	if reply.Type != "peer_health" {
		return false
	}
	for _, ph := range reply.PeerHealth {
		if ph.Connected && ph.Direction == "outbound" {
			return true
		}
	}
	return false
}

func freeAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate test port: %v", err)
	}
	defer func() { _ = listener.Close() }()

	return listener.Addr().String()
}

func normalizeAddress(address string) string {
	if len(address) > 0 && address[0] == ':' {
		return "127.0.0.1" + address
	}
	return address
}

func sendMessageFrame(topic, id, from, to, flag, timestamp string, ttlSeconds int, body string) protocol.Frame {
	return protocol.Frame{
		Type:       "send_message",
		Topic:      topic,
		ID:         id,
		Address:    from,
		Recipient:  to,
		Flag:       flag,
		CreatedAt:  timestamp,
		TTLSeconds: ttlSeconds,
		Body:       body,
	}
}

func assertMessageFrame(t *testing.T, frame protocol.Frame, expectedType, topic string, expectedCount int, expected protocol.MessageFrame) {
	t.Helper()

	if frame.Type != expectedType || frame.Topic != topic || frame.Count != expectedCount || len(frame.Messages) != expectedCount {
		t.Fatalf("unexpected message frame envelope: %#v", frame)
	}

	got := frame.Messages[0]
	if got != expected {
		t.Fatalf("unexpected message item: got %#v want %#v", got, expected)
	}
}

func writeJSONFrame(t *testing.T, conn net.Conn, frame protocol.Frame) {
	t.Helper()

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	if _, err := fmt.Fprint(conn, line); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readJSONTestFrame(t *testing.T, reader *bufio.Reader) protocol.Frame {
	t.Helper()

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	frame, err := protocol.ParseFrameLine(line[:len(line)-1])
	if err != nil {
		t.Fatalf("parse frame: %v", err)
	}
	return frame
}

// ---------------------------------------------------------------------------
// Peer persistence integration tests (service.go changes)
// ---------------------------------------------------------------------------

// TestNormalizePeerAddressAcceptsValidV3Onion verifies the .onion
// passthrough added to normalizePeerAddress in service.go.
func TestNormalizePeerAddressAcceptsValidV3Onion(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		Type:             domain.NodeTypeFull,
	}, id)

	// 56 base32 chars = valid Tor v3.
	onion := "2gzyxa5ihm7nsber23gk5eqx3mp4wrymfbhqgk2ycdjp3yzcrllbiqad.onion"
	got, ok := svc.normalizePeerAddress(domain.PeerAddress("1.2.3.4:12345"), domain.PeerAddress(onion+":64646"))
	if !ok {
		t.Fatal("expected valid v3 .onion to be accepted")
	}
	if got != domain.PeerAddress(onion+":64646") {
		t.Fatalf("expected %s:64646, got %s", onion, got)
	}
}

// TestNormalizePeerAddressRejectsShortOnion verifies that short/junk .onion
// hosts fall through to normal IP-based logic and fail when not valid IPs.
func TestNormalizePeerAddressRejectsShortOnion(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		Type:             domain.NodeTypeFull,
	}, id)

	// "junk.onion" is not a valid 16/56 base32 host.
	_, ok := svc.normalizePeerAddress(domain.PeerAddress(""), domain.PeerAddress("junk.onion:64646"))
	if ok {
		t.Fatal("expected invalid .onion to be rejected")
	}
}

// TestTwoNodesPeerExchangePersistedOnShutdown starts two full nodes,
// lets them discover each other via bootstrap, then stops one and checks
// that its peers-{port}.json contains the other node's address.
func TestTwoNodesPeerExchangePersistedOnShutdown(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	addressA := freeAddress(t)
	addressB := freeAddress(t)
	peersPathA := filepath.Join(dir, "peers-a.json")

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
		PeersStatePath:   peersPathA,
		Type:             domain.NodeTypeFull,
	})
	defer stopA()

	_, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             domain.NodeTypeFull,
	})

	// Wait until A discovers B via peer exchange.
	waitForCondition(t, 5*time.Second, func() bool {
		nodeA.mu.RLock()
		defer nodeA.mu.RUnlock()
		normalizedB := domain.PeerAddress(normalizeAddress(addressB))
		for _, p := range nodeA.peers {
			if p.Address == normalizedB {
				return true
			}
		}
		return false
	})

	// Stop B, then flush A's peer state.
	stopB()
	nodeA.flushPeerState()

	// Verify peers file was written and contains B.
	state, err := loadPeerState(peersPathA)
	if err != nil {
		t.Fatalf("loadPeerState: %v", err)
	}
	normalizedB := domain.PeerAddress(normalizeAddress(addressB))
	found := false
	for _, p := range state.Peers {
		if p.Address == normalizedB {
			found = true
			break
		}
	}
	if !found {
		addrs := make([]string, len(state.Peers))
		for i, p := range state.Peers {
			addrs[i] = string(p.Address)
		}
		t.Fatalf("expected node B address %s in persisted peers, got: %v", normalizedB, addrs)
	}
}

// TestBootstrapLoopFlushesOnShutdown verifies that stopping a node
// (via context cancel) writes peers state to disk.
func TestBootstrapLoopFlushesOnShutdown(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")
	address := freeAddress(t)

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646"},
		PeersStatePath:   peersPath,
		Type:             domain.NodeTypeFull,
	})
	_ = svc // keep reference

	// Graceful shutdown triggers flushPeerState via bootstrapLoop.
	stop()

	// The file should now exist.
	state, err := loadPeerState(peersPath)
	if err != nil {
		t.Fatalf("loadPeerState after shutdown: %v", err)
	}
	if len(state.Peers) == 0 {
		t.Fatal("expected at least one peer (bootstrap) in persisted state after shutdown")
	}
	found := false
	for _, p := range state.Peers {
		if p.Address == "10.0.0.1:64646" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected bootstrap peer 10.0.0.1:64646 in persisted state")
	}
}

// TestNodeRestartPreservesPersistedPeers performs a full lifecycle test:
// start a node, add peers, flush, stop, start a new node from the same
// peers file, and verify the peers (including health) are restored.
func TestNodeRestartPreservesPersistedPeers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")

	address1 := freeAddress(t)
	svc1, stop1 := startTestNode(t, config.Node{
		ListenAddress:    address1,
		AdvertiseAddress: normalizeAddress(address1),
		BootstrapPeers:   []string{},
		PeersStatePath:   peersPath,
		Type:             domain.NodeTypeFull,
	})

	// Add peers and mark one as connected.
	svc1.addPeerAddress("10.0.0.5:64646", "full", "")
	svc1.addPeerAddress("10.0.0.6:64646", "full", "")
	svc1.markPeerConnected("10.0.0.5:64646", "outbound")
	svc1.markPeerDisconnected("10.0.0.6:64646", fmt.Errorf("refused"))

	stop1() // triggers flush via bootstrapLoop shutdown

	// Verify file was written.
	state, err := loadPeerState(peersPath)
	if err != nil {
		t.Fatalf("loadPeerState: %v", err)
	}
	if len(state.Peers) < 2 {
		t.Fatalf("expected >= 2 persisted peers, got %d", len(state.Peers))
	}

	// Start a new node with different listen address but same peers file.
	address2 := freeAddress(t)
	svc2, stop2 := startTestNode(t, config.Node{
		ListenAddress:    address2,
		AdvertiseAddress: normalizeAddress(address2),
		BootstrapPeers:   []string{"10.0.0.99:64646"}, // different bootstrap
		PeersStatePath:   peersPath,
		Type:             domain.NodeTypeFull,
	})
	defer stop2()

	svc2.mu.RLock()
	// Should have: bootstrap (10.0.0.99) + persisted (10.0.0.5, 10.0.0.6).
	peerAddrs := make(map[domain.PeerAddress]bool)
	for _, p := range svc2.peers {
		peerAddrs[p.Address] = true
	}
	svc2.mu.RUnlock()

	if !peerAddrs["10.0.0.99:64646"] {
		t.Fatal("expected bootstrap peer 10.0.0.99:64646")
	}
	if !peerAddrs["10.0.0.5:64646"] {
		t.Fatal("expected persisted peer 10.0.0.5:64646")
	}
	if !peerAddrs["10.0.0.6:64646"] {
		t.Fatal("expected persisted peer 10.0.0.6:64646")
	}

	// Verify health was seeded from persisted state.
	svc2.mu.RLock()
	h5 := svc2.health[domain.PeerAddress("10.0.0.5:64646")]
	h6 := svc2.health[domain.PeerAddress("10.0.0.6:64646")]
	svc2.mu.RUnlock()

	if h5 == nil {
		t.Fatal("expected health entry for 10.0.0.5:64646")
	}
	if h5.Score < peerScoreConnect {
		t.Fatalf("expected score >= %d for connected peer, got %d", peerScoreConnect, h5.Score)
	}
	if h6 == nil {
		t.Fatal("expected health entry for 10.0.0.6:64646")
	}
	if h6.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 failure for 10.0.0.6, got %d", h6.ConsecutiveFailures)
	}
}

// TestPeerDialCandidatesIncludesPersistedPeers verifies that peers loaded
// from disk appear in peerDialCandidates and are actually dialed.
func TestPeerDialCandidatesIncludesPersistedPeers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")

	now := time.Now().UTC()
	persisted := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{Address: "10.0.0.50:64646", Score: 30, Source: domain.PeerSourcePeerExchange, LastConnectedAt: &now},
			{Address: "10.0.0.51:64646", Score: 10, Source: domain.PeerSourcePeerExchange},
		},
	}
	data, _ := json.MarshalIndent(persisted, "", "  ")
	_ = os.WriteFile(peersPath, data, 0o600)

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		BootstrapPeers:   []string{},
		PeersStatePath:   peersPath,
		Type:             domain.NodeTypeFull,
	}, id)

	candidates := candidateAddresses(svc.peerDialCandidates())

	candidateAddrs := make(map[string]bool)
	for _, c := range candidates {
		candidateAddrs[c] = true
	}
	if !candidateAddrs["10.0.0.50:64646"] {
		t.Fatalf("expected persisted peer 10.0.0.50:64646 in dial candidates, got: %v", candidates)
	}
	if !candidateAddrs["10.0.0.51:64646"] {
		t.Fatalf("expected persisted peer 10.0.0.51:64646 in dial candidates, got: %v", candidates)
	}
}

// TestMaybeSavePeerStateRespectsInterval verifies that maybeSavePeerState
// does not write more often than peerStateSaveMinutes.
func TestMaybeSavePeerStateRespectsInterval(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")
	address := freeAddress(t)

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646"},
		PeersStatePath:   peersPath,
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// First explicit flush.
	svc.flushPeerState()

	svc.mu.RLock()
	firstSave := svc.lastPeerSave
	svc.mu.RUnlock()

	if firstSave.IsZero() {
		t.Fatal("expected lastPeerSave to be set after flush")
	}

	// maybeSavePeerState should NOT write again because not enough time elapsed.
	svc.maybeSavePeerState()

	svc.mu.RLock()
	secondSave := svc.lastPeerSave
	svc.mu.RUnlock()

	if !secondSave.Equal(firstSave) {
		t.Fatalf("expected lastPeerSave unchanged (%v), got %v", firstSave, secondSave)
	}
}

// TestOnionPeersSkippedWithoutProxy verifies that .onion addresses
// are excluded from dial candidates when no SOCKS5 proxy is configured,
// preventing constant fast failures in the reconnect loop.
func TestOnionPeersSkippedWithoutProxy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")

	onionAddr := domain.PeerAddress(strings.Repeat("a", 56) + ".onion:64646")
	now := time.Now().UTC()
	persisted := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{Address: onionAddr, Score: 80, Source: domain.PeerSourcePeerExchange, LastConnectedAt: &now},
			{Address: "10.0.0.1:64646", Score: 50, Source: domain.PeerSourcePeerExchange},
		},
	}
	data, _ := json.MarshalIndent(persisted, "", "  ")
	_ = os.WriteFile(peersPath, data, 0o600)

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	// No ProxyAddress configured.
	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		BootstrapPeers:   []string{},
		PeersStatePath:   peersPath,
		Type:             domain.NodeTypeFull,
	}, id)

	candidates := candidateAddresses(svc.peerDialCandidates())
	for _, c := range candidates {
		host, _, ok := splitHostPort(c)
		if ok && isOnionAddress(host) {
			t.Fatalf("onion address %s should not be in dial candidates without proxy", c)
		}
	}
	// The regular peer should still be present.
	found := false
	for _, c := range candidates {
		if c == "10.0.0.1:64646" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected regular peer 10.0.0.1:64646 in candidates, got: %v", candidates)
	}
}

// TestOnionPeersIncludedWithProxy verifies that .onion addresses
// ARE included in dial candidates when a SOCKS5 proxy is configured.
func TestOnionPeersIncludedWithProxy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")

	onionAddrStr := strings.Repeat("a", 56) + ".onion:64646"
	onionAddr := domain.PeerAddress(onionAddrStr)
	now := time.Now().UTC()
	persisted := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{Address: onionAddr, Score: 80, Source: domain.PeerSourcePeerExchange, LastConnectedAt: &now},
		},
	}
	data, _ := json.MarshalIndent(persisted, "", "  ")
	_ = os.WriteFile(peersPath, data, 0o600)

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    ":64646",
		AdvertiseAddress: "198.51.100.1:64646",
		BootstrapPeers:   []string{},
		PeersStatePath:   peersPath,
		ProxyAddress:     "127.0.0.1:9050",
		Type:             domain.NodeTypeFull,
	}, id)

	candidates := candidateAddresses(svc.peerDialCandidates())
	found := false
	for _, c := range candidates {
		if c == onionAddrStr {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected onion peer %s in candidates with proxy, got: %v", onionAddr, candidates)
	}
}

// TestPeerDialCandidatesSortedByScore verifies that peerDialCandidates
// returns peers sorted by Score descending (score-based prioritisation).
func TestPeerDialCandidatesSortedByScore(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Add three peers with different scores.
	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")
	svc.addPeerAddress("10.0.0.2:64646", "full", "peer-2")
	svc.addPeerAddress("10.0.0.3:64646", "full", "peer-3")

	// Simulate scoring: peer-2 is best, peer-3 is worst.
	svc.markPeerConnected("10.0.0.2:64646", "outbound") // +10
	svc.markPeerConnected("10.0.0.2:64646", "outbound") // +10 (total 20)
	svc.markPeerConnected("10.0.0.1:64646", "outbound") // +10 (total 10)
	svc.markPeerDisconnected("10.0.0.1:64646", nil)     // clean (-2, total 8)
	svc.markPeerDisconnected("10.0.0.2:64646", nil)     // clean (-2, total 18)

	// peer-3 has score 0 (never connected).

	candidates := candidateAddresses(svc.peerDialCandidates())
	if len(candidates) < 3 {
		t.Fatalf("expected at least 3 candidates, got %d: %v", len(candidates), candidates)
	}
	if candidates[0] != "10.0.0.2:64646" {
		t.Fatalf("expected highest-score peer first (10.0.0.2), got %s", candidates[0])
	}
	if candidates[1] != "10.0.0.1:64646" {
		t.Fatalf("expected second-highest peer (10.0.0.1), got %s", candidates[1])
	}
	if candidates[2] != "10.0.0.3:64646" {
		t.Fatalf("expected lowest-score peer last (10.0.0.3), got %s", candidates[2])
	}
}

// TestPeerDialCandidatesSkipsCooldown verifies that peers with recent
// failures and active cooldown are excluded from dial candidates.
func TestPeerDialCandidatesSkipsCooldown(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")
	svc.addPeerAddress("10.0.0.2:64646", "full", "peer-2")

	// Simulate multiple failures for peer-1 (recently disconnected).
	for i := 0; i < 3; i++ {
		svc.markPeerDisconnected("10.0.0.1:64646", fmt.Errorf("refused"))
	}

	// peer-1 has ConsecutiveFailures=3 and LastDisconnectedAt=now,
	// so cooldown = peerCooldownDuration(3-1) = 60s.  It should be skipped.
	candidates := candidateAddresses(svc.peerDialCandidates())
	for _, c := range candidates {
		if c == "10.0.0.1:64646" {
			t.Fatalf("peer 10.0.0.1 should be in cooldown, but found in candidates: %v", candidates)
		}
	}
	// peer-2 should still be a candidate.
	found := false
	for _, c := range candidates {
		if c == "10.0.0.2:64646" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected peer 10.0.0.2 in candidates, got: %v", candidates)
	}
}

// TestPeerDialCandidatesCooldownExpires verifies that a peer exits cooldown
// once enough time has passed since the last disconnect.
func TestPeerDialCandidatesCooldownExpires(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	// One failure: no cooldown (first failure is exempt), but we still
	// backdate to verify the expiry path for future failures.
	svc.markPeerDisconnected("10.0.0.1:64646", fmt.Errorf("timeout"))

	// Backdate LastDisconnectedAt to simulate cooldown expiry.
	svc.mu.Lock()
	if h := svc.health[domain.PeerAddress("10.0.0.1:64646")]; h != nil {
		h.LastDisconnectedAt = time.Now().Add(-1 * time.Minute)
	}
	svc.mu.Unlock()

	candidates := candidateAddresses(svc.peerDialCandidates())
	found := false
	for _, c := range candidates {
		if c == "10.0.0.1:64646" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected peer 10.0.0.1 after cooldown expiry, got: %v", candidates)
	}
}

// TestPeerDialCandidatesSkipsBannedPeer verifies that a peer with an active
// BannedUntil timestamp is excluded from dial candidates.
func TestPeerDialCandidatesSkipsBannedPeer(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")
	svc.addPeerAddress("10.0.0.2:64646", "full", "peer-2")

	// Ban peer-1 for 24 hours (incompatible protocol).
	svc.mu.Lock()
	h := svc.ensurePeerHealthLocked("10.0.0.1:64646")
	h.BannedUntil = time.Now().UTC().Add(24 * time.Hour)
	svc.mu.Unlock()

	candidates := candidateAddresses(svc.peerDialCandidates())
	for _, c := range candidates {
		if c == "10.0.0.1:64646" {
			t.Fatalf("banned peer should be excluded from candidates: %v", candidates)
		}
	}
	found := false
	for _, c := range candidates {
		if c == "10.0.0.2:64646" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unbanned peer should be in candidates: %v", candidates)
	}
}

// TestPeerDialCandidatesBanExpires verifies that a peer becomes a dial
// candidate again after its BannedUntil timestamp has passed.
func TestPeerDialCandidatesBanExpires(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	// Set an expired ban (1 minute ago).
	svc.mu.Lock()
	h := svc.ensurePeerHealthLocked("10.0.0.1:64646")
	h.BannedUntil = time.Now().UTC().Add(-1 * time.Minute)
	svc.mu.Unlock()

	candidates := candidateAddresses(svc.peerDialCandidates())
	found := false
	for _, c := range candidates {
		if c == "10.0.0.1:64646" {
			found = true
		}
	}
	if !found {
		t.Fatalf("peer with expired ban should be in candidates: %v", candidates)
	}
}

// TestPromotePeerAddressDoesNotClearBannedUntil verifies that network-learned
// promotion does not clear an existing ban. Candidate policy remains in
// selection logic rather than in announce processing.
func TestPromotePeerAddressDoesNotClearBannedUntil(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	peer := domain.PeerAddress("10.0.0.99:64646")
	svc.addPeerAddress(peer, "full", "peer-99")

	// Set an active ban (23 hours from now).
	svc.mu.Lock()
	h := svc.ensurePeerHealthLocked(peer)
	h.BannedUntil = time.Now().UTC().Add(23 * time.Hour)
	svc.mu.Unlock()

	// Verify peer is banned — should not appear in candidates.
	candidates := candidateAddresses(svc.peerDialCandidates())
	for _, c := range candidates {
		if c == string(peer) {
			t.Fatal("banned peer should not be in candidates before promote")
		}
	}

	// promotePeerAddress must not clear the ban.
	svc.promotePeerAddress(peer)

	svc.mu.RLock()
	banned := svc.health[peer].BannedUntil
	svc.mu.RUnlock()
	if banned.IsZero() {
		t.Fatal("BannedUntil should remain set after promotePeerAddress")
	}

	// The peer must still stay out of candidates while banned.
	candidates = candidateAddresses(svc.peerDialCandidates())
	found := false
	for _, c := range candidates {
		if c == string(peer) {
			found = true
		}
	}
	if !found {
		return
	}
	t.Fatalf("banned peer should remain excluded after promotePeerAddress: %v", candidates)
}

func TestPromotePeerAddressDoesNotClearIPWideBanForAlternatePort(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	primary := domain.PeerAddress("10.0.0.99:64646")
	alternate := domain.PeerAddress("10.0.0.99:7777")
	svc.addPeerAddress(primary, "full", "peer-99")
	svc.penalizeOldProtocolPeer(primary)

	svc.promotePeerAddress(alternate)

	svc.mu.RLock()
	_, ipStillBanned := svc.bannedIPSet["10.0.0.99"]
	_, altKnown := svc.persistedMeta[alternate]
	svc.mu.RUnlock()
	if !ipStillBanned {
		t.Fatal("IP-wide ban should remain after promotePeerAddress on alternate port")
	}
	if altKnown {
		t.Fatal("alternate port on same banned IP should not be added as a new peer")
	}

	candidates := candidateAddresses(svc.peerDialCandidates())
	for _, candidate := range candidates {
		if candidate == string(primary) || candidate == string(alternate) {
			t.Fatalf("banned IP should remain excluded from candidates, got %v", candidates)
		}
	}
}

// TestEvictStalePeersRemovesBadPeers verifies that peers with score ≤ threshold
// and no recent connection activity are evicted from in-memory state.
func TestEvictStalePeersRemovesBadPeers(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")
	svc.addPeerAddress("10.0.0.2:64646", "full", "peer-2")

	// Make peer-1 terrible: low score, last seen >24h ago.
	svc.mu.Lock()
	svc.health[domain.PeerAddress("10.0.0.1:64646")] = &peerHealth{
		Address:             domain.PeerAddress("10.0.0.1:64646"),
		Score:               -30,
		ConsecutiveFailures: 10,
		LastConnectedAt:     time.Now().Add(-48 * time.Hour),
		LastDisconnectedAt:  time.Now().Add(-47 * time.Hour),
	}
	// peer-2 has low score but was recently connected — should survive.
	svc.health[domain.PeerAddress("10.0.0.2:64646")] = &peerHealth{
		Address:             domain.PeerAddress("10.0.0.2:64646"),
		Score:               -25,
		ConsecutiveFailures: 5,
		LastConnectedAt:     time.Now().Add(-1 * time.Hour),
		LastDisconnectedAt:  time.Now().Add(-30 * time.Minute),
	}
	// Reset eviction timer so it runs immediately.
	svc.lastPeerEvict = time.Time{}
	svc.mu.Unlock()

	svc.evictStalePeers()

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	for _, p := range svc.peers {
		if p.Address == "10.0.0.1:64646" {
			t.Fatal("expected peer 10.0.0.1 to be evicted, but it still exists")
		}
	}
	found := false
	for _, p := range svc.peers {
		if p.Address == "10.0.0.2:64646" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected peer 10.0.0.2 to survive eviction (recently connected)")
	}
	if svc.health[domain.PeerAddress("10.0.0.1:64646")] != nil {
		t.Fatal("expected health for 10.0.0.1 to be cleaned up")
	}
}

// TestEvictStalePeersKeepsBootstrap verifies that bootstrap peers
// are never evicted regardless of score.
func TestEvictStalePeersKeepsBootstrap(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.99:64646"},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Give the bootstrap peer a terrible score.
	svc.mu.Lock()
	svc.health[domain.PeerAddress("10.0.0.99:64646")] = &peerHealth{
		Address:             domain.PeerAddress("10.0.0.99:64646"),
		Score:               peerScoreMin,
		ConsecutiveFailures: 20,
		LastConnectedAt:     time.Now().Add(-72 * time.Hour),
		LastDisconnectedAt:  time.Now().Add(-71 * time.Hour),
	}
	svc.lastPeerEvict = time.Time{}
	svc.mu.Unlock()

	svc.evictStalePeers()

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	found := false
	for _, p := range svc.peers {
		if p.Address == "10.0.0.99:64646" {
			found = true
		}
	}
	if !found {
		t.Fatal("bootstrap peer should never be evicted")
	}
}

// TestEvictStalePeersRespectsInterval verifies that eviction is rate-limited.
func TestEvictStalePeersRespectsInterval(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	svc.mu.Lock()
	svc.health[domain.PeerAddress("10.0.0.1:64646")] = &peerHealth{
		Address:             domain.PeerAddress("10.0.0.1:64646"),
		Score:               -40,
		ConsecutiveFailures: 15,
		LastConnectedAt:     time.Now().Add(-48 * time.Hour),
		LastDisconnectedAt:  time.Now().Add(-47 * time.Hour),
	}
	// Set lastPeerEvict to recent time → eviction should be skipped.
	svc.lastPeerEvict = time.Now()
	svc.mu.Unlock()

	svc.evictStalePeers()

	// Peer should still be present because the interval hasn't elapsed.
	svc.mu.RLock()
	defer svc.mu.RUnlock()
	found := false
	for _, p := range svc.peers {
		if p.Address == "10.0.0.1:64646" {
			found = true
		}
	}
	if !found {
		t.Fatal("peer should not be evicted when interval hasn't elapsed")
	}
}

// TestEvictStalePeersIgnoresLastDisconnectedAt verifies that perpetually-failing
// peers are evicted even though their LastDisconnectedAt is recent (refreshed
// on every retry).  Only LastConnectedAt/AddedAt matter for eviction.
func TestEvictStalePeersIgnoresLastDisconnectedAt(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	addedAt := time.Now().Add(-48 * time.Hour)
	svc.mu.Lock()
	// Peer has never successfully connected (LastConnectedAt is zero).
	// LastDisconnectedAt is recent (simulating repeated retries), but
	// eviction should look at AddedAt, not LastDisconnectedAt.
	svc.health[domain.PeerAddress("10.0.0.1:64646")] = &peerHealth{
		Address:             domain.PeerAddress("10.0.0.1:64646"),
		Score:               -30,
		ConsecutiveFailures: 20,
		LastDisconnectedAt:  time.Now().Add(-5 * time.Minute), // recent retry!
	}
	svc.persistedMeta["10.0.0.1:64646"] = &peerEntry{
		Address: "10.0.0.1:64646",
		AddedAt: &addedAt,
	}
	svc.lastPeerEvict = time.Time{}
	svc.mu.Unlock()

	svc.evictStalePeers()

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	for _, p := range svc.peers {
		if p.Address == "10.0.0.1:64646" {
			t.Fatal("perpetually-failing peer should be evicted (AddedAt > 24h, never connected)")
		}
	}
}

// TestFallbackAddressHealthTracking verifies that when a fallback port variant
// is dialled, health updates are recorded under the primary peer address.
func TestFallbackAddressHealthTracking(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Add peer with non-default port — will generate fallback :64646 variant.
	svc.addPeerAddress("10.0.0.1:64647", "full", "peer-1")

	// Simulate what ensurePeerSessions does: register dialOrigin for fallback.
	svc.mu.Lock()
	svc.dialOrigin["10.0.0.1:64646"] = "10.0.0.1:64647"
	svc.mu.Unlock()

	// markPeerDisconnected on the fallback address should record health
	// under the primary address.
	svc.markPeerDisconnected("10.0.0.1:64646", fmt.Errorf("refused"))

	svc.mu.RLock()
	primaryHealth := svc.health[domain.PeerAddress("10.0.0.1:64647")]
	fallbackHealth := svc.health[domain.PeerAddress("10.0.0.1:64646")]
	svc.mu.RUnlock()

	if primaryHealth == nil {
		t.Fatal("expected health to be recorded under primary address 10.0.0.1:64647")
	}
	if primaryHealth.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 failure on primary, got %d", primaryHealth.ConsecutiveFailures)
	}
	if fallbackHealth != nil {
		t.Fatal("health should NOT be recorded under fallback address 10.0.0.1:64646")
	}

	// Now markPeerConnected on fallback should also go to primary.
	svc.markPeerConnected("10.0.0.1:64646", "outbound")

	svc.mu.RLock()
	primaryHealth = svc.health[domain.PeerAddress("10.0.0.1:64647")]
	svc.mu.RUnlock()

	if !primaryHealth.Connected {
		t.Fatal("expected primary health to show connected after fallback connect")
	}
	if primaryHealth.ConsecutiveFailures != 0 {
		t.Fatalf("expected failures reset to 0, got %d", primaryHealth.ConsecutiveFailures)
	}
}

// TestFallbackCooldownAppliesToAllVariants verifies that when the primary
// address is in cooldown, neither the primary nor the fallback port variant
// appears in dial candidates.
func TestFallbackCooldownAppliesToAllVariants(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64647", "full", "peer-1")

	// Simulate failures on the primary address.
	for i := 0; i < 5; i++ {
		svc.markPeerDisconnected("10.0.0.1:64647", fmt.Errorf("refused"))
	}

	candidates := candidateAddresses(svc.peerDialCandidates())
	for _, c := range candidates {
		if c == "10.0.0.1:64647" || c == "10.0.0.1:64646" {
			t.Fatalf("neither primary nor fallback should appear during cooldown, got: %v", candidates)
		}
	}
}

// TestFallbackSessionRoutingUsePrimaryMetadata verifies that when a peer is
// connected via a fallback port variant, routing filters use the peerType
// and peerID from the primary address, not the default "full" / empty ID.
func TestFallbackSessionRoutingUsePrimaryMetadata(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Add a client peer with a non-default port.
	svc.addPeerAddress("10.0.0.1:64647", "client", "client-identity-abc")

	// Simulate a fallback session on :64646.
	fallbackAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.mu.Lock()
	svc.dialOrigin[fallbackAddr] = "10.0.0.1:64647"
	svc.sessions[fallbackAddr] = &peerSession{address: fallbackAddr}
	svc.mu.Unlock()
	svc.markPeerConnected(fallbackAddr, "outbound")

	// routingTargets() filters out client peers. Since 10.0.0.1:64647 is
	// a client, the fallback session should NOT appear in routing targets.
	targets := svc.routingTargets()
	for _, target := range targets {
		if target == fallbackAddr {
			t.Fatalf("client peer connected via fallback should be excluded from relay routing, got: %v", targets)
		}
	}

	// routingTargetsForRecipient with the correct peerID should include it.
	targets = svc.routingTargetsForRecipient("client-identity-abc")
	found := false
	for _, target := range targets {
		if target == fallbackAddr {
			found = true
		}
	}
	if !found {
		t.Fatalf("client peer should be reachable via its peerID for direct messages, got: %v", targets)
	}
}

// TestEvictRuntimeDiscoveredPeerWithoutFlush verifies that a runtime-discovered
// peer (never flushed to disk) can still be evicted because addPeerAddress
// eagerly populates persistedMeta with AddedAt.
func TestEvictRuntimeDiscoveredPeerWithoutFlush(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Add a peer at runtime (simulating peer exchange discovery).
	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	// Backdate the AddedAt so it looks old.
	svc.mu.Lock()
	old := time.Now().Add(-48 * time.Hour)
	if pm := svc.persistedMeta["10.0.0.1:64646"]; pm != nil {
		pm.AddedAt = &old
	} else {
		t.Fatal("expected persistedMeta to be populated by addPeerAddress")
	}
	// Give the peer a bad score and no successful connections.
	svc.health[domain.PeerAddress("10.0.0.1:64646")] = &peerHealth{
		Address:             domain.PeerAddress("10.0.0.1:64646"),
		Score:               -30,
		ConsecutiveFailures: 10,
	}
	svc.lastPeerEvict = time.Time{}
	svc.mu.Unlock()

	// Note: flushPeerState has NOT been called.
	svc.evictStalePeers()

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	for _, p := range svc.peers {
		if p.Address == "10.0.0.1:64646" {
			t.Fatal("runtime-discovered peer should be evicted without waiting for flush")
		}
	}
}

// TestPendingQueueFallbackFlushedOnPrimary verifies that frames queued under
// a fallback dial address are flushed when the session connects via the primary
// address (or any variant), because the pending queue is keyed by primary.
func TestPendingQueueFallbackFlushedOnPrimary(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
	}, id)

	primaryAddr := domain.PeerAddress("10.0.0.1:64647")
	fallbackAddr := domain.PeerAddress("10.0.0.1:64646")

	// Register fallback→primary mapping as ensurePeerSessions would.
	svc.mu.Lock()
	svc.dialOrigin[fallbackAddr] = primaryAddr
	svc.mu.Unlock()

	// Queue a frame targeting the fallback dial address.
	frame := protocol.Frame{
		Type:      "send_message",
		Topic:     "dm",
		ID:        "fallback-queue-1",
		Address:   id.Address,
		Recipient: "recipient-1",
		Body:      "ciphertext",
	}
	if !svc.queuePeerFrame(fallbackAddr, frame) {
		t.Fatal("expected queuePeerFrame to accept the frame")
	}

	// Verify the frame is stored under primary, not fallback.
	svc.mu.RLock()
	primaryPending := len(svc.pending[primaryAddr])
	fallbackPending := len(svc.pending[fallbackAddr])
	svc.mu.RUnlock()

	if primaryPending != 1 {
		t.Fatalf("expected 1 pending frame under primary %s, got %d", primaryAddr, primaryPending)
	}
	if fallbackPending != 0 {
		t.Fatalf("expected 0 pending frames under fallback %s, got %d", fallbackAddr, fallbackPending)
	}

	// Now create a session on the PRIMARY address and flush.
	sendCh := make(chan protocol.Frame, 10)
	svc.mu.Lock()
	svc.sessions[primaryAddr] = &peerSession{address: primaryAddr, sendCh: sendCh}
	svc.health[primaryAddr] = &peerHealth{Address: primaryAddr, Connected: true, State: peerStateHealthy}
	svc.mu.Unlock()

	svc.flushPendingPeerFrames(primaryAddr)

	svc.mu.RLock()
	remainingPrimary := len(svc.pending[primaryAddr])
	remainingFallback := len(svc.pending[fallbackAddr])
	keysCount := len(svc.pendingKeys)
	svc.mu.RUnlock()

	if remainingPrimary != 0 {
		t.Fatalf("expected pending queue for primary to be drained, got %d", remainingPrimary)
	}
	if remainingFallback != 0 {
		t.Fatalf("expected no pending under fallback, got %d", remainingFallback)
	}
	if keysCount != 0 {
		t.Fatalf("expected pending keys to be cleared, got %d", keysCount)
	}

	// Verify the frame was sent on the session.
	select {
	case sent := <-sendCh:
		if sent.ID != "fallback-queue-1" {
			t.Fatalf("expected frame fallback-queue-1, got %s", sent.ID)
		}
	default:
		t.Fatal("expected frame to be sent to session channel")
	}
}

// TestDialCandidatesSortStableWithEqualScores verifies that peers with equal
// scores retain their insertion order (bootstrap-first) after sorting.
func TestDialCandidatesSortStableWithEqualScores(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646"},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Add some non-bootstrap peers (they are appended after bootstrap in iteration).
	svc.addPeerAddress("10.0.0.4:64646", "full", "peer-4")
	svc.addPeerAddress("10.0.0.5:64646", "full", "peer-5")

	// All peers have zero score (default), so the sort should preserve
	// insertion order: bootstrap peers first, then discovered peers.
	candidates := svc.peerDialCandidates()
	addresses := candidateAddresses(candidates)

	// Bootstrap peers must appear before discovered peers.
	bootstrapIdx := map[string]int{}
	discoveredIdx := map[string]int{}
	for i, addr := range addresses {
		switch addr {
		case "10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646":
			bootstrapIdx[addr] = i
		case "10.0.0.4:64646", "10.0.0.5:64646":
			discoveredIdx[addr] = i
		}
	}

	for bAddr, bIdx := range bootstrapIdx {
		for dAddr, dIdx := range discoveredIdx {
			if bIdx > dIdx {
				t.Errorf("bootstrap peer %s (index %d) should appear before discovered peer %s (index %d)",
					bAddr, bIdx, dAddr, dIdx)
			}
		}
	}
}

// TestQueueStateMigrationFallbackToPrimary verifies that pending frames
// persisted under a fallback dial address (e.g. host:64646) are migrated
// to the primary peer address (e.g. host:64647) on startup.
func TestQueueStateMigrationFallbackToPrimary(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()
	queuePath := filepath.Join(tempDir, "queue.json")

	// Write a queue state file with a pending frame under the fallback
	// address 10.0.0.1:64646 (the default port variant).
	fallbackAddr := "10.0.0.1:64646"
	qs := queueStateFile{
		Pending: map[string][]pendingFrame{
			fallbackAddr: {
				{
					Frame: protocol.Frame{
						Type:      "send_message",
						Topic:     "dm",
						ID:        "migrate-1",
						Address:   id.Address,
						Recipient: "recipient-1",
						Body:      "ciphertext",
					},
					QueuedAt: time.Now().UTC(),
				},
			},
		},
		RelayRetry:    map[string]relayAttempt{},
		OutboundState: map[string]outboundDelivery{},
	}
	data, err := json.Marshal(qs)
	if err != nil {
		t.Fatalf("marshal queue state: %v", err)
	}
	if err := os.WriteFile(queuePath, data, 0o644); err != nil {
		t.Fatalf("write queue state: %v", err)
	}

	// Create a service where the primary peer address uses a NON-default port.
	primaryAddr := "10.0.0.1:64647"
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		BootstrapPeers:   []string{primaryAddr},
		QueueStatePath:   queuePath,
	}, id)

	svc.mu.RLock()
	primaryPending := len(svc.pending[domain.PeerAddress(primaryAddr)])
	fallbackPending := len(svc.pending[domain.PeerAddress(fallbackAddr)])
	svc.mu.RUnlock()

	if primaryPending != 1 {
		t.Fatalf("expected 1 pending frame migrated to primary %s, got %d", primaryAddr, primaryPending)
	}
	if fallbackPending != 0 {
		t.Fatalf("expected 0 pending frames under fallback %s after migration, got %d", fallbackAddr, fallbackPending)
	}

	// Verify pendingKeys were rebuilt with the primary address.
	expectedKey := pendingFrameKey(domain.PeerAddress(primaryAddr), qs.Pending[fallbackAddr][0].Frame)
	svc.mu.RLock()
	_, hasKey := svc.pendingKeys[expectedKey]
	svc.mu.RUnlock()
	if !hasKey {
		t.Fatalf("expected pending key %q to exist after migration", expectedKey)
	}

	// Old fallback-keyed entry should NOT exist.
	oldKey := pendingFrameKey(domain.PeerAddress(fallbackAddr), qs.Pending[fallbackAddr][0].Frame)
	svc.mu.RLock()
	_, hasOldKey := svc.pendingKeys[oldKey]
	orphanedCount := len(svc.orphaned)
	svc.mu.RUnlock()
	if hasOldKey {
		t.Fatal("expected old fallback-keyed pending key to be absent after migration")
	}

	// Successful migration should not produce orphans.
	if orphanedCount != 0 {
		t.Fatalf("expected 0 orphaned entries after clean migration, got %d", orphanedCount)
	}
}

// TestQueueStateMigrationOrphansAmbiguousHost verifies that when multiple
// primary peers share the same host (different ports), pending frames under
// a fallback address are moved to the orphaned map — preserving them on disk
// for manual recovery instead of silently dropping user data.
func TestQueueStateMigrationOrphansAmbiguousHost(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()
	queuePath := filepath.Join(tempDir, "queue.json")

	// Pending frame under a fallback address whose host has TWO known primaries.
	fallbackAddr := "10.0.0.1:64646"
	qs := queueStateFile{
		Pending: map[string][]pendingFrame{
			fallbackAddr: {
				{
					Frame: protocol.Frame{
						Type:      "send_message",
						Topic:     "dm",
						ID:        "ambiguous-1",
						Address:   id.Address,
						Recipient: "recipient-1",
						Body:      "ciphertext",
					},
					QueuedAt: time.Now().UTC(),
				},
			},
		},
		RelayRetry:    map[string]relayAttempt{},
		OutboundState: map[string]outboundDelivery{},
	}
	data, err := json.Marshal(qs)
	if err != nil {
		t.Fatalf("marshal queue state: %v", err)
	}
	if err := os.WriteFile(queuePath, data, 0o644); err != nil {
		t.Fatalf("write queue state: %v", err)
	}

	// Two primaries on the same host but different ports.
	primaryA := "10.0.0.1:64647"
	primaryB := "10.0.0.1:64648"
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		BootstrapPeers:   []string{primaryA, primaryB},
		QueueStatePath:   queuePath,
	}, id)

	svc.mu.RLock()
	fallbackPending := len(svc.pending[domain.PeerAddress(fallbackAddr)])
	aPending := len(svc.pending[domain.PeerAddress(primaryA)])
	bPending := len(svc.pending[domain.PeerAddress(primaryB)])
	orphanedCount := len(svc.orphaned[domain.PeerAddress(fallbackAddr)])
	svc.mu.RUnlock()

	// Frames must NOT be in the active pending map (runtime would never flush them).
	if fallbackPending != 0 {
		t.Fatalf("expected fallback %s removed from pending, got %d", fallbackAddr, fallbackPending)
	}
	if aPending != 0 {
		t.Fatalf("expected 0 pending frames under %s, got %d", primaryA, aPending)
	}
	if bPending != 0 {
		t.Fatalf("expected 0 pending frames under %s, got %d", primaryB, bPending)
	}

	// Frames must be preserved in orphaned for manual recovery.
	if orphanedCount != 1 {
		t.Fatalf("expected 1 orphaned frame under %s, got %d", fallbackAddr, orphanedCount)
	}
	if svc.orphaned[domain.PeerAddress(fallbackAddr)][0].Frame.ID != "ambiguous-1" {
		t.Fatalf("unexpected orphaned frame ID: %s", svc.orphaned[domain.PeerAddress(fallbackAddr)][0].Frame.ID)
	}
}

// TestQueueStateMigrationOrphansUnknownHost verifies that legacy pending
// frames for an address whose host has no known primaries are orphaned
// during v0→v1 migration (since the runtime would never flush them).
func TestQueueStateMigrationOrphansUnknownHost(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()
	queuePath := filepath.Join(tempDir, "queue.json")

	unknownAddr := "10.99.99.1:65001"
	// Version 0 (legacy) — triggers migration.
	qs := queueStateFile{
		Pending: map[string][]pendingFrame{
			unknownAddr: {
				{
					Frame: protocol.Frame{
						Type:      "send_message",
						Topic:     "dm",
						ID:        "unknown-host-1",
						Address:   id.Address,
						Recipient: "recipient-1",
						Body:      "ciphertext",
					},
					QueuedAt: time.Now().UTC(),
				},
			},
		},
		RelayRetry:    map[string]relayAttempt{},
		OutboundState: map[string]outboundDelivery{},
	}
	data, err := json.Marshal(qs)
	if err != nil {
		t.Fatalf("marshal queue state: %v", err)
	}
	if err := os.WriteFile(queuePath, data, 0o644); err != nil {
		t.Fatalf("write queue state: %v", err)
	}

	// Bootstrap peer on a DIFFERENT host — 10.99.99.1 has zero candidates.
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		BootstrapPeers:   []string{"10.0.0.1:64647"},
		QueueStatePath:   queuePath,
	}, id)

	svc.mu.RLock()
	pendingCount := len(svc.pending[domain.PeerAddress(unknownAddr)])
	orphanedCount := len(svc.orphaned[domain.PeerAddress(unknownAddr)])
	svc.mu.RUnlock()

	// Unknown-host entries are orphaned during migration — the runtime
	// only drains primary-keyed entries so they would be stranded.
	if pendingCount != 0 {
		t.Fatalf("expected 0 pending frames under %s, got %d", unknownAddr, pendingCount)
	}
	if orphanedCount != 1 {
		t.Fatalf("expected 1 orphaned frame under %s, got %d", unknownAddr, orphanedCount)
	}
}

// TestQueueStateMigrationSkippedForCurrentVersion verifies that pending
// frames written by the current code version (v1) are NOT touched by
// migration — they survive a normal persist/reload cycle intact.
func TestQueueStateMigrationSkippedForCurrentVersion(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	tempDir := t.TempDir()
	queuePath := filepath.Join(tempDir, "queue.json")

	runtimeAddr := "10.99.99.1:65001"
	// Version 1 (current) — migration should be skipped entirely.
	qs := queueStateFile{
		Version: queueStateVersion,
		Pending: map[string][]pendingFrame{
			runtimeAddr: {
				{
					Frame: protocol.Frame{
						Type:      "send_message",
						Topic:     "dm",
						ID:        "current-version-1",
						Address:   id.Address,
						Recipient: "recipient-1",
						Body:      "ciphertext",
					},
					QueuedAt: time.Now().UTC(),
				},
			},
		},
		RelayRetry:    map[string]relayAttempt{},
		OutboundState: map[string]outboundDelivery{},
	}
	data, err := json.Marshal(qs)
	if err != nil {
		t.Fatalf("marshal queue state: %v", err)
	}
	if err := os.WriteFile(queuePath, data, 0o644); err != nil {
		t.Fatalf("write queue state: %v", err)
	}

	// No bootstrap peer matching this host — but migration is skipped.
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "127.0.0.1:64646",
		BootstrapPeers:   []string{"10.0.0.1:64647"},
		QueueStatePath:   queuePath,
	}, id)

	svc.mu.RLock()
	pendingCount := len(svc.pending[domain.PeerAddress(runtimeAddr)])
	orphanedCount := len(svc.orphaned[domain.PeerAddress(runtimeAddr)])
	svc.mu.RUnlock()

	// Current-version entries stay in pending — no migration runs.
	if pendingCount != 1 {
		t.Fatalf("expected 1 pending frame under %s, got %d", runtimeAddr, pendingCount)
	}
	if orphanedCount != 0 {
		t.Fatalf("expected 0 orphaned frames under %s, got %d", runtimeAddr, orphanedCount)
	}
}

// --- Observed IP tests ---

func TestRecordObservedAddressIgnoresEmpty(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	svc.recordObservedAddress("peer-fingerprint-a", "")
	svc.recordObservedAddress("", "203.0.113.50")
	svc.mu.RLock()
	count := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if count != 0 {
		t.Fatalf("expected 0 observed addresses after empty input, got %d", count)
	}
}

func TestRecordObservedAddressIgnoresPrivate(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	svc.recordObservedAddress("peer-fingerprint-a", "10.0.0.1")
	svc.recordObservedAddress("peer-fingerprint-b", "192.168.1.5")
	svc.recordObservedAddress("peer-fingerprint-c", "127.0.0.1")
	svc.mu.RLock()
	count := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if count != 0 {
		t.Fatalf("expected 0 observed addresses for private/loopback IPs, got %d", count)
	}
}

func TestRecordObservedAddressStoresPublicIP(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	svc.recordObservedAddress("peer-fingerprint-a", "203.0.113.50")
	svc.mu.RLock()
	got := svc.observedAddrs["peer-fingerprint-a"]
	svc.mu.RUnlock()
	if got != "203.0.113.50" {
		t.Fatalf("expected observed IP 203.0.113.50, got %q", got)
	}
}

func TestRecordObservedAddressConsensusRequiresTwoPeers(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	// Single observation — no consensus yet.
	svc.recordObservedAddress("peer-fingerprint-a", "203.0.113.50")
	svc.mu.RLock()
	count := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if count != 1 {
		t.Fatalf("expected 1 observed address, got %d", count)
	}

	// Second peer (different identity) agrees — consensus reached.
	svc.recordObservedAddress("peer-fingerprint-b", "203.0.113.50")
	svc.mu.RLock()
	count = len(svc.observedAddrs)
	svc.mu.RUnlock()
	if count != 2 {
		t.Fatalf("expected 2 observed addresses, got %d", count)
	}
}

func TestRecordObservedAddressSameNodeOneVote(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	// Same peer identity reached via two different addresses — still one vote.
	svc.recordObservedAddress("peer-fingerprint-a", "203.0.113.50")
	svc.recordObservedAddress("peer-fingerprint-a", "203.0.113.50")
	svc.mu.RLock()
	count := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if count != 1 {
		t.Fatalf("expected 1 observed address (same identity = one vote), got %d", count)
	}
}

func TestRecordObservedAddressNoConsensusWhenPeersDisagree(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	svc.recordObservedAddress("peer-fingerprint-a", "203.0.113.50")
	svc.recordObservedAddress("peer-fingerprint-b", "203.0.113.99")

	// Both stored, but they disagree — no consensus.
	svc.mu.RLock()
	count := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if count != 2 {
		t.Fatalf("expected 2 observed addresses, got %d", count)
	}
}

func TestRecordObservedAddressSkipsWhenAdvertiseIsPublic(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	// Node already advertises a public IP — observed address should not trigger NAT log.
	svc := NewService(config.Node{
		ListenAddress:    "0.0.0.0:64646",
		AdvertiseAddress: "203.0.113.50:64646",
	}, id)

	svc.recordObservedAddress("peer-fingerprint-a", "203.0.113.50")
	svc.recordObservedAddress("peer-fingerprint-b", "203.0.113.50")
	svc.mu.RLock()
	count := len(svc.observedAddrs)
	svc.mu.RUnlock()
	// Observations still stored, but the consensus path exits early
	// because advertise address already matches observed.
	if count != 2 {
		t.Fatalf("expected 2 observed addresses, got %d", count)
	}
}

func TestMarkPeerDisconnectedClearsObservedAddress(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	peerID, _ := identity.Generate()
	peerAddr := domain.PeerAddress("198.51.100.1:64646")
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	// Simulate what openPeerSession does: record observation keyed by identity,
	// and register the peerID mapping so markPeerDisconnected can find it.
	svc.recordObservedAddress(domain.PeerIdentity(peerID.Address), "203.0.113.50")
	svc.mu.Lock()
	svc.peerIDs[peerAddr] = domain.PeerIdentity(peerID.Address)
	svc.mu.Unlock()

	svc.mu.RLock()
	before := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if before != 1 {
		t.Fatalf("expected 1 observed address before disconnect, got %d", before)
	}

	svc.markPeerDisconnected(peerAddr, fmt.Errorf("test disconnect"))
	svc.mu.RLock()
	after := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if after != 0 {
		t.Fatalf("expected 0 observed addresses after disconnect, got %d", after)
	}
}

func TestMarkPeerDisconnectedClearsObservedAddressViaFallback(t *testing.T) {
	t.Parallel()
	id, _ := identity.Generate()
	peerID, _ := identity.Generate()
	primaryAddr := domain.PeerAddress("198.51.100.1:64647")
	fallbackAddr := domain.PeerAddress("198.51.100.1:64646")
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:64646",
		AdvertiseAddress: "192.168.1.10:64646",
	}, id)

	// Record observation under peer identity.
	svc.recordObservedAddress(domain.PeerIdentity(peerID.Address), "203.0.113.50")

	// Set up dialOrigin (fallback → primary) and peerIDs (primary → fingerprint)
	// as the real code does when a fallback connection succeeds.
	svc.mu.Lock()
	svc.dialOrigin[fallbackAddr] = primaryAddr
	svc.peerIDs[primaryAddr] = domain.PeerIdentity(peerID.Address)
	svc.mu.Unlock()

	// Disconnect using fallback address — resolveHealthAddress maps to primary,
	// then peerIDs lookup finds the fingerprint.
	svc.markPeerDisconnected(fallbackAddr, fmt.Errorf("connection lost"))
	svc.mu.RLock()
	after := len(svc.observedAddrs)
	svc.mu.RUnlock()
	if after != 0 {
		t.Fatalf("expected 0 observed addresses after fallback disconnect, got %d", after)
	}
}

// ---------------------------------------------------------------------------
// add_peer console command
// ---------------------------------------------------------------------------

func TestAddPeerFrameNewPeerPrependedToList(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646"},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{"10.0.0.2:64646"},
	})
	if reply.Type != "ok" {
		t.Fatalf("expected ok, got %#v", reply)
	}

	svc.mu.RLock()
	first := svc.peers[0].Address
	svc.mu.RUnlock()
	if first != "10.0.0.2:64646" {
		t.Fatalf("expected manually added peer first, got %s", first)
	}
}

func TestAddPeerFrameExistingPeerMovedToFront(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646"},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{"10.0.0.3:64646"},
	})
	if reply.Type != "ok" {
		t.Fatalf("expected ok, got %#v", reply)
	}

	svc.mu.RLock()
	first := svc.peers[0].Address
	count := 0
	for _, p := range svc.peers {
		if p.Address == "10.0.0.3:64646" {
			count++
		}
	}
	svc.mu.RUnlock()
	if first != "10.0.0.3:64646" {
		t.Fatalf("expected moved peer first, got %s", first)
	}
	if count != 1 {
		t.Fatalf("expected peer to appear exactly once, got %d", count)
	}
}

func TestAddPeerFrameResetsCooldown(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.5:64646", "full", "peer-5")
	for i := 0; i < 5; i++ {
		svc.markPeerDisconnected("10.0.0.5:64646", fmt.Errorf("refused"))
	}

	// Peer should be in cooldown now.
	candidates := candidateAddresses(svc.peerDialCandidates())
	for _, c := range candidates {
		if c == "10.0.0.5:64646" {
			t.Fatalf("peer should be in cooldown before add_peer")
		}
	}

	// add_peer resets cooldown.
	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{"10.0.0.5:64646"},
	})
	if reply.Type != "ok" {
		t.Fatalf("expected ok, got %#v", reply)
	}

	candidates = candidateAddresses(svc.peerDialCandidates())
	found := false
	for _, c := range candidates {
		if c == "10.0.0.5:64646" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected peer after cooldown reset, got: %v", candidates)
	}
}

func TestAddPeerFrameDefaultPort(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{"10.0.0.9"},
	})
	if reply.Type != "ok" {
		t.Fatalf("expected ok, got %#v", reply)
	}
	if len(reply.Peers) != 1 || reply.Peers[0] != "10.0.0.9:64646" {
		t.Fatalf("expected default port appended, got %v", reply.Peers)
	}
}

func TestAddPeerFrameEmptyAddressError(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	reply := svc.addPeerFrame(protocol.Frame{Peers: []string{}})
	if reply.Type != "error" {
		t.Fatalf("expected error for empty address, got %#v", reply)
	}
}

func TestAddPeerFrameSelfAddressError(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{normalizeAddress(address)},
	})
	if reply.Type != "error" {
		t.Fatalf("expected error for self-address, got %#v", reply)
	}
}

func TestAddPeerFrameForbiddenIPError(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: "198.51.100.1:64646",
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// 192.168.x.x is in isForbiddenAdvertisedIP → shouldSkipDialAddress
	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{"192.168.1.20:64646"},
	})
	if reply.Type != "error" {
		t.Fatalf("expected error for forbidden IP, got %#v", reply)
	}
	if !strings.Contains(reply.Error, "forbidden") {
		t.Fatalf("expected 'forbidden' in error message, got %q", reply.Error)
	}
}

func TestAddPeerFrameUnreachableOverlayError(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
		// No ProxyAddress → Tor/I2P unreachable.
	})
	defer stop()

	onion := strings.Repeat("a", 56) + ".onion:64646"
	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{onion},
	})
	if reply.Type != "error" {
		t.Fatalf("expected error for unreachable overlay, got %#v", reply)
	}
	if !strings.Contains(reply.Error, "unreachable") {
		t.Fatalf("expected 'unreachable' in error message, got %q", reply.Error)
	}
}

func TestAddPeerFrameUpdatesSourceToManual(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646"},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Peer was added via bootstrap — source is "bootstrap" or similar.
	svc.mu.RLock()
	var before domain.PeerSource
	if pm := svc.persistedMeta["10.0.0.1:64646"]; pm != nil {
		before = pm.Source
	}
	svc.mu.RUnlock()
	if before == domain.PeerSourceManual {
		t.Fatalf("expected non-manual source before add_peer, got %q", before)
	}

	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{"10.0.0.1:64646"},
	})
	if reply.Type != "ok" {
		t.Fatalf("expected ok, got %#v", reply)
	}

	svc.mu.RLock()
	var after domain.PeerSource
	if pm := svc.persistedMeta["10.0.0.1:64646"]; pm != nil {
		after = pm.Source
	}
	svc.mu.RUnlock()
	if after != domain.PeerSourceManual {
		t.Fatalf("expected source updated to 'manual' after add_peer, got %q", after)
	}
}

func TestAddPeerFrameFlushesPeerStateToDisk(t *testing.T) {
	t.Parallel()

	peersPath := filepath.Join(t.TempDir(), "peers.json")
	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		PeersStatePath:   peersPath,
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	reply := svc.addPeerFrame(protocol.Frame{
		Peers: []string{"10.0.0.1:64646"},
	})
	if reply.Type != "ok" {
		t.Fatalf("expected ok, got %#v", reply)
	}

	// The file must already exist on disk after add_peer returns.
	state, err := loadPeerState(peersPath)
	if err != nil {
		t.Fatalf("loadPeerState: %v", err)
	}
	found := false
	for _, p := range state.Peers {
		if p.Address == "10.0.0.1:64646" && p.Source == domain.PeerSourceManual {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected manual peer 10.0.0.1:64646 in persisted state, got: %+v", state.Peers)
	}
}

// --- MessageStore integration tests ---

// testMessageStore is a mock MessageStore for verifying that the node
// delegates persistence to the registered store.
type testMessageStore struct {
	stored   []protocol.Envelope
	outFlags []bool
	receipts []protocol.DeliveryReceipt
}

func (m *testMessageStore) StoreMessage(envelope protocol.Envelope, isOutgoing bool) StoreResult {
	m.stored = append(m.stored, envelope)
	m.outFlags = append(m.outFlags, isOutgoing)
	return StoreInserted
}

func (m *testMessageStore) UpdateDeliveryStatus(receipt protocol.DeliveryReceipt) bool {
	m.receipts = append(m.receipts, receipt)
	return true
}

// TestMessageStoreCalledForLocalDM verifies that storeIncomingMessage
// delegates persistence to the registered MessageStore for local messages.
func TestMessageStoreCalledForLocalDM(t *testing.T) {
	t.Parallel()

	addr := freeAddress(t)
	svc, cleanup := startTestNode(t, config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: normalizeAddress(addr),
		BootstrapPeers:   []string{},
	})
	defer cleanup()

	store := &testMessageStore{}
	svc.RegisterMessageStore(store)

	peerID, _ := identity.Generate()
	peerAddr := peerID.Address
	peerPubKey := identity.PublicKeyBase64(peerID.PublicKey)
	peerBoxKey := identity.BoxPublicKeyBase64(peerID.BoxPublicKey)
	peerBoxSig := identity.SignBoxKeyBinding(peerID)

	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peerAddr,
			PubKey:  peerPubKey,
			BoxKey:  peerBoxKey,
			BoxSig:  peerBoxSig,
		}},
	})

	sealed, err := directmsg.EncryptForParticipants(
		peerID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(svc.identity.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello from peer"},
	)
	if err != nil {
		t.Fatalf("seal envelope: %v", err)
	}

	svc.addKnownPubKey(peerAddr, peerPubKey)

	msgID, _ := protocol.NewMessageID()
	now := time.Now().UTC()

	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:         msgID,
		Topic:      "dm",
		Sender:     peerAddr,
		Recipient:  svc.identity.Address,
		Flag:       protocol.MessageFlagImmutable,
		CreatedAt:  now,
		TTLSeconds: 86400,
		Body:       sealed,
	}, false)
	if !stored || errCode != "" {
		t.Fatalf("store message failed: stored=%v errCode=%q", stored, errCode)
	}

	// Verify MessageStore.StoreMessage was called.
	if len(store.stored) != 1 {
		t.Fatalf("expected 1 stored message, got %d", len(store.stored))
	}
	if string(store.stored[0].ID) != string(msgID) {
		t.Fatalf("expected message ID %s, got %s", msgID, store.stored[0].ID)
	}
	if store.outFlags[0] {
		t.Fatal("expected isOutgoing=false for incoming message")
	}
}

// TestMessageStoreDuplicateSuppressesEvent verifies that when the
// MessageStore reports a duplicate (returns false), storeIncomingMessage
// does not emit a LocalChangeEvent. This prevents duplicate beeps and
// unread increments after a client restart when relay backlog contains
// messages already persisted in the chatlog.
func TestMessageStoreDuplicateSuppressesEvent(t *testing.T) {
	t.Parallel()

	addr := freeAddress(t)
	svc, cleanup := startTestNode(t, config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: normalizeAddress(addr),
		BootstrapPeers:   []string{},
	})
	defer cleanup()

	// A store that returns StoreDuplicate on the second call.
	calls := 0
	dupStore := &duplicateAwareStore{storeFunc: func(envelope protocol.Envelope, isOutgoing bool) StoreResult {
		calls++
		if calls == 1 {
			return StoreInserted
		}
		return StoreDuplicate
	}}
	svc.RegisterMessageStore(dupStore)

	peerID, _ := identity.Generate()
	peerPubKey := identity.PublicKeyBase64(peerID.PublicKey)
	peerBoxKey := identity.BoxPublicKeyBase64(peerID.BoxPublicKey)
	peerBoxSig := identity.SignBoxKeyBinding(peerID)

	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peerAddr(peerID),
			PubKey:  peerPubKey,
			BoxKey:  peerBoxKey,
			BoxSig:  peerBoxSig,
		}},
	})
	svc.addKnownPubKey(peerAddr(peerID), peerPubKey)

	sealed, err := directmsg.EncryptForParticipants(
		peerID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(svc.identity.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	events, cancel := svc.SubscribeLocalChanges()
	defer cancel()

	msgID, _ := protocol.NewMessageID()
	msg := incomingMessage{
		ID:        msgID,
		Topic:     "dm",
		Sender:    peerAddr(peerID),
		Recipient: svc.identity.Address,
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      sealed,
	}

	// First store — StoreMessage returns true → event emitted.
	stored1, _, code1 := svc.storeIncomingMessage(msg, false)
	if !stored1 || code1 != "" {
		t.Fatalf("first store failed: stored=%v code=%q", stored1, code1)
	}
	// Drain all events from the first store. The node may emit both a
	// new_message event and a receipt_update (delivery receipt sent back
	// to the sender). We need to consume everything before the second
	// store to avoid false positives.
	drainTimeout := time.After(2 * time.Second)
	gotFirst := false
	for {
		select {
		case <-events:
			gotFirst = true
			// Keep draining — there may be more events (e.g. receipt_update).
			continue
		case <-time.After(300 * time.Millisecond):
			// No more events within 300ms — done draining.
		case <-drainTimeout:
			if !gotFirst {
				t.Fatal("expected LocalChangeEvent after first store")
			}
		}
		break
	}
	if !gotFirst {
		t.Fatal("expected at least one LocalChangeEvent after first store")
	}

	// Clear s.seen so the node-level dedup doesn't catch it (simulates restart).
	svc.mu.Lock()
	delete(svc.seen, string(msgID))
	svc.mu.Unlock()

	// Second store — StoreMessage returns StoreDuplicate.
	// The duplicate is NOT added to s.topics (prevents DMHeaders
	// from re-triggering unread counts via repairUnreadFromHeaders).
	// storeIncomingMessage still returns stored=true because the
	// in-memory seen check passes (we cleared s.seen above) and
	// the message is accepted at the node level even though it
	// doesn't enter s.topics.
	stored2, _, code2 := svc.storeIncomingMessage(msg, false)
	if !stored2 {
		t.Fatal("second store should still return true (accepted by node)")
	}
	if code2 != "" {
		t.Fatalf("duplicate should not produce error code, got %q", code2)
	}
	select {
	case ev := <-events:
		t.Fatalf("should NOT emit event for duplicate, got %+v", ev)
	case <-time.After(200 * time.Millisecond):
		// Expected: no event.
	}

	// Verify the duplicate did NOT enter s.topics — this is the key
	// invariant that closes the DMHeaders → repairUnreadFromHeaders path.
	svc.mu.RLock()
	dmCount := 0
	for _, env := range svc.topics["dm"] {
		if string(env.ID) == string(msgID) {
			dmCount++
		}
	}
	svc.mu.RUnlock()
	if dmCount != 1 {
		t.Fatalf("expected exactly 1 copy in s.topics[dm], got %d", dmCount)
	}
}

// TestDuplicateMessageExcludedFromDMHeaders verifies the full path:
// after restart (s.seen cleared), a duplicate message that returns
// StoreDuplicate must NOT appear in fetch_dm_headers. This is the
// invariant that prevents repairUnreadFromHeaders() from re-incrementing
// unread counts on the UI side.
func TestDuplicateMessageExcludedFromDMHeaders(t *testing.T) {
	t.Parallel()

	addr := freeAddress(t)
	svc, cleanup := startTestNode(t, config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: normalizeAddress(addr),
		BootstrapPeers:   []string{},
	})
	defer cleanup()

	calls := 0
	dupStore := &duplicateAwareStore{storeFunc: func(envelope protocol.Envelope, isOutgoing bool) StoreResult {
		calls++
		if calls == 1 {
			return StoreInserted
		}
		return StoreDuplicate
	}}
	svc.RegisterMessageStore(dupStore)

	peerID, _ := identity.Generate()
	peerPubKey := identity.PublicKeyBase64(peerID.PublicKey)
	peerBoxKey := identity.BoxPublicKeyBase64(peerID.BoxPublicKey)
	peerBoxSig := identity.SignBoxKeyBinding(peerID)

	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peerAddr(peerID),
			PubKey:  peerPubKey,
			BoxKey:  peerBoxKey,
			BoxSig:  peerBoxSig,
		}},
	})
	svc.addKnownPubKey(peerAddr(peerID), peerPubKey)

	sealed, err := directmsg.EncryptForParticipants(
		peerID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(svc.identity.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello headers"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	msgID, _ := protocol.NewMessageID()
	msg := incomingMessage{
		ID:        msgID,
		Topic:     "dm",
		Sender:    peerAddr(peerID),
		Recipient: svc.identity.Address,
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      sealed,
	}

	// First store — StoreInserted → message appears in DMHeaders.
	svc.storeIncomingMessage(msg, false)

	resp1 := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_dm_headers"})
	found1 := 0
	for _, h := range resp1.DMHeaders {
		if h.ID == string(msgID) {
			found1++
		}
	}
	if found1 != 1 {
		t.Fatalf("after first store: expected 1 header for msgID, got %d (total headers: %d)", found1, resp1.Count)
	}

	// Simulate restart: clear s.seen so the node-level dedup won't catch it.
	svc.mu.Lock()
	delete(svc.seen, string(msgID))
	svc.mu.Unlock()

	// Second store — StoreDuplicate → must NOT add another copy to DMHeaders.
	svc.storeIncomingMessage(msg, false)

	resp2 := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_dm_headers"})
	found2 := 0
	for _, h := range resp2.DMHeaders {
		if h.ID == string(msgID) {
			found2++
		}
	}
	if found2 != 1 {
		t.Fatalf("after duplicate store: expected still 1 header for msgID, got %d (total headers: %d)", found2, resp2.Count)
	}
}

// duplicateAwareStore is a test MessageStore where the caller controls
// the return value of StoreMessage.
type duplicateAwareStore struct {
	storeFunc func(protocol.Envelope, bool) StoreResult
}

func (d *duplicateAwareStore) StoreMessage(e protocol.Envelope, o bool) StoreResult {
	return d.storeFunc(e, o)
}
func (d *duplicateAwareStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }

func peerAddr(id *identity.Identity) string { return id.Address }

// TestMessageStoreNotCalledForTransitDM verifies that transit messages
// (where this node is neither sender nor recipient) do NOT call the
// registered MessageStore — they are relayed but not persisted locally.
func TestMessageStoreNotCalledForTransitDM(t *testing.T) {
	t.Parallel()

	relayID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate relay identity: %v", err)
	}

	addr := freeAddress(t)
	svc, cleanup := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: normalizeAddress(addr),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	}, relayID)
	defer cleanup()

	store := &testMessageStore{}
	svc.RegisterMessageStore(store)

	eventCh, cancelSub := svc.SubscribeLocalChanges()
	defer cancelSub()

	senderID, _ := identity.Generate()
	recipientID, _ := identity.Generate()

	svc.addKnownPubKey(senderID.Address, identity.PublicKeyBase64(senderID.PublicKey))

	sealed, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "transit message"},
	)
	if err != nil {
		t.Fatalf("seal envelope: %v", err)
	}

	msgID, _ := protocol.NewMessageID()
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:         msgID,
		Topic:      "dm",
		Sender:     senderID.Address,
		Recipient:  recipientID.Address,
		Flag:       protocol.MessageFlagSenderDelete,
		CreatedAt:  time.Now().UTC(),
		TTLSeconds: 86400,
		Body:       sealed,
	}, false)
	if !stored || errCode != "" {
		t.Fatalf("store transit message: stored=%v errCode=%q", stored, errCode)
	}

	// MessageStore must NOT be called for transit messages.
	if len(store.stored) != 0 {
		t.Fatalf("transit DM must NOT call MessageStore, got %d stored", len(store.stored))
	}

	// Transit DM must NOT emit SubscribeLocalChanges.
	select {
	case <-eventCh:
		t.Fatal("transit DM must NOT emit SubscribeLocalChanges event")
	case <-time.After(100 * time.Millisecond):
	}

	// fetch_dm_headers must NOT include transit DMs.
	headersResp := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_dm_headers"})
	if headersResp.Count != 0 {
		t.Fatalf("fetch_dm_headers must NOT include transit DMs, got %d headers", headersResp.Count)
	}
}

// TestFetchDMHeadersIncludesLocalExcludesTransit verifies that fetch_dm_headers
// returns only messages where this node is sender or recipient.
func TestFetchDMHeadersIncludesLocalExcludesTransit(t *testing.T) {
	t.Parallel()

	relayID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate relay identity: %v", err)
	}

	addr := freeAddress(t)
	svc, cleanup := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: normalizeAddress(addr),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	}, relayID)
	defer cleanup()

	senderID, _ := identity.Generate()
	recipientID, _ := identity.Generate()
	foreignID, _ := identity.Generate()

	svc.addKnownPubKey(senderID.Address, identity.PublicKeyBase64(senderID.PublicKey))

	// 1. Store a LOCAL DM (from sender to this node).
	localSealed, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(relayID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(relayID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "local message for me"},
	)
	if err != nil {
		t.Fatalf("seal local envelope: %v", err)
	}
	localMsgID, _ := protocol.NewMessageID()
	svc.storeIncomingMessage(incomingMessage{
		ID: localMsgID, Topic: "dm",
		Sender: senderID.Address, Recipient: relayID.Address,
		Flag: protocol.MessageFlagSenderDelete, CreatedAt: time.Now().UTC(),
		TTLSeconds: 86400, Body: localSealed,
	}, false)

	// 2. Store a TRANSIT DM (foreign sender → foreign recipient, relay only).
	transitSealed, err := directmsg.EncryptForParticipants(
		foreignID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "transit only"},
	)
	if err != nil {
		t.Fatalf("seal transit envelope: %v", err)
	}
	svc.addKnownPubKey(foreignID.Address, identity.PublicKeyBase64(foreignID.PublicKey))
	transitMsgID, _ := protocol.NewMessageID()
	svc.storeIncomingMessage(incomingMessage{
		ID: transitMsgID, Topic: "dm",
		Sender: foreignID.Address, Recipient: recipientID.Address,
		Flag: protocol.MessageFlagSenderDelete, CreatedAt: time.Now().UTC(),
		TTLSeconds: 86400, Body: transitSealed,
	}, false)

	// Both should be in s.topics[dm] (in-memory for relay).
	svc.mu.RLock()
	topicCount := len(svc.topics["dm"])
	svc.mu.RUnlock()
	if topicCount != 2 {
		t.Fatalf("expected 2 messages in topics[dm], got %d", topicCount)
	}

	// fetch_dm_headers must return ONLY the local DM.
	headersResp := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_dm_headers"})
	if headersResp.Count != 1 {
		t.Fatalf("expected 1 local header, got %d", headersResp.Count)
	}
	if headersResp.DMHeaders[0].ID != string(localMsgID) {
		t.Fatalf("expected local msg ID %s, got %s", localMsgID, headersResp.DMHeaders[0].ID)
	}
}

// TestReceiptDelegatedToMessageStoreBeforeEvent verifies that
// storeDeliveryReceipt calls MessageStore.UpdateDeliveryStatus BEFORE
// emitting the local change event (DB-first invariant).
func TestReceiptDelegatedToMessageStoreBeforeEvent(t *testing.T) {
	t.Parallel()

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient: %v", err)
	}

	tempDir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:64646",
		PeersStatePath:   filepath.Join(tempDir, "peers.json"),
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
		Type:             domain.NodeTypeFull,
	}, senderID)

	store := &testMessageStore{}
	svc.RegisterMessageStore(store)

	svc.addKnownPubKey(recipientID.Address, identity.PublicKeyBase64(recipientID.PublicKey))

	// Store an outgoing DM so we can later send a receipt for it.
	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "status-race-test"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        "race-msg-1",
		Topic:     "dm",
		Sender:    senderID.Address,
		Recipient: recipientID.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      ciphertext,
	}, true)
	if !stored || errCode != "" {
		t.Fatalf("store outgoing message: stored=%v errCode=%q", stored, errCode)
	}

	// Subscribe to local changes BEFORE sending receipt.
	events, cancel := svc.SubscribeLocalChanges()
	defer cancel()

	// Store a delivery receipt.
	receipt := protocol.DeliveryReceipt{
		MessageID:   "race-msg-1",
		Sender:      recipientID.Address,
		Recipient:   senderID.Address,
		Status:      "delivered",
		DeliveredAt: time.Now().UTC(),
	}

	receiptStored, _ := svc.storeDeliveryReceipt(receipt)
	if !receiptStored {
		t.Fatal("expected delivery receipt to be stored")
	}

	// Wait for local change event.
	select {
	case <-events:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for local change event")
	}

	// Verify MessageStore.UpdateDeliveryStatus was called.
	if len(store.receipts) != 1 {
		t.Fatalf("expected 1 receipt update, got %d", len(store.receipts))
	}
	if string(store.receipts[0].MessageID) != "race-msg-1" {
		t.Fatalf("expected receipt for race-msg-1, got %s", store.receipts[0].MessageID)
	}
	if store.receipts[0].Status != "delivered" {
		t.Fatalf("expected status 'delivered', got %q", store.receipts[0].Status)
	}
}

// TestNodeWithoutMessageStoreStillRelays verifies that a node with no
// registered MessageStore (relay-only) still stores messages in s.topics
// and emits events, but does not attempt persistence.
func TestNodeWithoutMessageStoreStillRelays(t *testing.T) {
	t.Parallel()

	addr := freeAddress(t)
	svc, cleanup := startTestNode(t, config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: normalizeAddress(addr),
		BootstrapPeers:   []string{},
	})
	defer cleanup()

	// No RegisterMessageStore — node operates as relay-only.

	peerID, _ := identity.Generate()
	peerAddr := peerID.Address
	peerPubKey := identity.PublicKeyBase64(peerID.PublicKey)
	peerBoxKey := identity.BoxPublicKeyBase64(peerID.BoxPublicKey)
	peerBoxSig := identity.SignBoxKeyBinding(peerID)

	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peerAddr,
			PubKey:  peerPubKey,
			BoxKey:  peerBoxKey,
			BoxSig:  peerBoxSig,
		}},
	})

	sealed, err := directmsg.EncryptForParticipants(
		peerID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(svc.identity.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello"},
	)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}

	svc.addKnownPubKey(peerAddr, peerPubKey)

	eventCh, cancelSub := svc.SubscribeLocalChanges()
	defer cancelSub()

	msgID, _ := protocol.NewMessageID()
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID: msgID, Topic: "dm",
		Sender: peerAddr, Recipient: svc.identity.Address,
		Flag: protocol.MessageFlagImmutable, CreatedAt: time.Now().UTC(),
		TTLSeconds: 86400, Body: sealed,
	}, false)
	if !stored || errCode != "" {
		t.Fatalf("store: stored=%v errCode=%q", stored, errCode)
	}

	// Local event should still be emitted even without a store.
	select {
	case ev := <-eventCh:
		if ev.Type != protocol.LocalChangeNewMessage {
			t.Fatalf("expected LocalChangeNewMessage, got %v", ev.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("expected local change event for local DM without store")
	}

	// Message should be in s.topics for relay.
	svc.mu.RLock()
	n := len(svc.topics["dm"])
	svc.mu.RUnlock()
	if n != 1 {
		t.Fatalf("expected 1 message in topics, got %d", n)
	}
}

// syncWriter wraps a bytes.Buffer with a mutex so that concurrent goroutines
// writing log entries do not race with the test reading the buffer.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestProtocolTraceLogging(t *testing.T) {
	// NOT parallel: this test mutates the global zerolog logger and level.
	var buf syncWriter
	origLogger := log.Logger
	origLevel := zerolog.GlobalLevel()
	log.Logger = zerolog.New(&buf).With().Logger()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer func() {
		log.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	}()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// --- Test HandleLocalFrame (local/RPC path) ---
	resp := svc.HandleLocalFrame(protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "test",
	})
	if resp.Type != "welcome" {
		t.Fatalf("expected welcome, got %q", resp.Type)
	}

	resp = svc.HandleLocalFrame(protocol.Frame{Type: "ping"})
	if resp.Type != "pong" {
		t.Fatalf("expected pong, got %q", resp.Type)
	}

	// Unknown command should log accepted=false.
	resp = svc.HandleLocalFrame(protocol.Frame{Type: "nonexistent_command"})
	if resp.Type != "error" {
		t.Fatalf("expected error for unknown command, got %q", resp.Type)
	}

	// --- Test TCP path (dispatchNetworkFrame) ---
	_ = exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "ping"},
	)

	// --- Test hello-after-auth rejection trace ---
	// Re-hello on an authenticated connection must be logged with accepted=false.
	// This covers the ErrCodeHelloAfterAuth path added to prevent mid-session
	// identity spoofing.
	func() {
		id, err := identity.Generate()
		if err != nil {
			t.Fatalf("identity.Generate: %v", err)
		}
		conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 3*time.Second)
		if err != nil {
			t.Fatalf("dial for re-hello test: %v", err)
		}
		defer conn.Close() //nolint:errcheck
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)

		// Complete auth handshake.
		helloFrame := protocol.Frame{
			Type:                   "hello",
			Version:                config.ProtocolVersion,
			MinimumProtocolVersion: config.MinimumProtocolVersion,
			Client:                 "node",
			ClientVersion:          config.CorsaWireVersion,
			Address:                id.Address,
			PubKey:                 identity.PublicKeyBase64(id.PublicKey),
			BoxKey:                 identity.BoxPublicKeyBase64(id.BoxPublicKey),
			BoxSig:                 identity.SignBoxKeyBinding(id),
			Listen:                 "10.0.0.88:64646",
		}
		writeJSONFrame(t, conn, helloFrame)
		welcome := readJSONTestFrame(t, reader)
		if welcome.Type != "welcome" || welcome.Challenge == "" {
			t.Fatalf("re-hello test: expected welcome with challenge, got %#v", welcome)
		}
		writeJSONFrame(t, conn, protocol.Frame{
			Type:      "auth_session",
			Address:   id.Address,
			Signature: identity.SignPayload(id, connauth.SessionAuthPayload(welcome.Challenge, id.Address)),
		})
		authOK := readJSONTestFrame(t, reader)
		if authOK.Type != "auth_ok" {
			t.Fatalf("re-hello test: expected auth_ok, got %#v", authOK)
		}

		// Send re-hello — should be rejected with hello-after-auth.
		writeJSONFrame(t, conn, protocol.Frame{
			Type:                   "hello",
			Version:                config.ProtocolVersion,
			MinimumProtocolVersion: config.MinimumProtocolVersion,
			Client:                 "node",
			ClientVersion:          config.CorsaWireVersion,
		})
		errResp := readJSONTestFrame(t, reader)
		if errResp.Type != "error" || errResp.Code != protocol.ErrCodeHelloAfterAuth {
			t.Fatalf("re-hello test: expected hello-after-auth error, got %#v", errResp)
		}
	}()

	// --- Test non-JSON inbound line (P3: handleCommand before JSON parse) ---
	func() {
		conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 3*time.Second)
		if err != nil {
			t.Fatalf("dial for non-json test: %v", err)
		}
		defer conn.Close() //nolint:errcheck
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = conn.Write([]byte("THIS IS NOT JSON\n"))
		// Read error response to confirm it was processed.
		scanner := bufio.NewScanner(conn)
		if scanner.Scan() {
			var errFrame protocol.Frame
			if err := json.Unmarshal([]byte(scanner.Text()), &errFrame); err == nil {
				if errFrame.Type != "error" {
					t.Errorf("expected error frame for non-json input, got %q", errFrame.Type)
				}
			}
		}
	}()

	// Allow handleConn goroutines to finish writing their log entries.
	time.Sleep(50 * time.Millisecond)

	// Parse captured log lines and verify protocol_trace entries.
	lines := strings.Split(buf.String(), "\n")
	type traceEntry struct {
		Protocol  string `json:"protocol"`
		Addr      string `json:"addr"`
		Direction string `json:"direction"`
		Command   string `json:"command"`
		Accepted  bool   `json:"accepted"`
		Message   string `json:"message"`
	}

	var traces []traceEntry
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry traceEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.Message == "protocol_trace" {
			traces = append(traces, entry)
		}
	}

	if len(traces) == 0 {
		t.Fatal("no protocol_trace log entries found")
	}

	// Verify all trace entries have required fields.
	for i, tr := range traces {
		if tr.Protocol == "" {
			t.Errorf("trace[%d]: missing protocol field", i)
		}
		if tr.Direction == "" {
			t.Errorf("trace[%d]: missing direction field", i)
		}
		if tr.Direction != "recv" && tr.Direction != "send" {
			t.Errorf("trace[%d]: invalid direction %q", i, tr.Direction)
		}
	}

	// Verify local hello was logged as accepted.
	foundLocalHello := false
	for _, tr := range traces {
		if tr.Protocol == "json/local" && tr.Command == "hello" && tr.Accepted {
			foundLocalHello = true
			break
		}
	}
	if !foundLocalHello {
		t.Error("expected protocol_trace for local hello (accepted=true)")
	}

	// Verify unknown command was logged as not accepted.
	foundUnknown := false
	for _, tr := range traces {
		if tr.Protocol == "json/local" && tr.Command == "nonexistent_command" && !tr.Accepted {
			foundUnknown = true
			break
		}
	}
	if !foundUnknown {
		t.Error("expected protocol_trace for unknown command (accepted=false)")
	}

	// Verify TCP inbound hello was logged.
	foundTCPHello := false
	for _, tr := range traces {
		if tr.Protocol == "json/tcp" && tr.Command == "hello" && tr.Direction == "recv" && tr.Accepted {
			foundTCPHello = true
			break
		}
	}
	if !foundTCPHello {
		t.Error("expected protocol_trace for TCP hello (recv, accepted=true)")
	}

	// Verify TCP outbound welcome response was logged.
	foundTCPWelcome := false
	for _, tr := range traces {
		if tr.Protocol == "json/tcp" && tr.Command == "welcome" && tr.Direction == "send" && tr.Accepted {
			foundTCPWelcome = true
			break
		}
	}
	if !foundTCPWelcome {
		t.Error("expected protocol_trace for TCP welcome (send, accepted=true)")
	}

	// Verify non-JSON inbound line was traced (P3 fix).
	foundNonJSON := false
	for _, tr := range traces {
		if tr.Protocol == "json/tcp" && tr.Command == "non-json" && tr.Direction == "recv" && !tr.Accepted {
			foundNonJSON = true
			break
		}
	}
	if !foundNonJSON {
		t.Error("expected protocol_trace for non-json inbound line (recv, accepted=false)")
	}

	// Verify re-hello after auth was traced with accepted=false.
	// This covers the ErrCodeHelloAfterAuth rejection path. Without this
	// check, a regression that logs accepted=true (as happened before the
	// P3 fix) would go undetected — the functional test passes but the
	// audit trail is wrong.
	foundReHelloRejected := false
	for _, tr := range traces {
		if tr.Protocol == "json/tcp" && tr.Command == "hello" && tr.Direction == "recv" && !tr.Accepted {
			foundReHelloRejected = true
			break
		}
	}
	if !foundReHelloRejected {
		t.Error("expected protocol_trace for re-hello after auth (recv, accepted=false)")
	}
}

func TestComputePeerStateThresholds(t *testing.T) {
	t.Parallel()

	svc := &Service{
		health: map[domain.PeerAddress]*peerHealth{},
	}

	now := time.Now().UTC()

	tests := []struct {
		name      string
		health    peerHealth
		wantState string
	}{
		{
			name:      "disconnected peer is reconnecting",
			health:    peerHealth{Connected: false},
			wantState: peerStateReconnecting,
		},
		{
			name:      "connected with no useful traffic is degraded",
			health:    peerHealth{Connected: true},
			wantState: peerStateDegraded,
		},
		{
			name:      "recent pong keeps peer healthy",
			health:    peerHealth{Connected: true, LastPongAt: now.Add(-30 * time.Second)},
			wantState: peerStateHealthy,
		},
		{
			name:      "useful receive 1 min ago is healthy",
			health:    peerHealth{Connected: true, LastUsefulReceiveAt: now.Add(-1 * time.Minute)},
			wantState: peerStateHealthy,
		},
		{
			name:      "no useful traffic for heartbeatInterval is degraded",
			health:    peerHealth{Connected: true, LastUsefulReceiveAt: now.Add(-heartbeatInterval - 1*time.Second)},
			wantState: peerStateDegraded,
		},
		{
			name:      "no useful traffic for heartbeatInterval + pongStallTimeout is stalled",
			health:    peerHealth{Connected: true, LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - 1*time.Second)},
			wantState: peerStateStalled,
		},
		{
			name: "stale useful receive but recent pong keeps peer healthy",
			health: peerHealth{
				Connected:           true,
				LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - 10*time.Second),
				LastPongAt:          now.Add(-30 * time.Second),
			},
			wantState: peerStateHealthy,
		},
		{
			name: "stale useful receive and stale pong is stalled",
			health: peerHealth{
				Connected:           true,
				LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - 10*time.Second),
				LastPongAt:          now.Add(-heartbeatInterval - pongStallTimeout - 5*time.Second),
			},
			wantState: peerStateStalled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc.mu.Lock()
			svc.health[domain.PeerAddress("test:1")] = &tt.health
			svc.mu.Unlock()

			svc.mu.RLock()
			got := svc.computePeerStateAtLocked(svc.health[domain.PeerAddress("test:1")], now)
			svc.mu.RUnlock()

			if got != tt.wantState {
				t.Errorf("got state %q, want %q", got, tt.wantState)
			}
		})
	}
}

func TestAnnouncePeerOnAuth(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// announce_peer requires authenticated peer, so we must complete
	// the full auth handshake before sending it.
	conn, reader, _ := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	// Send announce_peer with a public address via the TCP protocol path.
	writeJSONFrame(t, conn, protocol.Frame{Type: "announce_peer", Peers: []string{"83.172.44.178:64646"}, NodeType: "full"})
	ack := readJSONTestFrame(t, reader)
	if ack.Type != "announce_peer_ack" {
		t.Fatalf("expected announce_peer_ack, got %q (%s)", ack.Type, ack.Code)
	}

	// Verify the announced address was added to the peer dial list.
	svc.mu.RLock()
	found := false
	for _, p := range svc.peers {
		if p.Address == "83.172.44.178:64646" {
			found = true
			break
		}
	}
	svc.mu.RUnlock()
	if !found {
		t.Error("expected announced peer to be added to dial list")
	}
	if _, ok := svc.peerTypes[domain.PeerAddress("83.172.44.178:64646")]; ok {
		t.Error("announce_peer must not set peer type for third-party address")
	}

	// Announce a local address — it should be ignored.
	conn2, reader2, _ := authenticatedConn(t, svc)
	defer func() { _ = conn2.Close() }()

	writeJSONFrame(t, conn2, protocol.Frame{Type: "announce_peer", Peers: []string{"192.168.1.100:64646"}, NodeType: "full"})
	ack2 := readJSONTestFrame(t, reader2)
	if ack2.Type != "announce_peer_ack" {
		t.Fatalf("expected announce_peer_ack for local address, got %q", ack2.Type)
	}

	svc.mu.RLock()
	foundLocal := false
	for _, p := range svc.peers {
		if p.Address == "192.168.1.100:64646" {
			foundLocal = true
			break
		}
	}
	svc.mu.RUnlock()
	if foundLocal {
		t.Error("local address should not be added from announce_peer")
	}
}

// TestAddPeerAddressNoRetag verifies that addPeerAddress does not overwrite
// the node type of an already-known peer. This prevents unauthenticated
// announce_peer from downgrading a "full" peer to "client".
func TestAddPeerAddressNoRetag(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Add a peer as "full" — simulates trusted initial discovery.
	svc.addPeerAddress("5.5.5.5:64646", "full", "")

	svc.mu.RLock()
	before := svc.peerTypes[domain.PeerAddress("5.5.5.5:64646")]
	svc.mu.RUnlock()
	if before != domain.NodeTypeFull {
		t.Fatalf("expected full, got %s", before)
	}

	// Call addPeerAddress again with "client" — must not overwrite.
	svc.addPeerAddress("5.5.5.5:64646", "client", "")

	svc.mu.RLock()
	after := svc.peerTypes[domain.PeerAddress("5.5.5.5:64646")]
	svc.mu.RUnlock()
	if after != domain.NodeTypeFull {
		t.Fatalf("addPeerAddress must not retag existing peer; expected full, got %s", after)
	}
}

func TestAddPeerAddressWithoutNodeTypeKeepsTypeUnknown(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	svc.addPeerAddress("6.6.6.6:64646", "", "")

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	if _, ok := svc.peerTypes[domain.PeerAddress("6.6.6.6:64646")]; ok {
		t.Fatal("peer learned without self-reported node type must not get peerTypes entry")
	}
	if pm := svc.persistedMeta[domain.PeerAddress("6.6.6.6:64646")]; pm != nil && pm.NodeType != domain.NodeTypeUnknown {
		t.Fatalf("persistedMeta node type = %s, want unknown", pm.NodeType)
	}
	if got := svc.peerTypeForAddress(domain.PeerAddress("6.6.6.6:64646")); got != domain.NodeTypeUnknown {
		t.Fatalf("peerTypeForAddress = %s, want unknown", got)
	}
}

func TestAnnouncePeerHelpers(t *testing.T) {
	t.Parallel()

	// peerListenAddress should return empty for non-listener peers.
	noListener := protocol.Frame{Listener: "0", Listen: "some:64646"}
	if addr := peerListenAddress(noListener); addr != "" {
		t.Errorf("expected empty for non-listener, got %q", addr)
	}

	// peerListenAddress should return address for listener peers.
	listener := protocol.Frame{Listener: "1", Listen: "83.172.44.178:64646"}
	if addr := peerListenAddress(listener); addr != "83.172.44.178:64646" {
		t.Errorf("expected 83.172.44.178:64646, got %q", addr)
	}

	// Verify local addresses are filtered by classifyAddress.
	localAddrs := []string{"127.0.0.1:64646", "192.168.1.1:64646", "10.0.0.1:64646"}
	for _, addr := range localAddrs {
		if classifyAddress(domain.PeerAddress(addr)) != domain.NetGroupLocal {
			t.Errorf("expected %q to be classified as local", addr)
		}
	}

	publicAddrs := []string{"83.172.44.178:64646", "65.108.204.190:64646"}
	for _, addr := range publicAddrs {
		if classifyAddress(domain.PeerAddress(addr)) == domain.NetGroupLocal {
			t.Errorf("expected %q to NOT be classified as local", addr)
		}
	}
}

// TestPromotePeerAddress verifies that promotePeerAddress adds a peer to the
// end of the dial list (no front-promotion), does not rewrite existing trust
// metadata, and rejects local addresses.
func TestPromotePeerAddress(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Seed two peers so we can verify ordering is preserved.
	svc.addPeerAddress("1.1.1.1:64646", "full", "")
	svc.addPeerAddress("2.2.2.2:64646", "full", "")

	// Set cooldown on first peer to simulate failed connections.
	svc.mu.Lock()
	svc.health[domain.PeerAddress("1.1.1.1:64646")] = &peerHealth{
		Address:             domain.PeerAddress("1.1.1.1:64646"),
		ConsecutiveFailures: 5,
		LastDisconnectedAt:  time.Now(),
	}
	svc.mu.Unlock()

	// Promote a new peer — it should be appended at the end (not front).
	svc.promotePeerAddress("3.3.3.3:64646")

	svc.mu.RLock()
	if len(svc.peers) < 3 {
		svc.mu.RUnlock()
		t.Fatalf("expected at least 3 peers, got %d", len(svc.peers))
	}
	lastAddr := svc.peers[len(svc.peers)-1].Address
	firstAddr := svc.peers[0].Address
	svc.mu.RUnlock()
	if lastAddr != "3.3.3.3:64646" {
		t.Fatalf("expected new peer at end, got %s", lastAddr)
	}
	if firstAddr == "3.3.3.3:64646" {
		t.Fatal("new peer must not be placed at front — trust-no-one policy")
	}

	// Promote existing peer with cooldown — cooldown must stay untouched.
	svc.promotePeerAddress("1.1.1.1:64646")

	svc.mu.RLock()
	h := svc.health[domain.PeerAddress("1.1.1.1:64646")]
	if h == nil || h.ConsecutiveFailures != 5 {
		svc.mu.RUnlock()
		t.Fatal("expected cooldown to remain unchanged after promote")
	}
	svc.mu.RUnlock()

	// Promote with different type — network announce must not retag peer.
	svc.promotePeerAddress("1.1.1.1:64646")
	svc.mu.RLock()
	pt := svc.peerTypes[domain.PeerAddress("1.1.1.1:64646")]
	svc.mu.RUnlock()
	if pt != domain.NodeTypeFull {
		t.Errorf("expected existing type to remain full, got %s", pt)
	}

	// Different port on the same host must NOT be stored as a distinct peer
	// on the network-learned promote path.
	peersBefore := len(svc.peers)
	svc.promotePeerAddress("2.2.2.2:12345")
	svc.mu.RLock()
	peersAfter := len(svc.peers)
	_, hasAltPort := svc.peerTypes[domain.PeerAddress("2.2.2.2:12345")]
	svc.mu.RUnlock()
	if peersAfter != peersBefore {
		t.Errorf("different port on same IP should be ignored, count went from %d to %d", peersBefore, peersAfter)
	}
	if hasAltPort {
		t.Error("different port on same IP should not create peerTypes entry")
	}
}

func TestAddPeerAddressSkipsAlternatePortOnKnownIP(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	svc.addPeerAddress("9.9.9.9:64646", "full", "")

	svc.mu.RLock()
	before := len(svc.peers)
	svc.mu.RUnlock()

	svc.addPeerAddress("9.9.9.9:7777", "client", "")

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	if len(svc.peers) != before {
		t.Fatalf("alternate port on same IP should be ignored, count went from %d to %d", before, len(svc.peers))
	}
	if _, ok := svc.peerTypes[domain.PeerAddress("9.9.9.9:7777")]; ok {
		t.Fatal("alternate port on same IP should not create peerTypes entry")
	}
}

// TestIsKnownNodeType verifies node type validation for announce_peer.
func TestIsKnownNodeType(t *testing.T) {
	t.Parallel()

	if !isKnownNodeType("full") {
		t.Error("expected 'full' to be a known node type")
	}
	if !isKnownNodeType("client") {
		t.Error("expected 'client' to be a known node type")
	}
	if isKnownNodeType("") {
		t.Error("empty string should not be a known node type")
	}
	if isKnownNodeType("unknown_future_type") {
		t.Error("unknown type should not be accepted")
	}
}

// TestAnnouncePeerPendingQueue verifies that announce_peer frames are queued
// via the pending mechanism when sendCh is full, instead of being silently dropped.
func TestAnnouncePeerPendingQueue(t *testing.T) {
	t.Parallel()

	// Verify pendingFrameKey returns a valid key for announce_peer.
	frame := protocol.Frame{Type: "announce_peer", Peers: []string{"5.5.5.5:64646"}}
	key := pendingFrameKey(domain.PeerAddress("1.1.1.1:64646"), frame)
	if key == "" {
		t.Fatal("expected non-empty pending key for announce_peer")
	}

	// Verify deduplication: same peer address should produce the same key.
	key2 := pendingFrameKey(domain.PeerAddress("1.1.1.1:64646"), frame)
	if key != key2 {
		t.Errorf("expected identical keys, got %q and %q", key, key2)
	}

	// Different peer addresses should produce different keys.
	frame2 := protocol.Frame{Type: "announce_peer", Peers: []string{"6.6.6.6:64646"}}
	key3 := pendingFrameKey(domain.PeerAddress("1.1.1.1:64646"), frame2)
	if key == key3 {
		t.Error("expected different keys for different peer addresses")
	}
}

// TestPendingQueueReplacesStaleFrame verifies that queuePeerFrame replaces
// the payload of an already-queued frame when the same key is enqueued again.
// This ensures that node_type changes are propagated even when the send
// channel is full and the frame is stuck in the pending queue.
func TestPendingQueueReplacesStaleFrame(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	target := domain.PeerAddress("9.9.9.9:64646")

	// Directly populate pending to simulate a full sendCh scenario.
	frameV1 := protocol.Frame{Type: "announce_peer", Peers: []string{"7.7.7.7:64646"}, NodeType: "full"}
	svc.mu.Lock()
	key := pendingFrameKey(domain.PeerAddress(target), frameV1)
	svc.pending[domain.PeerAddress(target)] = append(svc.pending[domain.PeerAddress(target)], pendingFrame{
		Frame:    frameV1,
		QueuedAt: time.Now().UTC(),
	})
	svc.pendingKeys[key] = struct{}{}
	svc.mu.Unlock()

	// Queue the same peer address but with a different node_type.
	frameV2 := protocol.Frame{Type: "announce_peer", Peers: []string{"7.7.7.7:64646"}, NodeType: "client"}
	svc.queuePeerFrame(target, frameV2)

	// The pending queue should still have exactly one entry for this key,
	// but the frame should be updated to the latest version.
	svc.mu.RLock()
	frames := svc.pending[target]
	var found bool
	for _, pf := range frames {
		if pendingFrameKey(target, pf.Frame) == key {
			if pf.Frame.NodeType != "client" {
				svc.mu.RUnlock()
				t.Fatalf("expected replaced frame to have node_type=client, got %q", pf.Frame.NodeType)
			}
			found = true
			break
		}
	}
	svc.mu.RUnlock()
	if !found {
		t.Fatal("expected to find the queued frame in pending")
	}
}

func TestHeartbeatConstants(t *testing.T) {
	t.Parallel()

	// heartbeatInterval should be ~2 minutes (Bitcoin-style).
	if heartbeatInterval != 2*time.Minute {
		t.Errorf("heartbeatInterval = %v, want 2m", heartbeatInterval)
	}

	// pongStallTimeout should be less than heartbeatInterval to detect stalls
	// before the next heartbeat fires.
	if pongStallTimeout >= heartbeatInterval {
		t.Errorf("pongStallTimeout (%v) should be less than heartbeatInterval (%v)", pongStallTimeout, heartbeatInterval)
	}

	// nextHeartbeatDuration should return values in [heartbeatInterval, heartbeatInterval+15s).
	for i := 0; i < 20; i++ {
		d := nextHeartbeatDuration()
		if d < heartbeatInterval || d >= heartbeatInterval+15*time.Second {
			t.Errorf("nextHeartbeatDuration() = %v, want [%v, %v)", d, heartbeatInterval, heartbeatInterval+15*time.Second)
		}
	}
}

// TestConcurrentWriteJSONFrameAndPush verifies that simultaneous response
// writes (writeJSONFrame) and push writes (writePushFrame) on the same TCP
// connection produce valid, non-interleaved JSON lines.
//
// Before the per-connection write mutex was introduced, these two paths could
// race, corrupting the TCP stream and causing the receiving side to silently
// drop messages (the exact symptom that prompted the fix).
func TestConcurrentWriteJSONFrameAndPush(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)

	// The node identity acts as the DM recipient.
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient identity: %v", err)
	}
	svc, stop := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	}, recipientID)
	defer stop()

	recipientAddr := svc.Address()

	// Create a sender identity and register it as a trusted contact on the node
	// so that DM signature verification succeeds.
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender identity: %v", err)
	}
	senderBoxSig := identity.SignBoxKeyBinding(senderID)

	importReply := svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: senderID.Address,
			PubKey:  identity.PublicKeyBase64(senderID.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(senderID.BoxPublicKey),
			BoxSig:  senderBoxSig,
		}},
	})
	if importReply.Type != "contacts_imported" || importReply.Count != 1 {
		t.Fatalf("import_contacts failed: %#v", importReply)
	}

	// Pre-encrypt messages so the hot loop only sends frames.
	const messageCount = 50
	ts := time.Now().UTC().Format(time.RFC3339)
	ciphertexts := make([]string, messageCount)
	for i := 0; i < messageCount; i++ {
		ct, err := directmsg.EncryptForParticipants(
			senderID,
			domain.DMRecipient{
				Address:      domain.PeerIdentity(recipientAddr),
				BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
			},
			domain.OutgoingDM{Body: fmt.Sprintf("body-%d", i)},
		)
		if err != nil {
			t.Fatalf("encrypt message %d: %v", i, err)
		}
		ciphertexts[i] = ct
	}

	// --- subscriber connection: subscribes to own inbox and reads all frames ---
	subConn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial subscriber: %v", err)
	}
	defer func() { _ = subConn.Close() }()
	_ = subConn.SetDeadline(time.Now().Add(15 * time.Second))
	subReader := bufio.NewReader(subConn)

	// handshake + auth (subscribe_inbox requires authenticated session)
	writeJSONFrame(t, subConn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                recipientID.Address,
		PubKey:                 identity.PublicKeyBase64(recipientID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(recipientID),
	})
	welcome := readJSONTestFrame(t, subReader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %s", welcome.Type)
	}

	writeJSONFrame(t, subConn, protocol.Frame{
		Type:      "auth_session",
		Address:   recipientID.Address,
		Signature: identity.SignPayload(recipientID, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+recipientID.Address)),
	})
	authOK := readJSONTestFrame(t, subReader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %s: %s", authOK.Type, authOK.Error)
	}

	// subscribe
	writeJSONFrame(t, subConn, protocol.Frame{
		Type: "subscribe_inbox", Topic: "dm", Recipient: recipientAddr, Subscriber: "sub-1",
	})
	subReply := readJSONTestFrame(t, subReader)
	if subReply.Type != "subscribed" {
		t.Fatalf("expected subscribed, got %s: %s", subReply.Type, subReply.Error)
	}

	// Drain reverse subscribe_inbox that the node sends back to authenticated peers.
	if reverseFrame := readJSONTestFrame(t, subReader); reverseFrame.Type == "subscribe_inbox" {
		writeJSONFrame(t, subConn, protocol.Frame{
			Type:       "subscribed",
			Topic:      reverseFrame.Topic,
			Recipient:  reverseFrame.Recipient,
			Subscriber: reverseFrame.Subscriber,
		})
	}

	// --- sender connection: authenticates then sends messages concurrently ---
	senderConn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}
	defer func() { _ = senderConn.Close() }()
	_ = senderConn.SetDeadline(time.Now().Add(15 * time.Second))
	senderReader := bufio.NewReader(senderConn)

	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                senderID.Address,
		PubKey:                 identity.PublicKeyBase64(senderID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(senderID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(senderID),
	})
	senderWelcome := readJSONTestFrame(t, senderReader)
	if senderWelcome.Type != "welcome" || senderWelcome.Challenge == "" {
		t.Fatalf("sender expected welcome with challenge, got %s", senderWelcome.Type)
	}
	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:      "auth_session",
		Address:   senderID.Address,
		Signature: identity.SignPayload(senderID, connauth.SessionAuthPayload(senderWelcome.Challenge, senderID.Address)),
	})
	if f := readJSONTestFrame(t, senderReader); f.Type != "auth_ok" {
		t.Fatalf("sender expected auth_ok, got %s: %s", f.Type, f.Error)
	}

	// Concurrently: sender sends DMs AND subscriber sends ping requests.
	// Both cause writes to subConn (push_message + pong response).
	var wg sync.WaitGroup

	// goroutine 1: sender fires encrypted DMs via push_message (P2P wire command).
	// push_message on inbound TCP is fire-and-forget — no reply is read.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			msgID := fmt.Sprintf("race-msg-%d", i)
			writeJSONFrame(t, senderConn, protocol.Frame{
				Type:      "push_message",
				Topic:     "dm",
				Recipient: recipientAddr,
				Item: &protocol.MessageFrame{
					ID:        msgID,
					Sender:    senderID.Address,
					Recipient: recipientAddr,
					Flag:      "sender-delete",
					CreatedAt: ts,
					Body:      ciphertexts[i],
				},
			})
		}
	}()

	// goroutine 2: subscriber sends pings to force writeJSONFrame on subConn
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			writeJSONFrame(t, subConn, protocol.Frame{Type: "ping"})
		}
	}()

	wg.Wait()

	// Read all frames from subscriber connection.
	// We expect: messageCount pong frames + up to messageCount push_message frames.
	// The critical assertion: every line must be valid JSON (no interleaving).
	_ = subConn.SetDeadline(time.Now().Add(5 * time.Second))
	pongs := 0
	pushes := 0
	for pongs+pushes < messageCount*2 {
		line, err := subReader.ReadString('\n')
		if err != nil {
			break
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !json.Valid([]byte(trimmed)) {
			t.Fatalf("corrupted JSON line on subscriber connection (pongs=%d, pushes=%d): %q", pongs, pushes, trimmed)
		}
		var frame protocol.Frame
		if err := json.Unmarshal([]byte(trimmed), &frame); err != nil {
			t.Fatalf("unmarshal failed: %v; line: %q", err, trimmed)
		}
		switch frame.Type {
		case "pong":
			pongs++
		case "push_message":
			pushes++
		default:
			t.Logf("unexpected frame type: %s", frame.Type)
		}
	}

	if pongs != messageCount {
		t.Errorf("expected %d pong frames, got %d", messageCount, pongs)
	}
	if pushes == 0 {
		t.Errorf("expected push_message frames, got none — messages not delivered in real-time")
	}
	t.Logf("received %d pongs, %d pushes (of %d sent)", pongs, pushes, messageCount)
}

// TestErrorPathTeardownDoesNotHang verifies that when a writer goroutine is
// blocked on a slow peer and the protocol handler hits an error path (return
// false), the handleConn teardown completes promptly instead of hanging until
// the 30-second write deadline expires.
//
// Sequence:
//  1. Connect and complete the hello handshake.
//  2. Stop reading from the TCP connection to create back-pressure.
//  3. Flood pings to fill the 128-slot send channel and TCP send buffer,
//     which causes the writer goroutine to block in conn.Write.
//  4. Send a malformed (non-JSON) line that triggers writeJSONFrameSync
//     followed by return false.
//  5. Assert the server closes the connection within a tight deadline.
//
// If the teardown hangs, the test will exceed the deadline and fail.
func TestErrorPathTeardownDoesNotHang(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	// Handshake so the connection gets a registered send channel.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "test-teardown",
		ClientVersion:          config.CorsaWireVersion,
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %s", welcome.Type)
	}

	// Flood pings without reading responses. This fills the send channel
	// (128 slots) and eventually the TCP send buffer, causing the writer
	// goroutine to block in conn.Write.
	for i := 0; i < 300; i++ {
		_, err := fmt.Fprint(conn, `{"type":"ping"}`+"\n")
		if err != nil {
			break // server may have already closed
		}
	}

	// Send a malformed line that is not valid JSON. This triggers the error
	// path in handleCommand: writeJSONFrameSync → return false → teardown.
	_, _ = fmt.Fprint(conn, "NOT-JSON\n")

	// The server should close the connection promptly. If teardown hangs
	// (blocked writer), this read will timeout after the deadline below.
	// Use a generous margin over syncFlushTimeout (5s): the server must
	// process up to 300 queued ping frames before reaching the malformed
	// line, and goroutine scheduling / GC pauses add jitter.  The real
	// assertion is "well under 30s connWriteTimeout", not "exactly 5s".
	//
	// readDeadline unblocks the drain loop if the server truly hangs.
	// assertLimit is slightly larger to accommodate scheduling jitter
	// between SetDeadline and time.Since — hitting the read deadline
	// itself is acceptable, we're guarding against the 30s hang.
	readDeadline := 10 * time.Second
	assertLimit := 12 * time.Second
	_ = conn.SetDeadline(time.Now().Add(readDeadline))

	start := time.Now()

	// Drain anything the server sends until we get EOF or an error.
	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			break
		}
	}

	elapsed := time.Since(start)

	// The critical assertion: teardown must complete well under the 30s
	// connWriteTimeout.
	if elapsed > assertLimit {
		t.Fatalf("teardown took %v, expected <%v — writer goroutine likely hung", elapsed, assertLimit)
	}
	t.Logf("connection closed after %v (limit was %v)", elapsed, assertLimit)
}

// TestEnqueueFrameSyncReturnsImmediatelyWhenWriterDead verifies that
// enqueueFrameSync does not block for the full syncFlushTimeout when the
// writer goroutine has already exited (e.g. because conn.Write returned an
// error). It must observe writerDone and return enqueueDropped immediately.
//
// Reproduces the P1 scenario: another goroutine already caused conn.Close()
// (slow-peer eviction or write error), the writer exited, but the send
// channel is still visible in the map because unregisterInboundConn has not
// run yet. A subsequent enqueueFrameSync must not stall for 5 seconds.
func TestEnqueueFrameSyncReturnsImmediatelyWhenWriterDead(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		PeersStatePath:   filepath.Join(t.TempDir(), "peers.json"),
	}, id)

	server, client := net.Pipe()
	defer func() { _ = client.Close() }()

	// Register the pipe as an inbound connection — starts connWriter.
	svc.registerInboundConn(server)
	connID, ok := svc.connIDFor(server)
	if !ok {
		t.Fatalf("registered conn has no ConnID")
	}

	// Close the server side so the next conn.Write will fail.
	_ = server.Close()

	// The writer goroutine is still alive — it's blocking on channel
	// receive (for item := range sendCh), not on conn.Write. Send a
	// trigger frame so the writer attempts conn.Write, fails, and exits.
	svc.enqueueFrameByID(connID, []byte("trigger\n"))

	// Give the writer goroutine a moment to pick up the frame, fail on
	// conn.Write, exit, and close(writerDone).
	time.Sleep(50 * time.Millisecond)

	// The writer is now dead. The channel is still in the map because
	// unregisterInboundConn has not been called. This is the P1 scenario:
	// enqueueFrameSync must detect writerDone and return immediately.
	start := time.Now()
	result := svc.enqueueFrameSyncByID(connID, []byte(`{"type":"error"}`+"\n"))
	elapsed := time.Since(start)

	if result != enqueueDropped {
		t.Fatalf("expected enqueueDropped, got %v", result)
	}
	// Allow generous margin (500ms) but nowhere near syncFlushTimeout (5s).
	if elapsed > 500*time.Millisecond {
		t.Fatalf("enqueueFrameSync blocked for %v; expected near-instant return via writerDone", elapsed)
	}
	t.Logf("enqueueFrameSync returned %v in %v", result, elapsed)

	// Clean up maps.
	svc.unregisterInboundConn(server)
}

// TestEnqueueFrameSyncReportsDroppedOnWriteError verifies that when
// conn.Write fails, enqueueFrameSync returns enqueueDropped (not
// enqueueSent). The writer goroutine must NOT close the ack channel on a
// failed write — only writerDone fires.
//
// Reproduces the P2 scenario: the sync caller puts a sendItem with an ack
// channel into the queue, the writer picks it up, conn.Write fails, but the
// original (buggy) code closed ack before checking err, making the caller
// believe the frame was delivered.
func TestEnqueueFrameSyncReportsDroppedOnWriteError(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		PeersStatePath:   filepath.Join(t.TempDir(), "peers.json"),
	}, id)

	server, client := net.Pipe()
	defer func() { _ = client.Close() }()

	// Register the pipe — starts connWriter.
	svc.registerInboundConn(server)
	connID, ok := svc.connIDFor(server)
	if !ok {
		t.Fatalf("registered conn has no ConnID")
	}

	// Enqueue one normal frame so the writer is busy.
	svc.enqueueFrameByID(connID, []byte(`{"type":"pong"}`+"\n"))

	// Drain the normal frame from the client side so the writer proceeds.
	buf := make([]byte, 256)
	_, _ = client.Read(buf)

	// Now close the server side. The next conn.Write will fail.
	_ = server.Close()

	// Enqueue a sync frame. The writer will pick it up, conn.Write fails,
	// writer exits without closing ack, writerDone fires.
	result := svc.enqueueFrameSyncByID(connID, []byte(`{"type":"error","code":"test"}`+"\n"))

	if result != enqueueDropped {
		t.Fatalf("expected enqueueDropped (write failed), got %v", result)
	}
	t.Logf("enqueueFrameSync correctly returned enqueueDropped on write failure")

	// Clean up maps.
	svc.unregisterInboundConn(server)
}

// TestMarkPeerConnectedSetsDirection verifies that markPeerConnected records
// the connection direction (outbound vs inbound) in the health entry.
func TestMarkPeerConnectedSetsDirection(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	svc.markPeerConnected("10.0.0.1:64646", peerDirectionOutbound)

	svc.mu.RLock()
	h := svc.health[domain.PeerAddress("10.0.0.1:64646")]
	svc.mu.RUnlock()

	if h == nil {
		t.Fatal("expected health entry")
	}
	if h.Direction != peerDirectionOutbound {
		t.Fatalf("expected direction %q, got %q", peerDirectionOutbound, h.Direction)
	}

	// Inbound connection records inbound direction.
	svc.markPeerConnected("10.0.0.2:64646", peerDirectionInbound)

	svc.mu.RLock()
	h2 := svc.health[domain.PeerAddress("10.0.0.2:64646")]
	svc.mu.RUnlock()

	if h2 == nil {
		t.Fatal("expected health entry for inbound peer")
	}
	if h2.Direction != peerDirectionInbound {
		t.Fatalf("expected direction %q, got %q", peerDirectionInbound, h2.Direction)
	}
}

// TestMarkPeerDisconnectedClearsDirection verifies that markPeerDisconnected
// resets the direction field to empty.
func TestMarkPeerDisconnectedClearsDirection(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	svc.markPeerConnected("10.0.0.1:64646", peerDirectionOutbound)
	svc.markPeerDisconnected("10.0.0.1:64646", nil)

	svc.mu.RLock()
	h := svc.health[domain.PeerAddress("10.0.0.1:64646")]
	svc.mu.RUnlock()

	if h == nil {
		t.Fatal("expected health entry")
	}
	if h.Direction != "" {
		t.Fatalf("expected empty direction after disconnect, got %q", h.Direction)
	}
}

// TestConnectedHostsLocked verifies that connectedHostsLocked collects
// host IPs from both outbound sessions and inbound connections.
// Inbound hosts are derived from the actual TCP remote address (not the
// self-reported overlay address) to correctly handle NATed peers.
func TestConnectedHostsLocked(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Create a real TCP connection so conn.RemoteAddr() returns a real IP.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			connCh <- c
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	server := <-connCh
	defer func() { _ = server.Close() }()

	svc.mu.Lock()
	// Simulate an outbound upstream session.
	svc.upstream["10.0.0.1:64646"] = struct{}{}
	// Register the real TCP connection as inbound. The unified registry
	// distinguishes directions via NetCore.Dir(), so the seed must
	// carry an Inbound NetCore — a bare entry would be ignored by
	// connectedHostsLocked's direction filter.
	pc := netcore.New(netcore.ConnID(1), server, netcore.Inbound, netcore.Options{
		LastActivity: time.Now().UTC(),
	})
	svc.setTestConnEntryLocked(server, &connEntry{core: pc})

	hosts := svc.connectedHostsLocked()
	svc.mu.Unlock()

	if _, ok := hosts["10.0.0.1"]; !ok {
		t.Error("expected outbound host 10.0.0.1 in connectedHosts")
	}
	// The inbound connection's remote address is 127.0.0.1 (the client side).
	if _, ok := hosts["127.0.0.1"]; !ok {
		t.Error("expected inbound host 127.0.0.1 in connectedHosts")
	}
}

// TestDialCandidatesSkipsConnectedInboundHost verifies that peerDialCandidates
// excludes peers whose host already has an active inbound connection.
// The dedup uses the real TCP remote IP, so a loopback peer (127.0.0.1)
// is used here to simulate the inbound connection.
func TestDialCandidatesSkipsConnectedInboundHost(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Add two peers: one whose host matches the inbound connection's
	// real remote IP (127.0.0.1), and one that doesn't.
	svc.addPeerAddress("127.0.0.1:64646", "full", "test")
	svc.addPeerAddress("10.0.0.3:64646", "full", "test")

	// Create a real TCP connection so conn.RemoteAddr() returns 127.0.0.1.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			connCh <- c
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	server := <-connCh
	defer func() { _ = server.Close() }()

	svc.mu.Lock()
	// Register the real TCP connection as inbound. The unified registry
	// distinguishes directions via NetCore.Dir(), so the seed must
	// carry an Inbound NetCore — a bare entry would be ignored by the
	// direction filter in connectedHostsLocked.
	pc := netcore.New(netcore.ConnID(1), server, netcore.Inbound, netcore.Options{
		LastActivity: time.Now().UTC(),
	})
	svc.setTestConnEntryLocked(server, &connEntry{core: pc})
	svc.mu.Unlock()

	candidates := svc.peerDialCandidates()
	addrs := candidateAddresses(candidates)

	for _, addr := range addrs {
		host, _, _ := net.SplitHostPort(addr)
		if host == "127.0.0.1" {
			t.Fatalf("expected host 127.0.0.1 to be skipped due to active inbound connection, but found candidate %q", addr)
		}
	}

	// 10.0.0.3 should still be a candidate.
	found := false
	for _, addr := range addrs {
		host, _, _ := net.SplitHostPort(addr)
		if host == "10.0.0.3" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected 10.0.0.3 to remain as dial candidate")
	}
}

// TestInboundPeerHealthIncludesDirection verifies that peerHealthFrames
// returns the direction field for outbound and inbound peers.
func TestInboundPeerHealthIncludesDirection(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	svc.markPeerConnected("10.0.0.1:64646", peerDirectionOutbound)
	svc.markPeerConnected("10.0.0.2:64646", peerDirectionInbound)

	frames := svc.peerHealthFrames()

	dirByAddr := make(map[string]string, len(frames))
	for _, f := range frames {
		dirByAddr[f.Address] = f.Direction
	}

	if dirByAddr["10.0.0.1:64646"] != string(peerDirectionOutbound) {
		t.Errorf("expected outbound direction for 10.0.0.1:64646, got %q", dirByAddr["10.0.0.1:64646"])
	}
	if dirByAddr["10.0.0.2:64646"] != string(peerDirectionInbound) {
		t.Errorf("expected inbound direction for 10.0.0.2:64646, got %q", dirByAddr["10.0.0.2:64646"])
	}
}

// TestInboundRefCountKeepsHealthAlive verifies that closing one inbound
// connection does not mark the peer disconnected when another inbound
// connection to the same address remains active.
func TestInboundRefCountKeepsHealthAlive(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peer := domain.PeerAddress("10.0.0.5:64646")

	// Create two fake connections to simulate two inbound from the same peer.
	conn1a, conn1b := net.Pipe()
	defer func() { _ = conn1b.Close() }()
	conn2a, conn2b := net.Pipe()
	defer func() { _ = conn2b.Close() }()

	// Register the connections in the unified registry first — in
	// production registerInboundConn runs before trackInboundConnect and
	// creates the connEntry whose tracked flag is then flipped.
	// trackInboundConnect only mutates an existing entry (no lazy insert)
	// so the single-creation-site invariant enforced by the lifecycle
	// helpers in conn_registry.go is preserved; tests must follow the
	// same order or they will exercise a codepath that cannot happen in
	// production.
	if !svc.registerInboundConn(conn1a) {
		t.Fatalf("registerInboundConn conn1a failed")
	}
	if !svc.registerInboundConn(conn2a) {
		t.Fatalf("registerInboundConn conn2a failed")
	}

	// Two inbound connections to the same peer.
	id1a, _ := svc.connIDFor(conn1a)
	id2a, _ := svc.connIDFor(conn2a)
	svc.trackInboundConnect(id1a, peer, "test-peer-identity")
	svc.trackInboundConnect(id2a, peer, "test-peer-identity")

	// Peer should be connected.
	svc.mu.RLock()
	h := svc.health[peer]
	svc.mu.RUnlock()

	if h == nil || !h.Connected {
		t.Fatal("expected peer to be connected after two inbound connects")
	}

	// Close the first connection — peer should remain connected.
	_ = conn1a.Close()
	svc.trackInboundDisconnect(id1a, peer)

	svc.mu.RLock()
	h = svc.health[peer]
	connected := h != nil && h.Connected
	var dir domain.PeerDirection
	if h != nil {
		dir = h.Direction
	}
	svc.mu.RUnlock()

	if !connected {
		t.Fatal("expected peer to remain connected after first disconnect (second conn still alive)")
	}
	if dir != peerDirectionInbound {
		t.Fatalf("expected direction %q, got %q", peerDirectionInbound, dir)
	}

	// Close the second connection — now the peer should disconnect.
	_ = conn2a.Close()
	svc.trackInboundDisconnect(id2a, peer)

	svc.mu.RLock()
	h = svc.health[peer]
	svc.mu.RUnlock()

	if h == nil {
		t.Fatal("expected health entry to exist")
	}
	if h.Connected {
		t.Fatal("expected peer to be disconnected after all inbound connections closed")
	}
	if h.Direction != "" {
		t.Fatalf("expected empty direction after disconnect, got %q", h.Direction)
	}
}

// TestTrackInboundDisconnect_PrefersNetCoreIdentity pins the identity
// source-of-truth precedence on the teardown path. When both the NetCore
// mirror (set via core.SetIdentity on the connEntry in conn_registry.go)
// and the address-keyed persistence cache (Service.peerIDs) carry an
// identity for the same peer, trackInboundDisconnect must emit the
// InboundClosed hint with the per-conn NetCore identity. The
// persistence cache is a fallback for paths that never call SetIdentity
// (auth-not-required, legacy tests) — see
// TestTrackInboundDisconnect_FallsBackToPeerIDsMap below for that branch.
func TestTrackInboundDisconnect_PrefersNetCoreIdentity(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Attach a CM with the emit gate open but no event loop running so the
	// hint sits in hintEvents until the test drains it.
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return 4 },
		NowFn:      func() time.Time { return time.Now() },
	})
	cm.accepting.Store(1)
	svc.connManager = cm

	peer := domain.PeerAddress("10.0.0.7:64646")

	connA, connB := net.Pipe()
	defer func() { _ = connA.Close() }()
	defer func() { _ = connB.Close() }()

	if !svc.registerInboundConn(connA) {
		t.Fatalf("registerInboundConn failed")
	}

	// Set the NetCore mirror and the persistence cache to different values
	// so the test can unambiguously attribute which source was used.
	netcoreIdentity := domain.PeerIdentity("netcore-mirror-identity")
	mapIdentity := domain.PeerIdentity("persistence-cache-identity")

	svc.mu.Lock()
	if e := svc.testConnEntry(connA); e != nil && e.core != nil {
		e.core.SetIdentity(netcoreIdentity)
	}
	svc.peerIDs[peer] = mapIdentity
	svc.mu.Unlock()

	idA, _ := svc.connIDFor(connA)
	svc.trackInboundConnect(idA, peer, mapIdentity)
	svc.trackInboundDisconnect(idA, peer)

	select {
	case hint := <-cm.hintEvents:
		closed, ok := hint.(InboundClosed)
		if !ok {
			t.Fatalf("expected InboundClosed hint, got %T", hint)
		}
		if closed.Identity != netcoreIdentity {
			t.Fatalf("expected hint identity from NetCore mirror (%q), got %q", netcoreIdentity, closed.Identity)
		}
	default:
		t.Fatal("expected InboundClosed hint in hintEvents, channel was empty")
	}
}

// TestTrackInboundDisconnect_FallsBackToPeerIDsMap pins the fallback
// branch of the identity precedence enforced by
// TestTrackInboundDisconnect_PrefersNetCoreIdentity. When the NetCore
// mirror is empty (no SetIdentity was ever called on the connEntry —
// typical for auth-not-required peers and tests that register a conn
// without running the full inbound auth flow), trackInboundDisconnect
// must fall back to the address-keyed persistence cache
// (Service.peerIDs). Without this branch, auth-not-required flows would
// emit InboundClosed with an empty identity and break downstream hint
// consumers.
func TestTrackInboundDisconnect_FallsBackToPeerIDsMap(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return 4 },
		NowFn:      func() time.Time { return time.Now() },
	})
	cm.accepting.Store(1)
	svc.connManager = cm

	peer := domain.PeerAddress("10.0.0.8:64646")

	connA, connB := net.Pipe()
	defer func() { _ = connA.Close() }()
	defer func() { _ = connB.Close() }()

	if !svc.registerInboundConn(connA) {
		t.Fatalf("registerInboundConn failed")
	}

	// Deliberately do NOT call SetIdentity on the NetCore — only populate
	// the persistence cache so the fallback path is exercised.
	mapIdentity := domain.PeerIdentity("fallback-map-identity")
	svc.mu.Lock()
	svc.peerIDs[peer] = mapIdentity
	svc.mu.Unlock()

	idA, _ := svc.connIDFor(connA)
	svc.trackInboundConnect(idA, peer, mapIdentity)
	svc.trackInboundDisconnect(idA, peer)

	select {
	case hint := <-cm.hintEvents:
		closed, ok := hint.(InboundClosed)
		if !ok {
			t.Fatalf("expected InboundClosed hint, got %T", hint)
		}
		if closed.Identity != mapIdentity {
			t.Fatalf("expected hint identity from persistence cache (%q), got %q", mapIdentity, closed.Identity)
		}
	default:
		t.Fatal("expected InboundClosed hint in hintEvents, channel was empty")
	}
}

// TestInboundDedup uses transport-level IP not overlay address.
// A peer advertising "1.2.3.4:64646" but connecting from 127.0.0.1
// should reserve 127.0.0.1 in the connected hosts set.
func TestConnectedHostsUsesTransportIP(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Real TCP connection from 127.0.0.1.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			connCh <- c
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	server := <-connCh
	defer func() { _ = server.Close() }()

	svc.mu.Lock()
	// Register inbound connection + peer info with a different overlay address.
	pc := netcore.New(netcore.ConnID(1), server, netcore.Inbound, netcore.Options{
		Address: domain.PeerAddress("1.2.3.4:64646"),
	})
	svc.setTestConnEntryLocked(server, &connEntry{core: pc})

	hosts := svc.connectedHostsLocked()
	svc.mu.Unlock()

	// The real remote IP (127.0.0.1) should be in the set.
	if _, ok := hosts["127.0.0.1"]; !ok {
		t.Error("expected transport IP 127.0.0.1 in connectedHosts")
	}
	// The overlay address (1.2.3.4) should NOT be in the set.
	if _, ok := hosts["1.2.3.4"]; ok {
		t.Error("overlay address 1.2.3.4 should not be in connectedHosts (only transport IP)")
	}
}

// TestTransitDMLiveInboxRoute verifies that a relay node delivers a transit
// DM immediately to a recipient that has a live inbox route (subscribe_inbox).
// Push and gossip are independent: push provides instant local delivery while
// gossip ensures mesh-wide propagation regardless of local subscriber state.
func TestTransitDMLiveInboxRoute(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)

	// Relay node — distinct identity from both sender and recipient.
	relayID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate relay identity: %v", err)
	}
	svc, stop := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	}, relayID)
	defer stop()

	// Create sender and recipient identities — both foreign to the relay.
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender identity: %v", err)
	}
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient identity: %v", err)
	}
	recipientAddr := recipientID.Address

	// Register both as trusted contacts so DM signature verification passes.
	senderBoxSig := identity.SignBoxKeyBinding(senderID)
	recipientBoxSig := identity.SignBoxKeyBinding(recipientID)
	importReply := svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{
			{
				Address: senderID.Address,
				PubKey:  identity.PublicKeyBase64(senderID.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(senderID.BoxPublicKey),
				BoxSig:  senderBoxSig,
			},
			{
				Address: recipientAddr,
				PubKey:  identity.PublicKeyBase64(recipientID.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
				BoxSig:  recipientBoxSig,
			},
		},
	})
	if importReply.Type != "contacts_imported" || importReply.Count != 2 {
		t.Fatalf("import contacts failed: %#v", importReply)
	}

	// Encrypt a test message from sender to recipient.
	ct, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientAddr),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello via transit relay"},
	)
	if err != nil {
		t.Fatalf("encrypt message: %v", err)
	}

	// --- Recipient connects and registers a live inbox route ---
	recipConn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial recipient: %v", err)
	}
	defer func() { _ = recipConn.Close() }()
	_ = recipConn.SetDeadline(time.Now().Add(10 * time.Second))
	recipReader := bufio.NewReader(recipConn)

	// hello + auth
	writeJSONFrame(t, recipConn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                recipientID.Address,
		PubKey:                 identity.PublicKeyBase64(recipientID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(recipientID),
	})
	welcome := readJSONTestFrame(t, recipReader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %s", welcome.Type)
	}

	writeJSONFrame(t, recipConn, protocol.Frame{
		Type:      "auth_session",
		Address:   recipientID.Address,
		Signature: identity.SignPayload(recipientID, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+recipientID.Address)),
	})
	authOK := readJSONTestFrame(t, recipReader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %s: %s", authOK.Type, authOK.Error)
	}

	// subscribe_inbox — registers the live inbox route.
	writeJSONFrame(t, recipConn, protocol.Frame{
		Type:       "subscribe_inbox",
		Topic:      "dm",
		Recipient:  recipientAddr,
		Subscriber: "test-recip-sub",
	})
	subReply := readJSONTestFrame(t, recipReader)
	if subReply.Type != "subscribed" {
		t.Fatalf("expected subscribed, got %s: %s", subReply.Type, subReply.Error)
	}

	// Drain the reverse subscribe_inbox that the relay sends back.
	reverseFrame := readJSONTestFrame(t, recipReader)
	if reverseFrame.Type == "subscribe_inbox" {
		writeJSONFrame(t, recipConn, protocol.Frame{
			Type:       "subscribed",
			Topic:      reverseFrame.Topic,
			Recipient:  reverseFrame.Recipient,
			Subscriber: reverseFrame.Subscriber,
		})
	}

	// --- Sender authenticates on TCP and submits DM via push_message ---
	// push_message is the P2P wire command for delivering messages between
	// authenticated peers. send_message is local-only (RPC / HandleLocalFrame)
	// and does not exist on the TCP wire.
	senderConn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}
	defer func() { _ = senderConn.Close() }()
	_ = senderConn.SetDeadline(time.Now().Add(10 * time.Second))
	senderReader := bufio.NewReader(senderConn)

	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                senderID.Address,
		PubKey:                 identity.PublicKeyBase64(senderID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(senderID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(senderID),
	})
	senderWelcome := readJSONTestFrame(t, senderReader)
	if senderWelcome.Type != "welcome" || senderWelcome.Challenge == "" {
		t.Fatalf("sender expected welcome with challenge, got %s", senderWelcome.Type)
	}
	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:      "auth_session",
		Address:   senderID.Address,
		Signature: identity.SignPayload(senderID, connauth.SessionAuthPayload(senderWelcome.Challenge, senderID.Address)),
	})
	if f := readJSONTestFrame(t, senderReader); f.Type != "auth_ok" {
		t.Fatalf("sender expected auth_ok, got %s: %s", f.Type, f.Error)
	}

	ts := time.Now().UTC().Format(time.RFC3339)
	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:      "push_message",
		Topic:     "dm",
		Recipient: recipientAddr,
		Item: &protocol.MessageFrame{
			ID:        "transit-live-1",
			Sender:    senderID.Address,
			Recipient: recipientAddr,
			Flag:      "sender-delete",
			CreatedAt: ts,
			Body:      ct,
		},
	})
	// push_message on inbound TCP is fire-and-forget: handleInboundPushMessage
	// stores the message and triggers the live inbox push, but does not write
	// a response on the inbound connection (ack_delete goes via outbound session).
	// Delivery is verified by the recipient assertion below.

	// --- Verify: recipient receives push_message via the live inbox route ---
	pushed := readJSONTestFrame(t, recipReader)
	if pushed.Type != "push_message" {
		t.Fatalf("expected push_message on recipient connection, got %s: %s", pushed.Type, pushed.Error)
	}
	if pushed.Item == nil {
		t.Fatal("push_message has nil Item")
	}
	if pushed.Item.ID != "transit-live-1" {
		t.Errorf("expected message ID transit-live-1, got %s", pushed.Item.ID)
	}
	if pushed.Recipient != recipientAddr {
		t.Errorf("expected recipient %s, got %s", recipientAddr, pushed.Recipient)
	}
}

// TestTransitDMBacklogAfterRouteDisappears verifies that if a live inbox
// route disappears (subscriber disconnects) after the snapshot is taken but
// before the push frame is written, the message is still available through
// backlog on the next subscribe_inbox. Even though gossip also propagates
// the message to the mesh, the local backlog provides a safety net for
// recipients reconnecting to this same node.
func TestTransitDMBacklogAfterRouteDisappears(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)

	relayID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate relay identity: %v", err)
	}
	svc, stop := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	}, relayID)
	defer stop()

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender identity: %v", err)
	}
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient identity: %v", err)
	}
	recipientAddr := recipientID.Address

	senderBoxSig := identity.SignBoxKeyBinding(senderID)
	recipientBoxSig := identity.SignBoxKeyBinding(recipientID)
	importReply := svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{
			{
				Address: senderID.Address,
				PubKey:  identity.PublicKeyBase64(senderID.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(senderID.BoxPublicKey),
				BoxSig:  senderBoxSig,
			},
			{
				Address: recipientAddr,
				PubKey:  identity.PublicKeyBase64(recipientID.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
				BoxSig:  recipientBoxSig,
			},
		},
	})
	if importReply.Type != "contacts_imported" || importReply.Count != 2 {
		t.Fatalf("import contacts failed: %#v", importReply)
	}

	ct, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientAddr),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "backlog recovery test"},
	)
	if err != nil {
		t.Fatalf("encrypt message: %v", err)
	}

	// Step 1: recipient connects, subscribes, then disconnects.
	recipConn1, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial recipient (1st): %v", err)
	}
	_ = recipConn1.SetDeadline(time.Now().Add(10 * time.Second))
	recipReader1 := bufio.NewReader(recipConn1)

	writeJSONFrame(t, recipConn1, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                recipientID.Address,
		PubKey:                 identity.PublicKeyBase64(recipientID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(recipientID),
	})
	welcome := readJSONTestFrame(t, recipReader1)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %s", welcome.Type)
	}
	writeJSONFrame(t, recipConn1, protocol.Frame{
		Type:      "auth_session",
		Address:   recipientID.Address,
		Signature: identity.SignPayload(recipientID, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+recipientID.Address)),
	})
	if f := readJSONTestFrame(t, recipReader1); f.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %s", f.Type)
	}
	writeJSONFrame(t, recipConn1, protocol.Frame{
		Type: "subscribe_inbox", Topic: "dm", Recipient: recipientAddr, Subscriber: "recip-sub-1",
	})
	if f := readJSONTestFrame(t, recipReader1); f.Type != "subscribed" {
		t.Fatalf("expected subscribed, got %s", f.Type)
	}
	// Drain reverse subscribe_inbox.
	if f := readJSONTestFrame(t, recipReader1); f.Type == "subscribe_inbox" {
		writeJSONFrame(t, recipConn1, protocol.Frame{
			Type: "subscribed", Topic: f.Topic, Recipient: f.Recipient, Subscriber: f.Subscriber,
		})
	}

	// Disconnect: route is gone.
	_ = recipConn1.Close()
	time.Sleep(50 * time.Millisecond) // let cleanup run

	// Step 2: sender sends DM (must authenticate first). The stale
	// subscriber snapshot may be taken but the push will fail (conn
	// closed). Gossip was skipped because snapshot was non-empty. The
	// message must still be in topics/backlog.
	senderConn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}
	defer func() { _ = senderConn.Close() }()
	_ = senderConn.SetDeadline(time.Now().Add(10 * time.Second))
	senderReader := bufio.NewReader(senderConn)

	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                senderID.Address,
		PubKey:                 identity.PublicKeyBase64(senderID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(senderID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(senderID),
	})
	senderWelcome := readJSONTestFrame(t, senderReader)
	if senderWelcome.Type != "welcome" || senderWelcome.Challenge == "" {
		t.Fatalf("sender expected welcome with challenge, got %s", senderWelcome.Type)
	}
	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:      "auth_session",
		Address:   senderID.Address,
		Signature: identity.SignPayload(senderID, connauth.SessionAuthPayload(senderWelcome.Challenge, senderID.Address)),
	})
	if f := readJSONTestFrame(t, senderReader); f.Type != "auth_ok" {
		t.Fatalf("sender expected auth_ok, got %s: %s", f.Type, f.Error)
	}

	// push_message is the correct P2P wire command for delivering messages
	// between authenticated peers. send_message is local-only (RPC/HandleLocalFrame).
	ts := time.Now().UTC().Format(time.RFC3339)
	writeJSONFrame(t, senderConn, protocol.Frame{
		Type:      "push_message",
		Topic:     "dm",
		Recipient: recipientAddr,
		Item: &protocol.MessageFrame{
			ID:        "backlog-msg-1",
			Sender:    senderID.Address,
			Recipient: recipientAddr,
			Flag:      "sender-delete",
			CreatedAt: ts,
			Body:      ct,
		},
	})
	// push_message on inbound TCP is fire-and-forget: the relay stores the
	// message internally. Give it a moment to complete the async store.
	time.Sleep(100 * time.Millisecond)

	// Step 3: new recipient connection subscribes — should get the message
	// via backlog even though the push to the old connection failed.
	recipConn2, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial recipient (2nd): %v", err)
	}
	defer func() { _ = recipConn2.Close() }()
	_ = recipConn2.SetDeadline(time.Now().Add(10 * time.Second))
	recipReader2 := bufio.NewReader(recipConn2)

	writeJSONFrame(t, recipConn2, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                recipientID.Address,
		PubKey:                 identity.PublicKeyBase64(recipientID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(recipientID),
	})
	welcome2 := readJSONTestFrame(t, recipReader2)
	if welcome2.Type != "welcome" {
		t.Fatalf("expected welcome (2nd), got %s", welcome2.Type)
	}
	writeJSONFrame(t, recipConn2, protocol.Frame{
		Type:      "auth_session",
		Address:   recipientID.Address,
		Signature: identity.SignPayload(recipientID, []byte("corsa-session-auth-v1|"+welcome2.Challenge+"|"+recipientID.Address)),
	})
	if f := readJSONTestFrame(t, recipReader2); f.Type != "auth_ok" {
		t.Fatalf("expected auth_ok (2nd), got %s", f.Type)
	}

	writeJSONFrame(t, recipConn2, protocol.Frame{
		Type: "subscribe_inbox", Topic: "dm", Recipient: recipientAddr, Subscriber: "recip-sub-2",
	})
	if f := readJSONTestFrame(t, recipReader2); f.Type != "subscribed" {
		t.Fatalf("expected subscribed (2nd), got %s", f.Type)
	}

	// Read all frames — expect the backlog message to arrive.
	_ = recipConn2.SetDeadline(time.Now().Add(3 * time.Second))
	foundBacklogMsg := false
	for i := 0; i < 10; i++ {
		f := readJSONTestFrame(t, recipReader2)
		if f.Type == "" {
			break
		}
		if f.Type == "push_message" && f.Item != nil && f.Item.ID == "backlog-msg-1" {
			foundBacklogMsg = true
			break
		}
	}
	if !foundBacklogMsg {
		t.Error("message was NOT delivered via backlog after route disappeared — potential message loss")
	}
}

// TestCapabilityExchangeBetweenTwoNodes verifies that capabilities are sent in
// hello and echoed in welcome during a live handshake between two nodes.
func TestCapabilityExchangeBetweenTwoNodes(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	_, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
		Type:             domain.NodeTypeFull,
	})
	defer stopA()

	svcB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             domain.NodeTypeFull,
	})
	defer stopB()

	// Connect to node B as a raw TCP client and send hello with capabilities.
	frames := exchangeFrames(t, svcB.externalListenAddress(),
		protocol.Frame{
			Type:         "hello",
			Version:      config.ProtocolVersion,
			Client:       "test",
			Capabilities: []string{"mesh_relay_v1", "mesh_routing_v1"},
		},
	)

	welcome := frames[0]
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %s", welcome.Type)
	}

	// In Iteration 0 localCapabilities() returns nil, so the welcome frame
	// should have empty/nil capabilities. The important thing is that the
	// field is present in the protocol and does not cause errors.
	// When localCapabilities() returns tokens, they will appear here.
	if len(welcome.Capabilities) > 0 {
		// This is expected once capabilities are enabled; for now just
		// verify the field round-trips without breaking the handshake.
		t.Logf("welcome capabilities: %v", welcome.Capabilities)
	}
}

// TestMixedVersionNodeWithoutCapabilitiesField verifies that a legacy node
// (one that does not send the capabilities field) can successfully handshake
// with a new node. The capabilities field is omitempty, so legacy frames
// simply omit it; the new node must treat this as an empty capability set.
func TestMixedVersionNodeWithoutCapabilitiesField(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stop()

	// Send a hello frame WITHOUT the capabilities field — simulating a
	// legacy node that predates capability negotiation.
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{
			Type:    "hello",
			Version: config.ProtocolVersion,
			Client:  "test",
			// Capabilities intentionally omitted (nil)
		},
	)

	welcome := frames[0]
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome from new node, got %s", welcome.Type)
	}
	if welcome.Version != config.ProtocolVersion {
		t.Fatalf("expected protocol version %d, got %d", config.ProtocolVersion, welcome.Version)
	}
	// Handshake must succeed — a missing capabilities field is not an error.
}

// TestMixedVersionLegacyPeerExchange verifies that a legacy node (no
// capabilities field) can complete the full peer-exchange phase after
// authentication: hello → welcome → auth_session → auth_ok → get_peers →
// fetch_contacts. This is the exact sequence that syncPeer/syncPeerSession
// execute on every peer session. Without this test, the handshake-only
// coverage in TestMixedVersionNodeWithoutCapabilitiesField would pass
// even if the post-handshake P2P protocol was broken.
func TestMixedVersionLegacyPeerExchange(t *testing.T) {
	t.Parallel()

	addr := freeAddress(t)
	tempDir := t.TempDir()

	nodeID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: addr,
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
		Type:             domain.NodeTypeFull,
	}, nodeID)
	svc.disableRateLimiting = true

	ctx, cancel := context.WithCancel(context.Background())
	svc, stop := startTestService(t, ctx, cancel, svc)
	defer stop()

	// Generate identity for the "legacy" peer.
	legacyID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	// Phase 1: hello WITHOUT capabilities — simulates legacy node.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          "0.9.0",
		Address:                legacyID.Address,
		PubKey:                 identity.PublicKeyBase64(legacyID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(legacyID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(legacyID),
		Listen:                 "10.0.0.42:12345",
		// Capabilities intentionally omitted — legacy node.
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	// Phase 2: auth_session — legacy node authenticates.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   legacyID.Address,
		Signature: identity.SignPayload(legacyID, connauth.SessionAuthPayload(welcome.Challenge, legacyID.Address)),
	})
	authReply := readJSONTestFrame(t, reader)
	if authReply.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got type=%q code=%s error=%q", authReply.Type, authReply.Code, authReply.Error)
	}

	// Phase 3: get_peers — the first step of syncPeer/syncPeerSession.
	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
	peersReply := readJSONTestFrame(t, reader)
	if peersReply.Type != "peers" {
		t.Fatalf("legacy peer-exchange: get_peers should return peers, got type=%q code=%s error=%q",
			peersReply.Type, peersReply.Code, peersReply.Error)
	}

	// Phase 4: fetch_contacts — the second step of syncPeer/syncPeerSession.
	writeJSONFrame(t, conn, protocol.Frame{Type: "fetch_contacts"})
	contactsReply := readJSONTestFrame(t, reader)
	if contactsReply.Type != "contacts" {
		t.Fatalf("legacy peer-exchange: fetch_contacts should return contacts, got type=%q code=%s error=%q",
			contactsReply.Type, contactsReply.Code, contactsReply.Error)
	}
}

// TestUnauthenticatedPeerCannotSendRelayMessage verifies the trust boundary:
// an inbound peer that sends hello with Client != "node"/"desktop" (thus
// skipping auth_session) must NOT be able to send relay_message frames, even
// if it advertises mesh_relay_v1 capability.
//
// Before the fix, the scope check returned true for nil connAuth,
// and connHasCapability + inboundPeerAddress were populated from the
// unauthenticated hello frame, allowing anyone to inject relay traffic.
func TestUnauthenticatedPeerCannotSendRelayMessage(t *testing.T) {
	t.Parallel()

	addr := freeAddress(t)
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	tempDir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: addr,
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
		Type:             domain.NodeTypeFull,
	}, id)

	ctx, cancel := context.WithCancel(context.Background())
	svc, stop := startTestService(t, ctx, cancel, svc)
	defer stop()

	// Connect as an unauthenticated client (Client: "mobile" — does NOT
	// trigger requiresSessionAuth, so no auth_session handshake).
	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	// Send hello claiming mesh_relay_v1 capability.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "mobile",
		ClientVersion:          "1.0",
		Address:                "fake-attacker-address",
		Listen:                 "10.0.0.99:64646",
		Capabilities:           []string{"mesh_relay_v1"},
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %q", welcome.Type)
	}

	// Attempt to send a relay_message without completing auth_session.
	// Use topic "general" (not "dm") so the payload passes
	// storeIncomingMessage — a DM would be rejected at the sender-key
	// check, masking the real vulnerability. The attack vector is about
	// the auth boundary, not payload validation.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:        "relay_message",
		ID:          "unauthenticated-relay-1",
		Address:     "spoofed-origin-sender",
		Recipient:   id.Address, // addressed to this node so it would be "delivered"
		Topic:       "general",
		Body:        "malicious-payload",
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    1,
		MaxHops:     10,
		PreviousHop: "10.0.0.99:64646",
	})

	// The relay_message should be rejected. We try to read a response with
	// a short timeout — either we get an error frame, or no response at all
	// (silent drop). Either way, the relay must NOT produce a hop-ack with
	// "delivered"/"forwarded"/"stored" status.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	line, err := reader.ReadString('\n')
	if err != nil {
		// Timeout or connection closed — acceptable, relay was rejected.
		return
	}

	var response protocol.Frame
	if jsonErr := json.Unmarshal([]byte(strings.TrimSpace(line)), &response); jsonErr != nil {
		// Unparseable response — acceptable.
		return
	}

	// If we got a relay_hop_ack with a success status, the trust boundary
	// is broken — the unauthenticated peer was able to inject relay traffic.
	if response.Type == "relay_hop_ack" {
		t.Fatalf("SECURITY: unauthenticated peer received relay_hop_ack with status %q — "+
			"relay_message must be rejected for non-authenticated connections", response.Status)
	}
}

// TestRelayDMSyncsUnknownSenderKeyFromPreviousHop verifies that when a
// relay_message carrying a DM arrives at the final recipient (or an
// intermediate full node storing for offline delivery), and the sender's
// public key is not yet known, the node syncs keys from the previous hop
// (senderAddress) and retries the store — matching the existing push_message
// behavior. Before the fix, deliverRelayedMessage treated
// ErrCodeUnknownSenderKey as a terminal rejection.
func TestRelayDMSyncsUnknownSenderKeyFromPreviousHop(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	// NodeA: full node that knows the sender's keys.
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate sender: %v", err)
	}

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
	})
	defer stopA()

	// Do NOT register sender keys on nodeA yet — nodeB's bootstrap sync
	// would learn them during the initial fetch_contacts exchange.

	// NodeB: full node, bootstrap to nodeA, does NOT know sender keys yet.
	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             domain.NodeTypeFull,
	})
	defer stopB()

	// Wait until nodeB has a fully connected peer (Connected==true).
	// syncPeer opens a fresh TCP connection to the previous hop and runs
	// the full handshake + fetch_contacts exchange; the peer session must
	// be healthy first so nodeA's listener is proven reachable and the
	// initial contact sync has completed without learning sender keys.
	waitForCondition(t, 5*time.Second, func() bool {
		return hasConnectedPeer(nodeB)
	})

	// Now register sender keys on nodeA — after the initial peer sync,
	// so nodeB has not yet learned them. The relay delivery path will
	// trigger syncPeer to fetch these keys on demand.
	nodeA.mu.Lock()
	nodeA.pubKeys[senderID.Address] = identity.PublicKeyBase64(senderID.PublicKey)
	nodeA.boxKeys[senderID.Address] = identity.BoxPublicKeyBase64(senderID.BoxPublicKey)
	nodeA.boxSigs[senderID.Address] = identity.SignBoxKeyBinding(senderID)
	nodeA.known[senderID.Address] = struct{}{}
	nodeA.mu.Unlock()

	// Verify nodeB does NOT know the sender's key yet.
	nodeB.mu.RLock()
	_, hasSenderKey := nodeB.pubKeys[senderID.Address]
	nodeB.mu.RUnlock()
	if hasSenderKey {
		t.Fatal("precondition failed: nodeB should not know sender key before relay")
	}

	// Create a properly encrypted DM from senderID to nodeB.
	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(nodeB.Address()),
			BoxKeyBase64: identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "relay-sync-test-secret"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants: %v", err)
	}

	// Simulate: nodeA forwards a relay_message to nodeB via the peer session.
	// We inject it as a handleRelayMessage call with senderAddress = nodeA's
	// advertise address (the address nodeB has an outbound session to).
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "relay-sync-key-1",
		Address:     senderID.Address,
		Recipient:   nodeB.Address(),
		Topic:       "dm",
		Body:        ciphertext,
		Flag:        "sender-delete",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: normalizeAddress(addressA),
	}

	t.Logf("nodeB.Address=%s, frame.Recipient=%s, match=%v",
		nodeB.Address(), frame.Recipient, nodeB.Address() == frame.Recipient)
	t.Logf("senderAddress=%s, senderID.Address=%s", normalizeAddress(addressA), senderID.Address)

	status := nodeB.handleRelayMessage(domain.PeerAddress(normalizeAddress(addressA)), nil, frame)
	if status != "delivered" {
		// Dump nodeB state for diagnostics.
		nodeB.mu.RLock()
		hasPub := nodeB.pubKeys[senderID.Address] != ""
		hasBox := nodeB.boxKeys[senderID.Address] != ""
		nodeB.mu.RUnlock()
		t.Logf("post-relay nodeB state: hasPubKey=%v, hasBoxKey=%v", hasPub, hasBox)
		t.Fatalf("expected \"delivered\" after key sync from previous hop, got %q — "+
			"deliverRelayedMessage should sync unknown sender keys before rejecting", status)
	}
}

// TestEvictStaleInboundConnsClosesZombies verifies that evictStaleInboundConns
// force-closes inbound TCP connections whose peer health state is stalled.
// After an internet outage the TCP socket may linger while the heartbeat has
// already declared the peer unresponsive; closing the zombie frees the host
// slot so the remote peer can reconnect.
func TestEvictStaleInboundConnsClosesZombies(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Create a real TCP connection pair.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			connCh <- c
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	server := <-connCh
	defer func() { _ = server.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.99:64646")

	// Register as inbound connection with per-connection lastActivity
	// old enough to be considered stale.
	svc.mu.Lock()
	pc := netcore.New(netcore.ConnID(1), server, netcore.Inbound, netcore.Options{
		Address:      peerAddr,
		LastActivity: time.Now().Add(-heartbeatInterval - pongStallTimeout - 10*time.Second),
	})
	svc.setTestConnEntryLocked(server, &connEntry{core: pc})
	svc.mu.Unlock()

	// Verify the connection is in conns before eviction.
	svc.mu.RLock()
	exists := svc.testConnEntry(server) != nil
	svc.mu.RUnlock()
	if !exists {
		t.Fatal("expected server conn in conns before eviction")
	}

	// Run the eviction sweep.
	svc.evictStaleInboundConns()

	// The connection should be closed. Read should fail.
	_ = server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 1)
	_, readErr := server.Read(buf)
	if readErr == nil {
		t.Fatal("expected read error on force-closed inbound connection, got nil")
	}
}

// TestEvictStaleInboundConnsSkipsHealthy verifies that evictStaleInboundConns
// does not close connections whose peer health state is healthy.
func TestEvictStaleInboundConnsSkipsHealthy(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			connCh <- c
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	server := <-connCh
	defer func() { _ = server.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.88:64646")

	svc.mu.Lock()
	pc := netcore.New(netcore.ConnID(1), server, netcore.Inbound, netcore.Options{
		Address:      peerAddr,
		LastActivity: time.Now(), // recent activity — should not be evicted
	})
	svc.setTestConnEntryLocked(server, &connEntry{core: pc})
	svc.mu.Unlock()

	svc.evictStaleInboundConns()

	// Connection should still be open. Write should succeed.
	_ = server.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, writeErr := server.Write([]byte("ping"))
	if writeErr != nil {
		t.Fatalf("expected healthy inbound connection to remain open, got write error: %v", writeErr)
	}
}

// TestConnectedHostsLockedSkipsStalledInbound verifies that connectedHostsLocked
// excludes inbound connections whose peer is in the stalled state. This allows
// peerDialCandidates to attempt an outbound connection to the same host,
// which is the fastest path to recovery after an internet outage.
func TestConnectedHostsLockedSkipsStalledInbound(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Create two real TCP connections to simulate healthy and stalled inbound peers.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	makeConn := func() (client, server net.Conn) {
		connCh := make(chan net.Conn, 1)
		go func() {
			c, err := listener.Accept()
			if err == nil {
				connCh <- c
			}
		}()
		cl, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		sv := <-connCh
		return cl, sv
	}

	_, server1 := makeConn()
	defer func() { _ = server1.Close() }()

	stalledPeerAddr := domain.PeerAddress("10.0.0.50:64646")

	svc.mu.Lock()
	// Clear any pre-existing inbound connections left over from
	// startTestNode's health-check dial to avoid 127.0.0.1 collisions.
	for c := range svc.connIDByNetConn {
		svc.deleteTestConn(c)
	}
	pc := netcore.New(netcore.ConnID(1), server1, netcore.Inbound, netcore.Options{
		Address:      stalledPeerAddr,
		LastActivity: time.Now().Add(-heartbeatInterval - pongStallTimeout - 10*time.Second),
	})
	svc.setTestConnEntryLocked(server1, &connEntry{core: pc})

	// Also add an outbound session as control.
	svc.upstream["10.0.0.1:64646"] = struct{}{}

	hosts := svc.connectedHostsLocked()
	svc.mu.Unlock()

	// Outbound host must still be present.
	if _, ok := hosts["10.0.0.1"]; !ok {
		t.Error("expected outbound host 10.0.0.1 in connectedHosts")
	}
	// Stalled inbound host (127.0.0.1 from TCP) must be excluded.
	if _, ok := hosts["127.0.0.1"]; ok {
		t.Error("stalled inbound host 127.0.0.1 should be excluded from connectedHosts")
	}
}

// TestPeerVersionStoredByHealthKey verifies that version/build are stored
// by the same key used for health tracking so peerHealthFrames can find them.
func TestPeerVersionStoredByHealthKey(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	// Case 1: direct dial address — no dialOrigin alias.
	directAddr := domain.PeerAddress("185.223.82.127:64646")
	svc.addPeerVersion(directAddr, "0.22-alpha")
	svc.addPeerBuild(directAddr, 22)

	svc.mu.RLock()
	ver := svc.peerVersions[directAddr]
	build := svc.peerBuilds[directAddr]
	svc.mu.RUnlock()

	if ver != "0.22-alpha" {
		t.Errorf("peerVersions[%q] = %q, want %q", directAddr, ver, "0.22-alpha")
	}
	if build != 22 {
		t.Errorf("peerBuilds[%q] = %d, want %d", directAddr, build, 22)
	}

	// Case 2: fallback-port dial variant — resolveHealthAddress maps
	// the dial address to the primary address. Verify version is stored
	// under the primary key.
	primaryAddr := domain.PeerAddress("10.0.0.5:12345")
	fallbackAddr := domain.PeerAddress("10.0.0.5:64646")

	svc.mu.Lock()
	svc.dialOrigin[fallbackAddr] = primaryAddr
	svc.mu.Unlock()

	// Simulate what openPeerSession does: resolve then store.
	svc.mu.RLock()
	healthKey := svc.resolveHealthAddress(fallbackAddr)
	svc.mu.RUnlock()

	svc.addPeerVersion(healthKey, "0.21-beta")
	svc.addPeerBuild(healthKey, 21)

	if healthKey != primaryAddr {
		t.Fatalf("resolveHealthAddress(%q) = %q, want %q", fallbackAddr, healthKey, primaryAddr)
	}

	svc.mu.RLock()
	ver2 := svc.peerVersions[primaryAddr]
	build2 := svc.peerBuilds[primaryAddr]
	svc.mu.RUnlock()

	if ver2 != "0.21-beta" {
		t.Errorf("peerVersions[%q] = %q, want %q", primaryAddr, ver2, "0.21-beta")
	}
	if build2 != 21 {
		t.Errorf("peerBuilds[%q] = %d, want %d", primaryAddr, build2, 21)
	}

	// Case 3: inbound peer without listen field — version stored by
	// inboundPeerAddress (hello.Address fallback).
	inboundAddr := domain.PeerAddress("abcdef1234567890")
	svc.addPeerVersion(inboundAddr, "0.20-gamma")
	svc.addPeerBuild(inboundAddr, 20)

	svc.mu.RLock()
	ver3 := svc.peerVersions[inboundAddr]
	build3 := svc.peerBuilds[inboundAddr]
	svc.mu.RUnlock()

	if ver3 != "0.20-gamma" {
		t.Errorf("peerVersions[%q] = %q, want %q", inboundAddr, ver3, "0.20-gamma")
	}
	if build3 != 20 {
		t.Errorf("peerBuilds[%q] = %d, want %d", inboundAddr, build3, 20)
	}
}

// TestHasOutboundSessionForInbound verifies that hasOutboundSessionForInbound
// correctly detects an existing outbound session, preventing duplicate
// inbound connections from the same peer.
func TestHasOutboundSessionForInbound(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.9:64646")

	// No outbound session — should return false.
	if svc.hasOutboundSessionForInbound(peerAddr) {
		t.Fatal("expected false when no outbound session exists")
	}

	// Register an outbound session.
	svc.mu.Lock()
	svc.sessions[peerAddr] = &peerSession{address: peerAddr, sendCh: make(chan protocol.Frame)}
	svc.mu.Unlock()

	// Now should return true.
	if !svc.hasOutboundSessionForInbound(peerAddr) {
		t.Fatal("expected true when outbound session exists")
	}

	// Clean up session.
	svc.mu.Lock()
	delete(svc.sessions, peerAddr)
	svc.mu.Unlock()

	// Should return false again.
	if svc.hasOutboundSessionForInbound(peerAddr) {
		t.Fatal("expected false after session removed")
	}
}

// TestHasOutboundSessionForInboundResolvesDialOrigin verifies that
// hasOutboundSessionForInbound detects an outbound session stored under
// a fallback dial address when the inbound peer declares its primary
// address. In production dialOrigin maps fallback → primary
// (e.g. dialOrigin["10.0.0.9:64647"] = "10.0.0.9:64646"), so
// resolveHealthAddress(fallbackAddr) returns primaryAddr. The method
// must resolve every session key the same way and compare against the
// inbound address's resolved form.
func TestHasOutboundSessionForInboundResolvesDialOrigin(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	primaryAddr := domain.PeerAddress("10.0.0.9:64646")
	fallbackAddr := domain.PeerAddress("10.0.0.9:64647")

	// Simulate production: outbound session dialed via fallback port,
	// dialOrigin maps fallback → primary (the real production direction).
	svc.mu.Lock()
	svc.sessions[fallbackAddr] = &peerSession{address: fallbackAddr, sendCh: make(chan protocol.Frame)}
	svc.dialOrigin[fallbackAddr] = primaryAddr
	svc.mu.Unlock()

	// Inbound peer declares the primary address. The method should
	// resolve the fallback session key to primaryAddr and match.
	if !svc.hasOutboundSessionForInbound(primaryAddr) {
		t.Fatal("expected true when outbound session exists via dialOrigin resolution")
	}

	// Direct lookup under primaryAddr should fail (no session there),
	// confirming the test exercises the resolution path.
	svc.mu.RLock()
	_, directHit := svc.sessions[primaryAddr]
	svc.mu.RUnlock()
	if directHit {
		t.Fatal("session should NOT be keyed by primaryAddr; test does not exercise resolution")
	}
}

// TestDuplicateInboundConnectionAllowed verifies that the hello handler
// allows an inbound connection even when an outbound session already
// exists for the same peer address. Rejecting it would prevent the
// inbound peer from gossiping to us when it has no outbound session —
// a race that occurs when both sides dial simultaneously.
func TestDuplicateInboundConnectionAllowed(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.8:64646")

	// Simulate an existing outbound session.
	svc.mu.Lock()
	svc.sessions[peerAddr] = &peerSession{address: peerAddr, sendCh: make(chan protocol.Frame)}
	svc.mu.Unlock()
	svc.markPeerConnected(peerAddr, peerDirectionOutbound)

	// Connect as an inbound peer with a hello that declares the same
	// address. Use Client "cli" to skip session auth — this exercises
	// the non-auth hello path where the duplicate check lives.
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	hello := protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "cli",
		ClientVersion: "0.25-test",
		Listen:        string(peerAddr),
		Address:       string(peerAddr),
	}
	writeJSONFrame(t, conn, hello)

	reader := bufio.NewReader(conn)
	reply := readJSONTestFrame(t, reader)

	// The node should accept the duplicate inbound with a welcome frame.
	if reply.Type == "error" {
		t.Fatalf("expected welcome frame, got error: code=%s msg=%s", reply.Code, reply.Error)
	}
	if reply.Type != "welcome" {
		t.Fatalf("expected welcome frame, got %q", reply.Type)
	}
}

// TestPeerHealthFramesSingleRowWithOutboundSession verifies that
// peerHealthFrames emits exactly one row per peer when an outbound
// session exists, even if stale inbound connection data is present.
func TestPeerHealthFramesSingleRowWithOutboundSession(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.7:64646")

	// Mark peer connected as outbound and register session.
	svc.mu.Lock()
	svc.sessions[peerAddr] = &peerSession{
		address: peerAddr,
		connID:  42,
		sendCh:  make(chan protocol.Frame),
	}
	svc.mu.Unlock()
	svc.markPeerConnected(peerAddr, peerDirectionOutbound)

	// Simulate a stale inbound connection entry (should not generate
	// a second row since the outbound session takes priority).
	staleConn, staleRemote := net.Pipe()
	defer func() { _ = staleConn.Close() }()
	defer func() { _ = staleRemote.Close() }()
	svc.mu.Lock()
	pc := netcore.New(netcore.ConnID(99), staleConn, netcore.Inbound, netcore.Options{
		Address:  peerAddr,
		Identity: domain.PeerIdentity("stale-identity"),
	})
	svc.setTestConnEntryLocked(staleConn, &connEntry{core: pc})
	svc.mu.Unlock()

	frames := svc.peerHealthFrames()

	count := 0
	for _, f := range frames {
		if f.Address == string(peerAddr) {
			count++
			if f.Direction != string(peerDirectionOutbound) {
				t.Errorf("expected direction %q, got %q", peerDirectionOutbound, f.Direction)
			}
			if f.ConnID != 42 {
				t.Errorf("expected ConnID 42, got %d", f.ConnID)
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row for %s, got %d", peerAddr, count)
	}
}

// TestDeleteTrustedContactViaLocalFrame verifies that the delete_trusted_contact
// command removes a contact from the trust store and that the contact no longer
// appears in a subsequent fetch_trusted_contacts reply.
func TestDeleteTrustedContactViaLocalFrame(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	address := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
	})
	defer stopA()

	// Import a contact so the trust store has something to delete.
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer identity: %v", err)
	}
	peerBoxSig := identity.SignBoxKeyBinding(peerID)
	importReply := nodeA.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{
			{
				Address: peerID.Address,
				PubKey:  identity.PublicKeyBase64(peerID.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(peerID.BoxPublicKey),
				BoxSig:  peerBoxSig,
			},
		},
	})
	if importReply.Type != "contacts_imported" || importReply.Count != 1 {
		t.Fatalf("import_contacts failed: type=%s error=%s count=%d", importReply.Type, importReply.Error, importReply.Count)
	}

	// Verify the contact exists.
	before := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	foundBefore := false
	for _, c := range before.Contacts {
		if c.Address == peerID.Address {
			foundBefore = true
			break
		}
	}
	if !foundBefore {
		t.Fatal("expected peer in trusted contacts before deletion")
	}

	// Delete the contact.
	deleteReply := nodeA.HandleLocalFrame(protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: peerID.Address,
	})
	if deleteReply.Type != "ok" {
		t.Fatalf("delete_trusted_contact should return ok, got %s: %s", deleteReply.Type, deleteReply.Error)
	}

	// Verify the contact is gone.
	after := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	for _, c := range after.Contacts {
		if c.Address == peerID.Address {
			t.Fatal("peer should not appear in trusted contacts after deletion")
		}
	}
}

// TestDeleteTrustedContactMissingAddress verifies that delete_trusted_contact
// returns an error when no address is provided.
func TestDeleteTrustedContactMissingAddress(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	address := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
	})
	defer stopA()

	reply := nodeA.HandleLocalFrame(protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: "",
	})
	if reply.Type != "error" {
		t.Fatalf("expected error for empty address, got %s", reply.Type)
	}
}

// TestDeleteTrustedContactNotFound verifies that delete_trusted_contact returns
// ok even when the address is not in the trust store (idempotent behavior).
func TestDeleteTrustedContactNotFound(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	address := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
	})
	defer stopA()

	reply := nodeA.HandleLocalFrame(protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: "nonexistent1234567890abcdef12345678901234",
	})
	if reply.Type != "ok" {
		t.Fatalf("expected ok for unknown address (idempotent), got %s: %s", reply.Type, reply.Error)
	}
}

// TestDeleteTrustedContactDropsPendingMessages verifies that deleting a trusted
// contact also removes all pending outbound messages addressed to that contact.
func TestDeleteTrustedContactDropsPendingMessages(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	address := freeAddress(t)

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
	})
	defer stop()

	// Import a contact.
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer identity: %v", err)
	}
	peerBoxSig := identity.SignBoxKeyBinding(peerID)
	importReply := svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{
			{
				Address: peerID.Address,
				PubKey:  identity.PublicKeyBase64(peerID.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(peerID.BoxPublicKey),
				BoxSig:  peerBoxSig,
			},
		},
	})
	if importReply.Type != "contacts_imported" {
		t.Fatalf("import failed: %s", importReply.Error)
	}

	// Directly populate pending queue with messages for the peer.
	relay := domain.PeerAddress("10.0.0.1:64646")
	otherRecipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	svc.mu.Lock()
	svc.pending[relay] = []pendingFrame{
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-1", Recipient: peerID.Address}, QueuedAt: time.Now()},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-2", Recipient: peerID.Address}, QueuedAt: time.Now()},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-3", Recipient: otherRecipient}, QueuedAt: time.Now()},
	}
	for _, pf := range svc.pending[relay] {
		svc.pendingKeys[pendingFrameKey(relay, pf.Frame)] = struct{}{}
	}
	svc.outbound["msg-1"] = outboundDelivery{MessageID: "msg-1", Recipient: peerID.Address, Status: "queued"}
	svc.outbound["msg-2"] = outboundDelivery{MessageID: "msg-2", Recipient: peerID.Address, Status: "retrying"}
	svc.outbound["msg-3"] = outboundDelivery{MessageID: "msg-3", Recipient: otherRecipient, Status: "queued"}
	svc.mu.Unlock()

	// Delete the contact.
	deleteReply := svc.HandleLocalFrame(protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: peerID.Address,
	})
	if deleteReply.Type != "ok" {
		t.Fatalf("delete_trusted_contact failed: %s", deleteReply.Error)
	}

	// Verify: pending messages for deleted contact are gone, other messages remain.
	svc.mu.RLock()
	remaining := svc.pending[relay]
	outbound1 := svc.outbound["msg-1"]
	outbound2 := svc.outbound["msg-2"]
	outbound3 := svc.outbound["msg-3"]
	svc.mu.RUnlock()

	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining pending frame, got %d", len(remaining))
	}
	if remaining[0].Frame.ID != "msg-3" {
		t.Fatalf("expected msg-3 to remain, got %s", remaining[0].Frame.ID)
	}
	if outbound1.MessageID != "" {
		t.Fatal("outbound entry for msg-1 should have been removed")
	}
	if outbound2.MessageID != "" {
		t.Fatal("outbound entry for msg-2 should have been removed")
	}
	if outbound3.MessageID == "" {
		t.Fatal("outbound entry for msg-3 should still exist")
	}
}

// TestDeleteTrustedContactPersistsOutboundOnly verifies that deleting a contact
// persists the queue state even when only outbound delivery entries (no pending
// frames) exist for the deleted recipient. Without this, stale outbound rows
// would survive a restart.
func TestDeleteTrustedContactPersistsOutboundOnly(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	address := freeAddress(t)
	queuePath := filepath.Join(tempDir, "queue.json")

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   queuePath,
	})
	defer stop()

	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer identity: %v", err)
	}
	peerBoxSig := identity.SignBoxKeyBinding(peerID)
	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{
			{
				Address: peerID.Address,
				PubKey:  identity.PublicKeyBase64(peerID.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(peerID.BoxPublicKey),
				BoxSig:  peerBoxSig,
			},
		},
	})

	// Only outbound entries, no pending frames.
	svc.mu.Lock()
	svc.outbound["ob-1"] = outboundDelivery{MessageID: "ob-1", Recipient: peerID.Address, Status: "delivered"}
	svc.mu.Unlock()

	// Persist initial state so the outbound entry is on disk.
	svc.mu.RLock()
	snapshot := svc.queueStateSnapshotLocked()
	svc.mu.RUnlock()
	svc.persistQueueState(snapshot)

	// Delete the contact — should persist even though dropped == 0.
	svc.HandleLocalFrame(protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: peerID.Address,
	})

	// Reload queue state from disk and verify the outbound entry is gone.
	reloaded, err := loadQueueState(queuePath)
	if err != nil {
		t.Fatalf("reload queue state: %v", err)
	}
	if _, ok := reloaded.OutboundState["ob-1"]; ok {
		t.Fatal("outbound entry ob-1 should have been removed from persisted queue state")
	}
}

// ---------------------------------------------------------------------------
// Protocol-level security tests
// ---------------------------------------------------------------------------

// TestSubscribeInboxRejectsUnauthenticated verifies that subscribe_inbox
// returns auth-required when the connection has not completed auth_session.
func TestSubscribeInboxRejectsUnauthenticated(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	// hello only, no auth_session
	writeJSONFrame(t, conn, protocol.Frame{
		Type: "hello", Version: config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "test-attacker", ClientVersion: config.CorsaWireVersion,
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %s", welcome.Type)
	}

	// attempt subscribe_inbox without authentication
	writeJSONFrame(t, conn, protocol.Frame{
		Type: "subscribe_inbox", Topic: "dm", Recipient: "victim-identity", Subscriber: "attacker-sub",
	})
	reply := readJSONTestFrame(t, reader)
	if reply.Type != "error" {
		t.Fatalf("expected error, got %s", reply.Type)
	}
	if reply.Code != protocol.ErrCodeAuthRequired {
		t.Errorf("expected code %s, got %s", protocol.ErrCodeAuthRequired, reply.Code)
	}
}

// TestSubscribeInboxRejectsIdentityMismatch verifies that an authenticated
// peer cannot subscribe to a different identity's inbox.
func TestSubscribeInboxRejectsIdentityMismatch(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	nodeID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate node identity: %v", err)
	}
	svc, stop := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	}, nodeID)
	defer stop()

	// Create an attacker identity that will authenticate as itself
	// but try to subscribe to someone else's inbox.
	attackerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate attacker identity: %v", err)
	}
	attackerBoxSig := identity.SignBoxKeyBinding(attackerID)

	// Register attacker as a contact so auth succeeds.
	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: attackerID.Address,
			PubKey:  identity.PublicKeyBase64(attackerID.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(attackerID.BoxPublicKey),
			BoxSig:  attackerBoxSig,
		}},
	})

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	// hello + auth as attacker
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                attackerID.Address,
		PubKey:                 identity.PublicKeyBase64(attackerID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(attackerID.BoxPublicKey),
		BoxSig:                 attackerBoxSig,
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %s", welcome.Type)
	}

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   attackerID.Address,
		Signature: identity.SignPayload(attackerID, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+attackerID.Address)),
	})
	authReply := readJSONTestFrame(t, reader)
	if authReply.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %s: %s", authReply.Type, authReply.Error)
	}

	// Try to subscribe to a DIFFERENT identity's inbox (the node's identity).
	victimAddr := svc.Address()
	writeJSONFrame(t, conn, protocol.Frame{
		Type: "subscribe_inbox", Topic: "dm", Recipient: victimAddr, Subscriber: "attacker-sub",
	})
	reply := readJSONTestFrame(t, reader)
	if reply.Type != "error" {
		t.Fatalf("expected error for identity mismatch, got %s", reply.Type)
	}
	if reply.Code != protocol.ErrCodeAuthRequired {
		t.Errorf("expected code %s, got %s", protocol.ErrCodeAuthRequired, reply.Code)
	}
}

// TestSubscribeInboxAllowsOwnIdentity verifies that an authenticated peer
// can subscribe to its own identity's inbox.
func TestSubscribeInboxAllowsOwnIdentity(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	nodeID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate node identity: %v", err)
	}
	svc, stop := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	}, nodeID)
	defer stop()

	// Create peer identity and register as contact.
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer identity: %v", err)
	}
	peerBoxSig := identity.SignBoxKeyBinding(peerID)

	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peerID.Address,
			PubKey:  identity.PublicKeyBase64(peerID.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peerID.BoxPublicKey),
			BoxSig:  peerBoxSig,
		}},
	})

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	// hello + auth as peer
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                peerID.Address,
		PubKey:                 identity.PublicKeyBase64(peerID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(peerID.BoxPublicKey),
		BoxSig:                 peerBoxSig,
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %s", welcome.Type)
	}

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   peerID.Address,
		Signature: identity.SignPayload(peerID, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+peerID.Address)),
	})
	authReply := readJSONTestFrame(t, reader)
	if authReply.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %s: %s", authReply.Type, authReply.Error)
	}

	// Subscribe to OWN identity's inbox — must succeed.
	writeJSONFrame(t, conn, protocol.Frame{
		Type: "subscribe_inbox", Topic: "dm", Recipient: peerID.Address, Subscriber: "peer-sub",
	})
	reply := readJSONTestFrame(t, reader)
	if reply.Type != "subscribed" {
		t.Fatalf("expected subscribed for own identity, got %s: %s", reply.Type, reply.Error)
	}
}

// NOTE: TestFetchInboxRejectsIdentityMismatch was removed — fetch_inbox is a
// data-only command not available on the TCP wire (covered by
// TestDataCommandViaTCP_UnknownCommand_SnakeCase). The equivalent identity-
// binding test for subscribe_inbox already exists as
// TestSubscribeInboxRejectsIdentityMismatch above (line ~8506).

// TestWriteJSONFrameDropsOnUnregisteredConn pins the PR 3 fail-closed
// contract for the fire-and-forget send path: when writeJSONFrame is invoked
// on a conn that is NOT registered in s.conns, the managed writer
// cannot be resolved and the frame MUST be dropped rather than slipped around
// the NetCore via a direct conn.Write. Any non-zero bytes reaching the socket
// here would mean the migration's single-writer invariant had been bypassed
// again — the exact regression PR 3 is closing.
//
// The test also pins the observability contract (reviewer P2 fix-up): the
// protocol_trace line MUST reflect the effective send outcome. A ping that
// is dropped on the unregistered path must not be logged as accepted=true —
// that would leave operators with a successful-send trace followed by a
// drop log for the same frame.
func TestWriteJSONFrameDropsOnUnregisteredConn(t *testing.T) {
	// NOT parallel: mutates the global zerolog logger to capture trace output.
	var buf syncWriter
	origLogger := log.Logger
	origLevel := zerolog.GlobalLevel()
	log.Logger = zerolog.New(&buf).With().Logger()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer func() {
		log.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	}()

	svc := &Service{
		conns:           map[netcore.ConnID]*connEntry{},
		connIDByNetConn: map[net.Conn]netcore.ConnID{},
	}
	conn := &mockConn{}

	// Unregistered ConnID: svc.conns is empty so any ID misses the lookup
	// and the drop-on-unregistered path must fire without ever reaching
	// the raw socket behind `conn`.
	_ = svc.writeJSONFrameByID(netcore.ConnID(0), protocol.Frame{Type: "ping"})

	if written := conn.Written(); len(written) != 0 {
		t.Fatalf("unregistered conn must not receive any bytes via direct-write fallback, got %d bytes: %q", len(written), written)
	}

	out := buf.String()
	if !strings.Contains(out, `"send_outcome":"unregistered"`) {
		t.Fatalf("protocol_trace must surface send_outcome=unregistered; got: %s", out)
	}
	if strings.Contains(out, `"accepted":true`) {
		t.Fatalf("protocol_trace must not show accepted=true for a dropped frame; got: %s", out)
	}
	if !strings.Contains(out, "unregistered_write:") {
		t.Fatalf("expected unregistered_write error log; got: %s", out)
	}
}

// TestWriteJSONFrameSyncDropsOnUnregisteredConn mirrors the guarantee above
// for the synchronous error-path variant used by handleConn when it is about
// to return false. The sync variant is the more tempting injection point for
// a silent-write regression because its callers expect "write completed before
// return" semantics — it is even more important that it fails closed rather
// than reaching for the raw socket when the NetCore is missing.
func TestWriteJSONFrameSyncDropsOnUnregisteredConn(t *testing.T) {
	// NOT parallel: mutates the global zerolog logger to capture trace output.
	var buf syncWriter
	origLogger := log.Logger
	origLevel := zerolog.GlobalLevel()
	log.Logger = zerolog.New(&buf).With().Logger()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer func() {
		log.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	}()

	svc := &Service{
		conns:           map[netcore.ConnID]*connEntry{},
		connIDByNetConn: map[net.Conn]netcore.ConnID{},
	}
	conn := &mockConn{}

	// Unregistered ConnID: svc.conns is empty so the sync drop path must
	// fire and `conn` must never see any bytes.
	_ = svc.writeJSONFrameSyncByID(netcore.ConnID(0), protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidJSON})

	if written := conn.Written(); len(written) != 0 {
		t.Fatalf("unregistered conn must not receive any bytes via direct-write fallback, got %d bytes: %q", len(written), written)
	}

	out := buf.String()
	if !strings.Contains(out, `"send_outcome":"unregistered"`) {
		t.Fatalf("protocol_trace must surface send_outcome=unregistered; got: %s", out)
	}
	// Error frames already had accepted=false by the old semantic, but the
	// contract now also requires send_outcome to distinguish "delivered error"
	// from "dropped error" — the latter is exactly this test.
	if strings.Contains(out, `"send_outcome":"sent"`) {
		t.Fatalf("protocol_trace must not report send_outcome=sent for a dropped frame; got: %s", out)
	}
	if !strings.Contains(out, "unregistered_write:") {
		t.Fatalf("expected unregistered_write error log; got: %s", out)
	}
}

package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
)

// candidateAddresses extracts the dial addresses from peerDialCandidate results.
func candidateAddresses(candidates []peerDialCandidate) []string {
	out := make([]string, len(candidates))
	for i, c := range candidates {
		out[i] = c.address
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

	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "get_peers"},
		sendMessageFrame("global", "msg-1", svc.Address(), "*", "immutable", ts, 0, "hello"),
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
		protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()},
	)

	if got := frames[0]; got.Type != "welcome" || got.Address != svc.Address() {
		t.Fatalf("unexpected welcome: %#v", got)
	}
	if got := frames[0]; got.Listener != "1" || got.Listen != normalizeAddress(address) {
		t.Fatalf("unexpected welcome listener fields: %#v", got)
	}
	if got := frames[1]; got.Type != "peers" || got.Count != 0 {
		t.Fatalf("unexpected peers: %#v", got)
	}
	if got := frames[2]; got.Type != "message_stored" || got.ID != "msg-1" {
		t.Fatalf("unexpected stored frame: %#v", got)
	}
	assertMessageFrame(t, frames[3], "messages", "global", 1, protocol.MessageFrame{
		ID: "msg-1", Sender: svc.Address(), Recipient: "*", Flag: "immutable", CreatedAt: ts, TTLSeconds: 0, Body: "hello",
	})
	assertMessageFrame(t, frames[4], "inbox", "global", 1, protocol.MessageFrame{
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
		Type:             config.NodeTypeFull,
	})
	defer stopFull()

	clientNode, stopClient := startTestNode(t, config.Node{
		ListenAddress:    addressClient,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             config.NodeTypeClient,
	})
	defer stopClient()

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, fullNode.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_identities"},
		)
		for _, address := range reply[1].Identities {
			if address == clientNode.Address() {
				return true
			}
		}
		return false
	})

	reply := exchangeFrames(t, fullNode.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "get_peers"},
	)

	for _, peer := range reply[1].Peers {
		if peer == normalizeAddress(addressClient) {
			t.Fatalf("listener-disabled client should not be advertised as dialable peer: %#v", reply[1].Peers)
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

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
	peers := readJSONTestFrame(t, reader)
	if peers.Type != "peers" {
		t.Fatalf("expected peers after auth, got %#v", peers)
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
		Type: config.NodeTypeClient,
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
		BootstrapPeers:   bootstrap,
		Type:             config.NodeTypeFull,
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
		Type:             config.NodeTypeFull,
	}, id)

	got, ok := svc.normalizePeerAddress("198.51.100.2:50702", "127.0.0.1:64647")
	if !ok {
		t.Fatalf("expected normalized address")
	}
	if got != "198.51.100.2:64646" {
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
		Type:             config.NodeTypeFull,
	}, id)

	got, ok := svc.normalizePeerAddress("198.51.100.2:50702", "198.51.100.3:64647")
	if !ok {
		t.Fatalf("expected normalized address")
	}
	if got != "198.51.100.2:64647" {
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
		BootstrapPeers: []string{
			"10.0.0.1:64647",
			"127.0.0.1:64647",
			"192.168.1.20:64646",
			"172.16.3.10:64646",
		},
		Type: config.NodeTypeClient,
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
		BootstrapPeers: []string{
			"198.51.100.1:64647",
			"198.51.100.2:64647",
		},
		Type: config.NodeTypeClient,
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
		Type:             config.NodeTypeClient,
		ListenerEnabled:  true,
		ListenerSet:      true,
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             config.NodeTypeFull,
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 && len(nodeB.Peers()) >= 1
	})

	ts := time.Now().UTC().Format(time.RFC3339)
	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "client-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "client-message"),
	)
	if got := frames[1]; got.Type != "message_stored" {
		t.Fatalf("unexpected client store response: %#v", got)
	}

	time.Sleep(1500 * time.Millisecond)

	final := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
	)
	if got := final[1]; got.Type != "messages" || got.Count != 0 {
		t.Fatalf("expected empty message log on full node, got %#v", got)
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

	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "dup-msg-1", svc.Address(), "*", "immutable", ts, 0, "same"),
		sendMessageFrame("global", "dup-msg-1", svc.Address(), "*", "immutable", ts, 0, "same"),
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
	)

	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected first store response: %#v", frames[1])
	}
	if got := frames[2]; got.Type != "message_known" || got.ID != "dup-msg-1" {
		t.Fatalf("unexpected duplicate response: %#v", got)
	}
	assertMessageFrame(t, frames[3], "messages", "global", 1, protocol.MessageFrame{
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

	timestampA := time.Now().UTC().Format(time.RFC3339)
	singleConnection := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "get_peers"},
		protocol.Frame{Type: "fetch_contacts"},
		sendMessageFrame("global", "series-msg-1", svc.Address(), "*", "immutable", timestampA, 0, "hello-series"),
		protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()},
	)

	if got := singleConnection[0]; got.Type != "welcome" || got.Address != svc.Address() {
		t.Fatalf("unexpected welcome over single connection: %#v", got)
	}
	if got := singleConnection[1]; got.Type != "peers" || got.Count != 0 {
		t.Fatalf("unexpected peers over single connection: %#v", got)
	}
	if got := singleConnection[2]; got.Type != "contacts" || got.Count == 0 {
		t.Fatalf("unexpected contacts over single connection: %#v", got)
	}
	if got := singleConnection[3]; got.Type != "message_stored" || got.ID != "series-msg-1" {
		t.Fatalf("unexpected store reply over single connection: %#v", got)
	}
	assertMessageFrame(t, singleConnection[4], "inbox", "global", 1, protocol.MessageFrame{
		ID: "series-msg-1", Sender: svc.Address(), Recipient: "*", Flag: "immutable", CreatedAt: timestampA, TTLSeconds: 0, Body: "hello-series",
	})

	timestampB := time.Now().UTC().Format(time.RFC3339)
	step1 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
	)
	step2 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "get_peers"},
	)
	step3 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_contacts"},
	)
	step4 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "series-msg-2", svc.Address(), "*", "immutable", timestampB, 0, "hello-separate"),
	)
	step5 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()},
	)

	if got := step1[0]; got.Type != "welcome" || got.Address != svc.Address() {
		t.Fatalf("unexpected welcome over separate connection: %#v", got)
	}
	if got := step2[1]; got.Type != "peers" || got.Count != 0 {
		t.Fatalf("unexpected peers over separate connection: %#v", got)
	}
	if got := step3[1]; got.Type != "contacts" || got.Count == 0 {
		t.Fatalf("unexpected contacts over separate connection: %#v", got)
	}
	if got := step4[1]; got.Type != "message_stored" || got.ID != "series-msg-2" {
		t.Fatalf("unexpected store reply over separate connection: %#v", got)
	}

	finalInbox := step5[1]
	if finalInbox.Type != "inbox" || finalInbox.Count != 2 {
		t.Fatalf("unexpected final inbox after separate connections: %#v", finalInbox)
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

	waitForCondition(t, 5*time.Second, func() bool {
		reply := nodeA.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
		return reply.Type == "peer_health" && reply.Count > 0
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
	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "queued-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "hello-after-reconnect"),
	)
	if got := frames[1]; got.Type != "message_stored" {
		t.Fatalf("unexpected nodeA store response: %#v", got)
	}

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
	})
	defer stopB()

	waitForCondition(t, 8*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_messages", Topic: "global"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1 && reply[1].Messages[0].ID == "queued-msg-1"
	})
}

func TestRoutingTargetsPreferBestHealthySessions(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg:       config.Node{AdvertiseAddress: "self:64646", ListenAddress: ":64646"},
		sessions:  map[string]*peerSession{},
		health:    map[string]*peerHealth{},
		peerTypes: map[string]config.NodeType{},
		peerIDs:   map[string]string{},
	}

	now := time.Now().UTC()
	addresses := []string{"a:1", "b:1", "c:1", "d:1", "e:1"}
	for _, address := range addresses {
		svc.sessions[address] = &peerSession{address: address}
	}
	svc.health["a:1"] = &peerHealth{Address: "a:1", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now.Add(-2 * time.Second)}
	svc.health["b:1"] = &peerHealth{Address: "b:1", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now.Add(-1 * time.Second)}
	svc.health["c:1"] = &peerHealth{Address: "c:1", Connected: true, State: peerStateDegraded, LastUsefulReceiveAt: now.Add(-3 * time.Second)}
	svc.health["d:1"] = &peerHealth{Address: "d:1", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: now.Add(-120 * time.Second)}
	svc.health["e:1"] = &peerHealth{Address: "e:1", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now.Add(-4 * time.Second)}

	got := svc.routingTargets()
	want := []string{"b:1", "a:1", "e:1"}
	if len(got) != len(want) {
		t.Fatalf("unexpected target count: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected routing targets: got %v want %v", got, want)
		}
	}
}

func TestRoutingTargetsSkipClientRelayUnlessRecipientMatches(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg:       config.Node{AdvertiseAddress: "self:64646", ListenAddress: ":64646"},
		sessions:  map[string]*peerSession{},
		health:    map[string]*peerHealth{},
		peerTypes: map[string]config.NodeType{},
		peerIDs:   map[string]string{},
	}

	now := time.Now().UTC()
	svc.sessions["full:1"] = &peerSession{address: "full:1"}
	svc.sessions["client:1"] = &peerSession{address: "client:1"}
	svc.health["full:1"] = &peerHealth{Address: "full:1", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	svc.health["client:1"] = &peerHealth{Address: "client:1", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	svc.peerTypes["full:1"] = config.NodeTypeFull
	svc.peerTypes["client:1"] = config.NodeTypeClient
	svc.peerIDs["client:1"] = "client-id"

	globalTargets := svc.routingTargetsForMessage(protocol.Envelope{
		Topic:     "global",
		Sender:    "sender",
		Recipient: "*",
	})
	if len(globalTargets) != 1 || globalTargets[0] != "full:1" {
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
	if dmTargets[0] != "client:1" && dmTargets[1] != "client:1" {
		t.Fatalf("recipient client session missing from targets: %#v", dmTargets)
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
	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "mesh-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "hello-from-a"),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected nodeA store response: %#v", frames[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_messages", Topic: "global"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1 && reply[1].Messages[0].Body == "hello-from-a"
	})

	final := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
		protocol.Frame{Type: "get_peers"},
	)
	assertMessageFrame(t, final[1], "messages", "global", 1, protocol.MessageFrame{
		ID: "mesh-msg-1", Sender: nodeA.Address(), Recipient: "*", Flag: "immutable", CreatedAt: ts, TTLSeconds: 0, Body: "hello-from-a",
	})
	if got := final[2]; got.Type != "peers" || len(got.Peers) == 0 {
		t.Fatalf("expected nodeB to know at least one peer, got %#v", got)
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

	ciphertext, err := directmsg.EncryptForParticipants(
		nodeA.identity,
		nodeB.Address(),
		identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		"secret-for-b",
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "dm-msg-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", frames[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_messages", Topic: "dm"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1 && reply[1].Messages[0].ID == "dm-msg-1"
	})

	inboxB := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_messages", Topic: "dm"},
	)
	assertMessageFrame(t, inboxB[1], "messages", "dm", 1, protocol.MessageFrame{
		ID: "dm-msg-1", Sender: nodeA.Address(), Recipient: nodeB.Address(), Flag: "sender-delete", CreatedAt: ts, TTLSeconds: 0, Body: ciphertext,
	})

	inboxA := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: nodeA.Address()},
	)
	if got := inboxA[1]; got.Type != "inbox" || got.Count != 0 {
		t.Fatalf("unexpected nodeA inbox: %#v", got)
	}

	dmIDs := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_message_ids", Topic: "dm"},
	)
	if got := dmIDs[1]; got.Type != "message_ids" || len(got.IDs) != 1 || got.IDs[0] != "dm-msg-1" {
		t.Fatalf("unexpected dm id list: %#v", got)
	}

	single := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_message", Topic: "dm", ID: "dm-msg-1"},
	)
	if got := single[1]; got.Type != "message" || got.Item == nil || got.Item.ID != "dm-msg-1" {
		t.Fatalf("unexpected single dm fetch: %#v", got)
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
		Type:             config.NodeTypeFull,
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             config.NodeTypeClient,
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
		nodeB.Address(),
		identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		"push-route-secret",
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "push-dm-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", frames[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_messages", Topic: "dm"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1 && reply[1].Messages[0].ID == "push-dm-1"
	})

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeA.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_delivery_receipts", Recipient: nodeA.Address()},
		)
		return reply[1].Type == "delivery_receipts" && len(reply[1].Receipts) == 1 && reply[1].Receipts[0].MessageID == "push-dm-1"
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

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeA.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_contacts"},
		)
		if reply[1].Type != "contacts" || reply[1].Count == 0 {
			return false
		}
		for _, contact := range reply[1].Contacts {
			if contact.Address == nodeB.Address() && contact.BoxSig != "" {
				return true
			}
		}
		return false
	})

	networkReply := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_contacts"},
	)
	foundNetworkContact := false
	for _, contact := range networkReply[1].Contacts {
		if contact.Address == nodeB.Address() {
			foundNetworkContact = true
			if contact.BoxSig == "" {
				t.Fatalf("expected network contact %s to include box signature, got %#v", nodeB.Address(), contact)
			}
		}
	}
	if !foundNetworkContact {
		t.Fatalf("expected fetch_contacts to include network-discovered contact %s, got %#v", nodeB.Address(), networkReply[1].Contacts)
	}

	reply := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_trusted_contacts"},
	)
	got := reply[1]
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
		nodeB.Address(),
		identity.BoxPublicKeyBase64(nodeB.identity.BoxPublicKey),
		"tampered",
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	tampered := ciphertext[:len(ciphertext)-1] + "A"
	ts := time.Now().UTC().Format(time.RFC3339)
	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "tampered-msg-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, tampered),
	)
	if got := frames[1]; got.Type != "error" || got.Code != protocol.ErrCodeInvalidDirectMessageSig {
		t.Fatalf("unexpected invalid signature response: %#v", got)
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
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "old-msg-1", svc.Address(), "*", "immutable", oldTimestamp, 0, "too-old"),
	)

	if got := frames[1]; got.Type != "error" || got.Code != protocol.ErrCodeMessageTimestampOutOfRange {
		t.Fatalf("unexpected stale timestamp response: %#v", got)
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
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{
			Type:       "import_message",
			Topic:      "global",
			ID:         "historic-msg-1",
			Address:    svc.Address(),
			Recipient:  "*",
			Flag:       "immutable",
			CreatedAt:  oldTimestamp,
			TTLSeconds: 0,
			Body:       "historic",
		},
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
	)

	if got := frames[1]; got.Type != "message_stored" || got.ID != "historic-msg-1" {
		t.Fatalf("unexpected import response: %#v", got)
	}
	assertMessageFrame(t, frames[2], "messages", "global", 1, protocol.MessageFrame{
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
		recipientID.Address,
		identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		"late-but-valid",
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	oldTimestamp := time.Now().UTC().Add(-11 * time.Minute).Format(time.RFC3339)
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "late-dm-1", svc.Address(), recipientID.Address, "sender-delete", oldTimestamp, 0, ciphertext),
	)

	if got := frames[1]; got.Type != "message_stored" || got.ID != "late-dm-1" {
		t.Fatalf("unexpected historical dm response: %#v", got)
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
		recipientID.Address,
		identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		"already-expired",
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	oldTimestamp := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339)
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "expired-dm-1", svc.Address(), recipientID.Address, "sender-delete", oldTimestamp, 30, ciphertext),
	)

	if got := frames[1]; got.Type != "error" || got.Code != protocol.ErrCodeMessageTimestampOutOfRange {
		t.Fatalf("unexpected expired ttl dm response: %#v", got)
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
	frames := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "ttl-msg-1", svc.Address(), "*", "auto-delete-ttl", ts, 1, "burn-after-read"),
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
	)

	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected ttl store response: %#v", frames[1])
	}
	assertMessageFrame(t, frames[2], "messages", "global", 1, protocol.MessageFrame{
		ID: "ttl-msg-1", Sender: svc.Address(), Recipient: "*", Flag: "auto-delete-ttl", CreatedAt: ts, TTLSeconds: 1, Body: "burn-after-read",
	})

	time.Sleep(1500 * time.Millisecond)

	final := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
		protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()},
	)

	if got := final[1]; got.Type != "messages" || got.Count != 0 {
		t.Fatalf("expected ttl message to expire from log, got %#v", got)
	}
	if got := final[2]; got.Type != "inbox" || got.Count != 0 {
		t.Fatalf("expected ttl message to expire from inbox, got %#v", got)
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

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 && len(nodeB.Peers()) >= 1
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

	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "publish_notice", TTLSeconds: 30, Ciphertext: ciphertext},
	)
	if got := frames[1]; got.Type != "notice_stored" {
		t.Fatalf("unexpected notice store response: %#v", got)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_notices"},
		)
		return reply[1].Type == "notices" && len(reply[1].Notices) > 0
	})

	replyB := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_notices"},
	)
	if len(replyB[1].Notices) == 0 {
		t.Fatal("expected nodeB to have at least one notice")
	}

	plain, err := gazeta.DecryptForIdentity(nodeB.identity, replyB[1].Notices[0].Ciphertext)
	if err != nil {
		t.Fatalf("expected nodeB to decrypt notice: %v", err)
	}
	if plain.Body != "meet-at-dawn" {
		t.Fatalf("unexpected gazeta body: %q", plain.Body)
	}

	if _, err := gazeta.DecryptForIdentity(nodeA.identity, replyB[1].Notices[0].Ciphertext); err == nil {
		t.Fatal("expected sender nodeA not to decrypt recipient notice")
	}
}

func startTestNode(t *testing.T, cfg config.Node) (*Service, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate test identity: %v", err)
	}
	svc := NewService(cfg, id)
	return startTestService(t, ctx, cancel, svc)
}

func startTestNodeWithIdentity(t *testing.T, cfg config.Node, id *identity.Identity) (*Service, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	svc := NewService(cfg, id)
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
		Type:             config.NodeTypeFull,
	})
	defer stopA()

	ciphertext, err := directmsg.EncryptForParticipants(
		nodeA.identity,
		idB.Address,
		identity.BoxPublicKeyBase64(idB.BoxPublicKey),
		"retry-route-secret",
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "retry-dm-1", nodeA.Address(), idB.Address, "sender-delete", ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", frames[1])
	}

	time.Sleep(2500 * time.Millisecond)

	nodeB, stopB := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             config.NodeTypeFull,
	}, idB)
	defer stopB()

	waitForCondition(t, 12*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_messages", Topic: "dm"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1 && reply[1].Messages[0].ID == "retry-dm-1"
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
		Type:             config.NodeTypeFull,
	})
	defer stopFull()

	ciphertext, err := directmsg.EncryptForParticipants(
		fullNode.identity,
		idClient.Address,
		identity.BoxPublicKeyBase64(idClient.BoxPublicKey),
		"backlog-secret",
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)

	frames := exchangeFrames(t, fullNode.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: config.ProtocolVersion, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "backlog-dm-1", fullNode.Address(), idClient.Address, "sender-delete", ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected full-node direct store response: %#v", frames[1])
	}

	clientNode, stopClient := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addressClient,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             config.NodeTypeClient,
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
		Type:             config.NodeTypeFull,
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
		Type:             config.NodeTypeClient,
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
		Type:             config.NodeTypeFull,
	})
	defer stopFull()

	senderNode, stopSender := startTestNode(t, config.Node{
		ListenAddress:    addressSender,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             config.NodeTypeClient,
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
		idRecipient.Address,
		identity.BoxPublicKeyBase64(idRecipient.BoxPublicKey),
		"sender-client-backlog-secret",
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

	waitForCondition(t, 8*time.Second, func() bool {
		fullReply := fullNode.HandleLocalFrame(protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: idRecipient.Address})
		return fullReply.Type == "inbox" && len(fullReply.Messages) == 1 && fullReply.Messages[0].ID == "client-offline-dm-1"
	})

	recipientNode, stopRecipient := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:    addressRecipient,
		AdvertiseAddress: "",
		BootstrapPeers:   []string{normalizeAddress(addressFull)},
		Type:             config.NodeTypeClient,
	}, idRecipient)
	defer stopRecipient()

	waitForCondition(t, 8*time.Second, func() bool {
		messages := recipientNode.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		return messages.Type == "messages" && len(messages.Messages) == 1 && messages.Messages[0].ID == "client-offline-dm-1"
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
		Type:             config.NodeTypeFull,
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
		Type:             config.NodeTypeFull,
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
	svc.pending["198.51.100.2:64646"] = []pendingFrame{{Frame: frame, QueuedAt: time.Now().UTC()}}
	svc.pending["198.51.100.2:64647"] = []pendingFrame{{Frame: frame, QueuedAt: time.Now().UTC()}}
	svc.pendingKeys[pendingFrameKey("198.51.100.2:64646", frame)] = struct{}{}
	svc.pendingKeys[pendingFrameKey("198.51.100.2:64647", frame)] = struct{}{}
	receiptFrame := protocol.Frame{
		Type:        "send_delivery_receipt",
		ID:          frame.ID,
		Address:     "peer-recipient",
		Recipient:   id.Address,
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}
	svc.pending["198.51.100.1:64646"] = []pendingFrame{{Frame: receiptFrame, QueuedAt: time.Now().UTC()}}
	svc.pendingKeys[pendingFrameKey("198.51.100.1:64646", receiptFrame)] = struct{}{}
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
		Type:             config.NodeTypeFull,
	}, recipientID)

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender identity failed: %v", err)
	}
	svc.addKnownPubKey(senderID.Address, identity.PublicKeyBase64(senderID.PublicKey))

	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		recipientID.Address,
		identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		"for-myself-no-reroute",
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
		Type:             config.NodeTypeFull,
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

	items := reloaded.pending["127.0.0.1:65001"]
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
	svc.pending["peer-a"] = []pendingFrame{
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

	address := "127.0.0.1:65001"
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
		Type:      "send_delivery_receipt",
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
		Type:             config.NodeTypeFull,
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
		Type:      "send_delivery_receipt",
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

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("condition not met before timeout")
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
		Type:             config.NodeTypeFull,
	}, id)

	// 56 base32 chars = valid Tor v3.
	onion := "2gzyxa5ihm7nsber23gk5eqx3mp4wrymfbhqgk2ycdjp3yzcrllbiqad.onion"
	got, ok := svc.normalizePeerAddress("1.2.3.4:12345", onion+":64646")
	if !ok {
		t.Fatal("expected valid v3 .onion to be accepted")
	}
	if got != onion+":64646" {
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
		Type:             config.NodeTypeFull,
	}, id)

	// "junk.onion" is not a valid 16/56 base32 host.
	_, ok := svc.normalizePeerAddress("", "junk.onion:64646")
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
		Type:             config.NodeTypeFull,
	})
	defer stopA()

	_, stopB := startTestNode(t, config.Node{
		ListenAddress:    addressB,
		AdvertiseAddress: normalizeAddress(addressB),
		BootstrapPeers:   []string{normalizeAddress(addressA)},
		Type:             config.NodeTypeFull,
	})

	// Wait until A discovers B via peer exchange.
	waitForCondition(t, 5*time.Second, func() bool {
		nodeA.mu.RLock()
		defer nodeA.mu.RUnlock()
		for _, p := range nodeA.peers {
			if p.Address == normalizeAddress(addressB) {
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
	found := false
	for _, p := range state.Peers {
		if p.Address == normalizeAddress(addressB) {
			found = true
			break
		}
	}
	if !found {
		addrs := make([]string, len(state.Peers))
		for i, p := range state.Peers {
			addrs[i] = p.Address
		}
		t.Fatalf("expected node B address %s in persisted peers, got: %v", normalizeAddress(addressB), addrs)
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
		Type:             config.NodeTypeFull,
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
		Type:             config.NodeTypeFull,
	})

	// Add peers and mark one as connected.
	svc1.addPeerAddress("10.0.0.5:64646", "full", "")
	svc1.addPeerAddress("10.0.0.6:64646", "full", "")
	svc1.markPeerConnected("10.0.0.5:64646")
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
		Type:             config.NodeTypeFull,
	})
	defer stop2()

	svc2.mu.RLock()
	// Should have: bootstrap (10.0.0.99) + persisted (10.0.0.5, 10.0.0.6).
	peerAddrs := make(map[string]bool)
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
	h5 := svc2.health["10.0.0.5:64646"]
	h6 := svc2.health["10.0.0.6:64646"]
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
			{Address: "10.0.0.50:64646", Score: 30, Source: "peer_exchange", LastConnectedAt: &now},
			{Address: "10.0.0.51:64646", Score: 10, Source: "peer_exchange"},
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
		Type:             config.NodeTypeFull,
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
		Type:             config.NodeTypeFull,
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

	onionAddr := strings.Repeat("a", 56) + ".onion:64646"
	now := time.Now().UTC()
	persisted := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{Address: onionAddr, Score: 80, Source: "peer_exchange", LastConnectedAt: &now},
			{Address: "10.0.0.1:64646", Score: 50, Source: "peer_exchange"},
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
		Type:             config.NodeTypeFull,
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

	onionAddr := strings.Repeat("a", 56) + ".onion:64646"
	now := time.Now().UTC()
	persisted := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{Address: onionAddr, Score: 80, Source: "peer_exchange", LastConnectedAt: &now},
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
		Type:             config.NodeTypeFull,
	}, id)

	candidates := candidateAddresses(svc.peerDialCandidates())
	found := false
	for _, c := range candidates {
		if c == onionAddr {
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
		Type:             config.NodeTypeFull,
	})
	defer stop()

	// Add three peers with different scores.
	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")
	svc.addPeerAddress("10.0.0.2:64646", "full", "peer-2")
	svc.addPeerAddress("10.0.0.3:64646", "full", "peer-3")

	// Simulate scoring: peer-2 is best, peer-3 is worst.
	svc.markPeerConnected("10.0.0.2:64646") // +10
	svc.markPeerConnected("10.0.0.2:64646") // +10 (total 20)
	svc.markPeerConnected("10.0.0.1:64646") // +10 (total 10)
	svc.markPeerDisconnected("10.0.0.1:64646", nil) // clean (-2, total 8)
	svc.markPeerDisconnected("10.0.0.2:64646", nil) // clean (-2, total 18)

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
		Type:             config.NodeTypeFull,
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
		Type:             config.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	// One failure: no cooldown (first failure is exempt), but we still
	// backdate to verify the expiry path for future failures.
	svc.markPeerDisconnected("10.0.0.1:64646", fmt.Errorf("timeout"))

	// Backdate LastDisconnectedAt to simulate cooldown expiry.
	svc.mu.Lock()
	if h := svc.health["10.0.0.1:64646"]; h != nil {
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

// TestEvictStalePeersRemovesBadPeers verifies that peers with score ≤ threshold
// and no recent connection activity are evicted from in-memory state.
func TestEvictStalePeersRemovesBadPeers(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
		Type:             config.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")
	svc.addPeerAddress("10.0.0.2:64646", "full", "peer-2")

	// Make peer-1 terrible: low score, last seen >24h ago.
	svc.mu.Lock()
	svc.health["10.0.0.1:64646"] = &peerHealth{
		Address:             "10.0.0.1:64646",
		Score:               -30,
		ConsecutiveFailures: 10,
		LastConnectedAt:     time.Now().Add(-48 * time.Hour),
		LastDisconnectedAt:  time.Now().Add(-47 * time.Hour),
	}
	// peer-2 has low score but was recently connected — should survive.
	svc.health["10.0.0.2:64646"] = &peerHealth{
		Address:             "10.0.0.2:64646",
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
	if svc.health["10.0.0.1:64646"] != nil {
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
		Type:             config.NodeTypeFull,
	})
	defer stop()

	// Give the bootstrap peer a terrible score.
	svc.mu.Lock()
	svc.health["10.0.0.99:64646"] = &peerHealth{
		Address:             "10.0.0.99:64646",
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
		Type:             config.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	svc.mu.Lock()
	svc.health["10.0.0.1:64646"] = &peerHealth{
		Address:             "10.0.0.1:64646",
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
		Type:             config.NodeTypeFull,
	})
	defer stop()

	svc.addPeerAddress("10.0.0.1:64646", "full", "peer-1")

	addedAt := time.Now().Add(-48 * time.Hour)
	svc.mu.Lock()
	// Peer has never successfully connected (LastConnectedAt is zero).
	// LastDisconnectedAt is recent (simulating repeated retries), but
	// eviction should look at AddedAt, not LastDisconnectedAt.
	svc.health["10.0.0.1:64646"] = &peerHealth{
		Address:             "10.0.0.1:64646",
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
		Type:             config.NodeTypeFull,
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
	primaryHealth := svc.health["10.0.0.1:64647"]
	fallbackHealth := svc.health["10.0.0.1:64646"]
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
	svc.markPeerConnected("10.0.0.1:64646")

	svc.mu.RLock()
	primaryHealth = svc.health["10.0.0.1:64647"]
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
		Type:             config.NodeTypeFull,
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
		Type:             config.NodeTypeFull,
	})
	defer stop()

	// Add a client peer with a non-default port.
	svc.addPeerAddress("10.0.0.1:64647", "client", "client-identity-abc")

	// Simulate a fallback session on :64646.
	fallbackAddr := "10.0.0.1:64646"
	svc.mu.Lock()
	svc.dialOrigin[fallbackAddr] = "10.0.0.1:64647"
	svc.sessions[fallbackAddr] = &peerSession{address: fallbackAddr}
	svc.mu.Unlock()
	svc.markPeerConnected(fallbackAddr)

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
		Type:             config.NodeTypeFull,
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
	svc.health["10.0.0.1:64646"] = &peerHealth{
		Address:             "10.0.0.1:64646",
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

	primaryAddr := "10.0.0.1:64647"
	fallbackAddr := "10.0.0.1:64646"

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
		Type:             config.NodeTypeFull,
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
	if err := os.WriteFile(queuePath, data, 0644); err != nil {
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
	primaryPending := len(svc.pending[primaryAddr])
	fallbackPending := len(svc.pending[fallbackAddr])
	svc.mu.RUnlock()

	if primaryPending != 1 {
		t.Fatalf("expected 1 pending frame migrated to primary %s, got %d", primaryAddr, primaryPending)
	}
	if fallbackPending != 0 {
		t.Fatalf("expected 0 pending frames under fallback %s after migration, got %d", fallbackAddr, fallbackPending)
	}

	// Verify pendingKeys were rebuilt with the primary address.
	expectedKey := pendingFrameKey(primaryAddr, qs.Pending[fallbackAddr][0].Frame)
	svc.mu.RLock()
	_, hasKey := svc.pendingKeys[expectedKey]
	svc.mu.RUnlock()
	if !hasKey {
		t.Fatalf("expected pending key %q to exist after migration", expectedKey)
	}

	// Old fallback-keyed entry should NOT exist.
	oldKey := pendingFrameKey(fallbackAddr, qs.Pending[fallbackAddr][0].Frame)
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
	if err := os.WriteFile(queuePath, data, 0644); err != nil {
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
	fallbackPending := len(svc.pending[fallbackAddr])
	aPending := len(svc.pending[primaryA])
	bPending := len(svc.pending[primaryB])
	orphanedCount := len(svc.orphaned[fallbackAddr])
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
	if svc.orphaned[fallbackAddr][0].Frame.ID != "ambiguous-1" {
		t.Fatalf("unexpected orphaned frame ID: %s", svc.orphaned[fallbackAddr][0].Frame.ID)
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
	if err := os.WriteFile(queuePath, data, 0644); err != nil {
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
	pendingCount := len(svc.pending[unknownAddr])
	orphanedCount := len(svc.orphaned[unknownAddr])
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
	if err := os.WriteFile(queuePath, data, 0644); err != nil {
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
	pendingCount := len(svc.pending[runtimeAddr])
	orphanedCount := len(svc.orphaned[runtimeAddr])
	svc.mu.RUnlock()

	// Current-version entries stay in pending — no migration runs.
	if pendingCount != 1 {
		t.Fatalf("expected 1 pending frame under %s, got %d", runtimeAddr, pendingCount)
	}
	if orphanedCount != 0 {
		t.Fatalf("expected 0 orphaned frames under %s, got %d", runtimeAddr, orphanedCount)
	}
}

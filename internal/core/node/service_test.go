package node

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
)

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

	got := svc.peerDialCandidates()
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

	got := svc.peerDialCandidates()
	if len(got) != config.DefaultOutgoingPeers {
		t.Fatalf("expected %d peer dial candidates, got %d: %#v", config.DefaultOutgoingPeers, len(got), got)
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "client-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "client-message"),
	)
	if got := frames[1]; got.Type != "message_stored" {
		t.Fatalf("unexpected client store response: %#v", got)
	}

	time.Sleep(1500 * time.Millisecond)

	final := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
	)
	step2 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "get_peers"},
	)
	step3 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_contacts"},
	)
	step4 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "series-msg-2", svc.Address(), "*", "immutable", timestampB, 0, "hello-separate"),
	)
	step5 := exchangeFrames(t, svc.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("global", "mesh-msg-1", nodeA.Address(), "*", "immutable", ts, 0, "hello-from-a"),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected nodeA store response: %#v", frames[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_messages", Topic: "global"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1 && reply[1].Messages[0].Body == "hello-from-a"
	})

	final := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "dm-msg-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", frames[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: nodeB.Address()},
		)
		return reply[1].Type == "inbox" && len(reply[1].Messages) == 1 && reply[1].Messages[0].ID == "dm-msg-1"
	})

	inboxB := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: nodeB.Address()},
	)
	assertMessageFrame(t, inboxB[1], "inbox", "dm", 1, protocol.MessageFrame{
		ID: "dm-msg-1", Sender: nodeA.Address(), Recipient: nodeB.Address(), Flag: "sender-delete", CreatedAt: ts, TTLSeconds: 0, Body: ciphertext,
	})

	inboxA := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: nodeA.Address()},
	)
	if got := inboxA[1]; got.Type != "inbox" || got.Count != 0 {
		t.Fatalf("unexpected nodeA inbox: %#v", got)
	}

	dmIDs := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "fetch_message_ids", Topic: "dm"},
	)
	if got := dmIDs[1]; got.Type != "message_ids" || len(got.IDs) != 1 || got.IDs[0] != "dm-msg-1" {
		t.Fatalf("unexpected dm id list: %#v", got)
	}

	single := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		sendMessageFrame("dm", "push-dm-1", nodeA.Address(), nodeB.Address(), "sender-delete", ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected nodeA direct store response: %#v", frames[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: nodeB.Address()},
		)
		return reply[1].Type == "inbox" && len(reply[1].Messages) == 1 && reply[1].Messages[0].ID == "push-dm-1"
	})

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeA.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "publish_notice", TTLSeconds: 30, Ciphertext: ciphertext},
	)
	if got := frames[1]; got.Type != "notice_stored" {
		t.Fatalf("unexpected notice store response: %#v", got)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeFrames(t, nodeB.externalListenAddress(),
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_notices"},
		)
		return reply[1].Type == "notices" && len(reply[1].Notices) > 0
	})

	replyB := exchangeFrames(t, nodeB.externalListenAddress(),
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
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
			protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
			protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: nodeB.Address()},
		)
		return reply[1].Type == "inbox" && len(reply[1].Messages) == 1 && reply[1].Messages[0].ID == "retry-dm-1"
	})
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

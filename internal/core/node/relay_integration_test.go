package node

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
)

// registerContact imports a contact's keys into the given service so that
// DM signature verification succeeds.
func registerContact(t *testing.T, svc *Service, id *identity.Identity) {
	t.Helper()
	reply := svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: id.Address,
			PubKey:  identity.PublicKeyBase64(id.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(id.BoxPublicKey),
			BoxSig:  identity.SignBoxKeyBinding(id),
		}},
	})
	if reply.Type != "contacts_imported" {
		t.Fatalf("import_contacts failed: %#v", reply)
	}
}

// ---------------------------------------------------------------------------
// Integration test: 4 full nodes in a chain, DM from first to last.
//
//	Sender ─→ NodeA ─→ NodeB ─→ NodeC ─→ Recipient
//
// The sender identity is foreign (not a node). The DM enters via NodeA,
// propagates through relay + gossip, and arrives at NodeC (which is the
// recipient's hosting node). The test verifies end-to-end delivery.
// ---------------------------------------------------------------------------

func TestRelayChain4NodesDMDelivery(t *testing.T) {
	t.Parallel()

	addrA := freeAddress(t)
	addrB := freeAddress(t)
	addrC := freeAddress(t)

	// Create sender and recipient identities (not nodes).
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient: %v", err)
	}

	// Chain: A → B → C (each bootstraps to the next).
	// All three are full nodes so they can relay.
	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addrA,
		AdvertiseAddress: normalizeAddress(addrA),
		BootstrapPeers:   []string{normalizeAddress(addrB)},
		Type:             config.NodeTypeFull,
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addrB,
		AdvertiseAddress: normalizeAddress(addrB),
		BootstrapPeers:   []string{normalizeAddress(addrA), normalizeAddress(addrC)},
		Type:             config.NodeTypeFull,
	})
	defer stopB()

	nodeC, stopC := startTestNode(t, config.Node{
		ListenAddress:    addrC,
		AdvertiseAddress: normalizeAddress(addrC),
		BootstrapPeers:   []string{normalizeAddress(addrB)},
		Type:             config.NodeTypeFull,
	})
	defer stopC()

	// Register sender and recipient keys on all nodes so DM verification works.
	for _, svc := range []*Service{nodeA, nodeB, nodeC} {
		registerContact(t, svc, senderID)
		registerContact(t, svc, recipientID)
	}

	// Wait for peer connections to establish across the chain.
	waitForCondition(t, 8*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 &&
			len(nodeB.Peers()) >= 2 &&
			len(nodeC.Peers()) >= 1
	})

	// Encrypt a DM from sender to recipient.
	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		recipientID.Address,
		identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		"hello-through-relay-chain",
	)
	if err != nil {
		t.Fatalf("encrypt DM: %v", err)
	}

	// Inject the DM into nodeA via inbound TCP.
	ts := time.Now().UTC().Format(time.RFC3339)
	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{
			Type: "hello", Version: config.ProtocolVersion,
			Client: "test", ClientVersion: config.CorsaWireVersion,
		},
		sendMessageFrame("dm", "chain-dm-1", senderID.Address, recipientID.Address,
			string(protocol.MessageFlagImmutable), ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("expected message_stored from nodeA, got %s: %s", frames[1].Type, frames[1].Error)
	}

	// Wait for the DM to propagate to nodeC via relay/gossip chain.
	waitForCondition(t, 15*time.Second, func() bool {
		reply := exchangeFrames(t, nodeC.externalListenAddress(),
			protocol.Frame{
				Type: "hello", Version: config.ProtocolVersion,
				Client: "test", ClientVersion: config.CorsaWireVersion,
			},
			protocol.Frame{Type: "fetch_messages", Topic: "dm"},
		)
		if reply[1].Type != "messages" {
			return false
		}
		for _, msg := range reply[1].Messages {
			if msg.ID == "chain-dm-1" && msg.Recipient == recipientID.Address {
				return true
			}
		}
		return false
	})
}

// ---------------------------------------------------------------------------
// Integration test: relay dedupe — same message_id from two neighbors
// results in only one stored copy.
// ---------------------------------------------------------------------------

func TestRelayDedupeFromTwoNeighbors(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	registerContact(t, svc, senderID)

	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		svc.Address(),
		identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey),
		"dedupe-test-body",
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339)
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "dedupe-msg-1",
		Address:     senderID.Address,
		Recipient:   svc.Address(),
		Topic:       "dm",
		Body:        ciphertext,
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   ts,
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: "10.0.0.1:9000",
	}

	// First relay from neighbor A — should succeed.
	status1 := svc.handleRelayMessage("10.0.0.1:9000", nil, frame)
	if status1 != "delivered" {
		t.Fatalf("first relay should deliver, got %q", status1)
	}

	// Same message_id from neighbor B — should be deduped (empty status).
	frame.PreviousHop = "10.0.0.2:9000"
	status2 := svc.handleRelayMessage("10.0.0.2:9000", nil, frame)
	if status2 != "" {
		t.Fatalf("duplicate relay should be dropped, got %q", status2)
	}

	// Verify only one copy stored.
	svc.mu.RLock()
	count := len(svc.topics["dm"])
	svc.mu.RUnlock()
	if count != 1 {
		t.Fatalf("expected exactly 1 stored message, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Integration test: mixed network with one legacy node (no capabilities).
//
//	Sender ─→ FullNode ─→ LegacyNode
//	              └──→ FullNode2
//
// The legacy node should still receive the DM via gossip (not relay).
// ---------------------------------------------------------------------------

func TestMixedNetworkLegacyNodeReceivesViaGossip(t *testing.T) {
	t.Parallel()

	addrFull := freeAddress(t)
	addrLegacy := freeAddress(t)

	// Full node with relay capability.
	nodeFull, stopFull := startTestNode(t, config.Node{
		ListenAddress:    addrFull,
		AdvertiseAddress: normalizeAddress(addrFull),
		BootstrapPeers:   []string{normalizeAddress(addrLegacy)},
		Type:             config.NodeTypeFull,
	})
	defer stopFull()

	// "Legacy" node — still a full node (it gets relay capability from
	// localCapabilities), but we verify that gossip (not relay) is the
	// delivery mechanism when the recipient happens to be local.
	nodeLegacy, stopLegacy := startTestNode(t, config.Node{
		ListenAddress:    addrLegacy,
		AdvertiseAddress: normalizeAddress(addrLegacy),
		BootstrapPeers:   []string{normalizeAddress(addrFull)},
		Type:             config.NodeTypeFull,
	})
	defer stopLegacy()

	// Wait for peer connections.
	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeFull.Peers()) >= 1 && len(nodeLegacy.Peers()) >= 1
	})

	// Store a global message on nodeFull and verify it propagates to nodeLegacy.
	ts := time.Now().UTC().Format(time.RFC3339)
	frames := exchangeFrames(t, nodeFull.externalListenAddress(),
		protocol.Frame{
			Type: "hello", Version: config.ProtocolVersion,
			Client: "test", ClientVersion: config.CorsaWireVersion,
		},
		sendMessageFrame("global", "mixed-msg-1", nodeFull.Address(), "*",
			"immutable", ts, 0, "hello-from-full"),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("unexpected store response: %#v", frames[1])
	}

	// Verify gossip delivers the message to the legacy node.
	waitForCondition(t, 8*time.Second, func() bool {
		reply := exchangeFrames(t, nodeLegacy.externalListenAddress(),
			protocol.Frame{
				Type: "hello", Version: config.ProtocolVersion,
				Client: "test", ClientVersion: config.CorsaWireVersion,
			},
			protocol.Frame{Type: "fetch_messages", Topic: "global"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1 &&
			reply[1].Messages[0].ID == "mixed-msg-1"
	})
}

// ---------------------------------------------------------------------------
// Mixed-version test: new node sends to old node via legacy gossip path.
// Old node without relay capability still receives DMs.
// ---------------------------------------------------------------------------

func TestMixedVersionNewToOldFallsBackToGossip(t *testing.T) {
	t.Parallel()

	addrNew := freeAddress(t)
	addrOld := freeAddress(t)

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}

	// "New" full node with relay.
	nodeNew, stopNew := startTestNode(t, config.Node{
		ListenAddress:    addrNew,
		AdvertiseAddress: normalizeAddress(addrNew),
		BootstrapPeers:   []string{normalizeAddress(addrOld)},
		Type:             config.NodeTypeFull,
	})
	defer stopNew()

	// "Old" full node (still has relay capability in tests, but we
	// use a global topic to test pure gossip delivery without DM encryption).
	nodeOld, stopOld := startTestNode(t, config.Node{
		ListenAddress:    addrOld,
		AdvertiseAddress: normalizeAddress(addrOld),
		BootstrapPeers:   []string{normalizeAddress(addrNew)},
		Type:             config.NodeTypeFull,
	})
	defer stopOld()

	registerContact(t, nodeNew, senderID)
	registerContact(t, nodeOld, senderID)

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeNew.Peers()) >= 1 && len(nodeOld.Peers()) >= 1
	})

	// Store a global message on the new node.
	ts := time.Now().UTC().Format(time.RFC3339)
	frames := exchangeFrames(t, nodeNew.externalListenAddress(),
		protocol.Frame{
			Type: "hello", Version: config.ProtocolVersion,
			Client: "test", ClientVersion: config.CorsaWireVersion,
		},
		sendMessageFrame("global", "compat-msg-1", senderID.Address, "*",
			"immutable", ts, 0, "gossip-works"),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("store failed: %#v", frames[1])
	}

	// Old node receives via gossip.
	waitForCondition(t, 8*time.Second, func() bool {
		reply := exchangeFrames(t, nodeOld.externalListenAddress(),
			protocol.Frame{
				Type: "hello", Version: config.ProtocolVersion,
				Client: "test", ClientVersion: config.CorsaWireVersion,
			},
			protocol.Frame{Type: "fetch_messages", Topic: "global"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1
	})
}

// ---------------------------------------------------------------------------
// Mixed-version test: old node sends, new node receives. Gossip path works
// regardless of relay capabilities.
// ---------------------------------------------------------------------------

func TestMixedVersionOldToNewContinuesToWork(t *testing.T) {
	t.Parallel()

	addrOld := freeAddress(t)
	addrNew := freeAddress(t)

	nodeOld, stopOld := startTestNode(t, config.Node{
		ListenAddress:    addrOld,
		AdvertiseAddress: normalizeAddress(addrOld),
		BootstrapPeers:   []string{normalizeAddress(addrNew)},
		Type:             config.NodeTypeFull,
	})
	defer stopOld()

	nodeNew, stopNew := startTestNode(t, config.Node{
		ListenAddress:    addrNew,
		AdvertiseAddress: normalizeAddress(addrNew),
		BootstrapPeers:   []string{normalizeAddress(addrOld)},
		Type:             config.NodeTypeFull,
	})
	defer stopNew()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeOld.Peers()) >= 1 && len(nodeNew.Peers()) >= 1
	})

	ts := time.Now().UTC().Format(time.RFC3339)
	frames := exchangeFrames(t, nodeOld.externalListenAddress(),
		protocol.Frame{
			Type: "hello", Version: config.ProtocolVersion,
			Client: "test", ClientVersion: config.CorsaWireVersion,
		},
		sendMessageFrame("global", "reverse-compat-1", nodeOld.Address(), "*",
			"immutable", ts, 0, "from-old-node"),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("store failed: %#v", frames[1])
	}

	waitForCondition(t, 15*time.Second, func() bool {
		reply := exchangeFrames(t, nodeNew.externalListenAddress(),
			protocol.Frame{
				Type: "hello", Version: config.ProtocolVersion,
				Client: "test", ClientVersion: config.CorsaWireVersion,
			},
			protocol.Frame{Type: "fetch_messages", Topic: "global"},
		)
		return reply[1].Type == "messages" && len(reply[1].Messages) == 1
	})
}

// ---------------------------------------------------------------------------
// Overload test: per-peer capacity cap.
// Pre-fills the relay state store to the per-peer limit via tryReserve
// (instant, no crypto overhead), then sends a few real relay_message frames
// through the pipeline and verifies they are rejected.
// (Global cap is verified by unit tests in relay_test.go.)
// ---------------------------------------------------------------------------

func TestRelayFloodDoesNotCauseUnboundedGrowth(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	floodPeer := "10.0.0.1:9000"

	// Pre-fill the store to the per-peer limit (instant, no pipeline).
	for i := 0; i < maxRelayStatesPerPeer; i++ {
		svc.relayStates.tryReserve(fmt.Sprintf("prefill-%d", i), floodPeer)
	}

	// Send a handful of real frames — they must be rejected.
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}

	for i := 0; i < 5; i++ {
		frame := protocol.Frame{
			Type:        "relay_message",
			ID:          fmt.Sprintf("flood-%d", i),
			Address:     senderID.Address,
			Recipient:   svc.Address(),
			Topic:       "dm",
			Body:        "fake-body",
			Flag:        "immutable",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339),
			HopCount:    2,
			MaxHops:     10,
			PreviousHop: floodPeer,
		}
		svc.handleRelayMessage(floodPeer, nil, frame)
	}

	svc.relayStates.mu.Lock()
	perPeerCount := svc.relayStates.perPeer[floodPeer]
	rejected := svc.relayStates.rejected
	svc.relayStates.mu.Unlock()

	if perPeerCount > maxRelayStatesPerPeer {
		t.Fatalf("per-peer relay count exceeded limit: %d > %d", perPeerCount, maxRelayStatesPerPeer)
	}
	if rejected == 0 {
		t.Fatal("expected some relay entries to be rejected due to per-peer capacity")
	}
}

// ---------------------------------------------------------------------------
// Overload test: one peer's flood does not block other peers.
// Pre-fills per-peer quota for peer1 via tryReserve, then verifies
// that peer2 can still deliver a relay_message through the pipeline.
// ---------------------------------------------------------------------------

func TestRelayFloodPerPeerLimitProtectsOtherPeers(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Saturate peer1's quota (instant, no pipeline overhead).
	floodPeer := "10.0.0.1:9000"
	for i := 0; i < maxRelayStatesPerPeer; i++ {
		svc.relayStates.tryReserve(fmt.Sprintf("flood-peer1-%d", i), floodPeer)
	}

	// A different peer should still be able to deliver.
	senderID, _ := identity.Generate()
	registerContact(t, svc, senderID)

	ciphertext, _ := directmsg.EncryptForParticipants(
		senderID,
		svc.Address(),
		identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey),
		"not-blocked",
	)

	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "from-peer2",
		Address:     senderID.Address,
		Recipient:   svc.Address(),
		Topic:       "dm",
		Body:        ciphertext,
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: "10.0.0.2:9000",
	}

	status := svc.handleRelayMessage("10.0.0.2:9000", nil, frame)
	if status != "delivered" {
		t.Fatalf("peer2 relay should succeed despite peer1 flood, got %q", status)
	}
}

// ---------------------------------------------------------------------------
// Integration test: relay chain with live inbox route. Sender injects a DM
// into NodeA. The recipient connects directly to NodeC with subscribe_inbox.
// The DM should be pushed to the recipient via the live route.
// ---------------------------------------------------------------------------

func TestRelayChainWithLiveInboxRoute(t *testing.T) {
	t.Parallel()

	addrA := freeAddress(t)
	addrB := freeAddress(t)

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient: %v", err)
	}

	// Two full nodes: A → B.
	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addrA,
		AdvertiseAddress: normalizeAddress(addrA),
		BootstrapPeers:   []string{normalizeAddress(addrB)},
		Type:             config.NodeTypeFull,
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:    addrB,
		AdvertiseAddress: normalizeAddress(addrB),
		BootstrapPeers:   []string{normalizeAddress(addrA)},
		Type:             config.NodeTypeFull,
	})
	defer stopB()

	// Register contacts on both nodes.
	for _, svc := range []*Service{nodeA, nodeB} {
		registerContact(t, svc, senderID)
		registerContact(t, svc, recipientID)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeA.Peers()) >= 1 && len(nodeB.Peers()) >= 1
	})

	// Recipient connects to nodeB with authenticated session + subscribe_inbox.
	recipConn, err := net.DialTimeout("tcp", nodeB.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial recipient: %v", err)
	}
	defer func() { _ = recipConn.Close() }()
	_ = recipConn.SetDeadline(time.Now().Add(15 * time.Second))
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

	// subscribe_inbox
	writeJSONFrame(t, recipConn, protocol.Frame{
		Type:       "subscribe_inbox",
		Topic:      "dm",
		Recipient:  recipientID.Address,
		Subscriber: "test-recip-sub",
	})
	subReply := readJSONTestFrame(t, recipReader)
	if subReply.Type != "subscribed" {
		t.Fatalf("expected subscribed, got %s: %s", subReply.Type, subReply.Error)
	}

	// Drain any reverse subscribe frame.
	_ = recipConn.SetDeadline(time.Now().Add(2 * time.Second))
	if reverseFrame, err := recipReader.ReadString('\n'); err == nil {
		var rf protocol.Frame
		rf, _ = protocol.ParseFrameLine(reverseFrame[:len(reverseFrame)-1])
		if rf.Type == "subscribe_inbox" {
			writeJSONFrame(t, recipConn, protocol.Frame{
				Type:       "subscribed",
				Topic:      rf.Topic,
				Recipient:  rf.Recipient,
				Subscriber: rf.Subscriber,
			})
		}
	}
	_ = recipConn.SetDeadline(time.Now().Add(15 * time.Second))

	// Encrypt DM and inject into nodeA.
	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		recipientID.Address,
		identity.BoxPublicKeyBase64(recipientID.BoxPublicKey),
		"hello-via-live-route",
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339)
	frames := exchangeFrames(t, nodeA.externalListenAddress(),
		protocol.Frame{
			Type: "hello", Version: config.ProtocolVersion,
			Client: "test", ClientVersion: config.CorsaWireVersion,
		},
		sendMessageFrame("dm", "live-route-1", senderID.Address, recipientID.Address,
			string(protocol.MessageFlagImmutable), ts, 0, ciphertext),
	)
	if frames[1].Type != "message_stored" {
		t.Fatalf("expected message_stored, got %s: %s", frames[1].Type, frames[1].Error)
	}

	// Read push_message on recipient connection.
	_ = recipConn.SetDeadline(time.Now().Add(10 * time.Second))
	pushed := readJSONTestFrame(t, recipReader)
	if pushed.Type != "push_message" {
		t.Fatalf("expected push_message on live route, got %s", pushed.Type)
	}
	if pushed.Item == nil || pushed.Item.ID != "live-route-1" {
		t.Fatalf("wrong push_message: %#v", pushed)
	}
}

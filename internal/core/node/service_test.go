package node

import (
	"bufio"
	"context"
	"fmt"
	"net"
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
		protocol.Frame{Type: "hello", Version: 1, Client: "test", ClientVersion: config.CorsaWireVersion},
		protocol.Frame{Type: "get_peers"},
		sendMessageFrame("global", "msg-1", svc.Address(), "*", "immutable", ts, 0, "hello"),
		protocol.Frame{Type: "fetch_messages", Topic: "global"},
		protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: svc.Address()},
	)

	if got := frames[0]; got.Type != "welcome" || got.Address != svc.Address() {
		t.Fatalf("unexpected welcome: %#v", got)
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

func TestClientNodeDoesNotForwardMeshTraffic(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:    addressA,
		AdvertiseAddress: normalizeAddress(addressA),
		BootstrapPeers:   []string{normalizeAddress(addressB)},
		Type:             config.NodeTypeClient,
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
	})
	defer stopB()

	waitForCondition(t, 5*time.Second, func() bool {
		return len(nodeB.Peers()) >= 1
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Run(ctx)
	}()

	waitForCondition(t, 3*time.Second, func() bool {
		conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		return false
	})

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

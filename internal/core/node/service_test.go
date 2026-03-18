package node

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/identity"
)

func TestSingleNodeProtocolFlow(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	lines := exchangeLines(t, svc.externalListenAddress(),
		"HELLO version=1 client=test",
		"GET_PEERS",
		"SEND_MESSAGE global "+svc.Address()+" * hello",
		"FETCH_MESSAGES global",
		"FETCH_INBOX global "+svc.Address(),
	)

	if got := lines[0]; !strings.HasPrefix(got, "WELCOME version=1 node=corsa network=gazeta-devnet node_type=full client_version=0.1-alpha services=identity,contacts,messages,gazeta,relay address=") {
		t.Fatalf("unexpected welcome: %q", got)
	}
	if got := lines[1]; got != "PEERS count=0 list=" {
		t.Fatalf("unexpected peers: %q", got)
	}
	if !strings.HasPrefix(lines[2], "MESSAGE_STORED topic=global count=1 id=") {
		t.Fatalf("unexpected stored response: %q", lines[2])
	}
	if got := lines[3]; got != "MESSAGES topic=global count=1 list="+svc.Address()+">*>hello" {
		t.Fatalf("unexpected messages: %q", got)
	}
	if got := lines[4]; got != "INBOX topic=global recipient="+svc.Address()+" count=1 list="+svc.Address()+">*>hello" {
		t.Fatalf("unexpected inbox: %q", got)
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

	lines := exchangeLines(t, nodeA.externalListenAddress(),
		"HELLO version=1 client=test",
		"SEND_MESSAGE global "+nodeA.Address()+" * client-message",
	)

	if !strings.HasPrefix(lines[1], "MESSAGE_STORED topic=global count=1 id=") {
		t.Fatalf("unexpected client store response: %q", lines[1])
	}

	time.Sleep(1500 * time.Millisecond)

	final := exchangeLines(t, nodeB.externalListenAddress(),
		"HELLO version=1 client=test",
		"FETCH_MESSAGES global",
	)

	if got := final[1]; got != "MESSAGES topic=global count=0 list=" {
		t.Fatalf("expected full node not to receive relayed client message, got %q", got)
	}
}

func TestDuplicateSendMessageIsDeduplicated(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	lines := exchangeLines(t, svc.externalListenAddress(),
		"HELLO version=1 client=test",
		"SEND_MESSAGE global "+svc.Address()+" * same",
		"SEND_MESSAGE global "+svc.Address()+" * same",
		"FETCH_MESSAGES global",
	)

	if !strings.HasPrefix(lines[1], "MESSAGE_STORED topic=global count=1 id=") {
		t.Fatalf("unexpected first store response: %q", lines[1])
	}
	if got := lines[2]; got != "MESSAGE_KNOWN topic=global count=1" {
		t.Fatalf("unexpected duplicate response: %q", got)
	}
	if got := lines[3]; got != "MESSAGES topic=global count=1 list="+svc.Address()+">*>same" {
		t.Fatalf("unexpected fetch response: %q", got)
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

	lines := exchangeLines(t, nodeA.externalListenAddress(),
		"HELLO version=1 client=test",
		"SEND_MESSAGE global "+nodeA.Address()+" * hello-from-a",
	)

	if !strings.HasPrefix(lines[1], "MESSAGE_STORED topic=global count=1 id=") {
		t.Fatalf("unexpected nodeA store response: %q", lines[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeLines(t, nodeB.externalListenAddress(),
			"HELLO version=1 client=test",
			"FETCH_MESSAGES global",
		)
		return len(reply) == 2 && reply[1] == "MESSAGES topic=global count=1 list="+nodeA.Address()+">*>hello-from-a"
	})

	final := exchangeLines(t, nodeB.externalListenAddress(),
		"HELLO version=1 client=test",
		"FETCH_MESSAGES global",
		"GET_PEERS",
	)

	if got := final[1]; got != "MESSAGES topic=global count=1 list="+nodeA.Address()+">*>hello-from-a" {
		t.Fatalf("unexpected propagated message list: %q", got)
	}
	if !strings.Contains(final[2], normalizeAddress(addressA)) {
		t.Fatalf("expected nodeB peer list to include nodeA, got %q", final[2])
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

	lines := exchangeLines(t, nodeA.externalListenAddress(),
		"HELLO version=1 client=test",
		"SEND_MESSAGE dm "+nodeA.Address()+" "+nodeB.Address()+" "+ciphertext,
	)
	if !strings.HasPrefix(lines[1], "MESSAGE_STORED topic=dm count=1 id=") {
		t.Fatalf("unexpected nodeA direct store response: %q", lines[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeLines(t, nodeB.externalListenAddress(),
			"HELLO version=1 client=test",
			"FETCH_INBOX dm "+nodeB.Address(),
		)
		return len(reply) == 2 && reply[1] == "INBOX topic=dm recipient="+nodeB.Address()+" count=1 list="+nodeA.Address()+">"+nodeB.Address()+">"+ciphertext
	})

	inboxB := exchangeLines(t, nodeB.externalListenAddress(),
		"HELLO version=1 client=test",
		"FETCH_INBOX dm "+nodeB.Address(),
	)
	if got := inboxB[1]; got != "INBOX topic=dm recipient="+nodeB.Address()+" count=1 list="+nodeA.Address()+">"+nodeB.Address()+">"+ciphertext {
		t.Fatalf("unexpected nodeB inbox: %q", got)
	}

	inboxA := exchangeLines(t, nodeA.externalListenAddress(),
		"HELLO version=1 client=test",
		"FETCH_INBOX dm "+nodeA.Address(),
	)
	if got := inboxA[1]; got != "INBOX topic=dm recipient="+nodeA.Address()+" count=0 list=" {
		t.Fatalf("unexpected nodeA inbox: %q", got)
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
	lines := exchangeLines(t, nodeA.externalListenAddress(),
		"HELLO version=1 client=test",
		"SEND_MESSAGE dm "+nodeA.Address()+" "+nodeB.Address()+" "+tampered,
	)
	if got := lines[1]; got != "ERR invalid-direct-message-signature" {
		t.Fatalf("unexpected invalid signature response: %q", got)
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

	lines := exchangeLines(t, nodeA.externalListenAddress(),
		"HELLO version=1 client=test",
		"PUBLISH_NOTICE 30 "+ciphertext,
	)
	if !strings.HasPrefix(lines[1], "NOTICE_STORED id=") {
		t.Fatalf("unexpected notice store response: %q", lines[1])
	}

	waitForCondition(t, 5*time.Second, func() bool {
		reply := exchangeLines(t, nodeB.externalListenAddress(),
			"HELLO version=1 client=test",
			"FETCH_NOTICES",
		)
		return len(reply) == 2 && strings.Contains(reply[1], ciphertext)
	})

	replyB := exchangeLines(t, nodeB.externalListenAddress(),
		"HELLO version=1 client=test",
		"FETCH_NOTICES",
	)
	noticesB, err := parseNoticeCiphertexts(strings.TrimSpace(replyB[1]))
	if err != nil {
		t.Fatalf("parseNoticeCiphertexts failed: %v", err)
	}
	if len(noticesB) == 0 {
		t.Fatal("expected nodeB to have at least one notice")
	}

	plain, err := gazeta.DecryptForIdentity(nodeB.identity, noticesB[0])
	if err != nil {
		t.Fatalf("expected nodeB to decrypt notice: %v", err)
	}
	if plain.Body != "meet-at-dawn" {
		t.Fatalf("unexpected gazeta body: %q", plain.Body)
	}

	if _, err := gazeta.DecryptForIdentity(nodeA.identity, noticesB[0]); err == nil {
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

func exchangeLines(t *testing.T, address string, commands ...string) []string {
	t.Helper()

	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	reader := bufio.NewReader(conn)
	lines := make([]string, 0, len(commands))
	for _, cmd := range commands {
		if _, err := fmt.Fprintf(conn, "%s\n", cmd); err != nil {
			t.Fatalf("write %q: %v", cmd, err)
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response for %q: %v", cmd, err)
		}

		lines = append(lines, strings.TrimSpace(line))
	}

	return lines
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
	if strings.HasPrefix(address, ":") {
		return "127.0.0.1" + address
	}
	return address
}

func parseNoticeCiphertexts(line string) ([]string, error) {
	const prefix = "NOTICES "
	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("unexpected notices line: %s", line)
	}

	fields := strings.Fields(line)
	for _, field := range fields {
		if strings.HasPrefix(field, "list=") {
			value := strings.TrimPrefix(field, "list=")
			if value == "" {
				return nil, nil
			}

			items := strings.Split(value, ",")
			out := make([]string, 0, len(items))
			for _, item := range items {
				parts := strings.SplitN(item, "@", 3)
				if len(parts) != 3 {
					continue
				}
				out = append(out, parts[2])
			}
			return out, nil
		}
	}

	return nil, fmt.Errorf("missing notice list in line: %s", line)
}

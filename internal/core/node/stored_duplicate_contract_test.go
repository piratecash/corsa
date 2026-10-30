package node

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// The fixture uses real SQL through chatlogBackedStore's test adapter, not
// the Service MessageStoreAdapter (which cannot be imported into package node).
func TestDurableDuplicateIsAcceptedWithoutAnotherArrival(t *testing.T) {
	f := newCrossPathFixture(t)
	msg := incomingMessage{
		ID: "e8016eac-8135-428b-96fe-3d42e25f9201", Topic: "dm", Sender: f.sender.Address,
		Recipient: f.svc.Address(), Flag: protocol.MessageFlagImmutable, CreatedAt: time.Now().UTC(),
		Body: sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey)),
	}
	envelope := protocol.Envelope{ID: msg.ID, Topic: msg.Topic, Sender: msg.Sender, Recipient: msg.Recipient, Flag: msg.Flag, Payload: []byte(msg.Body), CreatedAt: msg.CreatedAt}
	if result := f.store.StoreMessage(envelope, false); result != StoreInserted {
		t.Fatalf("seed durable row=%v", result)
	}
	ref := admissionRef(msg)
	before, found, err := f.store.store.EntryByRef(context.Background(), ref)
	if err != nil || !found {
		t.Fatalf("seed exact row: found=%v err=%v", found, err)
	}
	// Any call to Route is unexpected: a durable duplicate must leave before
	// routing, rather than merely relying on an empty routing decision.
	f.svc.router = NewMockRouter(t)
	reader := attachPushObserver(t, f.svc, f.sender.Address, netcore.ConnID(7310))
	for _, stage := range []string{"empty-bloom", "warm-bloom-without-backlog", "empty-bloom-again"} {
		t.Run(stage, func(t *testing.T) {
			if stage == "empty-bloom-again" {
				f.svc.gossipMu.Lock()
				f.svc.seen = newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, bloomDedupRotation, nil)
				f.svc.gossipMu.Unlock()
			}
			if accepted, count, code := f.svc.storeIncomingMessage(msg, true); !accepted || count != 0 || code != "" {
				t.Fatalf("durable duplicate must be accepted without an arrival: accepted=%v count=%d code=%q", accepted, count, code)
			}
			receipt := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
			if receipt.Receipt == nil || receipt.Receipt.MessageID != string(msg.ID) || receipt.Receipt.Sender != msg.Recipient || receipt.Receipt.Recipient != msg.Sender || receipt.Receipt.Status != protocol.ReceiptStatusDelivered {
				t.Fatalf("wrong duplicate receipt: %+v", receipt)
			}
			f.svc.WaitBackground()
			f.svc.gossipMu.RLock()
			count := len(f.svc.topics[msg.Topic])
			f.svc.gossipMu.RUnlock()
			if count != 0 {
				t.Fatalf("durable duplicate re-entered backlog: %d", count)
			}
			after, found, err := f.store.store.EntryByRef(context.Background(), ref)
			if err != nil || !found || after != before {
				t.Fatalf("duplicate changed durable row: before=%+v after=%+v found=%v err=%v", before, after, found, err)
			}
		})
	}
	if rows := f.store.rows("dm", domain.PeerIdentityFromWire(msg.Sender)); len(rows) != 1 {
		t.Fatalf("duplicate appended rows: %+v", rows)
	}
	if events := f.announcements(t); len(events) != 0 {
		t.Fatalf("durable duplicates emitted arrivals: %v", events)
	}
}

func TestBloomWithoutPositiveWitnessCannotAcceptDeferredStore(t *testing.T) {
	f := newCrossPathFixture(t)
	f.svc.messageStore = staticResultMessageStore{result: StoreDeferred}
	f.svc.router = NewMockRouter(t)
	msg := incomingMessage{
		ID: "8f3f0bea-8067-4ad9-a7c2-471c86c88a5e", Topic: "dm", Sender: f.sender.Address,
		Recipient: f.svc.Address(), Flag: protocol.MessageFlagImmutable, CreatedAt: time.Now().UTC(),
		Body: sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey)),
	}
	// A Bloom hit alone carries no exact accepted-row or backlog witness.
	f.svc.gossipMu.Lock()
	f.svc.seen.Add(admissionRef(msg).Key())
	f.svc.gossipMu.Unlock()

	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	core := newTestNetCore(t, netcore.ConnID(7311), client, netcore.Outbound, netcore.Options{})
	core.SetProtocolVersion(domain.ProtocolVersion(config.ProtocolVersion))
	f.svc.peerMu.Lock()
	f.svc.setTestConnEntryLocked(client, &connEntry{core: core})
	f.svc.peerMu.Unlock()
	f.svc.gossipMu.Lock()
	f.svc.subs[msg.Sender] = map[string]*subscriber{"observer": {id: "observer", recipient: msg.Sender, connID: core.ConnID()}}
	f.svc.gossipMu.Unlock()
	readDone := make(chan struct{})
	output := make(chan string, 1)
	go func() {
		defer close(readDone)
		if line, _ := bufio.NewReader(server).ReadString('\n'); len(line) != 0 {
			output <- line
		}
	}()
	accepted, count, code := f.svc.storeIncomingMessage(msg, true)
	f.svc.WaitBackground()
	// Join the actual reader after every producer has finished, so the
	// no-receipt assertion does not depend on a short observation sleep.
	_ = server.Close()
	_ = client.Close()
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("receipt observer did not join")
	}
	if accepted || count != 0 || code != protocol.ErrCodeStoreDeferred || shouldAckOnStoreResult(accepted, code) {
		t.Fatalf("unwitnessed Bloom hit accepted deferred store: accepted=%v count=%d code=%q", accepted, count, code)
	}
	select {
	case line := <-output:
		t.Fatalf("unwitnessed Bloom hit emitted a frame: %q", line)
	default:
	}
	f.svc.gossipMu.RLock()
	backlog := len(f.svc.topics[msg.Topic])
	f.svc.gossipMu.RUnlock()
	if backlog != 0 {
		t.Fatalf("deferred store entered backlog: %d", backlog)
	}
	if rows := f.store.rows("dm", domain.PeerIdentityFromWire(msg.Sender)); len(rows) != 0 {
		t.Fatalf("deferred store added a row: %+v", rows)
	}
	if events := f.announcements(t); len(events) != 0 {
		t.Fatalf("deferred store emitted arrivals: %v", events)
	}
}

package node

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// attachPushObserver registers a live subscriber for recipient over a
// real net.Pipe and returns a reader on the remote end, so frames pushed
// through writePushFrame become observable to the test. The connection
// advertises the current protocol version.
func attachPushObserver(t *testing.T, svc *Service, recipient string, connID netcore.ConnID) *bufio.Reader {
	t.Helper()
	return attachPushObserverWithVersion(t, svc, recipient, connID, config.ProtocolVersion)
}

// attachPushObserverWithVersion is attachPushObserver with an explicit
// advertised protocol version — used by the version-gate tests.
func attachPushObserverWithVersion(t *testing.T, svc *Service, recipient string, connID netcore.ConnID, version int) *bufio.Reader {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	core := netcore.New(connID, clientConn, netcore.Outbound, netcore.Options{})
	core.SetProtocolVersion(domain.ProtocolVersion(version))
	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientConn, &connEntry{core: core})
	svc.peerMu.Unlock()
	svc.gossipMu.Lock()
	if svc.subs[recipient] == nil {
		svc.subs[recipient] = make(map[string]*subscriber)
	}
	svc.subs[recipient]["observer"] = &subscriber{id: "observer", recipient: recipient, connID: core.ConnID()}
	svc.gossipMu.Unlock()
	return bufio.NewReader(serverConn)
}

// readPushedFrame reads frames from the observer until one of the wanted
// type arrives or the deadline passes.
func readPushedFrame(t *testing.T, reader *bufio.Reader, frameType string, deadline time.Duration) protocol.Frame {
	t.Helper()
	done := make(chan protocol.Frame, 1)
	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			frame, err := protocol.ParseFrameLine(line[:len(line)-1])
			if err != nil {
				continue
			}
			if frame.Type == frameType {
				done <- frame
				return
			}
		}
	}()
	select {
	case frame := <-done:
		return frame
	case <-time.After(deadline):
		t.Fatalf("timed out waiting for %s", frameType)
		return protocol.Frame{}
	}
}

// TestDuplicateInboundDMResendsDeliveredReceipt pins the receiver half of
// the sender-owned end-to-end retry contract. Scenario: the DM was
// delivered, but the delivered receipt got lost on the way back, so the
// sender retries with the SAME MessageID. The receiver must:
//
//   - dedupe silently — no second message.new event (no beep / unread);
//   - RE-SEND the delivered receipt — otherwise the sender can never
//     observe delivery and retries forever.
//
// This exercises the bloom-dedup duplicate path in storeIncomingMessage
// (s.seen.Has fires on the retry because the first arrival added the ID).
func TestDuplicateInboundDMResendsDeliveredReceipt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)
	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))

	// newTestService passes a nil bus; install a real one so message.new
	// publications are observable.
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	var newMessageEvents atomic.Int64
	bus.Subscribe(ebus.TopicMessageNew, func(protocol.LocalChangeEvent) {
		newMessageEvents.Add(1)
	})

	reader := attachPushObserver(t, svc, sender.Address, netcore.ConnID(7301))

	msg := incomingMessage{
		ID:        "dup-receipt-1",
		Topic:     "dm",
		Sender:    sender.Address,
		Recipient: svc.Address(),
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}

	stored, _, errCode := svc.storeIncomingMessage(msg, true)
	if !stored || errCode != "" {
		t.Fatalf("first arrival must be stored, got stored=%v errCode=%q", stored, errCode)
	}
	first := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if first.Receipt == nil || first.Receipt.MessageID != "dup-receipt-1" || first.Receipt.Status != protocol.ReceiptStatusDelivered {
		t.Fatalf("unexpected first receipt frame: %#v", first)
	}

	// The retry: same MessageID, deduped by s.seen.
	stored, _, errCode = svc.storeIncomingMessage(msg, true)
	if stored || errCode != "" {
		t.Fatalf("duplicate must be deduped, got stored=%v errCode=%q", stored, errCode)
	}
	second := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if second.Receipt == nil || second.Receipt.MessageID != "dup-receipt-1" || second.Receipt.Status != protocol.ReceiptStatusDelivered {
		t.Fatalf("unexpected re-sent receipt frame: %#v", second)
	}

	// The dedupe must stay silent on the UI side: exactly one
	// message.new for the two arrivals. The ebus handler is async — wait
	// for the first event, then give an erroneous second one a grace
	// window to show up before asserting.
	svc.WaitBackground()
	deadline := time.Now().Add(2 * time.Second)
	for newMessageEvents.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	if got := newMessageEvents.Load(); got != 1 {
		t.Fatalf("expected exactly 1 message.new event, got %d", got)
	}
}

// TestStoreDuplicateResendsDeliveredReceipt covers the chatlog-duplicate
// path: after a restart the bloom dedup is empty, but the message is
// already in chatlog, so MessageStore reports StoreDuplicate. The receiver
// must not emit message.new, yet must re-send the delivered receipt for the
// retrying sender.
func TestStoreDuplicateResendsDeliveredReceipt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)
	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))

	svc.messageStore = staticResultMessageStore{result: StoreDuplicate}

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	var newMessageEvents atomic.Int64
	bus.Subscribe(ebus.TopicMessageNew, func(protocol.LocalChangeEvent) {
		newMessageEvents.Add(1)
	})

	reader := attachPushObserver(t, svc, sender.Address, netcore.ConnID(7302))

	msg := incomingMessage{
		ID:        "dup-receipt-2",
		Topic:     "dm",
		Sender:    sender.Address,
		Recipient: svc.Address(),
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}

	stored, _, errCode := svc.storeIncomingMessage(msg, true)
	if !stored || errCode != "" {
		t.Fatalf("StoreDuplicate arrival reports stored=%v errCode=%q", stored, errCode)
	}

	receipt := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if receipt.Receipt == nil || receipt.Receipt.MessageID != "dup-receipt-2" || receipt.Receipt.Status != protocol.ReceiptStatusDelivered {
		t.Fatalf("unexpected receipt frame: %#v", receipt)
	}

	svc.WaitBackground()
	time.Sleep(300 * time.Millisecond)
	if got := newMessageEvents.Load(); got != 0 {
		t.Fatalf("StoreDuplicate must not emit message.new, got %d events", got)
	}
}

// TestBloomDupClearedFromBacklogStillResendsReceipt pins the durable-store
// arm of the bloom-dedup duplicate path: the envelope was delivered, then
// cleared from the runtime backlog (s.topics) by a same-identity
// subscriber's ack_delete — so the in-memory presence check fails — but the
// chatlog still has it. The retry must fall through to the MessageStore,
// get StoreDuplicate from the chatlog primary key, and re-send the
// delivered receipt instead of silently returning message_known.
func TestBloomDupClearedFromBacklogStillResendsReceipt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)
	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))

	store := &sequenceMessageStore{results: []StoreResult{StoreInserted, StoreDuplicate}}
	svc.messageStore = store

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	var newMessageEvents atomic.Int64
	bus.Subscribe(ebus.TopicMessageNew, func(protocol.LocalChangeEvent) {
		newMessageEvents.Add(1)
	})

	reader := attachPushObserver(t, svc, sender.Address, netcore.ConnID(7303))

	msg := incomingMessage{
		ID:        "dup-receipt-3",
		Topic:     "dm",
		Sender:    sender.Address,
		Recipient: svc.Address(),
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}

	stored, _, errCode := svc.storeIncomingMessage(msg, true)
	if !stored || errCode != "" {
		t.Fatalf("first arrival must be stored, got stored=%v errCode=%q", stored, errCode)
	}
	first := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if first.Receipt == nil || first.Receipt.MessageID != "dup-receipt-3" {
		t.Fatalf("unexpected first receipt frame: %#v", first)
	}

	// Simulate the ack_delete cleanup that removes the envelope from the
	// runtime backlog while the chatlog row survives.
	if removed := svc.deleteBacklogMessageForRecipient(svc.Address(), "dup-receipt-3"); removed != 1 {
		t.Fatalf("expected 1 envelope removed from backlog, got %d", removed)
	}

	// The retry: bloom hit + absent from topics + durable store present →
	// fall through to chatlog, which reports StoreDuplicate.
	stored, _, errCode = svc.storeIncomingMessage(msg, true)
	if !stored || errCode != "" {
		t.Fatalf("retry must be accepted via the durable-store arm, got stored=%v errCode=%q", stored, errCode)
	}
	second := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if second.Receipt == nil || second.Receipt.MessageID != "dup-receipt-3" || second.Receipt.Status != protocol.ReceiptStatusDelivered {
		t.Fatalf("unexpected re-sent receipt frame: %#v", second)
	}

	svc.WaitBackground()
	time.Sleep(300 * time.Millisecond)
	if got := newMessageEvents.Load(); got != 1 {
		t.Fatalf("expected exactly 1 message.new event across both arrivals, got %d", got)
	}
}

// staticResultMessageStore is a MessageStore stub that always reports the
// configured StoreResult and accepts every receipt update.
type staticResultMessageStore struct{ result StoreResult }

func (s staticResultMessageStore) StoreMessage(protocol.Envelope, bool) StoreResult { return s.result }
func (s staticResultMessageStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool {
	return true
}

// sequenceMessageStore is a MessageStore stub that replays the configured
// StoreResult sequence (last result repeats once exhausted) and accepts
// every receipt update.
type sequenceMessageStore struct {
	mu      sync.Mutex
	results []StoreResult
	calls   int
}

func (s *sequenceMessageStore) StoreMessage(protocol.Envelope, bool) StoreResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.calls
	if idx >= len(s.results) {
		idx = len(s.results) - 1
	}
	s.calls++
	return s.results[idx]
}

func (s *sequenceMessageStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }

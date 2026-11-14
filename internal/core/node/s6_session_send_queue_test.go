package node

import (
	"bufio"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestS6SessionSendQueueObservedServePreservesFramesAndCut(t *testing.T) {
	cfg, id := pendingConstructionInputs(t)
	key := pendingConstructionKey()
	s, readouts, err := NewServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &key, SessionSendQueueDepth: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.WaitBackground)
	address := domain.PeerAddress("127.0.0.1:64646")
	session, wire := newQueueTestSession(t, address, 2)
	session.connID = 1
	s.observeSessionSendQueue(session)
	s.peerMu.Lock()
	s.sessions[address] = session
	s.peerMu.Unlock()
	t.Cleanup(func() { _ = session.Close() })

	for _, id := range []string{"first", "second"} {
		if !session.enqueueSend(legacyPeerSendItem(protocol.Frame{Type: "relay_hop_ack", ID: id})) {
			t.Fatalf("refused %s", id)
		}
	}
	if session.enqueueSend(legacyPeerSendItem(protocol.Frame{Type: "relay_hop_ack", ID: "overflow"})) {
		t.Fatal("accepted overflow")
	}
	before := readouts.Pending.SessionSendQueue(1)
	if before.Depth != 2 || before.Enqueued != 2 || before.Taken != 0 || before.UnknownReason != "" {
		t.Fatalf("before serve: %+v", before)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.servePeerSession(ctx, session) }()
	if err := wire.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(wire)
	for _, id := range []string{"first", "second"} {
		raw, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var frame protocol.Frame
		if err := json.Unmarshal(raw, &frame); err != nil || frame.ID != id {
			t.Fatalf("wire frame for %s = %q (%v)", id, raw, err)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve loop did not exit")
	}
	final := readouts.Pending.SessionSendQueue(1)
	if final.Instance != before.Instance || final.Depth != 0 || final.Taken != 2 ||
		final.Enqueued != 2 || final.Drained != 0 || !final.Stopped || final.UnknownReason != "" {
		t.Fatalf("after serve: %+v", final)
	}
	if other := readouts.Pending.SessionSendQueue(2); other.UnknownReason != "session_not_registered" {
		t.Fatalf("unrelated ConnID acquired evidence: %+v", other)
	}
}

func TestS6SessionSendQueueClosesBeforeServeAndRejectsLateWork(t *testing.T) {
	s := &Service{observeSessionSendDepth: true}
	session, _ := newQueueTestSession(t, "127.0.0.1:64646", 2)
	s.observeSessionSendQueue(session)
	for range 2 {
		if !session.enqueueSend(legacyPeerSendItem(protocol.Frame{Type: "ping"})) {
			t.Fatal("refused queued frame")
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	session.closeSendQueue()
	if session.enqueueSend(legacyPeerSendItem(protocol.Frame{Type: "ping"})) {
		t.Fatal("accepted late frame")
	}
	if _, _, ok := session.takeObservedSend(); ok {
		t.Fatal("stale readiness consumed a frame")
	}
	final := session.sendQueueObservation()
	if !final.Stopped || final.Depth != 0 || final.Enqueued != 2 || final.Taken != 0 ||
		final.Drained != 2 || final.UnknownReason != "" {
		t.Fatalf("terminal drain: %+v", final)
	}
	if next := session.sendQueueObservation(); next != final {
		t.Fatalf("final cut advanced: before=%+v after=%+v", final, next)
	}
}

package node

import (
	"bufio"
	"context"
	"io"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestS6SessionInboxQueueCountsRequestAndServeReaders(t *testing.T) {
	cfg, identity := pendingConstructionInputs(t)
	key := pendingConstructionKey()
	s, readouts, err := NewServiceWithObservations(cfg, identity, nil, ObservationOptions{Pending: &key, SessionInboxQueueDepth: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.WaitBackground)
	address := domain.PeerAddress("127.0.0.1:64646")
	session, wire := newQueueTestSession(t, address, 2)
	session.connID = 1
	s.observeSessionInboxQueue(session)
	s.peerMu.Lock()
	s.sessions[address] = session
	s.health[address] = &peerHealth{Connected: true}
	s.peerMu.Unlock()
	t.Cleanup(func() { _ = session.Close() })
	if err := wire.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wireReader := bufio.NewReader(wire)
	readWire := func(want string) {
		t.Helper()
		raw, err := wireReader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		got, err := protocol.ParseFrameLine(raw)
		if err != nil || got.Type != want {
			t.Fatalf("wire type %q, want %q (%v)", got.Type, want, err)
		}
	}
	writeWire := func(kind string) {
		t.Helper()
		if _, err := io.WriteString(wire, `{"type":"`+kind+`"}`+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		s.readPeerSession(bufio.NewReader(session.conn), session)
	}()
	requestDone := make(chan error, 1)
	go func() {
		_, err := s.peerSessionRequest(session, protocol.Frame{Type: "ping"}, "pong")
		requestDone <- err
	}()
	readWire("ping")
	writeWire("pong")
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("request did not consume the reply")
	}
	first := readouts.Pending.SessionInboxQueue(1)
	if first.Enqueued != 1 || first.Taken != 1 || first.Depth != 0 || first.ReaderDone || first.UnknownReason != "" {
		t.Fatalf("request cut: %+v", first)
	}

	serveDone := make(chan error, 1)
	go func() { serveDone <- s.servePeerSession(ctx, session) }()
	writeWire("ping")
	readWire("pong")
	second := readouts.Pending.SessionInboxQueue(1)
	if second.Instance != first.Instance || second.Enqueued != 2 || second.Taken != 2 ||
		second.Depth != 0 || second.ReaderDone || second.UnknownReason != "" {
		t.Fatalf("serve cut: %+v", second)
	}
	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve loop did not stop")
	}
	writeWire("ping") // The socket reader may fill the inbox after the serve loop stops.
	if err := wire.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("socket reader did not stop")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	residue := readouts.Pending.SessionInboxQueue(1)
	if residue.Instance != second.Instance || !residue.ReaderDone || residue.Stopped ||
		residue.Enqueued != 3 || residue.Taken != 2 || residue.Depth != 1 || residue.UnknownReason != "" {
		t.Fatalf("unread residue lost: %+v", residue)
	}
}

func TestS6SessionInboxQueuePreservesReaderGrowthAfterBufferedTake(t *testing.T) {
	s := &Service{observeSessionInboxDepth: true}
	session := &peerSession{inboxCh: make(chan protocol.Frame, 1)}
	s.observeSessionInboxQueue(session)
	frame := protocol.Frame{Type: "pong"}
	if !session.offerInbox(frame) || session.offerInbox(frame) {
		t.Fatal("expected one accepted frame and one overflow")
	}
	session.finishInboxReader()
	before := session.inboxQueueObservation()
	if got, ok := session.takeObservedInbox(); !ok || got.Type != "pong" {
		t.Fatalf("buffered take = %+v, %v", got, ok)
	}
	after := session.inboxQueueObservation()
	if !after.ReaderDone || after.Depth != 0 || after.Taken != 1 || after.Enqueued != 1 ||
		after.UnknownReason != "" || after.Growth != before.Growth || session.offerInbox(frame) {
		t.Fatalf("post-reader cut: before=%+v after=%+v", before, after)
	}
}

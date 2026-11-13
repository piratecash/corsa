package node

import (
	"bufio"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestS6SessionQueueHistoryWaitsForReaderAndConsumer(t *testing.T) {
	cfg, identity := pendingConstructionInputs(t)
	key := pendingConstructionKey()
	s, readouts, err := NewServiceWithObservations(cfg, identity, nil, ObservationOptions{
		Pending: &key, SessionQueueHistoryCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.WaitBackground)
	ps, _ := newQueueTestSession(t, "127.0.0.1:64646", 2)
	ps.connID = 7
	s.observeSessionQueues(ps)
	ps.beginSessionQueueUse(false)
	ps.beginSessionQueueUse(true)
	if !ps.offerInbox(protocol.Frame{Type: "ping"}) || !ps.offerInbox(protocol.Frame{Type: "pong"}) {
		t.Fatal("fixture did not fill inbox")
	}
	checkPending := func(closed, readerDone bool) {
		t.Helper()
		cut := readouts.Pending.SessionQueueHistory()
		if cut.UnknownReason != "" || cut.Born != 1 || cut.Pending != 1 || len(cut.Rows) != 1 {
			t.Fatalf("early retirement: %+v", cut)
		}
		row := cut.Rows[0]
		if row.ConnID != 7 || row.Retired || row.Closed != closed || !row.ReaderStarted ||
			row.Inbox.ReaderDone != readerDone || row.Inbox.Depth != 2 {
			t.Fatalf("live cut: %+v", row)
		}
	}
	checkPending(false, false)
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	checkPending(true, false) // Reader was reserved before Close and has yet to run.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		s.readPeerSession(bufio.NewReader(ps.conn), ps)
	}()
	select {
	case <-readerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not return from closed transport")
	}
	checkPending(true, true) // The consumer still owns buffered frames.
	if got, ok := ps.takeObservedInbox(); !ok || got.Type != "ping" {
		t.Fatalf("post-close buffered take = %+v, %v", got, ok)
	}
	ps.endSessionQueueUse()
	cut := readouts.Pending.SessionQueueHistory()
	if cut.UnknownReason != "" || cut.Born != 1 || cut.Pending != 0 || len(cut.Rows) != 1 {
		t.Fatalf("final history: %+v", cut)
	}
	row := cut.Rows[0]
	if row.ConnID != 7 || !row.Closed || !row.Retired || !row.Send.Stopped || !row.ReaderStarted || !row.Inbox.ReaderDone ||
		row.Inbox.Enqueued != 2 || row.Inbox.Taken != 1 || row.Inbox.Depth != 1 {
		t.Fatalf("final queue accounting: %+v", row)
	}
	cut.Rows[0].Inbox.Depth = 0
	again := readouts.Pending.SessionQueueHistory()
	if len(again.Rows) != 1 || again.Rows[0] != row {
		t.Fatalf("history did not keep the original cut: %+v", again)
	}
	if s.sessionQueueHistory.sessions[ps.connID] != nil {
		t.Fatal("retirement retained the live session")
	}
	next := &peerSession{connID: 3}
	s.observeSessionQueues(next)
	t.Cleanup(func() { _ = next.Close() })
	mixed := readouts.Pending.SessionQueueHistory()
	if mixed.UnknownReason != "" || mixed.Born != 2 || mixed.Pending != 1 || len(mixed.Rows) != 2 ||
		mixed.Rows[0].ConnID != 3 || mixed.Rows[0].Retired || mixed.Rows[1] != row {
		t.Fatalf("mixed population: %+v", mixed)
	}
}

func TestS6SessionQueueHistoryBoundsUnpublishedPopulation(t *testing.T) {
	s := &Service{
		observeSessionSendDepth: true, observeSessionInboxDepth: true,
		sessionQueueHistory: newSessionQueueHistory(MaxSessionQueueHistory),
	}
	o := &PendingObservation{service: s}
	for i := MaxSessionQueueHistory; i > 0; i-- {
		ps := &peerSession{connID: domain.ConnID(i), inboxCh: make(chan protocol.Frame, 1)}
		s.observeSessionQueues(ps)
		// Setup failed before starting its reader or publishing a session.
		if err := ps.Close(); err != nil {
			t.Fatal(err)
		}
	}
	cut := o.SessionQueueHistory()
	if cut.UnknownReason != "" || cut.Pending != 0 || cut.Born != MaxSessionQueueHistory || len(cut.Rows) != MaxSessionQueueHistory {
		t.Fatalf("unpublished history: born=%d pending=%d rows=%d reason=%q", cut.Born, cut.Pending, len(cut.Rows), cut.UnknownReason)
	}
	for i, row := range cut.Rows {
		if row.ConnID != domain.ConnID(i+1) || !row.Retired || row.ReaderStarted || row.Inbox.ReaderDone ||
			row.Inbox.Depth != 0 || !row.Send.Stopped {
			t.Fatalf("unstarted reader row: %+v", row)
		}
	}
	s.observeSessionQueues(&peerSession{connID: MaxSessionQueueHistory + 1})
	cut = o.SessionQueueHistory()
	if cut.UnknownReason != "history_capacity_exhausted" || cut.Born != MaxSessionQueueHistory || len(cut.Rows) != 0 {
		t.Fatalf("exhaustion did not invalidate coverage: %+v", cut)
	}
}

func TestS6SessionQueueHistoryRejectsReusedID(t *testing.T) {
	s := &Service{sessionQueueHistory: newSessionQueueHistory(2)}
	s.observeSessionQueues(&peerSession{connID: 1})
	s.observeSessionQueues(&peerSession{connID: 1})
	cut := (&PendingObservation{service: s}).SessionQueueHistory()
	if cut.Born != 1 || cut.UnknownReason != "session_identity_collision" || len(cut.Rows) != 0 {
		t.Fatalf("reused ID was accepted: %+v", cut)
	}
	if s.sessionQueueHistory.sessions[1] != nil {
		t.Fatal("failed history retained a live session")
	}
}

package node

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func writerQueueService(t *testing.T, capacity int) (*Service, *PendingObservation) {
	t.Helper()
	cfg, id := pendingConstructionInputs(t)
	key := pendingConstructionKey()
	s, readouts, err := NewServiceWithObservations(cfg, id, nil, ObservationOptions{
		Pending: &key, WriterQueueHistoryCapacity: capacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(10*time.Second))
	return s, readouts.Pending
}

func TestS6WriterQueueHistoryRetainsJoinedWriters(t *testing.T) {
	s, observation := writerQueueService(t, 2)
	initial := observation.WriterQueueHistory()
	if initial.UnknownReason != "" || initial.Instance == ([16]byte{}) || initial.Registered != 0 || len(initial.Rows) != 0 {
		t.Fatalf("initial history = %+v", initial)
	}
	in, inPeer := net.Pipe()
	t.Cleanup(func() { _ = inPeer.Close(); _ = in.Close() })
	if err := s.registerInboundConn(in, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.unregisterInboundConn(in) })
	out, outPeer := net.Pipe()
	t.Cleanup(func() { _ = outPeer.Close(); _ = out.Close() })
	session := &peerSession{conn: out, connID: s.allocateConnID("writer-history-fixture", "")}
	t.Cleanup(func() { _ = session.Close() })
	if err := s.attachOutboundNetCore(session); err != nil {
		t.Fatal(err)
	}
	if status := session.netCore.SendRaw([]byte("frame")); status != netcore.SendOK {
		t.Fatalf("outbound send = %v", status)
	}
	if err := outPeer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(outPeer, make([]byte, len("frame"))); err != nil {
		t.Fatal(err)
	}
	live := observation.WriterQueueHistory()
	if live.UnknownReason != "" || live.Registered != 2 || len(live.Rows) != 2 ||
		live.Rows[0].Direction != domain.PeerDirectionInbound || live.Rows[1].Direction != domain.PeerDirectionOutbound ||
		live.Rows[1].Retired || live.Rows[1].Queue.Enqueued != 1 || live.Rows[1].Queue.Taken != 1 {
		t.Fatalf("active history = %+v", live)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	s.unregisterInboundConn(in)
	end := observation.WriterQueueHistory()
	if end.UnknownReason != "" || end.Registered != 2 || len(end.Rows) != 2 || end.Instance != initial.Instance {
		t.Fatalf("retired history = %+v", end)
	}
	for i, row := range end.Rows {
		before := live.Rows[i]
		if !row.Retired || !row.Queue.WriterExited || row.ConnID != before.ConnID || row.BirthAt != before.BirthAt ||
			row.Queue.Instance != before.Queue.Instance || row.Queue.At < before.Queue.At || row.Queue.Depth != 0 {
			t.Fatalf("retired row = %+v, live row = %+v", row, before)
		}
	}
	end.Rows[1].Queue.Enqueued = 0
	if got := observation.WriterQueueHistory().Rows[1].Queue.Enqueued; got != 1 {
		t.Fatalf("consumer changed retired queue count to %d", got)
	}
	// A later successful connection still works when the diagnostic history is full.
	extra, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close(); _ = extra.Close() })
	if err := s.registerInboundConn(extra, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.unregisterInboundConn(extra) })
	if got := observation.WriterQueueHistory(); got.UnknownReason != "history_capacity_exhausted" || got.Rows != nil {
		t.Fatalf("exhausted source claimed complete rows: %+v", got)
	}
}

func TestS6WriterQueueHistoryRejectsUnjoinedRetirement(t *testing.T) {
	s, observation := writerQueueService(t, 1)
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close(); _ = conn.Close() })
	session := &peerSession{conn: conn, connID: s.allocateConnID("writer-history-fixture", "")}
	t.Cleanup(func() { _ = session.Close() })
	if err := s.attachOutboundNetCore(session); err != nil {
		t.Fatal(err)
	}
	s.peerMu.Lock()
	s.unregisterConnLocked(conn) // Deliberately omit the owner join.
	s.peerMu.Unlock()
	if got := observation.WriterQueueHistory(); got.UnknownReason != "writer_final_cut_unavailable" || got.Rows != nil {
		t.Fatalf("unjoined writer claimed retirement: %+v", got)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if got := observation.WriterQueueHistory(); got.UnknownReason != "writer_final_cut_unavailable" {
		t.Fatalf("late close replaced lost history: %+v", got)
	}
}

func TestS6WriterQueueHistoryDefaultAndInvalidCapacity(t *testing.T) {
	_, observation := writerQueueService(t, 0)
	if got := observation.WriterQueueHistory(); got.UnknownReason != "source_unavailable" || got.Instance != ([16]byte{}) {
		t.Fatalf("default source = %+v", got)
	}
	cfg, id := pendingConstructionInputs(t)
	key := pendingConstructionKey()
	for _, options := range []ObservationOptions{
		{WriterQueueHistoryCapacity: 1}, {Pending: &key, WriterQueueHistoryCapacity: -1},
		{Pending: &key, WriterQueueHistoryCapacity: MaxWriterQueueHistory + 1},
	} {
		if s, _, err := NewServiceWithObservations(cfg, id, nil, options); s != nil || err == nil {
			t.Fatalf("invalid options accepted: %+v", options)
		}
	}
}

func TestS6WriterQueueObservationPreservesOriginalTransportAdmission(t *testing.T) {
	for _, mode := range []struct {
		name     string
		capacity int
	}{
		{"ordinary", 0},
		{"observed", 2},
	} {
		for _, direction := range []struct {
			name    string
			inbound bool
		}{
			{"inbound", true},
			{"outbound", false},
		} {
			t.Run(mode.name+"/"+direction.name, func(t *testing.T) {
				svc, observation := writerQueueService(t, mode.capacity)
				svc.runCtx = t.Context()
				old, dispatchOld := newApplicationReceiveTransport(t, svc, direction.inbound, 5960, "192.0.2.107:64646")
				live, dispatchLive := newApplicationReceiveTransport(t, svc, direction.inbound, 5961, "192.0.2.108:64646")
				if old.ConnID() == live.ConnID() {
					t.Fatal("fixture reused the original connection")
				}
				if mode.capacity > 0 {
					if history := observation.WriterQueueHistory(); history.UnknownReason != "" || history.Registered != 2 {
						t.Fatalf("writer history is unavailable: %+v", history)
					}
				} else if history := observation.WriterQueueHistory(); history.UnknownReason != "source_unavailable" {
					t.Fatalf("writer history is enabled by default: %+v", history)
				}

				old.ShutSendQueue()
				if svc.netCoreForID(old.ConnID()) != old || !live.AcceptingFrames() {
					t.Fatal("fixture requires an old registered core and a live replacement")
				}
				notice := protocol.Frame{Type: "push_notice", TTLSeconds: 30, Ciphertext: "writer-queue-observation"}
				dispatchOld(notice)
				old.Close()
				if svc.netCoreForID(old.ConnID()) != old {
					t.Fatal("writer join unexpectedly removed the test registration")
				}
				dispatchOld(notice)
				if got := svc.fetchNoticesFrame().Count; got != 0 {
					t.Fatalf("closed original stored %d notices", got)
				}
				dispatchLive(notice)
				if got := svc.fetchNoticesFrame().Count; got != 1 {
					t.Fatalf("live transport stored %d notices, want 1", got)
				}
			})
		}
	}
}

package node

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// enqueue_writer_done_test.go pins the two fire-and-forget enqueue paths
// against netcore.SendWriterDone.
//
// SendWriterDone is a NORMAL outcome, not a programming error: netcore shuts
// the queue the instant a socket write fails, so every frame that arrives
// afterwards on a dying connection gets it. Landing in the `default:` branch
// makes each of those frames print an ERROR line — a log storm at precisely
// the moment the operator needs to read the logs — and it invites a second
// Close on a connection that is already tearing itself down.

// failedNetCore builds a NetCore whose socket write has already failed, so the
// send queue is shut and every further enqueue answers SendWriterDone.
func failedNetCore(t *testing.T) (*netcore.NetCore, net.Conn) {
	t.Helper()

	local, remote := net.Pipe()
	core := netcore.New(netcore.ConnID(7), local, netcore.Outbound, netcore.Options{})
	// Closing both ends makes the writer's first Write fail, which is what
	// raises the "this link is finished" gate inside netcore.
	_ = remote.Close()
	_ = local.Close()

	frame := protocol.Frame{Type: "push_message", ID: "kill-the-writer"}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if st := core.Send(frame); st == netcore.SendWriterDone {
			return core, local
		}
		if time.Now().After(deadline) {
			t.Fatal("the netcore writer never reported SendWriterDone")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEnqueueFrameByIDHandlesWriterDone(t *testing.T) {
	// NOT parallel: it inspects the global zerolog output.
	var buf syncWriter
	origLogger := log.Logger
	log.Logger = zerolog.New(&buf).With().Logger()
	defer func() { log.Logger = origLogger }()

	svc := newTestService(t, config.NodeTypeFull)
	core, conn := failedNetCore(t)
	defer func() { _ = conn.Close() }()

	id := domain.ConnID(7)
	svc.peerMu.Lock()
	svc.conns[id] = &connEntry{core: core}
	svc.peerMu.Unlock()

	if got := svc.enqueueFrameByID(id, []byte("{\"type\":\"ping\"}\n")); got != enqueueDropped {
		t.Fatalf("enqueueFrameByID = %s, want dropped", got)
	}
	if logged := buf.String(); strings.Contains(logged, "unexpected netcore.SendStatus") {
		t.Fatalf("a dying connection produced an error log: %s", logged)
	}
}

func TestEnqueueSessionFrameHandlesWriterDone(t *testing.T) {
	// NOT parallel: it inspects the global zerolog output.
	var buf syncWriter
	origLogger := log.Logger
	log.Logger = zerolog.New(&buf).With().Logger()
	defer func() { log.Logger = origLogger }()

	svc := newTestService(t, config.NodeTypeFull)
	core, conn := failedNetCore(t)
	defer func() { _ = conn.Close() }()

	session := &peerSession{
		address:      domain.PeerAddress("10.0.0.77:4242"),
		conn:         conn,
		netCore:      core,
		sendCh:       make(chan peerSendItem, 1),
		inboxCh:      make(chan protocol.Frame, 1),
		errCh:        make(chan error, 1),
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	if got := svc.enqueueSessionFrame(session, []byte("{\"type\":\"ping\"}\n")); got != enqueueDropped {
		t.Fatalf("enqueueSessionFrame = %s, want dropped", got)
	}
	if logged := buf.String(); strings.Contains(logged, "unexpected netcore.SendStatus") {
		t.Fatalf("a dying connection produced an error log: %s", logged)
	}
}

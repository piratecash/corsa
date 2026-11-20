package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestLoadStandRouteWireOutboundRefusalThenActualPass(t *testing.T) {
	for _, name := range []string{"disconnected", "full-queue", "closed-queue"} {
		t.Run(name, func(t *testing.T) {
			s, o, now, queue, entries := fullSendMechanismFixture(t)
			const address domain.PeerAddress = "full-send-mechanism-peer"
			session := s.sessions[address]
			*now = 12
			switch name {
			case "disconnected":
				s.health[address].Connected = false
			case "full-queue":
				for len(queue) < cap(queue) {
					queue <- legacyPeerSendItem(protocol.Frame{Type: "sentinel", RawLine: "sentinel\n"})
				}
			case "closed-queue":
				session.sendClosed = true
			}
			if s.SendRouteAnnounceV3Full(t.Context(), address, 7, entries, domain.RouteWireFullConnect) ||
				o.incomplete || o.fullPassesPending != 0 || o.connections[1].fullSend != nil {
				t.Fatal("pre-admission refusal fabricated a supplied frame")
			}
			for len(queue) > 0 {
				if (<-queue).RawLine != "sentinel\n" {
					t.Fatal("refused frame entered the queue")
				}
			}
			s.health[address].Connected = true
			session.sendClosed = false
			if !s.SendRouteAnnounceV3Full(t.Context(), address, 7, entries, domain.RouteWireFullPeriodic) || len(queue) != 2 {
				t.Fatal("actual two-chunk periodic pass refused")
			}
			for len(queue) > 0 {
				data := o.prepareQueue(1, []byte((<-queue).RawLine), 0)
				o.startWrite(1, data)
				o.observe(1, routeWireWrite, string(data), true, 0)
			}
			*now = 21
			if err := o.captureFullReceipts(nil); err != nil {
				t.Fatal(err)
			}
			o.finish(1, routeWireRead)
			o.finish(1, routeWireWrite)
			rows, err := o.SealWindow([]RouteWireCensusEndpoint{{ConnID: 1, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: [32]byte{1}, Interval: domain.RouteWireSessionInterval{BeginBefore: 10, BeginAfter: 10, EndBefore: 20, EndAfter: 20}}})
			if err != nil || len(rows) != 1 || rows[0].FullSend == nil || rows[0].FullSend.Passes != 1 || rows[0].FullSend.PeriodicPasses != 1 || rows[0].FullSend.ConnectPasses != 0 || rows[0].FullSend.Destinations != 101 || rows[0].Sent.FullV3.Frames != 2 {
				t.Fatal("refusal changed the actual byte/pass proof", rows, err)
			}
		})
	}
}

func TestLoadStandRouteWireOutboundPartialAdmissionStaysInvalid(t *testing.T) {
	s, o, now, _, entries := fullSendMechanismFixture(t)
	const address domain.PeerAddress = "full-send-mechanism-peer"
	session := s.sessions[address]
	session.sendCh = make(chan peerSendItem, 1)
	*now = 12
	if s.SendRouteAnnounceV3Full(t.Context(), address, 7, entries, domain.RouteWireFullConnect) || !o.incomplete || o.fullPassesPending != 0 || len(session.sendCh) != 1 {
		t.Fatal("partly admitted pass was treated as zero work")
	}
	if full := o.connections[1].fullSend; full == nil || full.projection.Passes != 0 || full.offered.frames != 1 {
		t.Fatal("partial admission acquired completed-pass credit", full)
	}
}

func TestLoadStandRouteWireWriterBeforeFullIntentProof(t *testing.T) {
	_, o, now, _, entries := fullSendMechanismFixture(t)
	*now = 12
	pass := o.startFullPass(7, entries, domain.RouteWireFullPeriodic)
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 7, protocol.MaxFrameLine)
	if len(skipped) != 0 || len(chunks) != 2 {
		t.Fatal("fixture does not contain two actual chunks")
	}
	for _, chunk := range chunks {
		frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 7, chunk)
		if err != nil {
			t.Fatal(err)
		}
		data := o.prepareQueue(1, []byte(frame.RawLine), 0)
		o.startWrite(1, data)
		o.observe(1, routeWireWrite, string(data), true, 0)
		// The writer's actual completion may precede the enqueue caller's
		// observation, while this invocation still owns its pending pass.
		if o.fullPassesPending != 1 {
			t.Fatal("unfinished pass lost its pending ownership")
		}
		pass.frame(1, frame.RawLine)
	}
	pass.finish(true)
	if o.incomplete || o.fullPassesPending != 0 {
		t.Fatal("late intent callback lost an actual write")
	}
	full, ok := o.connections[1].fullSendProof(routeWireSnapshot(o.connections[1].counts[routeWireWrite]).FullV3)
	if !ok || full == nil || full.Passes != 1 || full.PeriodicPasses != 1 || full.Destinations != 101 {
		t.Fatal("late intent callback fabricated or lost the proof", full, ok)
	}
}

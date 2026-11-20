package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestLoadStandRouteWireZeroOfferThenActualCompletePass(t *testing.T) {
	s, o, now, queue, entries := fullSendMechanismFixture(t)
	const address domain.PeerAddress = "full-send-mechanism-peer"
	session := s.sessions[address]
	delete(s.sessions, address)
	*now = 12
	if s.SendRouteAnnounceV3Full(t.Context(), address, 7, entries, domain.RouteWireFullConnect) {
		t.Fatal("missing session sent")
	}
	if o.incomplete || o.fullPassesPending != 0 || len(queue) != 0 || o.connections[1].fullSend != nil {
		t.Fatal("zero-offer refusal fabricated work or lost observation")
	}
	s.sessions[address] = session
	if !s.SendRouteAnnounceV3Full(t.Context(), address, 7, entries, domain.RouteWireFullPeriodic) {
		t.Fatal("actual original full pass refused")
	}
	*now = 15
	if len(queue) != 2 {
		t.Fatal("actual two-chunk pass missing", len(queue))
	}
	for len(queue) > 0 {
		line := (<-queue).RawLine
		data := o.prepareQueue(1, []byte(line), 0)
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
	if err != nil || len(rows) != 1 || rows[0].FullSend == nil || rows[0].FullSend.Passes != 1 || rows[0].FullSend.ConnectPasses != 0 || rows[0].FullSend.PeriodicPasses != 1 || rows[0].FullSend.Destinations != 101 || rows[0].Sent.FullV3.Frames != 2 {
		t.Fatal("zero attempt changed actual proof", rows, err)
	}
}

func TestLoadStandRouteWireZeroOfferKeepsFailureGuards(t *testing.T) {
	for _, name := range []string{"prior-poison", "clock", "prebegin-crossing", "end-crossing", "bad-metadata", "bad-frame", "partial-offer", "empty-success"} {
		t.Run(name, func(t *testing.T) {
			_, o, now, _, entries := fullSendMechanismFixture(t)
			*now = 12
			if name == "prebegin-crossing" {
				*now = 9
			}
			epoch := uint64(7)
			if name == "bad-metadata" {
				epoch = 0
			}
			pass := o.startFullPass(epoch, entries, domain.RouteWireFullConnect)
			if pass == nil {
				t.Fatal("fixture pass absent")
			}
			switch name {
			case "prior-poison":
				o.incomplete = true
			case "clock":
				o.window.clock = func() (time.Duration, error) { return 0, errors.New("clock unavailable") }
			case "prebegin-crossing":
				*now = 12
			case "end-crossing":
				*now = 21
			case "bad-frame":
				pass.frame(1, "not a full frame\n")
			case "partial-offer":
				frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 7, entries[:1])
				if err != nil {
					t.Fatal(err)
				}
				pass.frame(1, frame.RawLine)
			}
			pass.finish(name == "empty-success")
			if !o.incomplete || o.fullPassesPending != 0 {
				t.Fatal("original failure guard waived", name)
			}
		})
	}
}

package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// Uses the original sender and its actual queue admission. No socket, goroutine,
// timer or limiter is substituted, and the original observer predicate remains.
func fullSendMechanismFixture(t *testing.T) (*Service, *LoadstandRouteWireObservation, *time.Duration, chan peerSendItem, []routing.AnnounceEntry) {
	t.Helper()
	s := newTestServiceWithRouting(t, idNodeA)
	now := time.Duration(1)
	window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
	if err != nil {
		t.Fatal(err)
	}
	o := &LoadstandRouteWireObservation{window: window, fullBudget: 102, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
	s.loadstandRouteWire.Store(o)
	const id domain.ConnID = 1
	o.begin(id)
	o.bind(id, idPeerB, domain.PeerDirectionOutbound, [32]byte{1})
	address := domain.PeerAddress("full-send-mechanism-peer")
	queue := make(chan peerSendItem, 16)
	s.sessions[address] = &peerSession{address: address, connID: id, peerIdentity: idPeerB, sendCh: queue, capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1}}
	s.health = map[domain.PeerAddress]*peerHealth{address: {Connected: true}}
	if err := seedLoadstandRouteWireTable(s.routingTable, idNodeA, 101); err != nil {
		t.Fatal(err)
	}
	projection := s.routingTable.AnnounceTo(idPeerB)
	entries := append([]routing.AnnounceEntry(nil), routing.BuildAnnounceSnapshot(projection).Entries...)
	s.routingTable.ReleaseAnnounceEntries(projection)
	if len(entries) != 101 {
		t.Fatal("fixture does not contain original 101-route projection", len(entries))
	}
	return s, o, &now, queue, entries
}
func fullSendMechanismDrain(t *testing.T, queue chan peerSendItem, epoch uint64, want int) {
	t.Helper()
	got := 0
	frames := 0
	for len(queue) > 0 {
		frame, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte((<-queue).RawLine))
		if err != nil || frame.Kind != protocol.RouteAnnounceV3KindFull || frame.Epoch != epoch {
			t.Fatal("original queue did not contain valid full wire frames", err)
		}
		got += len(frame.Entries)
		frames++
	}
	if got != want || frames < 2 {
		t.Fatal("complete original chunked send missing", got, frames)
	}
}
func TestLoadStandRouteWireFullSendPreWindowCrossingMechanism(t *testing.T) {
	for _, tc := range []struct {
		name            string
		start, admitted time.Duration
		incomplete      bool
	}{{"wholly-before", 9, 9, false}, {"prebegin-to-inside", 9, 12, true}, {"wholly-inside", 12, 13, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, o, now, queue, entries := fullSendMechanismFixture(t)
			*now = tc.start
			admissions := 0
			// Existing admission barrier runs after pass.frame and before the actual
			// enqueue, so the deferred finish observes precisely this later time.
			s.sendAdmissionBarrier = func() { admissions++; *now = tc.admitted }
			if !s.SendRouteAnnounceV3Full(t.Context(), "full-send-mechanism-peer", 7, entries, domain.RouteWireFullConnect) {
				t.Fatal("valid original sender rejected its queue")
			}
			if admissions < 2 {
				t.Fatal("original chunked admission path did not execute", admissions)
			}
			fullSendMechanismDrain(t, queue, 7, 101)
			if o.incomplete != tc.incomplete || o.fullPassesPending != 0 {
				t.Fatal("original full-pass boundary result changed", o.incomplete, o.fullPassesPending)
			}
			*now = 21
			err := o.captureFullReceipts(nil)
			if tc.incomplete {
				if !errors.Is(err, errRouteWireIncomplete) {
					t.Fatal("crossing did not preserve original capture refusal", err)
				}
			} else if err != nil {
				t.Fatal("control became incomplete", err)
			}
		})
	}
}
func TestLoadStandRouteWireFullSendProjectionReuseMechanism(t *testing.T) {
	for _, name := range []string{"same-projection", "caller-epoch-change", "destination-set-change"} {
		t.Run(name, func(t *testing.T) {
			s, o, now, queue, entries := fullSendMechanismFixture(t)
			*now = 12
			if !s.SendRouteAnnounceV3Full(t.Context(), "full-send-mechanism-peer", 7, entries, domain.RouteWireFullConnect) {
				t.Fatal("first valid pass not queued")
			}
			fullSendMechanismDrain(t, queue, 7, 101)
			if o.incomplete {
				t.Fatal("first pass not valid")
			}
			epoch := uint64(7)
			second := append([]routing.AnnounceEntry(nil), entries...)
			switch name {
			case "caller-epoch-change":
				epoch = 8 // Explicit valid sender API parameter, not a claim that topology changes Table.Epoch.
			case "destination-set-change":
				for _, entry := range second {
					if entry.Identity == idTargetX {
						t.Fatal("extra destination collides with seed fixture")
					}
				}
				second = append(second, routing.AnnounceEntry{Identity: idTargetX, Origin: idOriginC, Hops: 2, SeqNo: 1})
			}
			if !s.SendRouteAnnounceV3Full(t.Context(), "full-send-mechanism-peer", epoch, second, domain.RouteWireFullPeriodic) {
				t.Fatal("second individually valid pass not queued")
			}
			fullSendMechanismDrain(t, queue, epoch, len(second))
			wantIncomplete := name != "same-projection"
			if o.incomplete != wantIncomplete || o.fullPassesPending != 0 {
				t.Fatal("same-connection projection result changed", o.incomplete, o.fullPassesPending)
			}
			full := o.connections[1].fullSend
			if full == nil || full.projection.Epoch != 7 || full.projection.Destinations != 101 {
				t.Fatal("first projection replaced", full)
			}
			wantPasses := uint64(1)
			if !wantIncomplete {
				wantPasses = 2
			}
			if full.projection.Passes != wantPasses {
				t.Fatal("complete pass count changed", full.projection.Passes)
			}
		})
	}
}

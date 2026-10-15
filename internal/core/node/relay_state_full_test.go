package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestFullRelayStateStoreDoesNotUnsendAFrame: bookkeeping that fails after
// the bytes are gone cannot report that they never left.
//
// The relay-state entry buys Phase 3 hop-ack tracking: a timeout, and the
// stashed wire bytes a failover would re-emit. When the table is at
// capacity that entry is refused — and the send path used to turn that
// refusal into `relaySendRefused`, which says something entirely
// different and false: nothing went out. The caller then ran a gossip
// fallback for a frame already on the wire, never called
// confirmEnvelopeOnWire, and left the sender looking at `queued` for a
// message the peer was holding.
func TestFullRelayStateStoreDoesNotUnsendAFrame(t *testing.T) {
	t.Parallel()

	svc, peerAddr, frames := newInboundRelayFixture(t)

	// The relay-state table is full: every store from here is refused.
	svc.relayStates = newRelayStateStore()
	for i := range maxRelayStates {
		svc.relayStates.store(&relayForwardState{
			MessageID:    fmt.Sprintf("filler-%d", i),
			RemainingTTL: relayStateTTLSeconds,
		})
	}

	envelope := newInboundRelayEnvelope(t, svc, "relay-with-full-state")

	outcome := svc.sendTableRoutedRelayToAddress(context.Background(),
		domain.PeerAddress("inbound:"+peerAddr), envelope, time.Now().UTC())

	if outcome != relaySendHandedToPeer {
		t.Errorf("outcome = %v, want handed-to-peer: the frame was written before the state store was asked", outcome)
	}
	select {
	case frame := <-frames:
		if frame.ID != string(envelope.ID) {
			t.Errorf("the peer received %q, want %q", frame.ID, envelope.ID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nothing reached the peer, so this test proves nothing about what the outcome should say")
	}
	svc.WaitBackground()
}

// TestTableRoutedInboundRelayMarksRoutingTableSelection: a table-directed
// send through an inbound connection records the attempt as picked by the
// routing table, exactly like the outbound branch, so the stale hop_ack
// guard accepts an ack only from that next hop whichever direction the
// connection has.
func TestTableRoutedInboundRelayMarksRoutingTableSelection(t *testing.T) {
	t.Parallel()

	svc, peerAddr, frames := newInboundRelayFixture(t)
	svc.relayStates = newRelayStateStore()
	envelope := newInboundRelayEnvelope(t, svc, "relay-table-routed-inbound")

	outcome := svc.sendTableRoutedRelayToAddress(context.Background(),
		domain.PeerAddress("inbound:"+peerAddr), envelope, time.Now().UTC())
	if outcome != relaySendHandedToPeer {
		t.Fatalf("outcome = %v, want handed-to-peer", outcome)
	}
	select {
	case <-frames:
	case <-time.After(3 * time.Second):
		t.Fatal("nothing reached the peer")
	}

	_, selection := svc.relayStates.lookupForwardAttempt(string(envelope.ID))
	if selection != relayNextHopRoutingTable {
		t.Fatalf("NextHopSelection = %s, want %s", selection, relayNextHopRoutingTable)
	}
	svc.WaitBackground()
}

// newInboundRelayFixture builds a Service with one live inbound
// connection the relay can write to. It returns the connection's remote
// address (the relay resolves an "inbound:" address against it, which for
// a pipe is not the overlay one) and a channel of the frames the peer
// side read.
func newInboundRelayFixture(t *testing.T) (*Service, string, <-chan protocol.Frame) {
	t.Helper()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	core := newTestNetCore(t, netcore.ConnID(1), local, netcore.Inbound, netcore.Options{
		Address: domain.PeerAddress("10.0.0.1:64646"),
		Caps:    []domain.Capability{domain.CapMeshRelayV1},
	})
	peerAddr := core.RemoteAddr()
	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(local, &connEntry{core: core, tracked: true})
	svc.peerMu.Unlock()

	frames := make(chan protocol.Frame, 4)
	go func() {
		scanner := bufio.NewScanner(remote)
		scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for scanner.Scan() {
			var frame protocol.Frame
			if err := json.Unmarshal(scanner.Bytes(), &frame); err == nil {
				frames <- frame
			}
		}
	}()
	return svc, peerAddr, frames
}

// newInboundRelayEnvelope builds a DM envelope from svc to a fresh
// recipient identity.
func newInboundRelayEnvelope(t *testing.T, svc *Service, id protocol.MessageID) protocol.Envelope {
	t.Helper()
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return protocol.Envelope{
		ID: id, Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: time.Now().UTC(),
	}
}

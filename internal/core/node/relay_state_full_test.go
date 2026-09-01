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

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// A live inbound connection the relay can write to, with a reader so
	// the write completes.
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	core := netcore.New(netcore.ConnID(1), local, netcore.Inbound, netcore.Options{
		Address: domain.PeerAddress("10.0.0.1:64646"),
		Caps:    []domain.Capability{domain.CapMeshRelayV1},
	})
	// The relay resolves an "inbound:" address against the connection's
	// REMOTE address, which for a pipe is not the overlay one.
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

	// The relay-state table is full: every store from here is refused.
	svc.relayStates = newRelayStateStore()
	for i := range maxRelayStates {
		svc.relayStates.store(&relayForwardState{
			MessageID:    fmt.Sprintf("filler-%d", i),
			RemainingTTL: relayStateTTLSeconds,
		})
	}

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	envelope := protocol.Envelope{
		ID: "relay-with-full-state", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: time.Now().UTC(),
	}

	outcome := svc.sendRelayToAddress(context.Background(),
		domain.PeerAddress("inbound:"+peerAddr), envelope,
		domain.PeerIdentityFromWire(recipientID.Address), time.Now().UTC())

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

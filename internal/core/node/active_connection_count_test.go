package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestIsActiveConnectionFrame pins the shared predicate that both the
// connection LIST (getActiveConnections) and the connection COUNT
// (connection_count in getResourceUsage) filter on, so the two can never
// disagree about what "a live connection" is.
func TestIsActiveConnectionFrame(t *testing.T) {
	t.Parallel()

	base := protocol.PeerHealthFrame{
		Connected: true,
		ConnID:    42,
		State:     peerStateHealthy,
		SlotState: string(domain.SlotStateActive),
	}

	cases := []struct {
		name  string
		mutet func(f *protocol.PeerHealthFrame)
		want  bool
	}{
		{"healthy active", func(f *protocol.PeerHealthFrame) {}, true},
		{"degraded ok", func(f *protocol.PeerHealthFrame) { f.State = peerStateDegraded }, true},
		{"stalled ok", func(f *protocol.PeerHealthFrame) { f.State = peerStateStalled }, true},
		{"initializing slot ok", func(f *protocol.PeerHealthFrame) { f.SlotState = string(domain.SlotStateInitializing) }, true},
		{"empty slot ok", func(f *protocol.PeerHealthFrame) { f.SlotState = "" }, true},
		{"no PeerID still counts", func(f *protocol.PeerHealthFrame) { f.PeerID = "" }, true},

		{"not connected", func(f *protocol.PeerHealthFrame) { f.Connected = false }, false},
		{"zero conn id", func(f *protocol.PeerHealthFrame) { f.ConnID = 0 }, false},
		{"reconnecting state", func(f *protocol.PeerHealthFrame) { f.State = "reconnecting" }, false},
		{"empty state", func(f *protocol.PeerHealthFrame) { f.State = "" }, false},
		{"dialing slot", func(f *protocol.PeerHealthFrame) { f.SlotState = "dialing" }, false},
		{"queued slot", func(f *protocol.PeerHealthFrame) { f.SlotState = "queued" }, false},
	}

	for _, c := range cases {
		f := base
		c.mutet(&f)
		if got := isActiveConnectionFrame(f); got != c.want {
			t.Errorf("%s: isActiveConnectionFrame = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestActiveConnectionCount_BareFixtureZero pins that the counter is
// safe (no panic, returns 0) on a fixture without a primed peer-health
// snapshot — the same graceful-degradation path getResourceUsage relies
// on before Run() primes the hot-read snapshots.
func TestActiveConnectionCount_BareFixtureZero(t *testing.T) {
	t.Parallel()
	svc := &Service{}
	if got := svc.activeConnectionCount(); got != 0 {
		t.Fatalf("activeConnectionCount on bare fixture = %d, want 0", got)
	}
}

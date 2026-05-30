package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// announce_state_v3_epoch_test.go covers the Phase 4 per-peer
// route_announce_v3 epoch watermark on AnnouncePeerState (phase-4 §3.1,
// overview §7.1): first-sight adoption, stale rewind protection, reset
// detection on increase, and the session-boundary reset.

const idV3EpochPeer = "ee00000000000000000000000000000000000009"

func newEpochState(t *testing.T) *AnnouncePeerState {
	t.Helper()
	r := NewAnnounceStateRegistry()
	r.MarkReconnected(domain.PeerIdentity(idV3EpochPeer), nil)
	s := r.Get(domain.PeerIdentity(idV3EpochPeer))
	if s == nil {
		t.Fatalf("state must exist after MarkReconnected")
	}
	return s
}

func TestObserveV3Epoch_FirstSightAdopts(t *testing.T) {
	s := newEpochState(t)
	if got := s.ObserveV3Epoch(7); got != V3EpochApply {
		t.Fatalf("first sight: got verdict %d want V3EpochApply", got)
	}
	epoch, set := s.KnownV3Epoch()
	if !set || epoch != 7 {
		t.Fatalf("watermark after first sight: got (%d,%v) want (7,true)", epoch, set)
	}
}

func TestObserveV3Epoch_EqualApplies(t *testing.T) {
	s := newEpochState(t)
	s.ObserveV3Epoch(4)
	if got := s.ObserveV3Epoch(4); got != V3EpochApply {
		t.Fatalf("equal epoch: got verdict %d want V3EpochApply", got)
	}
}

func TestObserveV3Epoch_StaleRejectedWatermarkUnchanged(t *testing.T) {
	s := newEpochState(t)
	s.ObserveV3Epoch(10)
	if got := s.ObserveV3Epoch(3); got != V3EpochStale {
		t.Fatalf("stale epoch: got verdict %d want V3EpochStale", got)
	}
	// A single late replay must not rewind the watermark.
	if epoch, _ := s.KnownV3Epoch(); epoch != 10 {
		t.Fatalf("stale replay rewound watermark to %d, want 10", epoch)
	}
}

func TestObserveV3Epoch_IncreaseSignalsReset(t *testing.T) {
	s := newEpochState(t)
	s.ObserveV3Epoch(2)
	if got := s.ObserveV3Epoch(5); got != V3EpochReset {
		t.Fatalf("increased epoch: got verdict %d want V3EpochReset", got)
	}
	if epoch, _ := s.KnownV3Epoch(); epoch != 5 {
		t.Fatalf("watermark after reset: got %d want 5", epoch)
	}
}

func TestObserveV3Epoch_SessionBoundaryResetsWatermark(t *testing.T) {
	r := NewAnnounceStateRegistry()
	pid := domain.PeerIdentity(idV3EpochPeer)
	r.MarkReconnected(pid, nil)
	s := r.Get(pid)
	s.ObserveV3Epoch(9)

	// A disconnect/reconnect fronts a possibly-restarted peer: the
	// watermark must forget so a fresh low epoch is not misread as stale.
	r.MarkDisconnected(pid)
	r.MarkReconnected(pid, nil)
	s = r.Get(pid)
	if _, set := s.KnownV3Epoch(); set {
		t.Fatalf("watermark must be unset after session boundary")
	}
	if got := s.ObserveV3Epoch(1); got != V3EpochApply {
		t.Fatalf("post-reconnect first sight: got verdict %d want V3EpochApply", got)
	}
}

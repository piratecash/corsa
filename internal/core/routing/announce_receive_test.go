package routing

import "testing"

func TestAnnounceReceiveStateClosedAndEpochTransitions(t *testing.T) {
	for _, state := range []*AnnounceReceiveState{nil, {}} {
		state.MarkBaselineReceived()
		if state.ReceiveReady() || state.HasReceivedBaseline() || state.ObserveV3Epoch(1) != V3EpochStale {
			t.Fatal("absent owner accepted evidence")
		}
	}
	state := NewAnnounceReceiveState()
	if state.ObserveV3Epoch(1) != V3EpochApply {
		t.Fatal("first epoch rejected")
	}
	state.MarkBaselineReceived()
	if state.ObserveV3Epoch(2) != V3EpochReset || state.HasReceivedBaseline() {
		t.Fatal("epoch advance retained old full")
	}
	if state.ObserveV3Epoch(2) != V3EpochApply || state.HasReceivedBaseline() {
		t.Fatal("second delta regained full")
	}
	state.MarkBaselineReceived()
	if !state.HasReceivedBaseline() || state.ObserveV3Epoch(1) != V3EpochStale {
		t.Fatal("fresh full or stale epoch verdict lost")
	}
	state.Close()
	state.MarkBaselineReceived()
	if state.ReceiveReady() || state.HasReceivedBaseline() || state.ObserveV3Epoch(3) != V3EpochStale {
		t.Fatal("closed owner reopened")
	}
}

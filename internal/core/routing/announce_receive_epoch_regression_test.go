package routing

import "testing"

func TestObserveV3EpochAdvanceInvalidatesReceivedBaseline(t *testing.T) {
	state := newEpochState(t)
	if got := state.ObserveV3Epoch(1); got != V3EpochApply {
		t.Fatal(got)
	}
	state.MarkBaselineReceived()
	if got := state.ObserveV3Epoch(2); got != V3EpochReset {
		t.Fatal(got)
	}
	if state.HasReceivedBaseline() {
		t.Fatal("epoch2 inherited epoch1 receive baseline")
	}
	if got := state.ObserveV3Epoch(2); got != V3EpochApply {
		t.Fatal(got)
	}
	if state.HasReceivedBaseline() {
		t.Fatal("second epoch2 delta can bypass missing current baseline")
	}
	if got := state.ObserveV3Epoch(1); got != V3EpochStale {
		t.Fatal(got)
	}
	state.MarkBaselineReceived()
	if !state.HasReceivedBaseline() {
		t.Fatal("accepted epoch2 full did not establish baseline")
	}
}

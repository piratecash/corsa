package loadstand

import (
	"testing"
)

func TestHeadlessFinalReportHasOwnOutcomeAndOriginalBinding(t *testing.T) {
	binding, sdk := finalFixture(t)
	prior := NewTransportHighWater(binding.RuntimeBinding())
	report := HeadlessFinalReport{Version: sdk.Version, Nonce: sdk.Nonce, Fingerprint: sdk.Fingerprint, Identity: sdk.Identity, StartedAt: sdk.StartedAt, ReadAt: sdk.ReadAt, BytesSent: sdk.BytesSent, BytesReceived: sdk.BytesReceived, Quiescent: true, Outcome: HeadlessShutdownSucceeded}
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeHeadlessFinalReport(raw, binding, prior); err != nil || got.BytesSent != report.BytesSent {
		t.Fatal("exactheadless final", err)
	}
	if _, err = DecodeFinalReport(raw, binding, prior); err == nil {
		t.Fatal("headless report relabelled SDK")
	}
	sdkRaw, err := sdk.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeHeadlessFinalReport(sdkRaw, binding, prior); err == nil {
		t.Fatal("SDK report relabelled headless")
	}
	for _, mode := range []string{"nonce", "fingerprint", "identity", "unjoined", "counter-regression"} {
		copy := report
		high := prior
		switch mode {
		case "nonce":
			copy.Nonce[0]++
		case "fingerprint":
			copy.Fingerprint[0]++
		case "identity":
			copy.Identity[0]++
		case "unjoined":
			copy.Quiescent = false
		case "counter-regression":
			high.seen = true
			high.sent = copy.BytesSent + 1
		}
		raw, err := copy.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = DecodeHeadlessFinalReport(raw, binding, high); err == nil {
			t.Fatal("headless final guard weakened", mode)
		}
	}
}

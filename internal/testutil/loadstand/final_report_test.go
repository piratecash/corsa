package loadstand

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func finalFixture(t *testing.T) (CompletedStartBinding, FinalReport) {
	t.Helper()
	launch := launchBinding{nonce: LaunchNonce{1}, fingerprint: LaunchFingerprint{2}}
	receipt := StartReceipt{Public: PublicIdentity{Identity: domain.PeerIdentity{3}, PublicKey: "public", BoxPublicKey: "box", BoxSignature: "signature"}, Incarnation: Incarnation{Ordinal: 1, StartedAt: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)}}
	binding, err := completedStartBinding(launch, receipt)
	if err != nil {
		t.Fatal(err)
	}
	return binding, FinalReport{Version: 1, Nonce: launch.nonce, Fingerprint: launch.fingerprint, Identity: receipt.Public.Identity, StartedAt: receipt.Incarnation.StartedAt, ReadAt: receipt.Incarnation.StartedAt.Add(time.Second), BytesSent: 11, BytesReceived: 12, Quiescent: true, Outcome: SDKShutdownSucceeded}
}
func TestFinalReportAcceptsBoundZeroAndRejectsSchemaForgery(t *testing.T) {
	binding, report := finalFixture(t)
	prior := NewTransportHighWater(binding.RuntimeBinding())
	report.BytesSent = 0
	report.BytesReceived = 0
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFinalReport(raw, binding, prior)
	if err != nil || got.BytesSent != 0 || !got.StartedAt.Equal(report.StartedAt) {
		t.Fatal("complete postjoin zero must be measured", got, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 10 {
		t.Fatal("not exactly ten keys")
	}
	for key := range fields {
		for _, kind := range []string{"missing", "null", "case"} {
			t.Run(key+kind, func(t *testing.T) {
				copy := map[string]json.RawMessage{}
				for k, v := range fields {
					copy[k] = v
				}
				switch kind {
				case "missing":
					delete(copy, key)
				case "null":
					copy[key] = json.RawMessage("null")
				case "case":
					delete(copy, key)
					copy[strings.ToUpper(key)] = fields[key]
				}
				mutant, err := json.Marshal(copy)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := DecodeFinalReport(mutant, binding, prior); err == nil {
					t.Fatal("partial/malformed final report accepted")
				}
			})
		}
	}
	for _, raw := range [][]byte{append([]byte(`{"version":1,`), raw[1:]...), append(append([]byte{}, raw...), []byte(" {}")...), []byte(`null`), []byte(`{"bytes_sent":-1}`), []byte(strings.Repeat(" ", int(FinalReportLimit)+1))} {
		if _, err := DecodeFinalReport(raw, binding, prior); err == nil {
			t.Fatal("forged wire accepted")
		}
	}
}
func TestFinalReportBindingAndConstantSizePriorEvidence(t *testing.T) {
	binding, report := finalFixture(t)
	prior := NewTransportHighWater(binding.RuntimeBinding())
	stats := domain.TransportTrafficStats{StartedAt: report.StartedAt, ReadAt: report.ReadAt, BytesSent: 10, BytesReceived: 10}
	prior.Observe(stats, nil)
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeFinalReport(raw, binding, prior); err != nil {
		t.Fatal(err)
	}
	for _, mutant := range []string{"nonce", "fingerprint", "identity", "start", "backward", "sent", "received", "quiescent", "outcome", "version"} {
		t.Run(mutant, func(t *testing.T) {
			copy := report
			switch mutant {
			case "nonce":
				copy.Nonce[0]++
			case "fingerprint":
				copy.Fingerprint[0]++
			case "identity":
				copy.Identity[0]++
			case "start":
				copy.StartedAt = copy.StartedAt.Add(time.Second)
			case "backward":
				copy.ReadAt = copy.StartedAt.Add(-time.Nanosecond)
			case "sent":
				copy.BytesSent = 9
			case "received":
				copy.BytesReceived = 9
			case "quiescent":
				copy.Quiescent = false
			case "outcome":
				copy.Outcome = "failed"
			case "version":
				copy.Version = 2
			}
			raw, err := copy.Encode()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeFinalReport(raw, binding, prior); err == nil {
				t.Fatal("foreign/invalid final report accepted")
			}
		})
	}
	prior.Observe(stats, errors.New("missing sample"))
	prior.Observe(stats, nil)
	if _, err := DecodeFinalReport(raw, binding, prior); err == nil {
		t.Fatal("later sample cleared invalid prior state")
	}
}

func TestFinalReportCanonicalNumbersAndDigests(t *testing.T) {
	b, r := finalFixture(t)
	raw, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	prior := NewTransportHighWater(b.RuntimeBinding())
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, spec := range []struct{ key, value string }{{"bytes_sent", "-1"}, {"bytes_sent", "18446744073709551616"}, {"bytes_sent", "1.0"}, {"bytes_received", "\"12\""}, {"version", "256"}, {"version", "1e0"}, {"transport_quiescent", "\"true\""}, {"launch_nonce", "\"" + strings.ToUpper(LaunchNonce{255}.String()) + "\""}, {"launch_nonce", "\"" + strings.Repeat("0", 64) + "\""}, {"plan_sha256", "\"short\""}, {"unexpected", "0"}} {
		t.Run(spec.key+spec.value, func(t *testing.T) {
			copy := make(map[string]json.RawMessage, len(fields)+1)
			for k, v := range fields {
				copy[k] = v
			}
			copy[spec.key] = json.RawMessage(spec.value)
			wire, err := json.Marshal(copy)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeFinalReport(wire, b, prior); err == nil {
				t.Fatal("noncanonical/out-of-range final field accepted")
			}
		})
	}
	r.BytesSent = ^uint64(0)
	r.BytesReceived = ^uint64(0)
	raw, err = r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := DecodeFinalReport(raw, b, prior)
	if err != nil || stats.BytesSent != ^uint64(0) {
		t.Fatal("valid uint64 boundary lost", stats, err)
	}
}

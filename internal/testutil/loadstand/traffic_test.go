package loadstand

import (
	"errors"
	"testing"
)

func TestTransportSourceRequiresExplicitSupportedZeroAndPeriod(t *testing.T) {
	valid := `{"transport_traffic":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z","bytes_sent":0,"bytes_received":0}}`
	sample, err := NewProcessSource(protocolClient(t, 200, valid)).TransportTraffic(t.Context())
	if err != nil || sample.BytesSent != 0 || sample.BytesReceived != 0 {
		t.Fatalf("supported zero: %v", err)
	}
	for _, body := range []string{`{}`, `{"transport_traffic":null}`, `{"transport_traffic":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z","bytes_sent":0}}`} {
		if _, err := NewProcessSource(protocolClient(t, 200, body)).TransportTraffic(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
			t.Fatalf("missing field silently zero: %v", err)
		}
	}
	invalid := `{"transport_traffic":{"started_at":"2026-10-18T00:00:02Z","read_at":"2026-10-18T00:00:01Z","bytes_sent":0,"bytes_received":0}}`
	if _, err := NewProcessSource(protocolClient(t, 200, invalid)).TransportTraffic(t.Context()); !errors.Is(err, ErrInvalidSample) {
		t.Fatal(err)
	}
}
func TestSessionSourceRequiresAccountingEquality(t *testing.T) {
	body := `{"session_outcomes":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z","attempts":1,"succeeded":0,"errors_connect":0,"errors_compat":0,"errors_other":0}}`
	if _, err := NewProcessSource(protocolClient(t, 200, body)).SessionOutcomes(t.Context()); !errors.Is(err, ErrInvalidSample) {
		t.Fatalf("unaccounted session: %v", err)
	}
}

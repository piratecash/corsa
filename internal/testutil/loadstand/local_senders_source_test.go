package loadstand

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
)

func TestLocalSendersProjectionKeepsAbsenceAndRefusesMalformedAttribution(t *testing.T) {
	start := adapterAt.Add(-time.Second)
	snapshot := datagram.NewMetricsStartedAt(start).LocalSendersSnapshot()
	snapshot.ReadAt = &adapterAt
	for _, absent := range []json.RawMessage{nil} {
		got, err := DecodeLocalSendersSnapshot(absent, start, adapterAt)
		if err != nil || got != nil {
			t.Fatal("absence became measured zero", got, err)
		}
	}
	if got, err := DecodeLocalSendersSnapshot([]byte("null"), start, adapterAt); !errors.Is(err, ErrFamilyUnavailable) || got != nil {
		t.Fatal("explicit null attribution accepted", got, err)
	}
	for _, mode := range []string{"original", "noncoherent-live", "missing-row", "foreign-row", "null-counter", "negative", "overflow", "foreign-start", "future-read", "extra-counter"} {
		t.Run(mode, func(t *testing.T) {
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			rows := fields["by_sender"].(map[string]any)
			row := rows[datagram.LocalSenderIdentityResolver.String()].(map[string]any)
			switch mode {
			case "noncoherent-live":
				row["attempts"] = 1 // A lawful atomic read between increments.
			case "missing-row":
				delete(rows, "identity_resolver")
			case "foreign-row":
				rows["foreign"] = row
			case "null-counter":
				row["quota_capped"] = nil
			case "negative":
				row["quota_capped"] = -1
			case "overflow":
				row["quota_capped"] = json.Number("18446744073709551616")
			case "foreign-start":
				fields["started_at"] = start.Add(-time.Second)
			case "future-read":
				fields["read_at"] = adapterAt.Add(time.Second)
			case "extra-counter":
				row["hidden"] = 0
			}
			raw, err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeLocalSendersSnapshot(raw, start, adapterAt)
			if mode == "original" || mode == "noncoherent-live" {
				if err != nil || got == nil || len(got.BySender) != len(snapshot.BySender) {
					t.Fatal(got, err)
				}
			} else {
				want := ErrInvalidSample
				if mode == "null-counter" {
					want = ErrFamilyUnavailable
				}
				if !errors.Is(err, want) || got != nil {
					t.Fatal("malformed attribution accepted/wrong refusal", got, err)
				}
			}
		})
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"LOCAL_SENDERS", "Local_Senders"} {
		got, err := DecodeOptionalLocalSenders(map[string]json.RawMessage{name: raw}, start, adapterAt)
		if !errors.Is(err, ErrInvalidSample) || got != nil {
			t.Fatal("summary alias became absence", name, got, err)
		}
	}
	if got, err := DecodeOptionalLocalSenders(map[string]json.RawMessage{}, start, adapterAt); err != nil || got != nil {
		t.Fatal("shared optional omission changed", got, err)
	}
	if got, err := DecodeOptionalLocalSenders(map[string]json.RawMessage{"local_senders": raw}, start, adapterAt); err != nil || got == nil {
		t.Fatal("shared original key refused", got, err)
	}
	copy := CloneLocalSendersSnapshot(&snapshot)
	copy.BySender["identity_resolver"] = datagram.LocalSendCounts{Attempts: 99}
	*copy.ReadAt = copy.ReadAt.Add(time.Second)
	if snapshot.BySender["identity_resolver"].Attempts != 0 || !snapshot.ReadAt.Equal(adapterAt) {
		t.Fatal("report aliases source storage")
	}
}

func TestProcessDatagramRetainsExistingOptionalLocalSenders(t *testing.T) {
	fixture := datagramFixture()
	start, read := adapterAt.Add(-time.Second), adapterAt
	snapshot := datagram.NewMetricsStartedAt(start).LocalSendersSnapshot()
	snapshot.ReadAt = &read
	// The source's actual JSON block, not a new custom wire schema.
	fixture["local_senders"] = snapshot
	reading, err := typedSource(t, fixture).DatagramSummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	sample, err := reading.Get()
	if err != nil || sample.LocalSenders == nil || len(sample.LocalSenders.BySender) != len(snapshot.BySender) {
		t.Fatal("RPC source lost sender attribution", sample, err)
	}
	fixture["LOCAL_SENDERS"] = snapshot
	delete(fixture, "local_senders")
	if _, err := typedSource(t, fixture).DatagramSummary(t.Context()); !errors.Is(err, ErrInvalidSample) {
		t.Fatal("case-folded attribution became absent/wrong refusal", err)
	}
	delete(fixture, "LOCAL_SENDERS")
	delete(fixture, "local_senders")
	reading, err = typedSource(t, fixture).DatagramSummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	sample, err = reading.Get()
	if err != nil || sample.LocalSenders != nil {
		t.Fatal("legacy omission became measured zero", sample, err)
	}
}

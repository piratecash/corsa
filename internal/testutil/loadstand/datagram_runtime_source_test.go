package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestDatagramRuntimeAuthorityRejectsMissingMalformedAndForeignFacts(t *testing.T) {
	baseline := datagramStateFixture()
	baseline.State = domain.DatagramStateConfiguredOff
	baseline.ConfiguredEnabled = false
	raw, err := json.Marshal(baseline)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for name := range fields {
		for _, kind := range []string{"missing", "null", "case"} {
			t.Run(name+kind, func(t *testing.T) {
				copy := map[string]any{}
				for k, v := range fields {
					copy[k] = v
				}
				switch kind {
				case "missing":
					delete(copy, name)
				case "null":
					copy[name] = nil
				case "case":
					delete(copy, name)
					copy[strings.ToUpper(name)] = fields[name]
				}
				if _, err := stateAndSummarySource(t, copy, nil).DatagramSummary(t.Context()); err == nil {
					t.Fatal("partial state became off")
				}
			})
		}
	}
	for _, mutant := range []string{"version", "state", "configured", "identity", "start", "backward", "extra"} {
		t.Run(mutant, func(t *testing.T) {
			copy := map[string]any{}
			for k, v := range fields {
				copy[k] = v
			}
			switch mutant {
			case "version":
				copy["version"] = 2
			case "state":
				copy["state"] = "future"
			case "configured":
				copy["configured_enabled"] = true
			case "identity":
				copy["identity"] = domain.PeerIdentity{2}.String()
			case "start":
				copy["started_at"] = adapterAt.Add(-2 * time.Second)
			case "backward":
				copy["read_at"] = adapterAt.Add(-2 * time.Second)
			case "extra":
				copy["ready"] = true
			}
			if _, err := stateAndSummarySource(t, copy, nil).DatagramSummary(t.Context()); err == nil {
				t.Fatal("invalid state became off")
			}
		})
	}
	for _, kind := range []domain.DatagramRuntimeStateKind{domain.DatagramStateUnknown, domain.DatagramStateInitializationFailed} {
		state := datagramStateFixture()
		state.State = kind
		if _, err := stateAndSummarySource(t, state, map[string]any{"enabled": false}).DatagramSummary(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
			t.Fatal("unknown/failure became off", err)
		}
	}
	stale := baseline
	stale.ReadAt = stale.StartedAt
	reading, err := stateAndSummarySource(t, stale, nil).DatagramSummary(t.Context())
	if err != nil {
		t.Fatal("immutable same-incarnation fact needs artificial freshness", err)
	}
	if _, err := reading.Get(); !errors.Is(err, ErrDatagramDisabled) {
		t.Fatal(err)
	}
	if _, err := typedSource(t, map[string]any{"enabled": false}).DatagramSummary(t.Context()); !errors.Is(err, ErrInvalidSample) {
		t.Fatal("raw false overrode initialized state", err)
	}
	summary := datagramFixture()
	summary["metrics"].(map[string]any)["StartedAt"] = adapterAt.Add(-2 * time.Second)
	if _, err := typedSource(t, summary).DatagramSummary(t.Context()); !errors.Is(err, ErrInvalidSample) {
		t.Fatal("metrics crossed incarnation", err)
	}
}

func TestDatagramRuntimeAuthorityMalformedWireAndAccessStayUnavailable(t *testing.T) {
	binding := RuntimeBinding{Identity: domain.PeerIdentity{1}, StartedAt: adapterAt.Add(-time.Second)}
	for _, status := range []int{401, 403, 404, 503, 500} {
		source, err := NewBoundProcessSource(protocolClient(t, status, `{"enabled":false}`), adapterClock{adapterAt}, binding)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.DatagramSummary(t.Context()); err == nil {
			t.Fatal("error status became disabled", status)
		}
	}
	for _, raw := range []string{`{}`, `null`, `{"version":1,"version":1}`, `{"version":1,"Version":1}`, `{} {}`, `{"version":true}`, `{"enabled":false}`} {
		source, err := NewBoundProcessSource(protocolClient(t, 200, raw), adapterClock{adapterAt}, binding)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.DatagramSummary(t.Context()); err == nil {
			t.Fatal("malformed state became disabled", raw)
		}
	}
	if _, err := NewProcessSource(protocolClient(t, 200, `{"enabled":false}`)).DatagramSummary(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
		t.Fatal("unbound source gained authority", err)
	}
	if _, err := NewBoundProcessSource(nil, adapterClock{adapterAt}, binding); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}

func TestDatagramRuntimeAuthorityCancelledRequestIsUnavailable(t *testing.T) {
	state := datagramStateFixture()
	state.ConfiguredEnabled = false
	state.State = domain.DatagramStateConfiguredOff
	source := stateAndSummarySource(t, state, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := source.DatagramSummary(ctx); err == nil || errors.Is(err, ErrDatagramDisabled) {
		t.Fatal("cancelled request gained configured-off authority", err)
	}
}

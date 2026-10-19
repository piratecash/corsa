package node

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/stretchr/testify/mock"
)

func TestDatagramConstructorFailureNeverBecomesConfiguredOff(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenerSet: true, ListenerEnabled: false, EnableDatagramV1: true, TrustStorePath: filepath.Join(dir, "trust.json"), PeersStatePath: filepath.Join(dir, "peers.json")})
	constructor := NewMockdatagramConstructor(t)
	constructor.EXPECT().Build(mock.Anything, mock.Anything).RunAndReturn(func(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
		if !s.cfg.EnableDatagramV1 {
			t.Fatal("original request lost before actual constructor")
		}
		s.cfg.EnableDatagramV1 = false
		return nil, errors.New("controlled private instance failure")
	}).Once()
	s := newServiceWithDatagramConstructor(cfg, id, nil, nil, constructor)
	t.Cleanup(s.WaitBackground)
	if s.cfg.EnableDatagramV1 {
		t.Fatal("failure changed existing capability opt-out")
	}
	if advertised := s.localDatagramAdvertise(); advertised.Endpoint || advertised.Transit {
		t.Fatal("failure re-enabled advertisement")
	}
	state, err := s.DatagramRuntimeState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !state.ConfiguredEnabled || state.State != domain.DatagramStateInitializationFailed {
		t.Fatalf("constructor failure became off: %+v", state)
	}
	if _, err := s.FetchDatagramSummary(); !errors.Is(err, errDatagramNotEnabled) {
		t.Fatal("summary compatibility changed", err)
	}
	_, err = readLoadStandDatagram(t.Context(), s, loadstand.RuntimeBinding{Identity: state.Identity, StartedAt: state.StartedAt})
	if !errors.Is(err, loadstand.ErrFamilyUnavailable) {
		t.Fatalf("in-process constructor failure became disabled: %v", err)
	}
}
func TestDatagramConstructorFactsAreNotServingReadiness(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s := newDatagramLayerService(t, enabled)
		state, err := s.DatagramRuntimeState(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		expected := domain.DatagramStateConfiguredOff
		if enabled {
			expected = domain.DatagramStateInitialized
		}
		if state.State != expected || state.ConfiguredEnabled != enabled || !state.StartedAt.Equal(s.startedAt) {
			t.Fatalf("wrong constructed facts %+v", state)
		}
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if _, found := fields["ready"]; found || len(fields) != 6 {
			t.Fatal("constructor facts claim serving readiness")
		}
		binding := loadstand.RuntimeBinding{Identity: state.Identity, StartedAt: state.StartedAt}
		reading, err := readLoadStandDatagram(t.Context(), s, binding)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := reading.Get()
		if !enabled && !errors.Is(readErr, errDatagramNotEnabled) {
			t.Fatal(readErr)
		}
		binding.Identity[0] ^= 1
		if _, err := readLoadStandDatagram(t.Context(), s, binding); !errors.Is(err, loadstand.ErrInvalidSample) {
			t.Fatal("foreign incarnation became off", err)
		}
	}
	var nilService *Service
	for _, s := range []*Service{nilService, {}} {
		if _, err := s.DatagramRuntimeState(t.Context()); !errors.Is(err, domain.ErrDatagramStateUnavailable) {
			t.Fatal("unknown default became off", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newDatagramLayerService(t, false).DatagramRuntimeState(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// A missing or contradictory live plane is unavailable, even when the immutable
// constructor fact alone would otherwise identify an initialized/off service.
func TestDatagramRuntimeStateContradictoryPlaneIsUnavailable(t *testing.T) {
	initialized := newDatagramLayerService(t, true)
	off := newDatagramLayerService(t, false)
	layer := initialized.datagramLayer()
	initialized.datagramPlane.Store(nil)
	off.datagramPlane.Store(layer)
	for _, service := range []*Service{initialized, off} {
		if _, err := service.DatagramRuntimeState(t.Context()); !errors.Is(err, domain.ErrDatagramStateUnavailable) {
			t.Fatal("contradictory plane became authoritative", err)
		}
	}
}

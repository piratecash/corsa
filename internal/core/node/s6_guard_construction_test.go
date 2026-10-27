package node

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/trustedclock"
	"github.com/rs/zerolog/log"
)

func guardConstructionKey() GuardObservationKey {
	return GuardObservationKey{Anchor: "guard-anchor", ArtifactSHA256: strings.Repeat("e", 64), Boot: "guard-boot", ProcessInstance: "guard-process", ClockDomain: "secure-store-since-boot", ClockInstance: [16]byte{99}}
}

func TestS6GuardConstructionSameService(t *testing.T) {
	end := time.Now().Add(5 * time.Second)
	cfg, id := pendingConstructionInputs(t)
	pk, tk, gk := pendingConstructionKey(), terminalConstructionKey(), guardConstructionKey()
	pc := &pendingConstructionClock{}
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &pk, Terminal: &tk, Guard: &gk}, pc.read)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, end)
	if r.Pending == nil || r.Terminal == nil || r.Guard == nil || r.Pending.service != s || r.Terminal.service != s || r.Guard.service != s {
		t.Fatal("common constructor did not bind all three handles to one Service")
	}
	identity := domain.PeerIdentityFromWire(s.Address())
	if r.Pending.Identity() != identity || r.Terminal.Identity() != identity || r.Guard.Identity() != identity || s.secureSessions.store == nil || r.Guard.clock != s.secureSessions.store.clock {
		t.Fatal("handle is not the actual secure-store clock/owner")
	}
	p0, t0 := r.Pending.Snapshot(), r.Terminal.Snapshot()
	if p0.SourceID == ([16]byte{}) || t0.Instance == ([16]byte{}) || p0.SourceID == t0.Instance || p0.SourceID == gk.ClockInstance || t0.Instance == gk.ClockInstance {
		t.Fatal("source instances collapsed")
	}
	pk.Anchor, tk.Anchor, gk.Anchor = "changed", "changed", "changed"
	if r.Guard.Key() != guardConstructionKey() || r.Pending.Key() != pendingConstructionKey() || r.Terminal.Key() != terminalConstructionKey() {
		t.Fatal("caller mutated frozen construction keys")
	}
	if !s.queuePeerFrame("10.80.0.29:64646", s6PendingFrame(s, "guard-coexist")) {
		t.Fatal("original queue admission refused")
	}
	conn, entry := k10Attach(t, s, 1, netcore.Inbound)
	ps := k10Session(s, conn, entry)
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	ps.k10FinishReported(nil)
	if p, tv := r.Pending.Snapshot(), terminalCopiedFields(t, s, r.Terminal); p.SourceID != p0.SourceID || p.Count != 1 || tv.Instance != t0.Instance || tv.Classified != 1 || p.AttributionBound || tv.AttributionBound {
		t.Fatal("original sources did not coexist", p, tv)
	}
	snapshot := r.Guard.Snapshot()
	if !snapshot.Available || snapshot.AttributionBound || snapshot.SchemaVersion != 1 || snapshot.Instance != guardConstructionKey().ClockInstance {
		t.Fatal("guard source instance missing", snapshot)
	}
	if _, err := trustedclock.SinceBoot(); err != nil {
		if snapshot.Status != "UNKNOWN" || snapshot.Reason == "" {
			t.Fatal("unsupported source certified coverage", snapshot)
		}
		return
	}
	t.Run("native-clock", func(t *testing.T) {
		v := r.Guard.Snapshot()
		if v.Status != "COMPLETE" || !v.Guarded || v.Cut < v.CoverageStart || s.StartupRefusal() != nil {
			t.Fatal("actual native source not acquired before store construction", v, s.StartupRefusal())
		}
	})
}

func TestS6GuardConstructionBeforeOriginalLoad(t *testing.T) {
	device := newStoreClock()
	original := device.read()
	descriptor := prepareGuardObservation(guardConstructionKey(), domain.PeerIdentity{1})
	clock := descriptor.newClock(trustedclock.Sources{Wall: device.read, SinceBoot: device.sinceBoot})
	baseline := descriptor.readout.Snapshot()
	if baseline.Status != "COMPLETE" || baseline.Sequence != 0 {
		t.Fatal("initial source not frozen before load", baseline)
	}
	path := filepath.Join(t.TempDir(), "secure.json")
	data, err := json.Marshal(secureSessionFile{Format: secureSessionStoreFormat, Identities: map[domain.PeerIdentity]time.Time{}, Endpoints: map[domain.PeerAddress]endpointBindingRecord{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	audited := 0
	store := openSecureSessionStore(secureSessionStoreOptions{path: path, clock: clock, logger: log.Logger, audit: func(path string) error {
		audited++
		if audited == 1 {
			device.stepWall(2 * trustedclock.JumpTolerance)
		}
		return secretfile.AuditLocation(path) // original custody audit still decides
	}})
	if audited != 1 || store.clock != clock || descriptor.readout.clock != clock || !store.pinMainLoaded || store.startupRefusal() != nil {
		t.Fatal("actual disk-load path did not use original observed clock", audited, store.startupRefusal())
	}
	active := descriptor.readout.Snapshot()
	if active.Status != "COMPLETE" || !active.Guarded || !active.Active || active.Sequence != 1 || active.ClosedCount != 0 || clock.Stats().JumpsDetected != 1 {
		t.Fatal("loadFile first now missed the post-anchor jump", active, clock.Stats())
	}
	device.now = device.now.Add(time.Second)
	device.stepWall(-2 * trustedclock.JumpTolerance)
	if got := store.now(); !got.Equal(original.Add(time.Second)) {
		t.Fatal("original store security time changed", got)
	}
	final := descriptor.readout.Snapshot()
	if final.Status != "COMPLETE" || final.Active || final.Sequence != 2 || final.ClosedCount != 1 || final.TotalLower != time.Second || final.TotalUpper != time.Second || active.Sequence != 1 {
		t.Fatal("actual store exit lost original bracket or mutated old copy", final, active)
	}
}

func TestS6GuardConstructionActualSessions(t *testing.T) {
	if _, err := trustedclock.SinceBoot(); err != nil {
		return
	}
	t.Run("native-store-owner", func(t *testing.T) {
		device := newStoreClock()
		descriptor := prepareGuardObservation(guardConstructionKey(), domain.PeerIdentity{1})
		opts := sessionsOptionsIn(t.TempDir())
		opts.storePath, opts.markerPath = "", ""
		opts.clock, opts.guardObservation = device.read, descriptor
		sessions := buildSecureSessions(generatedIdentity(t), opts, productionSecureSessionBuilders())
		if sessions.store == nil || descriptor.readout.clock == nil || sessions.store.clock != descriptor.readout.clock || sessions.store.startupRefusal() != nil {
			t.Fatal("actual buildSecureSessions did not bind the original clock")
		}
		base := descriptor.readout.Snapshot()
		device.stepWall(2 * trustedclock.JumpTolerance)
		if err := sessions.store.noteProvenInbound(domain.PeerIdentity{21}); err != nil {
			t.Fatal(err)
		}
		during := descriptor.readout.Snapshot()
		if base.Status != "COMPLETE" || during.Status != "COMPLETE" || !during.Active || during.Sequence != base.Sequence+1 || !sessions.store.clock.Stats().JumpActive {
			t.Fatal("actual pin owner missed its clock transition", base, during)
		}
		device.stepWall(-2 * trustedclock.JumpTolerance)
		if err := sessions.store.noteProvenInbound(domain.PeerIdentity{22}); err != nil {
			t.Fatal(err)
		}
		after := descriptor.readout.Snapshot()
		if after.Status != "COMPLETE" || after.Active || after.ClosedCount != 1 || after.Sequence != during.Sequence+1 {
			t.Fatal("actual pin owner missed guard exit", after)
		}
	})
}

func TestS6GuardConstructionUnavailableAndDefaults(t *testing.T) {
	t.Run("nil-clock-original-calls", func(t *testing.T) {
		var descriptor *guardObservationConstruction
		boots, walls := 0, 0
		sources := trustedclock.Sources{Wall: func() time.Time { walls++; return time.Unix(1780000000, 0) }, SinceBoot: func() (time.Duration, error) { boots++; return time.Second, nil }}
		clock := descriptor.newClock(sources)
		if boots != 1 || walls != 1 || !clock.Stats().Guarded || clock.GuardDurationSnapshot().Status != "INACTIVE" {
			t.Fatal("nil descriptor changed original constructor calls", boots, walls)
		}
		_ = clock.Now()
		if boots != 2 || walls != 2 {
			t.Fatal("nil descriptor changed original Now calls", boots, walls)
		}
	})
	t.Run("default-and-nil", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		s := NewService(cfg, id, nil)
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		if s.secureSessions.store == nil || s.secureSessions.store.clock.GuardDurationSnapshot().Status != "INACTIVE" || s.pendingObservation != nil || s.terminalObservation != nil {
			t.Fatal("ordinary Service opted in")
		}
		var absent *GuardObservation
		if v := absent.Snapshot(); v.Available || v.Status != "INACTIVE" || v.Guarded || v.AttributionBound || absent.Key() != (GuardObservationKey{}) || absent.Identity() != (domain.PeerIdentity{}) {
			t.Fatal("nil readout fabricated evidence", v)
		}
	})
	t.Run("validation", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		for _, bad := range []GuardObservationKey{{}, {Anchor: "a", ArtifactSHA256: strings.Repeat("e", 64), Boot: "b", ProcessInstance: "p", ClockDomain: "d"}} {
			s, r, err := NewServiceWithObservations(cfg, id, nil, ObservationOptions{Guard: &bad})
			if err == nil || s != nil || r.Guard != nil {
				t.Fatal("invalid references/zero instance accepted")
			}
		}
		bad := guardConstructionKey()
		bad.ClockDomain = strings.Repeat("d", 129)
		if validGuardObservationKey(bad) {
			t.Fatal("unbounded key accepted")
		}
	})
	t.Run("initial-read-loss", func(t *testing.T) {
		descriptor := prepareGuardObservation(guardConstructionKey(), domain.PeerIdentity{1})
		raw := time.Unix(1780000000, 0)
		clock := descriptor.newClock(trustedclock.Sources{Wall: func() time.Time { return raw }, SinceBoot: func() (time.Duration, error) { return 0, errors.New("read unavailable") }})
		store := openSecureSessionStore(secureSessionStoreOptions{clock: clock, logger: log.Logger, audit: secretfile.AuditLocation})
		if store.clock != clock || store.startupRefusal() != nil {
			t.Fatal("initial observation failure changed original store startup", store.startupRefusal())
		}
		if got := store.now(); !got.Equal(raw) || clock.Stats().Guarded {
			t.Fatal("observation failure changed plain-clock fallback")
		}
		v := descriptor.readout.Snapshot()
		if !v.Available || v.Status != "UNKNOWN" || v.Guarded || v.Sequence != 0 || v.ClosedCount != 0 || v.GuardDurationSnapshot != clock.GuardDurationSnapshot() || v.AttributionBound {
			t.Fatal("readout filled unavailable values from security state", v)
		}
	})
	t.Run("legacy-clock-unavailable", func(t *testing.T) {
		descriptor := prepareGuardObservation(guardConstructionKey(), domain.PeerIdentity{1})
		opts := sessionsOptionsIn(t.TempDir())
		opts.guardObservation = descriptor
		builders := productionSecureSessionBuilders()
		builders.mode = modeAlways(sessionv2.ModeLegacyOnly)
		_ = buildSecureSessions(generatedIdentity(t), opts, builders)
		v := descriptor.readout.Snapshot()
		if descriptor.readout.clock != nil || v.Available || v.Status != "UNKNOWN" || v.Reason != "secure_store_clock_unavailable" {
			t.Fatal("legacy mode fabricated secure-store clock", v)
		}
	})
	t.Run("certificate-refusal-preserved", func(t *testing.T) {
		descriptor := prepareGuardObservation(guardConstructionKey(), domain.PeerIdentity{1})
		opts := sessionsOptionsIn(t.TempDir())
		opts.guardObservation = descriptor
		builders := productionSecureSessionBuilders()
		builders.certificates = func(sessionv2.Clock) (sessionv2.CertificateSource, error) {
			return sessionv2.CertificateSource{}, errInjectedCertificates
		}
		sessions := buildSecureSessions(generatedIdentity(t), opts, builders)
		if !errors.Is((&Service{secureSessions: sessions}).StartupRefusal(), errInjectedCertificates) || descriptor.readout.clock != nil || descriptor.readout.Snapshot().Status != "UNKNOWN" {
			t.Fatal("observation changed original certificate refusal")
		}
	})
}

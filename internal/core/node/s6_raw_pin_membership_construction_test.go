package node

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func rawPinBridgeRequest() RawPinMembershipObservationRequest {
	return RawPinMembershipObservationRequest{BaselineCapacity: 8, HistoryCapacity: 32, Instance: [16]byte{231}}
}
func rawPinBridgeUnknown(t *testing.T, v RawPinMembershipObservationSnapshot) {
	t.Helper()
	if v.Population != "RawPinMembership" || v.Status != "UNKNOWN" || v.Reason == "" || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
		t.Fatal("unavailable raw bridge certified membership", v)
	}
}
func TestRawPinMembershipConstruction(t *testing.T) {
	t.Run("copy-before-earlier-observer-clock-and-one-original-default-builder", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		request := rawPinBridgeRequest()
		want := request
		registered := registeredBridgeRequest()
		c := newK3ActualClock()
		var once sync.Once
		c.setHook(func() {
			once.Do(func() { request.Instance = [16]byte{}; request.BaselineCapacity = 0; request.HistoryCapacity = 0 })
		})
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RawPinMembership: &request, RegisteredProvenV2: &registered}, c.read)
		if err != nil || s == nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		o := r.RawPinMembership
		// Registered observation reads this callback before secure-session/store
		// construction. Raw's own later store-copy cannot repair a late Core copy.
		if request.Instance != ([16]byte{}) || r.RegisteredProvenV2 == nil || o == nil || s.secureSessions == nil || s.secureSessions.store == nil || o.service != s || o.store != s.secureSessions.store || o.source != o.store.pinMembership || o.Key().Instance != want.Instance || o.Identity() != domain.PeerIdentityFromWire(s.Address()) || len(o.source.history) != want.HistoryCapacity {
			t.Fatal("raw request not frozen before earlier observer callback")
		}
		v := o.Snapshot()
		if v.Status == "UNKNOWN" || v.Instance != want.Instance || v.Sequence != 0 || v.CountAtCut != 0 || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
			t.Fatal("selected original store missing before exposure", v)
		}
		request.Instance = [16]byte{99}
		request.HistoryCapacity = 1
		request.BaselineCapacity = 1
		key := o.Key()
		key.Instance[0] = 0
		if o.Key().Instance != want.Instance || len(o.source.history) != want.HistoryCapacity {
			t.Fatal("caller or returned key aliases bridge input")
		}
		offCfg, offID := pendingConstructionInputs(t)
		calls := 0
		off, readouts, err := newServiceWithObservations(offCfg, offID, nil, ObservationOptions{}, func() (time.Duration, error) {
			calls++
			return 0, errors.New("nil observation must not read this clock")
		})
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, off, time.Now().Add(5*time.Second))
		if calls != 0 || readouts.RawPinMembership != nil || off.secureSessions.store.pinMembership != nil {
			t.Fatal("ordinary Service acquired raw observation work")
		}
		pubCfg, pubID := pendingConstructionInputs(t)
		public, handle, err := NewServiceWithRawPinMembershipObservation(pubCfg, pubID, nil, want)
		if err != nil || public == nil || handle == nil {
			t.Fatal("public wrapper did not return original Service", err)
		}
		ownTerminalConstruction(t, public, time.Now().Add(5*time.Second))
		if handle.service != public || handle.store != public.secureSessions.store || handle.source != handle.store.pinMembership || handle.Key().Instance != want.Instance {
			t.Fatal("public wrapper created a foreign or fallback store")
		}
	})
	t.Run("actual-loaded-Service-store-entrypoint-one-snapshot-and-detached-prefix", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		ids := []domain.PeerIdentity{{61}, {62}}
		pinMembershipTestSeedDisk(t, cfg.SecureSessionStorePath, ids...)
		request := rawPinBridgeRequest()
		registered := registeredBridgeRequest()
		c := newK3ActualClock()
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RawPinMembership: &request, RegisteredProvenV2: &registered}, c.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		o := r.RawPinMembership
		if o == nil || o.service != s || o.store != s.secureSessions.store || o.source != o.store.pinMembership || o.Identity() != domain.PeerIdentityFromWire(s.Address()) {
			t.Fatal("not the actual loaded Service store/source")
		}
		c.configure(5*time.Second, 0, nil)
		calls := c.count()
		loaded := o.Snapshot()
		if c.count() != calls+1 || loaded.Sequence != 0 || loaded.CountAtCut != 2 || !reflect.DeepEqual(pinMembershipTestSet(loaded.BaselineIdentities), pinMembershipTestSet(ids)) || loaded.PointOnly.MemoryOnly || !loaded.PointOnly.Readable {
			t.Fatal("loaded baseline was replayed, reduced or sampled twice", loaded)
		}
		direct := o.store.pinMembershipSnapshot()
		if !reflect.DeepEqual(loaded, direct) {
			t.Fatal("typed alias changed engine DTO", loaded, direct)
		}
		loaded.BaselineIdentities[0] = domain.PeerIdentity{99}
		// This is the original returned Service's store entrypoint. It proves raw
		// memory membership, not that a TLS/session proof workload was executed.
		added := domain.PeerIdentity{63}
		if err := s.secureSessions.store.noteProvenInbound(added); err != nil {
			t.Fatal(err)
		}
		row := o.Snapshot()
		if row.Sequence != 1 || row.CountAtCut != 3 || len(row.Transitions) != 1 || !row.Transitions[0].Added || row.Transitions[0].Identity != added || !reflect.DeepEqual(pinMembershipTestSet(row.BaselineIdentities), pinMembershipTestSet(ids)) {
			t.Fatal("actual store mutation missing from bound readout", row)
		}
		row.Transitions[0].Identity = domain.PeerIdentity{99}
		fresh := o.Snapshot()
		if fresh.Transitions[0].Identity != added || len(loaded.Transitions) != 0 {
			t.Fatal("returned values alias engine history")
		}
		if reg := r.RegisteredProvenV2.Snapshot(); reg.RegisteredOwners != 0 || reg.Sequence != 0 {
			t.Fatal("a raw pin was relabelled as a registered proof owner", reg)
		}
		if fresh.Qualification != "UNKNOWN" || fresh.AttributionBound || !fresh.PastOnly || s.StartupRefusal() != nil {
			t.Fatal("readout changed qualification or original startup", fresh)
		}
	})
	t.Run("invalid-clock-and-binding-refusals-without-startup-or-fallback-policy", func(t *testing.T) {
		for _, request := range []RawPinMembershipObservationRequest{{BaselineCapacity: 0, HistoryCapacity: 1, Instance: [16]byte{1}}, {BaselineCapacity: maxPinnedIdentities + 1, HistoryCapacity: 1, Instance: [16]byte{1}}, {BaselineCapacity: 1, HistoryCapacity: pinMembershipMaxHistory + 1, Instance: [16]byte{1}}, {BaselineCapacity: 1, HistoryCapacity: 1}} {
			cfg, id := pendingConstructionInputs(t)
			calls := 0
			s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RawPinMembership: &request}, func() (time.Duration, error) { calls++; return 1, nil })
			if err != nil || s == nil || r.RawPinMembership == nil {
				t.Fatal("bad observation request denied real Service", err)
			}
			ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
			rawPinBridgeUnknown(t, r.RawPinMembership.Snapshot())
			if calls != 0 || s.StartupRefusal() != nil {
				t.Fatal("invalid observer performed clock work or changed startup")
			}
		}
		cfg, id := pendingConstructionInputs(t)
		request := rawPinBridgeRequest()
		c := newK3ActualClock()
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RawPinMembership: &request}, c.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		o := r.RawPinMembership
		before := o.Snapshot()
		c.configure(time.Hour, 0, errors.New("raw bridge clock unavailable"))
		if err := s.secureSessions.store.noteProvenInbound(domain.PeerIdentity{71}); err != nil || !s.secureSessions.store.identityPinned(domain.PeerIdentity{71}) {
			t.Fatal("observer gap vetoed original store pin", err)
		}
		after := o.Snapshot()
		rawPinBridgeUnknown(t, after)
		pinMembershipTestSame(t, before, after)
		if s.StartupRefusal() != nil {
			t.Fatal("raw clock uncertainty became startup refusal")
		}
		badKey := *o
		badKey.key.Instance[0]++
		for _, bad := range []*RawPinMembershipObservation{nil, {}, &badKey, {service: s, store: o.store, source: o.source, identity: domain.PeerIdentity{99}, key: o.key}, {service: s, source: o.source, identity: o.identity, key: o.key}, {service: s, store: o.store, identity: o.identity, key: o.key}} {
			rawPinBridgeUnknown(t, bad.Snapshot())
		}
		var absent *RawPinMembershipObservation
		if absent.Key() != (RawPinMembershipObservationKey{}) || !absent.Identity().IsZero() {
			t.Fatal("nil handle fabricated captured labels")
		}
		// Existing builder seams produce actual legacy-only/unavailable session
		// objects. These partial-Service bindings test only negative projection;
		// they do not claim public-constructor no-store branch reachability.
		for _, builder := range []secureSessionBuilders{{mode: func() (sessionv2.Mode, error) { return sessionv2.ModeLegacyOnly, nil }}, {mode: func() (sessionv2.Mode, error) { return 0, errors.New("existing mode seam unavailable") }}} {
			sessions := buildSecureSessions(id, secureSessionsOptions{clock: newStoreClock().read}, builder)
			if sessions.store != nil {
				t.Fatal("fixture unexpectedly manufactured a store")
			}
			service := &Service{identity: id, secureSessions: sessions}
			negative := &RawPinMembershipObservation{service: service, identity: domain.PeerIdentityFromWire(service.Address()), key: o.Key()}
			refusal := sessions.refusal
			rawPinBridgeUnknown(t, negative.Snapshot())
			if sessions.store != nil || sessions.refusal != refusal {
				t.Fatal("unavailable readout created fallback or changed original refusal")
			}
		}
		initialCfg, initialID := pendingConstructionInputs(t)
		initial, ir, err := newServiceWithObservations(initialCfg, initialID, nil, ObservationOptions{RawPinMembership: &request}, func() (time.Duration, error) { return 0, errors.New("initial duration unavailable") })
		if err != nil || initial == nil {
			t.Fatal("initial observation clock denied Service", err)
		}
		ownTerminalConstruction(t, initial, time.Now().Add(5*time.Second))
		rawPinBridgeUnknown(t, ir.RawPinMembership.Snapshot())
		if initial.StartupRefusal() != nil {
			t.Fatal("initial clock changed startup")
		}
	})
}

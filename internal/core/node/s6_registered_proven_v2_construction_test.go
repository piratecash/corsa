package node

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func registeredBridgeRequest() RegisteredProvenV2ObservationRequest {
	return RegisteredProvenV2ObservationRequest{OwnerCapacity: 8, HistoryCapacity: 64, Instance: [16]byte{181}}
}
func registeredBridgeUnknown(t *testing.T, v RegisteredProvenV2ObservationSnapshot) {
	t.Helper()
	if v.Population != "RegisteredProvenV2" || v.Status != "UNKNOWN" || v.Reason == "" || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
		t.Fatal("unavailable bridge fabricated qualification", v)
	}
}

func TestRegisteredProvenV2Construction(t *testing.T) {
	t.Run("request-copy-before-clock-one-original-builder-and-nil-default", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		request := registeredBridgeRequest()
		original := request
		clock := newK3ActualClock()
		var once sync.Once
		clock.setHook(func() {
			once.Do(func() { request.OwnerCapacity = 0; request.HistoryCapacity = 0; request.Instance = [16]byte{} })
		})
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RegisteredProvenV2: &request}, clock.read)
		if err != nil || s == nil {
			t.Fatal("typed construction refused original Service", err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		o := r.RegisteredProvenV2
		if o == nil || o.service != s || o.source != s.k3Registered || o.Identity() != domain.PeerIdentityFromWire(s.Address()) || o.Key().Instance != original.Instance || s.k3Registered == nil || len(s.k3Registered.history) != original.HistoryCapacity || s.k3Registered.ownerCapacity != original.OwnerCapacity {
			t.Fatal("request was not copied before original source clock/build")
		}
		request.Instance = [16]byte{99}
		request.OwnerCapacity = 1
		request.HistoryCapacity = 1
		key := o.Key()
		key.Instance[0] = 0
		if o.Key().Instance != original.Instance || s.k3Registered.ownerCapacity != original.OwnerCapacity || len(s.k3Registered.history) != original.HistoryCapacity {
			t.Fatal("caller/key copy changed retained construction")
		}
		empty := o.Snapshot()
		if empty.Instance != original.Instance || empty.Sequence != 0 || empty.RegisteredOwners != 0 || empty.DistinctIdentities != 0 || empty.Qualification != "UNKNOWN" || empty.AttributionBound || !empty.PastOnly {
			t.Fatal("source missing before first publication", empty)
		}
		ordinaryCfg, ordinaryID := pendingConstructionInputs(t)
		calls := 0
		ordinary, off, err := newServiceWithObservations(ordinaryCfg, ordinaryID, nil, ObservationOptions{}, func() (time.Duration, error) {
			calls++
			return 0, errors.New("ordinary path must not read observer clock")
		})
		if err != nil || ordinary == nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, ordinary, time.Now().Add(5*time.Second))
		if calls != 0 || ordinary.k3Registered != nil || off.RegisteredProvenV2 != nil || ordinary.k3RegisteredSnapshot().Status != "OFF" {
			t.Fatal("nil default acquired K3 observation work")
		}
		publicCfg, publicID := pendingConstructionInputs(t)
		public, handle, err := NewServiceWithRegisteredProvenV2Observation(publicCfg, publicID, nil, original)
		if err != nil || public == nil || handle == nil {
			t.Fatal("public typed wrapper failed", err)
		}
		ownTerminalConstruction(t, public, time.Now().Add(5*time.Second))
		if handle.service != public || handle.source != public.k3Registered || handle.Identity() != domain.PeerIdentityFromWire(public.Address()) || handle.Key().Instance != original.Instance {
			t.Fatal("public wrapper acquired a foreign Service")
		}
	})
	t.Run("actual-proof-owners-one-snapshot-and-detached-value-prefix", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		request := registeredBridgeRequest()
		clock := newK3ActualClock()
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RegisteredProvenV2: &request}, clock.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		o := r.RegisteredProvenV2
		if o == nil || o.service != s || o.source != s.k3Registered {
			t.Fatal("typed handle is not installed original source")
		}
		peer := k3ActualService(t, nil)
		first := k3ActualPair(t, s, peer)
		s.peerMu.RLock()
		beforeRead := s.k3Registered.sequence == 1 && s.k3Registered.length == 1
		s.peerMu.RUnlock()
		if !beforeRead {
			t.Fatal("readout rather than actual auth minted first publication")
		}
		clock.configure(5*time.Second, 0, nil)
		reads := clock.count()
		got := o.Snapshot()
		if clock.count() != reads+2 || got.Sequence != 1 || got.RegisteredOwners != 1 || got.DistinctIdentities != 1 || got.Qualification != "UNKNOWN" || got.AttributionBound || !got.PastOnly {
			t.Fatal("bridge duplicated/reduced engine read", got)
		}
		direct := s.k3RegisteredSnapshot()
		if !reflect.DeepEqual(got, direct) {
			t.Fatal("bridge reinterpreted engine value DTO", got, direct)
		}
		got.Transitions[0].Sequence = 99
		got.Transitions[0].AddedIdentity = domain.PeerIdentity{99}
		fresh := o.Snapshot()
		if fresh.Transitions[0].Sequence != 1 || fresh.Transitions[0].AddedIdentity == (domain.PeerIdentity{99}) {
			t.Fatal("public prefix aliases engine history")
		}
		reverse := k3ActualPair(t, peer, s)
		alias := o.Snapshot()
		if alias.RegisteredOwners != 1 || alias.Sequence != 1 {
			t.Fatal("outbound registry alias added an owner", alias)
		}
		k3Publish(t, s, reverse.outbound)
		both := o.Snapshot()
		if both.RegisteredOwners != 2 || both.DistinctIdentities != 1 || both.Sequence != 2 {
			t.Fatal("typed readout lost actual bidirectional owner dedup", both)
		}
		s.clearConnAuth(first.inbound.id)
		one := o.Snapshot()
		if one.RegisteredOwners != 1 || one.DistinctIdentities != 1 {
			t.Fatal("actual auth Clear missed typed source", one)
		}
		k3Withdraw(s, reverse.outbound)
		zero := o.Snapshot()
		if zero.RegisteredOwners != 0 || zero.DistinctIdentities != 0 || zero.Sequence != 4 {
			t.Fatal("last real owner removal missing", zero)
		}
	})
	t.Run("invalid-requests-clock-loss-and-conservative-handle-bindings", func(t *testing.T) {
		for _, request := range []RegisteredProvenV2ObservationRequest{{OwnerCapacity: 0, HistoryCapacity: 1, Instance: [16]byte{1}}, {OwnerCapacity: k3RegisteredMaxOwners + 1, HistoryCapacity: 1, Instance: [16]byte{1}}, {OwnerCapacity: 1, HistoryCapacity: k3RegisteredMaxHistory + 1, Instance: [16]byte{1}}, {OwnerCapacity: 1, HistoryCapacity: 1}} {
			cfg, id := pendingConstructionInputs(t)
			calls := 0
			s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RegisteredProvenV2: &request}, func() (time.Duration, error) { calls++; return time.Second, nil })
			if err != nil || s == nil || r.RegisteredProvenV2 == nil {
				t.Fatal("observer invalidity became constructor policy", err)
			}
			ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
			registeredBridgeUnknown(t, r.RegisteredProvenV2.Snapshot())
			if calls != 0 || s.StartupRefusal() != nil {
				t.Fatal("invalid request performed clock work or denied original startup")
			}
		}
		cfg, id := pendingConstructionInputs(t)
		request := registeredBridgeRequest()
		clock := newK3ActualClock()
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{RegisteredProvenV2: &request}, clock.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		o := r.RegisteredProvenV2
		peer := k3ActualService(t, nil)
		pair := k3ActualPair(t, s, peer)
		good := o.Snapshot()
		clock.configure(time.Hour, 0, errors.New("bridge-only clock unavailable"))
		s.clearConnAuth(pair.inbound.id)
		if s.connAuthStateByID(pair.inbound.id) != nil || s.StartupRefusal() != nil {
			t.Fatal("readout uncertainty changed original clear/startup")
		}
		failed := o.Snapshot()
		registeredBridgeUnknown(t, failed)
		k3SamePrefix(t, good, failed, true)
		reads := clock.count()
		clock.configure(2*time.Hour, 0, nil)
		again := o.Snapshot()
		k3SamePrefix(t, failed, again, true)
		if clock.count() != reads {
			t.Fatal("bridge restarted a faulted engine clock")
		}
		for _, bad := range []*RegisteredProvenV2Observation{nil, {}, {service: s, source: s.k3Registered, identity: domain.PeerIdentity{99}}, {service: peer, source: s.k3Registered, identity: o.Identity()}, {service: s, identity: o.Identity()}} {
			registeredBridgeUnknown(t, bad.Snapshot())
		}
		var absent *RegisteredProvenV2Observation
		if absent.Key() != (RegisteredProvenV2ObservationKey{}) || !absent.Identity().IsZero() {
			t.Fatal("nil handle fabricated owner/key")
		}
		initialCfg, initialID := pendingConstructionInputs(t)
		initial, initialReadouts, err := newServiceWithObservations(initialCfg, initialID, nil, ObservationOptions{RegisteredProvenV2: &request}, func() (time.Duration, error) { return 0, errors.New("initial clock unavailable") })
		if err != nil || initial == nil {
			t.Fatal("initial clock loss denied original construction", err)
		}
		ownTerminalConstruction(t, initial, time.Now().Add(5*time.Second))
		registeredBridgeUnknown(t, initialReadouts.RegisteredProvenV2.Snapshot())
		if initial.StartupRefusal() != nil {
			t.Fatal("initial observer failure became startup denial")
		}
	})
}

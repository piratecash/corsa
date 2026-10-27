package node

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func advertisedBridgeRequest() AdvertisedConnectionObservationRequest {
	return AdvertisedConnectionObservationRequest{ConnectionCapacity: 32, SlotCapacity: 8, HistoryCapacity: 256, Instance: [16]byte{211}}
}
func advertisedBridgeLabels(t *testing.T, v AdvertisedConnectionObservationSnapshot) {
	t.Helper()
	if v.Population != "AdvertisedConnections" || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
		t.Fatal("typed bridge changed source qualification", v)
	}
}
func advertisedBridgeUnknown(t *testing.T, v AdvertisedConnectionObservationSnapshot) {
	t.Helper()
	advertisedBridgeLabels(t, v)
	if v.Status != "UNKNOWN" || v.Reason == "" {
		t.Fatal("unavailable binding certified source", v)
	}
}

func TestAdvertisedConnectionConstruction(t *testing.T) {
	t.Run("entry-copy-before-earlier-store-observer-and-original-default-builder", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		request := advertisedBridgeRequest()
		want := request
		raw := rawPinBridgeRequest()
		clock := newK3ActualClock()
		var once sync.Once
		clock.setHook(func() {
			once.Do(func() {
				request.ConnectionCapacity = 0
				request.SlotCapacity = 0
				request.HistoryCapacity = 0
				request.Instance = [16]byte{}
			})
		})
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{AdvertisedConnections: &request, RawPinMembership: &raw}, clock.read)
		if err != nil || s == nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(10*time.Second))
		s.routeWithdrawalGracePeriodTest = -1
		o := r.AdvertisedConnections
		// Raw membership initializes in the original secure-store construction,
		// before the advertised source is made. A later source copy is too late.
		if request.Instance != ([16]byte{}) || r.RawPinMembership == nil || o == nil || o.service != s || o.source != s.k3Advertised || o.manager != s.connManager || o.manager.config.advertised != o.source || o.Key().Instance != want.Instance || o.Identity() != domain.PeerIdentityFromWire(s.Address()) || o.source.connectionCapacity != want.ConnectionCapacity || o.source.slotCapacity != want.SlotCapacity || len(o.source.history) != want.HistoryCapacity {
			t.Fatal("request not copied before earlier observer callback / one original owner lost")
		}
		clock.setHook(nil)
		v := o.Snapshot()
		advertisedBridgeLabels(t, v)
		if v.Status == "UNKNOWN" || v.Instance != want.Instance || v.CoveredSequence != 0 || v.CompletedSequence != 0 || v.PeerOwnersAtCut != 0 || v.SlotsAtCut != 0 {
			t.Fatal("prepublication baseline missing", v)
		}
		request = AdvertisedConnectionObservationRequest{ConnectionCapacity: 1, SlotCapacity: 1, HistoryCapacity: 1, Instance: [16]byte{99}}
		key := o.Key()
		key.Instance[0] = 0
		if o.Key().Instance == request.Instance || o.Key().Instance != want.Instance || o.source.connectionCapacity != want.ConnectionCapacity || o.source.slotCapacity != want.SlotCapacity || len(o.source.history) != want.HistoryCapacity {
			t.Fatal("caller/returned key mutated source configuration")
		}
		offCfg, offID := pendingConstructionInputs(t)
		calls := 0
		off, offReadouts, err := newServiceWithObservations(offCfg, offID, nil, ObservationOptions{}, func() (time.Duration, error) {
			calls++
			return 0, errors.New("default must not sample observation clock")
		})
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, off, time.Now().Add(10*time.Second))
		if calls != 0 || offReadouts.AdvertisedConnections != nil || off.k3Advertised != nil || off.connManager.config.advertised != nil || off.k3AdvertisedClaims != nil || off.k3AdvertisedGenerations != nil {
			t.Fatal("default selected observer work")
		}
		publicCfg, publicID := pendingConstructionInputs(t)
		public, handle, err := NewServiceWithAdvertisedConnectionObservation(publicCfg, publicID, nil, want)
		if err != nil || public == nil || handle == nil {
			t.Fatal("one-sided public construction failed", err)
		}
		ownTerminalConstruction(t, public, time.Now().Add(10*time.Second))
		if handle.service != public || handle.source != public.k3Advertised || handle.manager != public.connManager || handle.Key().Instance != want.Instance {
			t.Fatal("public wrapper returned another source or CM")
		}
	})
	t.Run("actual-owner-DTO-one-snapshot-pending-cut-and-detached-values", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		request := advertisedBridgeRequest()
		registered := registeredBridgeRequest()
		clock := newK3ActualClock()
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{AdvertisedConnections: &request, RegisteredProvenV2: &registered}, clock.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(10*time.Second))
		s.routeWithdrawalGracePeriodTest = -1
		o := r.AdvertisedConnections
		if o == nil || o.source != s.k3Advertised || o.manager != s.connManager || r.RegisteredProvenV2 == nil || r.RegisteredProvenV2.service != s {
			t.Fatal("typed composite did not use original owners")
		}
		_, _, rawID := advertisedActualInbound(t, s, protocol.Frame{Address: idPeerB.String(), Version: 0})
		peer := advertisedActualService(t, nil)
		genuine := advertisedActualPair(t, s, peer)
		actualAuth := k3Auth(t, s, genuine.inbound.id)
		s.rememberConnPeerAddr(genuine.inbound.id, protocol.Frame{Address: idTargetX.String(), Version: 30}, "127.0.0.1:64001")
		s.completeInboundAuth(genuine.inbound.id, actualAuth)
		clock.configure(100*time.Second, 0, nil)
		calls := clock.count()
		v := o.Snapshot()
		advertisedBridgeLabels(t, v)
		if clock.count() != calls+1 || v.Status == "UNKNOWN" || v.CompletedSequence != v.CoveredSequence || v.Pending != 0 {
			t.Fatal("quiescent getter did not delegate exactly once", v)
		}
		raw := advertisedFactFor(t, v, rawID, "inbound")
		conflict := advertisedFactFor(t, v, genuine.inbound.id, "inbound")
		if !raw.ClaimSeen || raw.Version != 0 || raw.Identity != idPeerB || !raw.ContextKnown || !conflict.ClaimConflict || conflict.Identity != idTargetX || conflict.Version != 30 || conflict.MetadataIdentity == idTargetX {
			t.Fatal("typed alias normalized unknown/conflicting actual claims", raw, conflict)
		}
		// Constant functional clock lets the unchanged original DTO be compared;
		// the second direct call is explicit test evidence, not getter behavior.
		direct := s.k3AdvertisedSnapshot()
		if !reflect.DeepEqual(v, direct) {
			t.Fatal("typed alias reduced or rewrote source DTO", v, direct)
		}
		if len(v.Facts) == 0 {
			t.Fatal("actual owner fixture produced no facts")
		}
		v.Facts[0].Peer.Address = "caller-mutated"
		v.Facts[0].Peer.Version = 999
		if next := o.Snapshot(); next.Facts[0].Peer.Address == "caller-mutated" || next.Facts[0].Peer.Version == 999 {
			t.Fatal("readout exposed source backing history")
		}
		if proof := r.RegisteredProvenV2.Snapshot(); proof.RegisteredOwners != 1 || proof.DistinctIdentities != 1 {
			t.Fatal("raw claim was mistaken for proof population", proof)
		}
		// A pending actual peer transaction and an already-completed CM suffix
		// must remain distinct through the public alias. No DTO/map is seeded.
		pendingCfg, pendingID := pendingConstructionInputs(t)
		pc := &advertisedProgramClock{}
		pendingRequest := advertisedBridgeRequest()
		pendingSvc, pr, err := newServiceWithObservations(pendingCfg, pendingID, nil, ObservationOptions{AdvertisedConnections: &pendingRequest}, pc.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, pendingSvc, time.Now().Add(10*time.Second))
		pendingSvc.routeWithdrawalGracePeriodTest = -1
		_, _, ownerID := advertisedActualInbound(t, pendingSvc, protocol.Frame{Address: idPeerB.String(), Version: 31})
		before := pr.AdvertisedConnections.Snapshot()
		t.Cleanup(pendingSvc.connManager.shutdown)
		run, wait, release := advertisedOwnedTasks(t)
		entered, allow := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { release(allow) })
		pc.set(advertisedClockStep{value: 1000 * time.Second, hook: func() { close(entered); <-allow }}, advertisedClockStep{value: 2000 * time.Second}, advertisedClockStep{value: 3000 * time.Second}, advertisedClockStep{value: 3500 * time.Second})
		done := run(func() {
			pendingSvc.rememberConnPeerAddr(ownerID, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		})
		wait(entered)
		cm := pendingSvc.connManager
		cm.mu.Lock()
		admitted := cm.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.70:64646"}, 8, nil).admitted
		cm.mu.Unlock()
		if admitted == nil {
			t.Fatal("actual CM fixture did not complete")
		}
		pendingCalls := pc.calls()
		pending := pr.AdvertisedConnections.Snapshot()
		advertisedBridgeLabels(t, pending)
		if pc.calls() != pendingCalls || pending.Pending != 1 || pending.CompletedSequence <= pending.CoveredSequence {
			t.Fatal("pending getter forced another clock/cut or hid completed suffix", pending)
		}
		advertisedSameCovered(t, before, pending)
		release(allow)
		wait(done)
		closedCalls := pc.calls()
		closed := pr.AdvertisedConnections.Snapshot()
		if pc.calls() != closedCalls+1 || closed.Pending != 0 || closed.CoveredSequence != closed.CompletedSequence || closed.Status == "UNKNOWN" {
			t.Fatal("quiescent getter failed one original closing sample", closed)
		}
	})
	t.Run("wrong-binding-no-clock-invalid-request-and-no-artificial-store-gate", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		request := advertisedBridgeRequest()
		clock := newK3ActualClock()
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{AdvertisedConnections: &request}, clock.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(10*time.Second))
		o := r.AdvertisedConnections
		otherCfg, otherID := pendingConstructionInputs(t)
		otherClock := newK3ActualClock()
		other, orr, err := newServiceWithObservations(otherCfg, otherID, nil, ObservationOptions{AdvertisedConnections: &request}, otherClock.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, other, time.Now().Add(10*time.Second))
		badKey, badService, badSource, badManager, badIdentity := *o, *o, *o, *o, *o
		badKey.key.Instance[0]++
		badService.service = other
		badSource.source = orr.AdvertisedConnections.source
		badManager.manager = other.connManager
		badIdentity.identity = domain.PeerIdentity{99}
		noIdentityService := &Service{k3Advertised: o.source, connManager: o.manager}
		for _, bad := range []*AdvertisedConnectionObservation{nil, {}, &badKey, &badService, &badSource, &badManager, &badIdentity, {service: s, source: o.source, identity: o.identity, key: o.key}, {service: s, manager: o.manager, identity: o.identity, key: o.key}, {service: noIdentityService, source: o.source, manager: o.manager, identity: o.identity, key: o.key}} {
			calls, otherCalls := clock.count(), otherClock.count()
			advertisedBridgeUnknown(t, bad.Snapshot())
			if clock.count() != calls || otherClock.count() != otherCalls {
				t.Fatal("bad binding sampled/fell back to original source")
			}
		}
		var absent *AdvertisedConnectionObservation
		if absent.Key() != (AdvertisedConnectionObservationKey{}) || !absent.Identity().IsZero() {
			t.Fatal("nil handle fabricated captured labels")
		}
		savedCMSource := o.manager.config.advertised
		o.manager.config.advertised = orr.AdvertisedConnections.source
		calls, otherCalls := clock.count(), otherClock.count()
		mismatch := o.Snapshot()
		o.manager.config.advertised = savedCMSource
		advertisedBridgeUnknown(t, mismatch)
		if clock.count() != calls || otherClock.count() != otherCalls {
			t.Fatal("manager source mismatch sampled another source")
		}
		// Dependency-negative only: the actual constructed source/CM remain valid
		// while an unrelated secure-session reference is temporarily absent. This
		// does not claim a public no-store constructor branch or new startup policy.
		func() {
			saved := s.secureSessions
			s.secureSessions = nil
			defer func() { s.secureSessions = saved }()
			calls := clock.count()
			v := o.Snapshot()
			advertisedBridgeLabels(t, v)
			if v.Status == "UNKNOWN" || clock.count() != calls+1 {
				t.Fatal("readout added a secure-store prerequisite", v)
			}
		}()
		for _, bad := range []AdvertisedConnectionObservationRequest{{ConnectionCapacity: 0, SlotCapacity: 1, HistoryCapacity: 1, Instance: [16]byte{1}}, {ConnectionCapacity: k3AdvertisedMaxConnections + 1, SlotCapacity: 1, HistoryCapacity: 1, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: 0, HistoryCapacity: 1, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: k3AdvertisedMaxSlots + 1, HistoryCapacity: 1, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: 1, HistoryCapacity: 0, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: 1, HistoryCapacity: k3AdvertisedMaxHistory + 1, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: 1, HistoryCapacity: 1}} {
			badCfg, badID := pendingConstructionInputs(t)
			calls := 0
			actual, br, err := newServiceWithObservations(badCfg, badID, nil, ObservationOptions{AdvertisedConnections: &bad}, func() (time.Duration, error) { calls++; return 1, nil })
			if err != nil || actual == nil || br.AdvertisedConnections == nil {
				t.Fatal("invalid observation denied original Service", err)
			}
			ownTerminalConstruction(t, actual, time.Now().Add(10*time.Second))
			advertisedBridgeUnknown(t, br.AdvertisedConnections.Snapshot())
			if calls != 0 || actual.StartupRefusal() != nil {
				t.Fatal("invalid observation changed original startup or sampled clock")
			}
		}
		initialCfg, initialID := pendingConstructionInputs(t)
		initial, ir, err := newServiceWithObservations(initialCfg, initialID, nil, ObservationOptions{AdvertisedConnections: &request}, func() (time.Duration, error) { return 0, errors.New("initial observer clock refusal") })
		if err != nil || initial == nil {
			t.Fatal("initial clock became construction policy", err)
		}
		ownTerminalConstruction(t, initial, time.Now().Add(10*time.Second))
		advertisedBridgeUnknown(t, ir.AdvertisedConnections.Snapshot())
		if initial.StartupRefusal() != nil {
			t.Fatal("initial clock changed startup")
		}
		_, core, ownerID := advertisedActualInbound(t, s, protocol.Frame{Address: idPeerB.String(), Version: 31})
		before := o.Snapshot()
		clock.configure(time.Hour, 0, errors.New("later observer clock refusal"))
		s.rememberConnPeerAddr(ownerID, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		after := o.Snapshot()
		advertisedBridgeUnknown(t, after)
		advertisedSameCovered(t, before, after)
		if core.ProtocolVersion() != 30 || s.StartupRefusal() != nil {
			t.Fatal("readout uncertainty vetoed original metadata/startup")
		}
	})
}

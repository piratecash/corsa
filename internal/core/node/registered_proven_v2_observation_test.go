package node

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Functional capacities and a controller-local duration source only. They are
// not deployment choices or a physical-clock/UTC/resource qualification.
type k3ActualClock struct {
	mu        sync.Mutex
	now, step time.Duration
	err       error
	calls     int
	hook      func()
}

func newK3ActualClock() *k3ActualClock {
	return &k3ActualClock{now: time.Second, step: time.Nanosecond}
}
func (c *k3ActualClock) read() (time.Duration, error) {
	c.mu.Lock()
	value, err, hook := c.now, c.err, c.hook
	c.now += c.step
	c.calls++
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return value, err
}
func (c *k3ActualClock) configure(now, step time.Duration, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now, c.step, c.err = now, step, err
}
func (c *k3ActualClock) setHook(fn func()) { c.mu.Lock(); defer c.mu.Unlock(); c.hook = fn }
func (c *k3ActualClock) count() int        { c.mu.Lock(); defer c.mu.Unlock(); return c.calls }
func k3ActualRequest(clock *k3ActualClock, owners, history int) k3RegisteredRequest {
	return k3RegisteredRequest{OwnerCapacity: owners, HistoryCapacity: history, Instance: [16]byte{7}, elapsed: clock.read}
}
func k3ActualService(t *testing.T, request *k3RegisteredRequest) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{k3Registered: request})
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}
func k3ActualPair(t *testing.T, accepted, dialer *Service) f1ProofFixture {
	t.Helper()
	owner := newF1FixtureOwner()
	server, client := tcpPair(t)
	original := registryFixtureAddressConn{Conn: server}
	owner.rawServer, owner.rawClient = original, client
	t.Cleanup(func() { owner.stopAndJoin(t) }) // before real TLS/auth/core workers
	// Reuse genuine sessionv2 Accept/Dial and original registry/auth owners.
	// Original owned local socket pair/stable address fixture; no F1 capability,
	// App.Run/Service.Run, external workload or positive synthetic map seeding.
	return f1PairOnRaw(t, false, nil, accepted, dialer, owner, original, client)
}
func k3ActualSnapshot(t *testing.T, s *Service, owners, identities int) k3RegisteredSnapshot {
	t.Helper()
	out := s.k3RegisteredSnapshot()
	if out.Population != "RegisteredProvenV2" || out.Qualification != "UNKNOWN" || out.AttributionBound || !out.PastOnly || out.RegisteredOwners != owners || out.DistinctIdentities != identities || len(out.Transitions) != int(out.Sequence) {
		t.Fatal("unexpected registered-proof prefix", out)
	}
	for i, row := range out.Transitions {
		if row.Sequence != uint64(i+1) || row.Clock.Before < 0 || row.Clock.After < row.Clock.Before {
			t.Fatal("unordered transition", row)
		}
	}
	return out
}
func k3SamePrefix(t *testing.T, before, after k3RegisteredSnapshot, freezeCut bool) {
	t.Helper()
	if before.Sequence != after.Sequence || before.RegisteredOwners != after.RegisteredOwners || before.DistinctIdentities != after.DistinctIdentities || before.Baseline != after.Baseline || !reflect.DeepEqual(before.Transitions, after.Transitions) || freezeCut && before.Cut != after.Cut {
		t.Fatal("coherent prefix changed", before, after)
	}
}
func k3ActualFault(t *testing.T, s *Service, before k3RegisteredSnapshot) k3RegisteredSnapshot {
	t.Helper()
	after := s.k3RegisteredSnapshot()
	if after.Status != "UNKNOWN" || after.Reason == "" {
		t.Fatal("fault not sticky UNKNOWN", after)
	}
	k3SamePrefix(t, before, after, true)
	s.peerMu.RLock()
	released := s.k3Registered.owners == nil && s.k3Registered.refs == nil && s.k3Registered.clock == nil
	s.peerMu.RUnlock()
	if !released {
		t.Fatal("fault retained strong owner/session/auth graphs or clock callback")
	}
	return after
}
func k3Publish(t *testing.T, s *Service, p *peerSession) {
	t.Helper()
	if !s.registerCMSession(p, p.address) {
		t.Fatal("original session publication refused")
	}
}
func k3Withdraw(s *Service, p *peerSession) {
	s.onCMSessionTeardown(SessionInfo{Address: p.address, DialAddress: p.address, Session: p})
}
func k3Auth(t *testing.T, s *Service, id domain.ConnID) *connauth.State {
	t.Helper()
	auth := s.connAuthStateByID(id)
	if _, ok := auth.ProvenIdentity(); !ok {
		t.Fatal("fixture lacks actual inbound v2 proof")
	}
	return auth
}
func k3Wait(t *testing.T, ch <-chan struct{}, deadline time.Time) {
	t.Helper()
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal("owned K3 barrier/join deadline")
	}
}

func TestK3RegisteredActualOwners(t *testing.T) {
	t.Run("constructor-empty-baseline-first-real-proof-and-default-nil", func(t *testing.T) {
		clock := newK3ActualClock()
		request := k3ActualRequest(clock, 8, 64)
		s := k3ActualService(t, &request)
		peer := k3ActualService(t, nil)
		empty := k3ActualSnapshot(t, s, 0, 0)
		if empty.Sequence != 0 || empty.Instance != request.Instance || s.k3Registered == nil || peer.k3Registered != nil {
			t.Fatal("late/default source attachment", empty)
		}
		ordinary := peer.k3RegisteredSnapshot()
		if ordinary.Status != "OFF" || ordinary.Qualification != "UNKNOWN" || ordinary.AttributionBound || !ordinary.PastOnly || ordinary.Transitions != nil {
			t.Fatal("ordinary source activated", ordinary)
		}
		f := k3ActualPair(t, s, peer)
		// Before FIRST post-auth Snapshot, the actual auth/publication hook must
		// already own the proof count and its row; a polling walk cannot mint it.
		s.peerMu.RLock()
		recorded := s.k3Registered.sequence == 1 && s.k3Registered.length == 1 && len(s.k3Registered.owners) == 1 && len(s.k3Registered.refs) == 1
		s.peerMu.RUnlock()
		if !recorded {
			t.Fatal("first real proof was not recorded before read publication")
		}
		got := k3ActualSnapshot(t, s, 1, 1)
		row := got.Transitions[0]
		if row.Added.Direction != "inbound" || row.Added.ConnID != f.inbound.id || row.AddedIdentity.IsZero() || row.Removed != (k3RegisteredOwnerRef{}) {
			t.Fatal("not original inbound owner", row)
		}
		s.peerMu.Lock()
		s.setTrackedByIDLocked(f.inbound.id, false)
		s.peerMu.Unlock()
		trackedIgnored := k3ActualSnapshot(t, s, 1, 1)
		k3SamePrefix(t, got, trackedIgnored, false)
		// Physical transport End alone does not erase a still-registered immutable
		// auth proof. This is registered population, not socket continuity.
		f.inbound.end()
		afterEnd := k3ActualSnapshot(t, s, 1, 1)
		k3SamePrefix(t, got, afterEnd, false)
		f.fixtureOwner.inboundCore.Close()
		registryUnregister(s, f.fixtureOwner.inboundWire)
		gone := k3ActualSnapshot(t, s, 0, 0)
		if gone.Sequence != got.Sequence+1 {
			t.Fatal("original unregister absent", gone)
		}
		if peer.k3Registered != nil {
			t.Fatal("proof work enabled ordinary peer observer")
		}
	})
	t.Run("actual-bidirectional-dedup-and-outbound-core-alias", func(t *testing.T) {
		clock := newK3ActualClock()
		request := k3ActualRequest(clock, 8, 64)
		s := k3ActualService(t, &request)
		peer := k3ActualService(t, nil)
		first := k3ActualPair(t, s, peer)
		second := k3ActualPair(t, s, peer)
		before := k3ActualSnapshot(t, s, 2, 1)
		reverse := k3ActualPair(t, peer, s)
		// attachOutboundNetCore is a registry alias, not another inbound owner.
		alias := k3ActualSnapshot(t, s, 2, 1)
		k3SamePrefix(t, before, alias, false)
		k3Publish(t, s, reverse.outbound)
		both := k3ActualSnapshot(t, s, 3, 1)
		if both.Sequence != before.Sequence+1 || both.Transitions[len(both.Transitions)-1].Added.Direction != "outbound" {
			t.Fatal("session publication not original/deduplicated", both)
		}
		s.clearConnAuth(first.inbound.id)
		k3ActualSnapshot(t, s, 2, 1)
		s.clearConnAuth(second.inbound.id)
		k3ActualSnapshot(t, s, 1, 1)
		k3Withdraw(s, reverse.outbound)
		last := k3ActualSnapshot(t, s, 0, 0)
		if last.Transitions[len(last.Transitions)-1].DistinctIdentities != 0 {
			t.Fatal("last original owner left union member")
		}
	})
	t.Run("same-identity-atomic-replacement-versus-separated-delete-readd", func(t *testing.T) {
		clock := newK3ActualClock()
		request := k3ActualRequest(clock, 8, 64)
		s := k3ActualService(t, &request)
		peer := k3ActualService(t, nil)
		first := k3ActualPair(t, peer, s)
		second := k3ActualPair(t, peer, s)
		if first.outbound.address != second.outbound.address || first.outbound == second.outbound || first.outbound.connID == second.outbound.connID {
			t.Fatal("fixture lacks distinct genuine owners at same fixture address")
		}
		k3Publish(t, s, first.outbound)
		before := k3ActualSnapshot(t, s, 1, 1)
		k3Publish(t, s, second.outbound)
		atomic := k3ActualSnapshot(t, s, 1, 1)
		if atomic.Sequence != before.Sequence+1 {
			t.Fatal("atomic replacement split into artificial gap", atomic)
		}
		row := atomic.Transitions[len(atomic.Transitions)-1]
		if row.Removed.ConnID != first.outbound.connID || row.Added.ConnID != second.outbound.connID || row.RemovedIdentity != row.AddedIdentity || row.Added.Generation <= row.Removed.Generation || row.RegisteredOwners != 1 || row.DistinctIdentities != 1 {
			t.Fatal("replacement row not whole owner transaction", row)
		}
		k3Withdraw(s, second.outbound)
		k3Publish(t, s, second.outbound)
		separate := k3ActualSnapshot(t, s, 1, 1)
		if separate.Sequence != atomic.Sequence+2 || separate.Transitions[len(separate.Transitions)-2].DistinctIdentities != 0 || separate.Transitions[len(separate.Transitions)-1].DistinctIdentities != 1 {
			t.Fatal("between-read delete/readd hidden by same final population", separate)
		}
	})
	t.Run("stale-connection-and-session-teardown-cannot-borrow-replacement", func(t *testing.T) {
		clock := newK3ActualClock()
		request := k3ActualRequest(clock, 8, 64)
		s := k3ActualService(t, &request)
		peer := k3ActualService(t, nil)
		first := k3ActualPair(t, peer, s)
		second := k3ActualPair(t, peer, s)
		k3Publish(t, s, first.outbound)
		k3Publish(t, s, second.outbound)
		before := k3ActualSnapshot(t, s, 1, 1)
		k3Withdraw(s, first.outbound)
		k3SamePrefix(t, before, k3ActualSnapshot(t, s, 1, 1), false)
		s.withdrawRefusedCMSession(first.outbound, first.outbound.address, 1, errors.New("controlled stale refusal"))
		k3SamePrefix(t, before, k3ActualSnapshot(t, s, 1, 1), false)
		s.withdrawServedCMSession(first.outbound, first.outbound.address, 1, errors.New("controlled stale served teardown"))
		k3SamePrefix(t, before, k3ActualSnapshot(t, s, 1, 1), false)
		oldInbound := k3ActualPair(t, s, peer)
		oldID := oldInbound.inbound.id
		oldInbound.fixtureOwner.inboundCore.Close()
		registryUnregister(s, oldInbound.fixtureOwner.inboundWire)
		replacement := k3ActualPair(t, s, peer)
		if oldID == replacement.inbound.id {
			t.Fatal("ConnID unexpectedly reused")
		}
		stable := k3ActualSnapshot(t, s, 2, 1)
		s.clearConnAuth(oldID)
		s.setConnAuthStateByID(oldID, nil)
		oldInbound.inbound.end()
		k3SamePrefix(t, stable, k3ActualSnapshot(t, s, 2, 1), false)
	})
	t.Run("legacy-claimed-v31-unverified-and-hidden-proof-mismatch-excluded", func(t *testing.T) {
		clock := newK3ActualClock()
		request := k3ActualRequest(clock, 8, 64)
		s := k3ActualService(t, &request)
		peer := k3ActualService(t, nil)
		f := k3ActualPair(t, s, peer)
		actual := k3Auth(t, s, f.inbound.id)
		proof, ok := actual.ProvenIdentity()
		if !ok {
			t.Fatal("missing genuine proof")
		}
		hello := actual.Hello
		hello.Version = 31
		legacy := &connauth.State{Hello: hello, Verified: true}
		if _, ok := legacy.ProvenIdentity(); ok {
			t.Fatal("Verified/claimedv31 fabricated v2")
		}
		s.setConnAuthStateByID(f.inbound.id, legacy)
		zero := k3ActualSnapshot(t, s, 0, 0)
		if zero.Status == "UNKNOWN" {
			t.Fatal("ordinary false proof treated as accessible contradiction")
		}
		unverified := &connauth.State{Hello: hello, Verified: false}
		s.setConnAuthStateByID(f.inbound.id, unverified)
		k3SamePrefix(t, zero, k3ActualSnapshot(t, s, 0, 0), false)
		wrongHello := hello
		wrongHello.Address = (domain.PeerIdentity{99}).String()
		hiddenMismatch := connauth.ProvenBySessionV2(wrongHello, proof)
		if _, ok := hiddenMismatch.ProvenIdentity(); ok {
			t.Fatal("hidden proof contradicted original hello validation")
		}
		s.setConnAuthStateByID(f.inbound.id, hiddenMismatch)
		k3SamePrefix(t, zero, k3ActualSnapshot(t, s, 0, 0), false)
		s.setConnAuthStateByID(f.inbound.id, actual)
		k3ActualSnapshot(t, s, 1, 1)
	})
	t.Run("actual-auth-overwrite-clear-and-accessible-outbound-conflict", func(t *testing.T) {
		clock := newK3ActualClock()
		request := k3ActualRequest(clock, 8, 64)
		s := k3ActualService(t, &request)
		peer := k3ActualService(t, nil)
		f := k3ActualPair(t, s, peer)
		actual := k3Auth(t, s, f.inbound.id)
		proof, _ := actual.ProvenIdentity()
		before := k3ActualSnapshot(t, s, 1, 1)
		replacement := connauth.ProvenBySessionV2(actual.Hello, proof)
		s.setConnAuthStateByID(f.inbound.id, replacement)
		after := k3ActualSnapshot(t, s, 1, 1)
		if after.Sequence != before.Sequence+1 {
			t.Fatal("auth pointer overwrite was not atomic owner replacement")
		}
		row := after.Transitions[len(after.Transitions)-1]
		if row.RemovedIdentity != row.AddedIdentity || row.RegisteredOwners != 1 || row.DistinctIdentities != 1 {
			t.Fatal("same-proof auth replacement manufactured gap", row)
		}
		s.clearConnAuth(f.inbound.id)
		cleared := k3ActualSnapshot(t, s, 0, 0)
		s.clearConnAuth(f.inbound.id)
		k3SamePrefix(t, cleared, k3ActualSnapshot(t, s, 0, 0), false)
		s.setConnAuthStateByID(f.inbound.id, replacement)
		k3ActualSnapshot(t, s, 1, 1)
		// Explicit impossible-metadata negative: the original genuine proof is
		// unchanged, but its immutable outbound identity field is contradicted.
		otherClock := newK3ActualClock()
		otherReq := k3ActualRequest(otherClock, 8, 64)
		out := k3ActualService(t, &otherReq)
		pair := k3ActualPair(t, peer, out)
		frozen := k3ActualSnapshot(t, out, 0, 0)
		original := pair.outbound.peerIdentity
		pair.outbound.peerIdentity = domain.PeerIdentity{77}
		t.Cleanup(func() { pair.outbound.peerIdentity = original })
		if !out.registerCMSession(pair.outbound, pair.outbound.address) {
			t.Fatal("observation fault vetoed original map publication")
		}
		k3ActualFault(t, out, frozen)
		out.peerMu.RLock()
		published := out.sessions[pair.outbound.address] == pair.outbound
		out.peerMu.RUnlock()
		if !published {
			t.Fatal("admission/publication changed on observer fault")
		}
		pair.outbound.peerIdentity = original
		k3Withdraw(out, pair.outbound)
		k3ActualFault(t, out, frozen)
	})
	t.Run("bounded-membership-history-and-arithmetic-fault-freeze", func(t *testing.T) {
		// Owner exhaustion must not veto a second genuine authentication, even
		// when both sessions prove the same identity and union count would stay1.
		clock := newK3ActualClock()
		req := k3ActualRequest(clock, 1, 8)
		s := k3ActualService(t, &req)
		peer := k3ActualService(t, nil)
		k3ActualPair(t, s, peer)
		firstPrefix := k3ActualSnapshot(t, s, 1, 1)
		clock.configure(firstPrefix.Cut.After, 0, nil)
		frozen := k3ActualSnapshot(t, s, 1, 1) // keep all intervening no-population cuts equal
		second := k3ActualPair(t, s, peer)
		k3Auth(t, s, second.inbound.id)
		k3ActualFault(t, s, frozen)
		second.fixtureOwner.inboundCore.Close()
		registryUnregister(s, second.fixtureOwner.inboundWire)
		k3ActualFault(t, s, frozen)
		historyClock := newK3ActualClock()
		historyReq := k3ActualRequest(historyClock, 8, 1)
		history := k3ActualService(t, &historyReq)
		f := k3ActualPair(t, history, peer)
		prefix := k3ActualSnapshot(t, history, 1, 1)
		history.clearConnAuth(f.inbound.id)
		if history.connAuthStateByID(f.inbound.id) != nil {
			t.Fatal("history cap vetoed actual auth clear")
		}
		k3ActualFault(t, history, prefix)
		// Internal counters are changed ONLY as negative overflow seams, never to
		// synthesize proof owners or a successful transition source.
		// A long existing map key is NOT a new network policy. The observation
		// refuses before retaining a ticket/clock while original registration wins.
		longClock := newK3ActualClock()
		longReq := k3ActualRequest(longClock, 8, 8)
		longSvc := k3ActualService(t, &longReq)
		longPair := k3ActualPair(t, peer, longSvc)
		longPrefix := k3ActualSnapshot(t, longSvc, 0, 0)
		originalAddress := longPair.outbound.address
		longAddress := domain.PeerAddress(strings.Repeat("x", 1025))
		longPair.outbound.address = longAddress
		t.Cleanup(func() { k3Withdraw(longSvc, longPair.outbound); longPair.outbound.address = originalAddress })
		callsBefore := longClock.count()
		if !longSvc.registerCMSession(longPair.outbound, longAddress) {
			t.Fatal("long observation key changed original registration")
		}
		longSvc.peerMu.RLock()
		present := longSvc.sessions[longAddress] == longPair.outbound
		longSvc.peerMu.RUnlock()
		if !present || longClock.count() != callsBefore {
			t.Fatal("address refusal retained/read observation clock or vetoed actual map")
		}
		k3ActualFault(t, longSvc, longPrefix)
		k3Withdraw(longSvc, longPair.outbound)
		longPair.outbound.address = originalAddress
		for _, field := range []string{"sequence", "generation"} {
			c := newK3ActualClock()
			q := k3ActualRequest(c, 8, 8)
			svc := k3ActualService(t, &q)
			p := k3ActualPair(t, svc, peer)
			svc.peerMu.Lock()
			if field == "sequence" {
				svc.k3Registered.sequence = ^uint64(0)
			} else {
				svc.k3Registered.generation = ^uint64(0)
			}
			svc.peerMu.Unlock()
			before := svc.k3RegisteredSnapshot()
			if field == "sequence" {
				svc.clearConnAuth(p.inbound.id)
			} else {
				auth := k3Auth(t, svc, p.inbound.id)
				proof, _ := auth.ProvenIdentity()
				svc.setConnAuthStateByID(p.inbound.id, connauth.ProvenBySessionV2(auth.Hello, proof))
			}
			k3ActualFault(t, svc, before)
		}
	})
	t.Run("clock-no-progress-sticky-failure-and-joined-snapshot-barrier", func(t *testing.T) {
		equal := newK3ActualClock()
		equal.configure(time.Second, 0, nil)
		request := k3ActualRequest(equal, 8, 64)
		s := k3ActualService(t, &request)
		peer := k3ActualService(t, nil)
		if out := k3ActualSnapshot(t, s, 0, 0); out.Status != "NO_PROGRESS" {
			t.Fatal("equal clocks invented positive interval", out)
		}
		f := k3ActualPair(t, s, peer)
		if out := k3ActualSnapshot(t, s, 1, 1); out.Status != "NO_PROGRESS" {
			t.Fatal("zero-width events faulted or invented duration", out)
		}
		equal.configure(2*time.Second, time.Nanosecond, nil)
		positive := k3ActualSnapshot(t, s, 1, 1)
		if positive.Status != "PAST" {
			t.Fatal("idle clock cut did not progress", positive)
		}
		returned := positive
		returned.Transitions[0].Sequence = 99
		returned.Transitions[0].AddedIdentity = domain.PeerIdentity{66}
		again := k3ActualSnapshot(t, s, 1, 1)
		if again.Transitions[0].Sequence != 1 || again.Transitions[0].AddedIdentity == (domain.PeerIdentity{66}) {
			t.Fatal("snapshot aliased immutable stored prefix")
		}
		// Real original auth Clear is paused by its source clock under peerMu;
		// a reader cannot publish the half-mutated source across that owner lock.
		entered, release, writerDone, readerDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once, releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		t.Cleanup(unblock)
		equal.setHook(func() { once.Do(func() { close(entered); <-release }) })
		deadline := time.Now().Add(2 * time.Second)
		go func() { defer close(writerDone); s.clearConnAuth(f.inbound.id) }()
		t.Cleanup(func() { unblock(); k3Wait(t, writerDone, time.Now().Add(2*time.Second)) })
		k3Wait(t, entered, deadline)
		if s.peerMu.TryRLock() {
			s.peerMu.RUnlock()
			t.Fatal("clock callback escaped actual owner mutation lock")
		}
		var observed k3RegisteredSnapshot
		go func() { defer close(readerDone); observed = s.k3RegisteredSnapshot() }()
		t.Cleanup(func() {
			unblock()
			end := time.Now().Add(2 * time.Second)
			k3Wait(t, writerDone, end)
			k3Wait(t, readerDone, end)
		})
		select {
		case <-readerDone:
			t.Fatal("read crossed paused owner mutation")
		default:
		}
		unblock()
		k3Wait(t, writerDone, deadline)
		k3Wait(t, readerDone, deadline)
		equal.setHook(nil)
		if observed.RegisteredOwners != 0 || observed.Sequence != again.Sequence+1 {
			t.Fatal("joined read saw half transaction", observed)
		}
		// Clock errors/regressions freeze the ENTIRE good prefix, not just history.
		for _, regress := range []bool{false, true} {
			c := newK3ActualClock()
			q := k3ActualRequest(c, 8, 8)
			svc := k3ActualService(t, &q)
			pair := k3ActualPair(t, svc, peer)
			before := k3ActualSnapshot(t, svc, 1, 1)
			if regress {
				c.configure(0, 0, nil)
			} else {
				c.configure(time.Hour, 0, errors.New("observation-only clock unavailable"))
			}
			svc.clearConnAuth(pair.inbound.id)
			if svc.connAuthStateByID(pair.inbound.id) != nil {
				t.Fatal("clock failure changed auth clear")
			}
			k3ActualFault(t, svc, before)
			calls := c.count()
			c.configure(2*time.Hour, time.Nanosecond, nil)
			k3ActualFault(t, svc, before)
			if c.count() != calls {
				t.Fatal("faulted snapshot polled or idle-advanced clock")
			}
		}
		// Before succeeds, then the original ClearAuth mutation completes before
		// the After read fails. The last coherent population must remain frozen;
		// clock uncertainty cannot roll back admission or invent a partial row.
		afterClock := newK3ActualClock()
		afterReq := k3ActualRequest(afterClock, 8, 8)
		afterSvc := k3ActualService(t, &afterReq)
		afterPair := k3ActualPair(t, afterSvc, peer)
		afterPrefix := k3ActualSnapshot(t, afterSvc, 1, 1)
		afterCalls := afterClock.count()
		afterClock.setHook(func() {
			afterClock.mu.Lock()
			afterClock.err = errors.New("After clock unavailable after original auth mutation")
			afterClock.hook = nil
			afterClock.mu.Unlock()
		})
		afterSvc.clearConnAuth(afterPair.inbound.id)
		if afterSvc.connAuthStateByID(afterPair.inbound.id) != nil {
			t.Fatal("After clock failure vetoed original auth clear")
		}
		if afterClock.count() != afterCalls+2 {
			t.Fatal("After failure did not follow one successful Before and actual mutation")
		}
		k3ActualFault(t, afterSvc, afterPrefix)
		afterFrozenCalls := afterClock.count()
		afterClock.configure(3*time.Hour, time.Nanosecond, nil)
		k3ActualFault(t, afterSvc, afterPrefix)
		if afterClock.count() != afterFrozenCalls {
			t.Fatal("After-fault snapshot retained clock or advanced frozen prefix")
		}
		for _, bad := range []k3RegisteredRequest{{OwnerCapacity: 0, HistoryCapacity: 1, Instance: [16]byte{1}}, {OwnerCapacity: k3RegisteredMaxOwners + 1, HistoryCapacity: 1, Instance: [16]byte{1}}, {OwnerCapacity: 1, HistoryCapacity: k3RegisteredMaxHistory + 1, Instance: [16]byte{1}}, {OwnerCapacity: 1, HistoryCapacity: 1}} {
			calls := 0
			bad.elapsed = func() (time.Duration, error) { calls++; return 1, nil }
			invalid := k3ActualService(t, &bad)
			out := invalid.k3RegisteredSnapshot()
			if out.Status != "UNKNOWN" || out.Sequence != 0 || out.RegisteredOwners != 0 || calls != 0 {
				t.Fatal("invalid request performed observation work or admitted a prefix", out, calls)
			}
		}
	})
}

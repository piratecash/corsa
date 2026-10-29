//go:build d1_resource_probe

package node

import (
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func readD1Checkpoint(r *D1FindNodeResourceReadout, s *Service) D1FindNodeCheckpoint {
	return r.CheckpointForService(s)
}

// Real sessionv2 proof/seed and original registry/auth/publication on a pipe.
// It creates no Session authority, FixtureDomain, Run or listening socket.
func d1CheckpointPair(t *testing.T, s *Service, f1 bool) (f1ProofFixture, *D1FindNodeResourceReadout) {
	t.Helper()
	var peer *Service
	var peerReadout *D1FindNodeResourceReadout
	// Match existing owned proof fixtures: exclude unrelated route-grace timers.
	s.routeWithdrawalGracePeriodTest = -1
	if f1 {
		peer, peerReadout = newD1ResourceForTest(t)
		peer.routeWithdrawalGracePeriodTest = -1
	} else {
		peer = f1Service(t)
	}
	owner := newF1FixtureOwner()
	left, right := net.Pipe()
	owner.rawServer, owner.rawClient = left, right
	t.Cleanup(func() { owner.stopAndJoin(t) })
	// Both actual diagnostic intros already contain F1; no capability injection.
	f := f1PairOnRaw(t, false, nil, s, peer, owner, left, right)
	s.publishAcceptedProofTransport(f.inbound)
	return f, peerReadout
}

func d1CheckpointUnknown(t *testing.T, result D1FindNodeCheckpoint) {
	t.Helper()
	if result.MembershipComplete || result.UnknownReason == "" || result.ActiveCount != 0 || result.F1Count != 0 || result.RegistryVisits != 0 || result.Rows != ([2]D1FindNodeMemberRow{}) {
		t.Fatal("partial/unsupported checkpoint became complete", result)
	}
	if !strings.HasPrefix(result.RetirementStatus, "UNKNOWN:") || !strings.HasPrefix(result.DrainStatus, "UNKNOWN:") {
		t.Fatal("unproved retirement/drain", result)
	}
}

func TestD1MembershipCheckpointOriginalRowsAndEnd(t *testing.T) {
	s, r := newD1ResourceForTest(t)
	check := func(active, f1 int) D1FindNodeCheckpoint {
		t.Helper()
		v := readD1Checkpoint(r, s)
		if !v.MembershipComplete || v.UnknownReason != "" || v.ServiceIdentity != s.Address() || v.ActiveCount != active || v.F1Count != f1 {
			t.Fatal("incomplete original membership", v)
		}
		if !strings.HasPrefix(v.RetirementStatus, "UNKNOWN:") || !strings.HasPrefix(v.DrainStatus, "UNKNOWN:") {
			t.Fatal("membership invented retirement/drain", v)
		}
		return v
	}
	initial := check(0, 0)
	var valueOnly func(reflect.Type) bool
	valueOnly = func(typ reflect.Type) bool {
		switch typ.Kind() {
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				if !valueOnly(typ.Field(i).Type) {
					return false
				}
			}
			return true
		case reflect.Array:
			return valueOnly(typ.Elem())
		case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return true
		default:
			return false
		}
	}
	if !valueOnly(reflect.TypeOf(initial)) {
		t.Fatal("checkpoint exported reference/authority graph")
	}
	a, _ := d1CheckpointPair(t, s, true)
	b, _ := d1CheckpointPair(t, s, true)
	v := check(2, 2)
	seen := map[domain.ConnID]domain.PeerIdentity{}
	for _, row := range v.Rows {
		if !row.PublishedF1 || row.ConnID == 0 || row.Identity.IsZero() {
			t.Fatal("unbound row", row)
		}
		seen[row.ConnID] = row.Identity
	}
	if seen[a.inbound.id] != domain.PeerIdentityFromWire(a.dialer.Address()) || seen[b.inbound.id] != domain.PeerIdentityFromWire(b.dialer.Address()) {
		t.Fatal("identity/ConnID substituted", v)
	}
	if v.Rows[0].ConnID >= v.Rows[1].ConnID {
		t.Fatal("unstable order")
	}
	b.inbound.life.mu.Lock()
	old := b.inbound.life.findNode.session
	b.inbound.life.mu.Unlock()
	b.inbound.end()
	if old.ConnID() != b.inbound.id || old.Identity().IsZero() {
		t.Fatal("test did not retain ended original IDs")
	}
	v = check(1, 1)
	if v.RegistryVisits != 2 || v.Rows[0].ConnID != a.inbound.id {
		t.Fatal("ended tombstone confused with empty registry", v)
	}
	// Remove only this fixture's original ended entry before adding U.
	s.peerMu.Lock()
	s.unregisterConnLocked(b.fixtureOwner.inboundWire)
	s.peerMu.Unlock()
	u, _ := d1CheckpointPair(t, s, false)
	v = check(2, 1)
	var unnegotiated D1FindNodeMemberRow
	for _, row := range v.Rows {
		if row.ConnID == u.inbound.id {
			unnegotiated = row
		}
	}
	if unnegotiated.ConnID == 0 || unnegotiated.PublishedF1 || unnegotiated.Identity != domain.PeerIdentityFromWire(u.dialer.Address()) {
		t.Fatal("non-F1 genuine identity missing/substituted", v)
	}
	v.Rows[0].ConnID = 999
	if readD1Checkpoint(r, s).Rows[0].ConnID == 999 {
		t.Fatal("returned value changed owner")
	}
	a.inbound.end()
	u.inbound.end()
	check(0, 0)
}

func TestD1MembershipCheckpointIncompleteAndForeignRefuse(t *testing.T) {
	s, r := newD1ResourceForTest(t)
	d1CheckpointUnknown(t, readD1Checkpoint(nil, s))
	d1CheckpointUnknown(t, readD1Checkpoint(r, nil))
	d1CheckpointUnknown(t, readD1Checkpoint(r, &Service{}))
	f, peerReadout := d1CheckpointPair(t, s, true)
	d1CheckpointUnknown(t, readD1Checkpoint(peerReadout, f.dialer)) // genuine outbound is outside closed listener scope
	l := f.inbound.life
	l.mu.Lock()
	entry := l.entry
	l.entry = &connEntry{}
	l.mu.Unlock()
	d1CheckpointUnknown(t, readD1Checkpoint(r, s))
	l.mu.Lock()
	l.entry = entry
	l.mu.Unlock()
	l.mu.Lock()
	witness := l.degreeWitness
	l.mu.Unlock()
	s.degree.mu.Lock()
	originalWitness := s.degree.active[witness.slot].witness
	copiedWitness := *originalWitness // identical scalar key/count/slot/generation, different original pointer
	s.degree.active[witness.slot].witness = &copiedWitness
	s.degree.mu.Unlock()
	defer func() {
		s.degree.mu.Lock()
		if s.degree.active[witness.slot].witness == &copiedWitness {
			s.degree.active[witness.slot].witness = originalWitness
		}
		s.degree.mu.Unlock()
	}()
	d1CheckpointUnknown(t, readD1Checkpoint(r, s))
	s.degree.mu.Lock()
	s.degree.active[witness.slot].witness = originalWitness
	s.degree.mu.Unlock()
	// Registry-only unknown keys consume coverage, never mint proof or F1.
	s.peerMu.Lock()
	publication := s.beginRegistryPublication()
	for id := domain.ConnID(90001); id < 90006; id++ {
		s.conns[id] = nil
	}
	publication.Complete()
	s.peerMu.Unlock()
	d1CheckpointUnknown(t, readD1Checkpoint(r, s))
	s.peerMu.Lock()
	publication = s.beginRegistryPublication()
	for id := domain.ConnID(90001); id < 90006; id++ {
		delete(s.conns, id)
	}
	publication.Complete()
	s.peerMu.Unlock()
	s.degree.mu.Lock()
	poisoned := s.degree.poisoned
	s.degree.poisoned = true
	s.degree.mu.Unlock()
	d1CheckpointUnknown(t, readD1Checkpoint(r, s))
	s.degree.mu.Lock()
	s.degree.poisoned = poisoned
	s.degree.mu.Unlock()
	_, _ = d1CheckpointPair(t, s, true)
	_, _ = d1CheckpointPair(t, s, false) // third genuine active proof exceeds2
	d1CheckpointUnknown(t, readD1Checkpoint(r, s))
	closed, handle := newD1ResourceForTest(t)
	closed.runCancel()
	d1CheckpointUnknown(t, readD1Checkpoint(handle, closed))
}

func TestD1MembershipCheckpointPublicationAndEndRace(t *testing.T) {
	s, r := newD1ResourceForTest(t)
	// Isolate this lock barrier from delivery jobs without cancelling the owner.
	// Never-Run construction starts no loops; future auth jobs are refused.
	// This test-only quiescence is not a production drain verdict.
	s.closeBackgroundAdmission()
	s.WaitBackground()
	publication := r.publisher.BeginPublication()
	d1CheckpointUnknown(t, readD1Checkpoint(r, s)) // actual BeginCapture sees busy W
	publication.Complete()
	f, _ := d1CheckpointPair(t, s, true)
	// Force a completed original W generation between the two private stamps.
	// Holding common blocks the reader only after its first stamp and peer R.
	l := f.inbound.life
	l.mu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			l.mu.Unlock()
		}
	})
	result := make(chan D1FindNodeCheckpoint, 1)
	readerDone := make(chan struct{})
	f.fixtureOwner.observerDone = readerDone
	go func() { defer close(readerDone); result <- readD1Checkpoint(r, s) }()
	waitFor(t, 2*time.Second, "checkpoint reached original common under peer R", func() bool {
		if s.peerMu.TryLock() {
			s.peerMu.Unlock()
			return false
		}
		return true
	})
	publication = r.publisher.BeginPublication()
	publication.Complete()
	l.mu.Unlock()
	locked = false
	select {
	case <-readerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("checkpoint generation reader did not join")
	}
	d1CheckpointUnknown(t, <-result)
	start, ended := make(chan struct{}), make(chan struct{})
	f.fixtureOwner.caseDone = ended // join even after an assertion failure
	go func() { <-start; f.inbound.end(); close(ended) }()
	close(start)
	// Race-tolerant probes, not a promise of overlapping scheduling or stale-branch coverage.
	for i := 0; i < 16; i++ {
		v := readD1Checkpoint(r, s)
		if !v.MembershipComplete {
			d1CheckpointUnknown(t, v)
			continue
		}
		if v.ActiveCount != v.F1Count || v.ActiveCount < 0 || v.ActiveCount > 1 {
			t.Fatal("partial race result", v)
		}
		if v.ActiveCount == 1 && (v.Rows[0].ConnID != f.inbound.id || v.Rows[0].Identity != domain.PeerIdentityFromWire(f.dialer.Address()) || !v.Rows[0].PublishedF1) {
			t.Fatal("race substituted original", v)
		}
	}
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("original End not confirmed within fixture bound")
	}
	v := readD1Checkpoint(r, s)
	if !v.MembershipComplete || v.ActiveCount != 0 || v.F1Count != 0 || v.RegistryVisits != 1 {
		t.Fatal("confirmed End failed to remove membership", v)
	}
	if !strings.HasPrefix(v.RetirementStatus, "UNKNOWN:") {
		t.Fatal("End callback completion overstated", v)
	}
}

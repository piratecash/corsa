package findnode

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func provenFixture(t *testing.T, connID domain.ConnID, group domain.SourceGroup, opened Instant, remote ...*identity.Identity) (Session, *identity.Identity) {
	t.Helper()
	a, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if len(remote) == 1 {
		b = remote[0]
	}
	network := domain.NetworkID("gazeta-devnet")
	al, err := sessionv2.NewLocal(a, network)
	if err != nil {
		t.Fatal(err)
	}
	bl, err := sessionv2.NewLocal(b, network)
	if err != nil {
		t.Fatal(err)
	}
	certs, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := listener.Accept()
		if e != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()
	dial, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	other := <-accepted
	if other == nil {
		t.Fatal("accept")
	}
	t.Cleanup(func() { _ = dial.Close(); _ = other.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	intro := protocol.Frame{Version: 31, Capabilities: []string{string(domain.CapOverlayFindNodeV1)}}
	type result struct {
		s   sessionv2.ProvenSession
		err error
	}
	ready := make(chan result, 1)
	go func() {
		s, e := sessionv2.Accept(ctx, other, bl, intro, certs, sessionv2.DefaultTimeouts)
		ready <- result{s, e}
	}()
	p, err := sessionv2.Dial(ctx, dial, al, intro, sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	if r := <-ready; r.err != nil {
		t.Fatal(r.err)
	}
	s, err := NewSession(p, connID, group, opened, []domain.Capability{domain.CapOverlayFindNodeV1})
	if err != nil {
		t.Fatal(err)
	}
	return s, b
}
func ordinary(SessionInfo) (SessionClass, error) { return ClassOrdinary, nil }
func viewFor(t *testing.T, target domain.PeerIdentity, now Instant, sessions ...Session) PrioritySnapshot {
	t.Helper()
	v, e := NewPrioritySnapshot(target, sessions, now, ordinary)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func request(id domain.FindNodeRequestID, target domain.PeerIdentity, challenge byte) protocol.FindNodeRequest {
	var c domain.FindNodeChallenge
	c[0] = challenge
	return protocol.FindNodeRequest{ID: id, Target: target, Challenge: c}
}
func bodyFor(t *testing.T, id *identity.Identity) AnswerBody {
	t.Helper()
	key, e := identity.ParsePublicKey(id.PublicKey)
	if e != nil {
		t.Fatal(e)
	}
	return AnswerBody{Target: domain.PeerIdentityFromWire(id.Address), PublicKey: key, Signature: domain.FindNodeSignature{1, 2, 3}}
}

func TestZeroProofCannotGrantOverlayRights(t *testing.T) {
	if _, e := NewSession(sessionv2.ProvenSession{}, 1, "192.0.0.0/16", 0, []domain.Capability{domain.CapOverlayFindNodeV1}); !errors.Is(e, ErrUnauthenticated) {
		t.Fatalf("zero proof: %v", e)
	}
}
func TestTargetCopiesKeepSignedBodyAndUseEachSessionsID(t *testing.T) {
	first, key := provenFixture(t, 1, "192.0.0.0/16", 0)
	second, _ := provenFixture(t, 2, "198.51.0.0/16", 0)
	third, _ := provenFixture(t, 3, domain.SourceGroupLoopback, 0)
	fourth, _ := provenFixture(t, 4, "2001:db8::/32", 0)
	cache, e := NewTargetCache(bodyFor(t, key).Target)
	if e != nil {
		t.Fatal(e)
	}
	now := Instant(time.Second)
	v := viewFor(t, cache.Identity(), now, first, second, third, fourth)
	a, e := cache.Admit(first, request(11, cache.Identity(), 7), now, now+Instant(time.Second), v)
	if e != nil || a.Kind != TargetNeedsSignature {
		t.Fatalf("first %v %v", a.Kind, e)
	}
	b, e := cache.Admit(second, request(22, cache.Identity(), 7), now, now+Instant(time.Second), v)
	if e != nil || b.Kind != TargetWaiting {
		t.Fatalf("pending copy %v %v", b.Kind, e)
	}
	deliveries, e := cache.Complete(a.Work, bodyFor(t, key), now)
	if e != nil || len(deliveries) != 2 {
		t.Fatalf("complete %d %v", len(deliveries), e)
	}
	ids := map[domain.ConnID]domain.FindNodeRequestID{}
	for _, d := range deliveries {
		ids[d.Session.ConnID()] = d.Answer.ID
		if d.Answer.Signature != bodyFor(t, key).Signature {
			t.Fatal("changed signed body")
		}
	}
	if ids[1] != 11 || ids[2] != 22 {
		t.Fatal("cached first envelope ID leaked to another session")
	}
	c, e := cache.Admit(third, request(33, cache.Identity(), 7), now, now+Instant(time.Second), v)
	if e != nil || c.Kind != TargetAnswerReady || c.Answer.ID != 33 || c.Answer.Signature != deliveries[0].Answer.Signature {
		t.Fatalf("ready copy %v %v", c.Kind, e)
	}
	if _, e := cache.Admit(first, request(99, cache.Identity(), 7), now, now+Instant(time.Second), v); !errors.Is(e, ErrDuplicateLink) {
		t.Fatalf("same-link duplicate %v", e)
	}
	if _, e := cache.Admit(fourth, request(44, cache.Identity(), 7), now, now+Instant(time.Second), v); !errors.Is(e, ErrDuplicateExcess) {
		t.Fatalf("fourth link %v", e)
	}
}
func TestExpiredSigningCompletionCannotResurrectEvictedChallenge(t *testing.T) {
	source, key := provenFixture(t, 1, "192.0.0.0/16", 0)
	cache, _ := NewTargetCache(bodyFor(t, key).Target)
	now := Instant(time.Second)
	v := viewFor(t, cache.Identity(), now, source)
	old, e := cache.Admit(source, request(1, cache.Identity(), 9), now, now+1, v)
	if e != nil {
		t.Fatal(e)
	}
	later := now + 1
	fresh, e := cache.Admit(source, request(2, cache.Identity(), 9), later, later+1, viewFor(t, cache.Identity(), later, source))
	if e != nil {
		t.Fatal(e)
	}
	if _, e := cache.Complete(old.Work, bodyFor(t, key), later); !errors.Is(e, ErrStaleWork) {
		t.Fatalf("old work accepted: %v", e)
	}
	if _, e := cache.Complete(fresh.Work, bodyFor(t, key), later); e != nil {
		t.Fatal(e)
	}
	if got := cache.Snapshot(); got.Entries != 1 || got.Ready != 1 || got.Expired != 1 {
		t.Fatalf("snapshot %+v", got)
	}
}
func TestForwardAnswerBindsTargetSessionAndRestoresSourceID(t *testing.T) {
	source, _ := provenFixture(t, 1, "192.0.0.0/16", 0)
	target, key := provenFixture(t, 2, "198.51.0.0/16", 0)
	impostor, _ := provenFixture(t, 3, "2001:db8::/32", 0)
	table := NewPendingTable()
	req := request(123, target.Identity(), 6)
	forward, e := table.Reserve(source, target, req, 0)
	if e != nil {
		t.Fatal(e)
	}
	answer := bodyFor(t, key).Answer(forward.Request.ID)
	if _, e := table.Answer(impostor, answer, 1); !errors.Is(e, ErrUnmatchedAnswer) {
		t.Fatalf("another session %v", e)
	}
	answer.Target = impostor.Identity()
	if _, e := table.Answer(target, answer, 1); !errors.Is(e, ErrUnmatchedAnswer) {
		t.Fatalf("wrong target %v", e)
	}
	answer.Target = target.Identity()
	delivery, e := table.Answer(target, answer, 1)
	if e != nil || delivery.Answer.ID != 123 || delivery.Session != source {
		t.Fatalf("original session/id binding: %v", e)
	}
	if _, e := table.Answer(target, answer, 1); !errors.Is(e, ErrUnmatchedAnswer) {
		t.Fatalf("late duplicate %v", e)
	}
	if got := table.Snapshot(); got.Pending != 0 || got.Completed != 1 {
		t.Fatalf("release %+v", got)
	}
}
func TestPairWindowAndReservationCalendarDoNotRefundSpentBudget(t *testing.T) {
	source, _ := provenFixture(t, 999, "192.0.0.0/16", 0)
	key, _ := NewPairKey(source, domain.PeerIdentity{9})
	pair, _ := NewPairWindow(key)
	for range 3 {
		if !pair.Allow(0) {
			t.Fatal("legal pair burst refused")
		}
	}
	if pair.Allow(0) || pair.Allow(Instant(30*time.Second)-1) {
		t.Fatal("pair cap bypass")
	}
	if !pair.Allow(Instant(30 * time.Second)) {
		t.Fatal("pair expiry boundary")
	}
	if _, e := NewForwardCalendar(domain.PeerIdentity{9}, 0, 5); e == nil {
		t.Fatal("increased burst")
	}
	cal, e := NewForwardCalendar(domain.PeerIdentity{9}, 0, 0)
	if e != nil {
		t.Fatal(e)
	}
	first, e := cal.Reserve(0, Instant(2*time.Second))
	if e != nil || first.At() != Instant(time.Second) {
		t.Fatalf("slot %v %v", first.At(), e)
	}
	if !cal.Cancel(first) {
		t.Fatal("cancel failed")
	}
	next, e := cal.Reserve(0, Instant(2*time.Second))
	if e != nil || next.At() != Instant(2*time.Second) {
		t.Fatal("cancel refunded conservative reservation")
	}
	if _, e := cal.Reserve(0, Instant(2*time.Second)); !errors.Is(e, ErrBudget) {
		t.Fatalf("admitted past deadline %v", e)
	}
	if e := cal.Consume(next, Instant(time.Second)); !errors.Is(e, ErrNotReady) {
		t.Fatalf("early send %v", e)
	}
	if e := cal.Consume(next, Instant(2*time.Second)); e != nil {
		t.Fatal(e)
	}
	if e := cal.Consume(next, Instant(2*time.Second)); !errors.Is(e, ErrStaleWork) {
		t.Fatalf("double spend %v", e)
	}
	if _, e := (TimingProfile{}).LatestForward(0); e == nil {
		t.Fatal("unknown timing became zero")
	}
}

func TestPriorityUsesAuthoritativeSessionClassAndOnlyFourOldest(t *testing.T) {
	sessions := make([]Session, 7)
	for i := range sessions {
		sessions[i], _ = provenFixture(t, domain.ConnID(i+1), "192.0.0.0/16", Instant(time.Duration(i)*time.Second))
	}
	now := Instant(11 * time.Minute)
	classify := func(info SessionInfo) (SessionClass, error) {
		if _, ok := info.Proof.Identity(); !ok {
			t.Fatal("class provider got an unproven identity")
		}
		if info.ConnID == 1 {
			return ClassSelected, nil
		}
		return ClassOrdinary, nil
	}
	priorities, e := NewPrioritySnapshot(sessions[0].Identity(), sessions, now, classify)
	if e != nil {
		t.Fatal(e)
	}
	for i, session := range sessions {
		got, ok := priorities.Priority(session)
		want := PriorityNew
		if i == 0 {
			want = PriorityProtected
		} else if i <= 4 {
			want = PriorityAged
		}
		if !ok || got != want {
			t.Fatalf("session %d got %v want %v", i+1, got, want)
		}
	}
	if _, e := NewPrioritySnapshot(sessions[0].Identity(), sessions, now, nil); e == nil {
		t.Fatal("missing C5 class provider silently accepted")
	}
}
func TestTargetBoundedEvictionPreservesProtectedEntries(t *testing.T) {
	sessions := make([]Session, 33)
	var key *identity.Identity
	for i := range sessions {
		group := domain.SourceGroup("192.0.0.0/16")
		if i >= 24 {
			group = "198.51.0.0/16"
		}
		sessions[i], key = provenFixture(t, domain.ConnID(i+1), group, 0)
	}
	cache, _ := NewTargetCache(bodyFor(t, key).Target)
	classify := func(info SessionInfo) (SessionClass, error) {
		if info.ConnID == 1 {
			return ClassContact, nil
		}
		return ClassOrdinary, nil
	}
	priorities, e := NewPrioritySnapshot(cache.Identity(), sessions, 0, classify)
	if e != nil {
		t.Fatal(e)
	}
	works := make([]SignWork, 256)
	for i := range works {
		r, e := cache.Admit(sessions[i/8], request(domain.FindNodeRequestID(i), cache.Identity(), byte(i)), 0, responderTimeout, priorities)
		if e != nil {
			t.Fatalf("fill %d: %v", i, e)
		}
		works[i] = r.Work
	}
	// Reusing a challenge is a copy; use a distinct 32-byte value for entry 257.
	req := request(999, cache.Identity(), 0)
	req.Challenge[1] = 1
	if _, e := cache.Admit(sessions[32], req, 0, responderTimeout, priorities); e != nil {
		t.Fatal(e)
	}
	if got := cache.Snapshot(); got.Entries != 256 || got.Evicted != 1 || got.SessionReferences != 256 {
		t.Fatalf("bound %+v", got)
	}
	if _, e := cache.Complete(works[0], bodyFor(t, key), 0); e != nil {
		t.Fatalf("protected work evicted: %v", e)
	}
	if _, e := cache.Complete(works[8], bodyFor(t, key), 0); !errors.Is(e, ErrStaleWork) {
		t.Fatalf("oldest heaviest NEW group not evicted: %v", e)
	}
	if e := cache.Expire(cacheLifetime); e != nil {
		t.Fatal(e)
	}
	if got := cache.Snapshot(); got.Entries != 0 || got.SessionReferences != 0 {
		t.Fatalf("expired references leaked %+v", got)
	}
}
func TestPendingCapsExpiryCloseAndCancelReleaseExactlyOnce(t *testing.T) {
	sources := make([]Session, 33)
	targets := make([]Session, 33)
	for i := range sources {
		sources[i], _ = provenFixture(t, domain.ConnID(i+1), "192.0.0.0/16", 0)
		targets[i], _ = provenFixture(t, domain.ConnID(i+101), "198.51.0.0/16", 0)
	}
	table := NewPendingTable()
	var first Forward
	for i := range 32 {
		for j := range 16 {
			target := targets[(i+j)%32]
			f, e := table.Reserve(sources[i], target, request(domain.FindNodeRequestID(j), target.Identity(), byte(j)), 0)
			if e != nil {
				t.Fatalf("fill %d/%d: %v", i, j, e)
			}
			if i == 0 && j == 0 {
				first = f
			}
		}
	}
	if got := table.Snapshot(); got.Pending != 512 {
		t.Fatalf("pending bound %+v", got)
	}
	if _, e := table.Reserve(sources[32], targets[32], request(0, targets[32].Identity(), 0), 0); !errors.Is(e, ErrCapacity) {
		t.Fatalf("node cap %v", e)
	}
	if !table.Cancel(first) || table.Cancel(first) {
		t.Fatal("cancel released twice")
	}
	if _, e := table.Reserve(sources[32], targets[32], request(0, targets[32].Identity(), 0), 0); e != nil {
		t.Fatal(e)
	}
	table.CloseSession(sources[32])
	table.CloseSession(sources[32])
	if got := table.Snapshot(); got.Pending != 511 || got.Closed != 1 || got.Cancelled != 1 {
		t.Fatalf("close counters %+v", got)
	}
	if e := table.Expire(responderTimeout); e != nil {
		t.Fatal(e)
	}
	if got := table.Snapshot(); got.Pending != 0 || got.Expired != 511 {
		t.Fatalf("expiry counters %+v", got)
	}
	// The same pending caps are enforced individually, before total saturation.
	table = NewPendingTable()
	for i := range 16 {
		if _, e := table.Reserve(sources[0], targets[i], request(domain.FindNodeRequestID(i), targets[i].Identity(), 0), 0); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := table.Reserve(sources[0], targets[16], request(16, targets[16].Identity(), 0), 0); !errors.Is(e, ErrCapacity) {
		t.Fatalf("source cap %v", e)
	}
	table = NewPendingTable()
	for i := range 16 {
		if _, e := table.Reserve(sources[i], targets[0], request(0, targets[0].Identity(), 0), 0); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := table.Reserve(sources[16], targets[0], request(0, targets[0].Identity(), 0), 0); !errors.Is(e, ErrCapacity) {
		t.Fatalf("target cap %v", e)
	}
}
func TestDelayedReservationsCannotExceedActualBurst(t *testing.T) {
	calendar, _ := NewForwardCalendar(domain.PeerIdentity{9}, 0, 0)
	slots := make([]Reservation, 5)
	for i := range slots {
		var e error
		slots[i], e = calendar.Reserve(0, Instant(6*time.Second))
		if e != nil {
			t.Fatal(e)
		}
	}
	for i := range 4 {
		if e := calendar.Consume(slots[i], Instant(5*time.Second)); e != nil {
			t.Fatal(e)
		}
	}
	if e := calendar.Consume(slots[4], Instant(5*time.Second)); !errors.Is(e, ErrNotReady) {
		t.Fatalf("late jobs created a burst of five: %v", e)
	}
	if e := calendar.Consume(slots[4], Instant(6*time.Second)); e != nil {
		t.Fatal(e)
	}
}

func TestSigningCalendarSharesTargetBudgetAcrossResponders(t *testing.T) {
	sessions := make([]Session, 5)
	for i := range sessions {
		sessions[i], _ = provenFixture(t, domain.ConnID(i+1), "192.0.0.0/16", 0)
	}
	if _, e := NewSigningCalendar(SigningProfile{}, 0); e == nil {
		t.Fatal("unapproved implicit signing profile")
	}
	if _, e := NewSigningCalendar(SigningProfile{NodeSpacing: 249 * time.Millisecond, MaxReservations: 256}, 0); e == nil {
		t.Fatal("increased signing budget")
	}
	scheduler, e := NewSigningCalendar(SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 256}, 0)
	if e != nil {
		t.Fatal(e)
	}
	slots := make([]SigningReservation, 4)
	for i := range slots {
		slots[i], e = scheduler.Reserve(sessions[i], 0, Instant(750*time.Millisecond))
		if e != nil {
			t.Fatal(e)
		}
		if slots[i].At() != Instant(time.Duration(i)*250*time.Millisecond) {
			t.Fatal("global signer contention ignored")
		}
	}
	if _, e := scheduler.Reserve(sessions[4], 0, Instant(750*time.Millisecond)); !errors.Is(e, ErrBudget) {
		t.Fatalf("fifth R bypassed node budget: %v", e)
	}
	for i, slot := range slots {
		if e := scheduler.Consume(slot, Instant(time.Duration(i)*250*time.Millisecond)); e != nil {
			t.Fatal(e)
		}
	}
	// Same-session burst remains four even with an otherwise idle 4/s signer.
	scheduler, e = NewSigningCalendar(SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8}, 0)
	if e != nil {
		t.Fatal(e)
	}
	// A distinct genuine session is needed: one Session has one signing owner.
	source, _ := provenFixture(t, 99, "2001:db8::/32", 0)
	for i := range 6 {
		slot, e := scheduler.Reserve(source, 0, Instant(2*time.Second))
		if e != nil {
			t.Fatal(e)
		}
		want := Instant(time.Duration(i) * 250 * time.Millisecond)
		if i >= 4 {
			want = Instant(time.Duration(i-3) * time.Second)
		}
		if slot.At() != want {
			t.Fatalf("session slot %d got %v want %v", i, slot.At(), want)
		}
	}
}

func TestConcurrentChallengeHasOneWorkAndBoundedSnapshot(t *testing.T) {
	source, key := provenFixture(t, 1, "192.0.0.0/16", 0)
	cache, _ := NewTargetCache(bodyFor(t, key).Target)
	priorities := viewFor(t, cache.Identity(), 0, source)
	results := make(chan TargetResult, 32)
	errs := make(chan error, 32)
	for i := range 32 {
		go func() {
			r, e := cache.Admit(source, request(domain.FindNodeRequestID(i), cache.Identity(), 1), 0, responderTimeout, priorities)
			if e != nil {
				errs <- e
				return
			}
			results <- r
		}()
	}
	works := 0
	for range 32 {
		select {
		case r := <-results:
			if r.Kind != TargetNeedsSignature {
				t.Fatal("unexpected duplicate admission")
			}
			works++
		case e := <-errs:
			if !errors.Is(e, ErrDuplicateLink) {
				t.Fatal(e)
			}
		}
	}
	if works != 1 {
		t.Fatalf("signature work count %d", works)
	}
	if got := cache.Snapshot(); got.Entries != 1 || got.Reserved != 1 || got.Accepted != 1 || got.Refused != 31 {
		t.Fatalf("concurrent snapshot %+v", got)
	}
}
func TestProtectedSessionQuotaAndCacheLifetimeAreExact(t *testing.T) {
	source, key := provenFixture(t, 1, "192.0.0.0/16", 0)
	cache, _ := NewTargetCache(bodyFor(t, key).Target)
	classify := func(SessionInfo) (SessionClass, error) { return ClassContact, nil }
	priorities, e := NewPrioritySnapshot(cache.Identity(), []Session{source}, 0, classify)
	if e != nil {
		t.Fatal(e)
	}
	var first SignWork
	for i := range 8 {
		r, e := cache.Admit(source, request(domain.FindNodeRequestID(i), cache.Identity(), byte(i)), 0, responderTimeout, priorities)
		if e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			first = r.Work
		}
	}
	if _, e := cache.Admit(source, request(9, cache.Identity(), 9), 0, responderTimeout, priorities); !errors.Is(e, ErrCapacity) {
		t.Fatalf("protected per-session quota %v", e)
	}
	if _, e := cache.Complete(first, bodyFor(t, key), 0); e != nil {
		t.Fatal(e)
	}
	if e := cache.Expire(cacheLifetime - 1); e != nil {
		t.Fatal(e)
	}
	if got := cache.Snapshot(); got.Entries != 1 || got.Ready != 1 || got.SessionReferences != 1 {
		t.Fatalf("ready lifetime %+v", got)
	}
	if e := cache.Expire(cacheLifetime); e != nil {
		t.Fatal(e)
	}
	if got := cache.Snapshot(); got.Entries != 0 || got.SessionReferences != 0 {
		t.Fatalf("expiry boundary %+v", got)
	}
	if e := cache.Expire(cacheLifetime - 1); !errors.Is(e, ErrClock) {
		t.Fatalf("wall-clock rollback accepted %v", e)
	}
}

func TestExpiredCalendarAliasesReleasePhysicalPlaces(t *testing.T) {
	forward, _ := NewForwardCalendar(domain.PeerIdentity{9}, 0, 4)
	slot, e := forward.Reserve(0, 0)
	if e != nil {
		t.Fatal(e)
	}
	if e := forward.Expire(0); e != nil || forward.Pending() != 1 {
		t.Fatal("inclusive deadline was expired early")
	}
	if e := forward.Expire(1); e != nil || forward.Pending() != 0 {
		t.Fatal("expired forward alias remained")
	}
	if forward.Cancel(slot) {
		t.Fatal("expired alias released twice")
	}
	source, _ := provenFixture(t, 1, "192.0.0.0/16", 0)
	signer, _ := NewSigningCalendar(SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 1}, 0)
	old, e := signer.Reserve(source, 0, 0)
	if e != nil {
		t.Fatal(e)
	}
	if e := signer.Expire(1); e != nil || signer.Pending() != 0 {
		t.Fatal("expired signing alias remained")
	}
	if signer.Cancel(old) {
		t.Fatal("expired signing alias released twice")
	}
	next, e := signer.Reserve(source, 1, Instant(time.Second))
	if e != nil || next.At() != Instant(250*time.Millisecond) {
		t.Fatalf("expiry refunded the projected node slot: %v", e)
	}
}

func TestTargetIdentityPendingAndPairKeysAggregateTwoRealSessions(t *testing.T) {
	target, remote := provenFixture(t, 1, "192.0.0.0/16", 0)
	second, _ := provenFixture(t, 2, "192.0.0.0/16", 0, remote)
	a, _ := provenFixture(t, 3, "198.51.0.0/16", 0)
	b, _ := provenFixture(t, 4, "2001:db8::/32", 0)
	table := NewPendingTable()
	var first Forward
	for i := range 16 {
		f, e := table.Reserve(a, target, request(domain.FindNodeRequestID(i), target.Identity(), byte(i)), 0)
		if e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			first = f
		}
	}
	if _, e := table.Reserve(b, second, request(99, target.Identity(), 99), 0); !errors.Is(e, ErrCapacity) {
		t.Fatalf("17th pending via second TLS link: %v", e)
	}
	// Aggregate quota does not permit substituting another link in a response.
	if _, e := table.Answer(second, bodyFor(t, remote).Answer(first.Request.ID), 0); !errors.Is(e, ErrUnmatchedAnswer) {
		t.Fatalf("wrong actual target link: %v", e)
	}
	if !table.Cancel(first) {
		t.Fatal("cancel")
	}
	if _, e := table.Reserve(b, second, request(99, target.Identity(), 99), 0); e != nil {
		t.Fatal(e)
	}
	key1, e := NewPairKey(a, target.Identity())
	if e != nil {
		t.Fatal(e)
	}
	key2, e := NewPairKey(a, second.Identity())
	if e != nil || key1 != key2 {
		t.Fatal("pair key followed target link instead of identity")
	}
	window, _ := NewPairWindow(key1)
	registry := map[PairKey]*PairWindow{key1: window}
	for range 3 {
		if !registry[key2].Allow(0) {
			t.Fatal("legal pair")
		}
	}
	if registry[key1].Allow(0) {
		t.Fatal("target link change reset pair window")
	}
	calendar, _ := NewForwardCalendar(target.Identity(), 0, 4)
	if calendar.Target() != second.Identity() {
		t.Fatal("calendar is not bound to exact identity")
	}
	calendars := map[domain.PeerIdentity]*ForwardCalendar{calendar.Target(): calendar}
	for range 4 {
		if _, e := calendars[second.Identity()].Reserve(0, 0); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := calendars[target.Identity()].Reserve(0, 0); !errors.Is(e, ErrBudget) {
		t.Fatalf("target link change reset forwarding tokens: %v", e)
	}
}
func TestReadyHistorySurvivesEveryRecipientCloseUntilTTL(t *testing.T) {
	sessions := make([]Session, 4)
	var key *identity.Identity
	for i := range sessions {
		sessions[i], key = provenFixture(t, domain.ConnID(i+1), "192.0.0.0/16", 0)
	}
	cache, _ := NewTargetCache(bodyFor(t, key).Target)
	priorities := viewFor(t, cache.Identity(), 0, sessions...)
	first, e := cache.Admit(sessions[0], request(11, cache.Identity(), 7), 0, 1, priorities)
	if e != nil {
		t.Fatal(e)
	}
	for i := 1; i < 3; i++ {
		if _, e := cache.Admit(sessions[i], request(domain.FindNodeRequestID(i+11), cache.Identity(), 7), 0, 1, priorities); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := cache.Complete(first.Work, bodyFor(t, key), 0); e != nil {
		t.Fatal(e)
	}
	for i := range 3 {
		cache.CloseSession(sessions[i])
		cache.CloseSession(sessions[i])
	}
	got := cache.Snapshot()
	if got.Entries != 1 || got.Ready != 1 || got.SessionReferences != 0 || got.HistoryReferences != 3 || got.Closed != 3 {
		t.Fatalf("close discarded dedup state: %+v", got)
	}
	if len(cache.Sessions()) != 0 {
		t.Fatal("closed TLS/session object retained as active owner")
	}
	priorities = viewFor(t, cache.Identity(), 0, sessions[3])
	if _, e := cache.Admit(sessions[3], request(44, cache.Identity(), 7), 0, 1, priorities); !errors.Is(e, ErrDuplicateExcess) {
		t.Fatalf("fourth historical recipient accepted: %v", e)
	}
	if e := cache.Expire(cacheLifetime); e != nil {
		t.Fatal(e)
	}
	priorities = viewFor(t, cache.Identity(), cacheLifetime, sessions[3])
	if r, e := cache.Admit(sessions[3], request(55, cache.Identity(), 7), cacheLifetime, cacheLifetime+1, priorities); e != nil || r.Kind != TargetNeedsSignature {
		t.Fatalf("TTL did not release history: %v", e)
	}
}
func TestLastClosePreservesReadyBodyAndInflightGeneration(t *testing.T) {
	a, key := provenFixture(t, 1, "192.0.0.0/16", 0)
	b, _ := provenFixture(t, 2, "198.51.0.0/16", 0)
	cache, _ := NewTargetCache(bodyFor(t, key).Target)
	first, e := cache.Admit(a, request(11, cache.Identity(), 7), 0, 1, viewFor(t, cache.Identity(), 0, a, b))
	if e != nil {
		t.Fatal(e)
	}
	cache.CloseSession(a)
	copied, e := cache.Admit(b, request(22, cache.Identity(), 7), 0, 1, viewFor(t, cache.Identity(), 0, b))
	if e != nil || copied.Kind != TargetWaiting {
		t.Fatalf("last close allowed second signing work: %v", e)
	}
	deliveries, e := cache.Complete(first.Work, bodyFor(t, key), 0)
	if e != nil || len(deliveries) != 1 || deliveries[0].Answer.ID != 22 {
		t.Fatalf("original inflight work lost: %v", e)
	}
	cache.CloseSession(b)
	c, _ := provenFixture(t, 3, "2001:db8::/32", 0)
	ready, e := cache.Admit(c, request(33, cache.Identity(), 7), 0, 1, viewFor(t, cache.Identity(), 0, c))
	if e != nil || ready.Kind != TargetAnswerReady || ready.Answer.ID != 33 {
		t.Fatalf("last close forced re-signing: %v", e)
	}
	if _, e := cache.Complete(first.Work, bodyFor(t, key), 0); !errors.Is(e, ErrStaleWork) {
		t.Fatalf("signing work completed twice: %v", e)
	}
}
func TestCancelledSigningGenerationRetainsTombstoneUntilDeadline(t *testing.T) {
	a, key := provenFixture(t, 1, "192.0.0.0/16", 0)
	b, _ := provenFixture(t, 2, "198.51.0.0/16", 0)
	cache, _ := NewTargetCache(bodyFor(t, key).Target)
	old, e := cache.Admit(a, request(1, cache.Identity(), 8), 0, 1, viewFor(t, cache.Identity(), 0, a, b))
	if e != nil {
		t.Fatal(e)
	}
	if !cache.Cancel(old.Work) || cache.Cancel(old.Work) {
		t.Fatal("cancel generation accounting")
	}
	if _, e := cache.Complete(old.Work, bodyFor(t, key), 0); !errors.Is(e, ErrStaleWork) {
		t.Fatal("cancelled work published answer")
	}
	if _, e := cache.Admit(b, request(2, cache.Identity(), 8), 0, 1, viewFor(t, cache.Identity(), 0, b)); !errors.Is(e, ErrStaleWork) {
		t.Fatal("cancel immediately created a second signing generation")
	}
	if got := cache.Snapshot(); got.Entries != 1 || got.CancelledWork != 1 || got.SessionReferences != 0 || got.HistoryReferences != 1 {
		t.Fatalf("cancel tombstone %+v", got)
	}
	fresh, e := cache.Admit(b, request(3, cache.Identity(), 8), 1, 2, viewFor(t, cache.Identity(), 1, b))
	if e != nil || fresh.Kind != TargetNeedsSignature {
		t.Fatal(e)
	}
	if _, e := cache.Complete(old.Work, bodyFor(t, key), 1); !errors.Is(e, ErrStaleWork) {
		t.Fatal("old generation resurrected after deadline")
	}
	if _, e := cache.Complete(fresh.Work, bodyFor(t, key), 1); e != nil {
		t.Fatal(e)
	}
}

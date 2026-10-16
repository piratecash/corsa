package node

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

const day = 24 * time.Hour

// clockJumpFixture is a store with one binding of each kind and a pin, on a
// test device's clocks, with its journal.
type clockJumpFixture struct {
	clock     *storeClock
	path      string
	store     *secureSessionStore
	journal   *storeJournal
	ordinary  domain.PeerAddress
	protected domain.PeerAddress
	pinned    domain.PeerIdentity
}

func newClockJumpFixture(t *testing.T) clockJumpFixture {
	t.Helper()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	fixture := clockJumpFixture{
		clock:     clock,
		path:      path,
		store:     store,
		journal:   journalledStore(store),
		ordinary:  "192.0.2.1:64646",
		protected: "192.0.2.2:64646",
		pinned:    domain.PeerIdentity{0xa3},
	}
	if _, err := store.noteProvenOutbound(dialledAt(fixture.ordinary), domain.PeerIdentity{0xa1}, endpointProtectionNone()); err != nil {
		t.Fatalf("bind ordinary: %v", err)
	}
	if _, err := store.noteProvenOutbound(dialledAt(fixture.protected), domain.PeerIdentity{0xa2}, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); err != nil {
		t.Fatalf("bind protected: %v", err)
	}
	if err := store.noteProvenInbound(fixture.pinned); err != nil {
		t.Fatalf("pin: %v", err)
	}
	return fixture
}

func (f clockJumpFixture) flush(t *testing.T) {
	t.Helper()
	if err := f.store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// assertProtectedAndPinStand checks what no clock may end: a protected
// binding and an identity pin.
func assertProtectedAndPinStand(t *testing.T, store *secureSessionStore, f clockJumpFixture) {
	t.Helper()
	if !store.endpointRequiresV2(f.protected) {
		t.Fatal("a protected binding stopped requiring v2 after a clock change")
	}
	if !store.identityPinned(f.pinned) {
		t.Fatal("an identity pin was lost after a clock change")
	}
}

// A wall clock set 60 days ahead in a running process — a forged NTP answer
// is enough — must not end every ordinary binding at once: the endpoint
// would take v1 again from an identity nobody pinned. The jump is journalled
// once as clock_jump_detected and counted.
func TestAWallClockJumpDoesNotEndOrdinaryBindingsInARunningProcess(t *testing.T) {
	t.Parallel()
	f := newClockJumpFixture(t)

	f.clock.stepWall(60 * day)
	f.flush(t)

	if !f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("a wall clock set 60 days ahead ended an ordinary binding in a running process")
	}
	if _, bound := bindingOf(f.store, f.ordinary); !bound {
		t.Fatal("the flush swept an ordinary binding as expired after a wall clock jump")
	}
	assertProtectedAndPinStand(t, f.store, f)
	jumps := f.journal.entries(t, "clock_jump_detected")
	if len(jumps) != 1 || jumps[0]["level"] != "warn" || jumps[0]["store"] != "secure-sessions-64646.json" || jumps[0]["ahead"] == nil {
		t.Fatalf("clock_jump_detected = %v; want one warning naming the store and how far ahead", jumps)
	}
	stats := f.store.stats()
	if !stats.ClockJumpGuarded || stats.ClockJumpsDetected != 1 || !stats.ClockJumpActive {
		t.Fatalf("stats = guarded %v, jumps %d, active %v; want guarded, 1, active", stats.ClockJumpGuarded, stats.ClockJumpsDetected, stats.ClockJumpActive)
	}

	f.clock.now = f.clock.now.Add(2 * renewalFlushEvery)
	f.flush(t)
	if got := len(f.journal.entries(t, "clock_jump_detected")); got != 1 {
		t.Fatalf("a jump still in force was journalled %d times, want once", got)
	}
}

// The excess of a jump is not counted, but the time that really goes by is:
// 30 days after the last v2 on the trusted time the binding ends as it would
// have without the jump.
func TestAfterAWallClockJumpTheTermRunsOnTheTimeThatWentBy(t *testing.T) {
	t.Parallel()
	f := newClockJumpFixture(t)

	f.clock.stepWall(60 * day)
	f.clock.now = f.clock.now.Add(endpointBindingTTL - time.Second)
	if !f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("the term ended before 30 days went by")
	}
	f.clock.now = f.clock.now.Add(2 * time.Second)
	if f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("the term outlived 30 days of time that went by")
	}
	f.flush(t)
	if _, bound := bindingOf(f.store, f.ordinary); bound {
		t.Fatal("the flush kept a binding past its term")
	}
	assertProtectedAndPinStand(t, f.store, f)
}

// At a restart there is nothing to hold the wall clock against: a jump made
// before it is taken as it is and ends ordinary bindings — the named residue.
// Protected bindings and pins are not touched.
func TestAWallClockJumpBeforeARestartEndsOrdinaryBindingsOnly(t *testing.T) {
	t.Parallel()
	f := newClockJumpFixture(t)

	f.clock.stepWall(60 * day)
	restarted := loadSecureSessionStore(f.path, f.clock.trusted())

	if restarted.endpointRequiresV2(f.ordinary) {
		t.Fatal("a restart after a 60-day jump kept an ordinary binding; the trusted time has no anchor at the start")
	}
	assertProtectedAndPinStand(t, restarted, f)
	if stats := restarted.stats(); stats.ClockJumpsDetected != 0 || stats.ClockJumpActive {
		t.Fatalf("a restart counted the jump made before it: jumps %d, active %v", stats.ClockJumpsDetected, stats.ClockJumpActive)
	}
}

// Ten days asleep move both clocks: no jump, and the sleep counts toward the
// term, which ends 30 days after the last v2 — not later.
func TestTenDaysOfSleepCountTowardTheTermAndAreNoJump(t *testing.T) {
	t.Parallel()
	f := newClockJumpFixture(t)

	f.clock.now = f.clock.now.Add(10 * day)
	f.flush(t)
	if !f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("ten days of sleep ended a 30-day term")
	}
	if got := len(f.journal.entries(t, "clock_jump_detected")); got != 0 {
		t.Fatalf("sleep was journalled as a clock jump %d times", got)
	}

	f.clock.now = f.clock.now.Add(20*day - time.Second)
	if !f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("the term ended before 30 days from the last v2")
	}
	f.clock.now = f.clock.now.Add(2 * time.Second)
	if f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("the term outlived 30 days from the last v2: the sleep was not counted")
	}
	if stats := f.store.stats(); stats.ClockJumpsDetected != 0 {
		t.Fatalf("sleep counted as %d clock jumps", stats.ClockJumpsDetected)
	}
}

// A wall clock set back only makes a term end later, and is no jump.
func TestAWallClockSetBackEndsNoTermEarly(t *testing.T) {
	t.Parallel()
	f := newClockJumpFixture(t)

	f.clock.stepWall(-10 * day)
	f.flush(t)
	if !f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("a wall clock set back ended an ordinary binding")
	}
	assertProtectedAndPinStand(t, f.store, f)
	if got := len(f.journal.entries(t, "clock_jump_detected")); got != 0 {
		t.Fatalf("a wall clock set back was journalled as a jump %d times", got)
	}
}

// On a platform without a clock since boot the guard is off, and says so at
// load and in the stats: a wall clock step is then taken as it is — the named
// residue — and protected bindings and pins still stand.
func TestWithoutAClockSinceBootTheStoreSaysTheGuardIsOff(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	journal := &storeJournal{}
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStoreWith(path, trustedclock.New(trustedclock.Sources{Wall: clock.read}), zerolog.New(journal))
	if lines := journal.entries(t, "clock_jump_guard_unavailable_wall_clock_steps_end_ordinary_bindings_early"); len(lines) != 1 || lines[0]["store"] != "secure-sessions-64646.json" {
		t.Fatalf("guard warning = %v; want one line naming the store", lines)
	}
	if store.stats().ClockJumpGuarded {
		t.Fatal("the stats report a guard the platform cannot give")
	}
	ordinary := domain.PeerAddress("192.0.2.1:64646")
	if _, err := store.noteProvenOutbound(dialledAt(ordinary), domain.PeerIdentity{0xa1}, endpointProtectionNone()); err != nil {
		t.Fatalf("bind: %v", err)
	}
	clock.stepWall(60 * day)
	if store.endpointRequiresV2(ordinary) {
		t.Fatal("an unguarded store did not take the wall clock as it is")
	}
}

// A wall clock set back and then returned must not be a way round the guard:
// renewals made while it is back would put last_v2 in the past, and the
// return — no jump, since the wall clock only comes back to the trusted time
// — would end every binding renewed meanwhile at once. A renewal never moves
// last_v2 back.
func TestARenewalWhileTheWallClockIsSetBackDoesNotShortenTheTerm(t *testing.T) {
	t.Parallel()
	f := newClockJumpFixture(t)
	owner := domain.PeerIdentity{0xa1}
	before, _ := bindingOf(f.store, f.ordinary)

	f.clock.stepWall(-endpointBindingTTL)
	if _, err := f.store.noteProvenOutbound(dialledAt(f.ordinary), owner, endpointProtectionNone()); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if renewed, _ := bindingOf(f.store, f.ordinary); renewed.lastV2.Before(before.lastV2) {
		t.Fatalf("a renewal moved last_v2 back from %v to %v", before.lastV2, renewed.lastV2)
	}

	f.clock.stepWall(endpointBindingTTL)
	f.flush(t)
	if !f.store.endpointRequiresV2(f.ordinary) {
		t.Fatal("a wall clock set back, a renewal and the clock's return ended the binding at once")
	}
	restarted := loadSecureSessionStore(f.path, f.clock.trusted())
	if !restarted.endpointRequiresV2(f.ordinary) {
		t.Fatal("after a restart the binding renewed while the clock was back ended at once")
	}
}

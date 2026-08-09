package node

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// contact_verify_budget_test.go pins the SUSTAINED half of the contact
// verification budget: the half a per-connection counter cannot express.
//
// The finding it locks: every fresh recovery dial used to build its own
// singleReplyContactBudget worth maxContactsPerResponse verifications, so the
// "budget" was a property of a TCP connection rather than of a neighbour. The
// dial is triggered by DM frames whose `sender` field the attacker writes, so
// the neighbour chose how often a brand-new budget was handed out — and the
// per-hop slot and the global pass cap bound CONCURRENCY only, never the total.

// ---------------------------------------------------------------------------
// The sustained budget across fresh dials
// ---------------------------------------------------------------------------

// serveContactsPerConnection answers every connection the listener accepts with
// the minimal syncPeer handshake (welcome without a challenge) and replies to
// each fetch_contacts with the same contact array, until the listener closes.
//
// One connection per dial is the point: this is the attacker's shape, where the
// connection is the thing that is cheap to replace.
func serveContactsPerConnection(t *testing.T, ln net.Listener, contacts []protocol.ContactFrame) {
	t.Helper()

	welcome, err := protocol.MarshalFrameLine(protocol.Frame{Type: "welcome"})
	if err != nil {
		t.Errorf("marshal welcome: %v", err)
		return
	}
	reply, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:     "contacts",
		Count:    len(contacts),
		Contacts: contacts,
	})
	if err != nil {
		t.Errorf("marshal contacts: %v", err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		serveOneContactsDial(conn, welcome, reply)
	}
}

func serveOneContactsDial(conn net.Conn, welcome, reply string) {
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}
	if _, err := conn.Write([]byte(welcome)); err != nil {
		return
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
		if err != nil {
			continue
		}
		if frame.Type == "fetch_contacts" {
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		}
	}
}

// TestSequentialFreshDialsShareOneContactVerifyBudget is the finding.
//
// The same remote endpoint answers a series of fresh recovery dials, each with
// a legitimately-sized contacts reply. Together they offer far more entries
// than the sustained budget covers, so the total number of signature checks
// this node performs must be bounded by the bucket — burst plus whatever the
// documented rate refilled while the test ran — and NOT by "one full budget per
// connection".
//
// The wall clock appears only as an UPPER bound on the refill allowance, never
// as a wait: a slower machine only makes the allowance more generous, so the
// assertion cannot flake, it can only become less strict.
func TestSequentialFreshDialsShareOneContactVerifyBudget(t *testing.T) {
	t.Parallel()

	const (
		dials           = 8
		contactsPerDial = 1024
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	contacts := validContactFrames(t, contactsPerDial)
	go serveContactsPerConnection(t, ln, contacts)

	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	started := time.Now()
	verified := 0
	firstDial := 0
	for dial := 0; dial < dials; dial++ {
		imported := svc.syncPeer(ctx, peerAddr, false)
		if dial == 0 {
			firstDial = imported
		}
		verified += imported
	}
	elapsed := time.Since(started)

	// The honest case first: a peer this node has not synced with recently is
	// met by a full bucket, so its whole reply is verified. A budget that
	// bounded the flood by refusing everybody would pass the assertion below
	// and be useless.
	if firstDial != contactsPerDial {
		t.Fatalf("the first dial imported %d of the %d contacts offered: an honest peer's first sync is being cut by the budget",
			firstDial, contactsPerDial)
	}

	offered := dials * contactsPerDial
	allowance := contactVerifyBurst + int(elapsed.Seconds()*contactVerifiesPerSecond)
	if verified > allowance {
		t.Fatalf("%d fresh dials from one endpoint bought %d signature verifications of the %d offered; the sustained budget allows %d (burst %d + %.1fs of refill at %d/s): the budget is per connection, and the neighbour chooses how many connections there are",
			dials, verified, offered, allowance, contactVerifyBurst, elapsed.Seconds(), contactVerifiesPerSecond)
	}
}

// ---------------------------------------------------------------------------
// The registry itself
// ---------------------------------------------------------------------------

// TestContactVerifyBudgetRefillsOverTime pins the other half: the budget is a
// rate limit, not a quota. An honest peer that legitimately needed a full sync
// must get its allowance back by waiting — no reconnect, no operator — and a
// reconnect must NOT be what returns it.
func TestContactVerifyBudgetRefillsOverTime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	key := contactVerifyKeyFromEndpoint("198.51.100.7:9000", "peer.example:9000")
	budget := svc.contactVerifyBudgetFor(key)

	for i := 0; i < contactVerifyBurst; i++ {
		if !budget.ChargeContactVerify() {
			t.Fatalf("the first sync was refused verification %d of the burst the bucket is sized for", i)
		}
	}
	if budget.ChargeContactVerify() {
		t.Fatal("the bucket is unbounded: the burst bought nothing")
	}

	now = now.Add(time.Second)
	granted := 0
	for budget.ChargeContactVerify() {
		granted++
		if granted > contactVerifyBurst {
			t.Fatal("one second refilled the whole burst")
		}
	}
	if granted != contactVerifiesPerSecond {
		t.Fatalf("one second refilled %d verifications, want the documented %d", granted, contactVerifiesPerSecond)
	}

	// A fresh handle for the same endpoint is the same bucket: a reconnect is
	// exactly that, and it must not be a reset.
	if svc.contactVerifyBudgetFor(key).ChargeContactVerify() {
		t.Fatal("a second handle for the same endpoint charged again: the budget still lives on the connection")
	}
}

// TestContactVerifyBudgetsAreIndependentPerRemote pins the separation: one
// neighbour spending its allowance must never refuse another's contact sync.
func TestContactVerifyBudgetsAreIndependentPerRemote(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	spent := svc.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint("198.51.100.7:9000", ""))
	for i := 0; i < contactVerifyBurst; i++ {
		if !spent.ChargeContactVerify() {
			t.Fatalf("the first endpoint was refused verification %d of its own burst", i)
		}
	}

	other := svc.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint("203.0.113.9:9000", ""))
	for i := 0; i < contactVerifyBurst; i++ {
		if !other.ChargeContactVerify() {
			t.Fatalf("a second endpoint was refused verification %d: the budgets are shared, so one neighbour starves every other", i)
		}
	}
}

// TestContactVerifyBudgetEvictionCannotResetADebt pins the memory bound and the
// trap inside it: the registry is capped, so entries are dropped — and dropping
// a throttled one would hand back exactly the reset that keying on the endpoint
// took away. Overflowing the map from cheap fresh endpoints must leave the
// drained bucket drained.
func TestContactVerifyBudgetEvictionCannotResetADebt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	attacker := contactVerifyKeyFromEndpoint("198.51.100.7:9000", "")
	drained := svc.contactVerifyBudgetFor(attacker)
	for i := 0; i < contactVerifyBurst; i++ {
		if !drained.ChargeContactVerify() {
			t.Fatalf("refused verification %d of the burst", i)
		}
	}

	// Each of these is a distinct endpoint that spends one token, which is the
	// cheapest way for an attacker to force the map to overflow.
	for i := 0; i < 4*maxTrackedContactVerifyRemotes; i++ {
		filler := contactVerifyKeyFromEndpoint("", domain.PeerAddress(contactTestAddress(i)))
		svc.contactVerifyBudgetFor(filler).ChargeContactVerify()
	}

	if tracked := svc.contactVerifyBudgets.trackedRemotes(); tracked > maxTrackedContactVerifyRemotes {
		t.Fatalf("the registry tracks %d remotes, cap is %d: the map is a memory sink of its own",
			tracked, maxTrackedContactVerifyRemotes)
	}
	if svc.contactVerifyBudgetFor(attacker).ChargeContactVerify() {
		t.Fatal("the drained bucket came back full after the registry overflowed: forcing an eviction is a budget reset, which is the bypass this budget exists to close")
	}
}

// TestFreshDialAndSessionShareOneRemoteBudget pins that the two import paths
// cannot be alternated for two budgets. They are one neighbour on one wire; a
// peer that spends its allowance answering a session sync must not find a full
// one waiting on the recovery dial.
func TestFreshDialAndSessionShareOneRemoteBudget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	session := &peerSession{address: domain.PeerAddress("198.51.100.7:9000")}
	viaSession := svc.contactVerifyBudgetFor(sessionContactVerifyKey(session))
	for i := 0; i < contactVerifyBurst; i++ {
		if !viaSession.ChargeContactVerify() {
			t.Fatalf("the session sync was refused verification %d of the burst", i)
		}
	}

	viaDial := svc.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint("198.51.100.7:9000", "198.51.100.7:9000"))
	if viaDial.ChargeContactVerify() {
		t.Fatal("the fresh dial to the endpoint that just spent its session budget got a full one: the two import paths hold separate budgets, so a neighbour alternates between them")
	}
}

// TestOverlayPeersDoNotShareTheProxyBudget pins the one case where the socket
// is the WRONG attribution.
//
// A .onion / .b32.i2p peer is reached through the local SOCKS proxy
// (dialPeer), so every overlay peer this node talks to shares one transport
// endpoint. Keying those on the socket would collapse the whole overlay into a
// single bucket — one hostile onion service could then starve every honest one,
// and the honest ones would starve each other. The overlay name is used
// instead, which is also the stronger attribution: a v3 onion name IS the
// service's public key.
func TestOverlayPeersDoNotShareTheProxyBudget(t *testing.T) {
	t.Parallel()

	const proxy = "127.0.0.1:9050"
	first := domain.PeerAddress(strings.Repeat("a", 56) + ".onion:9000")
	second := domain.PeerAddress(strings.Repeat("b", 56) + ".onion:9000")

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	spent := svc.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint(proxy, first))
	for i := 0; i < contactVerifyBurst; i++ {
		if !spent.ChargeContactVerify() {
			t.Fatalf("the first overlay peer was refused verification %d of its own burst", i)
		}
	}

	other := svc.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint(proxy, second))
	if !other.ChargeContactVerify() {
		t.Fatal("a second overlay peer was refused on the first one's spending: the budget is keyed on the SOCKS proxy socket, so the whole overlay shares one bucket")
	}

	// The same overlay peer over a new circuit — a different proxy connection —
	// is still the same neighbour and still the same bucket.
	if svc.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint("127.0.0.1:9150", first)).ChargeContactVerify() {
		t.Fatal("the first overlay peer got a fresh budget through a different proxy socket: the key follows the circuit instead of the peer")
	}
}

// ---------------------------------------------------------------------------
// A FULL registry
// ---------------------------------------------------------------------------

// drainRemote spends a remote's whole bucket and returns how many tokens it got.
func drainRemote(budget contactVerificationBudget) int {
	spent := 0
	for budget.ChargeContactVerify() {
		spent++
		if spent > contactVerifyBurst {
			return spent
		}
	}
	return spent
}

// saturateRegistryWithDebtors fills the registry to its cap with distinct
// endpoints that have each spent their WHOLE burst, and returns their keys.
//
// "All of them in debt" is the state the eviction policy has to survive: as long
// as one bucket has refilled, dropping THAT one forgives nothing and the cap is
// honoured for free. The interesting question is what happens when no such
// bucket exists.
func saturateRegistryWithDebtors(t *testing.T, svc *Service) []contactVerifyKey {
	t.Helper()

	keys := make([]contactVerifyKey, 0, maxTrackedContactVerifyRemotes)
	for i := 0; i < maxTrackedContactVerifyRemotes; i++ {
		key := contactVerifyKeyFromEndpoint(fmt.Sprintf("10.%d.%d.1:9000", i/256, i%256), "")
		if spent := drainRemote(svc.contactVerifyBudgetFor(key)); spent != contactVerifyBurst {
			t.Fatalf("endpoint %d spent %d tokens, want its whole burst %d", i, spent, contactVerifyBurst)
		}
		keys = append(keys, key)
	}
	return keys
}

// TestSaturatedRegistryGivesTheTailOneBudgetNotOnePerEndpoint is the finding.
//
// With every tracked bucket in debt, the registry has nothing it can drop for
// free. Dropping one anyway and creating a full bucket for the newcomer turns
// the memory bound into a budget reset: cycling through more endpoints than the
// registry can hold buys a fresh burst per endpoint. One /64 is 2^64 endpoints,
// so "more endpoints than the registry can hold" is not a constraint at all for
// an IPv6 prefix owner.
//
// The rule this pins: while the registry is saturated, everything that does not
// fit shares ONE budget — the tail of the world costs what a single neighbour
// costs, not one neighbour each.
func TestSaturatedRegistryGivesTheTailOneBudgetNotOnePerEndpoint(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	saturateRegistryWithDebtors(t, svc)

	const newcomers = 8
	granted := 0
	firstNewcomer := 0
	for i := 0; i < newcomers; i++ {
		spent := drainRemote(svc.contactVerifyBudgetFor(
			contactVerifyKeyFromEndpoint(fmt.Sprintf("203.0.113.%d:9000", i), "")))
		if i == 0 {
			firstNewcomer = spent
		}
		granted += spent
	}

	if granted > contactVerifyBurst {
		t.Fatalf("%d endpoints arriving at a saturated registry bought %d verifications between them, want at most one budget's %d: eviction hands the debt back, so cycling endpoints is a reset",
			newcomers, granted, contactVerifyBurst)
	}
	if firstNewcomer == 0 {
		t.Fatal("the first endpoint to arrive at a saturated registry got nothing: a full registry must degrade honest peers, not lock them out")
	}
	if tracked := svc.contactVerifyBudgets.trackedRemotes(); tracked > maxTrackedContactVerifyRemotes {
		t.Fatalf("the registry tracks %d remotes, cap is %d", tracked, maxTrackedContactVerifyRemotes)
	}
}

// TestDrainedRemoteStaysDrainedThroughASaturatedRegistry is the round-8 property
// re-pinned for the all-drained case: a remote that spent its budget must find
// it still spent after the registry has been churned as hard as an attacker can
// churn it.
func TestDrainedRemoteStaysDrainedThroughASaturatedRegistry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	attacker := contactVerifyKeyFromEndpoint("198.51.100.7:9000", "")
	if spent := drainRemote(svc.contactVerifyBudgetFor(attacker)); spent != contactVerifyBurst {
		t.Fatalf("the attacker spent %d tokens, want its whole burst %d", spent, contactVerifyBurst)
	}

	saturateRegistryWithDebtors(t, svc)
	for i := 0; i < 4*maxTrackedContactVerifyRemotes; i++ {
		svc.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint(
			fmt.Sprintf("172.16.%d.%d:9000", i/256%256, i%256), "")).ChargeContactVerify()
	}

	if svc.contactVerifyBudgetFor(attacker).ChargeContactVerify() {
		t.Fatal("the drained remote charged again after the registry was churned past its cap: forcing eviction is a budget reset")
	}
}

// TestIPv6EndpointsShareTheAssignmentPrefixBudget pins the granularity.
//
// A single IPv6 address is not a cost: the usual customer assignment is a /64,
// so an attacker with one allocation mints 2^64 endpoints for free, and a
// per-address budget is one budget per free endpoint. The bucket is therefore
// keyed on the /64, which makes minting an IPv6 endpoint cost the same as
// minting an IPv4 one — a new allocation.
func TestIPv6EndpointsShareTheAssignmentPrefixBudget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })

	first := contactVerifyKeyFromEndpoint("[2001:db8:1:2::1]:9000", "")
	sibling := contactVerifyKeyFromEndpoint("[2001:db8:1:2:aaaa:bbbb:cccc:dddd]:9000", "")
	otherPrefix := contactVerifyKeyFromEndpoint("[2001:db8:1:3::1]:9000", "")

	if spent := drainRemote(svc.contactVerifyBudgetFor(first)); spent != contactVerifyBurst {
		t.Fatalf("the first IPv6 endpoint spent %d tokens, want %d", spent, contactVerifyBurst)
	}
	if svc.contactVerifyBudgetFor(sibling).ChargeContactVerify() {
		t.Fatal("a second address in the same /64 got its own budget: an IPv6 prefix owner mints endpoints for free, so per-address is one budget per free endpoint")
	}
	if !svc.contactVerifyBudgetFor(otherPrefix).ChargeContactVerify() {
		t.Fatal("a different /64 was refused on the first prefix's spending: the aggregation is wider than an assignment and punishes unrelated peers")
	}
}

// TestContactVerifyRegistryNeverCallsOutUnderItsLock pins the mutex contract of
// CLAUDE.md on the one callback this registry owns.
//
// The clock is injected, so it is a CALLBACK: invoking it while `mu` is held
// makes the lock accidentally re-entrant for anything the clock touches, and it
// is the same rule that keeps the log sink — a far slower call — off this path.
// A clock that reads the registry back must therefore complete, not deadlock.
func TestContactVerifyRegistryNeverCallsOutUnderItsLock(t *testing.T) {
	t.Parallel()

	registry := &contactVerifyRegistry{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	registry.setClock(func() time.Time {
		registry.trackedRemotes()
		return now
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		registry.charge(contactVerifyKeyFromEndpoint("198.51.100.7:9000", ""))
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("charging the budget deadlocked against a clock that reads the registry: the injected clock is called while mu is held, and so is everything else on that path")
	}
}

// TestSaturationWarnLeavesTheLockBeforeItIsLogged pins where the log line is
// emitted, which is the second half of the mutex contract.
//
// `charge` decides UNDER mu what would have to be said and hands the facts back;
// the caller logs them after the unlock. A `log.Warn()` inside the locked
// section would put a log sink — the slowest thing on this path — between every
// remote on the node and its contact verification.
func TestSaturationWarnLeavesTheLockBeforeItIsLogged(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newTestService(t, config.NodeTypeFull)
	svc.contactVerifyBudgets.setClock(func() time.Time { return now })
	saturateRegistryWithDebtors(t, svc)

	newcomer := contactVerifyKeyFromEndpoint("203.0.113.1:9000", "")
	granted, warn := svc.contactVerifyBudgets.charge(newcomer)
	if !granted {
		t.Fatal("the first tail charge was refused: a saturated registry must degrade a newcomer, not lock it out")
	}
	if warn == nil {
		t.Fatal("a charge that landed in the tail bucket reported nothing to log: saturation is invisible to the operator")
	}
	if warn.key != newcomer || warn.charges != 1 {
		t.Fatalf("warn = %+v, want the newcomer's key and one charge", warn)
	}

	// Every further charge in the same interval is counted, not logged.
	for i := 0; i < 5; i++ {
		if _, repeat := svc.contactVerifyBudgets.charge(newcomer); repeat != nil {
			t.Fatalf("tail charge %d asked to be logged inside the same interval: the warn fires per signature check", i)
		}
	}

	// Charges made while the line is suppressed are not lost — the next line
	// speaks for all of them.
	registry := &contactVerifyRegistry{}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if first := registry.noteTailChargeLocked(newcomer, true, now); first == nil || first.charges != 1 {
		t.Fatalf("first = %+v, want the opening line", first)
	}
	for i := 0; i < 4; i++ {
		if suppressed := registry.noteTailChargeLocked(newcomer, true, now); suppressed != nil {
			t.Fatalf("charge %d inside the interval asked to be logged", i)
		}
	}
	next := registry.noteTailChargeLocked(newcomer, false, now.Add(contactVerifySaturationWarnInterval))
	if next == nil {
		t.Fatal("nothing was logged after the interval elapsed: sustained saturation goes silent")
	}
	if next.charges != 5 {
		t.Fatalf("the line after the interval speaks for %d charges, want the 5 it covers", next.charges)
	}
	if next.granted {
		t.Fatal("the warn reports the wrong verdict for the charge that carried it")
	}
}

// TestContactVerifyRegistryDoesNoIOUnderItsLock is the sibling sweep the mutex
// rule asks for: it is not enough that today's log line moved out, no call under
// mu may be a log sink, a callback or anything else that can block.
//
// It is asserted structurally because the property is about WHERE a call sits,
// which behaviour cannot show without a deliberately wedged sink. The clock —
// the one callback this type owns — is covered behaviourally by
// TestContactVerifyRegistryNeverCallsOutUnderItsLock.
func TestContactVerifyRegistryDoesNoIOUnderItsLock(t *testing.T) {
	t.Parallel()

	forbidden := map[string]struct{}{
		"Warn": {}, "Info": {}, "Error": {}, "Debug": {}, "Trace": {}, "Msg": {}, "Msgf": {},
	}
	locked := []string{
		"charge",
		"bucketLocked",
		"releaseRefilledLocked",
		"tailBucketLocked",
		"noteTailChargeLocked",
		"refillLocked",
		"trackedRemotes",
	}
	for _, function := range locked {
		for _, called := range callsInsideFunctionBody(t, "contact_verify_budget.go", function) {
			if _, banned := forbidden[called]; banned {
				t.Fatalf("%s calls %s while the registry mutex is held: a log sink under this lock stalls contact verification for every remote on the node",
					function, called)
			}
		}
	}
}

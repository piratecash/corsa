package node

import (
	"bufio"
	"context"
	"errors"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// Own genuine TLS/registry publication, not a Session or receipt substitute.
func ownedRawAccepted(t *testing.T, s, dialer *Service) (f1ProofFixture, findnode.Session) {
	t.Helper()
	f := f1PairOn(t, true, nil, s, dialer)
	s.publishAcceptedProofTransport(f.inbound)
	f.inbound.life.mu.Lock()
	b := f.inbound.life.findNode
	if b == nil || !b.published {
		f.inbound.life.mu.Unlock()
		t.Fatal("genuine raw fixture did not publish original listener")
	}
	member := b.session
	f.inbound.life.mu.Unlock()
	return f, member
}

func ownedRawAssertEmpty(t *testing.T, facts ownedFindNodeRawFacts, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || facts.allOriginal != nil || facts.classes != nil {
		t.Fatal("raw refusal returned partial facts or wrong error", facts, err, want)
	}
}

// The slot/dial distinction is exercised by the real accepted CM event. Alias
// setup is controlled bookkeeping, never an invented receipt or positive F1.
func ownedRawPrepareCMAtSlot(t *testing.T, f f1ProofFixture, slotAddress domain.PeerAddress) func() {
	t.Helper()
	p, owner := f.outbound, f.fixtureOwner
	serverDone, readerDone := make(chan struct{}), make(chan struct{})
	owner.serverDone, owner.readerDone = serverDone, readerDone
	owner.serviceJobs = [2]*Service{f.accepted, f.dialer}
	t.Cleanup(func() { owner.stopAndJoin(t) })
	f.accepted.peerMu.RLock()
	core := f.accepted.coreForIDLocked(f.inbound.id)
	f.accepted.peerMu.RUnlock()
	go func() {
		defer close(serverDone)
		reader := bufio.NewReader(core.Conn())
		for {
			line, err := f.accepted.readInboundCommandLineOwned(reader, f.inbound.id, f.inbound)
			if err != nil || !f.accepted.handleCommandOwned(f.inbound.id, line, f.inbound) {
				return
			}
		}
	}()
	go func() { defer close(readerDone); f.dialer.readPeerSession(bufio.NewReader(p.conn), p) }()
	f.dialer.peerMu.Lock()
	f.dialer.dialOrigin[p.address] = slotAddress
	f.dialer.peerMu.Unlock()
	cm := f.dialer.connManager
	cm.mu.Lock()
	cm.generation = 9
	cm.slots = []*slot{{Address: slotAddress, State: domain.SlotStateDialing, Generation: 9}}
	cm.mu.Unlock()
	return func() {
		cm.handleDialSucceeded(f.dialer.runCtx, DialSucceeded{Address: slotAddress, ConnectedAddress: p.address, SlotGeneration: 9, Session: p})
	}
}

func TestOwnedRawF1GenuineListenerAndActualCMKeepOriginals(t *testing.T) {
	s, _, sd := ownedF1TestService(t)
	d, _, dd := ownedF1TestService(t)
	f, listener := ownedRawAccepted(t, s, d)
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4})
	if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != listener || facts.classes[0] != findnode.ClassOrdinary {
		t.Fatal("raw listener lost original provenance/class", facts, err)
	}
	slotAddress := domain.PeerAddress("127.0.0.1:64003")
	if slotAddress == f.outbound.address {
		slotAddress = domain.PeerAddress("127.0.0.1:64004")
	}
	start := ownedRawPrepareCMAtSlot(t, f, slotAddress)
	dispatched := make(chan struct{})
	f.fixtureOwner.caseDone = dispatched
	go func() { defer close(dispatched); start() }()
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("actual CM dispatch did not join")
	}
	waitFor(t, 2*time.Second, "raw actual CM postsetup", func() bool { return len(f1Members(t, d).sessions) == 1 })
	facts, err = d.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4})
	d.peerMu.RLock()
	p := f.outbound
	l := p.common
	l.mu.Lock()
	b, receipt := l.findNode, p.findNodeReceipt
	original := findnode.Session{}
	if b != nil {
		original = b.session
	}
	validReceipt := receipt != nil && receipt.address == slotAddress && receipt.address != p.address
	l.mu.Unlock()
	d.peerMu.RUnlock()
	if err != nil || !validReceipt || len(facts.allOriginal) != 1 || facts.allOriginal[0] != original || facts.classes[0] != findnode.ClassOrdinary {
		t.Fatal("raw CM replaced original or confused fallback slot address", facts, err)
	}
	if _, err := sd.Observer().BeginCapture(); err == nil {
		t.Fatal("raw listener inspection made fixture Ready")
	}
	if _, err := dd.Observer().BeginCapture(); err == nil {
		t.Fatal("raw CM inspection made fixture Ready")
	}
}

func TestOwnedRawF1CompleteMemberAndVisitBounds(t *testing.T) {
	s, _, _ := ownedF1TestService(t)
	a, _, _ := ownedF1TestService(t)
	b, _, _ := ownedF1TestService(t)
	_, first := ownedRawAccepted(t, s, a)
	_, second := ownedRawAccepted(t, s, b)
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 2})
	if err != nil || len(facts.allOriginal) != 2 || len(facts.classes) != 2 {
		t.Fatal("exact M raw coverage failed", facts, err)
	}
	seen := map[findnode.Session]bool{}
	for _, member := range facts.allOriginal {
		seen[member] = true
	}
	// Either original may be a future target; raw capture has no target filter.
	if !seen[first] || !seen[second] {
		t.Fatal("raw original vector was filtered or substituted")
	}
	facts, err = s.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 2})
	ownedRawAssertEmpty(t, facts, err, findnode.ErrCapacity)
	left, right := net.Pipe()
	owner := newF1FixtureOwner()
	owner.rawServer, owner.rawClient = left, right
	t.Cleanup(func() { owner.stopAndJoin(t) })
	id := s.allocateConnID("owned_raw_non_f1_visit", domain.PeerAddress("pipe"))
	core := newTestNetCore(t, id, left, netcore.Inbound, netcore.Options{})
	owner.inboundCore = core
	s.peerMu.Lock()
	s.registerInboundConnLocked(left, core, nil, nil)
	s.peerMu.Unlock()
	facts, err = s.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 2})
	ownedRawAssertEmpty(t, facts, err, findnode.ErrCapacity)
	facts, err = s.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 3})
	if err != nil || len(facts.allOriginal) != 2 {
		t.Fatal("non-F1 visits changed complete original membership", facts, err)
	}
}

func TestOwnedRawF1NilCoreListenerAndOriginalTuple(t *testing.T) {
	s, _, _ := ownedF1TestService(t)
	d, _, _ := ownedF1TestService(t)
	f, member := ownedRawAccepted(t, s, d)
	l := f.inbound.life
	s.peerMu.Lock()
	e := s.connEntryByIDLocked(member.ConnID())
	originalCore := e.core
	e.core = nil
	s.peerMu.Unlock()
	t.Cleanup(func() {
		s.peerMu.Lock()
		if s.connEntryByIDLocked(member.ConnID()) == e {
			e.core = originalCore
		}
		s.peerMu.Unlock()
	})
	profile := findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 2}
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), profile)
	if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != member {
		t.Fatal("nil-core listener lost canonical handle", facts, err)
	}
	for _, kind := range []string{"token", "entry"} {
		t.Run(kind, func(t *testing.T) {
			// Listener fixture has no serving CM worker. Restore before owned cleanup.
			l.mu.Lock()
			token, entry := l.token, l.entry
			if kind == "token" {
				l.token = new(proofTransportServiceToken)
			} else {
				l.entry = nil
			}
			l.mu.Unlock()
			facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), profile)
			l.mu.Lock()
			l.token, l.entry = token, entry
			l.mu.Unlock()
			ownedRawAssertEmpty(t, facts, err, findnode.ErrUnauthenticated)
		})
	}
}

func TestOwnedRawF1StrictClassesStayWithCapturedHandle(t *testing.T) {
	s, _, _ := ownedF1ContactTestService(t)
	k, _, _ := ownedF1TestService(t)
	ordinary, _, _ := ownedF1TestService(t)
	contactResult := addLocalContact(t, s, k.identity)
	if contactResult.Fact != domain.LocalContactFactDurableConfirmed {
		t.Fatal("positive F1 K fact was not durably confirmed", contactResult)
	}
	_, contact := ownedRawAccepted(t, s, k)
	_, other := ownedRawAccepted(t, s, ordinary)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if contactResult.Identity != contact.Identity() {
		t.Fatal("durable fact and genuine TLS identity differ")
	}
	store := s.localContactPinStore()
	if store == nil {
		t.Fatal("positive F1 K has no original pin store")
	}
	store.mu.Lock()
	entry, present := store.identities[contact.Identity()]
	confirmed := present && entry.metadata != nil && entry.classMetadata().Contact.State == domain.IdentityPinAllocationEligible && entry.metadata.confirmedRevision != 0 && entry.metadata.confirmedRevision == entry.protectionRevision
	store.mu.Unlock()
	if !confirmed {
		t.Fatal("positive F1 K lacks current strict pin metadata ACK")
	}
	profile := findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4}
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), profile)
	if err != nil || len(facts.allOriginal) != 2 {
		t.Fatal("genuine strict class capture", facts, err)
	}
	got := map[findnode.Session]findnode.SessionClass{}
	for i, member := range facts.allOriginal {
		got[member] = facts.classes[i]
	}
	if got[contact] != findnode.ClassContact || got[other] != findnode.ClassOrdinary {
		t.Fatal("strict classes detached from original handles", got)
	}
	// Authority-read failure is a negative storage fixture, not positive K mint.
	s.trust.mu.Lock()
	prior := s.trust.authorityReadErr
	s.trust.authorityReadErr = errors.New("owned raw unreadable authority")
	s.trust.mu.Unlock()
	facts, err = s.collectOwnedFindNodeRawFacts(t.Context(), profile)
	s.trust.mu.Lock()
	s.trust.authorityReadErr = prior
	s.trust.mu.Unlock()
	ownedRawAssertEmpty(t, facts, err, findnode.ErrNotReady)
}

func TestOwnedRawF1CopiedProfileCancellationAndNoPartialFacts(t *testing.T) {
	s, _, _ := ownedF1TestService(t)
	d, _, _ := ownedF1TestService(t)
	f, _ := ownedRawAccepted(t, s, d)
	profile := findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 2}
	copied := profile
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), copied)
	profile.MaxMembers = 17
	if err != nil || len(facts.allOriginal) != 1 || cap(facts.allOriginal) != copied.MaxMembers {
		t.Fatal("raw copied profile or buffer bound changed", facts, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	l := f.inbound.life
	l.mu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			l.mu.Unlock()
		}
	})
	type result struct {
		facts ownedFindNodeRawFacts
		err   error
	}
	resultCh := make(chan result, 1)
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	go func() {
		defer close(done)
		facts, err := s.collectOwnedFindNodeRawFacts(ctx, copied)
		resultCh <- result{facts, err}
	}()
	waitFor(t, 2*time.Second, "raw reader reached original common under peer R", func() bool {
		if s.peerMu.TryLock() {
			s.peerMu.Unlock()
			return false
		}
		return true
	})
	cancel()
	l.mu.Unlock()
	locked = false
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("raw cancelled collector did not join")
	}
	r := <-resultCh
	ownedRawAssertEmpty(t, r.facts, r.err, context.Canceled)
}

func TestOwnedRawF1LayoutArithmeticAndPrivateConstructionBoundary(t *testing.T) {
	word := uintptr(strconv.IntSize / 8)
	row := reflect.TypeFor[findnode.Session]().Size() + reflect.TypeFor[findnode.SessionClass]().Size() + reflect.TypeFor[findNodePriorityContact]().Size()
	if row > uintptr(ownedFindNodeRawWordsPerMember)*word {
		t.Fatal("raw allocation arithmetic no longer covers closed row", row)
	}
	if reflect.TypeFor[findnode.Session]().Size() != word || reflect.TypeFor[findnode.SessionClass]().Size() != 1 {
		t.Fatal("raw Session/class closed layout changed")
	}
	contactShape := reflect.TypeFor[findNodePriorityContact]()
	if contactShape.NumField() != 3 || contactShape.Field(0).Name != "applicable" || contactShape.Field(1).Name != "confirmed" || contactShape.Field(2).Name != "err" {
		t.Fatal("raw contact layout widened")
	}
	shape := reflect.TypeFor[ownedFindNodeRawFacts]()
	if shape.NumField() != 2 || shape.Field(0).Name != "allOriginal" || shape.Field(1).Name != "classes" {
		t.Fatal("raw facts retained authority or extra state")
	}
	s, _, _ := ownedF1TestService(t)
	overflow := int(^uint(0)>>1)/(ownedFindNodeRawWordsPerMember*(strconv.IntSize/8)) + 1
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: overflow, MaxRegistryVisits: overflow})
	ownedRawAssertEmpty(t, facts, err, findnode.ErrInvalid)
	facts, err = s.collectOwnedFindNodeRawFacts(nil, findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 1}) //nolint:staticcheck // SA1012: intentional nil-context refusal/order negative; TODO would remove the tested input.
	ownedRawAssertEmpty(t, facts, err, findnode.ErrInvalid)
	plain := &Service{}
	facts, err = plain.collectOwnedFindNodeRawFacts(t.Context(), findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 1})
	ownedRawAssertEmpty(t, facts, err, findnode.ErrNotReady)
	trust, secure := s.trust, s.secureSessions
	s.trust, s.secureSessions = nil, nil
	_, coverage, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 1})
	s.trust, s.secureSessions = trust, secure
	if !errors.Is(err, findnode.ErrNotReady) || coverage != 0 {
		t.Fatal("owned frontdoor reached missing class stores", err)
	}
}

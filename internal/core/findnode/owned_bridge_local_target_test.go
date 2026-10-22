package findnode

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Observe the SAME child parked at the exact original source mutex. Dynamic
// unique source location survives trusted FORMAT without widening the matcher.
func ownedLocalTestGID() uint64 {
	var raw [128]byte
	n := runtime.Stack(raw[:], false)
	fields := bytes.Fields(raw[:n])
	if len(fields) < 2 {
		return 0
	}
	gid, _ := strconv.ParseUint(string(fields[1]), 10, 64)
	return gid
}
func ownedLocalReadGID(started <-chan uint64) uint64 {
	select {
	case gid := <-started:
		return gid
	case <-time.After(2 * time.Second):
		return 0
	}
}
func ownedLocalAwaitSourceWait(gid uint64, method, anchor string) bool {
	raw, err := os.ReadFile("owned_bridge_local_target.go")
	if err != nil || gid == 0 || bytes.Count(raw, []byte(anchor)) != 1 {
		return false
	}
	offset := bytes.Index(raw, []byte(anchor))
	line := bytes.Count(raw[:offset], []byte("\n")) + 1
	location := []byte(fmt.Sprintf("/owned_bridge_local_target.go:%d", line))
	header := []byte(fmt.Sprintf("goroutine %d ", gid))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		complete := false
		var dump []byte
		for size := 512 << 10; size <= 8<<20; size *= 2 {
			dump = make([]byte, size)
			n := runtime.Stack(dump, true)
			dump = dump[:n]
			if n < size {
				complete = true
				break
			}
		}
		if complete {
			for _, block := range bytes.Split(dump, []byte("\n\n")) {
				if bytes.HasPrefix(block, header) && bytes.Contains(block, []byte("sync.(*Mutex).Lock")) && bytes.Contains(block, []byte(method+"(")) && bytes.Contains(block, location) {
					return true
				}
			}
		}
		runtime.Gosched()
	}
	return false
}

type ownedLocalFixtureState struct {
	intake  DormantIntake
	sink    ObservationSink
	local   OwnedLocalTarget
	domain  findnodebridge.FixtureDomain
	clock   *atomic.Int64
	key     *identity.Identity
	sources []Session
}

func ownedLocalFixture(t *testing.T, n int, profiles ...SigningProfile) ownedLocalFixtureState {
	t.Helper()
	d, clock, _ := ownedOriginalDomain(t)
	intake, sink, err := NewExecutingFixtureIntake(d.Consumer(), d.Observer(), ownedBridgeProfile(), max(n, 1))
	if err != nil {
		t.Fatal(err)
	}
	key, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	public, err := identity.ParsePublicKey(key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	profile := SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8}
	if len(profiles) > 1 {
		t.Fatal("one explicit fixture signing profile")
	}
	if len(profiles) == 1 {
		profile = profiles[0]
	}
	local, err := intake.NewLocalTarget(public, domain.NetworkID("gazeta-devnet"), profile)
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]Session, n)
	for i := range n {
		sources[i] = ownedOriginalSession(t, domain.ConnID(i+1), 0)
	}
	return ownedLocalFixtureState{intake, sink, local, d, clock, key, sources}
}
func (f ownedLocalFixtureState) cut(t *testing.T) OwnedCut {
	t.Helper()
	stamp, at := ownedBridgePoint(t, f.domain)
	members := make([]ObservedMember, len(f.sources))
	for i, source := range f.sources {
		members[i] = ObservedMember{Session: source, Class: ClassOrdinary}
	}
	cut, err := f.sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 10*time.Second), members, ObservedDegree{Incoming: len(members), Window: len(members)})
	if err != nil {
		t.Fatal(err)
	}
	return cut
}
func (f ownedLocalFixtureState) received(t *testing.T, source Session, id domain.FindNodeRequestID, challenge byte) OwnedReceivedRequest {
	t.Helper()
	stamp, at := ownedBridgePoint(t, f.domain)
	var nonce domain.FindNodeChallenge
	nonce[0] = challenge
	raw, err := protocol.MarshalFindNodeRequest(protocol.FindNodeRequest{ID: id, Target: f.local.local.target.cache.self, Challenge: nonce})
	if err != nil {
		t.Fatal(err)
	}
	input, err := f.sink.SealReceived(stamp, at, source, raw)
	if err != nil {
		t.Fatal(err)
	}
	return input
}
func (f ownedLocalFixtureState) reserve(t *testing.T, source Session, id domain.FindNodeRequestID, challenge byte) OwnedLocalResult {
	t.Helper()
	result, err := f.local.ReserveReceived(f.received(t, source, id, challenge), f.cut(t))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestOwnedLocalTargetOriginalReceiveAndSingleCalendar(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	public, _ := identity.ParsePublicKey(f.key.PublicKey)
	if _, err := f.intake.NewLocalTarget(public, domain.NetworkID("gazeta-devnet"), SigningProfile{NodeSpacing: time.Second, MaxReservations: 8}); !errors.Is(err, ErrInvalid) {
		t.Fatal("second local calendar minted", err)
	}
	input := f.received(t, source, 11, 1)
	cut := f.cut(t)
	result, err := f.local.ReserveReceived(input, cut)
	if err != nil || result.Kind != LocalReserved {
		t.Fatal("genuine projected local reservation", result, err)
	}
	o := f.local.local.target
	if o.counters.Consumed != 0 || o.calendar.nextActual != 0 || source.state.signingActual.tokens != forwardBurst {
		t.Fatal("projected local reservation spent actual T debt")
	}
	if _, err = f.local.ReserveReceived(input, cut); !errors.Is(err, ErrStaleWork) {
		t.Fatal("local received replay", err)
	}
	if err = result.Work.Cancel(); err != nil {
		t.Fatal(err)
	}
	if o.calendar.nextProjected != Instant(250*time.Millisecond) || source.state.signingProjected.tokens != forwardBurst-forwardToken {
		t.Fatal("cancellation refunded accepted projected debt")
	}
}
func TestOwnedLocalTargetFreshTAfterSourceMutexWait(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	result := f.reserve(t, source, 11, 1)
	source.state.mu.Lock()
	type outcome struct {
		permit OwnedLocalSignPermit
		input  PublicSigningInput
		err    error
	}
	done := make(chan outcome, 1)
	started := make(chan uint64, 1)
	go func() { started <- ownedLocalTestGID(); p, in, err := result.Work.Take(); done <- outcome{p, in, err} }()
	gid := ownedLocalReadGID(started)
	waited := ownedLocalAwaitSourceWait(gid, "OwnedLocalSignWork.Take", "w.identity.source.state.mu.Lock()\n\tdefer w.identity.source.state.mu.Unlock()\n\t// Accepted original work uses current clock->authR custody after ALL waits.")
	f.clock.Store(int64(time.Second))
	source.state.mu.Unlock()
	select {
	case got := <-done:
		if got.permit.work != nil {
			defer func() {
				if _, cleanupErr := got.permit.Complete(nil); cleanupErr != nil && !errors.Is(cleanupErr, ErrStaleWork) {
					t.Error("local permit fallback cleanup", cleanupErr)
				}
			}()
		}
		if !waited {
			t.Fatal("local Take did not actually wait on its exact original source mutex", got.err)
		}
		if got.err != nil {
			t.Fatal(got.err)
		}
		o := f.local.local.target
		if o.last != Instant(time.Second) || o.calendar.nextActual != Instant(1250*time.Millisecond) || source.state.signingActual.at != Instant(time.Second) || source.state.signingActual.tokens != forwardBurst-forwardToken {
			t.Fatal("local T reused pre-wait clock or did not debit both actual budgets")
		}
		sig := targetOwnerSignature(t, f.key, got.input)
		answers, err := got.permit.Complete(&sig)
		if err != nil || len(answers) != 1 || answers[0].answer.source != source || answers[0].answer.body.ID != 11 {
			t.Fatal("local answer lost original source/request", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("local take did not join")
	}
}
func TestOwnedLocalTargetExactDeadlineRefusesFreshT(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	result := f.reserve(t, source, 11, 1)
	source.state.mu.Lock()
	done := make(chan error, 1)
	started := make(chan uint64, 1)
	go func() { started <- ownedLocalTestGID(); _, _, err := result.Work.Take(); done <- err }()
	waited := ownedLocalAwaitSourceWait(ownedLocalReadGID(started), "OwnedLocalSignWork.Take", "w.identity.source.state.mu.Lock()\n\tdefer w.identity.source.state.mu.Unlock()\n\t// Accepted original work uses current clock->authR custody after ALL waits.")
	f.clock.Store(int64(6 * time.Second))
	source.state.mu.Unlock()
	select {
	case err := <-done:
		if !waited {
			t.Fatal("deadline Take did not actually wait on its exact original source mutex", err)
		}
		if !errors.Is(err, ErrStaleWork) {
			t.Fatal("local exact deadline admitted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deadline take did not join")
	}
	o := f.local.local.target
	if o.counters.Consumed != 0 || o.calendar.nextActual != 0 || source.state.signingActual.tokens != forwardBurst || o.calendar.Pending() != 0 {
		t.Fatal("deadline refusal spent actual debt or leaked exact alias")
	}
}
func TestOwnedLocalTargetCannotForgeConsumeOrReplay(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	result := f.reserve(t, f.sources[0], 11, 1)
	if _, err := (OwnedLocalSignPermit{work: result.Work.work}).Complete(nil); !errors.Is(err, ErrStaleWork) {
		t.Fatal("completed local work before T", err)
	}
	p, in, err := result.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = result.Work.Take(); !errors.Is(err, ErrStaleWork) {
		t.Fatal("replayed local T", err)
	}
	sig := targetOwnerSignature(t, f.key, in)
	if _, err = p.Complete(&sig); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Complete(&sig); !errors.Is(err, ErrStaleWork) {
		t.Fatal("replayed local completion", err)
	}
	if f.local.local.target.counters.Consumed != 1 {
		t.Fatal("local ticket double-debited actual budget")
	}
}
func TestOwnedLocalTargetEndAfterTCompletesOriginalWithoutReplacement(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	result := f.reserve(t, source, 11, 1)
	p, in, err := result.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	publication := f.domain.Publisher().BeginPublication()
	source.End()
	publication.Complete()
	if err = f.local.EndOriginalSession(source); err != nil {
		t.Fatal(err)
	}
	f.clock.Store(int64(time.Second))
	sig := targetOwnerSignature(t, f.key, in)
	answers, err := p.Complete(&sig)
	if err != nil || len(answers) != 0 {
		t.Fatal("End made consumed local completion impossible or retargeted answer", err)
	}
	if f.local.local.target.counters.Consumed != 1 || f.local.local.target.counters.Completed != 1 {
		t.Fatal("End refunded or erased accepted local T")
	}
}
func TestOwnedLocalTargetSigningFailureRetiresWithoutRefund(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	result := f.reserve(t, source, 11, 1)
	p, _, err := result.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Complete(nil); !errors.Is(err, ErrStaleWork) {
		t.Fatal("failed signer accepted answer", err)
	}
	o := f.local.local.target
	if source.state.signingActual.tokens != forwardBurst-forwardToken || o.calendar.nextActual != Instant(250*time.Millisecond) || o.calendar.Pending() != 0 || result.Work.work.entry.phase != targetCancelled {
		t.Fatal("failed signing refunded T or left live signing work")
	}
}
func TestOwnedLocalTargetDuplicateRecipientsKeepOriginalIDs(t *testing.T) {
	f := ownedLocalFixture(t, 2)
	first := f.reserve(t, f.sources[0], 11, 1)
	second := f.reserve(t, f.sources[1], 22, 1)
	if second.Kind != LocalWaitingExisting || second.Work.work != nil {
		t.Fatal("duplicate local challenge minted second signing ticket")
	}
	p, in, err := first.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	sig := targetOwnerSignature(t, f.key, in)
	answers, err := p.Complete(&sig)
	if err != nil || len(answers) != 2 || answers[0].answer.source != f.sources[0] || answers[0].answer.body.ID != 11 || answers[1].answer.source != f.sources[1] || answers[1].answer.body.ID != 22 {
		t.Fatal("duplicate delivery lost exact original recipients/IDs", err)
	}
	if f.local.local.target.counters.Consumed != 1 {
		t.Fatal("duplicate challenge spent signing budget twice")
	}
}

func TestOwnedLocalTargetStagingKeepsLiveMapsAndDebtUntouched(t *testing.T) {
	f := ownedLocalFixture(t, 2)
	first := f.reserve(t, f.sources[0], 11, 1)
	input := f.received(t, f.sources[1], 22, 1)
	cut := f.cut(t)
	o := f.local.local.target
	original := first.Work.work.entry
	oldAlias := original.coordinated.alias
	priorities, err := NewPrioritySnapshot(o.cache.self, f.sources, Instant(cut.sealedCut.at.Elapsed()), func(SessionInfo) (SessionClass, error) { return ClassOrdinary, nil })
	if err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	o.cache.mu.Lock()
	o.calendar.mu.Lock()
	f.sources[1].state.mu.Lock()
	plan, err := stageOwnedLocalAdmissionLocked(o, input.receivedRequest.identity, priorities, Instant(6*time.Second))
	if err != nil {
		f.sources[1].state.mu.Unlock()
		o.calendar.mu.Unlock()
		o.cache.mu.Unlock()
		o.mu.Unlock()
		t.Fatal(err)
	}
	staged := plan.cache.entries[original.challenge]
	delete(staged.recipients, f.sources[0])
	staged.coordinated.alias = SigningReservation{}
	untouched := original.recipients[f.sources[0]] == 11 && original.coordinated.alias == oldAlias && len(o.cache.entries) == 1 && len(o.cache.sessions) == 1 && len(o.calendar.slots) == 1 && o.calendar.nextProjected == Instant(250*time.Millisecond) && o.calendar.nextActual == 0 && f.sources[1].state.signer == nil && f.sources[1].state.signingActual == (forwardBucket{}) && f.sources[1].state.signingProjected == (forwardBucket{})
	f.sources[1].state.mu.Unlock()
	o.calendar.mu.Unlock()
	o.cache.mu.Unlock()
	o.mu.Unlock()
	if !untouched {
		t.Fatal("detached local stage aliased live recipients/coordinated state or charged debt")
	}
	if err = first.Work.Cancel(); err != nil {
		t.Fatal(err)
	}
}
func TestOwnedLocalTargetStagedAdmissionPreservesOtherOriginalTicket(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	first := f.reserve(t, source, 11, 1)
	original := first.Work.work.entry
	alias := original.coordinated.alias
	second := f.reserve(t, source, 22, 2)
	if f.local.local.target.cache.entries[original.challenge] != original || original.coordinated.alias != alias {
		t.Fatal("later staging replaced original entry generation or alias")
	}
	p, in, err := first.Work.Take()
	if err != nil {
		t.Fatal("later staging invalidated original signing ticket", err)
	}
	sig := targetOwnerSignature(t, f.key, in)
	if _, err = p.Complete(&sig); err != nil {
		t.Fatal(err)
	}
	f.clock.Store(int64(250 * time.Millisecond))
	p, in, err = second.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	sig = targetOwnerSignature(t, f.key, in)
	if _, err = p.Complete(&sig); err != nil {
		t.Fatal(err)
	}
	o := f.local.local.target
	if o.counters.Consumed != 2 || o.calendar.nextActual != Instant(500*time.Millisecond) || source.state.signingActual.tokens != forwardBurst-2*forwardToken+Instant(250*time.Millisecond) {
		t.Fatal("independent original local tickets did not retain separate actual debt")
	}
}

func TestOwnedLocalTargetRuntimeConstructingAndForeignObserver(t *testing.T) {
	key, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	public, err := identity.ParsePublicKey(key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	profile := SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8}
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "original-runtime-constructing", true: "foreign-original-observer"}[foreign], func(t *testing.T) {
			done := make(chan struct{})
			t.Cleanup(func() { close(done) })
			d, err := findnodebridge.NewRuntimeDomain(done)
			if err != nil {
				t.Fatal(err)
			}
			observer := d.Observer()
			if foreign {
				otherDone := make(chan struct{})
				t.Cleanup(func() { close(otherDone) })
				other, err := findnodebridge.NewRuntimeDomain(otherDone)
				if err != nil {
					t.Fatal(err)
				}
				observer = other.Observer()
			}
			intake, _, err := NewExecutingIntake(d.Consumer(), observer, ownedBridgeProfile(), 8)
			if err != nil {
				t.Fatal(err)
			}
			local, err := intake.NewLocalTarget(public, domain.NetworkID("gazeta-devnet"), profile)
			if foreign {
				if !errors.Is(err, ErrUnauthenticated) || local.local != nil || intake.dormantOwner.localTarget != nil {
					t.Fatal("foreign original observer constructed local clock authority", err)
				}
			} else if err != nil || local.local == nil {
				t.Fatal("original runtime local target requires premature Ready", err)
			}
			if _, err = d.Observer().BeginCapture(); err == nil {
				t.Fatal("local constructor prematurely changed Runtime to Ready")
			}
			if lease, err := d.Consumer().TryReadClock(findnodebridge.CaptureStamp{}); err == nil || lease != nil {
				if lease != nil {
					lease.Release()
				}
				t.Fatal("construction point granted admission lease")
			}
			if !foreign {
				if _, err := local.ReserveReceived(OwnedReceivedRequest{}, OwnedCut{}); err == nil {
					t.Fatal("Constructing local target admitted without original ingress/cut")
				}
			}
			// This owner-controlled test seal succeeds exactly once after construction;
			// NewLocalTarget itself never publishes the construction transition.
			if err = d.Publisher().SealConstruction(); err != nil {
				t.Fatal("constructor consumed construction seal", err)
			}
		})
	}
}

func TestOwnedLocalTargetStagedRefusalPreservesAcceptedOriginal(t *testing.T) {
	f := ownedLocalFixture(t, 2)
	first := f.reserve(t, f.sources[0], 11, 1)
	original := first.Work.work.entry
	alias := original.coordinated.alias
	input := f.received(t, f.sources[1], 22, 2)
	stamp, at := ownedBridgePoint(t, f.domain)
	cut, err := f.sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 500*time.Millisecond), []ObservedMember{{Session: f.sources[0], Class: ClassOrdinary}, {Session: f.sources[1], Class: ClassOrdinary}}, ObservedDegree{Incoming: 2, Window: 2})
	if err != nil {
		t.Fatal(err)
	}
	f.sources[1].state.mu.Lock()
	done := make(chan error, 1)
	started := make(chan uint64, 1)
	go func() { started <- ownedLocalTestGID(); _, err := f.local.ReserveReceived(input, cut); done <- err }()
	waited := ownedLocalAwaitSourceWait(ownedLocalReadGID(started), "OwnedLocalTarget.ReserveReceived", "id.source.state.mu.Lock()")
	f.clock.Store(int64(500 * time.Millisecond))
	f.sources[1].state.mu.Unlock()
	select {
	case err := <-done:
		if !waited {
			t.Fatal("local reservation did not actually wait on its exact original source mutex", err)
		}
		if !errors.Is(err, ErrStaleWork) {
			t.Fatal("staged local refusal crossed exact cut boundary", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("staged local admission did not join")
	}
	o := f.local.local.target
	if len(o.cache.entries) != 1 || o.cache.entries[original.challenge] != original || original.coordinated.alias != alias || original.recipients[f.sources[0]] != 11 || o.calendar.Pending() != 1 || o.calendar.nextProjected != Instant(250*time.Millisecond) || o.calendar.nextActual != 0 || f.sources[1].state.signer != nil {
		t.Fatal("refused detached stage changed accepted original/pointer/alias/debt")
	}
	if err = first.Work.Cancel(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedLocalTargetOutOfOrderDuplicatePreservesOriginalReplyDeadline(t *testing.T) {
	f := ownedLocalFixture(t, 2)
	earlier := f.received(t, f.sources[1], 22, 1) // genuine earlier receipt, processed later
	f.clock.Store(int64(time.Second))
	first := f.reserve(t, f.sources[0], 11, 1)
	result, err := f.local.ReserveReceived(earlier, f.cut(t))
	if !errors.Is(err, ErrBudget) || result.Kind != 0 {
		t.Fatal("out-of-order duplicate exceeded its original response deadline", err)
	}
	entry := first.Work.work.entry
	if entry.coordinated.signBy != Instant(7*time.Second) || len(entry.recipients) != 1 || entry.recipients[f.sources[0]] != 11 || f.sources[1].state.signer != nil {
		t.Fatal("earlier duplicate rewrote accepted original alias/history/debt")
	}
	permit, in, err := first.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	sig := targetOwnerSignature(t, f.key, in)
	answers, err := permit.Complete(&sig)
	if err != nil || len(answers) != 1 || answers[0].answer.expires != Instant(7*time.Second) {
		t.Fatal("local completion refreshed/lost original signing deadline", err)
	}
}

func TestOwnedLocalTargetCachedCopyUsesThisOriginalResponseExpiry(t *testing.T) {
	f := ownedLocalFixture(t, 2)
	first := f.reserve(t, f.sources[0], 11, 1)
	permit, in, err := first.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	sig := targetOwnerSignature(t, f.key, in)
	if _, err = permit.Complete(&sig); err != nil {
		t.Fatal(err)
	}
	f.clock.Store(int64(time.Second))
	result := f.reserve(t, f.sources[1], 22, 1)
	if result.Kind != LocalReadyCopy || result.Work.work != nil || result.Copy.answer == nil || result.Copy.answer.source != f.sources[1] || result.Copy.answer.body.ID != 22 || result.Copy.answer.expires != Instant(7*time.Second) {
		t.Fatal("cached copy lost this original recipient/request deadline")
	}
	entry := first.Work.work.entry
	if entry.expires != cacheLifetime {
		t.Fatal("answer cache retention changed to request response deadline")
	}
	queued, err := result.Copy.QueueAnswerForOriginal(f.sources[1])
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Store(int64(7 * time.Second))
	if _, _, err = queued.Take(); !errors.Is(err, ErrNotReady) {
		t.Fatal("cached answer used cache retention as original response expiry", err)
	}
	if f.local.local.target.counters.Consumed != 1 || f.sources[1].state.signer != nil {
		t.Fatal("cached answer minted another local T/debit")
	}
}

func TestOwnedLocalTargetEndBeforeTRefusesWithoutActualDebit(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	result := f.reserve(t, source, 11, 1)
	publication := f.domain.Publisher().BeginPublication()
	source.End()
	publication.Complete()
	if err := f.local.EndOriginalSession(source); err != nil {
		t.Fatal(err)
	}
	if _, _, err := result.Work.Take(); err == nil {
		t.Fatal("ended original source acquired local T")
	}
	o := f.local.local.target
	if o.counters.Consumed != 0 || o.calendar.nextActual != 0 || source.state.signingActual.tokens != forwardBurst || o.calendar.Pending() != 0 || result.Work.work.entry.phase != targetCancelled {
		t.Fatal("End before local T spent actual debt or retained exact signing alias")
	}
}

func (f ownedLocalFixtureState) cutClasses(t *testing.T, classes []SessionClass) OwnedCut {
	t.Helper()
	if len(classes) != len(f.sources) {
		t.Fatal("exact original class fixture length")
	}
	stamp, at := ownedBridgePoint(t, f.domain)
	members := make([]ObservedMember, len(f.sources))
	for i, source := range f.sources {
		members[i] = ObservedMember{Session: source, Class: classes[i]}
	}
	cut, err := f.sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 10*time.Second), members, ObservedDegree{Incoming: len(members), Window: len(members)})
	if err != nil {
		t.Fatal(err)
	}
	return cut
}
func TestOwnedLocalTargetRejectsProjectedSlotAtStrictDeadline(t *testing.T) {
	f := ownedLocalFixture(t, 2, SigningProfile{NodeSpacing: 6 * time.Second, MaxReservations: 8})
	first := f.reserve(t, f.sources[0], 11, 1)
	_, err := f.local.ReserveReceived(f.received(t, f.sources[1], 22, 2), f.cut(t))
	if !errors.Is(err, ErrBudget) {
		t.Fatal("projected alias at strict local T deadline was accepted", err)
	}
	o := f.local.local.target
	if len(o.cache.entries) != 1 || o.calendar.Pending() != 1 || o.calendar.nextProjected != Instant(6*time.Second) || f.sources[1].state.signer != nil {
		t.Fatal("impossible signing slot changed accepted row or projected debt")
	}
	if err = first.Work.Cancel(); err != nil {
		t.Fatal(err)
	}
}
func TestOwnedLocalTargetAcceptedSlotSurvivesOldCutAndUnrelatedEnd(t *testing.T) {
	for _, endOther := range []bool{false, true} {
		t.Run(map[bool]string{false: "expired-selection-cut", true: "unrelated-original-End"}[endOther], func(t *testing.T) {
			f := ownedLocalFixture(t, 2)
			first := f.reserve(t, f.sources[0], 11, 1)
			input := f.received(t, f.sources[1], 22, 2)
			stamp, at := ownedBridgePoint(t, f.domain)
			cut, err := f.sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 100*time.Millisecond), []ObservedMember{{Session: f.sources[0], Class: ClassOrdinary}, {Session: f.sources[1], Class: ClassOrdinary}}, ObservedDegree{Incoming: 2, Window: 2})
			if err != nil {
				t.Fatal(err)
			}
			second, err := f.local.ReserveReceived(input, cut)
			if err != nil {
				t.Fatal(err)
			}
			publication := f.domain.Publisher().BeginPublication()
			if endOther {
				f.sources[0].End()
			}
			publication.Complete()
			if endOther {
				if err = f.local.EndOriginalSession(f.sources[0]); err != nil {
					t.Fatal(err)
				}
			}
			f.clock.Store(int64(250 * time.Millisecond))
			permit, in, err := second.Work.Take()
			if err != nil {
				t.Fatal("accepted local alias re-admitted historical selection cut/other original", err)
			}
			sig := targetOwnerSignature(t, f.key, in)
			answers, err := permit.Complete(&sig)
			if err != nil || len(answers) != 1 || answers[0].answer.source != f.sources[1] || answers[0].answer.body.ID != 22 {
				t.Fatal("accepted exact source lost local completion after unrelated publication", err)
			}
			if !endOther {
				if err = first.Work.Cancel(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestOwnedLocalTargetAcceptedTDoesNotGateOnUnrelatedExpiredEntry(t *testing.T) {
	f := ownedLocalFixture(t, 2)
	first := f.reserve(t, f.sources[0], 11, 1)
	original := first.Work.work.entry
	alias := original.coordinated.alias
	f.clock.Store(int64(time.Second))
	second := f.reserve(t, f.sources[1], 22, 2)
	f.sources[1].state.mu.Lock()
	type result struct {
		permit OwnedLocalSignPermit
		input  PublicSigningInput
		err    error
	}
	done := make(chan result, 1)
	started := make(chan uint64, 1)
	go func() { started <- ownedLocalTestGID(); p, in, err := second.Work.Take(); done <- result{p, in, err} }()
	waited := ownedLocalAwaitSourceWait(ownedLocalReadGID(started), "OwnedLocalSignWork.Take", "w.identity.source.state.mu.Lock()\n\tdefer w.identity.source.state.mu.Unlock()\n\t// Accepted original work uses current clock->authR custody after ALL waits.")
	f.clock.Store(int64(6 * time.Second))
	f.sources[1].state.mu.Unlock()
	select {
	case got := <-done:
		if got.permit.work != nil {
			defer func() {
				if _, cleanupErr := got.permit.Complete(nil); cleanupErr != nil && !errors.Is(cleanupErr, ErrStaleWork) {
					t.Error("local permit fallback cleanup", cleanupErr)
				}
			}()
		}
		if !waited || got.err != nil {
			t.Fatal("accepted current T was blocked by unrelated expired original", got.err)
		}
		o := f.local.local.target
		if original.coordinated == nil || original.coordinated.alias != alias || o.calendar.Pending() != 1 || f.sources[0].state.signingProjected.tokens != forwardBurst-forwardToken {
			t.Fatal("accepted T erased unrelated original alias/debt under R")
		}
		sig := targetOwnerSignature(t, f.key, got.input)
		answers, err := got.permit.Complete(&sig)
		if err != nil || len(answers) != 1 || answers[0].answer.source != f.sources[1] {
			t.Fatal("exact current completion lost after normal other-row cleanup", err)
		}
		if f.sources[0].state.signingProjected.tokens != forwardBurst-forwardToken {
			t.Fatal("normal expiry refunded other's accepted debt")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("accepted T did not join after original source wait")
	}
}
func TestOwnedLocalTargetAcceptedL1CannotRedrawIntoPressureVictim(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	source := f.sources[0]
	first, err := f.local.ReserveReceived(f.received(t, source, 11, 1), f.cutClasses(t, []SessionClass{ClassContact}))
	if err != nil {
		t.Fatal(err)
	}
	permit, in, err := first.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	original := first.Work.work.entry
	for i := 2; i <= 8; i++ {
		f.reserve(t, source, domain.FindNodeRequestID(10+i), byte(i))
	} // current draw now Ordinary
	ninth := f.reserve(t, source, 19, 9)
	if ninth.Kind != LocalReserved || f.local.local.target.cache.entries[original.challenge] != original || original.coordinated == nil || original.coordinated.phase != signingConsumed {
		t.Fatal("live accepted L1 promise was redrawn and evicted by later own-row pressure")
	}
	sig := targetOwnerSignature(t, f.key, in)
	answers, err := permit.Complete(&sig)
	if err != nil || len(answers) != 1 || answers[0].answer.body.ID != 11 {
		t.Fatal("accepted L1 consumed permit revoked by later arrival", err)
	}
}
func TestOwnedLocalTargetProtectedDuplicatePromiseAndExactEnd(t *testing.T) {
	for _, endProtected := range []bool{false, true} {
		t.Run(map[bool]string{false: "pressure-after-class-redraw", true: "exact-protected-recipient-End"}[endProtected], func(t *testing.T) {
			f := ownedLocalFixture(t, 2)
			first := f.reserve(t, f.sources[0], 11, 1)
			waiting, err := f.local.ReserveReceived(f.received(t, f.sources[1], 22, 1), f.cutClasses(t, []SessionClass{ClassOrdinary, ClassContact}))
			if err != nil || waiting.Kind != LocalWaitingExisting {
				t.Fatal("genuine protected duplicate not retained", err)
			}
			permit, in, err := first.Work.Take()
			if err != nil {
				t.Fatal(err)
			}
			entry := first.Work.work.entry
			if endProtected {
				publication := f.domain.Publisher().BeginPublication()
				f.sources[1].End()
				publication.Complete()
				if err = f.local.EndOriginalSession(f.sources[1]); err != nil {
					t.Fatal(err)
				}
				for _, protected := range entry.coordinated.protected {
					if protected == f.sources[1] {
						t.Fatal("ended historical recipient retained L1 privilege/reference")
					}
				}
			} else {
				for i := 2; i <= 8; i++ {
					f.reserve(t, f.sources[1], domain.FindNodeRequestID(20+i), byte(i))
				}
				f.reserve(t, f.sources[1], 29, 9)
				if f.local.local.target.cache.entries[entry.challenge] != entry {
					t.Fatal("protected duplicate's accepted shared job evicted after redraw")
				}
			}
			sig := targetOwnerSignature(t, f.key, in)
			answers, err := permit.Complete(&sig)
			want := 2
			if endProtected {
				want = 1
			}
			if err != nil || len(answers) != want || answers[0].answer.source != f.sources[0] || answers[0].answer.body.ID != 11 {
				t.Fatal("protected shared job lost exact recipient completion", err)
			}
		})
	}
}

func TestOwnedLocalTargetEarlyTakePreservesAcceptedAliasForLegalSlot(t *testing.T) {
	f := ownedLocalFixture(t, 2)
	first := f.reserve(t, f.sources[0], 11, 1)
	second := f.reserve(t, f.sources[1], 22, 2)
	entry := second.Work.work.entry
	alias := entry.coordinated.alias
	if _, _, err := second.Work.Take(); !errors.Is(err, ErrNotReady) {
		t.Fatal("future accepted local slot admitted before legal time", err)
	}
	o := f.local.local.target
	if second.Work.work.phase.Load() != ownedLocalSignReady || entry.coordinated == nil || entry.coordinated.alias != alias || o.calendar.Pending() != 2 || o.counters.Consumed != 0 || o.calendar.nextActual != 0 || f.sources[1].state.signingActual.tokens != forwardBurst {
		t.Fatal("early actual Take lost accepted alias/ticket/debt")
	}
	f.clock.Store(int64(250 * time.Millisecond))
	permit, in, err := second.Work.Take()
	if err != nil {
		t.Fatal("accepted future local slot lost after lawful early refusal", err)
	}
	sig := targetOwnerSignature(t, f.key, in)
	answers, err := permit.Complete(&sig)
	if err != nil || len(answers) != 1 || answers[0].answer.body.ID != 22 || o.counters.Consumed != 1 {
		t.Fatal("accepted future slot did not consume exactly once", err)
	}
	if err = first.Work.Cancel(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedLocalTargetCompletionNeedsNoCurrentPublicationLease(t *testing.T) {
	f := ownedLocalFixture(t, 1)
	result := f.reserve(t, f.sources[0], 11, 1)
	permit, in, err := result.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	sig := targetOwnerSignature(t, f.key, in)
	publication := f.domain.Publisher().BeginPublication() // genuine unrelated W, no clock held
	type completed struct {
		answers []OwnedAnswerWork
		err     error
	}
	done := make(chan completed, 1)
	go func() { answers, err := permit.Complete(&sig); done <- completed{answers, err} }()
	beforeRelease := false
	var got completed
	select {
	case got = <-done:
		beforeRelease = true
	case <-time.After(2 * time.Second):
	}
	publication.Complete()
	if !beforeRelease {
		select {
		case got = <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("local completion did not join after publication release")
		}
	}
	if !beforeRelease {
		t.Fatal("local signing completion reacquired current R instead of original clock-only completion", got.err)
	}
	if got.err != nil || len(got.answers) != 1 || got.answers[0].answer.source != f.sources[0] {
		t.Fatal("clock-only completion lost original source", got.err)
	}
}

// The second Reserve owns owner/cache/calendar before the first work performs
// its pre-lock original check. Stack observation joins no application channel
// from that check to install: the old blanket copy conflicts under -race.
func TestOwnedLocalTargetConcurrentStageKeepsOriginalChallenge(t *testing.T) {
	for _, sameChallenge := range []bool{false, true} {
		name := "different-challenge"
		if sameChallenge {
			name = "duplicate-challenge"
		}
		t.Run(name, func(t *testing.T) {
			f := ownedLocalFixture(t, 2)
			first := f.reserve(t, f.sources[0], 11, 1)
			o := f.local.local.target
			original := first.Work.work.entry
			challenge := original.challenge
			alias := original.coordinated.alias
			secondChallenge := byte(2)
			if sameChallenge {
				secondChallenge = 1
			}
			input, cut := f.received(t, f.sources[1], 22, secondChallenge), f.cut(t)
			type reserveResult struct {
				result OwnedLocalResult
				err    error
			}
			type delayResult struct {
				delay time.Duration
				err   error
			}
			reserved := make(chan reserveResult, 1)
			delayed := make(chan delayResult, 1)
			reserveStarted, waitStarted := false, false
			reserveJoined, waitJoined := false, false
			var second OwnedLocalResult
			f.sources[1].state.mu.Lock()
			sourceHeld := true
			unlockSource := func() {
				if sourceHeld {
					sourceHeld = false
					f.sources[1].state.mu.Unlock()
				}
			}
			// Installed before dispatch; every early failure first releases the
			// original source gate, then joins both exact children before fixture End.
			t.Cleanup(func() {
				unlockSource()
				if reserveStarted && !reserveJoined {
					select {
					case got := <-reserved:
						second, reserveJoined = got.result, true
					case <-time.After(2 * time.Second):
						t.Error("concurrent stage cleanup did not join original Reserve")
					}
				}
				if waitStarted && !waitJoined {
					select {
					case <-delayed:
						waitJoined = true
					case <-time.After(2 * time.Second):
						t.Error("concurrent stage cleanup did not join original WaitDelay")
					}
				}
				_ = second.Work.Cancel()
				_ = first.Work.Cancel()
			})
			reserveGID := make(chan uint64, 1)
			reserveStarted = true
			go func() {
				reserveGID <- ownedLocalTestGID()
				result, err := f.local.ReserveReceived(input, cut)
				reserved <- reserveResult{result, err}
			}()
			if !ownedLocalAwaitSourceWait(ownedLocalReadGID(reserveGID), "OwnedLocalTarget.ReserveReceived", "id.source.state.mu.Lock()") {
				t.Fatal("second Reserve did not hold original owner/cache/calendar at exact source wait")
			}
			waitGID := make(chan uint64, 1)
			waitStarted = true
			go func() {
				waitGID <- ownedLocalTestGID()
				delay, err := first.Work.WaitDelay()
				delayed <- delayResult{delay, err}
			}()
			if !ownedLocalAwaitSourceWait(ownedLocalReadGID(waitGID), "OwnedLocalSignWork.WaitDelay", "o.mu.Lock()\n\tdefer o.mu.Unlock()\n\to.cache.mu.Lock()") {
				t.Fatal("first original precheck did not reach owner wait before second stage install")
			}
			unlockSource()
			select {
			case got := <-reserved:
				second, reserveJoined = got.result, true
				want := LocalReserved
				if sameChallenge {
					want = LocalWaitingExisting
				}
				if got.err != nil || second.Kind != want {
					t.Fatal("concurrent original second reservation refused", second.Kind, got.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("second original Reserve did not join")
			}
			select {
			case got := <-delayed:
				waitJoined = true
				if got.err != nil || got.delay != 0 {
					t.Fatal("concurrent stage invalidated accepted original wake hint", got.delay, got.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first original WaitDelay did not join")
			}
			if original.challenge != challenge || o.cache.entries[challenge] != original || original.coordinated.alias != alias {
				t.Fatal("concurrent stage changed immutable challenge, original generation or exact alias")
			}
			if o.calendar.nextActual != 0 || o.counters.Consumed != 0 || f.sources[0].state.signingActual.tokens != forwardBurst {
				t.Fatal("concurrent stage or wake hint spent actual T debt")
			}
			permit, signingInput, err := first.Work.Take()
			if err != nil || signingInput.Challenge() != challenge {
				t.Fatal("concurrent stage lost exact accepted first T", err)
			}
			defer func() {
				if _, cleanupErr := permit.Complete(nil); cleanupErr != nil && !errors.Is(cleanupErr, ErrStaleWork) {
					t.Error("local permit fallback cleanup", cleanupErr)
				}
			}()
			signature := targetOwnerSignature(t, f.key, signingInput)
			answers, err := permit.Complete(&signature)
			wantAnswers := 1
			if sameChallenge {
				wantAnswers = 2
			}
			if err != nil || len(answers) != wantAnswers || answers[0].answer.source != f.sources[0] || answers[0].answer.body.ID != 11 {
				t.Fatal("concurrent stage completion lost original answer", err)
			}
			if sameChallenge && (answers[1].answer.source != f.sources[1] || answers[1].answer.body.ID != 22) {
				t.Fatal("concurrent duplicate lost exact second original recipient")
			}
			if o.counters.Consumed != 1 || o.calendar.nextActual != Instant(250*time.Millisecond) || f.sources[0].state.signingActual.tokens != forwardBurst-forwardToken {
				t.Fatal("concurrent original T did not debit exactly once")
			}
		})
	}
}

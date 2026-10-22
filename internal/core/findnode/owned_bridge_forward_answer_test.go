package findnode

import (
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func ownedRelayAnswerFixture(t *testing.T) (executingFixture, OwnedForwardWork, OwnedWritePermit, protocol.FindNodeAnswer) {
	t.Helper()
	f := executingNew(t, 1, 4, false)
	target, id := provenFixture(t, 2, "owned-originals", 0)
	f.members[1] = target
	batch := f.admit(t, 1)
	work, ok := batch.Next()
	if !ok {
		t.Fatal("actual original reservation absent")
	}
	queued, err := work.QueueForOriginal(target)
	if err != nil {
		t.Fatal(err)
	}
	_, permit, err := queued.Take()
	if err != nil {
		t.Fatal(err)
	}
	answer := bodyFor(t, id).Answer(work.work.forwarded.ID)
	return f, work, permit, answer
}
func ownedRelaySeal(t *testing.T, f executingFixture, target Session, answer protocol.FindNodeAnswer) OwnedReceivedAnswer {
	t.Helper()
	raw, err := protocol.MarshalFindNodeAnswer(answer)
	if err != nil {
		t.Fatal(err)
	}
	stamp, at := ownedBridgePoint(t, f.domain)
	input, err := f.sink.SealAnswer(stamp, at, target, raw)
	if err != nil {
		t.Fatal(err)
	}
	return input
}
func TestOwnedRelayAnswerRestoresOnlyEnvelopeAndOriginalWriter(t *testing.T) {
	f, forward, permit, answer := ownedRelayAnswerFixture(t)
	if err := permit.Finish(true); err != nil {
		t.Fatal(err)
	}
	input := ownedRelaySeal(t, f, f.members[1], answer)
	result, err := f.intake.MatchOwnedAnswer(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceSession() != f.members[0] || result.answer.expires != forward.work.receipt.reservation.entry.expires {
		t.Fatal("relay lost original source or response expiry")
	}
	expected := answer
	expected.ID = 17
	if !reflect.DeepEqual(result.answer.body, expected) {
		t.Fatal("relay altered signed body or failed original envelope restore")
	}
	if _, err := f.intake.MatchOwnedAnswer(input); !errors.Is(err, ErrStaleWork) {
		t.Fatal("same receive cell replayed", err)
	}
	replacement := ownedOriginalSession(t, domain.ConnID(1), 0)
	if _, err := result.QueueAnswerForOriginal(replacement); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("numeric source replacement accepted", err)
	}
	queued, err := result.QueueAnswerForOriginal(f.members[0])
	if err != nil {
		t.Fatal(err)
	}
	data, done, err := queued.Take()
	if err != nil {
		t.Fatal(err)
	}
	got, err := protocol.ParseFindNodeAnswer(data)
	if err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatal("typed writer bytes changed signed answer", got, err)
	}
	if err := done.Finish(true); err != nil {
		t.Fatal(err)
	}
	if err := done.Finish(true); !errors.Is(err, ErrStaleWork) {
		t.Fatal("answer permit replayed", err)
	}
	if f.intake.dormantOwner.coordinator.snapshot().pending != 0 {
		t.Fatal("answer left original pending row")
	}
}
func TestOwnedRelayAnswerDuringDispatchRetainsOriginalUntilFinish(t *testing.T) {
	f, forward, permit, answer := ownedRelayAnswerFixture(t)
	result, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
	if err != nil {
		t.Fatal(err)
	}
	entry := forward.work.receipt.reservation.entry
	if entry.intake.phase != candidateTerminalDispatch || f.intake.dormantOwner.coordinator.snapshot().pending != 1 {
		t.Fatal("early answer discarded original in-flight writer")
	}
	if err := result.CancelAnswer(); err != nil {
		t.Fatal(err)
	}
	if err := permit.Finish(true); err != nil {
		t.Fatal(err)
	}
	if f.intake.dormantOwner.coordinator.snapshot().pending != 0 || f.members[1].state.nextID != 1 {
		t.Fatal("original Finish failed terminal row or refunded ID")
	}
}
func TestOwnedRelayAnswerWrongTargetAndIDCannotCorrelate(t *testing.T) {
	for _, kind := range []string{"id", "session", "body-target"} {
		t.Run(kind, func(t *testing.T) {
			f, forward, permit, answer := ownedRelayAnswerFixture(t)
			if err := permit.Finish(true); err != nil {
				t.Fatal(err)
			}
			target := f.members[1]
			switch kind {
			case "id":
				answer.ID++
			case "session":
				target = ownedOriginalSession(t, 2, 0)
			default:
				other, err := identity.Generate()
				if err != nil {
					t.Fatal(err)
				}
				answer = bodyFor(t, other).Answer(answer.ID)
			}
			before := forward.work.receipt.reservation.entry.intake.calendar.projected
			_, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, target, answer))
			if !errors.Is(err, ErrUnmatchedAnswer) {
				t.Fatal("foreign target/id correlated", err)
			}
			if f.intake.dormantOwner.coordinator.snapshot().pending != 1 || forward.work.receipt.reservation.entry.intake.calendar.projected != before {
				t.Fatal("unmatched answer mutated original debt/row")
			}
		})
	}
}
func TestOwnedRelayAnswerFreshExpiryAfterCalendarWait(t *testing.T) {
	f, forward, permit, answer := ownedRelayAnswerFixture(t)
	if err := permit.Finish(true); err != nil {
		t.Fatal(err)
	}
	input := ownedRelaySeal(t, f, f.members[1], answer)
	entry := forward.work.receipt.reservation.entry
	calendar := entry.intake.calendar
	debt := calendar.projected
	// Observe this exact child at the actual original calendar lock, not merely
	// a launched goroutine or a delay. The source line is derived from the pinned
	// current body so canonical FORMAT cannot weaken this synchronization gate.
	source, err := os.ReadFile("owned_bridge_forward_answer.go")
	if err != nil {
		t.Fatal(err)
	}
	anchor := "calendar.mu.Lock()"
	if strings.Count(string(source), anchor) != 1 {
		t.Fatal("exact original calendar wait anchor")
	}
	line := strings.Count(string(source[:strings.Index(string(source), anchor)]), "\n") + 1
	calendar.mu.Lock()
	locked := true
	defer func() {
		if locked {
			calendar.mu.Unlock()
		}
	}()
	done := make(chan error, 1)
	gid := make(chan string, 1)
	go func() {
		header := make([]byte, 128)
		n := runtime.Stack(header, false)
		fields := strings.Fields(string(header[:n]))
		if len(fields) < 2 {
			gid <- ""
			return
		}
		gid <- fields[1]
		_, err := f.intake.MatchOwnedAnswer(input)
		done <- err
	}()
	child := <-gid
	if child == "" {
		t.Fatal("exact answer child GID missing")
	}
	waiting := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stacks := make([]byte, 1<<20)
		n := runtime.Stack(stacks, true)
		if n == len(stacks) {
			t.Fatal("bounded complete answer stacks required")
		}
		for _, stack := range strings.Split(string(stacks[:n]), "\n\n") {
			if strings.HasPrefix(stack, "goroutine "+child+" [") && strings.Contains(stack, "findnode.DormantIntake.MatchOwnedAnswer") && strings.Contains(stack, "/owned_bridge_forward_answer.go:"+strconv.Itoa(line)+" ") && (strings.Contains(stack, "sync.(*Mutex).Lock") || strings.Contains(stack, "sync.(*Mutex).lockSlow")) {
				waiting = true
				break
			}
		}
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waiting {
		t.Fatal("exact answer child did not wait on original calendar")
	}
	f.clock.Store(int64(entry.expires))
	calendar.mu.Unlock()
	locked = false
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnmatchedAnswer) {
			t.Fatal("exact original expiry admitted delayed answer", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("original answer calendar waiter did not join")
	}
	if calendar.projected != debt || f.intake.dormantOwner.coordinator.snapshot().pending != 1 {
		t.Fatal("fresh equality refusal fabricated cleanup under R or refunded debt")
	}
	if _, err := f.intake.MatchOwnedAnswer(input); !errors.Is(err, ErrStaleWork) {
		t.Fatal("refused original answer receipt replayed", err)
	}
}
func TestOwnedAnswerWriterExactExpiryAndOriginalEnd(t *testing.T) {
	for _, kind := range []string{"expiry", "End"} {
		t.Run(kind, func(t *testing.T) {
			f, _, permit, answer := ownedRelayAnswerFixture(t)
			if err := permit.Finish(true); err != nil {
				t.Fatal(err)
			}
			result, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
			if err != nil {
				t.Fatal(err)
			}
			queued, err := result.QueueAnswerForOriginal(f.members[0])
			if err != nil {
				t.Fatal(err)
			}
			if kind == "expiry" {
				f.clock.Store(int64(result.answer.expires))
			} else {
				publication := f.domain.Publisher().BeginPublication()
				f.members[0].End()
				publication.Complete()
				if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := queued.Take(); err == nil {
				t.Fatal("expired/ended original answer gained writer start")
			}
			if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) {
				t.Fatal("refused answer work replayed", err)
			}
		})
	}
}
func TestOwnedAnswerWriterEndAfterStartedStillFinishesOriginalOnce(t *testing.T) {
	f, _, forwardPermit, answer := ownedRelayAnswerFixture(t)
	if err := forwardPermit.Finish(true); err != nil {
		t.Fatal(err)
	}
	work, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
	if err != nil {
		t.Fatal(err)
	}
	queued, err := work.QueueAnswerForOriginal(f.members[0])
	if err != nil {
		t.Fatal(err)
	}
	publication := f.domain.Publisher().BeginPublication()
	publication.Complete() // unrelated epoch must not revoke queued answer
	_, permit, err := queued.Take()
	if err != nil {
		t.Fatal(err)
	}
	publication = f.domain.Publisher().BeginPublication()
	f.members[0].End()
	publication.Complete()
	if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
		t.Fatal(err)
	}
	f.stop()
	if err := permit.Finish(true); err != nil {
		t.Fatal("End/done stranded original admitted answer", err)
	}
	if err := permit.Finish(false); !errors.Is(err, ErrStaleWork) {
		t.Fatal("answer Finish replayed", err)
	}
}

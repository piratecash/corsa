package netcoretest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// backend_teardown_test.go pins the one property the Backend's blocking sends
// used to lack: a teardown can always run, and it takes the parked senders with
// it.
//
// The previous implementation held b.mu.RLock across the channel send, so a
// sender waiting for room on a saturated channel blocked every writer of that
// mutex — and Shutdown, Unregister and Close, the only three calls that could
// have released it, all take it with write intent. The result was not a flaky
// test, it was a hard deadlock in whichever test happened to saturate a queue,
// and the doc-comment outcome "ErrSendChanClosed if the Backend is shut down
// during send" could not occur.
//
// These tests live in the package rather than beside the rest in
// netcoretest_test because they drive offerBarrier, and the barrier is what
// makes them a statement about the fence instead of about the scheduler: it
// stops the sender between the registry lookup and the offer, which is exactly
// where the old implementation held the read lock. Without it the test would
// have to guess when the sender got there, and would pass on the deadlocking
// implementation every time Shutdown won that guess.

// parkedSender is a Backend with one registered connection whose channels are
// both saturated, and one sender stopped between the lookup and the offer.
type parkedSender struct {
	backend *Backend
	id      domain.ConnID
	entered chan struct{}
	proceed chan struct{}
	armed   sync.Once
	freed   sync.Once
}

// newParkedSender saturates both channels of a fresh connection so that any
// blocking offer that follows provably parks, then arms the barrier for the
// next sender through.
func newParkedSender(t *testing.T, id domain.ConnID) *parkedSender {
	t.Helper()
	p := &parkedSender{
		backend: NewWithOptions(Options{OutboundBuffer: 1}),
		id:      id,
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	p.backend.Register(id, netcore.Outbound, "203.0.113.7:7007")

	// One frame each way fills both channels: nothing drains them, so every
	// later offer has to wait for a reader that will never come.
	if err := p.backend.SendFrame(context.Background(), id, []byte("filler")); err != nil {
		t.Fatalf("saturating SendFrame: %v", err)
	}
	if err := p.backend.Inject(context.Background(), id, []byte("filler")); err != nil {
		t.Fatalf("saturating Inject: %v", err)
	}

	p.backend.lookupBarrier = func() {
		p.armed.Do(func() {
			close(p.entered)
			<-p.proceed
		})
	}
	// A failed assertion before release would otherwise leave the sender on
	// p.proceed for the rest of the package run.
	t.Cleanup(p.release)
	return p
}

// park runs send in its own goroutine and returns once it is stopped between
// the registry lookup and the channel offer.
func (p *parkedSender) park(t *testing.T, send func() error) <-chan error {
	t.Helper()
	got := make(chan error, 1)
	go func() { got <- send() }()
	select {
	case <-p.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sender never reached the offer barrier")
	}
	return got
}

// release lets the parked sender perform its offer.
func (p *parkedSender) release() { p.freed.Do(func() { close(p.proceed) }) }

// TestTeardownReleasesASenderParkedOnAFullChannel is the deadlock test. Every
// teardown entry point is covered against every blocking send, because the
// defect was in the lock discipline they share and not in any one of them.
func TestTeardownReleasesASenderParkedOnAFullChannel(t *testing.T) {
	t.Parallel()

	sends := []struct {
		name string
		call func(*parkedSender) func() error
	}{
		{"SendFrameSync", func(p *parkedSender) func() error {
			return func() error {
				return p.backend.SendFrameSync(context.Background(), p.id, []byte("parked"))
			}
		}},
		{"Inject", func(p *parkedSender) func() error {
			return func() error {
				return p.backend.Inject(context.Background(), p.id, []byte("parked"))
			}
		}},
	}
	teardowns := []struct {
		name string
		call func(*parkedSender)
	}{
		{"Shutdown", func(p *parkedSender) { p.backend.Shutdown() }},
		{"Unregister", func(p *parkedSender) { p.backend.Unregister(p.id) }},
		{"Close", func(p *parkedSender) { _ = p.backend.Close(context.Background(), p.id) }},
	}

	for _, send := range sends {
		for _, teardown := range teardowns {
			t.Run(send.name+"/"+teardown.name, func(t *testing.T) {
				t.Parallel()
				p := newParkedSender(t, domain.ConnID(1))
				got := p.park(t, send.call(p))

				// The teardown starts while the sender is provably inside the
				// send path, and finishes only once the sender has left the
				// offer — which is why it runs on its own goroutine and the
				// sender is released right after.
				done := make(chan struct{})
				go func() {
					defer close(done)
					teardown.call(p)
				}()
				p.release()

				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatalf("%s never returned while a sender was parked on a full channel: "+
						"the sender is holding the registry lock across its offer, and %s is the "+
						"only thing that could have released it", teardown.name, teardown.name)
				}
				select {
				case err := <-got:
					if !errors.Is(err, netcore.ErrSendChanClosed) {
						t.Fatalf("%s parked across %s = %v, want ErrSendChanClosed",
							send.name, teardown.name, err)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("%s never returned after %s: a torn-down slot must release "+
						"its parked senders, not leave them waiting for a reader that will "+
						"never come", send.name, teardown.name)
				}
			})
		}
	}
}

// tornDownSender is a Backend with one registered connection whose channels
// are EMPTY, and one sender stopped between the registry lookup and the offer
// until the slot has been torn down under it.
//
// It is the case the saturated-channel fixture above cannot reach. There the
// offer has nowhere to go, so the teardown arm is the ONLY ready case and the
// answer is forced; here the offer would succeed, both cases are ready at
// once, and `select` picks between them at random — which is where a sender is
// told its frame was accepted by a connection that no longer exists.
type tornDownSender struct {
	backend *Backend
	id      domain.ConnID
	slot    *connSlot
	entered chan struct{}
	armed   sync.Once
}

// newTornDownSender registers a connection with room in both channels and arms
// the barrier so the next sender through leaves it only once teardown has
// raised the slot's fence. Waiting on `done` inside the barrier is what makes
// the interleaving the starting state of the offer instead of a race the test
// has to win.
func newTornDownSender(t *testing.T, id domain.ConnID) *tornDownSender {
	t.Helper()
	p := &tornDownSender{
		backend: NewWithOptions(Options{OutboundBuffer: 4}),
		id:      id,
		entered: make(chan struct{}),
	}
	p.backend.Register(id, netcore.Outbound, "203.0.113.9:7009")

	p.backend.mu.RLock()
	p.slot = p.backend.conns[id]
	p.backend.mu.RUnlock()
	if p.slot == nil {
		t.Fatal("Register did not create a slot")
	}

	p.backend.lookupBarrier = func() {
		p.armed.Do(func() {
			close(p.entered)
			<-p.slot.done
		})
	}
	return p
}

// park runs send in its own goroutine and returns once it is inside the send
// path, past the registry lookup and before the offer.
func (p *tornDownSender) park(t *testing.T, send func() error) <-chan error {
	t.Helper()
	got := make(chan error, 1)
	go func() { got <- send() }()
	select {
	case <-p.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sender never reached the offer barrier")
	}
	return got
}

// TestTeardownAnswersASenderWithRoomLeftInTheChannel is the free-channel twin
// of the deadlock test above, and it covers the branch the saturated fixture
// cannot: the offer that WOULD have succeeded.
//
// Every send is included, the non-blocking one especially: it has no teardown
// arm at all, so before the pre-offer check it answered nil — "accepted" — for
// a connection whose registration had already been dropped and whose channels
// were about to be closed.
func TestTeardownAnswersASenderWithRoomLeftInTheChannel(t *testing.T) {
	t.Parallel()

	sends := []struct {
		name string
		call func(*tornDownSender) func() error
	}{
		{"SendFrame", func(p *tornDownSender) func() error {
			return func() error {
				return p.backend.SendFrame(context.Background(), p.id, []byte("late"))
			}
		}},
		{"SendFrameSync", func(p *tornDownSender) func() error {
			return func() error {
				return p.backend.SendFrameSync(context.Background(), p.id, []byte("late"))
			}
		}},
		{"Inject", func(p *tornDownSender) func() error {
			return func() error {
				return p.backend.Inject(context.Background(), p.id, []byte("late"))
			}
		}},
	}
	teardowns := []struct {
		name string
		call func(*tornDownSender)
	}{
		{"Shutdown", func(p *tornDownSender) { p.backend.Shutdown() }},
		{"Unregister", func(p *tornDownSender) { p.backend.Unregister(p.id) }},
		{"Close", func(p *tornDownSender) { _ = p.backend.Close(context.Background(), p.id) }},
	}

	for _, send := range sends {
		for _, teardown := range teardowns {
			t.Run(send.name+"/"+teardown.name, func(t *testing.T) {
				t.Parallel()
				p := newTornDownSender(t, domain.ConnID(1))
				got := p.park(t, send.call(p))

				// The teardown raises the fence, which is what releases the
				// barrier; it then blocks on the sender join until the sender
				// has left the offer.
				done := make(chan struct{})
				go func() {
					defer close(done)
					teardown.call(p)
				}()

				select {
				case err := <-got:
					if !errors.Is(err, netcore.ErrSendChanClosed) {
						t.Fatalf("%s offering into a free channel of a slot torn down by %s = %v, "+
							"want ErrSendChanClosed: the registration was already dropped and both "+
							"channels are about to be closed, so \"accepted\" is an answer about a "+
							"connection that no longer exists", send.name, teardown.name, err)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("%s never returned after %s", send.name, teardown.name)
				}
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatalf("%s never returned while a sender was inside the send path", teardown.name)
				}
			})
		}
	}
}

// lateTornDownSender is a Backend with one registered connection whose channels
// are EMPTY, and one sender stopped between the teardown PRE-CHECK and the
// channel offer.
//
// It is the window the pre-check cannot cover, and the one the fixtures above
// deliberately do not reach: they raise the fence before the sender reads it, so
// the check itself answers. Here the sender has already read the fence down —
// the check is behind it — and the teardown lands while it is on its way to the
// channel. Priority between "the slot is gone" and "there is room in the
// buffer" cannot be expressed by the ORDER of select cases, because `select`
// picks at random among the ready ones, so the honest answer has to come from a
// re-check the offer runs on its way out.
type lateTornDownSender struct {
	backend *Backend
	id      domain.ConnID
	slot    *connSlot
	entered chan struct{}
	armed   sync.Once
}

// newLateTornDownSender arms the barrier that sits AFTER the pre-check: the
// sender leaves it only once teardown has raised the slot's fence, so the
// interleaving is the starting state of the offer rather than a race the test
// has to win.
func newLateTornDownSender(t *testing.T, id domain.ConnID, saturated bool) *lateTornDownSender {
	t.Helper()
	p := &lateTornDownSender{
		backend: NewWithOptions(Options{OutboundBuffer: 1}),
		id:      id,
		entered: make(chan struct{}),
	}
	p.backend.Register(id, netcore.Outbound, "203.0.113.11:7011")
	if saturated {
		// One frame each way fills both channels; nothing drains them.
		if err := p.backend.SendFrame(context.Background(), id, []byte("filler")); err != nil {
			t.Fatalf("saturating SendFrame: %v", err)
		}
		if err := p.backend.Inject(context.Background(), id, []byte("filler")); err != nil {
			t.Fatalf("saturating Inject: %v", err)
		}
	}

	p.backend.mu.RLock()
	p.slot = p.backend.conns[id]
	p.backend.mu.RUnlock()
	if p.slot == nil {
		t.Fatal("Register did not create a slot")
	}

	p.backend.offerBarrier = func() {
		p.armed.Do(func() {
			close(p.entered)
			<-p.slot.done
		})
	}
	return p
}

// park runs send in its own goroutine and returns once it is past the pre-check
// and before the offer.
func (p *lateTornDownSender) park(t *testing.T, send func() error) <-chan error {
	t.Helper()
	got := make(chan error, 1)
	go func() { got <- send() }()
	select {
	case <-p.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sender never reached the post-check barrier")
	}
	return got
}

// TestTeardownAnswersASenderPastThePreCheck is the finding: the pre-offer check
// left a window, and both send forms answered "accepted" inside it.
//
// The non-blocking send had nothing but the check — a frame that found room
// after the fence went up was reported as enqueued. The blocking one had a
// teardown ARM, but an arm is not a priority: with room left in the channel both
// cases are ready and `select` chooses between them at random, so the promised
// ErrSendChanClosed arrived about half the time.
//
// The answer is about the REGISTRATION, not about the bytes. The frame may well
// sit in the buffer — closing a Go channel does not discard what is buffered,
// and removeLocked joins the senders before it closes anything, so a reader
// draining Outbound(id) still receives it. What the sender is told is that the
// connection it addressed is gone, which is the fact it has to act on.
func TestTeardownAnswersASenderPastThePreCheck(t *testing.T) {
	t.Parallel()

	sends := []struct {
		name string
		call func(*lateTornDownSender) func() error
	}{
		{"SendFrame", func(p *lateTornDownSender) func() error {
			return func() error {
				return p.backend.SendFrame(context.Background(), p.id, []byte("late"))
			}
		}},
		{"SendFrameSync", func(p *lateTornDownSender) func() error {
			return func() error {
				return p.backend.SendFrameSync(context.Background(), p.id, []byte("late"))
			}
		}},
		{"Inject", func(p *lateTornDownSender) func() error {
			return func() error {
				return p.backend.Inject(context.Background(), p.id, []byte("late"))
			}
		}},
	}
	teardowns := []struct {
		name string
		call func(*lateTornDownSender)
	}{
		{"Shutdown", func(p *lateTornDownSender) { p.backend.Shutdown() }},
		{"Unregister", func(p *lateTornDownSender) { p.backend.Unregister(p.id) }},
		{"Close", func(p *lateTornDownSender) { _ = p.backend.Close(context.Background(), p.id) }},
	}

	// Both channel states, because they are what the two send forms disagree
	// about: with room left the offer succeeds and the answer must still be the
	// teardown's, with none the non-blocking form would otherwise report a
	// saturated queue for a connection that no longer exists.
	channels := []struct {
		name      string
		saturated bool
	}{
		{"room in the channel", false},
		{"saturated channel", true},
	}

	for _, channel := range channels {
		for _, send := range sends {
			for _, teardown := range teardowns {
				t.Run(channel.name+"/"+send.name+"/"+teardown.name, func(t *testing.T) {
					t.Parallel()
					p := newLateTornDownSender(t, domain.ConnID(1), channel.saturated)
					got := p.park(t, send.call(p))

					done := make(chan struct{})
					go func() {
						defer close(done)
						teardown.call(p)
					}()

					select {
					case err := <-got:
						if !errors.Is(err, netcore.ErrSendChanClosed) {
							t.Fatalf("%s offering after the pre-check, torn down by %s = %v, want "+
								"ErrSendChanClosed: the answer is about the registration, and that "+
								"registration was dropped while the sender was on its way to the channel",
								send.name, teardown.name, err)
						}
					case <-time.After(2 * time.Second):
						t.Fatalf("%s never returned after %s", send.name, teardown.name)
					}
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Fatalf("%s never returned while a sender was inside the send path", teardown.name)
					}
				})
			}
		}
	}
}

// TestParkedSenderIsReleasedByItsOwnCtx is the other half of the escape
// contract: teardown is not the only way out of a saturated channel. Without
// this the previous behaviour — a wait bounded by nothing the caller controls —
// would still be reachable for any test that never tears its Backend down.
func TestParkedSenderIsReleasedByItsOwnCtx(t *testing.T) {
	t.Parallel()
	p := newParkedSender(t, domain.ConnID(2))

	ctx, cancel := context.WithCancel(context.Background())
	got := p.park(t, func() error { return p.backend.Inject(ctx, p.id, []byte("parked")) })
	p.release()
	cancel()

	select {
	case err := <-got:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Inject parked on a full channel and cancelled = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Inject ignored its ctx while parked on a full channel")
	}
}

// TestTeardownStillClosesTheChannelsAfterTheLastSender guards the half of the
// dilemma the read lock was there for. The lock is gone, so the ordering has to
// carry the guarantee instead: the close must come after every sender has left,
// or it lands on a live one as a send on a closed channel.
//
// The assertion is the observable one — the channel is closed once the teardown
// returns, and no sender panicked — because "no send raced the close" has no
// direct expression; the -race twin of this property is
// TestBackend_SendRacingShutdownNeverPanics.
func TestTeardownStillClosesTheChannelsAfterTheLastSender(t *testing.T) {
	t.Parallel()
	p := newParkedSender(t, domain.ConnID(3))
	outbound := p.backend.Outbound(p.id)
	inbound := p.backend.Inbound(p.id)

	got := p.park(t, func() error {
		return p.backend.SendFrameSync(context.Background(), p.id, []byte("parked"))
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.backend.Shutdown()
	}()
	p.release()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown never returned")
	}
	if err := <-got; !errors.Is(err, netcore.ErrSendChanClosed) {
		t.Fatalf("parked SendFrameSync = %v, want ErrSendChanClosed", err)
	}

	for name, ch := range map[string]<-chan []byte{"outbound": outbound, "inbound": inbound} {
		// One buffered filler each, then the closed signal: closing a channel
		// does not discard what is buffered, which is why a frame accepted an
		// instant before teardown is still delivered and the offer does not
		// re-check on its way out.
		if _, ok := <-ch; !ok {
			t.Fatalf("%s channel lost the frame buffered before teardown", name)
		}
		if _, ok := <-ch; ok {
			t.Fatalf("%s channel is still open after Shutdown", name)
		}
	}
}

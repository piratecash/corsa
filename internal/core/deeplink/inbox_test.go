package deeplink_test

import (
	"sync"
	"testing"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// TestInboxHandsOverInOrder: what producers push is what the owning
// goroutine drains, once, in order.
func TestInboxHandsOverInOrder(t *testing.T) {
	var inbox deeplink.Inbox

	inbox.Push("corsa:group/one")
	inbox.Push("corsa:group/two")

	drained := inbox.Drain()
	if len(drained) != 2 || drained[0] != "corsa:group/one" || drained[1] != "corsa:group/two" {
		t.Fatalf("drained %v", drained)
	}
	if again := inbox.Drain(); again != nil {
		t.Errorf("second drain returned %v, want nil", again)
	}
}

// TestInboxDropsAFlood: a queue that grew without bound would be a
// local process's way to make the UI import forever.
func TestInboxDropsAFlood(t *testing.T) {
	var inbox deeplink.Inbox
	for i := 0; i < 100; i++ {
		inbox.Push("corsa:group/flood")
	}
	if drained := inbox.Drain(); len(drained) > 8 {
		t.Fatalf("queued %d links, want the cap to hold", len(drained))
	}
}

// TestInboxIsSafeAcrossGoroutines: producers are OS callback threads,
// the consumer is the UI goroutine. Run with -race.
func TestInboxIsSafeAcrossGoroutines(t *testing.T) {
	var inbox deeplink.Inbox

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				inbox.Drain()
				return
			default:
				inbox.Drain()
			}
		}
	}()
	for i := 0; i < 200; i++ {
		inbox.Push("corsa:group/x")
	}
	close(stop)
	wg.Wait()
}

// TestInboxIgnoresEmptyPushes: a platform that hands over an empty
// string (no intent data) must not queue a frame's worth of nothing.
func TestInboxIgnoresEmptyPushes(t *testing.T) {
	var inbox deeplink.Inbox
	inbox.Push("")
	if drained := inbox.Drain(); drained != nil {
		t.Fatalf("drained %v, want nil", drained)
	}
}

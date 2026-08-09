package netcore

import (
	"sync"
	"testing"
	"time"
)

// queue_ownership_test.go pins WHO owns sendCh. The queue has many producers
// and exactly one consumer, so no producer can ever prove it is the last one —
// which makes "the receiving side closes the channel" a defect, not a style
// choice: the close lands on a live producer as a send on a closed channel.
//
// Both tests below attack the same window from the two angles it can be
// observed from: the deterministic one places a producer inside it, the
// racing one lets the detector find it.

// TestCloseNeverShutsTheQueueUnderAParkedProducer is the deterministic half.
//
// enqueueBarrier stops a producer between its door check and its channel
// offer — the exact interleaving that no arrangement of public calls can
// produce — and Close() runs while it is parked. queueFrame is called
// directly rather than through SendRaw because every public entry point wraps
// the offer in a recover(): the panic would be swallowed and the test would
// see the same status either way.
//
// Closing the queue from its receiving side has TWO outcomes, on opposite
// sides of the channel, and this test has to name whichever one it hits —
// a message about the wrong one sends the next reader after the wrong defect:
//
//   - the PRODUCER panics, because its offer lands on a closed channel. That
//     is the outcome the ownership rule is usually stated for, and it is what
//     the parked producer below observes;
//   - the CONSUMER never exits, because a receive from a closed channel is
//     always ready: drainQueued's `default` branch becomes unreachable, the
//     sweep spins for ever and Close() blocks on writerExited. The producer
//     never even gets to panic, so an assertion that only watched it would
//     report a timeout with no defect named.
//
// Close() therefore runs on its own goroutine and is waited for explicitly:
// the wait is what turns the second outcome into a sentence instead of a
// package-level test timeout.
func TestCloseNeverShutsTheQueueUnderAParkedProducer(t *testing.T) {
	t.Parallel()
	conn := newAutoConn()
	pc := New(21, conn, Inbound, Options{})

	entered := make(chan struct{})
	proceed := make(chan struct{})
	var armed sync.Once
	pc.enqueueBarrier = func() {
		armed.Do(func() {
			close(entered)
			<-proceed
		})
	}

	type outcome struct {
		status   SendStatus
		panicked bool
	}
	done := make(chan outcome, 1)
	go func() {
		res := outcome{status: SendStatusInvalid}
		defer func() {
			if r := recover(); r != nil {
				res.panicked = true
			}
			done <- res
		}()
		res.status = pc.queueFrame(sendItem{data: []byte("parked\n")})
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("producer never reached the enqueue barrier")
	}

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		pc.Close()
	}()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() never returned: the queue was closed by its own receiver, " +
			"so every receive on sendCh is ready, drainQueued never reaches its " +
			"default branch and the writer goroutine cannot exit — Close() waits " +
			"on writerExited for ever")
	}
	close(proceed)

	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("producer never returned after Close")
	}

	if got.panicked {
		t.Fatal("Close() closed sendCh under a producer that had already passed " +
			"the door: the offer landed on a closed channel and panicked instead " +
			"of being answered a status")
	}
	if got.status != SendChanClosed {
		t.Fatalf("producer parked across Close() answered %v, want SendChanClosed", got.status)
	}
	if conn.attemptCount() != 0 {
		t.Fatalf("socket write attempts = %d, want 0 — nothing may reach the wire after Close", conn.attemptCount())
	}
}

// TestConcurrentSendAndCloseNeverRaceOnTheQueue is the racing half: the very
// interleaving the node layer produces on every teardown, where a background
// goroutine still holds the connection while the shutdown path closes it.
// Without the detector this test is nearly silent — the panic is recovered
// and reported as an ordinary status — so it is written to be run under
// -race, where the close/send pair on one channel is a reported race.
func TestConcurrentSendAndCloseNeverRaceOnTheQueue(t *testing.T) {
	t.Parallel()
	const (
		rounds            = 40
		producers         = 4
		framesPerProducer = 64
	)

	for round := range rounds {
		conn := newAutoConn()
		pc := New(ConnID(1000+round), conn, Inbound, Options{})

		warm := make(chan struct{})
		var warmed sync.Once
		var wg sync.WaitGroup
		for range producers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range framesPerProducer {
					pc.SendRaw([]byte("frame\n"))
					warmed.Do(func() { close(warm) })
				}
			}()
		}

		// Overlap is the point: Close() must start while producers are still
		// offering, not after they have all finished.
		<-warm
		pc.Close()
		wg.Wait()
	}
}

package node

import (
	"sync"
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestLocalChangeCancelNeverClosesUnderAPublisher pins the fence around
// s.events. The cancel returned by SubscribeLocalChanges closes the
// subscriber's channel, so the publisher may not hold a reference to that
// channel outside gossipMu: a publisher that snapshotted the set and offered
// after the unlock sends into a channel the cancel has already closed, which
// panics in whatever goroutine happened to emit the event.
//
// Inboxes are deliberately left undrained — a full inbox takes the default
// branch and would hide the window the test is aiming at.
func TestLocalChangeCancelNeverClosesUnderAPublisher(t *testing.T) {
	s := &Service{}
	s.initMaps()

	// Sized to reproduce the window without flooding the package run: every
	// subscribe/cancel pair emits six trace lines.
	const (
		resident   = 64
		publishers = 4
		churners   = 4
		iterations = 500
	)

	// Resident subscribers stretch each emit over a long walk, so a cancel
	// racing it has a wide target instead of a single channel.
	for range resident {
		_, cancel := s.SubscribeLocalChanges()
		defer cancel()
	}

	event := protocol.LocalChangeEvent{Type: protocol.LocalChangeNewMessage, MessageID: "fence"}

	var wg sync.WaitGroup
	for range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				s.emitLocalChange(event)
			}
		}()
	}

	for range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				_, cancel := s.SubscribeLocalChanges()
				cancel()
			}
		}()
	}

	wg.Wait()
}

package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

// checkpointProbe swaps the store the checkpointer sees, so a test can
// make the truncation fail and then let it recover.
type checkpointProbe struct {
	mu      sync.Mutex
	store   *chatlog.Store
	calls   int
	reached chan struct{}
}

func (p *checkpointProbe) current() *chatlog.Store {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	store := p.store
	select {
	case p.reached <- struct{}{}:
	default:
	}
	return store
}

func (p *checkpointProbe) heal(store *chatlog.Store) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = store
}

func (p *checkpointProbe) attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// TestCheckpointRetriesAfterAFailedTruncation: a BUSY database is the
// normal reason wal_checkpoint(TRUNCATE) fails, and it is transient. Doing
// nothing leaves the pages of the deleted message in the -wal file until
// some unrelated deletion happens to ask again — which is not a guarantee,
// and the deletion promised one.
func TestCheckpointRetriesAfterAFailedTruncation(t *testing.T) {
	t.Parallel()

	owner := domain.PeerIdentityFromWire("1111111111111111111111111111111111111111")
	probe := &checkpointProbe{
		store:   newClosedChatlogStore(t, owner),
		reached: make(chan struct{}, 8),
	}
	checkpointer := &deleteCheckpointer{
		store:    probe.current,
		ctx:      context.Background,
		delay:    time.Millisecond,
		retryCap: 5 * time.Millisecond,
	}
	t.Cleanup(checkpointer.stop)

	checkpointer.request()

	// The first attempt fails against the closed database; the retry is
	// armed by the run itself, with nobody left to ask for it.
	waitForCheckpointAttempts(t, probe, 2)

	probe.heal(newTestChatlogStore(t, owner))
	before := probe.attempts()
	waitForCheckpointAttempts(t, probe, before+1)

	// Once it succeeds the retries stop.
	settled := probe.attempts()
	time.Sleep(40 * time.Millisecond)
	if got := probe.attempts(); got != settled {
		t.Errorf("the checkpointer kept running after a successful truncation: %d → %d", settled, got)
	}
}

// TestStoppedCheckpointerDoesNotRetry: shutdown must not leave a timer
// behind — the database closes right after, and the checkpoint writes.
func TestStoppedCheckpointerDoesNotRetry(t *testing.T) {
	t.Parallel()

	owner := domain.PeerIdentityFromWire("1111111111111111111111111111111111111111")
	probe := &checkpointProbe{
		store:   newClosedChatlogStore(t, owner),
		reached: make(chan struct{}, 8),
	}
	checkpointer := &deleteCheckpointer{
		store:    probe.current,
		ctx:      context.Background,
		delay:    time.Millisecond,
		retryCap: 5 * time.Millisecond,
	}

	checkpointer.request()
	waitForCheckpointAttempts(t, probe, 1)
	checkpointer.stop()

	settled := probe.attempts()
	time.Sleep(40 * time.Millisecond)
	if got := probe.attempts(); got != settled {
		t.Errorf("a stopped checkpointer kept retrying: %d → %d", settled, got)
	}
}

func waitForCheckpointAttempts(t *testing.T, probe *checkpointProbe, want int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for probe.attempts() < want {
		select {
		case <-probe.reached:
		case <-deadline:
			t.Fatalf("checkpoint attempts = %d, want %d", probe.attempts(), want)
		}
	}
}

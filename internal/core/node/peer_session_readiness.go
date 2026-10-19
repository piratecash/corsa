package node

import (
	"context"
	"net"
	"sync"
	"time"
)

// peerSessionReadiness is a session-local ordering barrier, not a frame queue.
// Its pointer, context and wait limit are immutable before the reader starts;
// state, failure and changed belong to mu, an independent leaf mutex.
type peerSessionReadiness struct {
	ctx       context.Context
	changed   chan struct{}
	mu        sync.Mutex
	state     peerSessionReadyState
	failure   error
	waitLimit time.Duration
}

type peerSessionReadyState uint8

const (
	peerSessionReadPending peerSessionReadyState = iota + 1
	peerSessionReadReady
	peerSessionReadAborted
	peerSessionReadClosed
)

func newPeerSessionReadiness(ctx context.Context) *peerSessionReadiness {
	return &peerSessionReadiness{
		ctx: ctx, changed: make(chan struct{}), state: peerSessionReadPending,
		waitLimit: peerRequestTimeout,
	}
}

// prepareReadyLocked requires b.mu. An abort that won before authentication
// cannot be resurrected by a late waiter. Ready is irreversible by dial cancel.
func (b *peerSessionReadiness) prepareReadyLocked() error {
	if b.state == peerSessionReadReady {
		return nil
	}
	if b.state != peerSessionReadPending {
		return b.failure
	}
	if b.ctx.Err() != nil {
		return b.abortLocked(context.Cause(b.ctx))
	}
	return nil
}

// publishReadyLocked requires b.mu, with the caller's peerMu already held.
// The caller publishes authOK before opening the reader's next-line gate.
func (b *peerSessionReadiness) publishReadyLocked() {
	if b.state == peerSessionReadPending {
		b.state = peerSessionReadReady
		close(b.changed)
	}
}

// abortLocked requires b.mu. A committed ready wins later dial cancellation
// or a concurrent wait timeout; only session Close can end it afterwards.
func (b *peerSessionReadiness) abortLocked(cause error) error {
	if b.state == peerSessionReadReady {
		return nil
	}
	if b.state == peerSessionReadPending {
		b.state, b.failure = peerSessionReadAborted, cause
		close(b.changed)
	}
	return b.failure
}

func (b *peerSessionReadiness) abort(cause error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.abortLocked(cause)
}

func (b *peerSessionReadiness) stop() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == peerSessionReadPending {
		close(b.changed)
	}
	b.state, b.failure = peerSessionReadClosed, net.ErrClosed
}

func (b *peerSessionReadiness) wait() error {
	b.mu.Lock()
	if b.state != peerSessionReadPending {
		failure := b.failure
		b.mu.Unlock()
		return failure
	}
	b.mu.Unlock()

	timer := time.NewTimer(b.waitLimit)
	defer timer.Stop()
	var cause error
	select {
	case <-b.changed:
	case <-b.ctx.Done():
		cause = context.Cause(b.ctx)
	case <-timer.C:
		cause = context.DeadlineExceeded
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if cause != nil {
		return b.abortLocked(cause)
	}
	return b.failure
}

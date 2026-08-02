package desktop

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	corsanode "github.com/piratecash/corsa/internal/core/node"
)

type NodeRuntime struct {
	service *corsanode.Service

	mu      sync.RWMutex
	running bool
	err     error
	done    chan struct{} // closed when the Start goroutine's Service.Run returns
}

func NewNodeRuntime(service *corsanode.Service) *NodeRuntime {
	return &NodeRuntime{service: service}
}

func (r *NodeRuntime) Start(ctx context.Context) {
	done := make(chan struct{})

	r.mu.Lock()
	r.running = true
	r.err = nil
	r.done = done
	r.mu.Unlock()

	r.service.PrimeBootstrapPeers()

	go func() {
		defer close(done)
		err := r.service.Run(ctx)

		r.mu.Lock()
		defer r.mu.Unlock()

		r.running = false
		if err != nil && !errors.Is(err, context.Canceled) {
			r.err = err
		}
	}()
}

// Wait blocks until the node has fully stopped or the timeout elapses,
// reporting whether it actually finished. "Fully" is two distinct
// phases, both of which write to the chatlog and must therefore precede
// client.Close() on the shutdown path:
//
//  1. the Service.Run goroutine started by Start (network handlers
//     persisting received messages and receipts);
//  2. the service's fire-and-forget background pool (goBackground /
//     backgroundWg) — durable writes such as MarkSeenConfirmed land in
//     sqlite from there, detached from Run's own lifetime.
//
// Closing sqlite under either loses or errors those writes mid-flight.
// Returns true immediately when Start was never called.
func (r *NodeRuntime) Wait(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	r.mu.RLock()
	done := r.done
	r.mu.RUnlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			return false
		}
	}

	bg := make(chan struct{})
	go func() {
		r.service.WaitBackground()
		close(bg)
	}()
	select {
	case <-bg:
		return true
	case <-time.After(time.Until(deadline)):
		return false
	}
}

func (r *NodeRuntime) ListenAddress() string {
	return r.service.ListenAddress()
}

// ProtocolVersion returns the wire protocol version compiled into this
// build. Surfaced for UI fallback paths (e.g. the console info tab) so they
// can render a sensible value before the first probe response populates
// service.NodeStatus.ProtocolVersion. Constant for the lifetime of the
// process — the value is set at compile time via config.ProtocolVersion.
func (r *NodeRuntime) ProtocolVersion() int {
	return config.ProtocolVersion
}

func (r *NodeRuntime) Address() string {
	return r.service.Address()
}

func (r *NodeRuntime) Running() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.running
}

func (r *NodeRuntime) Error() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.err == nil {
		return ""
	}
	return r.err.Error()
}

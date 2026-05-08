package desktop

import (
	"context"
	"errors"
	"sync"

	"github.com/piratecash/corsa/internal/core/config"
	corsanode "github.com/piratecash/corsa/internal/core/node"
)

type NodeRuntime struct {
	service *corsanode.Service

	mu      sync.RWMutex
	running bool
	err     error
}

func NewNodeRuntime(service *corsanode.Service) *NodeRuntime {
	return &NodeRuntime{service: service}
}

func (r *NodeRuntime) Start(ctx context.Context) {
	r.mu.Lock()
	r.running = true
	r.err = nil
	r.mu.Unlock()

	r.service.PrimeBootstrapPeers()

	go func() {
		err := r.service.Run(ctx)

		r.mu.Lock()
		defer r.mu.Unlock()

		r.running = false
		if err != nil && !errors.Is(err, context.Canceled) {
			r.err = err
		}
	}()
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
	return string(r.service.Address())
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

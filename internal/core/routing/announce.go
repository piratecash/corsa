package routing

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// DefaultAnnounceInterval is the periodic announcement interval.
	// Every 30 seconds, the node sends its routing table to all peers
	// that support mesh_routing_v1.
	DefaultAnnounceInterval = 30 * time.Second
)

// PeerSender abstracts the ability to send announce_routes frames to a
// specific peer. node.Service implements this interface to decouple the
// routing package from the network layer.
type PeerSender interface {
	// SendAnnounceRoutes sends a list of AnnounceEntry items as an
	// announce_routes frame to the peer identified by peerAddress.
	// peerIdentity is the Ed25519 fingerprint of the peer — used by
	// the routing table for split horizon (exclude routes learned from
	// this peer). peerAddress is the transport address used by
	// node.Service to locate the session.
	//
	// Returns true if the frame was enqueued successfully.
	SendAnnounceRoutes(peerAddress PeerAddress, routes []AnnounceEntry) bool
}

// AnnounceLoop runs periodic and triggered routing announcements. It
// owns a background goroutine that wakes every DefaultAnnounceInterval
// and sends the local routing table to all capable peers. Triggered
// updates (connect, disconnect) bypass the timer and send immediately.
//
// The loop is designed to be started once from node.Service.Run and
// stopped on context cancellation.
type AnnounceLoop struct {
	table    *Table
	sender   PeerSender
	interval time.Duration

	// triggerCh receives signals to send an immediate update.
	// Buffered to 1 so multiple rapid triggers coalesce.
	triggerCh chan struct{}

	// peersFn returns the current list of peers that support
	// mesh_routing_v1. Each entry is (transport_address, identity).
	peersFn func() []AnnounceTarget

	mu      sync.Mutex
	running bool
}

// AnnounceTarget identifies a peer for announcement purposes.
type AnnounceTarget struct {
	// Address is the transport address used to enqueue frames.
	Address PeerAddress
	// Identity is the peer's Ed25519 fingerprint — used for split horizon.
	Identity PeerIdentity
}

// AnnounceLoopOption configures the AnnounceLoop.
type AnnounceLoopOption func(*AnnounceLoop)

// WithAnnounceInterval overrides the default periodic interval.
func WithAnnounceInterval(d time.Duration) AnnounceLoopOption {
	return func(a *AnnounceLoop) {
		a.interval = d
	}
}

// NewAnnounceLoop creates a new loop. peersFn is called on every tick to
// discover which peers should receive announcements.
func NewAnnounceLoop(
	table *Table,
	sender PeerSender,
	peersFn func() []AnnounceTarget,
	opts ...AnnounceLoopOption,
) *AnnounceLoop {
	a := &AnnounceLoop{
		table:     table,
		sender:    sender,
		interval:  DefaultAnnounceInterval,
		triggerCh: make(chan struct{}, 1),
		peersFn:   peersFn,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Run starts the periodic announce loop. It blocks until ctx is cancelled.
func (a *AnnounceLoop) Run(ctx context.Context) {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return
	}
	a.running = true
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.running = false
		a.mu.Unlock()
	}()

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.announceToAllPeers()
		case <-a.triggerCh:
			a.announceToAllPeers()
			// Reset the ticker so we don't double-announce shortly after
			// a triggered update.
			ticker.Reset(a.interval)
		}
	}
}

// TriggerUpdate requests an immediate announcement cycle. Safe to call
// from any goroutine. Multiple rapid calls coalesce into a single cycle.
func (a *AnnounceLoop) TriggerUpdate() {
	select {
	case a.triggerCh <- struct{}{}:
	default:
		// Already pending — coalesce.
	}
}

// PendingTrigger reports whether a triggered update is queued but not yet
// consumed by the announce loop. Intended for unit tests that verify
// TriggerUpdate was called without running the full loop.
func (a *AnnounceLoop) PendingTrigger() bool {
	select {
	case <-a.triggerCh:
		// Was pending — put it back so the loop still sees it.
		a.triggerCh <- struct{}{}
		return true
	default:
		return false
	}
}

// announceToAllPeers sends the routing table to every capable peer,
// applying split horizon per peer.
func (a *AnnounceLoop) announceToAllPeers() {
	peers := a.peersFn()
	if len(peers) == 0 {
		return
	}

	for _, peer := range peers {
		routes := a.table.AnnounceTo(peer.Identity)
		if len(routes) == 0 {
			continue
		}
		if !a.sender.SendAnnounceRoutes(peer.Address, routes) {
			log.Debug().
				Str("peer", string(peer.Address)).
				Int("routes", len(routes)).
				Msg("announce_routes_send_failed")
		}
	}

	log.Debug().
		Int("peers", len(peers)).
		Msg("announce_cycle_complete")
}

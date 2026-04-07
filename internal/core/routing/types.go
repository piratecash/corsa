package routing

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// PeerIdentity and PeerAddress are re-exported from domain so that
// callers importing routing do not need a separate domain import.
type (
	PeerIdentity = domain.PeerIdentity
	PeerAddress  = domain.PeerAddress
)

var (
	ErrEmptyIdentity       = errors.New("routing: Identity must not be empty")
	ErrEmptyOrigin         = errors.New("routing: Origin must not be empty")
	ErrEmptyNextHop        = errors.New("routing: NextHop must not be empty")
	ErrInvalidHops         = errors.New("routing: Hops must be between 1 and HopsInfinity (16)")
	ErrDirectHopsMust1     = errors.New("routing: direct route must have Hops=1")
	ErrDirectNextHop       = errors.New("routing: direct route NextHop must equal Identity (directly connected)")
	ErrDirectForeignOrigin = errors.New("routing: direct route Origin must equal localOrigin")
	ErrNoLocalOrigin       = errors.New("routing: Table.localOrigin not set (use WithLocalOrigin)")
	ErrEmptyPeerID         = errors.New("routing: peerIdentity must not be empty")
	ErrLocalSourceReserved = errors.New("routing: RouteSourceLocal is synthetic and cannot be persisted via UpdateRoute")
)

// Flap detection defaults. A peer that disconnects and reconnects more than
// FlapThreshold times within FlapWindow is considered unstable. Once the
// threshold is crossed, the peer enters hold-down: reconnections during
// HoldDownDuration still create routes, but with PenalizedTTL instead of
// the default — giving the network time to converge before the route
// expires again if the link flaps once more.
const (
	DefaultFlapWindow       = 120 * time.Second
	DefaultFlapThreshold    = 3
	DefaultHoldDownDuration = 30 * time.Second
	DefaultPenalizedTTL     = 30 * time.Second
)

// HopsInfinity marks a route as withdrawn. Only the origin node may
// set hops to this value on the wire; transit nodes invalidate locally
// and stop advertising the route instead.
const HopsInfinity = 16

// peerFlapState tracks disconnect events for a single peer identity.
// Used by the Table to detect link flapping and apply hold-down.
type peerFlapState struct {
	// withdrawTimes records timestamps of recent RemoveDirectPeer calls.
	// Only events within the flap window are retained.
	withdrawTimes []time.Time

	// holdDownUntil is the time until which AddDirectPeer will apply
	// a penalized (shorter) TTL. Zero means no hold-down active.
	holdDownUntil time.Time
}

// RouteSource indicates how a route was learned. The trust hierarchy is:
// direct > hop_ack > announcement. A route learned through a more trusted
// source is preferred over one with the same (identity, origin, nextHop)
// triple learned through a less trusted source.
type RouteSource uint8

const (
	RouteSourceAnnouncement RouteSource = iota // learned via announce_routes frame
	RouteSourceHopAck                          // confirmed by relay_hop_ack
	RouteSourceDirect                          // directly connected peer
	RouteSourceLocal                           // synthetic: the node itself (hops=0, never expires)
)

// String returns a human-readable representation for logging and debugging.
func (s RouteSource) String() string {
	switch s {
	case RouteSourceLocal:
		return "local"
	case RouteSourceDirect:
		return "direct"
	case RouteSourceHopAck:
		return "hop_ack"
	case RouteSourceAnnouncement:
		return "announcement"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// TrustRank returns a numeric rank for comparison. Higher rank means
// more trusted. This avoids relying on iota ordering.
func (s RouteSource) TrustRank() int {
	switch s {
	case RouteSourceLocal:
		return 3
	case RouteSourceDirect:
		return 2
	case RouteSourceHopAck:
		return 1
	case RouteSourceAnnouncement:
		return 0
	default:
		return -1
	}
}

// RouteEntry represents a single route in the distance-vector table.
//
// Dedup key: (Identity, Origin, NextHop) — this triple uniquely identifies
// a route lineage. Two entries with the same triple are the same route at
// different points in time; the one with the higher SeqNo wins.
type RouteEntry struct {
	// Identity is the Ed25519 fingerprint of the destination node.
	Identity PeerIdentity

	// Origin is the peer identity that originally advertised this route.
	// Only the origin may advance SeqNo or send a withdrawal (hops=16)
	// on the wire.
	Origin PeerIdentity

	// NextHop is the peer identity from which we learned this route.
	// This is a peer identity, not a transport address — a single identity
	// may have multiple concurrent sessions.
	NextHop PeerIdentity

	// Hops is the distance to the destination. 1 means directly connected,
	// HopsInfinity (16) means withdrawn.
	Hops int

	// SeqNo is a monotonically increasing sequence number scoped to the
	// Origin. Only the origin may advance it. Comparison is valid only
	// between entries sharing the same (Identity, Origin) pair.
	SeqNo uint64

	// Source indicates how this route was learned. Used for trust-based
	// tie-breaking within the same (Identity, Origin, NextHop) triple.
	Source RouteSource

	// ExpiresAt is the absolute time when this entry expires. Derived from
	// RemainingTTL at insertion time. After expiry the route is treated as
	// withdrawn.
	ExpiresAt time.Time

	// Extra holds opaque JSON fields from the wire that this node does not
	// understand. Preserved across table updates so that re-announced routes
	// carry forward-compatible extensions unchanged.
	Extra json.RawMessage
}

// Validate checks structural invariants of the entry. Returns an error
// if the entry is malformed and must not be inserted into the table.
//
// Source-specific rules:
//   - RouteSourceDirect: Hops must be 1, NextHop must equal Identity
//     (a direct route means the destination is our immediate neighbor).
func (e RouteEntry) Validate() error {
	if string(e.Identity) == "" {
		return ErrEmptyIdentity
	}
	if string(e.Origin) == "" {
		return ErrEmptyOrigin
	}
	if string(e.NextHop) == "" {
		return ErrEmptyNextHop
	}
	if e.Source == RouteSourceLocal {
		if e.Hops != 0 {
			return errors.New("routing: local route must have Hops=0")
		}
	} else if e.Hops < 1 || e.Hops > HopsInfinity {
		return ErrInvalidHops
	}
	if e.Source == RouteSourceDirect {
		if e.Hops != 1 {
			return ErrDirectHopsMust1
		}
		if e.NextHop != e.Identity {
			return ErrDirectNextHop
		}
	}
	return nil
}

// IsWithdrawn returns true if the route has been explicitly withdrawn
// (hops == HopsInfinity) or has expired.
func (e RouteEntry) IsWithdrawn() bool {
	return e.Hops >= HopsInfinity
}

// IsExpired returns true if the route TTL has elapsed. The check is
// inclusive: a route whose ExpiresAt equals now is considered expired.
// This keeps ttl_seconds=0 and expired=true consistent in RPC output.
func (e RouteEntry) IsExpired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt)
}

// DedupKey returns the triple that uniquely identifies a route lineage.
func (e RouteEntry) DedupKey() RouteTriple {
	return RouteTriple{
		Identity: e.Identity,
		Origin:   e.Origin,
		NextHop:  e.NextHop,
	}
}

// RouteTriple is the deduplication key for route entries.
type RouteTriple struct {
	Identity PeerIdentity
	Origin   PeerIdentity
	NextHop  PeerIdentity
}

// Snapshot is an immutable point-in-time view of the routing table.
// Safe to read concurrently without locks. All fields are captured
// under a single lock acquisition, so they represent a consistent state.
type Snapshot struct {
	// Routes maps destination identity to all known routes for that identity.
	Routes map[PeerIdentity][]RouteEntry

	// TakenAt is the timestamp when the snapshot was captured.
	TakenAt time.Time

	// TotalEntries is the count of all route entries (including withdrawn/expired).
	TotalEntries int

	// ActiveEntries is the count of non-withdrawn, non-expired entries.
	ActiveEntries int

	// FlapState contains the flap detection state captured at the same instant
	// as the routes, avoiding inconsistency between separate reads.
	FlapState []FlapEntry
}

// BestRoute returns the best (lowest hop count, highest trust) non-withdrawn
// route for the given identity, or nil if none exists.
func (s Snapshot) BestRoute(identity PeerIdentity) *RouteEntry {
	routes, ok := s.Routes[identity]
	if !ok {
		return nil
	}
	var best *RouteEntry
	for i := range routes {
		r := &routes[i]
		if r.IsWithdrawn() || r.IsExpired(s.TakenAt) {
			continue
		}
		if best == nil || isBetter(r, best) {
			best = r
		}
	}
	return best
}

// AnnounceEntry is the wire-safe projection of a RouteEntry.
// It contains only the fields transmitted in announce_routes frames.
// Produced by RouteEntry.ToAnnounceEntry or Table.AnnounceTo.
//
// Extra carries opaque JSON fields from the wire that this node does not
// understand. When re-announcing a learned route, Extra is forwarded
// unchanged — enabling forward-compatible relay of future protocol
// extensions (e.g. onion box keys) through older nodes.
type AnnounceEntry struct {
	Identity PeerIdentity
	Origin   PeerIdentity
	Hops     int
	SeqNo    uint64

	// Extra holds unknown wire fields for forward-compatible relay.
	// Nil for locally originated routes.
	Extra json.RawMessage
}

// ToAnnounceEntry projects a RouteEntry into the wire format for
// announce_routes frames. The wire carries the sender's local hop count
// as-is — the receiver adds +1 when inserting into its own table
// (Phase 1.2 receive path). This matches the roadmap convention where
// A sends {X, hops=1} for a direct peer and B stores it as hops=2.
//
// Fields not transmitted on the wire (NextHop, Source, ExpiresAt)
// are stripped — the receiver derives them locally.
func (e RouteEntry) ToAnnounceEntry() AnnounceEntry {
	return AnnounceEntry{
		Identity: e.Identity,
		Origin:   e.Origin,
		Hops:     e.Hops,
		SeqNo:    e.SeqNo,
		Extra:    e.Extra,
	}
}

// FlapEntry describes the flap detection state for a single peer.
// Exported for RPC observability — callers should treat this as read-only.
type FlapEntry struct {
	// PeerIdentity is the Ed25519 fingerprint of the peer.
	PeerIdentity PeerIdentity

	// RecentWithdrawals is the number of disconnect events within the flap window.
	RecentWithdrawals int

	// InHoldDown is true if the peer is currently in hold-down.
	InHoldDown bool

	// HoldDownUntil is the time when hold-down expires. Zero if not in hold-down.
	HoldDownUntil time.Time
}

// isBetter returns true if candidate is a better route than current.
// Source priority wins first (direct > hop_ack > announcement),
// then lower hop count breaks the tie.
func isBetter(candidate, current *RouteEntry) bool {
	if candidate.Source.TrustRank() != current.Source.TrustRank() {
		return candidate.Source.TrustRank() > current.Source.TrustRank()
	}
	return candidate.Hops < current.Hops
}

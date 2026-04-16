package capture

import (
	"net/netip"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Connection metadata — injected by the node layer
// ---------------------------------------------------------------------------

// ConnInfo is the minimal connection metadata that the Manager needs
// to set up a capture session. Resolved from the node connection registry.
type ConnInfo struct {
	ConnID   domain.ConnID
	RemoteIP netip.Addr
	PeerDir  domain.PeerDirection
}

// ConnResolver is the interface the Manager uses to look up connection
// metadata without depending on the full node.Service.
type ConnResolver interface {
	// ConnInfoByID returns metadata for a single live connection.
	ConnInfoByID(id domain.ConnID) (ConnInfo, bool)
	// ConnInfoByIP returns metadata for all live connections with the given remote IP.
	ConnInfoByIP(ip netip.Addr) []ConnInfo
	// AllConnInfo returns metadata for all live connections.
	AllConnInfo() []ConnInfo
}

// ---------------------------------------------------------------------------
// Domain-level result models (NOT RPC-shaped DTO)
// ---------------------------------------------------------------------------

// StartEntry describes a single capture session in a start result.
type StartEntry struct {
	ConnID   domain.ConnID
	RemoteIP netip.Addr
	PeerDir  domain.PeerDirection
	Format   domain.CaptureFormat
	FilePath string
}

// RuleEntry describes a capture rule that was installed or removed.
type RuleEntry struct {
	Scope          domain.CaptureScope
	Target         netip.Addr          // zero for scope=all
	Format         domain.CaptureFormat
	CreatedAt      time.Time
	MatchedConnIDs []domain.ConnID
}

// StartResult is the domain-level response for any start command.
type StartResult struct {
	Started        []StartEntry
	AlreadyActive  []StartEntry
	InstalledRules []RuleEntry
	NotFound       []string
	Conflicts      []string
	Errors         []string
}

// StopResult is the domain-level response for a stop command.
type StopResult struct {
	Stopped      []StartEntry
	RemovedRules []RuleEntry
	NotFound     []string
	Errors       []string
}

// SessionSnapshot is the immutable point-in-time view of a capture session,
// safe to pass to RPC/UI without holding any locks.
type SessionSnapshot struct {
	ConnID        domain.ConnID
	RemoteIP      netip.Addr
	PeerDirection domain.PeerDirection
	Format        domain.CaptureFormat
	Scope         domain.CaptureScope
	FilePath      string
	StartedAt     time.Time
	DroppedEvents int64
	Recording     bool
	Error         string
}

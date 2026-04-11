package domain

import "time"

// ---------------------------------------------------------------------------
// PeerProvider DTOs — types produced by PeerProvider for consumers
// (ConnectionManager, RPC handlers, diagnostics, persistence).
// ---------------------------------------------------------------------------

// CandidatePeer is a filtered, scored, deduplicated entry ready for dial.
// Produced by PeerProvider.Candidates().
type CandidatePeer struct {
	Address       PeerAddress   // primary address from known peers
	DialAddresses []PeerAddress // ordered: [0] primary, [1] fallback port (if any)
	Source        PeerSource
	Score         int
}

// RestoreEntry carries the full set of static fields loaded from peers.json.
// Separate from Add() so the compiler catches misuse: Add() for runtime
// discovery, Restore() for startup loading.
type RestoreEntry struct {
	Address PeerAddress
	Source  PeerSource
	AddedAt time.Time
	Network NetGroup
}

// BannedIPEntry represents an IP-wide ban record.
// Stored in Service.bannedIPSet, surfaced to PeerProvider via callback,
// and persisted in the banned_ips section of peers.json.
type BannedIPEntry struct {
	IP            string
	BannedUntil   time.Time
	BanOrigin     PeerAddress
	BanReason     string
	AffectedPeers []PeerAddress // addresses on this IP at ban time; persisted so the list survives top-500 trim
}

// ---------------------------------------------------------------------------
// Diagnostics — types returned by PeerProvider for operator-facing RPC.
// ---------------------------------------------------------------------------

// ExcludeReason describes why a known peer was excluded from Candidates().
type ExcludeReason string

const (
	ExcludeReasonForbidden    ExcludeReason = "forbidden"
	ExcludeReasonBanned       ExcludeReason = "banned"
	ExcludeReasonBannedDirect ExcludeReason = "banned_direct"
	ExcludeReasonCooldown     ExcludeReason = "cooldown"
	ExcludeReasonConnected    ExcludeReason = "connected"
	ExcludeReasonQueued       ExcludeReason = "queued"
	ExcludeReasonUnreachable  ExcludeReason = "unreachable"
	ExcludeReasonIPDedup      ExcludeReason = "ip_dedup"
)

// KnownPeerInfo is the unfiltered diagnostic view returned by
// PeerProvider.KnownPeers(). Each entry includes ExcludeReasons
// explaining why the peer was (or wasn't) excluded from Candidates().
// ExcludeReasons is a list — one peer can match multiple exclusion
// criteria simultaneously (e.g. banned + connected).
type KnownPeerInfo struct {
	Address        PeerAddress
	Source         PeerSource
	AddedAt        time.Time
	Network        NetGroup
	Score          int
	Failures       int
	BannedUntil    time.Time
	Connected      bool
	ExcludeReasons []ExcludeReason
}

// BannedIPInfo is the diagnostic view returned by PeerProvider.BannedIPs().
// Operates at the IP level because ban propagation works per-IP:
// one banned port → entire IP banned.
type BannedIPInfo struct {
	IP            string
	BannedUntil   time.Time
	BanOrigin     PeerAddress   // address that received the direct ban
	BanReason     string        // "incompatible_protocol" (only reason in the system)
	AffectedPeers []PeerAddress // all known addresses on this IP affected by propagation
}

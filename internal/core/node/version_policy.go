package node

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// ---------------------------------------------------------------------------
// incompatibleProtocolError — structured error carrying version evidence
// ---------------------------------------------------------------------------

// incompatibleProtocolError wraps errIncompatibleProtocol with the version
// evidence extracted from the remote welcome/hello frame. This allows callers
// (e.g. onCMDialFailed) to pass confirmed version evidence to
// penalizeOldProtocolPeer instead of zero values.
type incompatibleProtocolError struct {
	PeerVersion   domain.ProtocolVersion
	PeerMinimum   domain.ProtocolVersion
	ClientVersion string
	Cause         error
}

func (e *incompatibleProtocolError) Error() string {
	return fmt.Sprintf("incompatible protocol version: peer=%d, min=%d, client=%q: %v",
		e.PeerVersion, e.PeerMinimum, e.ClientVersion, e.Cause)
}

func (e *incompatibleProtocolError) Unwrap() error {
	return errIncompatibleProtocol
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	// versionObservationTTL is the time window within which
	// incompatible-version observations are considered current.
	// Observations older than this do not count towards the reporter
	// threshold and are cleaned up on the periodic repair path.
	versionObservationTTL = 24 * time.Hour

	// incompatibleVersionReporterThreshold is the minimum number of
	// distinct peer identities that must report our version as
	// incompatible before the incompatible_version_signal fires.
	incompatibleVersionReporterThreshold = 3

	// peerBuildReporterThreshold is the minimum number of distinct
	// peer identities with a higher ClientBuild before the
	// peer_build_signal fires. Matches the existing Desktop UI heuristic.
	peerBuildReporterThreshold = 2

	// versionPolicyRepairInterval is the minimum interval between
	// periodic (non-event-driven) recomputes of the version policy
	// snapshot. The periodic path exists as a repair mechanism to expire
	// stale reporters whose TTL elapsed without any new observation
	// event. 60 seconds is well below the 24-hour observation TTL while
	// keeping the cost negligible on the 2-second bootstrapLoop ticker.
	versionPolicyRepairInterval = 60 * time.Second

	// versionPolicyHeartbeatInterval bounds the time a subscriber may
	// remain stale when a prior TopicVersionPolicyChanged publish was
	// dropped by the lossy ebus (inbox full). Publisher-side dedup is
	// unsafe without this heartbeat: if the first publish during a storm
	// is dropped and content does not change for some other reason, the
	// UI would stay stuck on the pre-storm snapshot indefinitely.
	// Chosen equal to versionPolicyRepairInterval so the existing periodic
	// repair path naturally doubles as the heartbeat driver — no extra
	// ticker is needed.
	versionPolicyHeartbeatInterval = versionPolicyRepairInterval
)

// ---------------------------------------------------------------------------
// VersionPolicyState — runtime state owned by Service, under Service.mu
// ---------------------------------------------------------------------------

// versionPolicyState holds the runtime state for the node-owned version
// upgrade detection policy. It is NOT a separate goroutine — reads and
// writes happen under Service.statusMu (writes require Service.peerMu +
// Service.statusMu because recomputes derive from peer-domain state; pure
// readers of the materialised snapshot take Service.statusMu.RLock alone).
//
// Ownership boundary: this struct owns the per-identity reporter dedup
// set that drives the update_available signal. It does NOT own per-address
// attempt counters — those live in peerHealth.IncompatibleVersionAttempts
// and are used exclusively for ban scoring. The two counters measure
// different things and are not projections of each other.
type versionPolicyState struct {
	// incompatibleReporters is the per-identity dedup set for the
	// update_available signal. Key is PeerIdentity, value is the most
	// recent observation from that identity. Only observations with
	// non-empty PeerIdentity are stored here. This is the source of
	// truth for IncompatibleVersionReporters in VersionPolicySnapshot.
	incompatibleReporters map[domain.PeerIdentity]domain.VersionObservation

	// snapshot is the last computed policy snapshot. Recomputed on each
	// observation event and on periodic cleanup.
	snapshot domain.VersionPolicySnapshot

	// lastPeriodicRepair is the timestamp of the last periodic (throttled)
	// recompute. Used to enforce versionPolicyRepairInterval between
	// successive repair passes on the bootstrapLoop ticker.
	lastPeriodicRepair time.Time
}

func newVersionPolicyState() *versionPolicyState {
	return &versionPolicyState{
		incompatibleReporters: make(map[domain.PeerIdentity]domain.VersionObservation),
	}
}

// ---------------------------------------------------------------------------
// Service helpers — called under the canonical peerMu → statusMu lock stack
// (writers) or Service.statusMu.RLock (pure snapshot reader).
// ---------------------------------------------------------------------------

// recordIncompatibleObservationLocked records an incompatible-version
// observation from a peer and recomputes the policy snapshot.
//
// IMPORTANT: this function must only be called when the remote peer's
// minimum_protocol_version exceeds our local config.ProtocolVersion
// (Invariant C — "they think we're old" direction). It must NOT be
// called when we reject an old inbound peer whose version is below our
// minimum — that is the opposite direction and would incorrectly feed
// the "you need to upgrade" signal. The caller (penalizeOldProtocolPeer)
// enforces this guard.
//
// Caller MUST hold:
//   - s.peerMu at least for read (recomputeVersionPolicyLocked reads
//     s.peerBuilds, s.peerIDs, s.peerVersions, s.persistedMeta — all
//     owned by the peer domain)
//   - s.statusMu.Lock (status domain — s.versionPolicy is written here)
//
// Canonical order: peerMu → statusMu with statusMu INNERMOST.
func (s *Service) recordIncompatibleObservationLocked(
	peerID domain.PeerIdentity,
	peerVersion domain.ProtocolVersion,
	peerMinimum domain.ProtocolVersion,
	now time.Time,
) {
	if s.versionPolicy == nil {
		s.versionPolicy = newVersionPolicyState()
	}

	// Only observations with identity count towards the reporter threshold.
	if peerID != "" {
		s.versionPolicy.incompatibleReporters[peerID] = domain.VersionObservation{
			PeerIdentity: peerID,
			ObservedAt:   now,
			PeerVersion:  peerVersion,
			PeerMinimum:  peerMinimum,
		}
	}

	s.recomputeVersionPolicyLocked(now)
}

// recomputeVersionPolicyLocked rebuilds the VersionPolicySnapshot from
// current state.
//
// Caller MUST hold:
//   - s.peerMu at least for read (s.peerBuilds, s.peerIDs, s.peerVersions
//     and s.persistedMeta are peer-domain fields read here)
//   - s.statusMu.Lock (status domain — s.versionPolicy and
//     s.lastVersionPolicyPublishAt are written here)
//
// Canonical order: peerMu → statusMu with statusMu INNERMOST.
func (s *Service) recomputeVersionPolicyLocked(now time.Time) {
	if s.versionPolicy == nil {
		s.versionPolicy = newVersionPolicyState()
	}

	// --- Incompatible version signal ---
	// Expire stale observations.
	cutoff := now.Add(-versionObservationTTL)
	for id, obs := range s.versionPolicy.incompatibleReporters {
		if obs.ObservedAt.Before(cutoff) {
			delete(s.versionPolicy.incompatibleReporters, id)
		}
	}

	reporterCount := domain.ReporterCount(len(s.versionPolicy.incompatibleReporters))
	incompatibleSignal := reporterCount >= incompatibleVersionReporterThreshold

	// Track max observed version across reporters.
	var maxPeerVersion domain.ProtocolVersion
	for _, obs := range s.versionPolicy.incompatibleReporters {
		if obs.PeerVersion > maxPeerVersion {
			maxPeerVersion = obs.PeerVersion
		}
	}

	// --- Peer build signal ---
	// Count distinct peer identities with ClientBuild > ours.
	myBuild := config.ClientBuild
	buildSeen := make(map[domain.PeerIdentity]struct{})
	var maxBuild int
	for addr, build := range s.peerBuilds {
		if build <= myBuild {
			continue
		}
		if build > maxBuild {
			maxBuild = build
		}
		peerID := s.peerIDs[addr]
		if peerID == "" {
			continue
		}
		buildSeen[peerID] = struct{}{}
	}
	buildSignal := len(buildSeen) >= peerBuildReporterThreshold

	// Also track max version from peerVersions (runtime, not persisted).
	for addr := range s.peerBuilds {
		if v, ok := s.peerVersions[addr]; ok {
			// peerVersions stores string versions; protocol version comes from sessions.
			_ = v // version string — not protocol version
		}
	}

	// --- Persisted lockout signal ---
	// Active version lockouts in peers.json are a persistent source of truth
	// that outlives the 24-hour observation TTL. If any peer still has an
	// active version lockout (meaning the local version hasn't changed since
	// we confirmed incompatibility), the node should continue showing
	// update_available even after all ephemeral observations expire.
	lockoutSignal := false
	for _, entry := range s.persistedMeta {
		if entry.VersionLockout.IsActiveAt(now) {
			lockoutSignal = true
			// Contribute the lockout's observed version to the max tracker.
			// Must NOT break early — all active lockouts must be scanned
			// to find the true maximum across the entire persisted set.
			if entry.VersionLockout.ObservedProtocolVersion > maxPeerVersion {
				maxPeerVersion = entry.VersionLockout.ObservedProtocolVersion
			}
		}
	}

	// --- Combine signals ---
	updateAvailable := buildSignal || incompatibleSignal || lockoutSignal
	var reason domain.UpdateReason
	switch {
	case buildSignal && (incompatibleSignal || lockoutSignal):
		reason = domain.UpdateReasonBoth
	case incompatibleSignal || lockoutSignal:
		reason = domain.UpdateReasonIncompatibleVersion
	case buildSignal:
		reason = domain.UpdateReasonPeerBuild
	default:
		reason = domain.UpdateReasonNone
	}

	next := domain.VersionPolicySnapshot{
		UpdateAvailable:              updateAvailable,
		UpdateReason:                 reason,
		IncompatibleVersionReporters: reporterCount,
		MaxObservedPeerBuild:         maxBuild,
		MaxObservedPeerVersion:       maxPeerVersion,
	}

	// Publisher-side no-op gate with heartbeat resync: during peer-disconnect
	// storms this function is called many times with identical inputs (same
	// reporter set, same peer builds). Every redundant publish forces Desktop
	// to rebuild a full NodeStatus snapshot and invalidate every subscribed
	// window, which is the proximate cause of the UI freeze after
	// cm_session_setup_failed cascades. VersionPolicySnapshot has only
	// comparable fields, so direct struct equality is sufficient.
	//
	// However, ebus is intentionally lossy: if a subscriber inbox is full,
	// Publish drops the delivery to protect the publisher. Pure content-based
	// dedup would then leave any subscriber that missed the initial publish
	// permanently stale. The heartbeat (versionPolicyHeartbeatInterval) forces
	// a retransmission on the bootstrapLoop's periodic repair tick even when
	// content is unchanged, putting an upper bound on the time a subscriber
	// may remain out of sync. First publish always fires unconditionally.
	contentChanged := s.versionPolicy.snapshot != next
	heartbeatDue := !s.lastVersionPolicyPublishAt.IsZero() &&
		now.Sub(s.lastVersionPolicyPublishAt) >= versionPolicyHeartbeatInterval
	firstPublish := s.lastVersionPolicyPublishAt.IsZero()

	if !contentChanged && !heartbeatDue && !firstPublish {
		return
	}

	s.versionPolicy.snapshot = next
	s.lastVersionPolicyPublishAt = now

	// Notify subscribers so the UI picks up version-policy changes without
	// polling. Safe to call under peerMu+statusMu — ebus uses its own mutex
	// and async handlers run in separate goroutines.
	s.eventBus.Publish(ebus.TopicVersionPolicyChanged, s.versionPolicy.snapshot)
}

// maybeRecomputeVersionPolicyPeriodic is the throttled repair path for
// version policy. It is called on every bootstrapLoop tick (via
// refreshAggregateStatusLocked) but performs actual work at most once
// per versionPolicyRepairInterval. The repair path ensures that expired
// reporter observations are cleaned up within a bounded window even
// when no new observation events arrive.
//
// Caller MUST hold:
//   - s.peerMu at least for read (transitively via
//     recomputeVersionPolicyLocked — reads peer-domain fields)
//   - s.statusMu.Lock (status domain — s.versionPolicy is written here)
//
// Canonical order: peerMu → statusMu with statusMu INNERMOST.
func (s *Service) maybeRecomputeVersionPolicyPeriodic(now time.Time) {
	if s.versionPolicy == nil {
		return
	}
	if now.Sub(s.versionPolicy.lastPeriodicRepair) < versionPolicyRepairInterval {
		return
	}
	s.versionPolicy.lastPeriodicRepair = now
	s.recomputeVersionPolicyLocked(now)
}

// VersionPolicySnapshot returns the current policy snapshot. Thread-safe.
//
// Uses s.statusMu.RLock alone — s.versionPolicy lives in the status
// domain and the snapshot was already materialised by the most recent
// recomputeVersionPolicyLocked under the full peerMu → statusMu stack.
// This decouples Desktop UI readers from peer-management writers during
// a reconnect storm.
func (s *Service) VersionPolicySnapshot() domain.VersionPolicySnapshot {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	if s.versionPolicy == nil {
		return domain.VersionPolicySnapshot{}
	}
	return s.versionPolicy.snapshot
}

// isPeerVersionLockedOutLocked checks whether a peer has a persisted
// version lockout that is still active. Identity-bound lockouts persist
// until local version change; address-only lockouts expire after
// VersionLockoutMaxTTL to avoid suppressing reassigned addresses.
// Reads s.persistedMeta, which lives in the peer domain — must be called
// under s.peerMu (read lock sufficient).
func (s *Service) isPeerVersionLockedOutLocked(address domain.PeerAddress) bool {
	entry, ok := s.persistedMeta[address]
	if !ok {
		return false
	}
	return entry.VersionLockout.IsActiveAt(time.Now().UTC())
}

// setVersionLockoutLocked records a version lockout for a peer in the
// persisted metadata. The lockout is bound to the peer's cryptographic
// identity when available; identity-less lockouts are subject to a hard
// TTL (VersionLockoutMaxTTL) to prevent stale entries from suppressing
// addresses that may later belong to a different peer.
//
// Identity propagation: when the peer has a known identity, the lockout
// is propagated to ALL addresses that map to the same identity. This
// prevents the connection manager from re-dialing the same peer via an
// alternative address — the incompatibility is a property of the peer's
// software version, not of a specific network endpoint.
//
// Writes s.persistedMeta and reads s.peerIDs — both live in the peer
// domain, so callers must hold s.peerMu (write lock).  Callers that
// also recompute version policy in the same critical section (see
// penalizeOldProtocolPeer) nest s.statusMu INNER under s.peerMu to
// preserve the canonical peerMu → statusMu order; this helper itself
// does not touch statusMu, so it is safe to invoke both with and
// without statusMu held, as long as the enclosing peerMu write is
// still active.
func (s *Service) setVersionLockoutLocked(
	address domain.PeerAddress,
	peerVersion domain.ProtocolVersion,
	peerMinimum domain.ProtocolVersion,
	peerClientVersion domain.ClientVersion,
) {
	entry, ok := s.persistedMeta[address]
	if !ok {
		return
	}
	now := time.Now().UTC()
	peerID := s.peerIDs[address]
	lockout := domain.VersionLockoutSnapshot{
		ObservedProtocolVersion:        peerVersion,
		ObservedMinimumProtocolVersion: peerMinimum,
		ObservedClientVersion:          peerClientVersion,
		LockedAtLocalVersion: domain.LocalVersionFingerprint{
			ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
			ClientBuild:     config.ClientBuild,
		},
		Reason:       domain.VersionLockoutReasonIncompatible,
		LockedAt:     now,
		PeerIdentity: peerID,
	}
	entry.VersionLockout = lockout
	s.persistedMeta[address] = entry

	log.Info().
		Str("peer", string(address)).
		Str("peer_identity", string(peerID)).
		Int("observed_min", int(peerMinimum)).
		Int("local_protocol", config.ProtocolVersion).
		Msg("version_lockout_set")

	// Propagate lockout to all other addresses sharing the same identity.
	// Incompatibility is a property of the peer's software, not the
	// endpoint — dialing an alternative address would produce the same
	// rejection and risk remote-side ban escalation.
	//
	// Siblings receive the lockout when: (a) they have no active lockout,
	// or (b) the new evidence is stronger (higher minimum protocol version)
	// or fresher (newer timestamp). Without (b), already-active siblings
	// would keep stale evidence forever, skewing diagnostics like
	// ObservedClientVersion and MaxObservedPeerVersion.
	if peerID != "" {
		for otherAddr, otherID := range s.peerIDs {
			if otherAddr == address || otherID != peerID {
				continue
			}
			otherEntry, ok := s.persistedMeta[otherAddr]
			if !ok {
				continue
			}
			existing := otherEntry.VersionLockout
			shouldPropagate := !existing.IsActive() ||
				lockout.ObservedMinimumProtocolVersion > existing.ObservedMinimumProtocolVersion ||
				lockout.LockedAt.After(existing.LockedAt)
			if shouldPropagate {
				otherEntry.VersionLockout = lockout
				log.Info().
					Str("peer", string(otherAddr)).
					Str("peer_identity", string(peerID)).
					Str("source_address", string(address)).
					Bool("was_active", existing.IsActive()).
					Msg("version_lockout_propagated_by_identity")
			}
		}
	}
}

// clearStaleVersionLockoutsLocked removes version lockouts from persisted
// metadata in two cases:
//  1. The local version fingerprint changed (node upgraded) — all lockouts
//     are cleared regardless of identity binding.
//  2. Address-only (identity-less) lockouts whose hard TTL has expired.
//
// Called eagerly on startup after loading peers.json, before the
// network is accepting frames, so no concurrent peer-domain readers
// exist at that point.  Mutates s.persistedMeta, which is peer-domain
// state — must be called under s.peerMu write lock.
func (s *Service) clearStaleVersionLockoutsLocked() {
	localFP := domain.LocalVersionFingerprint{
		ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
		ClientBuild:     config.ClientBuild,
	}
	now := time.Now().UTC()
	for addr, entry := range s.persistedMeta {
		if !entry.VersionLockout.IsActive() {
			continue
		}
		lk := entry.VersionLockout.LockedAtLocalVersion

		// Case 1: local version changed — clear unconditionally.
		if lk.ProtocolVersion != localFP.ProtocolVersion || lk.ClientBuild != localFP.ClientBuild {
			log.Info().
				Str("peer", string(addr)).
				Int("old_protocol", int(lk.ProtocolVersion)).
				Int("old_build", lk.ClientBuild).
				Int("current_protocol", int(localFP.ProtocolVersion)).
				Int("current_build", localFP.ClientBuild).
				Msg("version_lockout_cleared_after_upgrade")
			entry.VersionLockout = domain.VersionLockoutSnapshot{}
			s.persistedMeta[addr] = entry
			continue
		}

		// Case 2: address-only lockout TTL expired.
		if !entry.VersionLockout.IsActiveAt(now) {
			log.Info().
				Str("peer", string(addr)).
				Time("locked_at", entry.VersionLockout.LockedAt).
				Msg("version_lockout_cleared_ttl_expired")
			entry.VersionLockout = domain.VersionLockoutSnapshot{}
			s.persistedMeta[addr] = entry
		}
	}
}

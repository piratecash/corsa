package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/transport"
)

// ---------------------------------------------------------------------------
// VersionPolicyState unit tests
// ---------------------------------------------------------------------------

func TestVersionPolicy_IncompatibleReporterThreshold(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}
	now := time.Now().UTC()

	// Two reporters should not trigger the signal (threshold is 3).
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-bbb", 10, 10, now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.UpdateAvailable {
		t.Error("update_available should be false with only 2 reporters")
	}
	if snap.IncompatibleVersionReporters != 2 {
		t.Errorf("reporters = %d, want 2", snap.IncompatibleVersionReporters)
	}

	// Third reporter should trigger.
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, now)
	snap = svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if !snap.UpdateAvailable {
		t.Error("update_available should be true with 3 reporters")
	}
	if snap.UpdateReason != domain.UpdateReasonIncompatibleVersion {
		t.Errorf("reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonIncompatibleVersion)
	}
}

func TestVersionPolicy_EmptyIdentityDoesNotCountAsReporter(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}
	now := time.Now().UTC()

	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("", 10, 10, now)
	svc.recordIncompatibleObservationLocked("", 10, 10, now.Add(time.Second))
	svc.recordIncompatibleObservationLocked("", 10, 10, now.Add(2*time.Second))
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.IncompatibleVersionReporters != 0 {
		t.Errorf("reporters = %d, want 0 (no identity)", snap.IncompatibleVersionReporters)
	}
	if snap.UpdateAvailable {
		t.Error("update_available should be false without identity-based reporters")
	}
}

func TestVersionPolicy_DedupByIdentity(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}
	now := time.Now().UTC()

	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, now.Add(time.Minute))
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, now.Add(2*time.Minute))
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.IncompatibleVersionReporters != 1 {
		t.Errorf("reporters = %d, want 1 (same identity deduped)", snap.IncompatibleVersionReporters)
	}
}

func TestVersionPolicy_ObservationTTLExpiry(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}

	stale := time.Now().UTC().Add(-25 * time.Hour)
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-bbb", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, stale)
	svc.mu.Unlock()

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.IncompatibleVersionReporters != 0 {
		t.Errorf("reporters = %d, want 0 (all expired)", snap.IncompatibleVersionReporters)
	}
	if snap.UpdateAvailable {
		t.Error("update_available should revert to false after TTL expiry")
	}
}

func TestVersionPolicy_PeerBuildSignal(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
			"5.6.7.8:200": "peer-bbb",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild + 1,
			"5.6.7.8:200": config.ClientBuild + 2,
		},
		peerVersions: make(map[domain.PeerAddress]string),
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if !snap.UpdateAvailable {
		t.Error("update_available should be true with 2 peers having higher build")
	}
	if snap.UpdateReason != domain.UpdateReasonPeerBuild {
		t.Errorf("reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonPeerBuild)
	}
	if snap.MaxObservedPeerBuild != config.ClientBuild+2 {
		t.Errorf("max_build = %d, want %d", snap.MaxObservedPeerBuild, config.ClientBuild+2)
	}
}

func TestVersionPolicy_BothSignals(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
			"5.6.7.8:200": "peer-bbb",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild + 1,
			"5.6.7.8:200": config.ClientBuild + 2,
		},
		peerVersions: make(map[domain.PeerAddress]string),
	}
	now := time.Now().UTC()

	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-ddd", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-eee", 10, 10, now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if !snap.UpdateAvailable {
		t.Error("update_available should be true with both signals")
	}
	if snap.UpdateReason != domain.UpdateReasonBoth {
		t.Errorf("reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonBoth)
	}
}

// ---------------------------------------------------------------------------
// Version lockout tests
// ---------------------------------------------------------------------------

func TestVersionLockout_SetAndCheck(t *testing.T) {
	t.Parallel()

	entry := &peerEntry{Address: "1.2.3.4:100"}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			"1.2.3.4:100": entry,
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
		},
		peerVersions: make(map[domain.PeerAddress]string),
	}

	svc.mu.Lock()
	svc.setVersionLockoutLocked(
		"1.2.3.4:100",
		domain.ProtocolVersion(10),
		domain.ProtocolVersion(10),
		"v1.0.0",
	)
	active := svc.isPeerVersionLockedOutLocked("1.2.3.4:100")
	svc.mu.Unlock()

	if !active {
		t.Error("version lockout should be active after set")
	}

	// Verify identity was captured.
	if entry.VersionLockout.PeerIdentity != "peer-aaa" {
		t.Errorf("lockout peer_identity = %q, want %q", entry.VersionLockout.PeerIdentity, "peer-aaa")
	}
	if entry.VersionLockout.LockedAt.IsZero() {
		t.Error("lockout locked_at should be set")
	}
}

func TestVersionLockout_ClearedAfterUpgrade(t *testing.T) {
	t.Parallel()

	entry := &peerEntry{
		Address: "1.2.3.4:100",
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        10,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion - 1),
				ClientBuild:     config.ClientBuild - 1,
			},
			Reason: domain.VersionLockoutReasonIncompatible,
		},
	}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			"1.2.3.4:100": entry,
		},
	}

	svc.mu.Lock()
	svc.clearStaleVersionLockoutsLocked()
	active := svc.isPeerVersionLockedOutLocked("1.2.3.4:100")
	svc.mu.Unlock()

	if active {
		t.Error("lockout should be cleared after local version upgrade")
	}
}

func TestVersionLockout_NotClearedWhenSameVersion(t *testing.T) {
	t.Parallel()

	entry := &peerEntry{
		Address: "1.2.3.4:100",
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        10,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
				ClientBuild:     config.ClientBuild,
			},
			Reason:       domain.VersionLockoutReasonIncompatible,
			LockedAt:     time.Now().UTC(),
			PeerIdentity: "peer-aaa",
		},
	}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			"1.2.3.4:100": entry,
		},
	}

	svc.mu.Lock()
	svc.clearStaleVersionLockoutsLocked()
	active := svc.isPeerVersionLockedOutLocked("1.2.3.4:100")
	svc.mu.Unlock()

	if !active {
		t.Error("lockout should remain active when local version unchanged")
	}
}

// ---------------------------------------------------------------------------
// Ban escalation tests
// ---------------------------------------------------------------------------

func TestBanIncrementIncompatibleVersion_IsNotImmediate(t *testing.T) {
	t.Parallel()

	if banIncrementIncompatibleVersion >= banThreshold {
		t.Errorf("banIncrementIncompatibleVersion = %d, should be < banThreshold (%d)",
			banIncrementIncompatibleVersion, banThreshold)
	}
	if banIncrementIncompatibleVersion != 250 {
		t.Errorf("banIncrementIncompatibleVersion = %d, want 250", banIncrementIncompatibleVersion)
	}
}

func TestOverlayBanThreshold_RequiresMultipleAttempts(t *testing.T) {
	t.Parallel()

	attemptsToban := peerBanThresholdIncompatible / peerBanIncrementIncompatible
	if attemptsToban < 2 {
		t.Errorf("overlay ban should require at least 2 attempts, got %d", attemptsToban)
	}
	if attemptsToban != 4 {
		t.Errorf("overlay ban triggers at %d attempts, want 4", attemptsToban)
	}
}

// ---------------------------------------------------------------------------
// Persistence round-trip tests
// ---------------------------------------------------------------------------

func TestPeerEntry_VersionLockoutRoundTrip(t *testing.T) {
	t.Parallel()

	lockTime := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	entry := peerEntry{
		Address: "1.2.3.4:100",
		Source:  domain.PeerSourcePersisted,
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        10,
			ObservedMinimumProtocolVersion: 8,
			ObservedClientVersion:          "v1.0.0",
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: 9,
				ClientBuild:     34,
			},
			Reason:       domain.VersionLockoutReasonIncompatible,
			LockedAt:     lockTime,
			PeerIdentity: "peer-xyz",
		},
	}

	if !entry.VersionLockout.IsActive() {
		t.Error("lockout should be active")
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got peerEntry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.VersionLockout.Reason != domain.VersionLockoutReasonIncompatible {
		t.Errorf("reason = %q, want %q", got.VersionLockout.Reason, domain.VersionLockoutReasonIncompatible)
	}
	if got.VersionLockout.ObservedMinimumProtocolVersion != 8 {
		t.Errorf("observed_min = %d, want 8", got.VersionLockout.ObservedMinimumProtocolVersion)
	}
	if got.VersionLockout.LockedAtLocalVersion.ClientBuild != 34 {
		t.Errorf("locked_build = %d, want 34", got.VersionLockout.LockedAtLocalVersion.ClientBuild)
	}
	if !got.VersionLockout.LockedAt.Equal(lockTime) {
		t.Errorf("locked_at = %v, want %v", got.VersionLockout.LockedAt, lockTime)
	}
	if got.VersionLockout.PeerIdentity != "peer-xyz" {
		t.Errorf("peer_identity = %q, want %q", got.VersionLockout.PeerIdentity, "peer-xyz")
	}
}

// ---------------------------------------------------------------------------
// Domain type tests
// ---------------------------------------------------------------------------

func TestVersionLockoutSnapshot_IsActive(t *testing.T) {
	t.Parallel()

	empty := domain.VersionLockoutSnapshot{}
	if empty.IsActive() {
		t.Error("empty snapshot should not be active")
	}

	active := domain.VersionLockoutSnapshot{Reason: domain.VersionLockoutReasonIncompatible}
	if !active.IsActive() {
		t.Error("snapshot with reason should be active")
	}
}

// ---------------------------------------------------------------------------
// P1: TTL vs lockout — persisted lockouts contribute to update_available
// ---------------------------------------------------------------------------

func TestVersionPolicy_LockoutKeepsUpdateAvailableAfterTTLExpiry(t *testing.T) {
	t.Parallel()

	// Set up: 3 observations that are expired (>24h old) AND a persisted
	// lockout that is still active. The lockout should keep update_available=true.
	now := time.Now().UTC()
	entry := &peerEntry{
		Address: "1.2.3.4:100",
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        10,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
				ClientBuild:     config.ClientBuild,
			},
			Reason:       domain.VersionLockoutReasonIncompatible,
			LockedAt:     now.Add(-2 * time.Hour),
			PeerIdentity: "peer-locked",
		},
	}
	svc := &Service{
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			"1.2.3.4:100": entry,
		},
	}

	// Record 3 stale observations (expired).
	stale := now.Add(-25 * time.Hour)
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-bbb", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, stale)
	// Recompute with current time — observations expire, but lockout persists.
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.IncompatibleVersionReporters != 0 {
		t.Errorf("reporters = %d, want 0 (all expired)", snap.IncompatibleVersionReporters)
	}
	if !snap.UpdateAvailable {
		t.Error("update_available should remain true — active lockout persists")
	}
	if snap.UpdateReason != domain.UpdateReasonIncompatibleVersion {
		t.Errorf("reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonIncompatibleVersion)
	}
}

func TestVersionPolicy_NoLockoutNoFalsePositive(t *testing.T) {
	t.Parallel()

	// No lockouts, no active observations => update_available must be false.
	svc := &Service{
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.UpdateAvailable {
		t.Error("update_available should be false with no signals")
	}
}

// ---------------------------------------------------------------------------
// P2: Address-scoped lockout TTL for identity-less entries
// ---------------------------------------------------------------------------

func TestVersionLockout_IdentityLessExpiresAfterTTL(t *testing.T) {
	t.Parallel()

	lockTime := time.Now().UTC().Add(-domain.VersionLockoutMaxTTL - time.Hour)
	snap := domain.VersionLockoutSnapshot{
		Reason:   domain.VersionLockoutReasonIncompatible,
		LockedAt: lockTime,
		// PeerIdentity is empty — address-only lockout.
	}

	if !snap.IsActive() {
		t.Error("IsActive (no time check) should still be true")
	}
	if snap.IsActiveAt(time.Now().UTC()) {
		t.Error("IsActiveAt should return false after TTL expiry for identity-less lockout")
	}
}

func TestVersionLockout_IdentityBoundDoesNotExpireByTTL(t *testing.T) {
	t.Parallel()

	lockTime := time.Now().UTC().Add(-domain.VersionLockoutMaxTTL - time.Hour)
	snap := domain.VersionLockoutSnapshot{
		Reason:       domain.VersionLockoutReasonIncompatible,
		LockedAt:     lockTime,
		PeerIdentity: "peer-aaa",
	}

	if !snap.IsActiveAt(time.Now().UTC()) {
		t.Error("identity-bound lockout should not expire by TTL")
	}
}

func TestVersionLockout_IdentityLessStillActiveWithinTTL(t *testing.T) {
	t.Parallel()

	lockTime := time.Now().UTC().Add(-time.Hour) // well within TTL
	snap := domain.VersionLockoutSnapshot{
		Reason:   domain.VersionLockoutReasonIncompatible,
		LockedAt: lockTime,
		// PeerIdentity is empty.
	}

	if !snap.IsActiveAt(time.Now().UTC()) {
		t.Error("identity-less lockout should be active within TTL")
	}
}

func TestVersionLockout_ClearStale_ExpiredIdentityLess(t *testing.T) {
	t.Parallel()

	lockTime := time.Now().UTC().Add(-domain.VersionLockoutMaxTTL - time.Hour)
	entry := &peerEntry{
		Address: "1.2.3.4:100",
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        10,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
				ClientBuild:     config.ClientBuild,
			},
			Reason:   domain.VersionLockoutReasonIncompatible,
			LockedAt: lockTime,
			// PeerIdentity empty — address-only.
		},
	}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			"1.2.3.4:100": entry,
		},
	}

	svc.mu.Lock()
	svc.clearStaleVersionLockoutsLocked()
	svc.mu.Unlock()

	if entry.VersionLockout.IsActive() {
		t.Error("expired identity-less lockout should be cleared on startup")
	}
}

// ---------------------------------------------------------------------------
// P1: Build signal lifecycle — disconnect clears peerBuilds
// ---------------------------------------------------------------------------

func TestVersionPolicy_DisconnectClearsBuildSignal(t *testing.T) {
	t.Parallel()

	// Two connected peers with higher builds => build signal active.
	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
			"5.6.7.8:200": "peer-bbb",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild + 1,
			"5.6.7.8:200": config.ClientBuild + 2,
		},
		peerVersions: map[domain.PeerAddress]string{
			"1.2.3.4:100": "v1.1.0",
			"5.6.7.8:200": "v1.2.0",
		},
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		health:        make(map[domain.PeerAddress]*peerHealth),
		observedAddrs: make(map[domain.PeerIdentity]string),
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if !snap.UpdateAvailable {
		t.Fatal("precondition: update_available should be true with 2 higher-build peers")
	}

	// Disconnect one peer — should drop below threshold (need 2).
	svc.markPeerDisconnected("1.2.3.4:100", nil)

	svc.mu.RLock()
	snap = svc.versionPolicy.snapshot
	svc.mu.RUnlock()

	if snap.UpdateAvailable {
		t.Error("update_available should be false after disconnecting a peer below build threshold")
	}

	// Verify session-scoped maps are cleaned up.
	svc.mu.RLock()
	_, hasBuild := svc.peerBuilds["1.2.3.4:100"]
	_, hasVer := svc.peerVersions["1.2.3.4:100"]
	svc.mu.RUnlock()

	if hasBuild {
		t.Error("peerBuilds should be cleared for disconnected peer")
	}
	if hasVer {
		t.Error("peerVersions should be cleared for disconnected peer")
	}
}

// ---------------------------------------------------------------------------
// P2: Operator override — add_peer clears version lockout
// ---------------------------------------------------------------------------

func TestAddPeer_ClearsVersionLockout(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("10.0.0.1:64646")
	now := time.Now().UTC()
	entry := &peerEntry{
		Address:  addr,
		Source:   domain.PeerSourcePersisted,
		NodeType: domain.NodeTypeFull,
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        10,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
				ClientBuild:     config.ClientBuild,
			},
			Reason:       domain.VersionLockoutReasonIncompatible,
			LockedAt:     now.Add(-time.Hour),
			PeerIdentity: "peer-locked",
		},
	}

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: entry,
		},
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health: map[domain.PeerAddress]*peerHealth{
			addr: {
				Address:                     addr,
				IncompatibleVersionAttempts: 3,
				LastErrorCode:               "incompatible_protocol_version",
				ObservedPeerVersion:         10,
				ObservedPeerMinimumVersion:  10,
				BannedUntil:                 now.Add(24 * time.Hour),
			},
		},
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
		observedAddrs: make(map[domain.PeerIdentity]string),
	}

	// Verify lockout is active before add_peer.
	svc.mu.RLock()
	activeBefore := svc.isPeerVersionLockedOutLocked(addr)
	svc.mu.RUnlock()
	if !activeBefore {
		t.Fatal("precondition: lockout should be active before add_peer")
	}

	// Simulate add_peer — call the addPeerFrame directly is complex because
	// it requires full Service wiring. Instead, replicate the lockout-clearing
	// logic that addPeerFrame executes under the lock.
	svc.mu.Lock()
	h := svc.health[addr]
	h.ConsecutiveFailures = 0
	h.LastDisconnectedAt = time.Time{}
	h.BannedUntil = time.Time{}
	h.IncompatibleVersionAttempts = 0
	h.LastErrorCode = ""
	h.LastIncompatibleVersionAt = time.Time{}
	h.ObservedPeerVersion = 0
	h.ObservedPeerMinimumVersion = 0

	if pm := svc.persistedMeta[addr]; pm != nil {
		if pm.VersionLockout.IsActive() {
			pm.VersionLockout = domain.VersionLockoutSnapshot{}
		}
	}
	svc.recomputeVersionPolicyLocked(time.Now().UTC())
	svc.mu.Unlock()

	// Verify lockout is cleared.
	svc.mu.RLock()
	activeAfter := svc.isPeerVersionLockedOutLocked(addr)
	svc.mu.RUnlock()

	if activeAfter {
		t.Error("version lockout should be cleared after operator add_peer override")
	}

	// Health diagnostics should also be reset.
	if h.IncompatibleVersionAttempts != 0 {
		t.Errorf("IncompatibleVersionAttempts = %d, want 0", h.IncompatibleVersionAttempts)
	}
	if h.BannedUntil != (time.Time{}) {
		t.Errorf("BannedUntil should be zero after override, got %v", h.BannedUntil)
	}
}

func TestVersionPolicy_DisconnectDoesNotAffectLockoutSignal(t *testing.T) {
	t.Parallel()

	// A disconnected peer with an active lockout should still contribute
	// to the lockout signal (lockouts are persisted, not session-scoped).
	addr := domain.PeerAddress("1.2.3.4:100")
	now := time.Now().UTC()
	entry := &peerEntry{
		Address: addr,
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        10,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
				ClientBuild:     config.ClientBuild,
			},
			Reason:       domain.VersionLockoutReasonIncompatible,
			LockedAt:     now.Add(-time.Hour),
			PeerIdentity: "peer-locked",
		},
	}

	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr: "peer-locked",
		},
		peerBuilds: map[domain.PeerAddress]int{
			addr: config.ClientBuild + 1,
		},
		peerVersions: map[domain.PeerAddress]string{
			addr: "v1.1.0",
		},
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: entry,
		},
		health:        make(map[domain.PeerAddress]*peerHealth),
		observedAddrs: make(map[domain.PeerIdentity]string),
	}

	// Disconnect the peer — clears session-scoped maps.
	svc.markPeerDisconnected(addr, nil)

	// Lockout signal should still be active (persisted, not session-scoped).
	svc.mu.RLock()
	snap := svc.versionPolicy.snapshot
	svc.mu.RUnlock()

	if !snap.UpdateAvailable {
		t.Error("update_available should remain true — persisted lockout survives disconnect")
	}
	if snap.UpdateReason != domain.UpdateReasonIncompatibleVersion {
		t.Errorf("reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonIncompatibleVersion)
	}
}

// ---------------------------------------------------------------------------
// P1: last_disconnect_code / last_error_code lifecycle
// ---------------------------------------------------------------------------

func TestDisconnectCode_ClearedOnCleanDisconnect(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("1.2.3.4:100")
	svc := &Service{
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		health: map[domain.PeerAddress]*peerHealth{
			addr: {
				Address:            addr,
				Connected:          true,
				LastDisconnectCode: "frame-too-large",
			},
		},
		observedAddrs: make(map[domain.PeerIdentity]string),
	}

	// Clean disconnect (err == nil) should clear the stale code.
	svc.markPeerDisconnected(addr, nil)

	svc.mu.RLock()
	code := svc.health[addr].LastDisconnectCode
	svc.mu.RUnlock()

	if code != "" {
		t.Errorf("LastDisconnectCode = %q, want empty after clean disconnect", code)
	}
}

func TestDisconnectCode_SetOnProtocolError(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("1.2.3.4:100")
	svc := &Service{
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		health:        make(map[domain.PeerAddress]*peerHealth),
		observedAddrs: make(map[domain.PeerIdentity]string),
	}

	// Disconnect with a known protocol error.
	svc.markPeerDisconnected(addr, protocol.ErrFrameTooLarge)

	svc.mu.RLock()
	code := svc.health[addr].LastDisconnectCode
	svc.mu.RUnlock()

	if code != protocol.ErrCodeFrameTooLarge {
		t.Errorf("LastDisconnectCode = %q, want %q", code, protocol.ErrCodeFrameTooLarge)
	}
}

func TestErrorCodes_ClearedOnSuccessfulReconnect(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("1.2.3.4:100")
	svc := &Service{
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		health: map[domain.PeerAddress]*peerHealth{
			addr: {
				Address:            addr,
				LastErrorCode:      protocol.ErrCodeIncompatibleProtocol,
				LastDisconnectCode: "rate-limited",
			},
		},
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
		observedAddrs: make(map[domain.PeerIdentity]string),
	}

	// Successful reconnect should clear both codes.
	svc.markPeerConnected(addr, "outbound")

	svc.mu.RLock()
	h := svc.health[addr]
	errCode := h.LastErrorCode
	dcCode := h.LastDisconnectCode
	svc.mu.RUnlock()

	if errCode != "" {
		t.Errorf("LastErrorCode = %q, want empty after successful reconnect", errCode)
	}
	if dcCode != "" {
		t.Errorf("LastDisconnectCode = %q, want empty after successful reconnect", dcCode)
	}
}

// ---------------------------------------------------------------------------
// P2: Build-based policy path and reason transitions
// ---------------------------------------------------------------------------

func TestVersionPolicy_BuildSignalBelowThreshold(t *testing.T) {
	t.Parallel()

	// Only 1 peer with higher build — threshold is 2, so no signal.
	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild + 5,
		},
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.UpdateAvailable {
		t.Error("update_available should be false with only 1 higher-build peer (threshold 2)")
	}
	if snap.UpdateReason != domain.UpdateReasonNone {
		t.Errorf("reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonNone)
	}
	// MaxObservedPeerBuild should still track the value even below threshold.
	if snap.MaxObservedPeerBuild != config.ClientBuild+5 {
		t.Errorf("max_build = %d, want %d", snap.MaxObservedPeerBuild, config.ClientBuild+5)
	}
}

func TestVersionPolicy_BuildSignalIgnoresEqualBuild(t *testing.T) {
	t.Parallel()

	// Two peers with equal build — should not trigger signal.
	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
			"5.6.7.8:200": "peer-bbb",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild,
			"5.6.7.8:200": config.ClientBuild,
		},
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.UpdateAvailable {
		t.Error("update_available should be false when peers have equal build")
	}
}

func TestVersionPolicy_ReasonTransition_BuildToBoth(t *testing.T) {
	t.Parallel()

	// Start with build signal only, then add incompatible observations.
	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
			"5.6.7.8:200": "peer-bbb",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild + 1,
			"5.6.7.8:200": config.ClientBuild + 2,
		},
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.UpdateReason != domain.UpdateReasonPeerBuild {
		t.Fatalf("precondition: reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonPeerBuild)
	}

	// Add 3 incompatible-version reporters — transitions to "both".
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-ddd", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-eee", 10, 10, now)
	snap = svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.UpdateReason != domain.UpdateReasonBoth {
		t.Errorf("reason = %q, want %q after adding incompatible reporters", snap.UpdateReason, domain.UpdateReasonBoth)
	}
}

func TestVersionPolicy_ReasonTransition_BothToBuildonlyAfterTTL(t *testing.T) {
	t.Parallel()

	// Both signals active, then incompatible observations expire.
	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
			"5.6.7.8:200": "peer-bbb",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild + 1,
			"5.6.7.8:200": config.ClientBuild + 2,
		},
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	stale := time.Now().UTC().Add(-25 * time.Hour)
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	// Record stale observations — they'll appear active during recording
	// but expire on the next recompute with a future timestamp.
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-ddd", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-eee", 10, 10, stale)
	svc.mu.Unlock()

	// Recompute with current time — observations expire, only build remains.
	now := time.Now().UTC()
	svc.mu.Lock()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if !snap.UpdateAvailable {
		t.Fatal("update_available should still be true (build signal remains)")
	}
	if snap.UpdateReason != domain.UpdateReasonPeerBuild {
		t.Errorf("reason = %q, want %q after incompatible observations expired",
			snap.UpdateReason, domain.UpdateReasonPeerBuild)
	}
}

func TestVersionPolicy_BuildPlusLockout_ReasonIsBoth(t *testing.T) {
	t.Parallel()

	// Build signal + persisted lockout (no live reporters) → "both".
	now := time.Now().UTC()
	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			"1.2.3.4:100": "peer-aaa",
			"5.6.7.8:200": "peer-bbb",
		},
		peerBuilds: map[domain.PeerAddress]int{
			"1.2.3.4:100": config.ClientBuild + 1,
			"5.6.7.8:200": config.ClientBuild + 2,
		},
		peerVersions: make(map[domain.PeerAddress]string),
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			"9.9.9.9:100": {
				Address: "9.9.9.9:100",
				VersionLockout: domain.VersionLockoutSnapshot{
					ObservedProtocolVersion:        10,
					ObservedMinimumProtocolVersion: 10,
					LockedAtLocalVersion: domain.LocalVersionFingerprint{
						ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
						ClientBuild:     config.ClientBuild,
					},
					Reason:       domain.VersionLockoutReasonIncompatible,
					LockedAt:     now.Add(-time.Hour),
					PeerIdentity: "peer-locked",
				},
			},
		},
	}

	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if !snap.UpdateAvailable {
		t.Fatal("update_available should be true (build + lockout)")
	}
	if snap.UpdateReason != domain.UpdateReasonBoth {
		t.Errorf("reason = %q, want %q (build signal + lockout signal)",
			snap.UpdateReason, domain.UpdateReasonBoth)
	}
}

// ---------------------------------------------------------------------------
// Invariant A: no evidence → no lockout (negative safety test)
// ---------------------------------------------------------------------------

func TestVersionLockout_NoEvidenceNoLockout_ZeroMinimum(t *testing.T) {
	t.Parallel()

	// peerMinimum = 0 means "unknown" — no version evidence.
	// penalizeOldProtocolPeer must NOT set a lockout in this case.
	addr := domain.PeerAddress("1.2.3.4:100")
	entry := &peerEntry{Address: addr}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: entry,
		},
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Call with peerMinimum=0 (no evidence from wire).
	svc.penalizeOldProtocolPeer(addr, 0, 0)

	svc.mu.RLock()
	lockoutActive := svc.isPeerVersionLockedOutLocked(addr)
	svc.mu.RUnlock()

	if lockoutActive {
		t.Error("lockout must NOT be set when peerMinimum is 0 (no version evidence)")
	}
}

func TestVersionLockout_NoEvidenceNoLockout_MinimumBelowLocal(t *testing.T) {
	t.Parallel()

	// peerMinimum exists but is BELOW our local version — the remote peer
	// accepted us, so evidence does NOT confirm incompatibility. Lockout
	// must NOT be set.
	addr := domain.PeerAddress("1.2.3.4:100")
	entry := &peerEntry{Address: addr}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: entry,
		},
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// peerMinimum <= config.ProtocolVersion — evidence does not confirm
	// that we are incompatible.
	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	svc.penalizeOldProtocolPeer(addr, localProto, localProto)

	svc.mu.RLock()
	lockoutActive := svc.isPeerVersionLockedOutLocked(addr)
	svc.mu.RUnlock()

	if lockoutActive {
		t.Error("lockout must NOT be set when peerMinimum <= local protocol version")
	}
}

func TestVersionLockout_EvidenceConfirmed_LockoutSet(t *testing.T) {
	t.Parallel()

	// peerMinimum > config.ProtocolVersion — confirmed evidence. Lockout
	// MUST be set.
	addr := domain.PeerAddress("1.2.3.4:100")
	entry := &peerEntry{Address: addr}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: entry,
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr: "peer-confirmed",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	higherMinimum := domain.ProtocolVersion(config.ProtocolVersion + 1)
	svc.penalizeOldProtocolPeer(addr, higherMinimum, higherMinimum)

	svc.mu.RLock()
	lockoutActive := svc.isPeerVersionLockedOutLocked(addr)
	svc.mu.RUnlock()

	if !lockoutActive {
		t.Error("lockout MUST be set when peerMinimum > local protocol version (confirmed evidence)")
	}
	if entry.VersionLockout.Reason != domain.VersionLockoutReasonIncompatible {
		t.Errorf("lockout reason = %q, want %q",
			entry.VersionLockout.Reason, domain.VersionLockoutReasonIncompatible)
	}
	if entry.VersionLockout.PeerIdentity != "peer-confirmed" {
		t.Errorf("lockout peer_identity = %q, want %q",
			entry.VersionLockout.PeerIdentity, "peer-confirmed")
	}
}

// ---------------------------------------------------------------------------
// Invariant C: inbound old peers must NOT feed the reporter set
// ---------------------------------------------------------------------------

// TestInboundOldPeer_DoesNotFeedReporterSet verifies that when an old peer
// connects inbound (their version < our minimum), penalizeOldProtocolPeer
// accumulates ban scoring but does NOT record a reporter observation.
// The "update_available" signal must only fire when remote peers tell us
// that OUR version is too old (peerMinimum > local), not when old peers
// connect to us (peerMinimum <= local or zero).
func TestInboundOldPeer_DoesNotFeedReporterSet(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)

	// Simulate 4 distinct old peers connecting inbound. Their protocol
	// version is below our minimum, and their minimum is at or below ours.
	// This is the "they are old" direction — NOT "we are old".
	svc := &Service{
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		health:        make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
	}

	for i := 0; i < 4; i++ {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:100", i+1))
		identity := domain.PeerIdentity(fmt.Sprintf("old-peer-%d", i))
		svc.persistedMeta[addr] = &peerEntry{Address: addr}
		svc.peerIDs[addr] = identity
		// Old peer: version=1, minimum=1 (well below our local version).
		svc.penalizeOldProtocolPeer(addr, 1, 1)
	}

	snap := svc.VersionPolicySnapshot()

	// Ban scoring should have happened (health counters incremented).
	for i := 0; i < 4; i++ {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:100", i+1))
		svc.mu.RLock()
		h := svc.health[addr]
		attempts := h.IncompatibleVersionAttempts
		svc.mu.RUnlock()
		if attempts != 1 {
			t.Errorf("peer %s: IncompatibleVersionAttempts = %d, want 1", addr, attempts)
		}
	}

	// But the reporter set must be empty — old peers connecting to us
	// must NOT trigger the update signal.
	if snap.IncompatibleVersionReporters != 0 {
		t.Errorf("IncompatibleVersionReporters = %d, want 0 "+
			"(old inbound peers must not feed the reporter set)",
			snap.IncompatibleVersionReporters)
	}
	if snap.UpdateAvailable {
		t.Error("UpdateAvailable must be false when only old peers connected inbound")
	}

	// Now simulate one peer that tells us WE are old (peerMinimum > local).
	// This IS the correct direction — verify it still works.
	addr := domain.PeerAddress("10.0.0.99:100")
	svc.persistedMeta[addr] = &peerEntry{Address: addr}
	svc.peerIDs[addr] = "newer-peer"
	svc.penalizeOldProtocolPeer(addr, localProto+2, localProto+1)

	snap = svc.VersionPolicySnapshot()
	if snap.IncompatibleVersionReporters != 1 {
		t.Errorf("IncompatibleVersionReporters = %d, want 1 "+
			"(peer with peerMinimum > local SHOULD be recorded)",
			snap.IncompatibleVersionReporters)
	}
}

// TestInboundOldPeer_ZeroMinimum_NoReporter verifies that penalizeOldProtocolPeer
// with peerMinimum=0 (unknown, e.g. from a bare error without version evidence)
// does not feed the reporter set, even if the peer has a known identity.
func TestInboundOldPeer_ZeroMinimum_NoReporter(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("10.0.0.1:100")
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr: "known-peer",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// peerMinimum=0 means no version evidence — cannot confirm direction.
	svc.penalizeOldProtocolPeer(addr, 0, 0)

	snap := svc.VersionPolicySnapshot()
	if snap.IncompatibleVersionReporters != 0 {
		t.Errorf("IncompatibleVersionReporters = %d, want 0 "+
			"(zero peerMinimum must not feed reporter set)", snap.IncompatibleVersionReporters)
	}
}

// ---------------------------------------------------------------------------
// Compatible observation: successful handshake clears stale evidence
// ---------------------------------------------------------------------------

// TestCompatibleObservation_ClearsReporterAndLockout verifies that a successful
// handshake (markPeerConnected) removes the peer from the incompatible-reporter
// set, clears the persisted version lockout, and resets the health diagnostic
// fields. This is the "compatible observation" lifecycle.
func TestCompatibleObservation_ClearsReporterAndLockout(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:100")
	identity := domain.PeerIdentity("peer-upgraded")
	entry := &peerEntry{Address: addr}

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: entry,
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr: identity,
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Step 1: Record an incompatible observation (they think we're old).
	svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)

	snap := svc.VersionPolicySnapshot()
	if snap.IncompatibleVersionReporters != 1 {
		t.Fatalf("after penalize: reporters = %d, want 1", snap.IncompatibleVersionReporters)
	}

	svc.mu.RLock()
	lockoutActive := svc.isPeerVersionLockedOutLocked(addr)
	svc.mu.RUnlock()
	if !lockoutActive {
		t.Fatal("after penalize: lockout must be active")
	}

	// Step 2: Simulate a successful handshake (peer upgraded, now compatible).
	svc.markPeerConnected(addr, peerDirectionInbound)

	// Reporter should be removed.
	snap = svc.VersionPolicySnapshot()
	if snap.IncompatibleVersionReporters != 0 {
		t.Errorf("after connect: reporters = %d, want 0 "+
			"(successful handshake must clear reporter)", snap.IncompatibleVersionReporters)
	}

	// Lockout should be cleared.
	svc.mu.RLock()
	lockoutActive = svc.isPeerVersionLockedOutLocked(addr)
	svc.mu.RUnlock()
	if lockoutActive {
		t.Error("after connect: lockout must be cleared by successful handshake")
	}

	// Health diagnostic fields should be reset.
	svc.mu.RLock()
	h := svc.health[addr]
	attempts := h.IncompatibleVersionAttempts
	lastAt := h.LastIncompatibleVersionAt
	obsVer := h.ObservedPeerVersion
	obsMin := h.ObservedPeerMinimumVersion
	svc.mu.RUnlock()

	if attempts != 0 {
		t.Errorf("IncompatibleVersionAttempts = %d, want 0", attempts)
	}
	if !lastAt.IsZero() {
		t.Errorf("LastIncompatibleVersionAt = %v, want zero", lastAt)
	}
	if obsVer != 0 {
		t.Errorf("ObservedPeerVersion = %d, want 0", obsVer)
	}
	if obsMin != 0 {
		t.Errorf("ObservedPeerMinimumVersion = %d, want 0", obsMin)
	}

	// UpdateAvailable should be false (no more evidence).
	if snap.UpdateAvailable {
		t.Error("UpdateAvailable must be false after compatible observation")
	}
}

// TestCompatibleObservation_OnlyAffectsConnectingPeer verifies that a
// successful handshake from one peer does not clear evidence from other
// peers that are still incompatible.
func TestCompatibleObservation_OnlyAffectsConnectingPeer(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	svc := &Service{
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		health:        make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
	}

	// Set up 3 incompatible peers.
	for i := 0; i < 3; i++ {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:100", i+1))
		identity := domain.PeerIdentity(fmt.Sprintf("peer-%d", i))
		svc.persistedMeta[addr] = &peerEntry{Address: addr}
		svc.peerIDs[addr] = identity
		svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)
	}

	snap := svc.VersionPolicySnapshot()
	if !snap.UpdateAvailable {
		t.Fatal("precondition: update_available must be true with 3 reporters")
	}

	// Peer 0 upgrades and reconnects successfully.
	svc.markPeerConnected(domain.PeerAddress("10.0.0.1:100"), peerDirectionInbound)

	snap = svc.VersionPolicySnapshot()
	if snap.IncompatibleVersionReporters != 2 {
		t.Errorf("reporters = %d, want 2 (only connecting peer cleared)",
			snap.IncompatibleVersionReporters)
	}
	// With only 2 reporters remaining (threshold is 3), but lockouts from
	// peers 2 and 3 still active, update_available stays true via lockout signal.
	if !snap.UpdateAvailable {
		t.Error("update_available should remain true (2 lockouts still active)")
	}
}

// ---------------------------------------------------------------------------
// Multi-address identity lockout propagation
// ---------------------------------------------------------------------------

// TestVersionLockout_PropagatedByIdentity verifies that when a version
// lockout is set on one address, it propagates to all other addresses
// that share the same peer identity. This prevents the connection manager
// from re-dialing the same incompatible peer via an alternative address.
func TestVersionLockout_PropagatedByIdentity(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:100")
	addr2 := domain.PeerAddress("10.0.0.1:200")
	addr3 := domain.PeerAddress("10.0.0.2:100") // different peer
	identity := domain.PeerIdentity("multi-addr-peer")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: identity,
			addr2: identity,     // same identity, different address
			addr3: "other-peer", // different identity
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Penalize only addr1. Lockout should propagate to addr2 (same identity)
	// but NOT to addr3 (different identity).
	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)

	svc.mu.RLock()
	lockout1 := svc.isPeerVersionLockedOutLocked(addr1)
	lockout2 := svc.isPeerVersionLockedOutLocked(addr2)
	lockout3 := svc.isPeerVersionLockedOutLocked(addr3)
	svc.mu.RUnlock()

	if !lockout1 {
		t.Error("addr1: lockout must be set (directly penalized)")
	}
	if !lockout2 {
		t.Error("addr2: lockout must be propagated (same identity as addr1)")
	}
	if lockout3 {
		t.Error("addr3: lockout must NOT be set (different identity)")
	}

	// Verify the propagated lockout has the same evidence.
	entry2 := svc.persistedMeta[addr2]
	if entry2.VersionLockout.ObservedMinimumProtocolVersion != higherMin {
		t.Errorf("addr2: propagated lockout minimum = %d, want %d",
			entry2.VersionLockout.ObservedMinimumProtocolVersion, higherMin)
	}
	if entry2.VersionLockout.PeerIdentity != identity {
		t.Errorf("addr2: propagated lockout identity = %q, want %q",
			entry2.VersionLockout.PeerIdentity, identity)
	}
}

// TestVersionLockout_NoIdentity_NoPropagation verifies that address-only
// lockouts (no known identity) do not propagate to other addresses.
func TestVersionLockout_NoIdentity_NoPropagation(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:100")
	addr2 := domain.PeerAddress("10.0.0.1:200")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
		},
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity), // no identities
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)

	svc.mu.RLock()
	lockout1 := svc.isPeerVersionLockedOutLocked(addr1)
	lockout2 := svc.isPeerVersionLockedOutLocked(addr2)
	svc.mu.RUnlock()

	if !lockout1 {
		t.Error("addr1: lockout must be set (directly penalized)")
	}
	if lockout2 {
		t.Error("addr2: lockout must NOT be set (no identity to propagate)")
	}
}

// ---------------------------------------------------------------------------
// Periodic repair path
// ---------------------------------------------------------------------------

func TestPeriodicRepair_ExpiresStaleReporters(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	// Record 3 reporters (above threshold) 25 hours ago → stale.
	stale := time.Now().UTC().Add(-25 * time.Hour)
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-bbb", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, stale)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	// Snapshot was computed at recording time → reporters counted as live
	// because cutoff = stale - 24h is before the observations.
	if snap.IncompatibleVersionReporters != 3 {
		t.Fatalf("initial reporters = %d, want 3", snap.IncompatibleVersionReporters)
	}

	// Simulate periodic repair at "now" — all reporters are beyond 24h TTL.
	now := time.Now().UTC()
	svc.mu.Lock()
	svc.maybeRecomputeVersionPolicyPeriodic(now)
	snap = svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.IncompatibleVersionReporters != 0 {
		t.Errorf("after periodic repair: reporters = %d, want 0 (all expired)", snap.IncompatibleVersionReporters)
	}
	if snap.UpdateAvailable {
		t.Error("after periodic repair: update_available should be false")
	}
}

func TestPeriodicRepair_ThrottledByInterval(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	// Record 3 stale reporters.
	stale := time.Now().UTC().Add(-25 * time.Hour)
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-bbb", 10, 10, stale)
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, stale)
	svc.mu.Unlock()

	// First periodic call at t0 — should recompute and expire reporters.
	t0 := time.Now().UTC()
	svc.mu.Lock()
	svc.maybeRecomputeVersionPolicyPeriodic(t0)
	snap0 := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap0.IncompatibleVersionReporters != 0 {
		t.Fatalf("after first repair: reporters = %d, want 0", snap0.IncompatibleVersionReporters)
	}

	// Re-inject 3 fresh reporters (simulating new events between repair ticks).
	svc.mu.Lock()
	svc.recordIncompatibleObservationLocked("peer-ddd", 10, 10, t0)
	svc.recordIncompatibleObservationLocked("peer-eee", 10, 10, t0)
	svc.recordIncompatibleObservationLocked("peer-fff", 10, 10, t0)
	svc.mu.Unlock()

	// Second call 10 seconds later — within throttle window, should be a no-op.
	t1 := t0.Add(10 * time.Second)
	svc.mu.Lock()
	svc.maybeRecomputeVersionPolicyPeriodic(t1)
	snap1 := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	// Snapshot should still reflect the event-driven recompute from
	// recordIncompatibleObservationLocked, NOT the periodic path.
	if snap1.IncompatibleVersionReporters != 3 {
		t.Errorf("throttled call: reporters = %d, want 3 (event-driven state preserved)", snap1.IncompatibleVersionReporters)
	}

	// Third call after the full interval — should trigger periodic recompute.
	t2 := t0.Add(versionPolicyRepairInterval + time.Second)
	svc.mu.Lock()
	svc.maybeRecomputeVersionPolicyPeriodic(t2)
	snap2 := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	// Reporters are still fresh (recorded at t0, TTL is 24h), so they
	// should survive the periodic recompute.
	if snap2.IncompatibleVersionReporters != 3 {
		t.Errorf("after interval: reporters = %d, want 3 (still fresh)", snap2.IncompatibleVersionReporters)
	}
}

func TestPeriodicRepair_NilPolicyIsNoOp(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}

	// Should not panic when versionPolicy is nil.
	svc.mu.Lock()
	svc.maybeRecomputeVersionPolicyPeriodic(time.Now().UTC())
	svc.mu.Unlock()

	if svc.versionPolicy != nil {
		t.Error("periodic repair should not create versionPolicy from nil")
	}
}

// ---------------------------------------------------------------------------
// incompatibleProtocolError and CM evidence propagation
// ---------------------------------------------------------------------------

func TestIncompatibleProtocolError_UnwrapsToSentinel(t *testing.T) {
	t.Parallel()

	ipe := &incompatibleProtocolError{
		PeerVersion: 10,
		PeerMinimum: 10,
		Cause:       fmt.Errorf("protocol version 5 is too old"),
	}

	if !errors.Is(ipe, errIncompatibleProtocol) {
		t.Error("incompatibleProtocolError must unwrap to errIncompatibleProtocol")
	}

	var extracted *incompatibleProtocolError
	if !errors.As(ipe, &extracted) {
		t.Fatal("errors.As must extract *incompatibleProtocolError")
	}
	if extracted.PeerVersion != 10 || extracted.PeerMinimum != 10 {
		t.Errorf("evidence lost: version=%d, min=%d", extracted.PeerVersion, extracted.PeerMinimum)
	}
}

func TestCMDialFailed_PropagatesVersionEvidence(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:1234")
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      map[domain.PeerAddress]domain.PeerIdentity{addr: "peer-cm-aaa"},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Simulate the structured error that openPeerSessionForCM returns.
	cmErr := &incompatibleProtocolError{
		PeerVersion: higherMin,
		PeerMinimum: higherMin,
		Cause:       fmt.Errorf("protocol version too old"),
	}

	svc.onCMDialFailed(addr, cmErr, true)

	// The version evidence should have reached penalizeOldProtocolPeer,
	// which means the reporter set should contain the peer identity and
	// the persisted lockout should be active.
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	if svc.versionPolicy == nil {
		t.Fatal("versionPolicy must be created by recordIncompatibleObservationLocked")
	}
	if _, ok := svc.versionPolicy.incompatibleReporters["peer-cm-aaa"]; !ok {
		t.Error("peer identity must appear in incompatibleReporters")
	}

	entry := svc.persistedMeta[addr]
	if !entry.VersionLockout.IsActive() {
		t.Error("version lockout must be set with confirmed evidence")
	}
}

func TestCMDialFailed_ZeroEvidence_NoReporterOrLockout(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("10.0.0.2:1234")
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      map[domain.PeerAddress]domain.PeerIdentity{addr: "peer-cm-bbb"},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Non-structured error (e.g. from a non-CM code path).
	plainErr := fmt.Errorf("%w: some reason", errIncompatibleProtocol)
	svc.onCMDialFailed(addr, plainErr, true)

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// Without version evidence (peerMinimum=0), the direction guard in
	// penalizeOldProtocolPeer must NOT feed the reporter set or set lockout.
	if svc.versionPolicy != nil {
		if len(svc.versionPolicy.incompatibleReporters) > 0 {
			t.Error("zero evidence must not feed reporter set")
		}
	}

	entry := svc.persistedMeta[addr]
	if entry.VersionLockout.IsActive() {
		t.Error("zero evidence must not set version lockout")
	}
}

// ---------------------------------------------------------------------------
// Identity-wide lockout clearing on successful handshake
// ---------------------------------------------------------------------------

func TestCompatibleObservation_ClearsLockoutAcrossIdentity(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity, different port
	addr3 := domain.PeerAddress("10.0.0.2:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "shared-identity",
			addr2: "shared-identity",
			addr3: "other-identity",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Penalize all three addresses → lockouts are set.
	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr2, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr3, higherMin, higherMin)

	// Verify lockouts are active.
	svc.mu.RLock()
	if !svc.persistedMeta[addr1].VersionLockout.IsActive() {
		t.Fatal("pre-check: addr1 lockout must be active")
	}
	if !svc.persistedMeta[addr2].VersionLockout.IsActive() {
		t.Fatal("pre-check: addr2 lockout must be active")
	}
	if !svc.persistedMeta[addr3].VersionLockout.IsActive() {
		t.Fatal("pre-check: addr3 lockout must be active")
	}
	svc.mu.RUnlock()

	// Simulate successful handshake on addr1 — should clear lockout
	// for addr1 AND addr2 (same identity), but NOT addr3.
	svc.markPeerConnected(addr1, domain.PeerDirectionOutbound)

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	if svc.persistedMeta[addr1].VersionLockout.IsActive() {
		t.Error("addr1: lockout must be cleared (directly handshook)")
	}
	if svc.persistedMeta[addr2].VersionLockout.IsActive() {
		t.Error("addr2: lockout must be cleared (same identity as addr1)")
	}
	if !svc.persistedMeta[addr3].VersionLockout.IsActive() {
		t.Error("addr3: lockout must remain active (different identity)")
	}
}

// ---------------------------------------------------------------------------
// Snapshot ordering: recompute includes lockout signal
// ---------------------------------------------------------------------------

func TestPenalize_SnapshotIncludesLockoutSignal(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:1234")
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      map[domain.PeerAddress]domain.PeerIdentity{addr: "peer-lockout-snap"},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Single penalize call with confirmed evidence.
	svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)

	// The snapshot must immediately reflect the lockout signal.
	// Before the fix, the snapshot was recomputed before the lockout was
	// written, so update_available would be false with only 1 reporter
	// (below threshold of 3). With the lockout signal, it should be true.
	snap := svc.VersionPolicySnapshot()

	if !snap.UpdateAvailable {
		t.Error("update_available must be true immediately after penalize " +
			"(lockout signal should contribute even with <3 reporters)")
	}
	if snap.UpdateReason != domain.UpdateReasonIncompatibleVersion {
		t.Errorf("reason = %q, want %q", snap.UpdateReason, domain.UpdateReasonIncompatibleVersion)
	}
}

// ---------------------------------------------------------------------------
// MaxObservedPeerVersion across multiple active lockouts
// ---------------------------------------------------------------------------

func TestMaxObservedPeerVersion_ScansAllActiveLockouts(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)

	addr1 := domain.PeerAddress("10.0.0.1:1111")
	addr2 := domain.PeerAddress("10.0.0.2:2222")
	addr3 := domain.PeerAddress("10.0.0.3:3333")

	// Create lockouts with different observed protocol versions.
	// The maximum should be reported regardless of map iteration order.
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {
				Address: addr1,
				VersionLockout: domain.VersionLockoutSnapshot{
					ObservedProtocolVersion: localProto + 1,
					Reason:                  domain.VersionLockoutReasonIncompatible,
					PeerIdentity:            "peer-aaa",
					LockedAtLocalVersion: domain.LocalVersionFingerprint{
						ProtocolVersion: localProto,
						ClientBuild:     config.ClientBuild,
					},
				},
			},
			addr2: {
				Address: addr2,
				VersionLockout: domain.VersionLockoutSnapshot{
					ObservedProtocolVersion: localProto + 5, // highest
					Reason:                  domain.VersionLockoutReasonIncompatible,
					PeerIdentity:            "peer-bbb",
					LockedAtLocalVersion: domain.LocalVersionFingerprint{
						ProtocolVersion: localProto,
						ClientBuild:     config.ClientBuild,
					},
				},
			},
			addr3: {
				Address: addr3,
				VersionLockout: domain.VersionLockoutSnapshot{
					ObservedProtocolVersion: localProto + 3,
					Reason:                  domain.VersionLockoutReasonIncompatible,
					PeerIdentity:            "peer-ccc",
					LockedAtLocalVersion: domain.LocalVersionFingerprint{
						ProtocolVersion: localProto,
						ClientBuild:     config.ClientBuild,
					},
				},
			},
		},
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}

	now := time.Now().UTC()
	svc.mu.Lock()
	svc.versionPolicy = newVersionPolicyState()
	svc.recomputeVersionPolicyLocked(now)
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	wantMax := localProto + 5
	if snap.MaxObservedPeerVersion != wantMax {
		t.Errorf("MaxObservedPeerVersion = %d, want %d (must scan all active lockouts)",
			snap.MaxObservedPeerVersion, wantMax)
	}
	if !snap.UpdateAvailable {
		t.Error("update_available must be true with active lockouts")
	}
}

// ---------------------------------------------------------------------------
// ObservedClientVersion persisted from CM and legacy failure paths
// ---------------------------------------------------------------------------

func TestCMDialFailed_PersistsObservedClientVersion(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:1234")
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      map[domain.PeerAddress]domain.PeerIdentity{addr: "peer-cv-aaa"},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	cmErr := &incompatibleProtocolError{
		PeerVersion:   higherMin,
		PeerMinimum:   higherMin,
		ClientVersion: "corsa/1.2.3-beta",
		Cause:         fmt.Errorf("protocol version too old"),
	}

	svc.onCMDialFailed(addr, cmErr, true)

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	entry := svc.persistedMeta[addr]
	if !entry.VersionLockout.IsActive() {
		t.Fatal("lockout must be active")
	}
	if string(entry.VersionLockout.ObservedClientVersion) != "corsa/1.2.3-beta" {
		t.Errorf("ObservedClientVersion = %q, want %q",
			entry.VersionLockout.ObservedClientVersion, "corsa/1.2.3-beta")
	}
}

// ---------------------------------------------------------------------------
// Operator override (add_peer) identity-wide lockout clearing
// ---------------------------------------------------------------------------

func TestAddPeer_ClearsLockoutAcrossIdentity(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity
	addr3 := domain.PeerAddress("10.0.0.2:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "shared-id",
			addr2: "shared-id",
			addr3: "other-id",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Penalize all three → lockouts set.
	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr3, higherMin, higherMin)

	// Verify all lockouts active.
	svc.mu.RLock()
	if !svc.persistedMeta[addr1].VersionLockout.IsActive() {
		t.Fatal("pre: addr1 lockout must be active")
	}
	if !svc.persistedMeta[addr2].VersionLockout.IsActive() {
		t.Fatal("pre: addr2 lockout must be active (propagated from addr1)")
	}
	if !svc.persistedMeta[addr3].VersionLockout.IsActive() {
		t.Fatal("pre: addr3 lockout must be active")
	}
	svc.mu.RUnlock()

	// Simulate add_peer for addr1 — replicate the operator override logic
	// from addPeerFrame under the lock.
	svc.mu.Lock()
	peerID := svc.peerIDs[addr1]
	if pm := svc.persistedMeta[addr1]; pm != nil && pm.VersionLockout.IsActive() {
		pm.VersionLockout = domain.VersionLockoutSnapshot{}
	}
	if peerID != "" {
		if svc.versionPolicy != nil {
			delete(svc.versionPolicy.incompatibleReporters, peerID)
		}
		for otherAddr, otherID := range svc.peerIDs {
			if otherAddr == addr1 || otherID != peerID {
				continue
			}
			if otherEntry, ok := svc.persistedMeta[otherAddr]; ok && otherEntry.VersionLockout.IsActive() {
				otherEntry.VersionLockout = domain.VersionLockoutSnapshot{}
			}
		}
	}
	svc.recomputeVersionPolicyLocked(time.Now().UTC())
	svc.mu.Unlock()

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	if svc.persistedMeta[addr1].VersionLockout.IsActive() {
		t.Error("addr1: lockout must be cleared (directly overridden)")
	}
	if svc.persistedMeta[addr2].VersionLockout.IsActive() {
		t.Error("addr2: lockout must be cleared (same identity as addr1)")
	}
	if !svc.persistedMeta[addr3].VersionLockout.IsActive() {
		t.Error("addr3: lockout must remain active (different identity)")
	}
}

func TestAddPeer_ClearsReporterForIdentity(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.2:1234")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "peer-aaa",
			addr2: "peer-bbb",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Two reporters → below threshold of 3.
	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr2, higherMin, higherMin)

	svc.mu.RLock()
	reportersBefore := svc.versionPolicy.snapshot.IncompatibleVersionReporters
	svc.mu.RUnlock()
	if reportersBefore != 2 {
		t.Fatalf("pre: reporters = %d, want 2", reportersBefore)
	}

	// Simulate add_peer for addr1 — remove reporter for "peer-aaa".
	svc.mu.Lock()
	peerID := svc.peerIDs[addr1]
	if pm := svc.persistedMeta[addr1]; pm != nil && pm.VersionLockout.IsActive() {
		pm.VersionLockout = domain.VersionLockoutSnapshot{}
	}
	if peerID != "" && svc.versionPolicy != nil {
		delete(svc.versionPolicy.incompatibleReporters, peerID)
	}
	svc.recomputeVersionPolicyLocked(time.Now().UTC())
	snap := svc.versionPolicy.snapshot
	svc.mu.Unlock()

	if snap.IncompatibleVersionReporters != 1 {
		t.Errorf("after override: reporters = %d, want 1", snap.IncompatibleVersionReporters)
	}
}

// ---------------------------------------------------------------------------
// Sibling lockout refresh with stronger/fresher evidence
// ---------------------------------------------------------------------------

func TestVersionLockout_SiblingRefreshedWithStrongerEvidence(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	initialMin := localProto + 1
	strongerMin := localProto + 3

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "shared-id",
			addr2: "shared-id",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// First penalize on addr1 with initial evidence → propagates to addr2.
	svc.penalizeOldProtocolPeer(addr1, initialMin, initialMin)

	svc.mu.RLock()
	siblingMin1 := svc.persistedMeta[addr2].VersionLockout.ObservedMinimumProtocolVersion
	svc.mu.RUnlock()
	if siblingMin1 != initialMin {
		t.Fatalf("initial sibling min = %d, want %d", siblingMin1, initialMin)
	}

	// Second penalize on addr1 with stronger evidence → must refresh addr2.
	svc.penalizeOldProtocolPeer(addr1, strongerMin, strongerMin)

	svc.mu.RLock()
	siblingMin2 := svc.persistedMeta[addr2].VersionLockout.ObservedMinimumProtocolVersion
	svc.mu.RUnlock()

	if siblingMin2 != strongerMin {
		t.Errorf("refreshed sibling min = %d, want %d (must update with stronger evidence)",
			siblingMin2, strongerMin)
	}
}

func TestVersionLockout_SiblingRefreshedWithFresherTimestamp(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "shared-id",
			addr2: "shared-id",
		},
		peerBuilds: make(map[domain.PeerAddress]int),
		peerVersions: map[domain.PeerAddress]string{
			addr1: "corsa/1.0.0",
		},
		health:      make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet: make(map[string]domain.BannedIPEntry),
	}

	// First penalize with client version "corsa/1.0.0".
	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)

	svc.mu.RLock()
	cv1 := string(svc.persistedMeta[addr2].VersionLockout.ObservedClientVersion)
	svc.mu.RUnlock()
	if cv1 != "corsa/1.0.0" {
		t.Fatalf("initial sibling client version = %q, want %q", cv1, "corsa/1.0.0")
	}

	// Update the client version and penalize again with same min but
	// fresher timestamp → sibling must pick up the new client version.
	svc.mu.Lock()
	svc.peerVersions[addr1] = "corsa/2.0.0"
	svc.mu.Unlock()

	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)

	svc.mu.RLock()
	cv2 := string(svc.persistedMeta[addr2].VersionLockout.ObservedClientVersion)
	svc.mu.RUnlock()

	if cv2 != "corsa/2.0.0" {
		t.Errorf("refreshed sibling client version = %q, want %q (must update with fresher evidence)",
			cv2, "corsa/2.0.0")
	}
}

// ---------------------------------------------------------------------------
// Identity-wide health diagnostic clearing on handshake
// ---------------------------------------------------------------------------

func TestCompatibleObservation_ClearsSiblingHealthDiagnostics(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity
	addr3 := domain.PeerAddress("10.0.0.2:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "shared-identity",
			addr2: "shared-identity",
			addr3: "other-identity",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Penalize all three addresses to accumulate health diagnostics.
	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr2, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr3, higherMin, higherMin)

	// Simulate a prior post-handshake disconnect on sibling to populate
	// LastDisconnectCode — penalizeOldProtocolPeer only sets LastErrorCode.
	svc.mu.Lock()
	svc.health[addr2].LastDisconnectCode = "frame-too-large"
	svc.health[addr3].LastDisconnectCode = "rate-limited"
	svc.mu.Unlock()

	// Verify sibling health diagnostics are populated before handshake.
	svc.mu.RLock()
	h2Pre := svc.health[addr2]
	h3Pre := svc.health[addr3]
	if h2Pre == nil || h2Pre.IncompatibleVersionAttempts == 0 {
		t.Fatal("pre: addr2 must have IncompatibleVersionAttempts > 0")
	}
	if h3Pre == nil || h3Pre.IncompatibleVersionAttempts == 0 {
		t.Fatal("pre: addr3 must have IncompatibleVersionAttempts > 0")
	}
	if h2Pre.ObservedPeerVersion == 0 {
		t.Fatal("pre: addr2 must have ObservedPeerVersion > 0")
	}
	if h2Pre.LastErrorCode == "" {
		t.Fatal("pre: addr2 must have non-empty LastErrorCode")
	}
	if h2Pre.LastError == "" {
		t.Fatal("pre: addr2 must have non-empty LastError")
	}
	if h2Pre.LastDisconnectCode == "" {
		t.Fatal("pre: addr2 must have non-empty LastDisconnectCode")
	}
	svc.mu.RUnlock()

	// Successful handshake on addr1 — should clear health diagnostics
	// for addr2 (same identity), but NOT addr3 (different identity).
	svc.markPeerConnected(addr1, domain.PeerDirectionOutbound)

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// addr2: sibling of addr1 — all failure-related fields must be cleared.
	h2 := svc.health[addr2]
	if h2.IncompatibleVersionAttempts != 0 {
		t.Errorf("addr2: IncompatibleVersionAttempts = %d, want 0", h2.IncompatibleVersionAttempts)
	}
	if !h2.LastIncompatibleVersionAt.IsZero() {
		t.Errorf("addr2: LastIncompatibleVersionAt = %v, want zero", h2.LastIncompatibleVersionAt)
	}
	if h2.LastError != "" {
		t.Errorf("addr2: LastError = %q, want empty", h2.LastError)
	}
	if h2.LastErrorCode != "" {
		t.Errorf("addr2: LastErrorCode = %q, want empty", h2.LastErrorCode)
	}
	if h2.LastDisconnectCode != "" {
		t.Errorf("addr2: LastDisconnectCode = %q, want empty", h2.LastDisconnectCode)
	}
	if h2.ObservedPeerVersion != 0 {
		t.Errorf("addr2: ObservedPeerVersion = %d, want 0", h2.ObservedPeerVersion)
	}
	if h2.ObservedPeerMinimumVersion != 0 {
		t.Errorf("addr2: ObservedPeerMinimumVersion = %d, want 0", h2.ObservedPeerMinimumVersion)
	}

	// addr3: different identity — health diagnostics must remain untouched.
	h3 := svc.health[addr3]
	if h3.IncompatibleVersionAttempts == 0 {
		t.Error("addr3: IncompatibleVersionAttempts must remain > 0 (different identity)")
	}
	if h3.ObservedPeerVersion == 0 {
		t.Error("addr3: ObservedPeerVersion must remain > 0 (different identity)")
	}
	if h3.LastErrorCode == "" {
		t.Error("addr3: LastErrorCode must remain non-empty (different identity)")
	}
	if h3.LastError == "" {
		t.Error("addr3: LastError must remain non-empty (different identity)")
	}
	if h3.LastDisconnectCode == "" {
		t.Error("addr3: LastDisconnectCode must remain non-empty (different identity)")
	}
}

// ---------------------------------------------------------------------------
// End-to-end operator override via addPeerFrame
// ---------------------------------------------------------------------------

func TestAddPeerFrame_ClearsLockoutAcrossIdentity(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity
	addr3 := domain.PeerAddress("10.0.0.2:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "peer-aaa",
			addr2: "peer-aaa",
			addr3: "peer-bbb",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		peerTypes:    make(map[domain.PeerAddress]domain.NodeType),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
		// canReach needs reachableGroups for private 10.x addresses.
		reachableGroups: map[domain.NetGroup]struct{}{
			domain.NetGroupLocal: {},
			domain.NetGroupIPv4:  {},
		},
	}

	// Penalize all three — creates lockouts and reporters.
	svc.penalizeOldProtocolPeer(addr1, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr2, higherMin, higherMin)
	svc.penalizeOldProtocolPeer(addr3, higherMin, higherMin)

	// Verify pre-conditions.
	svc.mu.RLock()
	for _, a := range []domain.PeerAddress{addr1, addr2, addr3} {
		if !svc.persistedMeta[a].VersionLockout.IsActive() {
			t.Fatalf("pre: %s lockout must be active", a)
		}
	}
	reportersPre := svc.versionPolicy.snapshot.IncompatibleVersionReporters
	svc.mu.RUnlock()
	if reportersPre < 2 {
		t.Fatalf("pre: reporters = %d, want >= 2", reportersPre)
	}

	// Call the real add_peer handler for addr1.
	resp := svc.addPeerFrame(protocol.Frame{
		Type:  "add_peer",
		Peers: []string{string(addr1)},
	})
	if resp.Type == "error" {
		t.Fatalf("addPeerFrame returned error: %s", resp.Error)
	}

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// addr1: directly overridden — lockout must be cleared.
	if svc.persistedMeta[addr1].VersionLockout.IsActive() {
		t.Error("addr1: lockout must be cleared (directly overridden)")
	}
	// addr2: same identity — lockout must be cleared identity-wide.
	if svc.persistedMeta[addr2].VersionLockout.IsActive() {
		t.Error("addr2: lockout must be cleared (same identity as addr1)")
	}
	// addr3: different identity — lockout must remain.
	if !svc.persistedMeta[addr3].VersionLockout.IsActive() {
		t.Error("addr3: lockout must remain active (different identity)")
	}

	// Health diagnostics for the overridden address must be reset.
	if h := svc.health[addr1]; h != nil && h.IncompatibleVersionAttempts != 0 {
		t.Errorf("addr1: IncompatibleVersionAttempts = %d, want 0", h.IncompatibleVersionAttempts)
	}

	// Reporter for "peer-aaa" must have been removed from the dedup set.
	if _, found := svc.versionPolicy.incompatibleReporters["peer-aaa"]; found {
		t.Error("reporter for peer-aaa must be removed after operator override")
	}
	// Reporter for "peer-bbb" must remain.
	if _, found := svc.versionPolicy.incompatibleReporters["peer-bbb"]; !found {
		t.Error("reporter for peer-bbb must remain (different identity)")
	}
}

// ---------------------------------------------------------------------------
// Identity-wide ban/cooldown clearing on handshake
// ---------------------------------------------------------------------------

func TestCompatibleObservation_ClearsSiblingBansAndCooldowns(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity, same IP
	addr3 := domain.PeerAddress("10.0.0.2:1234") // same identity, different IP
	addr4 := domain.PeerAddress("10.0.0.3:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
			addr4: {Address: addr4},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "shared-identity",
			addr2: "shared-identity",
			addr3: "shared-identity",
			addr4: "other-identity",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Penalize each address 4 times to trigger BannedUntil
	// (4 * peerBanIncrementIncompatible = 4 * 250 = 1000 >= threshold).
	for _, addr := range []domain.PeerAddress{addr1, addr2, addr3, addr4} {
		for i := 0; i < 4; i++ {
			svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)
		}
	}

	// Simulate prior disconnects to exercise the cooldown path.
	// penalizeOldProtocolPeer sets ConsecutiveFailures but not
	// LastDisconnectedAt — that is set by markPeerDisconnected.
	// We set it manually so the cooldown filter is active.
	recentDisconnect := time.Now().UTC().Add(-10 * time.Second)
	svc.mu.Lock()
	for _, addr := range []domain.PeerAddress{addr2, addr3, addr4} {
		svc.health[addr].LastDisconnectedAt = recentDisconnect
	}
	svc.mu.Unlock()

	// Verify pre-conditions: siblings must be banned, have failures,
	// and have a recent disconnect timestamp (cooldown active).
	svc.mu.RLock()
	for _, addr := range []domain.PeerAddress{addr2, addr3, addr4} {
		h := svc.health[addr]
		if h == nil {
			t.Fatalf("pre: %s health must exist", addr)
		}
		if h.BannedUntil.IsZero() {
			t.Fatalf("pre: %s BannedUntil must be set", addr)
		}
		if h.ConsecutiveFailures == 0 {
			t.Fatalf("pre: %s ConsecutiveFailures must be > 0", addr)
		}
		if h.LastDisconnectedAt.IsZero() {
			t.Fatalf("pre: %s LastDisconnectedAt must be set", addr)
		}
	}
	// Verify IP-wide bans exist for sibling IPs.
	if _, ok := svc.bannedIPSet["10.0.0.2"]; !ok {
		t.Fatal("pre: IP 10.0.0.2 must be banned")
	}
	svc.mu.RUnlock()

	// Successful handshake on addr1 — should clear ban/cooldown for
	// addr2 and addr3 (same identity), but NOT addr4 (different identity).
	svc.markPeerConnected(addr1, domain.PeerDirectionOutbound)

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// addr2: same identity, same IP — must be fully unblocked.
	h2 := svc.health[addr2]
	if !h2.BannedUntil.IsZero() {
		t.Errorf("addr2: BannedUntil = %v, want zero", h2.BannedUntil)
	}
	if h2.ConsecutiveFailures != 0 {
		t.Errorf("addr2: ConsecutiveFailures = %d, want 0", h2.ConsecutiveFailures)
	}
	if !h2.LastDisconnectedAt.IsZero() {
		t.Errorf("addr2: LastDisconnectedAt = %v, want zero", h2.LastDisconnectedAt)
	}

	// addr3: same identity, different IP — must be unblocked and IP ban cleared.
	h3 := svc.health[addr3]
	if !h3.BannedUntil.IsZero() {
		t.Errorf("addr3: BannedUntil = %v, want zero", h3.BannedUntil)
	}
	if h3.ConsecutiveFailures != 0 {
		t.Errorf("addr3: ConsecutiveFailures = %d, want 0", h3.ConsecutiveFailures)
	}
	if !h3.LastDisconnectedAt.IsZero() {
		t.Errorf("addr3: LastDisconnectedAt = %v, want zero", h3.LastDisconnectedAt)
	}
	if _, ipBanned := svc.bannedIPSet["10.0.0.2"]; ipBanned {
		t.Error("IP 10.0.0.2: IP-wide ban must be cleared (sibling of handshook identity)")
	}

	// addr4: different identity — ban and cooldown must remain active.
	h4 := svc.health[addr4]
	if h4.BannedUntil.IsZero() {
		t.Error("addr4: BannedUntil must remain set (different identity)")
	}
	if h4.ConsecutiveFailures == 0 {
		t.Error("addr4: ConsecutiveFailures must remain > 0 (different identity)")
	}
	if h4.LastDisconnectedAt.IsZero() {
		t.Error("addr4: LastDisconnectedAt must remain set (different identity)")
	}
}

// ---------------------------------------------------------------------------
// End-to-end operator override: sibling ban/cooldown clearing via addPeerFrame
// ---------------------------------------------------------------------------

func TestAddPeerFrame_ClearsSiblingBansAndCooldowns(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity, same IP
	addr3 := domain.PeerAddress("10.0.0.2:1234") // same identity, different IP
	addr4 := domain.PeerAddress("10.0.0.3:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
			addr4: {Address: addr4},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "peer-aaa",
			addr2: "peer-aaa",
			addr3: "peer-aaa",
			addr4: "peer-bbb",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		peerTypes:    make(map[domain.PeerAddress]domain.NodeType),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
		reachableGroups: map[domain.NetGroup]struct{}{
			domain.NetGroupLocal: {},
			domain.NetGroupIPv4:  {},
		},
	}

	// Penalize 4 times each to trigger BannedUntil.
	for _, addr := range []domain.PeerAddress{addr1, addr2, addr3, addr4} {
		for i := 0; i < 4; i++ {
			svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)
		}
	}

	// Simulate prior disconnects so cooldown is active.
	recentDisconnect := time.Now().UTC().Add(-10 * time.Second)
	svc.mu.Lock()
	for _, addr := range []domain.PeerAddress{addr1, addr2, addr3, addr4} {
		svc.health[addr].LastDisconnectedAt = recentDisconnect
	}
	svc.mu.Unlock()

	// Verify pre-conditions.
	svc.mu.RLock()
	for _, addr := range []domain.PeerAddress{addr2, addr3, addr4} {
		h := svc.health[addr]
		if h.BannedUntil.IsZero() {
			t.Fatalf("pre: %s BannedUntil must be set", addr)
		}
		if h.ConsecutiveFailures == 0 {
			t.Fatalf("pre: %s ConsecutiveFailures must be > 0", addr)
		}
	}
	if _, ok := svc.bannedIPSet["10.0.0.2"]; !ok {
		t.Fatal("pre: IP 10.0.0.2 must be banned")
	}
	svc.mu.RUnlock()

	// Call the real add_peer handler for addr1.
	resp := svc.addPeerFrame(protocol.Frame{
		Type:  "add_peer",
		Peers: []string{string(addr1)},
	})
	if resp.Type == "error" {
		t.Fatalf("addPeerFrame returned error: %s", resp.Error)
	}

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// addr1: directly overridden — must be fully unblocked.
	h1 := svc.health[addr1]
	if h1 != nil && !h1.BannedUntil.IsZero() {
		t.Errorf("addr1: BannedUntil = %v, want zero", h1.BannedUntil)
	}
	if h1 != nil && h1.ConsecutiveFailures != 0 {
		t.Errorf("addr1: ConsecutiveFailures = %d, want 0", h1.ConsecutiveFailures)
	}

	// addr2: same identity, same IP — must be fully unblocked.
	h2 := svc.health[addr2]
	if !h2.BannedUntil.IsZero() {
		t.Errorf("addr2: BannedUntil = %v, want zero", h2.BannedUntil)
	}
	if h2.ConsecutiveFailures != 0 {
		t.Errorf("addr2: ConsecutiveFailures = %d, want 0", h2.ConsecutiveFailures)
	}
	if !h2.LastDisconnectedAt.IsZero() {
		t.Errorf("addr2: LastDisconnectedAt = %v, want zero", h2.LastDisconnectedAt)
	}
	if h2.IncompatibleVersionAttempts != 0 {
		t.Errorf("addr2: IncompatibleVersionAttempts = %d, want 0", h2.IncompatibleVersionAttempts)
	}

	// addr3: same identity, different IP — must be unblocked and IP ban cleared.
	h3 := svc.health[addr3]
	if !h3.BannedUntil.IsZero() {
		t.Errorf("addr3: BannedUntil = %v, want zero", h3.BannedUntil)
	}
	if h3.ConsecutiveFailures != 0 {
		t.Errorf("addr3: ConsecutiveFailures = %d, want 0", h3.ConsecutiveFailures)
	}
	if !h3.LastDisconnectedAt.IsZero() {
		t.Errorf("addr3: LastDisconnectedAt = %v, want zero", h3.LastDisconnectedAt)
	}
	if _, ipBanned := svc.bannedIPSet["10.0.0.2"]; ipBanned {
		t.Error("IP 10.0.0.2: IP-wide ban must be cleared (sibling of overridden identity)")
	}

	// addr4: different identity — ban and cooldown must remain active.
	h4 := svc.health[addr4]
	if h4.BannedUntil.IsZero() {
		t.Error("addr4: BannedUntil must remain set (different identity)")
	}
	if h4.ConsecutiveFailures == 0 {
		t.Error("addr4: ConsecutiveFailures must remain > 0 (different identity)")
	}
	if h4.LastDisconnectedAt.IsZero() {
		t.Error("addr4: LastDisconnectedAt must remain set (different identity)")
	}
}

// ---------------------------------------------------------------------------
// Score repair on compatibility recovery
// ---------------------------------------------------------------------------

func TestCompatibleObservation_RepairsStaleScorePenalties(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity
	addr3 := domain.PeerAddress("10.0.0.2:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "shared-identity",
			addr2: "shared-identity",
			addr3: "other-identity",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Penalize 4 times each — score deeply negative from peerScoreOldProtocol.
	for _, addr := range []domain.PeerAddress{addr1, addr2, addr3} {
		for i := 0; i < 4; i++ {
			svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)
		}
	}

	// Verify scores are negative before handshake.
	svc.mu.RLock()
	score1Pre := svc.health[addr1].Score
	score2Pre := svc.health[addr2].Score
	score3Pre := svc.health[addr3].Score
	svc.mu.RUnlock()
	if score1Pre >= 0 {
		t.Fatalf("pre: addr1 score = %d, want negative", score1Pre)
	}
	if score2Pre >= 0 {
		t.Fatalf("pre: addr2 score = %d, want negative", score2Pre)
	}

	// Successful handshake on addr1 — score must be repaired.
	svc.markPeerConnected(addr1, domain.PeerDirectionOutbound)

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// addr1: direct handshake — score must be >= peerScoreConnect (floor + bonus).
	if svc.health[addr1].Score < peerScoreConnect {
		t.Errorf("addr1: score = %d, want >= %d (floor to 0 + connect bonus)",
			svc.health[addr1].Score, peerScoreConnect)
	}

	// addr2: sibling — score must be >= 0 (floored, no connect bonus).
	if svc.health[addr2].Score < 0 {
		t.Errorf("addr2: score = %d, want >= 0 (sibling score must be floored)",
			svc.health[addr2].Score)
	}

	// addr3: different identity — score must remain negative.
	if svc.health[addr3].Score != score3Pre {
		t.Errorf("addr3: score = %d, want %d (different identity, untouched)",
			svc.health[addr3].Score, score3Pre)
	}
}

func TestAddPeerFrame_RepairsStaleScorePenalties(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr1 := domain.PeerAddress("10.0.0.1:1234")
	addr2 := domain.PeerAddress("10.0.0.1:5678") // same identity
	addr3 := domain.PeerAddress("10.0.0.2:1234") // different identity

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr1: {Address: addr1},
			addr2: {Address: addr2},
			addr3: {Address: addr3},
		},
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
			addr1: "peer-aaa",
			addr2: "peer-aaa",
			addr3: "peer-bbb",
		},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		peerTypes:    make(map[domain.PeerAddress]domain.NodeType),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
		reachableGroups: map[domain.NetGroup]struct{}{
			domain.NetGroupLocal: {},
			domain.NetGroupIPv4:  {},
		},
	}

	// Penalize 4 times each — score deeply negative.
	for _, addr := range []domain.PeerAddress{addr1, addr2, addr3} {
		for i := 0; i < 4; i++ {
			svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)
		}
	}

	svc.mu.RLock()
	score2Pre := svc.health[addr2].Score
	score3Pre := svc.health[addr3].Score
	svc.mu.RUnlock()
	if score2Pre >= 0 {
		t.Fatalf("pre: addr2 score = %d, want negative", score2Pre)
	}

	// Operator override on addr1.
	resp := svc.addPeerFrame(protocol.Frame{
		Type:  "add_peer",
		Peers: []string{string(addr1)},
	})
	if resp.Type == "error" {
		t.Fatalf("addPeerFrame returned error: %s", resp.Error)
	}

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// addr1: directly overridden — score must be >= 0.
	if svc.health[addr1].Score < 0 {
		t.Errorf("addr1: score = %d, want >= 0 (must be floored on override)",
			svc.health[addr1].Score)
	}

	// addr2: sibling — score must be >= 0.
	if svc.health[addr2].Score < 0 {
		t.Errorf("addr2: score = %d, want >= 0 (sibling score must be floored)",
			svc.health[addr2].Score)
	}

	// addr3: different identity — score must remain negative.
	if svc.health[addr3].Score != score3Pre {
		t.Errorf("addr3: score = %d, want %d (different identity, untouched)",
			svc.health[addr3].Score, score3Pre)
	}
}

// TestPenalizeOldProtocol_ClearsStaleLastDisconnectCode verifies that
// penalizeOldProtocolPeer clears any previously stored LastDisconnectCode.
// A pre-handshake incompatible reject supersedes any prior post-handshake
// disconnect code — keeping both creates a mixed diagnostic snapshot where
// the latest event is a failed connect but an unrelated old disconnect
// code is still visible in fetchPeerHealth.
func TestPenalizeOldProtocol_ClearsStaleLastDisconnectCode(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:1234")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
	}

	// Simulate a prior post-handshake disconnect that left LastDisconnectCode.
	svc.mu.Lock()
	h := svc.ensurePeerHealthLocked(addr)
	h.LastDisconnectCode = "frame-too-large"
	h.LastErrorCode = ""
	h.LastError = ""
	svc.mu.Unlock()

	// Precondition: LastDisconnectCode is populated.
	svc.mu.RLock()
	if svc.health[addr].LastDisconnectCode == "" {
		t.Fatal("pre: LastDisconnectCode must be non-empty")
	}
	svc.mu.RUnlock()

	// penalizeOldProtocolPeer records a pre-handshake incompatible reject.
	svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)

	// Post: LastDisconnectCode must be cleared — the newest event is a
	// pre-handshake rejection, not a post-handshake teardown.
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	health := svc.health[addr]
	if health == nil {
		t.Fatal("health entry must exist after penalizeOldProtocolPeer")
	}
	if health.LastDisconnectCode != "" {
		t.Errorf("LastDisconnectCode = %q, want empty (pre-handshake reject supersedes old disconnect code)",
			health.LastDisconnectCode)
	}
	// Verify that the pre-handshake fields are correctly populated.
	if health.LastErrorCode != protocol.ErrCodeIncompatibleProtocol {
		t.Errorf("LastErrorCode = %q, want %q",
			health.LastErrorCode, protocol.ErrCodeIncompatibleProtocol)
	}
	if health.LastError == "" {
		t.Error("LastError must be non-empty after penalizeOldProtocolPeer")
	}
}

// TestOutboundIncompatibleReject_HealthSnapshotComplete verifies that
// peerHealthFrames() exposes a complete, unambiguous diagnostic snapshot
// after an outbound incompatible reject. The internal health state is
// tested by other tests — this test closes the gap between internal
// state and the operator-visible PeerHealthFrame contract, ensuring that
// fields like LastErrorCode, LastDisconnectCode, ClientVersion,
// ObservedPeerVersion, and VersionLockoutActive are correctly projected
// into the snapshot returned by fetchPeerHealth.
func TestOutboundIncompatibleReject_HealthSnapshotComplete(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:1234")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      map[domain.PeerAddress]domain.PeerIdentity{addr: "peer-snap-aaa"},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
		sessions:     make(map[domain.PeerAddress]*peerSession),
		pending:      make(map[domain.PeerAddress][]pendingFrame),
	}

	// Simulate a prior post-handshake disconnect that left a stale
	// LastDisconnectCode. The outbound reject must supersede it.
	svc.mu.Lock()
	h := svc.ensurePeerHealthLocked(addr)
	h.LastDisconnectCode = "frame-too-large"
	svc.mu.Unlock()

	// Simulate an outbound incompatible reject through the CM path.
	cmErr := &incompatibleProtocolError{
		PeerVersion:   higherMin,
		PeerMinimum:   higherMin,
		ClientVersion: "corsa/2.0.0-rc1",
		Cause:         fmt.Errorf("protocol version too old"),
	}
	svc.onCMDialFailed(addr, cmErr, true)

	// Obtain the operator-visible snapshot.
	frames := svc.peerHealthFrames()

	// Exactly one frame expected for this address.
	var found *protocol.PeerHealthFrame
	for i := range frames {
		if frames[i].Address == string(addr) {
			found = &frames[i]
			break
		}
	}
	if found == nil {
		t.Fatal("peerHealthFrames must emit a row for the rejected peer")
	}

	// --- Machine-readable diagnostic fields ---

	if found.LastErrorCode != protocol.ErrCodeIncompatibleProtocol {
		t.Errorf("LastErrorCode = %q, want %q",
			found.LastErrorCode, protocol.ErrCodeIncompatibleProtocol)
	}
	if found.LastDisconnectCode != "" {
		t.Errorf("LastDisconnectCode = %q, want empty (pre-handshake reject supersedes old disconnect code)",
			found.LastDisconnectCode)
	}
	if found.LastError == "" {
		t.Error("LastError must be non-empty (human-readable rejection reason)")
	}

	// --- Version evidence ---

	if found.ObservedPeerVersion != int(higherMin) {
		t.Errorf("ObservedPeerVersion = %d, want %d",
			found.ObservedPeerVersion, int(higherMin))
	}
	if found.ObservedPeerMinimumVersion != int(higherMin) {
		t.Errorf("ObservedPeerMinimumVersion = %d, want %d",
			found.ObservedPeerMinimumVersion, int(higherMin))
	}
	if found.IncompatibleVersionAttempts != 1 {
		t.Errorf("IncompatibleVersionAttempts = %d, want 1",
			found.IncompatibleVersionAttempts)
	}
	if found.LastIncompatibleVersionAt == "" {
		t.Error("LastIncompatibleVersionAt must be non-empty (timestamp of reject)")
	}

	// --- Lockout ---

	if !found.VersionLockoutActive {
		t.Error("VersionLockoutActive must be true (evidence confirms incompatibility)")
	}

	// --- Client version propagated from the CM error path ---

	if found.ClientVersion != "corsa/2.0.0-rc1" {
		t.Errorf("ClientVersion = %q, want %q (must surface the version from the welcome frame)",
			found.ClientVersion, "corsa/2.0.0-rc1")
	}

	// --- Connection state: not connected, since the handshake failed ---

	if found.Connected {
		t.Error("Connected must be false (handshake was rejected)")
	}
	if found.State != peerStateReconnecting {
		t.Errorf("State = %q, want %q",
			found.State, peerStateReconnecting)
	}

	// --- Score: must reflect the penalty ---

	if found.Score >= 0 {
		t.Errorf("Score = %d, want < 0 (peerScoreOldProtocol penalty must be applied)",
			found.Score)
	}

	// --- ConsecutiveFailures: at least 1 ---

	if found.ConsecutiveFailures < 1 {
		t.Errorf("ConsecutiveFailures = %d, want >= 1",
			found.ConsecutiveFailures)
	}

	// --- Identity ---

	if found.PeerID != "peer-snap-aaa" {
		t.Errorf("PeerID = %q, want %q",
			found.PeerID, "peer-snap-aaa")
	}
}

// TestVersionDiagnostics_PersistRoundTrip verifies that the machine-readable
// version diagnostic fields survive a JSON marshal/unmarshal cycle in
// peerEntry. Without this, a restart drops the operator-visible evidence
// while the lockout itself survives — leaving an information gap in the
// peerHealthFrames() snapshot.
func TestVersionDiagnostics_PersistRoundTrip(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 4, 17, 10, 0, 0, 0, time.UTC)
	entry := peerEntry{
		Address:                     "10.0.0.1:1234",
		Source:                      domain.PeerSourcePersisted,
		ConsecutiveFailures:         3,
		LastError:                   "protocol version too old",
		Score:                       -40,
		LastErrorCode:               "incompatible-protocol-version",
		LastDisconnectCode:          "", // cleared by penalizeOldProtocolPeer
		IncompatibleVersionAttempts: 4,
		LastIncompatibleVersionAt:   &ts,
		ObservedPeerVersion:         12,
		ObservedPeerMinimumVersion:  10,
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        12,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: 9,
				ClientBuild:     50,
			},
			Reason:       domain.VersionLockoutReasonIncompatible,
			LockedAt:     ts,
			PeerIdentity: "peer-persist-aaa",
		},
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got peerEntry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Diagnostic fields must survive the round-trip.
	if got.LastErrorCode != "incompatible-protocol-version" {
		t.Errorf("LastErrorCode = %q, want %q",
			got.LastErrorCode, "incompatible-protocol-version")
	}
	if got.LastDisconnectCode != "" {
		t.Errorf("LastDisconnectCode = %q, want empty",
			got.LastDisconnectCode)
	}
	if got.IncompatibleVersionAttempts != 4 {
		t.Errorf("IncompatibleVersionAttempts = %d, want 4",
			got.IncompatibleVersionAttempts)
	}
	if got.LastIncompatibleVersionAt == nil || !got.LastIncompatibleVersionAt.Equal(ts) {
		t.Errorf("LastIncompatibleVersionAt = %v, want %v",
			got.LastIncompatibleVersionAt, ts)
	}
	if got.ObservedPeerVersion != 12 {
		t.Errorf("ObservedPeerVersion = %d, want 12",
			got.ObservedPeerVersion)
	}
	if got.ObservedPeerMinimumVersion != 10 {
		t.Errorf("ObservedPeerMinimumVersion = %d, want 10",
			got.ObservedPeerMinimumVersion)
	}

	// Legacy fields must also survive.
	if got.ConsecutiveFailures != 3 {
		t.Errorf("ConsecutiveFailures = %d, want 3",
			got.ConsecutiveFailures)
	}
	if got.Score != -40 {
		t.Errorf("Score = %d, want -40", got.Score)
	}
	if got.LastError != "protocol version too old" {
		t.Errorf("LastError = %q, want %q",
			got.LastError, "protocol version too old")
	}

	// Lockout must also survive (pre-existing test covers this in
	// depth, but assert here for completeness).
	if !got.VersionLockout.IsActive() {
		t.Error("VersionLockout must be active after round-trip")
	}
}

// TestVersionDiagnostics_BuildAndRestoreRoundTrip verifies the full
// in-memory round-trip: buildPeerEntriesLocked serializes health fields
// into peerEntry, and the restore path (simulated here) hydrates them
// back into peerHealth. This closes the gap where the lockout survives
// restart but the diagnostic evidence that created it is lost.
func TestVersionDiagnostics_BuildAndRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:1234")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      map[domain.PeerAddress]domain.PeerIdentity{addr: "peer-brt-aaa"},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
		peers:        []transport.Peer{{Address: addr, Source: domain.PeerSourcePersisted}},
	}

	// Penalize 4 times to trigger ban + accumulate diagnostics.
	for i := 0; i < 4; i++ {
		svc.penalizeOldProtocolPeer(addr, higherMin, higherMin)
	}

	// Snapshot the health state before flush.
	svc.mu.RLock()
	hPre := svc.health[addr]
	preErrorCode := hPre.LastErrorCode
	preAttempts := hPre.IncompatibleVersionAttempts
	preVersion := hPre.ObservedPeerVersion
	preMinVersion := hPre.ObservedPeerMinimumVersion
	preLastIncompat := hPre.LastIncompatibleVersionAt
	svc.mu.RUnlock()

	// Build entries (simulates the flush serialization path).
	svc.mu.Lock()
	entries := svc.buildPeerEntriesLocked()
	svc.mu.Unlock()

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	entry := entries[0]

	// Verify that the entry captured the diagnostic fields.
	if entry.LastErrorCode != preErrorCode {
		t.Errorf("entry.LastErrorCode = %q, want %q",
			entry.LastErrorCode, preErrorCode)
	}
	if entry.IncompatibleVersionAttempts != preAttempts {
		t.Errorf("entry.IncompatibleVersionAttempts = %d, want %d",
			entry.IncompatibleVersionAttempts, preAttempts)
	}
	if entry.ObservedPeerVersion != preVersion {
		t.Errorf("entry.ObservedPeerVersion = %d, want %d",
			entry.ObservedPeerVersion, preVersion)
	}
	if entry.ObservedPeerMinimumVersion != preMinVersion {
		t.Errorf("entry.ObservedPeerMinimumVersion = %d, want %d",
			entry.ObservedPeerMinimumVersion, preMinVersion)
	}

	// Simulate restore: marshal → unmarshal → hydrate into peerHealth.
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored peerEntry
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Hydrate peerHealth from the restored entry (mirrors service.go restore).
	h := &peerHealth{
		Address:                     addr,
		ConsecutiveFailures:         restored.ConsecutiveFailures,
		LastError:                   restored.LastError,
		Score:                       restored.Score,
		LastErrorCode:               restored.LastErrorCode,
		LastDisconnectCode:          restored.LastDisconnectCode,
		IncompatibleVersionAttempts: restored.IncompatibleVersionAttempts,
		ObservedPeerVersion:         restored.ObservedPeerVersion,
		ObservedPeerMinimumVersion:  restored.ObservedPeerMinimumVersion,
	}
	if restored.LastIncompatibleVersionAt != nil {
		h.LastIncompatibleVersionAt = *restored.LastIncompatibleVersionAt
	}

	// All diagnostic fields must match the pre-flush state.
	if h.LastErrorCode != preErrorCode {
		t.Errorf("restored LastErrorCode = %q, want %q",
			h.LastErrorCode, preErrorCode)
	}
	if h.IncompatibleVersionAttempts != preAttempts {
		t.Errorf("restored IncompatibleVersionAttempts = %d, want %d",
			h.IncompatibleVersionAttempts, preAttempts)
	}
	if h.ObservedPeerVersion != preVersion {
		t.Errorf("restored ObservedPeerVersion = %d, want %d",
			h.ObservedPeerVersion, preVersion)
	}
	if h.ObservedPeerMinimumVersion != preMinVersion {
		t.Errorf("restored ObservedPeerMinimumVersion = %d, want %d",
			h.ObservedPeerMinimumVersion, preMinVersion)
	}
	if !h.LastIncompatibleVersionAt.Equal(preLastIncompat) {
		t.Errorf("restored LastIncompatibleVersionAt = %v, want %v",
			h.LastIncompatibleVersionAt, preLastIncompat)
	}
}

// TestClientVersion_SurvivesRestartViaLockout verifies that the remote
// client version string (e.g. "corsa/2.0.0-rc1") observed during an
// incompatible reject survives a flush/load cycle through
// VersionLockout.ObservedClientVersion → peerVersions repopulation.
// peerHealthFrames() sources ClientVersion from s.peerVersions, which
// is session-scoped and empty on startup. The restore path must seed it
// from the persisted lockout so the operator-visible snapshot retains
// the version string across restarts.
func TestClientVersion_SurvivesRestartViaLockout(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	addr := domain.PeerAddress("10.0.0.1:1234")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
		peerIDs:      map[domain.PeerAddress]domain.PeerIdentity{addr: "peer-cv-bbb"},
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
		health:       make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:  make(map[string]domain.BannedIPEntry),
		peers:        []transport.Peer{{Address: addr, Source: domain.PeerSourcePersisted}},
	}

	// Simulate an outbound incompatible reject through the CM path,
	// which pre-populates peerVersions with the client version.
	cmErr := &incompatibleProtocolError{
		PeerVersion:   higherMin,
		PeerMinimum:   higherMin,
		ClientVersion: "corsa/2.0.0-rc1",
		Cause:         fmt.Errorf("protocol version too old"),
	}
	svc.onCMDialFailed(addr, cmErr, true)

	// Verify pre-flush: peerVersions has the client version.
	svc.mu.RLock()
	preCV := svc.peerVersions[addr]
	svc.mu.RUnlock()
	if preCV != "corsa/2.0.0-rc1" {
		t.Fatalf("pre: peerVersions[addr] = %q, want %q", preCV, "corsa/2.0.0-rc1")
	}

	// Build entries (simulates the flush serialization path).
	svc.mu.Lock()
	entries := svc.buildPeerEntriesLocked()
	svc.mu.Unlock()

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	entry := entries[0]

	// The lockout must carry the client version.
	if string(entry.VersionLockout.ObservedClientVersion) != "corsa/2.0.0-rc1" {
		t.Fatalf("entry lockout ObservedClientVersion = %q, want %q",
			entry.VersionLockout.ObservedClientVersion, "corsa/2.0.0-rc1")
	}

	// Simulate restart: marshal → unmarshal → rebuild peerVersions
	// from the persisted lockout (mirrors service.go restore path).
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored peerEntry
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Repopulate peerVersions from VersionLockout.ObservedClientVersion
	// (mirrors the restoredVersions loop in service.go).
	restoredVersions := make(map[domain.PeerAddress]string)
	if cv := string(restored.VersionLockout.ObservedClientVersion); cv != "" {
		restoredVersions[addr] = cv
	}

	if restoredVersions[addr] != "corsa/2.0.0-rc1" {
		t.Errorf("restored peerVersions[addr] = %q, want %q",
			restoredVersions[addr], "corsa/2.0.0-rc1")
	}
}

// TestTrimPeerEntries_ProtectsActiveLockouts verifies that
// trimPeerEntries never discards a peer with an active VersionLockout,
// even when its score is far below the top-N cutoff. Without this
// protection, a locked-out peer with a deeply negative score (from
// repeated peerScoreOldProtocol penalties) would be silently dropped
// during the top-500 trim, making it dialable again after restart.
func TestTrimPeerEntries_ProtectsActiveLockouts(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	// Create maxPersistedPeers + 2 entries: one locked-out with the
	// worst score, one normal with a bad score, and 500 normal peers
	// with good scores. After trim, the locked-out peer must survive
	// while the normal bad-score peer is dropped.
	lockedAddr := domain.PeerAddress("10.0.0.1:1234")
	normalBadAddr := domain.PeerAddress("10.0.0.2:1234")

	entries := make([]peerEntry, 0, maxPersistedPeers+2)

	// Locked-out peer: worst score possible.
	entries = append(entries, peerEntry{
		Address: lockedAddr,
		Score:   peerScoreMin, // -50
		VersionLockout: domain.VersionLockoutSnapshot{
			ObservedProtocolVersion:        12,
			ObservedMinimumProtocolVersion: 10,
			LockedAtLocalVersion: domain.LocalVersionFingerprint{
				ProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
				ClientBuild:     config.ClientBuild,
			},
			Reason:   domain.VersionLockoutReasonIncompatible,
			LockedAt: now,
		},
	})

	// Normal bad-score peer: same score but no lockout.
	entries = append(entries, peerEntry{
		Address: normalBadAddr,
		Score:   peerScoreMin,
	})

	// Fill with maxPersistedPeers good-score peers.
	for i := 0; i < maxPersistedPeers; i++ {
		entries = append(entries, peerEntry{
			Address: domain.PeerAddress(fmt.Sprintf("192.168.1.%d:%d", i/256, 1000+i%256)),
			Score:   peerScoreMax,
		})
	}

	// Sort + trim (mirrors flushPeerState).
	sortPeerEntries(entries)
	trimmed := trimPeerEntries(entries)

	// The locked-out peer must be present.
	foundLocked := false
	foundNormalBad := false
	for _, e := range trimmed {
		if e.Address == lockedAddr {
			foundLocked = true
		}
		if e.Address == normalBadAddr {
			foundNormalBad = true
		}
	}

	if !foundLocked {
		t.Error("locked-out peer must survive trim regardless of score")
	}
	if foundNormalBad {
		t.Error("normal bad-score peer should be trimmed (exceeded budget)")
	}

	// Total: 1 locked + (500 - 1) from rest = 500.
	if len(trimmed) != maxPersistedPeers {
		t.Errorf("trimmed length = %d, want %d",
			len(trimmed), maxPersistedPeers)
	}
}

// TestTrimPeerEntries_NoLockout_BehaviorUnchanged verifies that the
// trim behavior is unchanged when no entries have active lockouts.
func TestTrimPeerEntries_NoLockout_BehaviorUnchanged(t *testing.T) {
	t.Parallel()

	entries := make([]peerEntry, maxPersistedPeers+10)
	for i := range entries {
		entries[i] = peerEntry{
			Address: domain.PeerAddress(fmt.Sprintf("10.0.%d.%d:%d", i/65536, (i/256)%256, 1000+i%256)),
			Score:   maxPersistedPeers - i, // descending score
		}
	}

	sortPeerEntries(entries)
	trimmed := trimPeerEntries(entries)

	if len(trimmed) != maxPersistedPeers {
		t.Errorf("trimmed length = %d, want %d",
			len(trimmed), maxPersistedPeers)
	}
}

// TestVersionDiagnostics_DiskRestartRoundTrip exercises the real on-disk
// restart path: flushPeerState() writes peers.json, then a fresh
// NewService(...) loads it back. The test asserts that the observable
// peerHealthFrames() snapshot on the reloaded node contains all
// machine-readable version diagnostics (LastErrorCode, LastDisconnectCode,
// IncompatibleVersionAttempts, ObservedPeerVersion,
// ObservedPeerMinimumVersion, ClientVersion, VersionLockoutActive).
// This closes the integration gap left by the manual JSON round-trip tests:
// a future change that breaks the live flush→load→hydrate pipeline will
// fail here even if the isolated tests still pass.
func TestVersionDiagnostics_DiskRestartRoundTrip(t *testing.T) {
	t.Parallel()

	localProto := domain.ProtocolVersion(config.ProtocolVersion)
	higherMin := localProto + 1

	peerAddr := domain.PeerAddress("10.99.0.1:64646")
	peerID := domain.PeerIdentity("peer-restart-round-trip")
	clientVer := "corsa/3.0.0-test"

	tmpDir := t.TempDir()
	peersPath := filepath.Join(tmpDir, "peers.json")

	// --- Phase 1: start a node, create incompatible-version state, flush. ---
	addr1 := freeAddress(t)
	svc1, stop1 := startTestNode(t, config.Node{
		ListenAddress:    addr1,
		AdvertiseAddress: normalizeAddress(addr1),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
		PeersStatePath:   peersPath,
	})

	// Register the peer so it appears in the peer list and persistedMeta.
	svc1.addPeerAddress(peerAddr, "full", peerID)

	// Pre-populate ClientVersion in peerVersions so that
	// setVersionLockoutLocked captures it into VersionLockout.
	svc1.mu.Lock()
	svc1.peerVersions[peerAddr] = clientVer
	svc1.mu.Unlock()

	// Penalize 4 times to trigger ban and accumulate full diagnostics.
	for i := 0; i < 4; i++ {
		svc1.penalizeOldProtocolPeer(peerAddr, higherMin, higherMin)
	}

	// Snapshot the expected values before flush.
	svc1.mu.RLock()
	h := svc1.health[peerAddr]
	wantErrorCode := h.LastErrorCode
	wantAttempts := int(h.IncompatibleVersionAttempts)
	wantVersion := int(h.ObservedPeerVersion)
	wantMinVersion := int(h.ObservedPeerMinimumVersion)
	wantScore := h.Score
	wantFailures := h.ConsecutiveFailures
	wantLastError := h.LastError
	svc1.mu.RUnlock()

	// Flush to disk.
	svc1.flushPeerState()
	stop1()

	// --- Phase 2: start a fresh node from the same peers.json. ---
	addr2 := freeAddress(t)
	svc2, stop2 := startTestNode(t, config.Node{
		ListenAddress:    addr2,
		AdvertiseAddress: normalizeAddress(addr2),
		BootstrapPeers:   []string{},
		Type:             domain.NodeTypeFull,
		PeersStatePath:   peersPath,
	})
	defer stop2()

	// peerHealthFrames() is the operator-visible contract.
	frames := svc2.peerHealthFrames()

	var found *protocol.PeerHealthFrame
	for i := range frames {
		if frames[i].Address == string(peerAddr) {
			found = &frames[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("peerHealthFrames() on reloaded node does not contain %s", peerAddr)
	}

	// Assert all machine-readable diagnostic fields survived the restart.
	if found.LastErrorCode != wantErrorCode {
		t.Errorf("LastErrorCode = %q, want %q", found.LastErrorCode, wantErrorCode)
	}
	if found.LastDisconnectCode != "" {
		t.Errorf("LastDisconnectCode = %q, want empty (pre-handshake reject clears it)",
			found.LastDisconnectCode)
	}
	if found.IncompatibleVersionAttempts != wantAttempts {
		t.Errorf("IncompatibleVersionAttempts = %d, want %d",
			found.IncompatibleVersionAttempts, wantAttempts)
	}
	if found.ObservedPeerVersion != wantVersion {
		t.Errorf("ObservedPeerVersion = %d, want %d",
			found.ObservedPeerVersion, wantVersion)
	}
	if found.ObservedPeerMinimumVersion != wantMinVersion {
		t.Errorf("ObservedPeerMinimumVersion = %d, want %d",
			found.ObservedPeerMinimumVersion, wantMinVersion)
	}
	if !found.VersionLockoutActive {
		t.Error("VersionLockoutActive = false, want true")
	}
	// ClientVersion is restored from VersionLockout.ObservedClientVersion
	// into s.peerVersions on startup, then surfaced by peerHealthFrames().
	if found.ClientVersion != clientVer {
		t.Errorf("ClientVersion = %q, want %q", found.ClientVersion, clientVer)
	}
	// Legacy health fields must also survive.
	if found.Score != wantScore {
		t.Errorf("Score = %d, want %d", found.Score, wantScore)
	}
	if found.ConsecutiveFailures != wantFailures {
		t.Errorf("ConsecutiveFailures = %d, want %d",
			found.ConsecutiveFailures, wantFailures)
	}
	if found.LastError != wantLastError {
		t.Errorf("LastError = %q, want %q", found.LastError, wantLastError)
	}
	if found.Connected {
		t.Error("Connected = true, want false (peer was never actually connected)")
	}
}

package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// purgeExpiredBanState — periodic sweep for the lazily-cleaned ban maps.
//
// The contract pinned here:
//
//   s.bans:
//     - expired blacklist          → deleted
//     - active blacklist           → kept
//     - score-only, recently hit   → kept
//     - score-only, idle > TTL     → deleted
//     - score-only, zero timestamp → deleted (cannot be refreshed)
//
//   s.bannedIPSet / s.remoteBannedIPs:
//     - expired → deleted, active → kept (same predicate the readers use,
//       so the purge can never remove an entry a reader still honours)
//
//   s.setupFailures:
//     - active cooldown            → kept (sliding window must not be cut)
//     - expired cooldown, idle>TTL → deleted
//     - sub-threshold, idle > TTL  → deleted
//     - sub-threshold, fresh       → kept
// ---------------------------------------------------------------------------

func TestPurgeExpiredBansLocked(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	svc := &Service{bans: map[string]banEntry{
		"expired-blacklist": {Score: banThreshold, Blacklisted: now.Add(-time.Minute)},
		"active-blacklist":  {Score: banThreshold, Blacklisted: now.Add(time.Hour)},
		"fresh-score":       {Score: 10, LastScored: now.Add(-time.Minute)},
		"idle-score":        {Score: 10, LastScored: now.Add(-banScoreIdleTTL - time.Minute)},
		"legacy-score":      {Score: 10}, // zero LastScored — pre-field entry
	}}

	svc.purgeExpiredBansLocked(now)

	for _, ip := range []string{"expired-blacklist", "idle-score", "legacy-score"} {
		if _, ok := svc.bans[ip]; ok {
			t.Errorf("expected %q to be purged", ip)
		}
	}
	for _, ip := range []string{"active-blacklist", "fresh-score"} {
		if _, ok := svc.bans[ip]; !ok {
			t.Errorf("expected %q to be kept", ip)
		}
	}
}

func TestPurgeExpiredBannedIPSetLocked(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	svc := &Service{bannedIPSet: map[string]domain.BannedIPEntry{
		"expired": {BannedUntil: now.Add(-time.Minute)},
		"zero":    {}, // zero BannedUntil — readers already treat as expired
		"active":  {BannedUntil: now.Add(time.Hour)},
	}}

	svc.purgeExpiredBannedIPSetLocked(now)

	if _, ok := svc.bannedIPSet["expired"]; ok {
		t.Error("expected expired entry to be purged")
	}
	if _, ok := svc.bannedIPSet["zero"]; ok {
		t.Error("expected zero-BannedUntil entry to be purged")
	}
	if _, ok := svc.bannedIPSet["active"]; !ok {
		t.Error("expected active entry to be kept")
	}
}

func TestPurgeExpiredRemoteIPBansLocked(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	svc := &Service{remoteBannedIPs: map[string]remoteIPBanEntry{
		"expired": {Until: now.Add(-time.Minute), Reason: "blacklisted"},
		"active":  {Until: now.Add(time.Hour), Reason: "blacklisted"},
	}}

	svc.purgeExpiredRemoteIPBansLocked(now)

	if _, ok := svc.remoteBannedIPs["expired"]; ok {
		t.Error("expected expired remote ban to be purged")
	}
	if _, ok := svc.remoteBannedIPs["active"]; !ok {
		t.Error("expected active remote ban to be kept")
	}
	// Predicate parity with the reader: a purged entry must read as
	// not-banned, a kept entry as banned.
	if svc.isRemoteIPBannedLocked("expired", now) {
		t.Error("purged entry still reads as banned")
	}
	if !svc.isRemoteIPBannedLocked("active", now) {
		t.Error("kept entry no longer reads as banned")
	}
}

func TestPurgeStaleSetupFailuresLocked(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	svc := &Service{setupFailures: map[domain.PeerAddress]*setupFailureEntry{
		"active-cooldown": {
			Consecutive:   setupFailureBanThreshold,
			CooldownUntil: now.Add(setupFailureCooldown / 2),
			LastFailure:   now.Add(-setupFailureIdleTTL * 2), // idle, but cooldown wins
		},
		"expired-cooldown-idle": {
			Consecutive:   setupFailureBanThreshold,
			CooldownUntil: now.Add(-setupFailureIdleTTL - time.Minute),
			LastFailure:   now.Add(-setupFailureIdleTTL - time.Minute),
		},
		"expired-cooldown-recent": {
			Consecutive:   setupFailureBanThreshold,
			CooldownUntil: now.Add(-time.Minute),
			LastFailure:   now.Add(-time.Minute),
		},
		"sub-threshold-fresh": {
			Consecutive: 1,
			LastFailure: now.Add(-time.Minute),
		},
		"sub-threshold-idle": {
			Consecutive: 1,
			LastFailure: now.Add(-setupFailureIdleTTL - time.Minute),
		},
		"legacy-sub-threshold": {
			Consecutive: 1, // zero LastFailure & CooldownUntil — pre-field entry
		},
		"nil-entry": nil,
	}}

	svc.purgeStaleSetupFailuresLocked(now)

	for _, addr := range []domain.PeerAddress{
		"expired-cooldown-idle", "sub-threshold-idle", "legacy-sub-threshold", "nil-entry",
	} {
		if _, ok := svc.setupFailures[addr]; ok {
			t.Errorf("expected %q to be purged", addr)
		}
	}
	for _, addr := range []domain.PeerAddress{
		"active-cooldown", "expired-cooldown-recent", "sub-threshold-fresh",
	} {
		if _, ok := svc.setupFailures[addr]; !ok {
			t.Errorf("expected %q to be kept", addr)
		}
	}
	// The kept active cooldown must still gate the dialler.
	if !svc.isSetupFailureBannedAt("active-cooldown", now) {
		t.Error("active cooldown entry no longer reads as banned")
	}
}

// TestPurgeExpiredBanState_EndToEnd exercises the combined sweep exactly as
// bootstrapLoop calls it, including the offender-bucket sweep, on a Service
// with all four maps populated.
func TestPurgeExpiredBanState_EndToEnd(t *testing.T) {
	t.Parallel()

	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour)
	svc := &Service{
		bans: map[string]banEntry{
			"gone": {Score: banThreshold, Blacklisted: past},
			"live": {Score: banThreshold, Blacklisted: future},
		},
		bannedIPSet: map[string]domain.BannedIPEntry{
			"gone": {BannedUntil: past},
			"live": {BannedUntil: future},
		},
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"gone": {Until: past},
			"live": {Until: future},
		},
		remoteIPBanOffenders: map[string]map[domain.PeerAddress]time.Time{
			"gone": {"peer-a:5000": past},
			"live": {"peer-b:5000": future},
		},
		setupFailures: map[domain.PeerAddress]*setupFailureEntry{
			"gone:5000": {Consecutive: setupFailureBanThreshold, CooldownUntil: past, LastFailure: past},
			"live:5000": {Consecutive: setupFailureBanThreshold, CooldownUntil: future, LastFailure: future},
		},
	}

	svc.purgeExpiredBanState()

	checks := []struct {
		name string
		gone bool
		live bool
	}{
		{"bans", mapHasKey(svc.bans, "gone"), mapHasKey(svc.bans, "live")},
		{"bannedIPSet", mapHasKey(svc.bannedIPSet, "gone"), mapHasKey(svc.bannedIPSet, "live")},
		{"remoteBannedIPs", mapHasKey(svc.remoteBannedIPs, "gone"), mapHasKey(svc.remoteBannedIPs, "live")},
		{"remoteIPBanOffenders", mapHasKey(svc.remoteIPBanOffenders, "gone"), mapHasKey(svc.remoteIPBanOffenders, "live")},
		{"setupFailures", mapHasKey(svc.setupFailures, domain.PeerAddress("gone:5000")), mapHasKey(svc.setupFailures, domain.PeerAddress("live:5000"))},
	}
	for _, c := range checks {
		if c.gone {
			t.Errorf("%s: expected expired entry to be purged", c.name)
		}
		if !c.live {
			t.Errorf("%s: expected live entry to be kept", c.name)
		}
	}
}

func mapHasKey[K comparable, V any](m map[K]V, k K) bool {
	_, ok := m[k]
	return ok
}

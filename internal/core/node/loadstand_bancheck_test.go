package node

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

var errLoadStandBansPresent = errors.New("load stand node holds bans; the sample would measure them")

// loadStandBanKind names where a ban lives, so a journal entry says which
// mechanism fired.
type loadStandBanKind string

const (
	// loadStandBanScore: an IP has a non-zero local ban score (s.bans) —
	// the command rate limit or another misbehaviour check fired, even if
	// the blacklist threshold is not reached yet.
	loadStandBanScore loadStandBanKind = "ban_score"
	// loadStandBanBlacklist: an IP is blacklisted locally (s.bans).
	loadStandBanBlacklist loadStandBanKind = "blacklist"
	// loadStandBanIPWide: an IP-wide ban this node applied (s.bannedIPSet).
	loadStandBanIPWide loadStandBanKind = "ip_ban"
	// loadStandBanRemoteIPWide: a responder told this node its IP is
	// banned (s.remoteBannedIPs).
	loadStandBanRemoteIPWide loadStandBanKind = "remote_ip_ban"
	// loadStandBanPeer: a peer address this node will not dial
	// (health BannedUntil) — PeerProvider widens it to the whole IP.
	loadStandBanPeer loadStandBanKind = "peer_ban"
	// loadStandBanRemotePeer: a responder banned this node at one address
	// (persistedMeta RemoteBannedUntil).
	loadStandBanRemotePeer loadStandBanKind = "remote_peer_ban"
	// loadStandSelfIdentityCooldown: this node met its own identity at an
	// address and stopped dialling that address for a while (health
	// BannedUntil with LastErrorCode self-identity). PeerProvider does not
	// widen it to the IP, so it cuts off nothing but a route to itself.
	loadStandSelfIdentityCooldown loadStandBanKind = "self_identity_cooldown"
)

// loadStandBanPolicy is the stand's explicit decision about which findings
// invalidate a sample. Every kind but a self-identity cooldown always does;
// the zero value treats the cooldown as the harmless self-route it is.
type loadStandBanPolicy struct {
	SelfIdentityCooldownInvalidates bool
}

func (p loadStandBanPolicy) invalidates(kind loadStandBanKind) bool {
	return kind != loadStandSelfIdentityCooldown || p.SelfIdentityCooldownInvalidates
}

type loadStandBanFinding struct {
	Kind loadStandBanKind
	// Key is the IP or peer address the ban is held against.
	Key string
	// Until is the ban's expiry; zero for a score, which has none.
	Until time.Time
}

func (f loadStandBanFinding) String() string {
	if f.Until.IsZero() {
		return fmt.Sprintf("%s %s", f.Kind, f.Key)
	}
	return fmt.Sprintf("%s %s until %s", f.Kind, f.Key, f.Until.UTC().Format(time.RFC3339))
}

type loadStandBansPresentError struct {
	Findings []loadStandBanFinding
}

func (e *loadStandBansPresentError) Error() string {
	described := make([]string, 0, len(e.Findings))
	for _, finding := range e.Findings {
		described = append(described, finding.String())
	}
	return fmt.Sprintf("%s: %s", errLoadStandBansPresent, strings.Join(described, "; "))
}

func (e *loadStandBansPresentError) Is(target error) bool { return target == errLoadStandBansPresent }

// loadStandBanFindings reports every ban and cooldown in force on svc at now,
// for the journal. It only reads, and takes peerMu and ipStateMu one after the
// other, never nested: the two halves need not be one consistent snapshot,
// because each finding is judged on its own.
func loadStandBanFindings(svc *Service, now time.Time) []loadStandBanFinding {
	return append(peerDomainBanFindings(svc, now), ipDomainBanFindings(svc, now)...)
}

// checkLoadStandBanFree is the validity check every stand sample is taken
// behind. DisableRateLimiting only switches off the accept-path checks: bans
// are still earned and still obeyed everywhere else, and on a stand where
// every node is 127.0.0.1 a single one cuts a node off from the whole network
// and outlives a restart through peers.json. A sample taken after that
// measures the ban, so the stand records the findings and discards the
// sample instead. policy decides which findings count.
func checkLoadStandBanFree(svc *Service, now time.Time, policy loadStandBanPolicy) error {
	invalidating := invalidatingLoadStandBanFindings(svc, now, policy)
	if len(invalidating) == 0 {
		return nil
	}
	return &loadStandBansPresentError{Findings: invalidating}
}

// invalidatingLoadStandBanFindings are the findings the policy treats as
// invalidating a sample — what checkLoadStandBanFree refuses on and what a
// stand sample records.
func invalidatingLoadStandBanFindings(svc *Service, now time.Time, policy loadStandBanPolicy) []loadStandBanFinding {
	var invalidating []loadStandBanFinding
	for _, finding := range loadStandBanFindings(svc, now) {
		if policy.invalidates(finding.Kind) {
			invalidating = append(invalidating, finding)
		}
	}
	return invalidating
}

// peerDomainBanFindings reads the per-address bans held under peerMu.
func peerDomainBanFindings(svc *Service, now time.Time) []loadStandBanFinding {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()

	var findings []loadStandBanFinding
	for address, health := range svc.health {
		if health != nil && health.BannedUntil.After(now) {
			findings = append(findings, loadStandBanFinding{Kind: peerBanKind(health), Key: string(address), Until: health.BannedUntil})
		}
	}
	for address, meta := range svc.persistedMeta {
		if meta != nil && meta.RemoteBannedUntil != nil && meta.RemoteBannedUntil.After(now) {
			findings = append(findings, loadStandBanFinding{Kind: loadStandBanRemotePeer, Key: string(address), Until: *meta.RemoteBannedUntil})
		}
	}
	return findings
}

// peerBanKind separates the self-identity cooldown from a real per-address
// ban by the same rule PeerProvider uses to decide whether to widen it.
//
// Caller must hold s.peerMu (read).
func peerBanKind(health *peerHealth) loadStandBanKind {
	if health.LastErrorCode == protocol.ErrCodeSelfIdentity {
		return loadStandSelfIdentityCooldown
	}
	return loadStandBanPeer
}

// ipDomainBanFindings reads the IP-wide bans and scores held under ipStateMu.
func ipDomainBanFindings(svc *Service, now time.Time) []loadStandBanFinding {
	svc.ipStateMu.RLock()
	defer svc.ipStateMu.RUnlock()

	var findings []loadStandBanFinding
	for ip, entry := range svc.bans {
		switch {
		case entry.Blacklisted.After(now):
			findings = append(findings, loadStandBanFinding{Kind: loadStandBanBlacklist, Key: ip, Until: entry.Blacklisted})
		case entry.Score > 0:
			findings = append(findings, loadStandBanFinding{Kind: loadStandBanScore, Key: ip})
		}
	}
	for ip, entry := range svc.bannedIPSet {
		if entry.BannedUntil.After(now) {
			findings = append(findings, loadStandBanFinding{Kind: loadStandBanIPWide, Key: ip, Until: entry.BannedUntil})
		}
	}
	for ip, entry := range svc.remoteBannedIPs {
		if entry.Until.After(now) {
			findings = append(findings, loadStandBanFinding{Kind: loadStandBanRemoteIPWide, Key: ip, Until: entry.Until})
		}
	}
	return findings
}

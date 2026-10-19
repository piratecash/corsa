package node

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type banDiagnosticIP struct {
	IP            string               `json:"ip"`
	BannedUntil   time.Time            `json:"banned_until"`
	BanOrigin     domain.PeerAddress   `json:"ban_origin"`
	BanReason     string               `json:"ban_reason"`
	AffectedPeers []domain.PeerAddress `json:"affected_peers"`
}

type localBanDiagnostic struct {
	IP               string     `json:"ip"`
	Score            int        `json:"score"`
	BlacklistedUntil *time.Time `json:"blacklisted_until,omitempty"`
}

type peerBanDiagnostic struct {
	Endpoint             domain.PeerAddress `json:"endpoint"`
	Until                time.Time          `json:"until"`
	SelfIdentityCooldown bool               `json:"self_identity_cooldown"`
}

type remoteIPBanDiagnostic struct {
	IP     string    `json:"ip"`
	Until  time.Time `json:"until"`
	Reason string    `json:"reason"`
}

type banDiagnosticsLifecycle struct {
	ctx     context.Context
	stopped chan struct{}
}

type banDiagnosticsRequest struct {
	ctx       context.Context
	lifecycle *banDiagnosticsLifecycle
	result    chan banDiagnosticsSnapshot
}

const banDiagnosticsReadTimeout = 5 * time.Second

var errBanDiagnosticsNotRunning = errors.New("ban diagnostics refresh is not running")

type banDiagnosticsSnapshot struct {
	ObservationStartedAt               *time.Time              `json:"observation_started_at,omitempty"`
	Fresh                              bool                    `json:"fresh"`
	LocalIPBans                        []remoteIPBanDiagnostic `json:"local_ip_bans"`
	LoopbackIdentitiesScored           int                     `json:"loopback_identities_scored"`
	LoopbackIdentitiesBanned           int                     `json:"loopback_identities_banned"`
	LoopbackIdentityPenaltiesAvailable bool                    `json:"loopback_identity_penalties_available"`
	Version                            int                     `json:"version"`
	Ready                              bool                    `json:"ready"`
	StartedAt                          time.Time               `json:"started_at"`
	GeneratedAt                        *time.Time              `json:"generated_at,omitempty"`
	BannedIPs                          []banDiagnosticIP       `json:"banned_ips"`
	Count                              int                     `json:"count"`
	LocalBans                          []localBanDiagnostic    `json:"local_bans"`
	PeerBans                           []peerBanDiagnostic     `json:"peer_bans"`
	RemotePeerBans                     []peerBanDiagnostic     `json:"remote_peer_bans"`
	RemoteIPBans                       []remoteIPBanDiagnostic `json:"remote_ip_bans"`
}

func emptyBanDiagnosticsSnapshot(startedAt time.Time) banDiagnosticsSnapshot {
	return banDiagnosticsSnapshot{Version: 2, StartedAt: startedAt,
		LocalIPBans: []remoteIPBanDiagnostic{}, BannedIPs: []banDiagnosticIP{}, LocalBans: []localBanDiagnostic{},
		PeerBans: []peerBanDiagnostic{}, RemotePeerBans: []peerBanDiagnostic{}, RemoteIPBans: []remoteIPBanDiagnostic{},
	}
}

// ListBannedJSON is a lock-free snapshot read. Before startup priming, Ready
// explicitly distinguishes unavailable state from an observed empty set.
func (s *Service) ListBannedJSON() (json.RawMessage, error) {
	s.banDiagnosticsAccessNanos.Store(time.Now().UnixNano())
	snapshot := s.banDiagnosticsSnap.Load()
	if snapshot == nil {
		empty := emptyBanDiagnosticsSnapshot(s.startedAt)
		return json.Marshal(empty)
	}
	return json.Marshal(snapshot)
}

func (s *Service) maybeRebuildBanDiagnosticsSnapshot() {
	last := s.banDiagnosticsAccessNanos.Load()
	if last == 0 || time.Since(time.Unix(0, last)) > peerHealthRebuildIdleAfter {
		return
	}
	s.rebuildBanDiagnosticsSnapshot()
}

func (s *Service) rebuildBanDiagnosticsSnapshot() {
	now := time.Now().UTC()
	snapshot := emptyBanDiagnosticsSnapshot(s.startedAt)
	// Provider callbacks may take Service locks, so finish them before entering
	// either domain below. The individual source snapshots are not atomic together.
	if s.peerProvider != nil {
		for _, entry := range s.peerProvider.BannedIPs() {
			snapshot.BannedIPs = append(snapshot.BannedIPs, banDiagnosticIP{
				IP: entry.IP, BannedUntil: entry.BannedUntil, BanOrigin: entry.BanOrigin,
				BanReason: entry.BanReason, AffectedPeers: append([]domain.PeerAddress{}, entry.AffectedPeers...),
			})
		}
	}
	s.collectPeerBanDiagnostics(&snapshot, now)
	s.collectIPBanDiagnostics(&snapshot, now)
	sortBanDiagnostics(&snapshot)
	if s.loopbackInbound != nil {
		snapshot.LoopbackIdentitiesScored, snapshot.LoopbackIdentitiesBanned = s.loopbackInbound.identities.gauges()
		snapshot.LoopbackIdentityPenaltiesAvailable = true
	}
	snapshot.Count = len(snapshot.BannedIPs)
	snapshot.Ready = true
	snapshot.ObservationStartedAt = &now
	generatedAt := time.Now().UTC()
	snapshot.GeneratedAt = &generatedAt
	s.banDiagnosticsSnap.Store(&snapshot)
}

func (s *Service) collectPeerBanDiagnostics(snapshot *banDiagnosticsSnapshot, now time.Time) {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	for endpoint, health := range s.health {
		if health != nil && health.BannedUntil.After(now) {
			snapshot.PeerBans = append(snapshot.PeerBans, peerBanDiagnostic{Endpoint: endpoint, Until: health.BannedUntil,
				SelfIdentityCooldown: health.LastErrorCode == protocol.ErrCodeSelfIdentity})
		}
	}
	for endpoint, entry := range s.persistedMeta {
		if entry != nil && entry.RemoteBannedUntil != nil && entry.RemoteBannedUntil.After(now) {
			snapshot.RemotePeerBans = append(snapshot.RemotePeerBans, peerBanDiagnostic{Endpoint: endpoint, Until: *entry.RemoteBannedUntil})
		}
	}
}

func (s *Service) collectIPBanDiagnostics(snapshot *banDiagnosticsSnapshot, now time.Time) {
	s.ipStateMu.RLock()
	defer s.ipStateMu.RUnlock()
	for ip, entry := range s.bans {
		if entry.Score <= 0 && !entry.Blacklisted.After(now) {
			continue
		}
		diagnostic := localBanDiagnostic{IP: ip, Score: entry.Score}
		if entry.Blacklisted.After(now) {
			until := entry.Blacklisted
			diagnostic.BlacklistedUntil = &until
		}
		snapshot.LocalBans = append(snapshot.LocalBans, diagnostic)
	}
	for ip, entry := range s.bannedIPSet {
		if entry.BannedUntil.After(now) {
			snapshot.LocalIPBans = append(snapshot.LocalIPBans, remoteIPBanDiagnostic{IP: ip, Until: entry.BannedUntil, Reason: entry.BanReason})
		}
	}
	for ip, entry := range s.remoteBannedIPs {
		if entry.Until.After(now) {
			snapshot.RemoteIPBans = append(snapshot.RemoteIPBans, remoteIPBanDiagnostic{IP: ip, Until: entry.Until, Reason: entry.Reason})
		}
	}
}

func sortBanDiagnostics(snapshot *banDiagnosticsSnapshot) {
	sort.Slice(snapshot.BannedIPs, func(i, j int) bool { return snapshot.BannedIPs[i].IP < snapshot.BannedIPs[j].IP })
	for i := range snapshot.BannedIPs {
		sort.Slice(snapshot.BannedIPs[i].AffectedPeers, func(a, b int) bool {
			return snapshot.BannedIPs[i].AffectedPeers[a] < snapshot.BannedIPs[i].AffectedPeers[b]
		})
	}
	sort.Slice(snapshot.LocalBans, func(i, j int) bool { return snapshot.LocalBans[i].IP < snapshot.LocalBans[j].IP })
	sort.Slice(snapshot.PeerBans, func(i, j int) bool { return snapshot.PeerBans[i].Endpoint < snapshot.PeerBans[j].Endpoint })
	sort.Slice(snapshot.RemotePeerBans, func(i, j int) bool { return snapshot.RemotePeerBans[i].Endpoint < snapshot.RemotePeerBans[j].Endpoint })
	sort.Slice(snapshot.LocalIPBans, func(i, j int) bool { return snapshot.LocalIPBans[i].IP < snapshot.LocalIPBans[j].IP })
	sort.Slice(snapshot.RemoteIPBans, func(i, j int) bool { return snapshot.RemoteIPBans[i].IP < snapshot.RemoteIPBans[j].IP })
}

// FreshBanDiagnostics establishes a read barrier on the refresher, never on
// an RPC goroutine holding a Service lock. Cancellation ends queueing/waiting;
// only a snapshot collected for this request is marked fresh.
func (s *Service) FreshBanDiagnostics(ctx context.Context) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, banDiagnosticsReadTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lifecycle := s.banDiagnosticsLifecycle.Load()
	if s.banDiagnosticsRequests == nil || !s.banDiagnosticsLifecycleActive(lifecycle) {
		return nil, errBanDiagnosticsNotRunning
	}
	request := banDiagnosticsRequest{ctx: ctx, lifecycle: lifecycle, result: make(chan banDiagnosticsSnapshot, 1)}
	select {
	case s.banDiagnosticsRequests <- request:
	case <-lifecycle.ctx.Done():
		return nil, errBanDiagnosticsNotRunning
	case <-lifecycle.stopped:
		return nil, errBanDiagnosticsNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case snapshot := <-request.result:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !s.banDiagnosticsLifecycleActive(lifecycle) {
			return nil, errBanDiagnosticsNotRunning
		}
		snapshot.Fresh = true
		raw, err := json.Marshal(snapshot)
		if err != nil {
			return nil, err
		}
		if !s.banDiagnosticsLifecycleActive(lifecycle) {
			return nil, errBanDiagnosticsNotRunning
		}
		return raw, nil
	case <-lifecycle.ctx.Done():
		return nil, errBanDiagnosticsNotRunning
	case <-lifecycle.stopped:
		return nil, errBanDiagnosticsNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) banDiagnosticsLifecycleActive(lifecycle *banDiagnosticsLifecycle) bool {
	if lifecycle == nil || s.banDiagnosticsLifecycle.Load() != lifecycle || lifecycle.ctx.Err() != nil {
		return false
	}
	select {
	case <-lifecycle.stopped:
		return false
	default:
		return true
	}
}

func (s *Service) runBanDiagnosticsRefresh(ctx context.Context) {
	lifecycle := &banDiagnosticsLifecycle{ctx: ctx, stopped: make(chan struct{})}
	s.banDiagnosticsLifecycle.Store(lifecycle)
	defer close(lifecycle.stopped)
	ticker := time.NewTicker(networkStatsSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			s.maybeRebuildBanDiagnosticsSnapshot()
		case request := <-s.banDiagnosticsRequests:
			if !s.banDiagnosticsLifecycleActive(lifecycle) {
				return
			}
			if request.lifecycle != lifecycle || request.ctx.Err() != nil {
				continue
			}
			s.rebuildBanDiagnosticsSnapshot()
			if !s.banDiagnosticsLifecycleActive(lifecycle) {
				return
			}
			if request.ctx.Err() != nil {
				continue
			}
			request.result <- *s.banDiagnosticsSnap.Load()
		}
	}
}

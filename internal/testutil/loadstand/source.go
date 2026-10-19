package loadstand

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

type ProcessSource struct {
	client  *RPCClient
	clock   runjournal.Clock
	binding *RuntimeBinding // immutable copy, only for incarnation-bound sampling
}

func NewProcessSource(client *RPCClient) *ProcessSource {
	return &ProcessSource{client: client, clock: runjournal.SystemClock{}}
}

func NewProcessSourceWithClock(client *RPCClient, clock runjournal.Clock) (*ProcessSource, error) {
	if client == nil || clock == nil {
		return nil, ErrInvalidConfiguration
	}
	return &ProcessSource{client: client, clock: clock}, nil
}
func (s *ProcessSource) Status(ctx context.Context) (domain.NodeStatus, error) {
	var status domain.NodeStatus
	if err := s.readDTO(ctx, "getNodeStatus", &status); err != nil {
		return status, err
	}
	if status.Address == "" || status.Address != status.Identity {
		return domain.NodeStatus{}, ErrInvalidSample
	}
	return status, nil
}
func (s *ProcessSource) Runtime(ctx context.Context) (domain.RuntimeMetrics, error) {
	var sample domain.RuntimeMetrics
	if err := s.readDTO(ctx, "getRuntimeMetrics", &sample); err != nil {
		return sample, err
	}
	if sample.Version != 1 || sample.StartedAt.IsZero() || sample.ReadAt.Before(sample.StartedAt) {
		return domain.RuntimeMetrics{}, ErrFamilyUnavailable
	}
	if len(sample.Unsupported) != 0 || sample.HeapLiveBytes == nil || sample.HeapObjectsBytes == nil || sample.HeapAllocsBytes == nil || sample.GoMemoryBytes == nil || sample.GCCycles == nil || sample.Goroutines == nil || sample.GOMAXPROCS == nil || sample.CPUTotalSeconds == nil || sample.CPUUserSeconds == nil || sample.CPUGCSeconds == nil || sample.CPUIdleSeconds == nil {
		return domain.RuntimeMetrics{}, ErrFamilyUnavailable
	}
	return sample, nil
}

type IPBan struct {
	IP    string    `json:"ip"`
	Until time.Time `json:"until"`
}
type LocalBan struct {
	IP               string     `json:"ip"`
	Score            int        `json:"score"`
	BlacklistedUntil *time.Time `json:"blacklisted_until"`
}
type PeerBan struct {
	Endpoint             domain.PeerAddress `json:"endpoint"`
	Until                time.Time          `json:"until"`
	SelfIdentityCooldown bool               `json:"self_identity_cooldown"`
}

// Pointers in this transport DTO distinguish a missing old-release family
// from an observed empty family or a supported zero count.
type BanObservation struct {
	ProviderIPBans           *[]ProviderIPBan `json:"banned_ips"`
	ProviderCount            *int             `json:"count"`
	Version                  int              `json:"version"`
	Ready                    *bool            `json:"ready"`
	Fresh                    *bool            `json:"fresh"`
	StartedAt                time.Time        `json:"started_at"`
	ObservationStartedAt     time.Time        `json:"observation_started_at"`
	GeneratedAt              time.Time        `json:"generated_at"`
	LocalBans                *[]LocalBan      `json:"local_bans"`
	LocalIPBans              *[]IPBan         `json:"local_ip_bans"`
	PeerBans                 *[]PeerBan       `json:"peer_bans"`
	RemotePeerBans           *[]PeerBan       `json:"remote_peer_bans"`
	RemoteIPBans             *[]IPBan         `json:"remote_ip_bans"`
	LoopbackIdentitiesScored *int             `json:"loopback_identities_scored"`
	LoopbackIdentitiesBanned *int             `json:"loopback_identities_banned"`
	LoopbackAvailable        *bool            `json:"loopback_identity_penalties_available"`
}

func (s *ProcessSource) BanObservation(ctx context.Context) (BanObservation, error) {
	return s.readCompleteBans(ctx)
}
func trueReading(value *bool) bool { return value != nil && *value }
func (s *ProcessSource) RouteSummary(ctx context.Context) (map[string]json.RawMessage, error) {
	return s.commandObject(ctx, "fetchRouteSummary")
}
func (s *ProcessSource) ReadFamily(ctx context.Context, name string, target any) error {
	summary, err := s.RouteSummary(ctx)
	if err != nil {
		return err
	}
	return decodeRequired(summary, name, target)
}
func decodeRequired(summary map[string]json.RawMessage, name string, target any) error {
	raw, found := summary[name]
	if !found || string(raw) == "null" {
		return fmt.Errorf("%w: %s", ErrFamilyUnavailable, name)
	}
	if err := decodeDTO(raw, target); err != nil {
		return fmt.Errorf("%w: %s", err, name)
	}
	return nil
}

// ConnectedPeers is a readiness check, not a neighbour-composition sampler.
func (s *ProcessSource) ConnectedPeers(ctx context.Context) ([]domain.PeerAddress, error) {
	var frame protocol.Frame
	if err := s.readDTO(ctx, "fetchPeerHealth", &frame); err != nil {
		return nil, err
	}
	if frame.Type != "peer_health" {
		return nil, ErrFamilyUnavailable
	}
	peers := make([]domain.PeerAddress, 0, len(frame.PeerHealth))
	for _, peer := range frame.PeerHealth {
		if peer.Connected {
			peers = append(peers, domain.PeerAddress(peer.Address))
		}
	}
	return peers, nil
}

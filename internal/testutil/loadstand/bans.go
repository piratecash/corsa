package loadstand

import (
	"bytes"
	"context"
	"encoding/json"
	"net/netip"
	"time"
)

type ProviderIPBan struct {
	IP    string    `json:"ip"`
	Until time.Time `json:"banned_until"`
}
type BanKind string

const (
	BanScore                BanKind = "ban_score"
	BanBlacklist            BanKind = "blacklist"
	BanIPWide               BanKind = "ip_ban"
	BanProviderIP           BanKind = "provider_ip_ban"
	BanRemoteIPWide         BanKind = "remote_ip_ban"
	BanPeer                 BanKind = "peer_ban"
	BanRemotePeer           BanKind = "remote_peer_ban"
	BanSelfIdentityCooldown BanKind = "self_identity_cooldown"
	BanLoopbackScore        BanKind = "loopback_identity_score"
	BanLoopbackBanned       BanKind = "loopback_identity_ban"
)

type BanPolicy struct{ SelfIdentityCooldownInvalidates bool }
type BanFinding struct {
	Kind  BanKind
	Key   string
	Until time.Time
	Count uint64
}

func (s *ProcessSource) readCompleteBans(ctx context.Context) (BanObservation, error) {
	before := s.clock.Now()
	fields, err := s.commandObject(ctx, "listBanned", "version", "ready", "fresh", "started_at", "observation_started_at", "generated_at", "banned_ips", "count", "local_bans", "local_ip_bans", "peer_bans", "remote_peer_bans", "remote_ip_bans", "loopback_identities_scored", "loopback_identities_banned", "loopback_identity_penalties_available")
	if err != nil {
		return BanObservation{}, err
	}
	after := s.clock.Now()
	raw, err := json.Marshal(fields)
	if err != nil {
		return BanObservation{}, ErrInvalidSample
	}
	var sample BanObservation
	if err := decodeDTO(raw, &sample); err != nil {
		return BanObservation{}, ErrInvalidSample
	}
	if sample.Version != 2 || !trueReading(sample.Ready) || !trueReading(sample.Fresh) || !trueReading(sample.LoopbackAvailable) {
		return BanObservation{}, ErrFamilyUnavailable
	}
	if sample.StartedAt.IsZero() || sample.ObservationStartedAt.Before(sample.StartedAt) || sample.GeneratedAt.Before(sample.ObservationStartedAt) || before.IsZero() || after.Before(before) || sample.ObservationStartedAt.Before(before) || sample.GeneratedAt.After(after) || *sample.LoopbackIdentitiesScored < 0 || *sample.LoopbackIdentitiesBanned < 0 {
		return BanObservation{}, ErrInvalidSample
	}
	if sample.ProviderIPBans == nil || sample.ProviderCount == nil || *sample.ProviderCount != len(*sample.ProviderIPBans) {
		return BanObservation{}, ErrInvalidSample
	}
	families := []struct {
		name     string
		required []string
	}{{"local_bans", []string{"ip", "score"}}, {"local_ip_bans", []string{"ip", "until"}}, {"remote_ip_bans", []string{"ip", "until"}}, {"banned_ips", []string{"ip", "banned_until"}}, {"peer_bans", []string{"endpoint", "until", "self_identity_cooldown"}}, {"remote_peer_bans", []string{"endpoint", "until", "self_identity_cooldown"}}}
	for _, family := range families {
		rows, err := value[[]json.RawMessage](fields, family.name)
		if err != nil {
			return BanObservation{}, err
		}
		for _, raw := range rows {
			row, err := object(raw, family.required...)
			if err != nil {
				return BanObservation{}, err
			}
			if err := validateBanRow(row, family.name); err != nil {
				return BanObservation{}, err
			}
		}
	}
	return sample, nil
}
func validateBanRow(row map[string]json.RawMessage, family string) error {
	if family == "peer_bans" || family == "remote_peer_bans" {
		endpoint, err := value[string](row, "endpoint")
		if err != nil {
			return err
		}
		if endpoint == "" {
			return ErrInvalidSample
		}
		if _, err := value[bool](row, "self_identity_cooldown"); err != nil {
			return err
		}
		_, err = timestamp(row, "until")
		return err
	}
	ip, err := value[string](row, "ip")
	if err != nil {
		return err
	}
	if _, err := netip.ParseAddr(ip); err != nil {
		return ErrInvalidSample
	}
	switch family {
	case "local_bans":
		score, err := value[int](row, "score")
		if err != nil {
			return err
		}
		if score < 0 {
			return ErrInvalidSample
		}
		if raw, ok := row["blacklisted_until"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			_, err = timestamp(row, "blacklisted_until")
			return err
		}
		return nil
	case "banned_ips":
		_, err = timestamp(row, "banned_until")
	default:
		_, err = timestamp(row, "until")
	}
	return err
}

// Bans uses all fresh R1 families. An old or incomplete R1 response is an error,
// never a weaker PeerProvider-only success. now is the caller's sample boundary;
// the injected source clock also verifies the node's request-local read barrier.
func (s *ProcessSource) Bans(ctx context.Context, now time.Time, policy BanPolicy) ([]BanFinding, error) {
	sample, err := s.BanObservation(ctx)
	if err != nil {
		return nil, err
	}
	if now.IsZero() || sample.GeneratedAt.Before(now) {
		return nil, ErrInvalidSample
	}
	findings := make([]BanFinding, 0)
	add := func(kind BanKind, key string, until time.Time) {
		if until.After(now) {
			findings = append(findings, BanFinding{Kind: kind, Key: key, Until: until})
		}
	}
	for _, ban := range *sample.LocalBans {
		if ban.BlacklistedUntil != nil && ban.BlacklistedUntil.After(now) {
			add(BanBlacklist, ban.IP, *ban.BlacklistedUntil)
		} else if ban.Score > 0 {
			findings = append(findings, BanFinding{Kind: BanScore, Key: ban.IP})
		}
	}
	for _, ban := range *sample.LocalIPBans {
		add(BanIPWide, ban.IP, ban.Until)
	}
	for _, ban := range *sample.ProviderIPBans {
		add(BanProviderIP, ban.IP, ban.Until)
	}
	for _, ban := range *sample.RemoteIPBans {
		add(BanRemoteIPWide, ban.IP, ban.Until)
	}
	for _, ban := range *sample.PeerBans {
		kind := BanPeer
		if ban.SelfIdentityCooldown {
			if !policy.SelfIdentityCooldownInvalidates {
				continue
			}
			kind = BanSelfIdentityCooldown
		}
		add(kind, string(ban.Endpoint), ban.Until)
	}
	for _, ban := range *sample.RemotePeerBans {
		add(BanRemotePeer, string(ban.Endpoint), ban.Until)
	}
	if *sample.LoopbackIdentitiesScored > 0 {
		findings = append(findings, BanFinding{Kind: BanLoopbackScore, Key: "loopback", Count: uint64(*sample.LoopbackIdentitiesScored)})
	}
	if *sample.LoopbackIdentitiesBanned > 0 {
		findings = append(findings, BanFinding{Kind: BanLoopbackBanned, Key: "loopback", Count: uint64(*sample.LoopbackIdentitiesBanned)})
	}
	return findings, nil
}

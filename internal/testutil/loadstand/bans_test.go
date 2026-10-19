package loadstand

import (
	"errors"
	"testing"
	"time"
)

func banFixture() map[string]any {
	until := adapterAt.Add(time.Minute)
	return map[string]any{"version": 2, "ready": true, "fresh": true, "started_at": adapterAt.Add(-time.Second), "observation_started_at": adapterAt, "generated_at": adapterAt, "banned_ips": []any{map[string]any{"ip": "192.0.2.1", "banned_until": until}}, "count": 1, "local_bans": []any{map[string]any{"ip": "192.0.2.2", "score": 1}, map[string]any{"ip": "192.0.2.3", "score": 2, "blacklisted_until": until}}, "local_ip_bans": []any{map[string]any{"ip": "192.0.2.4", "until": until}}, "remote_ip_bans": []any{map[string]any{"ip": "192.0.2.5", "until": until}}, "peer_bans": []any{map[string]any{"endpoint": "192.0.2.6:64650", "until": until, "self_identity_cooldown": false}, map[string]any{"endpoint": "192.0.2.7:64650", "until": until, "self_identity_cooldown": true}}, "remote_peer_bans": []any{map[string]any{"endpoint": "192.0.2.8:64650", "until": until, "self_identity_cooldown": false}}, "loopback_identities_scored": 2, "loopback_identities_banned": 1, "loopback_identity_penalties_available": true}
}
func TestProcessBansAllFamiliesAndPolicy(t *testing.T) {
	for _, strict := range []bool{false, true} {
		s := typedSource(t, banFixture())
		findings, err := s.Bans(t.Context(), adapterAt, BanPolicy{SelfIdentityCooldownInvalidates: strict})
		if err != nil {
			t.Fatal(err)
		}
		kinds := map[BanKind]BanFinding{}
		for _, finding := range findings {
			kinds[finding.Kind] = finding
		}
		expected := []BanKind{BanScore, BanBlacklist, BanProviderIP, BanIPWide, BanRemoteIPWide, BanPeer, BanRemotePeer, BanLoopbackScore, BanLoopbackBanned}
		if strict {
			expected = append(expected, BanSelfIdentityCooldown)
		}
		if len(kinds) != len(expected) {
			t.Fatalf("lost ban family: %v", kinds)
		}
		for _, kind := range expected {
			if _, ok := kinds[kind]; !ok {
				t.Fatalf("lost kind %s", kind)
			}
		}
		if kinds[BanLoopbackScore].Count != 2 || kinds[BanLoopbackBanned].Count != 1 {
			t.Fatal("loopback aggregates lost")
		}
	}
	fixture := banFixture()
	fixture["local_ip_bans"] = []any{map[string]any{"ip": "192.0.2.4", "until": adapterAt}}
	findings, err := typedSource(t, fixture).Bans(t.Context(), adapterAt, BanPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range findings {
		if finding.Kind == BanIPWide {
			t.Fatal("expired boundary counted as active")
		}
	}
}
func TestProcessBansRejectsForgedFreshnessAndPartialRows(t *testing.T) {
	for _, mutant := range []string{"stale_fresh_true", "future_generated", "invalid_ip", "count_mismatch", "missing_peer_policy", "missing_score", "null_family"} {
		t.Run(mutant, func(t *testing.T) {
			fixture := banFixture()
			switch mutant {
			case "stale_fresh_true":
				fixture["observation_started_at"] = adapterAt.Add(-time.Nanosecond)
			case "future_generated":
				fixture["generated_at"] = adapterAt.Add(time.Nanosecond)
			case "invalid_ip":
				fixture["banned_ips"].([]any)[0].(map[string]any)["ip"] = "not-an-ip"
			case "count_mismatch":
				fixture["count"] = 0
			case "missing_peer_policy":
				delete(fixture["peer_bans"].([]any)[0].(map[string]any), "self_identity_cooldown")
			case "missing_score":
				delete(fixture["local_bans"].([]any)[0].(map[string]any), "score")
			case "null_family":
				fixture["remote_ip_bans"] = nil
			}
			if _, err := typedSource(t, fixture).Bans(t.Context(), adapterAt, BanPolicy{}); err == nil {
				t.Fatal("invalid R1 became clear R1")
			}
		})
	}
	s := typedSource(t, banFixture())
	s.clock = &backwardAdapterClock{at: adapterAt}
	if _, err := s.BanObservation(t.Context()); !errors.Is(err, ErrInvalidSample) {
		t.Fatalf("backward source clock accepted: %v", err)
	}
}

type backwardAdapterClock struct {
	at    time.Time
	reads int
}

func (c *backwardAdapterClock) Now() time.Time {
	c.reads++
	if c.reads > 1 {
		return c.at.Add(-time.Nanosecond)
	}
	return c.at
}

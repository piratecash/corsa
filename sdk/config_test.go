package sdk

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestNodeConfigAdvertisePortMapping covers the AdvertisePort plumbing from
// the public *uint16 SDK field through the (Config).internal() boundary into
// the validated *domain.PeerPort consumed by the internal config layer. The
// boundary is the single conversion point between the SDK's primitive optional
// type and the internal domain type, so it must be covered explicitly:
//
//   - nil → nil (operator did not configure the port; internal layer applies
//     EffectiveAdvertisePort fallback to DefaultPeerPort).
//   - valid 1..65535 → pointer to PeerPort(value).
//   - invalid (0; uint16 cannot represent >65535) → nil, matching the
//     CORSA_ADVERTISE_PORT operator-side fallback path.
//
// Without this test the SDK boundary could silently regress to "always nil"
// (the bug this case was created to prevent: an SDK app on a non-default port
// would advertise 64646 instead of its real listening port).
func TestNodeConfigAdvertisePortMapping(t *testing.T) {
	t.Parallel()

	valid := uint16(64648)
	zero := uint16(0)

	cases := []struct {
		name string
		in   *uint16
		want *domain.PeerPort
	}{
		{
			name: "nil_falls_back_to_internal_default",
			in:   nil,
			want: nil,
		},
		{
			name: "valid_64648_propagates_as_peer_port",
			in:   &valid,
			want: peerPortPtr(domain.PeerPort(64648)),
		},
		{
			name: "zero_collapses_to_nil_via_is_valid",
			in:   &zero,
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			cfg.Node.AdvertisePort = tc.in

			got := cfg.internal().Node.AdvertisePort

			switch {
			case tc.want == nil && got == nil:
				return
			case tc.want == nil && got != nil:
				t.Fatalf("AdvertisePort = %v, want nil", *got)
			case tc.want != nil && got == nil:
				t.Fatalf("AdvertisePort = nil, want %v", *tc.want)
			case *tc.want != *got:
				t.Fatalf("AdvertisePort = %v, want %v", *got, *tc.want)
			}
		})
	}
}

func peerPortPtr(p domain.PeerPort) *domain.PeerPort {
	return &p
}

// TestNodeConfigHoldDMUntilReachableMapping pins the SDK boundary for the
// reachability gate: the public *bool field maps into the internal bool with
// "nil means the default, which is ENABLED". Without this test the SDK path
// could silently regress to the bool zero value (false = legacy blind gossip),
// the exact bug this case was created to prevent — an embedded/SDK runtime
// running legacy behaviour despite the operator default being ON.
func TestNodeConfigHoldDMUntilReachableMapping(t *testing.T) {
	t.Parallel()

	tr := true
	fa := false

	cases := []struct {
		name string
		in   *bool
		want bool
	}{
		{name: "nil_defaults_to_enabled", in: nil, want: true},
		{name: "explicit_true_stays_enabled", in: &tr, want: true},
		{name: "explicit_false_is_kill_switch", in: &fa, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			cfg.Node.HoldDMUntilReachable = tc.in

			if got := cfg.internal().Node.HoldDMUntilReachable; got != tc.want {
				t.Fatalf("HoldDMUntilReachable = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNodeConfigEnvelopeRetentionMapping pins the SDK boundary for the
// transit gossip-storm cure: the public *bool maps into the internal bool with
// "nil means the default, which is ENABLED". Guards against the SDK path
// silently shipping the bool zero value (false = legacy no-ceiling), which
// would leave embedded runtimes accepting/re-propagating aged transit DMs.
func TestNodeConfigEnvelopeRetentionMapping(t *testing.T) {
	t.Parallel()

	tr := true
	fa := false

	cases := []struct {
		name string
		in   *bool
		want bool
	}{
		{name: "nil_defaults_to_enabled", in: nil, want: true},
		{name: "explicit_true_stays_enabled", in: &tr, want: true},
		{name: "explicit_false_is_kill_switch", in: &fa, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			cfg.Node.EnvelopeRetentionEnabled = tc.in

			if got := cfg.internal().Node.EnvelopeRetentionEnabled; got != tc.want {
				t.Fatalf("EnvelopeRetentionEnabled = %v, want %v", got, tc.want)
			}
		})
	}
}

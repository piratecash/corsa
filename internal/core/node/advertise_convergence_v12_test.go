package node

// Advertise-address contract tests under the v12 cleanup baseline.
//
// This file collects:
//
//   - the extractAdvertisePort boundary matrix;
//   - the negative-assertion guard that validateAdvertisedAddress never
//     rejects on advertise-learning grounds;
//   - end-to-end positives for world-routable / non-routable observed IPs;
//   - the welcome wire-shape contract: empty Listen, populated
//     AdvertisePort, and observed_address present as NAT-detection
//     telemetry (no longer an authoritative self-advertise source);
//   - the lenient parser contract that a corrupt advertise_port never
//     breaks an entire Frame decode.
//
// See docs/protocol/handshake.md (Advertise Convergence) for the
// wire/behavioural contract these tests pin.

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestExtractAdvertisePort_BoundaryCases pins the port extractor's
// IsValid-gated fallback. Every non-[1..65535] wire value — zero, negative,
// above upper bound — collapses to the PeerPort form of config.DefaultPeerPort
// so the persisted row and the learned candidate endpoint never carry a
// nonsensical port. This is the wire contract: "any non-integer payload,
// missing field, or value outside 1..65535 collapses to DefaultPeerPort".
func TestExtractAdvertisePort_BoundaryCases(t *testing.T) {
	defaultPort, err := strconv.Atoi(config.DefaultPeerPort)
	if err != nil {
		t.Fatalf("config.DefaultPeerPort %q must parse as int: %v", config.DefaultPeerPort, err)
	}
	want := domain.PeerPort(defaultPort)

	cases := []struct {
		name string
		raw  domain.PeerPort
		want domain.PeerPort
	}{
		// Valid range — kept verbatim.
		{name: "low_bound_kept", raw: 1, want: 1},
		{name: "default_port_kept", raw: want, want: want},
		{name: "upper_bound_kept", raw: 65535, want: 65535},
		// Out of range — collapse to DefaultPeerPort.
		{name: "zero_collapses_to_default", raw: 0, want: want},
		{name: "negative_collapses_to_default", raw: -1, want: want},
		{name: "above_upper_bound_collapses_to_default", raw: 65536, want: want},
		{name: "large_collapses_to_default", raw: 1 << 30, want: want},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractAdvertisePort(protocol.Frame{AdvertisePort: tc.raw})
			if got != tc.want {
				t.Fatalf("extractAdvertisePort(AdvertisePort=%d) = %d, want %d",
					tc.raw, got, tc.want)
			}
		})
	}
}

// TestExtractAdvertisePort_AbsentAndStringWireShape verifies the two wire
// shapes the parser must tolerate for hello.advertise_port: field absent
// (zero PeerPort after decode) and a v10-style JSON string preserved by
// PeerPort.UnmarshalJSON. Both collapse to DefaultPeerPort via the
// IsValid-gated fallback without failing the whole Frame decode.
func TestExtractAdvertisePort_AbsentAndStringWireShape(t *testing.T) {
	defaultPort, err := strconv.Atoi(config.DefaultPeerPort)
	if err != nil {
		t.Fatalf("config.DefaultPeerPort %q must parse as int: %v", config.DefaultPeerPort, err)
	}
	wantDefault := domain.PeerPort(defaultPort)

	t.Run("field_absent_collapses_to_default", func(t *testing.T) {
		t.Parallel()
		var frame protocol.Frame
		if err := json.Unmarshal([]byte(`{"type":"hello","listener":"1","listen":"198.51.100.5:64646"}`), &frame); err != nil {
			t.Fatalf("json.Unmarshal hello without advertise_port: %v", err)
		}
		if frame.AdvertisePort != 0 {
			t.Fatalf("frame.AdvertisePort expected zero PeerPort on missing field, got %d", frame.AdvertisePort)
		}
		if got := extractAdvertisePort(frame); got != wantDefault {
			t.Fatalf("extractAdvertisePort on missing field = %d, want %d", got, wantDefault)
		}
	})

	t.Run("string_wire_shape_decodes_and_propagates", func(t *testing.T) {
		t.Parallel()
		var frame protocol.Frame
		if err := json.Unmarshal([]byte(`{"type":"hello","listener":"1","listen":"198.51.100.5:64646","advertise_port":"64646"}`), &frame); err != nil {
			t.Fatalf("json.Unmarshal hello with string advertise_port: %v", err)
		}
		if frame.AdvertisePort != 64646 {
			t.Fatalf("string-shaped advertise_port: got %d want 64646", frame.AdvertisePort)
		}
		if got := extractAdvertisePort(frame); got != 64646 {
			t.Fatalf("extractAdvertisePort on string shape = %d, want 64646", got)
		}
	})

	t.Run("unparseable_string_collapses_to_default", func(t *testing.T) {
		t.Parallel()
		var frame protocol.Frame
		if err := json.Unmarshal([]byte(`{"type":"hello","listener":"1","listen":"198.51.100.5:64646","advertise_port":"abc"}`), &frame); err != nil {
			t.Fatalf("json.Unmarshal hello with garbage advertise_port: %v", err)
		}
		if frame.AdvertisePort != 0 {
			t.Fatalf("garbage advertise_port expected zero PeerPort, got %d", frame.AdvertisePort)
		}
		if got := extractAdvertisePort(frame); got != wantDefault {
			t.Fatalf("extractAdvertisePort on garbage = %d, want %d", got, wantDefault)
		}
	})

	t.Run("out_of_range_string_collapses_to_default", func(t *testing.T) {
		t.Parallel()
		var frame protocol.Frame
		if err := json.Unmarshal([]byte(`{"type":"hello","listener":"1","listen":"198.51.100.5:64646","advertise_port":"70000"}`), &frame); err != nil {
			t.Fatalf("json.Unmarshal hello with out-of-range string: %v", err)
		}
		if frame.AdvertisePort.IsValid() {
			t.Fatalf("70000 must not pass IsValid, got valid=true with value %d", frame.AdvertisePort)
		}
		if got := extractAdvertisePort(frame); got != wantDefault {
			t.Fatalf("extractAdvertisePort on out-of-range string = %d, want %d", got, wantDefault)
		}
	})
}

// TestValidateAdvertisedAddress_NeverRejects is the negative-assertion
// guard required by the v12 cleanup contract: the runtime path must NEVER
// reject an inbound hello on advertise-learning grounds, regardless of
// what the peer declares in hello.listen. Every previously rejection-
// producing shape must now flow through to an accept-branch decision so
// a malformed or disagreeing peer never breaks its own session. The
// surviving accept decisions are non_listener, legacy_direct, match,
// or local_exception.
func TestValidateAdvertisedAddress_NeverRejects(t *testing.T) {
	cases := []struct {
		name        string
		observedTCP string
		frame       protocol.Frame
	}{
		{
			name:        "observed_world_listen_private_was_legacy_reject",
			observedTCP: "203.0.113.50:45123",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "10.0.0.7:64646",
				AdvertisePort: 64646,
			},
		},
		{
			name:        "observed_world_listen_world_different_ip",
			observedTCP: "203.0.113.51:45123",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "198.51.100.9:64646",
				AdvertisePort: 64646,
			},
		},
		{
			name:        "observed_private_legacy_reject_shape",
			observedTCP: "10.0.0.50:45123",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "198.51.100.9:64646",
				AdvertisePort: 64646,
			},
		},
		{
			name:        "unparseable_observed_listen_world",
			observedTCP: "not-an-ip-address",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "198.51.100.9:64646",
				AdvertisePort: 64646,
			},
		},
		{
			name:        "advertise_port_zero_was_legacy_invalid",
			observedTCP: "203.0.113.52:45123",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "198.51.100.9:64646",
				AdvertisePort: 0,
			},
		},
		{
			name:        "advertise_port_out_of_range",
			observedTCP: "203.0.113.53:45123",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "198.51.100.9:64646",
				AdvertisePort: 70000,
			},
		},
		{
			name:        "listen_empty_listener_flag_yes",
			observedTCP: "203.0.113.54:45123",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "",
				AdvertisePort: 64646,
			},
		},
		{
			name:        "listen_host_nonsense",
			observedTCP: "203.0.113.55:45123",
			frame: protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "not-a-host:abc",
				AdvertisePort: 64646,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := validateAdvertisedAddress(tc.observedTCP, tc.frame)
			switch result.Decision {
			case advertiseDecisionMatch,
				advertiseDecisionLocalException,
				advertiseDecisionLegacyDirect,
				advertiseDecisionNonListener:
				// Accept branches — expected.
			default:
				t.Fatalf("v12 contract violation: unexpected decision %q for %s",
					result.Decision, tc.name)
			}
		})
	}
}

// TestValidateAdvertisedAddress_WorldObservedProducesCandidate is the
// end-to-end positive: a world-routable observed IP plus a valid
// advertise_port produces an announceable row whose NormalizedAdvertisedIP
// mirrors the observed IP and whose AdvertisePort propagates unchanged.
// This is the single announce-candidate path under the v12 contract.
func TestValidateAdvertisedAddress_WorldObservedProducesCandidate(t *testing.T) {
	t.Parallel()

	const (
		observedTCP  = "203.0.113.77:45123"
		observedHost = "203.0.113.77"
		advPort      = domain.PeerPort(40404)
	)

	result := validateAdvertisedAddress(observedTCP, protocol.Frame{
		Type:     "hello",
		Listener: "1",
		// hello.listen.host intentionally set to a DIFFERENT world IP
		// to prove the contract ignores it as a truth input.
		Listen:        "198.51.100.55:64646",
		AdvertisePort: advPort,
	})

	if result.Decision != advertiseDecisionMatch {
		t.Fatalf("decision: got %q want %q", result.Decision, advertiseDecisionMatch)
	}
	if result.PersistAnnounceState != announceStateAnnounceable {
		t.Fatalf("announce_state: got %q want %q", result.PersistAnnounceState, announceStateAnnounceable)
	}
	if result.PersistWriteMode != persistWriteModeCreateOrUpdate {
		t.Fatalf("write_mode: got %q want %q", result.PersistWriteMode, persistWriteModeCreateOrUpdate)
	}
	if !result.AllowAnnounce {
		t.Fatalf("AllowAnnounce must be true for world-routable observed match")
	}
	if result.NormalizedObservedIP.String() != observedHost {
		t.Fatalf("NormalizedObservedIP: got %q want %q", result.NormalizedObservedIP.String(), observedHost)
	}
	// The trusted advertised IP mirrors the observed IP, not the
	// hello.listen host. This is the assertion that pins "observed IP
	// is the sole truth source".
	if result.NormalizedAdvertisedIP.String() != observedHost {
		t.Fatalf("NormalizedAdvertisedIP: got %q want %q (must mirror observed IP, NOT hello.listen host)",
			result.NormalizedAdvertisedIP.String(), observedHost)
	}
	if result.ObservedIPHint.String() != observedHost {
		t.Fatalf("ObservedIPHint: got %q want %q", result.ObservedIPHint.String(), observedHost)
	}
	if result.AdvertisePort != advPort {
		t.Fatalf("AdvertisePort propagation: got %d want %d", result.AdvertisePort, advPort)
	}
}

// TestValidateAdvertisedAddress_LocalObservedNoAnnounce pins the invariant
// "private observed IP → no announce candidate, no leak via peer exchange".
// A peer reached via a non-routable observed IP still produces an accept
// decision (the contract never rejects on advertise-learning grounds) but
// AllowAnnounce must be false, the persisted announce_state must be
// direct_only, and ObservedIPHint must carry the canonical observed IP so
// the runtime history can still record the private observation.
// NormalizedAdvertisedIP is intentionally NOT asserted here: the runtime
// echoes the self-reported hello.listen host as a diagnostic even on
// non-routable observations, and the peer-exchange leak guard lives on
// AllowAnnounce / PersistAnnounceState, not on that diagnostic.
func TestValidateAdvertisedAddress_LocalObservedNoAnnounce(t *testing.T) {
	cases := []struct {
		name        string
		observedTCP string
	}{
		{name: "rfc1918_observed", observedTCP: "10.0.0.5:45123"},
		{name: "cgnat_observed", observedTCP: "100.64.0.5:45123"},
		{name: "link_local_observed", observedTCP: "169.254.1.1:45123"},
		{name: "loopback_observed", observedTCP: "127.0.0.1:45123"},
		{name: "ula_observed", observedTCP: "[fc00::5]:45123"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := validateAdvertisedAddress(tc.observedTCP, protocol.Frame{
				Type:          "hello",
				Listener:      "1",
				Listen:        "198.51.100.9:64646",
				AdvertisePort: 64646,
			})
			if result.Decision != advertiseDecisionLocalException {
				t.Fatalf("decision: got %q want %q", result.Decision, advertiseDecisionLocalException)
			}
			if result.AllowAnnounce {
				t.Fatalf("AllowAnnounce must be false for non-routable observed IP (peer-exchange leak guard)")
			}
			if result.PersistAnnounceState != announceStateDirectOnly {
				t.Fatalf("announce_state: got %q want %q", result.PersistAnnounceState, announceStateDirectOnly)
			}
			if result.PersistWriteMode != persistWriteModeCreateOrUpdate {
				t.Fatalf("write_mode: got %q want %q", result.PersistWriteMode, persistWriteModeCreateOrUpdate)
			}
			// ObservedIPHint must carry the canonical observed IP so
			// the runtime observed-IP history still records the private
			// observation (diagnostics / later-session reconciliation).
			if !result.ObservedIPHint.IsValid() {
				t.Fatalf("ObservedIPHint must expose the canonical observed IP even on non-routable observations")
			}
		})
	}
}

// TestWelcomeFrame_AdvertiseAndObservedFields asserts the wire-shape
// invariants for the welcome frame:
//   - Listen is EMPTY (the v12 cleanup contract — host is no longer
//     a wire concept, and AdvertisePort carries the listening port);
//   - ObservedAddress is still emitted; it feeds recordObservedAddress
//     on the dialer side, which in turn drives the "NAT detected"
//     telemetry. Under the v12 baseline the field is diagnostic
//     telemetry only — there is no authoritative self-advertise
//     consumer left in production code (selfAdvertiseEndpoint and
//     observedConsensusIPLocked were removed because the emit path
//     no longer publishes a Listen host);
//   - AdvertisePort is carried as a JSON integer;
//   - ProtocolVersion / MinimumProtocolVersion reflect the current floor.
//
// This is the regression guard that catches both directions of the
// contract: a refactor that re-introduces host on the wire (Listen
// non-empty) AND a refactor that drops observed_address (cutting the
// NAT-detection telemetry input).
func TestWelcomeFrame_AdvertiseAndObservedFields(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := &Service{
		identity: id,
		cfg: config.Node{
			Type:            config.NodeTypeFull,
			ListenerEnabled: true,
			ListenerSet:     true,
		},
	}
	frame := svc.welcomeFrame("challenge-xyz", "203.0.113.99")

	if frame.Type != "welcome" {
		t.Fatalf("frame.Type: got %q want %q", frame.Type, "welcome")
	}
	if frame.Listen != "" {
		t.Fatalf("welcome.listen must be empty under the v12 contract (host is not a wire concept), got %q", frame.Listen)
	}
	if frame.ObservedAddress != "203.0.113.99" {
		t.Fatalf("welcome.observed_address must be carried (outbound consensus input), got %q", frame.ObservedAddress)
	}
	if !frame.AdvertisePort.IsValid() {
		t.Fatalf("welcome.advertise_port must be populated, got %d", frame.AdvertisePort)
	}
	if frame.Version != config.ProtocolVersion {
		t.Fatalf("welcome.protocol_version: got %d want %d", frame.Version, config.ProtocolVersion)
	}
	if frame.MinimumProtocolVersion != config.MinimumProtocolVersion {
		t.Fatalf("welcome.minimum_protocol_version: got %d want %d",
			frame.MinimumProtocolVersion, config.MinimumProtocolVersion)
	}

	// Wire-level check: the marshalled JSON must contain observed_address
	// so an outbound dialer keyed on that field name still finds it.
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("json.Marshal(welcome): %v", err)
	}
	if !containsKey(raw, "observed_address") {
		t.Fatalf("welcome JSON must carry observed_address key (outbound consensus), got %s", raw)
	}
	if !containsKey(raw, "advertise_port") {
		t.Fatalf("welcome JSON must carry advertise_port key, got %s", raw)
	}
	if containsKey(raw, "listen") {
		t.Fatalf("welcome JSON must NOT carry listen key under v12 contract, got %s", raw)
	}
	// advertise_port must be an integer on the wire — not a JSON string.
	if bytesContains(raw, []byte(`"advertise_port":"`)) {
		t.Fatalf("welcome.advertise_port must be JSON integer, not string, got %s", raw)
	}
}

// TestFrame_LenientAdvertisePortNeverBreaksDecode is the parser-side
// contract test: no matter how corrupt the advertise_port wire payload
// is, the whole Frame must still decode without error. A single
// malformed field cannot fail an entire hello/welcome frame.
func TestFrame_LenientAdvertisePortNeverBreaksDecode(t *testing.T) {
	raws := []string{
		`{"type":"hello","advertise_port":64646}`,
		`{"type":"hello","advertise_port":"64646"}`,
		`{"type":"hello","advertise_port":""}`,
		`{"type":"hello","advertise_port":"abc"}`,
		`{"type":"hello","advertise_port":null}`,
		`{"type":"hello","advertise_port":64646.5}`,
		`{"type":"hello","advertise_port":-1}`,
		`{"type":"hello","advertise_port":70000}`,
		// Field missing entirely — pins the wire contract
		// "absent field == zero PeerPort".
		`{"type":"hello"}`,
	}
	for _, raw := range raws {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			var frame protocol.Frame
			if err := json.Unmarshal([]byte(raw), &frame); err != nil {
				t.Fatalf("Frame decode must never fail on corrupt advertise_port, got error %v for %s", err, raw)
			}
			if frame.Type != "hello" {
				t.Fatalf("Frame.Type: got %q want %q (decode corruption)", frame.Type, "hello")
			}
		})
	}
}

// TestValidateAdvertisedAddress_StatelessAcceptDoesNotEmitMismatch is
// the negative-assertion guard required by §12.2 of the v12 cleanup
// plan: even with a hello whose listen.host disagrees with the observed
// TCP IP, the accept branch must not synthesise any mismatch wire
// signal. There is no longer a place in advertiseValidationResult to
// carry such a notice; this test pins the structural absence.
func TestValidateAdvertisedAddress_StatelessAcceptDoesNotEmitMismatch(t *testing.T) {
	t.Parallel()

	result := validateAdvertisedAddress("203.0.113.250:45000", protocol.Frame{
		Type:          "hello",
		Listener:      "1",
		Listen:        "198.51.100.250:64646",
		AdvertisePort: 64646,
	})

	if result.Decision != advertiseDecisionMatch {
		t.Fatalf("decision: got %q want %q (world-routable observed must accept as match)",
			result.Decision, advertiseDecisionMatch)
	}
	if !result.AllowAnnounce {
		t.Fatalf("world-routable observed must produce AllowAnnounce=true")
	}
}

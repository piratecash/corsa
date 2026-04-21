package node

// V11 contract tests for the advertise-address phase 1 deprecation rollout.
// Kept in a dedicated file so reviewers can spot the negative-assertion
// guards (runtime path NEVER rejects / emits mismatch notices) and the
// extractAdvertisePort boundary matrix without wading through the full
// v10 convergence matrix in advertise_convergence_test.go.
//
// See docs/advertise-address-phase1-deprecation.md §14.2 for the checklist
// these tests cover.

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
// nonsensical port. This is the §7.2 wire contract: "any non-integer
// payload, missing field, or value outside 1..65535 collapses to
// DefaultPeerPort".
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
// shapes a v11 parser must tolerate for hello.advertise_port: field absent
// (zero PeerPort after decode) and a v10-style JSON string. Both collapse
// to DefaultPeerPort via the IsValid-gated fallback without failing the
// whole Frame decode.
func TestExtractAdvertisePort_AbsentAndStringWireShape(t *testing.T) {
	defaultPort, err := strconv.Atoi(config.DefaultPeerPort)
	if err != nil {
		t.Fatalf("config.DefaultPeerPort %q must parse as int: %v", config.DefaultPeerPort, err)
	}
	wantDefault := domain.PeerPort(defaultPort)

	t.Run("field_absent_collapses_to_default", func(t *testing.T) {
		t.Parallel()
		// A hello frame that simply omits advertise_port decodes to the
		// zero PeerPort, which IsValid rejects — extractAdvertisePort
		// must fall back to DefaultPeerPort.
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
		// A v10 peer that historically emitted the port as a string must
		// still decode; PeerPort.UnmarshalJSON unwraps "\"64646\"" to 64646
		// and extractAdvertisePort propagates the valid value unchanged.
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
		// Garbage string payload must not fail the Frame decode; the
		// lenient PeerPort.UnmarshalJSON collapses it to zero, then
		// extractAdvertisePort routes that through the DefaultPeerPort
		// fallback.
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
		// A string payload that parses as an integer but falls outside
		// 1..65535 must also collapse — the advertise-convergence layer
		// rejects it at IsValid, and extractAdvertisePort substitutes
		// DefaultPeerPort.
		var frame protocol.Frame
		if err := json.Unmarshal([]byte(`{"type":"hello","listener":"1","listen":"198.51.100.5:64646","advertise_port":"70000"}`), &frame); err != nil {
			t.Fatalf("json.Unmarshal hello with out-of-range string: %v", err)
		}
		// 70000 fails the 1..65535 gate — extractAdvertisePort must
		// substitute DefaultPeerPort rather than trust the raw value.
		if frame.AdvertisePort.IsValid() {
			t.Fatalf("70000 must not pass IsValid, got valid=true with value %d", frame.AdvertisePort)
		}
		if got := extractAdvertisePort(frame); got != wantDefault {
			t.Fatalf("extractAdvertisePort on out-of-range string = %d, want %d", got, wantDefault)
		}
	})
}

// TestValidateAdvertisedAddress_V11NeverRejects is the negative-assertion
// guard required by §14.2: under the phase 1 deprecation contract the
// runtime path must NEVER flip ShouldReject=true and NEVER attach a
// RejectNotice to the result, regardless of what the peer declares in
// hello.listen. Every previously rejection-producing shape must now flow
// through to an accept-branch decision so a malformed or disagreeing peer
// never breaks its own session.
func TestValidateAdvertisedAddress_V11NeverRejects(t *testing.T) {
	cases := []struct {
		name        string
		observedTCP string
		frame       protocol.Frame
	}{
		{
			name:        "observed_world_listen_private_was_v10_reject",
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
			name:        "observed_private_legacy_v10_reject_shape",
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
			name:        "advertise_port_zero_was_v10_invalid",
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
			if result.ShouldReject {
				t.Fatalf("v11 contract violation: ShouldReject=true for %s (decision=%q)",
					tc.name, result.Decision)
			}
			if result.RejectNotice != nil {
				t.Fatalf("v11 contract violation: RejectNotice attached for %s (decision=%q)",
					tc.name, result.Decision)
			}
			// observed-address-mismatch must not be produced by the
			// штатный path under any circumstance. Any surviving decision
			// value produced from this helper is one of the accept
			// branches — world_mismatch is legacy-only and unreachable here.
			if result.Decision == advertiseDecisionWorldMismatch {
				t.Fatalf("v11 contract violation: decision=world_mismatch for %s (штатный path must never produce this)", tc.name)
			}
			if result.Decision == advertiseDecisionInvalid {
				t.Fatalf("v11 contract violation: decision=invalid for %s (штатный path must never produce this)", tc.name)
			}
		})
	}
}

// TestValidateAdvertisedAddress_WorldObservedProducesCandidate is the
// end-to-end positive under v11: a world-routable observed IP plus a
// valid advertise_port produces an announceable row whose
// NormalizedAdvertisedIP mirrors the observed IP and whose
// AdvertisePort propagates unchanged. This is the single announce
// candidate path under the phase 1 contract.
func TestValidateAdvertisedAddress_WorldObservedProducesCandidate(t *testing.T) {
	t.Parallel()

	const (
		observedTCP  = "203.0.113.77:45123"
		observedHost = "203.0.113.77"
		advPort      = domain.PeerPort(40404)
	)

	result := validateAdvertisedAddress(observedTCP, protocol.Frame{
		Type:          "hello",
		Listener:      "1",
		// hello.listen.host intentionally set to a DIFFERENT world IP
		// to prove v11 ignores it as a truth input.
		Listen:        "198.51.100.55:64646",
		AdvertisePort: advPort,
	})

	if result.Decision != advertiseDecisionMatch {
		t.Fatalf("decision: got %q want %q", result.Decision, advertiseDecisionMatch)
	}
	if result.ShouldReject {
		t.Fatalf("v11 accept branch must not flip ShouldReject")
	}
	if result.RejectNotice != nil {
		t.Fatalf("v11 accept branch must not attach a RejectNotice")
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
	if result.NormalizedObservedIP != domain.PeerIP(observedHost) {
		t.Fatalf("NormalizedObservedIP: got %q want %q", result.NormalizedObservedIP, observedHost)
	}
	// v11: the trusted advertised IP mirrors the observed IP, not the
	// hello.listen host. This is the assertion that pins "observed IP
	// is the sole truth source".
	if result.NormalizedAdvertisedIP != domain.PeerIP(observedHost) {
		t.Fatalf("NormalizedAdvertisedIP: got %q want %q (must mirror observed IP, NOT hello.listen host)",
			result.NormalizedAdvertisedIP, observedHost)
	}
	if result.ObservedIPHint != domain.PeerIP(observedHost) {
		t.Fatalf("ObservedIPHint: got %q want %q", result.ObservedIPHint, observedHost)
	}
	if result.AdvertisePort != advPort {
		t.Fatalf("AdvertisePort propagation: got %d want %d", result.AdvertisePort, advPort)
	}
}

// TestValidateAdvertisedAddress_LocalObservedNoAnnounce pins the §14.2
// invariant "private observed IP → no announce candidate, no leak via
// peer exchange". A peer reached via a non-routable observed IP still
// produces an accept decision (v11 never rejects on advertise-learning
// grounds) but AllowAnnounce must be false, the persisted announce_state
// must be direct_only, and ObservedIPHint must carry the canonical
// observed IP so the runtime history can still record the private
// observation. NormalizedAdvertisedIP is intentionally NOT asserted here:
// the runtime echoes the self-reported hello.listen host as a diagnostic
// even on non-routable observations, and the peer-exchange leak guard
// lives on AllowAnnounce / PersistAnnounceState, not on that diagnostic.
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
			if result.ShouldReject {
				t.Fatalf("v11 accept branch must not flip ShouldReject")
			}
			if result.RejectNotice != nil {
				t.Fatalf("v11 accept branch must not attach a RejectNotice")
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
			if result.ObservedIPHint == "" {
				t.Fatalf("ObservedIPHint must expose the canonical observed IP even on non-routable observations")
			}
		})
	}
}

// TestHandleObservedAddressMismatchNotice_WeakHintGate verifies that a
// legacy v10 notice is dropped on the floor when v11 passive learning has
// already produced an observed consensus. The override must survive
// untouched — otherwise a single stray v10 peer could silently replace
// a stronger learned endpoint.
func TestHandleObservedAddressMismatchNotice_WeakHintGate(t *testing.T) {
	const (
		consensusIP domain.PeerIP = "203.0.113.80"
		legacyHint  string        = "198.51.100.77"
	)

	svc := newAdvertiseTestService("198.51.100.10:64646")
	// Seed the consensus map with two matching votes so
	// observedConsensusIPLocked crosses observedAddrConsensusThreshold.
	svc.observedAddrs = map[domain.PeerIdentity]string{
		"peer-aaa": string(consensusIP),
		"peer-bbb": string(consensusIP),
	}

	details, err := protocol.MarshalObservedAddressMismatchDetails(legacyHint)
	if err != nil {
		t.Fatalf("MarshalObservedAddressMismatchDetails: %v", err)
	}
	svc.handleObservedAddressMismatchNotice(protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Status:  protocol.ConnectionStatusClosing,
		Code:    protocol.ErrCodeObservedAddressMismatch,
		Details: details,
	})

	if svc.trustedSelfAdvertiseIP != "" {
		t.Fatalf("v11 weak-hint gate breached: trustedSelfAdvertiseIP=%q (must stay empty when consensus exists)",
			svc.trustedSelfAdvertiseIP)
	}
	// selfAdvertiseEndpoint must publish the consensus IP, not the
	// legacy hint — this is the observable effect callers rely on.
	want := string(consensusIP) + ":" + config.DefaultPeerPort
	if got := svc.selfAdvertiseEndpoint(); got != want {
		t.Fatalf("selfAdvertiseEndpoint: got %q want %q", got, want)
	}
}

// TestHandleObservedAddressMismatchNotice_AppliedWithoutConsensus is the
// companion fallback: without a v11 consensus the legacy notice is still
// allowed to seed trustedSelfAdvertiseIP so early-lifetime nodes
// (not yet accumulated inbound observations) can still learn a routable
// self-endpoint from a v10 peer. Applied value must be canonicalised.
func TestHandleObservedAddressMismatchNotice_AppliedWithoutConsensus(t *testing.T) {
	const legacyHint = "203.0.113.99"

	svc := newAdvertiseTestService("198.51.100.10:64646")
	// observedAddrs left empty on purpose — no v11 consensus yet.

	details, err := protocol.MarshalObservedAddressMismatchDetails(legacyHint)
	if err != nil {
		t.Fatalf("MarshalObservedAddressMismatchDetails: %v", err)
	}
	svc.handleObservedAddressMismatchNotice(protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Status:  protocol.ConnectionStatusClosing,
		Code:    protocol.ErrCodeObservedAddressMismatch,
		Details: details,
	})

	if string(svc.trustedSelfAdvertiseIP) != legacyHint {
		t.Fatalf("trustedSelfAdvertiseIP: got %q want %q (legacy notice must seed the override when no consensus exists)",
			svc.trustedSelfAdvertiseIP, legacyHint)
	}
	want := legacyHint + ":" + config.DefaultPeerPort
	if got := svc.selfAdvertiseEndpoint(); got != want {
		t.Fatalf("selfAdvertiseEndpoint: got %q want %q", got, want)
	}
}

// TestHandleObservedAddressMismatchNotice_PrivateHintRejected guards the
// non-routable observed-IP filter. A peer reporting 10.x / 192.168.x /
// loopback must never be allowed to downgrade our advertise endpoint to
// a private range — the override stays empty and the node falls back to
// cfg.AdvertiseAddress.
func TestHandleObservedAddressMismatchNotice_PrivateHintRejected(t *testing.T) {
	cases := []string{
		"10.0.0.7",
		"192.168.1.1",
		"127.0.0.1",
		"100.64.0.5",
		"fc00::5",
	}
	for _, hint := range cases {
		hint := hint
		t.Run(hint, func(t *testing.T) {
			t.Parallel()
			svc := newAdvertiseTestService("198.51.100.10:64646")
			details, err := protocol.MarshalObservedAddressMismatchDetails(hint)
			if err != nil {
				t.Fatalf("MarshalObservedAddressMismatchDetails(%q): %v", hint, err)
			}
			svc.handleObservedAddressMismatchNotice(protocol.Frame{
				Type:    protocol.FrameTypeConnectionNotice,
				Status:  protocol.ConnectionStatusClosing,
				Code:    protocol.ErrCodeObservedAddressMismatch,
				Details: details,
			})
			if svc.trustedSelfAdvertiseIP != "" {
				t.Fatalf("private hint %q must not seed trustedSelfAdvertiseIP, got %q",
					hint, svc.trustedSelfAdvertiseIP)
			}
		})
	}
}

// TestWelcomeFrame_V11CompatFields asserts the wire-compat invariants for
// the welcome frame emitted by a v11 responder: ObservedAddress field is
// still present (legacy v10 parsers consume it), AdvertisePort is carried
// as a JSON integer (v11 wire contract), and ProtocolVersion / MinimumProtocolVersion
// reflect the post-phase-1 values. This is the single test that catches a
// regression where a refactor drops observed_address and breaks v10 peers
// that still rely on it for self-learning.
func TestWelcomeFrame_V11CompatFields(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := &Service{
		identity: id,
		cfg: config.Node{
			Type:             config.NodeTypeFull,
			ListenerEnabled:  true,
			ListenerSet:      true,
			AdvertiseAddress: "203.0.113.10:64646",
		},
	}
	frame := svc.welcomeFrame("challenge-xyz", "203.0.113.99")

	if frame.Type != "welcome" {
		t.Fatalf("frame.Type: got %q want %q", frame.Type, "welcome")
	}
	if frame.ObservedAddress != "203.0.113.99" {
		t.Fatalf("welcome.observed_address must be carried for v10 compat, got %q", frame.ObservedAddress)
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
	// so a v10 parser keyed on that field name still finds it.
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("json.Marshal(welcome): %v", err)
	}
	if !containsKey(raw, "observed_address") {
		t.Fatalf("welcome JSON must carry observed_address key for v10 compat, got %s", raw)
	}
	if !containsKey(raw, "advertise_port") {
		t.Fatalf("welcome JSON must carry advertise_port key for v11 receivers, got %s", raw)
	}
	// advertise_port must be an integer on the wire — not a JSON string.
	// The raw form for a typical port like 64646 should contain
	// `"advertise_port":64646`, not `"advertise_port":"64646"`.
	if bytesContains(raw, []byte(`"advertise_port":"`)) {
		t.Fatalf("welcome.advertise_port must be JSON integer, not string, got %s", raw)
	}
}

// TestSelfAdvertiseEndpoint_LearnedObservedWinsOverConfig pins the §14.2
// checklist item "learned observed IP has priority over
// CORSA_ADVERTISE_ADDRESS". Once two inbound peers report the same
// observed IP, observedConsensusIPLocked crosses the threshold and
// selfAdvertiseEndpoint must return that IP paired with the operator's
// EffectiveAdvertisePort — the host from CORSA_ADVERTISE_ADDRESS is
// demoted to a last-resort fallback.
func TestSelfAdvertiseEndpoint_LearnedObservedWinsOverConfig(t *testing.T) {
	t.Parallel()
	svc := newAdvertiseTestService("198.51.100.10:64646") // legacy cfg host
	svc.observedAddrs = map[domain.PeerIdentity]string{
		"peer-aaa": "203.0.113.200",
		"peer-bbb": "203.0.113.200",
	}
	want := "203.0.113.200:" + config.DefaultPeerPort
	if got := svc.selfAdvertiseEndpoint(); got != want {
		t.Fatalf("selfAdvertiseEndpoint: got %q want %q (learned observed must override cfg host)", got, want)
	}
}

// TestSelfAdvertiseEndpoint_LearnedObservedWinsOverLegacyOverride stacks
// the priority further: even if a legacy v10 mismatch notice previously
// seeded trustedSelfAdvertiseIP, a subsequent v11 consensus must win.
// This catches the regression where the v10 override silently pinned
// the runtime to a stale address after consensus was reached.
func TestSelfAdvertiseEndpoint_LearnedObservedWinsOverLegacyOverride(t *testing.T) {
	t.Parallel()
	svc := newAdvertiseTestService("198.51.100.10:64646")
	svc.trustedSelfAdvertiseIP = "203.0.113.210" // seeded by legacy notice
	svc.observedAddrs = map[domain.PeerIdentity]string{
		"peer-aaa": "203.0.113.211",
		"peer-bbb": "203.0.113.211",
	}
	want := "203.0.113.211:" + config.DefaultPeerPort
	if got := svc.selfAdvertiseEndpoint(); got != want {
		t.Fatalf("selfAdvertiseEndpoint: got %q want %q (v11 consensus must override legacy hint)", got, want)
	}
}

// TestFrame_LenientAdvertisePortNeverBreaksDecode is the parser-side
// contract test: no matter how corrupt the advertise_port wire payload
// is, the whole Frame must still decode without error. This is the
// mixed-network compat guarantee — a single malformed field cannot fail
// an entire hello/welcome frame.
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
		// Field missing entirely — not a regression risk but pins the
		// wire contract "absent field == zero PeerPort".
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

package node

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/transport"
)

// TestValidateAdvertisedAddress covers every accept branch of the v12
// advertise convergence decision matrix. The helper is a pure function:
// no Service state, no network side effects — so each row of the matrix
// maps directly to one test case. The v12 contract says the helper
// never rejects on advertise-learning grounds, so the case shape no
// longer carries notice / reject expectations.
func TestValidateAdvertisedAddress(t *testing.T) {
	cases := []struct {
		name              string
		observedTCP       string
		frame             protocol.Frame
		wantDecision      advertiseDecision
		wantAnnounceState announceState
		wantWriteMode     persistWriteMode
	}{
		{
			name:        "non_listener_explicit",
			observedTCP: "203.0.113.10:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "0",
				Listen:   "",
			},
			wantDecision:      advertiseDecisionNonListener,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			// Legacy frame with no explicit listener flag AND no usable
			// listen string: falls to legacy_direct (accept, direct_only)
			// because we cannot claim the peer is a listener without an
			// explicit Listener="1" flag and without a usable listen
			// endpoint. An explicit Listener="1" with the same
			// world-routable observed IP would instead produce `match` —
			// that path is covered by match_after_canonicalisation /
			// world_routable_observed_overrides_listen_host below. This
			// branch is reachable today only via desktop / local RPC
			// frames that legitimately omit listener; v12 peer hellos
			// always set listener explicitly.
			name:        "legacy_direct_no_listen",
			observedTCP: "203.0.113.11:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "",
				Listen:   "",
			},
			wantDecision:      advertiseDecisionLegacyDirect,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			// Legacy frame with wildcard bind in hello.listen and no
			// explicit listener flag. usableListen rejects 0.0.0.0 as a
			// dial target, and without Listener="1" we cannot promote
			// the peer to announceable — the branch falls to legacy_direct.
			// Reachable in the v12 baseline only through legacy / mixed
			// fixtures that still echo listen on the wire.
			name:        "legacy_direct_wildcard_bind",
			observedTCP: "203.0.113.12:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "",
				Listen:   "0.0.0.0:64646",
			},
			wantDecision:      advertiseDecisionLegacyDirect,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			name:        "match_after_canonicalisation",
			observedTCP: "203.0.113.13:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "203.0.113.13:64646",
			},
			wantDecision:      advertiseDecisionMatch,
			wantAnnounceState: announceStateAnnounceable,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			name:        "local_exception_private_observed_public_advertise",
			observedTCP: "192.168.1.20:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "203.0.113.14:64646",
			},
			wantDecision:      advertiseDecisionLocalException,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			// v12 baseline: hello.listen is not a truth input (and
			// emitters omit it entirely; this fixture echoes it as a
			// legacy/mixed-network shape). The observed IP is
			// world-routable, so the decision is match and the runtime
			// NEVER rejects on the disagreement nor emits any mismatch
			// wire signal. The pre-v11 matrix classified this case as
			// world_mismatch; the test name is preserved for git history
			// while the assertion pins the v12 accept-only outcome.
			name:        "world_routable_observed_overrides_listen_host",
			observedTCP: "203.0.113.15:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "198.51.100.99:64646",
			},
			wantDecision:      advertiseDecisionMatch,
			wantAnnounceState: announceStateAnnounceable,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			// v12 baseline: peer claiming a private range in hello.listen
			// while coming in on a world-routable observed IP is
			// still announceable — the observed IP wins. The pre-v11
			// path rejected on "world mismatch"; the runtime no longer
			// uses hello.listen as truth, so no reject / no notice.
			name:        "world_routable_observed_with_private_listen_is_match",
			observedTCP: "203.0.113.16:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "10.0.0.7:64646",
			},
			wantDecision:      advertiseDecisionMatch,
			wantAnnounceState: announceStateAnnounceable,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			// v12 baseline: an unparseable observed TCP address falls
			// to local_exception rather than invalid-reject. We accept
			// the handshake but never learn a candidate — the non-IP
			// transport case stays compatible with the advertise-
			// learning contract (no observed-IP rewrite for non-IP
			// transports).
			name:        "unparseable_observed_is_local_exception",
			observedTCP: "not-a-host:port",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "203.0.113.17:64646",
			},
			wantDecision:      advertiseDecisionLocalException,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := validateAdvertisedAddress(tc.observedTCP, tc.frame)
			if result.Decision != tc.wantDecision {
				t.Fatalf("decision mismatch: got %q want %q", result.Decision, tc.wantDecision)
			}
			if result.PersistAnnounceState != tc.wantAnnounceState {
				t.Fatalf("announce_state mismatch: got %q want %q", result.PersistAnnounceState, tc.wantAnnounceState)
			}
			if result.PersistWriteMode != tc.wantWriteMode {
				t.Fatalf("write_mode mismatch: got %q want %q", result.PersistWriteMode, tc.wantWriteMode)
			}
		})
	}
}

// containsKey does a byte-level scan for a JSON key name. We cannot use
// json.Unmarshal to assert absence — that would hide the presence of a
// literal "null" value under the same key. We only care that the raw
// wire form does not contain the key at all.
func containsKey(raw []byte, key string) bool {
	needle := []byte(`"` + key + `"`)
	return bytesContains(raw, needle)
}

func bytesContains(haystack, needle []byte) bool {
	n, m := len(haystack), len(needle)
	if m == 0 || m > n {
		return false
	}
	for i := 0; i+m <= n; i++ {
		match := true
		for j := 0; j < m; j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// newAdvertiseTestService builds a minimal Service usable by the
// advertise-convergence unit tests. No goroutines are started, no net
// listener is opened — only the maps the convergence helpers read and
// write. Tests that need to mutate cfg fields supply them on the
// returned pointer.
func newAdvertiseTestService() *Service {
	return &Service{
		cfg: config.Node{
			Type:            config.NodeTypeFull,
			ListenerEnabled: true,
			ListenerSet:     true,
		},
		persistedMeta:           make(map[domain.PeerAddress]*peerEntry),
		observedAddrs:           make(map[domain.PeerIdentity]string),
		observedIPHistoryByPeer: make(map[domain.PeerAddress][]domain.PeerIP),
		remoteBannedIPs:         make(map[string]remoteIPBanEntry),
	}
}

// TestApplyAdvertiseValidationResult_StickyState asserts that a peer
// already marked announceable is NOT downgraded by a subsequent
// non_listener or legacy_direct decision. Those two decisions describe
// the current session only — they must not clobber the trust learned
// from an earlier match. Only local_exception is an explicit downgrade
// trigger after the v12 cleanup removed the world_mismatch decision.
func TestApplyAdvertiseValidationResult_StickyState(t *testing.T) {
	cases := []struct {
		name     string
		decision advertiseDecision
	}{
		{name: "non_listener_must_not_downgrade", decision: advertiseDecisionNonListener},
		{name: "legacy_direct_must_not_downgrade", decision: advertiseDecisionLegacyDirect},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			svc := newAdvertiseTestService()
			const peerAddr domain.PeerAddress = "203.0.113.40:64646"
			svc.persistedMeta[peerAddr] = &peerEntry{
				Address:                peerAddr,
				AnnounceState:          announceStateAnnounceable,
				TrustedAdvertiseIP:     "203.0.113.40",
				TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
				TrustedAdvertisePort:   config.DefaultPeerPort,
			}
			svc.applyAdvertiseValidationResult(peerAddr, advertiseValidationResult{
				Decision:             tc.decision,
				PersistAnnounceState: announceStateDirectOnly,
				PersistWriteMode:     persistWriteModeCreateOrUpdate,
			})
			pm := svc.persistedMeta[peerAddr]
			if pm == nil {
				t.Fatalf("persistedMeta entry vanished")
			}
			if pm.AnnounceState != announceStateAnnounceable {
				t.Fatalf("announce_state downgraded: got %q want %q", pm.AnnounceState, announceStateAnnounceable)
			}
			if pm.TrustedAdvertiseIP != "203.0.113.40" {
				t.Fatalf("trusted_advertise_ip mutated: got %q", pm.TrustedAdvertiseIP)
			}
			if pm.TrustedAdvertiseSource != trustedAdvertiseSourceInbound {
				t.Fatalf("trusted_advertise_source mutated: got %q", pm.TrustedAdvertiseSource)
			}
		})
	}
}

// TestApplyAdvertiseValidationResult_UpdateExistingSkipsCreate verifies
// that a downgrade-only persist write does NOT create a new persistedMeta
// row when the peer was previously unknown. Downgrade-only write mode
// must not inject rows for unknown peers — that would inflate the top-500
// persistence budget with hostile data.
func TestApplyAdvertiseValidationResult_UpdateExistingSkipsCreate(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "203.0.113.42:64646"
	svc.applyAdvertiseValidationResult(peerAddr, advertiseValidationResult{
		Decision:             advertiseDecisionLocalException,
		ObservedIPHint:       "198.51.100.42",
		PersistAnnounceState: announceStateDirectOnly,
		PersistWriteMode:     persistWriteModeUpdateExisting,
	})
	if _, ok := svc.persistedMeta[peerAddr]; ok {
		t.Fatalf("unexpected persistedMeta row created for unknown peer under update-existing-only mode")
	}
	// Runtime observation hint should still be recorded so the
	// outbound convergence loop can use it during the same session.
	if history := svc.observedIPHistoryForPeer(peerAddr); len(history) != 1 {
		t.Fatalf("observed IP history not updated: %v", history)
	}
}

// TestApplyAdvertiseValidationResult_LocalExceptionDowngrades verifies
// that local_exception decision against an already-announceable peer
// downgrades to direct_only. The sticky rule allows downgrade only on
// explicit triggers — local_exception is one of them.
func TestApplyAdvertiseValidationResult_LocalExceptionDowngrades(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "203.0.113.43:64646"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                peerAddr,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.43",
		TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
	}
	svc.applyAdvertiseValidationResult(peerAddr, advertiseValidationResult{
		Decision:             advertiseDecisionLocalException,
		ObservedIPHint:       "192.168.0.17",
		PersistAnnounceState: announceStateDirectOnly,
		PersistWriteMode:     persistWriteModeCreateOrUpdate,
	})
	pm := svc.persistedMeta[peerAddr]
	if pm.AnnounceState != announceStateDirectOnly {
		t.Fatalf("announce_state not downgraded by local_exception: got %q", pm.AnnounceState)
	}
}

// TestValidateAdvertisedAddress_IPv6Mapped verifies that an
// IPv4-mapped IPv6 observed address (::ffff:1.2.3.4) is canonicalised
// to its bare IPv4 form so the compare against an IPv4-advertised
// address does not produce a spurious mismatch.
func TestValidateAdvertisedAddress_IPv6Mapped(t *testing.T) {
	result := validateAdvertisedAddress("[::ffff:203.0.113.50]:45123", protocol.Frame{
		Type:     "hello",
		Listener: "1",
		Listen:   "203.0.113.50:64646",
	})
	if result.Decision != advertiseDecisionMatch {
		t.Fatalf("expected match on IPv4-mapped IPv6, got %q", result.Decision)
	}
	if result.NormalizedObservedIP != "203.0.113.50" {
		t.Fatalf("observed IP not canonicalised: got %q", result.NormalizedObservedIP)
	}
	if result.NormalizedAdvertisedIP != "203.0.113.50" {
		t.Fatalf("advertised IP not canonicalised: got %q", result.NormalizedAdvertisedIP)
	}
}

// TestValidateAdvertisedAddress_ForbiddenAdvertiseListenIgnored pins the
// contract for the "peer claims a private range in hello.listen" case.
// Under the v12 cleanup baseline hello.listen is not a truth input at
// all: the announce candidate is derived from the observed TCP IP
// exclusively, so a malicious / buggy hello.listen cannot poison the
// candidate. The runtime path accepts the handshake, records
// announceable against the OBSERVED IP, and never emits any reject
// signal. NormalizedAdvertisedIP mirrors the observed IP — not the
// forbidden one claimed in hello.listen — which is what keeps the
// trusted advertise triple clean.
func TestValidateAdvertisedAddress_ForbiddenAdvertiseListenIgnored(t *testing.T) {
	result := validateAdvertisedAddress("203.0.113.60:45123", protocol.Frame{
		Type:          "hello",
		Listener:      "1",
		Listen:        "10.0.0.7:64646",
		AdvertisePort: domain.PeerPort(64646),
	})
	if result.Decision != advertiseDecisionMatch {
		t.Fatalf("expected match (hello.listen is ignored as truth), got %q", result.Decision)
	}
	if result.NormalizedAdvertisedIP != "203.0.113.60" {
		t.Fatalf("NormalizedAdvertisedIP: got %q want %q (must mirror observed IP, never the forbidden listen host)",
			result.NormalizedAdvertisedIP, "203.0.113.60")
	}
	if result.ObservedIPHint != "203.0.113.60" {
		t.Fatalf("ObservedIPHint: got %q want %q", result.ObservedIPHint, "203.0.113.60")
	}
	if !result.AllowAnnounce {
		t.Fatalf("AllowAnnounce must be true for world-routable observed IP, got false")
	}
	if result.AdvertisePort != domain.PeerPort(64646) {
		t.Fatalf("AdvertisePort: got %d want 64646 (the contract takes the self-reported advertise_port)", result.AdvertisePort)
	}
}

// TestHandleConnectionNotice_IgnoresUnknownCode asserts that the
// connection_notice dispatcher silently drops any frame whose Code is
// not a recognised dispatch target. After the v12 cleanup the only
// recognised code is ErrCodePeerBanned; everything else (including
// the historical observed-address-mismatch which is no longer
// produced by the wire) must fall through without touching shared
// state.
func TestHandleConnectionNotice_IgnoresUnknownCode(t *testing.T) {
	const peerAddr domain.PeerAddress = "198.51.100.10:64646"
	svc := newAdvertiseTestService()
	beforeRows := len(svc.persistedMeta)
	svc.handleConnectionNotice(peerAddr, protocol.Frame{
		Type: protocol.FrameTypeConnectionNotice,
		Code: "unrelated-notice",
	})
	if got := len(svc.persistedMeta); got != beforeRows {
		t.Fatalf("unrelated notice mutated persistedMeta: before=%d after=%d", beforeRows, got)
	}
}

// TestHandlePeerBannedNotice_PeerBanRecordsPerPeer asserts that a
// well-formed reason=peer-ban connection_notice stores the
// responder-supplied expiration on the dialler's persisted peerEntry so
// the PeerProvider gate can skip THIS address until the window elapses.
// reason=peer-ban writes only into the per-peer table — the IP-wide
// table must stay empty because the responder scoped the ban to one
// address. The responder's Until is trusted verbatim — even a
// multi-year window is preserved, since a peer that has decided to
// refuse us is authoritative about when it will change its mind.
func TestHandlePeerBannedNotice_PeerBanRecordsPerPeer(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.90:64646"
	svc := newAdvertiseTestService()
	svc.persistedMeta[peerAddr] = &peerEntry{Address: peerAddr}

	// Responder says: banned for ten years.
	until := time.Now().UTC().Add(10 * 365 * 24 * time.Hour).Truncate(time.Second)
	details, err := protocol.MarshalPeerBannedDetails(until, protocol.PeerBannedReasonPeerBan)
	if err != nil {
		t.Fatalf("marshal peer-banned details: %v", err)
	}

	svc.handleConnectionNotice(peerAddr, protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	})

	// persistedMeta is peer-domain (peerMu); remoteBannedIPs is
	// ipState-domain (ipStateMu) — hold both in canonical order.
	svc.peerMu.RLock()
	svc.ipStateMu.RLock()
	entry := svc.persistedMeta[peerAddr]
	ipBanCount := len(svc.remoteBannedIPs)
	svc.ipStateMu.RUnlock()
	svc.peerMu.RUnlock()
	if entry == nil || entry.RemoteBannedUntil == nil {
		t.Fatalf("RemoteBannedUntil was not recorded on the peer entry")
	}
	if !entry.RemoteBannedUntil.Equal(until) {
		t.Fatalf("RemoteBannedUntil: got %s want %s (responder Until must be preserved verbatim)",
			entry.RemoteBannedUntil.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	if entry.RemoteBanReason != string(protocol.PeerBannedReasonPeerBan) {
		t.Fatalf("RemoteBanReason: got %q want %q",
			entry.RemoteBanReason, protocol.PeerBannedReasonPeerBan)
	}
	if ipBanCount != 0 {
		t.Fatalf("reason=peer-ban must not touch the IP-wide table, got %d entries", ipBanCount)
	}
}

// TestHandlePeerBannedNotice_UnknownPeerIgnored asserts that a peer-banned
// notice for an address we have never seen before is silently ignored.
// Creating an entry on the fly here would open an unbounded growth path
// for a hostile responder fabricating addresses in the notice payload.
func TestHandlePeerBannedNotice_UnknownPeerIgnored(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.91:64646"
	svc := newAdvertiseTestService()
	// Deliberately do not register peerAddr in persistedMeta.

	until := time.Now().UTC().Add(time.Hour)
	details, err := protocol.MarshalPeerBannedDetails(until, protocol.PeerBannedReasonPeerBan)
	if err != nil {
		t.Fatalf("marshal peer-banned details: %v", err)
	}

	svc.handleConnectionNotice(peerAddr, protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	})

	svc.peerMu.RLock()
	_, stillUnknown := svc.persistedMeta[peerAddr]
	svc.peerMu.RUnlock()
	if stillUnknown {
		t.Fatalf("unknown peer must not be inserted into persistedMeta by a peer-banned notice")
	}
}

// TestHandlePeerBannedNotice_ParseFailureDoesNotRecord asserts that a
// malformed Details payload (unparseable Until) does not install any ban
// window. The ban is only as good as the responder-supplied timing data
// — a dialler that invented a fallback window on a parse failure would
// let a buggy or hostile responder pin its dial loop with garbage input.
func TestHandlePeerBannedNotice_ParseFailureDoesNotRecord(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.92:64646"
	svc := newAdvertiseTestService()
	svc.persistedMeta[peerAddr] = &peerEntry{Address: peerAddr}

	// An Until field that is not a valid RFC3339 time.
	bad := json.RawMessage(`{"until":"not-a-timestamp","reason":"peer-ban"}`)

	svc.handleConnectionNotice(peerAddr, protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: bad,
	})

	svc.peerMu.RLock()
	entry := svc.persistedMeta[peerAddr]
	svc.peerMu.RUnlock()
	if entry != nil && entry.RemoteBannedUntil != nil {
		t.Fatalf("RemoteBannedUntil must remain unset when Details parsing fails, got %s",
			entry.RemoteBannedUntil.Format(time.RFC3339))
	}
}

// TestHandlePeerBannedNotice_BlacklistedEscalatesToIPWide is the
// regression guard for the blast-radius scoping fix: a single
// reason=blacklisted notice suppresses ONLY the offending peer
// (per-peer record), leaving innocent siblings behind the same egress
// diallable. The IP-wide gate — which suppresses every sibling — engages
// only once ipWideRemoteBanMinOffenders DISTINCT peers behind the IP
// have independently sent blacklisted, i.e. the whole egress is hostile.
func TestHandlePeerBannedNotice_BlacklistedEscalatesToIPWide(t *testing.T) {
	const ip = "203.0.113.90"
	svc := newAdvertiseTestService()
	svc.remoteBannedIPs = make(map[string]remoteIPBanEntry)
	svc.remoteIPBanOffenders = make(map[string]map[domain.PeerAddress]time.Time)

	until := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	details, err := protocol.MarshalPeerBannedDetails(until, protocol.PeerBannedReasonBlacklisted)
	if err != nil {
		t.Fatalf("marshal peer-banned details: %v", err)
	}
	notice := protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	}

	// Distinct offender addresses sharing one egress IP.
	offenders := []domain.PeerAddress{
		domain.PeerAddress(ip + ":64646"),
		domain.PeerAddress(ip + ":64647"),
		domain.PeerAddress(ip + ":64648"),
	}
	sibling := domain.PeerAddress(ip + ":7070") // innocent, never sends a notice

	ipWideActive := func() bool {
		svc.ipStateMu.RLock()
		defer svc.ipStateMu.RUnlock()
		_, ok := svc.remoteBannedIPs[ip]
		return ok
	}
	remoteBanned := func(addr domain.PeerAddress) bool {
		svc.peerMu.RLock()
		svc.ipStateMu.RLock()
		defer svc.ipStateMu.RUnlock()
		defer svc.peerMu.RUnlock()
		return svc.isPeerRemoteBannedLocked(addr, time.Now().UTC())
	}

	// Below the threshold: per-peer suppression only, no IP-wide.
	for i := 0; i < ipWideRemoteBanMinOffenders-1; i++ {
		svc.peerMu.Lock()
		svc.persistedMeta[offenders[i]] = &peerEntry{Address: offenders[i]}
		svc.peerMu.Unlock()
		svc.handleConnectionNotice(offenders[i], notice)
	}
	if ipWideActive() {
		t.Fatalf("IP-wide ban must NOT engage before %d distinct offenders", ipWideRemoteBanMinOffenders)
	}
	if remoteBanned(sibling) {
		t.Fatal("innocent sibling must stay diallable before IP-wide escalation")
	}
	// Each offender that sent a notice IS suppressed per-peer.
	if !remoteBanned(offenders[0]) {
		t.Fatal("offender must be suppressed per-peer on its own blacklisted notice")
	}

	// The threshold-th distinct offender escalates to the IP-wide gate.
	svc.peerMu.Lock()
	svc.persistedMeta[offenders[ipWideRemoteBanMinOffenders-1]] = &peerEntry{Address: offenders[ipWideRemoteBanMinOffenders-1]}
	svc.peerMu.Unlock()
	svc.handleConnectionNotice(offenders[ipWideRemoteBanMinOffenders-1], notice)

	if !ipWideActive() {
		t.Fatalf("IP-wide ban must engage at %d distinct offenders", ipWideRemoteBanMinOffenders)
	}
	svc.ipStateMu.RLock()
	ipEntry := svc.remoteBannedIPs[ip]
	svc.ipStateMu.RUnlock()
	if !ipEntry.Until.Equal(until) {
		t.Fatalf("IP-wide entry Until: got %s want %s",
			ipEntry.Until.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	if ipEntry.Reason != string(protocol.PeerBannedReasonBlacklisted) {
		t.Fatalf("IP-wide entry Reason: got %q want %q",
			ipEntry.Reason, string(protocol.PeerBannedReasonBlacklisted))
	}
	// Now even the unknown sibling is suppressed via the IP-wide gate.
	if !remoteBanned(sibling) {
		t.Fatal("sibling on an escalated IP must be suppressed by the dial gate")
	}
}

// TestHandlePeerBannedNotice_BlacklistedOnFallbackLandsOnPrimary is the
// regression guard for the generated-fallback edge case: PeerProvider
// dials a known non-default primary plus a generated default-port
// fallback variant. If a blacklisted notice arrives on the FALLBACK
// (which it can, mid-handshake, before the dial succeeds), the per-peer
// ban must land on the canonical PRIMARY (resolved via the dialOrigin
// alias that dialForCM now registers before the handshake) rather than
// being dropped as an unknown address. The old "first notice = whole IP"
// behaviour masked this; the scoped contract requires the resolve.
func TestHandlePeerBannedNotice_BlacklistedOnFallbackLandsOnPrimary(t *testing.T) {
	const primary domain.PeerAddress = "203.0.113.200:64646"
	const fallback domain.PeerAddress = "203.0.113.200:64647" // same host, generated variant
	svc := newAdvertiseTestService()
	svc.remoteBannedIPs = make(map[string]remoteIPBanEntry)
	svc.remoteIPBanOffenders = make(map[string]map[domain.PeerAddress]time.Time)
	svc.dialOrigin = make(map[domain.PeerAddress]domain.PeerAddress)

	// Known primary owns the persistedMeta row; the fallback is not a
	// peer in its own right. dialForCM pre-registers the alias before
	// the handshake, which is what lets the notice resolve.
	svc.persistedMeta[primary] = &peerEntry{Address: primary}
	svc.dialOrigin[fallback] = primary

	until := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	details, err := protocol.MarshalPeerBannedDetails(until, protocol.PeerBannedReasonBlacklisted)
	if err != nil {
		t.Fatalf("marshal peer-banned details: %v", err)
	}

	// Notice arrives on the FALLBACK address.
	svc.handleConnectionNotice(fallback, protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	})

	svc.peerMu.RLock()
	entry := svc.persistedMeta[primary]
	_, fallbackRow := svc.persistedMeta[fallback]
	svc.peerMu.RUnlock()

	if entry == nil || entry.RemoteBannedUntil == nil {
		t.Fatal("blacklisted notice on a generated fallback must record the per-peer ban on its canonical primary, not drop it")
	}
	if !entry.RemoteBannedUntil.Equal(until) {
		t.Fatalf("primary RemoteBannedUntil = %v, want %v", entry.RemoteBannedUntil, until)
	}
	if fallbackRow {
		t.Fatal("the fallback alias must not get its own persistedMeta row")
	}
}

// TestBuildPeerEntriesLocked_PreservesRemoteBan is the persistence
// regression guard: a per-peer remote ban written into persistedMeta by
// recordRemoteBanLocked must be copied into the serialized peerEntry so
// it survives flushPeerState + restart. buildPeerEntriesLocked rebuilds
// each entry from scratch; before the fix it dropped RemoteBannedUntil /
// RemoteBanReason, so the next flush rewrote peers.json without the ban
// and a restart resumed the retry storm.
func TestBuildPeerEntriesLocked_PreservesRemoteBan(t *testing.T) {
	const addr domain.PeerAddress = "203.0.113.220:64646"
	svc := newAdvertiseTestService()
	svc.peers = []transport.Peer{{Address: addr, Source: domain.PeerSourcePersisted}}

	until := time.Now().UTC().Add(2 * time.Hour)
	svc.persistedMeta[addr] = &peerEntry{
		Address:           addr,
		RemoteBannedUntil: &until,
		RemoteBanReason:   string(protocol.PeerBannedReasonPeerBan),
	}

	svc.peerMu.Lock()
	entries := svc.buildPeerEntriesLocked(nil)
	svc.peerMu.Unlock()

	var found *peerEntry
	for i := range entries {
		if entries[i].Address == addr {
			found = &entries[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("no serialized entry for %s", addr)
	}
	if found.RemoteBannedUntil == nil {
		t.Fatal("buildPeerEntriesLocked must preserve RemoteBannedUntil so the per-peer ban survives flush/restart")
	}
	if !found.RemoteBannedUntil.Equal(until) {
		t.Fatalf("RemoteBannedUntil = %v, want %v", found.RemoteBannedUntil, until)
	}
	if found.RemoteBanReason != string(protocol.PeerBannedReasonPeerBan) {
		t.Fatalf("RemoteBanReason = %q, want peer-ban", found.RemoteBanReason)
	}
}

// TestRecordOutboundAuthSuccess_FallbackClearsCanonicalPerPeerBan is the
// recovery regression guard for the generated-fallback case: when a
// handshake succeeds via a fallback dial address, the per-peer remote
// ban recorded against the canonical primary must be cleared. Before the
// fix, recordOutboundAuthSuccess cleared the raw (unknown) fallback
// address, leaving the primary's stale RemoteBannedUntil to re-suppress
// the peer after the session closed.
func TestRecordOutboundAuthSuccess_FallbackClearsCanonicalPerPeerBan(t *testing.T) {
	const primary domain.PeerAddress = "203.0.113.210:64646"
	const fallback domain.PeerAddress = "203.0.113.210:64647"
	svc := newAdvertiseTestService()
	svc.dialOrigin = map[domain.PeerAddress]domain.PeerAddress{fallback: primary}

	until := time.Now().UTC().Add(2 * time.Hour)
	svc.persistedMeta[primary] = &peerEntry{
		Address:           primary,
		RemoteBannedUntil: &until,
		RemoteBanReason:   string(protocol.PeerBannedReasonPeerBan),
	}

	// Handshake succeeded via the FALLBACK address.
	svc.recordOutboundAuthSuccess(fallback, string(fallback))

	svc.peerMu.RLock()
	entry := svc.persistedMeta[primary]
	svc.peerMu.RUnlock()
	if entry == nil || entry.RemoteBannedUntil != nil {
		t.Fatal("successful fallback handshake must clear the canonical primary's per-peer remote ban")
	}
}

// TestIsPeerRemoteBanned_CanonicalizesLookupKey pins that the read gate
// derives its IP-wide lookup key the same way record/clear do
// (remoteIPBanKey). A ban stored under the canonical IPv4 key must be
// hit by a candidate carrying a non-canonical IPv4-in-IPv6 form for the
// same address; otherwise PeerProvider (which passes the raw candidate
// address into RemoteBannedFn) could redial a still-banned egress.
func TestIsPeerRemoteBanned_CanonicalizesLookupKey(t *testing.T) {
	svc := newAdvertiseTestService()
	svc.remoteBannedIPs = make(map[string]remoteIPBanEntry)

	future := time.Now().UTC().Add(time.Hour)
	svc.remoteBannedIPs["203.0.113.5"] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}

	// Candidate carries the IPv4-in-IPv6 form of the same address.
	candidate := domain.PeerAddress("[::ffff:203.0.113.5]:64646")
	svc.peerMu.RLock()
	svc.ipStateMu.RLock()
	banned := svc.isPeerRemoteBannedLocked(candidate, time.Now().UTC())
	svc.ipStateMu.RUnlock()
	svc.peerMu.RUnlock()
	if !banned {
		t.Fatal("candidate with a non-canonical IP form must hit the IP-wide ban stored under the canonical key")
	}
}

// TestRecordOutboundAuthSuccess_ClearsHostnameKeyedIPBan is the
// regression guard for DNS / manual peers: handlePeerBannedNotice keys
// the IP-wide ban on the HOSTNAME (canonicalIPFromHost returns "" for a
// non-IP host), but a successful handshake resolves to a numeric IP.
// Recovery must clear under the same notice-derived key (remoteIPBanKey),
// not only the resolved RemoteAddr IP, or the hostname-keyed ban (and its
// volatile offender bucket) would survive and keep suppressing the peer.
func TestRecordOutboundAuthSuccess_ClearsHostnameKeyedIPBan(t *testing.T) {
	const peerAddr domain.PeerAddress = "peer.example:64646"
	const banKey = "peer.example"
	svc := newAdvertiseTestService()
	svc.remoteBannedIPs = make(map[string]remoteIPBanEntry)
	svc.remoteIPBanOffenders = make(map[string]map[domain.PeerAddress]time.Time)

	// An escalated IP-wide ban keyed on the HOSTNAME (as the notice handler
	// keys it), plus a lingering offender bucket under the same key.
	future := time.Now().UTC().Add(12 * time.Hour)
	svc.remoteBannedIPs[banKey] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}
	svc.remoteIPBanOffenders[banKey] = map[domain.PeerAddress]time.Time{peerAddr: future}

	// Handshake succeeds; RemoteAddr resolves to a numeric IP that does
	// NOT match the hostname key.
	svc.recordOutboundAuthSuccess(peerAddr, "203.0.113.5:64646")

	svc.ipStateMu.RLock()
	_, stillBanned := svc.remoteBannedIPs[banKey]
	_, stillTracked := svc.remoteIPBanOffenders[banKey]
	svc.ipStateMu.RUnlock()
	if stillBanned {
		t.Fatal("hostname-keyed IP-wide ban must be cleared on successful handshake")
	}
	if stillTracked {
		t.Fatal("hostname-keyed offender bucket must be cleared on successful handshake")
	}
}

// TestHandlePeerBannedNotice_PeerBanDoesNotPropagateToSiblings pins the
// symmetric contract: reason=peer-ban is scoped to one PeerAddress, so
// a sibling on the same IP MUST remain dialable when no IP-wide record
// exists. Confusing the two scopes would over-ban legitimate peers
// sharing an egress with one misbehaving node.
func TestHandlePeerBannedNotice_PeerBanDoesNotPropagateToSiblings(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.91:64646"
	svc := newAdvertiseTestService()
	svc.remoteBannedIPs = make(map[string]remoteIPBanEntry)
	svc.persistedMeta[peerAddr] = &peerEntry{Address: peerAddr}

	until := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	details, err := protocol.MarshalPeerBannedDetails(until, protocol.PeerBannedReasonPeerBan)
	if err != nil {
		t.Fatalf("marshal peer-banned details: %v", err)
	}

	svc.handleConnectionNotice(peerAddr, protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	})

	// No IP-wide record must have been written. remoteBannedIPs is
	// ipState-domain, so read it under ipStateMu (see the field's
	// lock contract in service.go).
	svc.ipStateMu.RLock()
	_, hasIPRecord := svc.remoteBannedIPs["203.0.113.91"]
	svc.ipStateMu.RUnlock()
	if hasIPRecord {
		t.Fatal("reason=peer-ban must NOT populate remoteBannedIPs")
	}

	// isPeerRemoteBannedLocked reads BOTH the per-peer (peerMu) and
	// IP-wide (ipStateMu) tables, so hold both in canonical
	// peerMu → ipStateMu order.
	remoteBanned := func(addr domain.PeerAddress) bool {
		svc.peerMu.RLock()
		svc.ipStateMu.RLock()
		defer svc.ipStateMu.RUnlock()
		defer svc.peerMu.RUnlock()
		return svc.isPeerRemoteBannedLocked(addr, time.Now().UTC())
	}

	// The original peer IS banned via the per-peer record.
	if !remoteBanned(peerAddr) {
		t.Fatal("original peer must be suppressed by per-peer remote ban")
	}

	// A sibling on the same IP must remain dialable.
	if remoteBanned(domain.PeerAddress("203.0.113.91:7070")) {
		t.Fatal("sibling on same IP must stay dialable when reason was peer-ban")
	}
}

// TestRecordObservedIPHistory_BoundAndDedup verifies both the
// bounded-history cap at observedIPHistoryMaxSize entries and the
// adjacent-duplicate suppression that stops the outbound convergence
// loop from ping-ponging on a repeated observation.
func TestRecordObservedIPHistory_BoundAndDedup(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "203.0.113.100:64646"

	// Adjacent duplicates fold to a single entry.
	svc.ipStateMu.Lock()
	svc.recordObservedIPHintLocked(peerAddr, "203.0.113.200")
	svc.recordObservedIPHintLocked(peerAddr, "203.0.113.200")
	svc.recordObservedIPHintLocked(peerAddr, "203.0.113.200")
	svc.ipStateMu.Unlock()
	if history := svc.observedIPHistoryForPeer(peerAddr); len(history) != 1 {
		t.Fatalf("adjacent duplicates must be deduplicated: %v", history)
	}

	// Overflow past the cap retains only the last observedIPHistoryMaxSize
	// distinct entries.
	svc.ipStateMu.Lock()
	for i := 0; i < observedIPHistoryMaxSize+3; i++ {
		svc.recordObservedIPHintLocked(peerAddr, entryIP(i))
	}
	svc.ipStateMu.Unlock()
	history := svc.observedIPHistoryForPeer(peerAddr)
	if len(history) != observedIPHistoryMaxSize {
		t.Fatalf("history capped length: got %d want %d",
			len(history), observedIPHistoryMaxSize)
	}
	// Oldest entries must have fallen off the front.
	if history[0] != entryIP(3) {
		t.Fatalf("front of history: got %q want %q", history[0], entryIP(3))
	}
	if history[len(history)-1] != entryIP(observedIPHistoryMaxSize+2) {
		t.Fatalf("tail of history: got %q want %q",
			history[len(history)-1], entryIP(observedIPHistoryMaxSize+2))
	}
}

// entryIP produces a distinct world-reachable IP for test history
// fixtures — using 198.51.100.0/24 (RFC 5737 documentation range).
func entryIP(i int) domain.PeerIP {
	// Only i < 256 is exercised by the tests above, so no overflow.
	return domain.PeerIP("198.51.100." + strconv.Itoa(i+1))
}

// TestRecordOutboundConfirmed_WritesTrustedAdvertise verifies that the
// outbound success writer installs all three parts of the trusted
// advertise triple (IP / source / port) and never creates the row with
// an announceable state that relies on a missing field.
func TestRecordOutboundConfirmed_WritesTrustedAdvertise(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "203.0.113.120:64646"
	svc.recordOutboundConfirmed(peerAddr, "203.0.113.120", domain.PeerPort(64646))

	pm := svc.persistedMeta[peerAddr]
	if pm == nil {
		t.Fatalf("persistedMeta row not created")
	}
	if pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("announce_state: got %q want %q", pm.AnnounceState, announceStateAnnounceable)
	}
	if pm.TrustedAdvertiseIP != "203.0.113.120" {
		t.Fatalf("trusted_advertise_ip: got %q", pm.TrustedAdvertiseIP)
	}
	if pm.TrustedAdvertiseSource != trustedAdvertiseSourceOutbound {
		t.Fatalf("trusted_advertise_source: got %q want %q",
			pm.TrustedAdvertiseSource, trustedAdvertiseSourceOutbound)
	}
	if pm.TrustedAdvertisePort != "64646" {
		t.Fatalf("trusted_advertise_port: got %q want %q", pm.TrustedAdvertisePort, "64646")
	}
}

// TestRecordOutboundConfirmed_CanonicalisesMappedIPv6 asserts that a
// dialled IPv4-mapped IPv6 address is stored in canonical IPv4 form so
// the pair (trusted_ip, peer_address) does not drift on restart.
func TestRecordOutboundConfirmed_CanonicalisesMappedIPv6(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "203.0.113.121:64646"
	svc.recordOutboundConfirmed(peerAddr, "::ffff:203.0.113.121", domain.PeerPort(64646))

	pm := svc.persistedMeta[peerAddr]
	if pm == nil {
		t.Fatalf("persistedMeta row not created")
	}
	if pm.TrustedAdvertiseIP != "203.0.113.121" {
		t.Fatalf("trusted_advertise_ip not canonicalised: got %q", pm.TrustedAdvertiseIP)
	}
}

// TestRecordOutboundConfirmed_RejectsHostname pins the contract: the
// outbound writer must never persist a hostname in TrustedAdvertiseIP.
// DNS / manually configured bootstrap peers have a hostname-form
// session.address (e.g. "peer.example:64646"), and a naive caller that
// forwards the host part of that string would poison TrustedAdvertiseIP
// with a non-canonical value. The observed-IP downgrade sweep compares
// TrustedAdvertiseIP against canonical IPs derived from real TCP
// RemoteAddrs — a hostname there would never match and the announceable
// row would permanently survive a world_mismatch event.
func TestRecordOutboundConfirmed_RejectsHostname(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "peer.example:64646"

	// Pre-seed an unrelated announceable row: the refused call must not
	// perturb any other entry in the map.
	const other domain.PeerAddress = "203.0.113.140:64646"
	svc.persistedMeta[other] = &peerEntry{
		Address:                other,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.140",
		TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
		TrustedAdvertisePort:   config.DefaultPeerPort,
	}

	cases := []domain.PeerIP{
		"peer.example",   // DNS hostname
		"not.a.valid.ip", // multi-label string that is not an IP literal
		"garbage::bad",   // looks like IPv6 but is not parseable
	}
	for _, badIP := range cases {
		svc.recordOutboundConfirmed(peerAddr, badIP, domain.PeerPort(64646))
	}

	if _, exists := svc.persistedMeta[peerAddr]; exists {
		t.Fatalf("non-IP dialedIP must never create a persistedMeta row; got one for %q", peerAddr)
	}
	if pm := svc.persistedMeta[other]; pm.TrustedAdvertiseIP != "203.0.113.140" {
		t.Fatalf("unrelated row mutated by refused call: got %q", pm.TrustedAdvertiseIP)
	}
	if pm := svc.persistedMeta[other]; pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("unrelated row announce_state mutated: got %q", pm.AnnounceState)
	}
}

// TestRecordOutboundConfirmed_PreservesExistingRowOnHostname asserts
// that a refused call is a true no-op: a previously-learned trusted
// advertise triple for the same peer address must survive byte-for-byte.
// Silently overwriting the row with a hostname (or clearing it) would
// retroactively corrupt earlier correct evidence.
func TestRecordOutboundConfirmed_PreservesExistingRowOnHostname(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "peer.example:64646"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                peerAddr,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.150", // canonical IP from a prior session
		TrustedAdvertiseSource: trustedAdvertiseSourceOutbound,
		TrustedAdvertisePort:   "64646",
	}

	svc.recordOutboundConfirmed(peerAddr, domain.PeerIP("peer.example"), domain.PeerPort(64646))

	pm := svc.persistedMeta[peerAddr]
	if pm == nil {
		t.Fatalf("existing row vanished on refused call")
	}
	if pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("announce_state mutated: got %q want %q", pm.AnnounceState, announceStateAnnounceable)
	}
	if pm.TrustedAdvertiseIP != "203.0.113.150" {
		t.Fatalf("trusted_advertise_ip overwritten by refused call: got %q want %q",
			pm.TrustedAdvertiseIP, "203.0.113.150")
	}
	if pm.TrustedAdvertiseSource != trustedAdvertiseSourceOutbound {
		t.Fatalf("trusted_advertise_source mutated: got %q", pm.TrustedAdvertiseSource)
	}
	if pm.TrustedAdvertisePort != "64646" {
		t.Fatalf("trusted_advertise_port mutated: got %q", pm.TrustedAdvertisePort)
	}
}

// TestRecordOutboundAuthSuccess_HostnamePeerWritesCanonicalIP covers the
// integration path for DNS/manual peers. peerAddress carries the
// hostname form ("peer.example:64646"), but the connection wrapper
// always exposes a canonical IP through RemoteAddr() after OS DNS
// resolution. The auth-success hook must source the trusted advertise
// IP from the wrapper's RemoteAddr() — not peerAddress — otherwise
// TrustedAdvertiseIP would store the unresolved hostname and silently
// break the observed-IP downgrade sweep.
func TestRecordOutboundAuthSuccess_HostnamePeerWritesCanonicalIP(t *testing.T) {
	svc := newAdvertiseTestService()
	const hostnameAddr domain.PeerAddress = "peer.example:64646"

	// The wrapper's RemoteAddr() returns the OS-resolved TCP host:port,
	// not the dialed hostname — that is the whole reason the helper
	// takes this string rather than peerAddress.
	svc.recordOutboundAuthSuccess(hostnameAddr, "203.0.113.160:64646")

	pm := svc.persistedMeta[hostnameAddr]
	if pm == nil {
		t.Fatalf("persistedMeta row not created for hostname peer")
	}
	if pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("announce_state: got %q want %q", pm.AnnounceState, announceStateAnnounceable)
	}
	if pm.TrustedAdvertiseIP != "203.0.113.160" {
		t.Fatalf("trusted_advertise_ip: got %q want %q (must be canonical IP from RemoteAddr, not hostname)",
			pm.TrustedAdvertiseIP, "203.0.113.160")
	}
	if pm.TrustedAdvertiseSource != trustedAdvertiseSourceOutbound {
		t.Fatalf("trusted_advertise_source: got %q want %q",
			pm.TrustedAdvertiseSource, trustedAdvertiseSourceOutbound)
	}
	if pm.TrustedAdvertisePort != "64646" {
		t.Fatalf("trusted_advertise_port: got %q want %q", pm.TrustedAdvertisePort, "64646")
	}
}

// TestRecordOutboundAuthSuccess_CanonicalisesMappedIPv6RemoteAddr
// asserts that an IPv4-mapped IPv6 RemoteAddr is normalised to its bare
// IPv4 form before being written to TrustedAdvertiseIP. Without this,
// the same peer dialled over an IPv4 socket and over a dual-stack
// socket would yield two different TrustedAdvertiseIP values and the
// sweep would fail to match one of them.
func TestRecordOutboundAuthSuccess_CanonicalisesMappedIPv6RemoteAddr(t *testing.T) {
	svc := newAdvertiseTestService()
	const hostnameAddr domain.PeerAddress = "dualstack.example:64646"

	// net.TCPAddr{IP: "::ffff:203.0.113.161", Port: 64646}.String() →
	// "[::ffff:203.0.113.161]:64646"; the wrapper publishes exactly
	// this form via RemoteAddr(), so canonicalisation must happen
	// inside the helper.
	svc.recordOutboundAuthSuccess(hostnameAddr, "[::ffff:203.0.113.161]:64646")

	pm := svc.persistedMeta[hostnameAddr]
	if pm == nil {
		t.Fatalf("persistedMeta row not created for hostname peer")
	}
	if pm.TrustedAdvertiseIP != "203.0.113.161" {
		t.Fatalf("trusted_advertise_ip not canonicalised: got %q want %q",
			pm.TrustedAdvertiseIP, "203.0.113.161")
	}
}

// TestRecordOutboundAuthSuccess_EmptyInputsAreNoop guards the defensive
// entry checks. Callers can hand in an empty peerAddress (programmer
// error) or an empty remoteAddr (wrapper published nothing yet / unit
// tests); neither must panic or mutate persistedMeta.
func TestRecordOutboundAuthSuccess_EmptyInputsAreNoop(t *testing.T) {
	svc := newAdvertiseTestService()
	const peerAddr domain.PeerAddress = "peer.example:64646"

	// Empty remoteAddr must bail before any write attempt.
	svc.recordOutboundAuthSuccess(peerAddr, "")

	// Empty peerAddress must bail even with a valid remote.
	svc.recordOutboundAuthSuccess("", "203.0.113.172:64646")

	if len(svc.persistedMeta) != 0 {
		t.Fatalf("empty inputs must not mutate persistedMeta; got %d entries", len(svc.persistedMeta))
	}
}

// TestRecordOutboundAuthSuccess_ClearsRemoteBan pins the behaviour wired
// up for the reviewer's P1 finding: a previously-recorded remote ban
// on the peer must be dropped as soon as the same peer completes a
// fresh handshake. Otherwise the PeerProvider gate would keep skipping
// a peer that is demonstrably accepting us again, silently extending
// the suppression the ban window was introduced to end.
func TestRecordOutboundAuthSuccess_ClearsRemoteBan(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.180:64646"
	svc := newAdvertiseTestService()

	// Seed a remote-ban record for this peer.
	future := time.Now().UTC().Add(24 * time.Hour)
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:           peerAddr,
		RemoteBannedUntil: &future,
		RemoteBanReason:   string(protocol.PeerBannedReasonBlacklisted),
	}

	svc.recordOutboundAuthSuccess(peerAddr, "203.0.113.180:64646")

	pm := svc.persistedMeta[peerAddr]
	if pm == nil {
		t.Fatalf("persistedMeta row missing after successful auth")
	}
	if pm.RemoteBannedUntil != nil {
		t.Fatalf("RemoteBannedUntil must be cleared on successful handshake, got %s",
			pm.RemoteBannedUntil.Format(time.RFC3339))
	}
	if pm.RemoteBanReason != "" {
		t.Fatalf("RemoteBanReason must be cleared on successful handshake, got %q",
			pm.RemoteBanReason)
	}
}

// TestRecordOutboundAuthSuccess_ClearsRemoteIPBan pins the reviewer's P1
// finding: when a reason=blacklisted notice has stored an IP-wide record
// in remoteBannedIPs, a subsequent successful handshake with any peer on
// that IP must drop the IP-wide entry. Clearing only the per-peer record
// would leave the IP-wide gate suppressing every sibling PeerAddress
// behind the same egress until the original window elapses, even though
// the responder has demonstrated it is accepting us again.
func TestRecordOutboundAuthSuccess_ClearsRemoteIPBan(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.181:64646"
	const bannedIP = "203.0.113.181"
	svc := newAdvertiseTestService()

	// Seed an IP-wide remote-ban entry for the dialled host
	// (remoteBannedIPs is ipState-domain — write under ipStateMu).
	future := time.Now().UTC().Add(24 * time.Hour)
	svc.ipStateMu.Lock()
	svc.remoteBannedIPs[bannedIP] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}
	svc.ipStateMu.Unlock()
	// persistedMeta is intentionally empty: the IP-wide record is
	// independent of the per-peer row and must be cleared on its own.

	svc.recordOutboundAuthSuccess(peerAddr, "203.0.113.181:64646")

	svc.ipStateMu.RLock()
	_, ok := svc.remoteBannedIPs[bannedIP]
	svc.ipStateMu.RUnlock()
	if ok {
		t.Fatalf("remoteBannedIPs[%q] must be cleared on successful handshake", bannedIP)
	}
}

// TestRecordOutboundAuthSuccess_ClearsRemoteIPBanFreesSiblings is the
// blast-radius companion: a successful handshake from ONE sibling
// unblocks EVERY sibling behind the same IP. Before the fix the IP-wide
// record would linger even after the per-peer record was cleared, so
// siblings discovered later (or re-registered from peer exchange) would
// keep getting skipped by the PeerProvider gate.
func TestRecordOutboundAuthSuccess_ClearsRemoteIPBanFreesSiblings(t *testing.T) {
	const sibling1 domain.PeerAddress = "198.51.100.55:64646"
	const sibling2 domain.PeerAddress = "198.51.100.55:64647"
	const bannedIP = "198.51.100.55"
	svc := newAdvertiseTestService()

	future := time.Now().UTC().Add(12 * time.Hour)
	svc.ipStateMu.Lock()
	svc.remoteBannedIPs[bannedIP] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}
	svc.ipStateMu.Unlock()

	// isPeerRemoteBannedLocked reads BOTH domains — hold peerMu → ipStateMu.
	remoteBanned := func(addr domain.PeerAddress) bool {
		svc.peerMu.RLock()
		svc.ipStateMu.RLock()
		defer svc.ipStateMu.RUnlock()
		defer svc.peerMu.RUnlock()
		return svc.isPeerRemoteBannedLocked(addr, time.Now().UTC())
	}

	// Pre-gate: sibling2 must be suppressed before we recover via sibling1.
	if !remoteBanned(sibling2) {
		t.Fatalf("pre-condition: sibling2 must be suppressed by the IP-wide ban")
	}

	svc.recordOutboundAuthSuccess(sibling1, "198.51.100.55:64646")

	if remoteBanned(sibling2) {
		t.Fatalf("sibling2 must no longer be suppressed after sibling1's handshake")
	}
	svc.ipStateMu.RLock()
	_, ok := svc.remoteBannedIPs[bannedIP]
	svc.ipStateMu.RUnlock()
	if ok {
		t.Fatalf("remoteBannedIPs[%q] must be cleared after sibling1's handshake", bannedIP)
	}
}

// TestRecordOutboundAuthSuccess_PreservesPeerBanSiblingRows pins the
// scope boundary between the two tables after a sibling handshake on
// the same IP. reason=peer-ban and reason=blacklisted live in separate
// tables by construction (per-peer vs. IP-wide), and IP-wide recovery
// must touch ONLY the IP-wide table: a standalone peer-ban on another
// address is an independent responder decision that a sibling handshake
// cannot speak for. Without this guard, IP-wide recovery would silently
// forgive a peer-specific ban the responder still enforces.
func TestRecordOutboundAuthSuccess_PreservesPeerBanSiblingRows(t *testing.T) {
	const (
		peerA    domain.PeerAddress = "198.51.100.105:64646"
		peerC    domain.PeerAddress = "198.51.100.105:64648"
		bannedIP                    = "198.51.100.105"
	)
	svc := newAdvertiseTestService()

	future := time.Now().UTC().Add(12 * time.Hour)
	svc.ipStateMu.Lock()
	svc.remoteBannedIPs[bannedIP] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}
	svc.ipStateMu.Unlock()
	// peerC has a STANDALONE per-peer ban (reason=peer-ban) from an
	// earlier independent notice — not a mirror of the IP-wide record.
	cUntil := future
	svc.persistedMeta[peerC] = &peerEntry{
		Address:           peerC,
		RemoteBannedUntil: &cUntil,
		RemoteBanReason:   string(protocol.PeerBannedReasonPeerBan),
	}
	svc.persistedMeta[peerA] = &peerEntry{Address: peerA}

	svc.recordOutboundAuthSuccess(peerA, "198.51.100.105:64646")

	svc.ipStateMu.RLock()
	_, ok := svc.remoteBannedIPs[bannedIP]
	svc.ipStateMu.RUnlock()
	if ok {
		t.Fatalf("remoteBannedIPs[%q] must be cleared by the handshake", bannedIP)
	}
	// peerC's standalone peer-ban must survive: handshake with a
	// different sibling is not proof the responder accepts peerC.
	entryC := svc.persistedMeta[peerC]
	if entryC.RemoteBannedUntil == nil {
		t.Fatalf("peerC standalone peer-ban must be preserved after sibling handshake")
	}
	if !entryC.RemoteBannedUntil.Equal(future) {
		t.Fatalf("peerC RemoteBannedUntil must be unchanged, got %v (want %v)",
			entryC.RemoteBannedUntil, future)
	}
	if entryC.RemoteBanReason != string(protocol.PeerBannedReasonPeerBan) {
		t.Fatalf("peerC RemoteBanReason must stay %q, got %q",
			protocol.PeerBannedReasonPeerBan, entryC.RemoteBanReason)
	}
}

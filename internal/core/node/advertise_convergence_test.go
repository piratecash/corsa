package node

import (
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestValidateAdvertisedAddress covers every branch of the advertise
// convergence decision matrix. The helper is a pure function: no
// Service state, no network side effects — so each row of the matrix
// maps directly to one test case.
func TestValidateAdvertisedAddress(t *testing.T) {
	cases := []struct {
		name              string
		observedTCP       string
		frame             protocol.Frame
		wantDecision      advertiseDecision
		wantShouldReject  bool
		wantNoticePresent bool
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
			name:        "legacy_direct_no_listen",
			observedTCP: "203.0.113.11:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "",
			},
			wantDecision:      advertiseDecisionLegacyDirect,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeCreateOrUpdate,
		},
		{
			name:        "legacy_direct_wildcard_bind",
			observedTCP: "203.0.113.12:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
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
			name:        "world_mismatch_different_world_ips",
			observedTCP: "203.0.113.15:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "198.51.100.99:64646",
			},
			wantDecision:      advertiseDecisionWorldMismatch,
			wantShouldReject:  true,
			wantNoticePresent: true,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeUpdateExisting,
		},
		{
			name:        "world_mismatch_advertise_private_observed_public",
			observedTCP: "203.0.113.16:45123",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "10.0.0.7:64646",
			},
			wantDecision:      advertiseDecisionWorldMismatch,
			wantShouldReject:  true,
			wantNoticePresent: true,
			wantAnnounceState: announceStateDirectOnly,
			wantWriteMode:     persistWriteModeUpdateExisting,
		},
		{
			name:        "invalid_unparseable_observed",
			observedTCP: "not-a-host:port",
			frame: protocol.Frame{
				Type:     "hello",
				Listener: "1",
				Listen:   "203.0.113.17:64646",
			},
			wantDecision:      advertiseDecisionInvalid,
			wantShouldReject:  true,
			wantAnnounceState: announceStateUnset,
			wantWriteMode:     persistWriteModeSkip,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := validateAdvertisedAddress(tc.observedTCP, tc.frame)
			if result.Decision != tc.wantDecision {
				t.Fatalf("decision mismatch: got %q want %q", result.Decision, tc.wantDecision)
			}
			if result.ShouldReject != tc.wantShouldReject {
				t.Fatalf("ShouldReject mismatch: got %v want %v", result.ShouldReject, tc.wantShouldReject)
			}
			if tc.wantNoticePresent && result.RejectNotice == nil {
				t.Fatalf("expected RejectNotice frame, got nil")
			}
			if !tc.wantNoticePresent && result.RejectNotice != nil {
				t.Fatalf("unexpected RejectNotice for decision %s", result.Decision)
			}
			if result.PersistAnnounceState != tc.wantAnnounceState {
				t.Fatalf("announce_state mismatch: got %q want %q", result.PersistAnnounceState, tc.wantAnnounceState)
			}
			if result.PersistWriteMode != tc.wantWriteMode {
				t.Fatalf("write_mode mismatch: got %q want %q", result.PersistWriteMode, tc.wantWriteMode)
			}
			if tc.wantNoticePresent {
				if result.RejectNotice.Type != protocol.FrameTypeConnectionNotice {
					t.Fatalf("notice frame type mismatch: got %q", result.RejectNotice.Type)
				}
				if result.RejectNotice.Code != protocol.ErrCodeObservedAddressMismatch {
					t.Fatalf("notice code mismatch: got %q", result.RejectNotice.Code)
				}
				if result.RejectNotice.Status != protocol.ConnectionStatusClosing {
					t.Fatalf("notice status mismatch: got %q", result.RejectNotice.Status)
				}
				// Details must round-trip through the typed helper.
				details, err := protocol.ParseObservedAddressMismatchDetails(result.RejectNotice.Details)
				if err != nil {
					t.Fatalf("parse notice details: %v", err)
				}
				if details.ObservedAddress == "" {
					t.Fatalf("notice details.observed_address is empty")
				}
			}
		})
	}
}

// TestBuildObservedMismatchNoticeOmitsEmptyDetails verifies that the
// notice constructor does not inject a literal "null" details payload
// when the observed IP is unknown — the frame must serialise without
// the details field so legacy peers parsing the JSON see no extra key.
func TestBuildObservedMismatchNoticeOmitsEmptyDetails(t *testing.T) {
	notice := buildObservedMismatchNotice("")
	if notice == nil {
		t.Fatalf("expected non-nil notice frame")
	}
	raw, err := json.Marshal(notice)
	if err != nil {
		t.Fatalf("marshal notice: %v", err)
	}
	if containsKey(raw, "details") {
		t.Fatalf("notice JSON must omit empty details, got %s", raw)
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
// write. Tests that need a cfg.AdvertiseAddress value can supply it via
// the returned pointer and EffectiveListenerEnabled will be true.
func newAdvertiseTestService(advertise string) *Service {
	return &Service{
		cfg: config.Node{
			Type:             config.NodeTypeFull,
			ListenerEnabled:  true,
			ListenerSet:      true,
			AdvertiseAddress: advertise,
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
// from an earlier match. Only world_mismatch and local_exception are
// explicit downgrade triggers.
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
			svc := newAdvertiseTestService("203.0.113.40:64646")
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

// TestApplyAdvertiseValidationResult_WorldMismatchDowngrades verifies
// that a previously announceable peer is downgraded to direct_only on a
// world_mismatch and that the misadvertise bucket is charged exactly
// banIncrementAdvertiseMismatch points. AdvertiseMismatchCount must
// advance monotonically so the ranking signal survives restarts.
func TestApplyAdvertiseValidationResult_WorldMismatchDowngrades(t *testing.T) {
	svc := newAdvertiseTestService("203.0.113.41:64646")
	const peerAddr domain.PeerAddress = "203.0.113.41:64646"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                peerAddr,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.41",
		TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
		TrustedAdvertisePort:   config.DefaultPeerPort,
		AdvertiseMismatchCount: 2,
	}
	svc.applyAdvertiseValidationResult(peerAddr, advertiseValidationResult{
		Decision:             advertiseDecisionWorldMismatch,
		ObservedIPHint:       "198.51.100.41",
		PersistAnnounceState: announceStateDirectOnly,
		PersistWriteMode:     persistWriteModeUpdateExisting,
	})
	pm := svc.persistedMeta[peerAddr]
	if pm == nil {
		t.Fatalf("persistedMeta entry vanished")
	}
	if pm.AnnounceState != announceStateDirectOnly {
		t.Fatalf("announce_state not downgraded: got %q want %q", pm.AnnounceState, announceStateDirectOnly)
	}
	if pm.AdvertiseMismatchCount != 3 {
		t.Fatalf("advertise_mismatch_count: got %d want 3", pm.AdvertiseMismatchCount)
	}
	if pm.ForgivableMisadvertisePoints != banIncrementAdvertiseMismatch {
		t.Fatalf("forgivable_misadvertise_points: got %d want %d",
			pm.ForgivableMisadvertisePoints, banIncrementAdvertiseMismatch)
	}
	if pm.LastObservedIP != "198.51.100.41" {
		t.Fatalf("last_observed_ip not recorded: got %q", pm.LastObservedIP)
	}
	if pm.LastObservedAt == nil {
		t.Fatalf("last_observed_at must be set on world_mismatch with hint")
	}
	if history := svc.observedIPHistoryForPeer(peerAddr); len(history) != 1 || history[0] != "198.51.100.41" {
		t.Fatalf("observed IP history not updated: %v", history)
	}
}

// TestApplyAdvertiseValidationResult_WorldMismatchSkipsCreate verifies
// that a world_mismatch decision does NOT create a new persistedMeta row
// when the peer was previously unknown. Downgrade-only write mode must
// not inject rows for misbehaving peers — that would inflate the top-500
// persistence budget with hostile data.
func TestApplyAdvertiseValidationResult_WorldMismatchSkipsCreate(t *testing.T) {
	svc := newAdvertiseTestService("203.0.113.42:64646")
	const peerAddr domain.PeerAddress = "203.0.113.42:64646"
	svc.applyAdvertiseValidationResult(peerAddr, advertiseValidationResult{
		Decision:             advertiseDecisionWorldMismatch,
		ObservedIPHint:       "198.51.100.42",
		PersistAnnounceState: announceStateDirectOnly,
		PersistWriteMode:     persistWriteModeUpdateExisting,
	})
	if _, ok := svc.persistedMeta[peerAddr]; ok {
		t.Fatalf("unexpected persistedMeta row created for unknown peer on world_mismatch")
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
	svc := newAdvertiseTestService("203.0.113.43:64646")
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

// TestApplyAdvertiseValidationResult_WorldMismatchCatchesPortChange
// reproduces the port-rotation leak: a peer previously known as
// 203.0.113.42:64646 and marked announceable presents itself in a
// later session as 203.0.113.42:55555 with a world_mismatch listen.
// The direct persistedMeta[peerAddress] lookup resolves against the
// new port and finds nothing, so without the observed-IP sweep the
// stale announceable row survives and keeps leaking through peer
// exchange. The sweep must demote the old row by matching on
// TrustedAdvertiseIP == ObservedIPHint and must also charge mismatch
// accounting on the surviving row — otherwise a peer could rotate
// ports to dodge the rollout-5 ranking penalty.
func TestApplyAdvertiseValidationResult_WorldMismatchCatchesPortChange(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const oldPeerAddr domain.PeerAddress = "203.0.113.42:64646"
	svc.persistedMeta[oldPeerAddr] = &peerEntry{
		Address:                      oldPeerAddr,
		AnnounceState:                announceStateAnnounceable,
		TrustedAdvertiseIP:           "203.0.113.42",
		TrustedAdvertiseSource:       trustedAdvertiseSourceInbound,
		TrustedAdvertisePort:         config.DefaultPeerPort,
		AdvertiseMismatchCount:       1,
		ForgivableMisadvertisePoints: banIncrementAdvertiseMismatch,
	}

	// New session keys off the rotated port — no direct row to match.
	const newPeerAddr domain.PeerAddress = "203.0.113.42:55555"
	svc.applyAdvertiseValidationResult(newPeerAddr, advertiseValidationResult{
		Decision:             advertiseDecisionWorldMismatch,
		ObservedIPHint:       "203.0.113.42",
		PersistAnnounceState: announceStateDirectOnly,
		PersistWriteMode:     persistWriteModeUpdateExisting,
	})

	pm := svc.persistedMeta[oldPeerAddr]
	if pm == nil {
		t.Fatalf("old persistedMeta row vanished unexpectedly")
	}
	if pm.AnnounceState != announceStateDirectOnly {
		t.Fatalf("stale announceable row not downgraded: got %q want %q",
			pm.AnnounceState, announceStateDirectOnly)
	}
	if pm.LastObservedIP != "203.0.113.42" {
		t.Fatalf("last_observed_ip not recorded on swept row: got %q", pm.LastObservedIP)
	}
	if pm.LastObservedAt == nil {
		t.Fatalf("last_observed_at must be set on swept row")
	}
	// Mismatch accounting must land on the surviving row even though
	// the incoming peerAddress resolves to nothing — otherwise the
	// port-rotation path silently skips rollout-5 ranking.
	if pm.AdvertiseMismatchCount != 2 {
		t.Fatalf("advertise_mismatch_count not charged on swept row: got %d want 2", pm.AdvertiseMismatchCount)
	}
	if pm.ForgivableMisadvertisePoints != 2*banIncrementAdvertiseMismatch {
		t.Fatalf("forgivable_misadvertise_points not charged on swept row: got %d want %d",
			pm.ForgivableMisadvertisePoints, 2*banIncrementAdvertiseMismatch)
	}
}

// TestApplyAdvertiseValidationResult_WorldMismatchNoDoubleCharge guards
// the dedup path: when the incoming peerAddress coincides with a row
// the sweep just charged (same trusted IP, same existing key), the
// main switch must not apply a second mismatch/points increment on
// the same event. A single world_mismatch is one misbehaviour — billing
// it twice would accelerate the peer to ban threshold artificially.
func TestApplyAdvertiseValidationResult_WorldMismatchNoDoubleCharge(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const peerAddr domain.PeerAddress = "203.0.113.43:64646"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                peerAddr,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.43",
		TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
		TrustedAdvertisePort:   config.DefaultPeerPort,
	}

	svc.applyAdvertiseValidationResult(peerAddr, advertiseValidationResult{
		Decision:             advertiseDecisionWorldMismatch,
		ObservedIPHint:       "203.0.113.43",
		PersistAnnounceState: announceStateDirectOnly,
		PersistWriteMode:     persistWriteModeUpdateExisting,
	})

	pm := svc.persistedMeta[peerAddr]
	if pm == nil {
		t.Fatalf("persistedMeta row vanished unexpectedly")
	}
	if pm.AnnounceState != announceStateDirectOnly {
		t.Fatalf("announce_state: got %q want %q", pm.AnnounceState, announceStateDirectOnly)
	}
	if pm.AdvertiseMismatchCount != 1 {
		t.Fatalf("double-charge: advertise_mismatch_count got %d want 1", pm.AdvertiseMismatchCount)
	}
	if pm.ForgivableMisadvertisePoints != banIncrementAdvertiseMismatch {
		t.Fatalf("double-charge: forgivable_misadvertise_points got %d want %d",
			pm.ForgivableMisadvertisePoints, banIncrementAdvertiseMismatch)
	}
}

// TestDowngradeAnnounceableByObservedIPLocked_MatchesTrustedIP verifies
// that the sweep matches on TrustedAdvertiseIP alone when the persisted
// Address is a hostname (the host part does not resemble an IP). This
// covers peers reached via DNS name but whose confirmed advertise was
// a raw IP.
func TestDowngradeAnnounceableByObservedIPLocked_MatchesTrustedIP(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const addr domain.PeerAddress = "peer.example:64646"
	svc.persistedMeta[addr] = &peerEntry{
		Address:                addr,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.50",
		TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
	}

	svc.mu.Lock()
	svc.downgradeAnnounceableByObservedIPLocked("203.0.113.50")
	svc.mu.Unlock()

	if pm := svc.persistedMeta[addr]; pm.AnnounceState != announceStateDirectOnly {
		t.Fatalf("TrustedAdvertiseIP match not demoted: got %q", pm.AnnounceState)
	}
}

// TestDowngradeAnnounceableByObservedIPLocked_LeavesUnrelatedUntouched
// asserts the sweep scope is exactly peers whose observed IP matches
// the hint. Unrelated announceable rows must remain unchanged — a
// targeted downgrade must never ripple into a mass state reset.
func TestDowngradeAnnounceableByObservedIPLocked_LeavesUnrelatedUntouched(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const other domain.PeerAddress = "203.0.113.60:64646"
	svc.persistedMeta[other] = &peerEntry{
		Address:                other,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.60",
		TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
	}

	svc.mu.Lock()
	svc.downgradeAnnounceableByObservedIPLocked("203.0.113.99")
	svc.mu.Unlock()

	if pm := svc.persistedMeta[other]; pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("unrelated peer demoted by sweep: got %q", pm.AnnounceState)
	}
}

// TestDowngradeAnnounceableByObservedIPLocked_EmptyHintIsNoop covers
// the guard against an empty hint — any caller that forwards an empty
// ObservedIPHint must not trigger a blanket downgrade.
func TestDowngradeAnnounceableByObservedIPLocked_EmptyHintIsNoop(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const addr domain.PeerAddress = "203.0.113.70:64646"
	svc.persistedMeta[addr] = &peerEntry{
		Address:                addr,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.70",
		TrustedAdvertiseSource: trustedAdvertiseSourceInbound,
	}

	svc.mu.Lock()
	svc.downgradeAnnounceableByObservedIPLocked("")
	svc.mu.Unlock()

	if pm := svc.persistedMeta[addr]; pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("empty hint must be no-op, got %q", pm.AnnounceState)
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

// TestValidateAdvertisedAddress_ForbiddenAdvertise verifies that a
// world-reachable observed address combined with a non-routable
// advertise (e.g. peer claiming 10.0.0.7) is classified as
// world_mismatch and rejected. No peer should be able to poison us into
// announcing a private-range address.
func TestValidateAdvertisedAddress_ForbiddenAdvertise(t *testing.T) {
	result := validateAdvertisedAddress("203.0.113.60:45123", protocol.Frame{
		Type:     "hello",
		Listener: "1",
		Listen:   "10.0.0.7:64646",
	})
	if result.Decision != advertiseDecisionWorldMismatch {
		t.Fatalf("expected world_mismatch, got %q", result.Decision)
	}
	if !result.ShouldReject {
		t.Fatalf("world_mismatch must set ShouldReject")
	}
	if result.RejectNotice == nil {
		t.Fatalf("world_mismatch must carry a RejectNotice")
	}
}

// TestRepayMisadvertisePenaltyOnAuth_Caps verifies the repay bucket
// invariant: a single successful auth can refund at most
// banIncrementAdvertiseMismatch, and the cumulative repay never exceeds
// what was originally charged for misadvertise.
func TestRepayMisadvertisePenaltyOnAuth_Caps(t *testing.T) {
	svc := newAdvertiseTestService("203.0.113.61:64646")
	const peerAddr domain.PeerAddress = "203.0.113.61:64646"
	const banIP domain.PeerIP = "203.0.113.61"

	// Two mismatch events → 2 * banIncrementAdvertiseMismatch charged.
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                      peerAddr,
		ForgivableMisadvertisePoints: 2 * banIncrementAdvertiseMismatch,
	}

	svc.mu.Lock()
	repaid := svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, banIP)
	svc.mu.Unlock()
	if repaid != banIncrementAdvertiseMismatch {
		t.Fatalf("first repay: got %d want %d", repaid, banIncrementAdvertiseMismatch)
	}
	if pm := svc.persistedMeta[peerAddr]; pm.ForgivableMisadvertisePoints != banIncrementAdvertiseMismatch {
		t.Fatalf("remaining misadvertise points: got %d want %d",
			pm.ForgivableMisadvertisePoints, banIncrementAdvertiseMismatch)
	}

	// Second repay drains the remaining bucket exactly.
	svc.mu.Lock()
	repaid = svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, banIP)
	svc.mu.Unlock()
	if repaid != banIncrementAdvertiseMismatch {
		t.Fatalf("second repay: got %d want %d", repaid, banIncrementAdvertiseMismatch)
	}

	// Third repay must be a no-op — the bucket is empty and the invariant
	// forbids refunding more than was charged for misadvertise.
	svc.mu.Lock()
	repaid = svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, banIP)
	svc.mu.Unlock()
	if repaid != 0 {
		t.Fatalf("third repay must be zero, got %d", repaid)
	}
	if pm := svc.persistedMeta[peerAddr]; pm.ForgivableMisadvertisePoints != 0 {
		t.Fatalf("bucket not drained: got %d", pm.ForgivableMisadvertisePoints)
	}
	if pm := svc.persistedMeta[peerAddr]; pm.MisadvertisePointsRepaid != 2*banIncrementAdvertiseMismatch {
		t.Fatalf("cumulative repay: got %d want %d",
			pm.MisadvertisePointsRepaid, 2*banIncrementAdvertiseMismatch)
	}
}

// TestRepayMisadvertisePenaltyOnAuth_MirrorsBanScore reproduces the
// forgiveness leak between the peer-level bucket and the transport-level
// ban table. Each world_mismatch charges two scores: the peerEntry
// forgivable bucket AND the per-IP s.bans[ip].Score via addBanScore.
// A successful auth must refund BOTH in lock-step — otherwise a noisy
// but honest peer keeps climbing toward banThreshold on the IP table
// even though its peer-level bucket is correctly decayed.
func TestRepayMisadvertisePenaltyOnAuth_MirrorsBanScore(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	svc.bans = map[string]banEntry{
		"203.0.113.80": {Score: 2 * banIncrementAdvertiseMismatch},
	}
	const peerAddr domain.PeerAddress = "203.0.113.80:64646"
	const banIP domain.PeerIP = "203.0.113.80"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                      peerAddr,
		ForgivableMisadvertisePoints: 2 * banIncrementAdvertiseMismatch,
	}

	// First successful auth refunds one increment in both places.
	svc.mu.Lock()
	svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, banIP)
	svc.mu.Unlock()
	if got := svc.bans["203.0.113.80"].Score; got != banIncrementAdvertiseMismatch {
		t.Fatalf("ban score after first repay: got %d want %d",
			got, banIncrementAdvertiseMismatch)
	}

	// Second successful auth drains the remaining balance exactly.
	svc.mu.Lock()
	svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, banIP)
	svc.mu.Unlock()
	if got := svc.bans["203.0.113.80"].Score; got != 0 {
		t.Fatalf("ban score after full repay: got %d want 0", got)
	}
}

// TestRepayMisadvertisePenaltyOnAuth_BanScoreClampsAtZero guards
// against over-refund: if the forgivable bucket still carries more
// points than the current s.bans[ip].Score (e.g. the ban score was
// partially cleared by another code path between charge and repay),
// the mirror refund must clamp at zero rather than go negative.
func TestRepayMisadvertisePenaltyOnAuth_BanScoreClampsAtZero(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	svc.bans = map[string]banEntry{
		"203.0.113.81": {Score: 10}, // less than banIncrementAdvertiseMismatch
	}
	const peerAddr domain.PeerAddress = "203.0.113.81:64646"
	const banIP domain.PeerIP = "203.0.113.81"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                      peerAddr,
		ForgivableMisadvertisePoints: banIncrementAdvertiseMismatch,
	}

	svc.mu.Lock()
	svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, banIP)
	svc.mu.Unlock()

	if got := svc.bans["203.0.113.81"].Score; got != 0 {
		t.Fatalf("ban score must clamp at zero, got %d", got)
	}
}

// TestRepayMisadvertisePenaltyOnAuth_MissingBanEntryIsSafe covers the
// case where the IP was never banned (clean peer): the repay must
// still update the peer-level bucket without touching s.bans.
func TestRepayMisadvertisePenaltyOnAuth_MissingBanEntryIsSafe(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	svc.bans = map[string]banEntry{} // empty — IP never seen in ban table
	const peerAddr domain.PeerAddress = "203.0.113.82:64646"
	const banIP domain.PeerIP = "203.0.113.82"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                      peerAddr,
		ForgivableMisadvertisePoints: banIncrementAdvertiseMismatch,
	}

	svc.mu.Lock()
	repaid := svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, banIP)
	svc.mu.Unlock()

	if repaid != banIncrementAdvertiseMismatch {
		t.Fatalf("repay must proceed when IP not in bans: got %d want %d",
			repaid, banIncrementAdvertiseMismatch)
	}
	if pm := svc.persistedMeta[peerAddr]; pm.ForgivableMisadvertisePoints != 0 {
		t.Fatalf("peer-level bucket not drained: got %d",
			pm.ForgivableMisadvertisePoints)
	}
	if _, exists := svc.bans["203.0.113.82"]; exists {
		t.Fatalf("bans map must not gain a phantom entry on refund")
	}
}

// TestRepayMisadvertisePenaltyOnAuth_HostnamePeerRefundsRealIP is the
// regression guard for the hostname peer divergence. Before the fix,
// repayMisadvertisePenaltyOnAuthLocked parsed the host component of
// peerAddress to key s.bans — which works for IP peers but silently
// fails for DNS / manually-added bootstrap peers whose peerAddress
// carries an unresolved hostname. addBanScore keys s.bans under the
// real TCP peer IP (from conn.RemoteAddr()), so a hostname lookup
// never matched and the IP-level ban score accumulated toward
// banThreshold while the peer-level bucket decayed normally — the
// exact divergence the earlier refund mirror was meant to close.
// The fix threads the verified ban IP through an explicit parameter;
// this test drives the hostname case directly.
func TestRepayMisadvertisePenaltyOnAuth_HostnamePeerRefundsRealIP(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const realIP = "203.0.113.90"
	svc.bans = map[string]banEntry{
		realIP: {Score: 2 * banIncrementAdvertiseMismatch},
	}
	// peerAddress is a hostname — NOT an IP. splitHostPort on this
	// would return "bootstrap.example.com", which must NOT be used as
	// the s.bans key.
	const peerAddr domain.PeerAddress = "bootstrap.example.com:64646"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                      peerAddr,
		ForgivableMisadvertisePoints: 2 * banIncrementAdvertiseMismatch,
	}

	svc.mu.Lock()
	repaid := svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, domain.PeerIP(realIP))
	svc.mu.Unlock()

	if repaid != banIncrementAdvertiseMismatch {
		t.Fatalf("hostname peer repay: got %d want %d",
			repaid, banIncrementAdvertiseMismatch)
	}
	if got := svc.bans[realIP].Score; got != banIncrementAdvertiseMismatch {
		t.Fatalf("ban score at real IP %s: got %d want %d (must be refunded, not hostname-keyed)",
			realIP, got, banIncrementAdvertiseMismatch)
	}
	if _, exists := svc.bans["bootstrap.example.com"]; exists {
		t.Fatalf("bans map must not gain a hostname-keyed entry")
	}
	if pm := svc.persistedMeta[peerAddr]; pm.ForgivableMisadvertisePoints != banIncrementAdvertiseMismatch {
		t.Fatalf("peer-level bucket: got %d want %d",
			pm.ForgivableMisadvertisePoints, banIncrementAdvertiseMismatch)
	}
}

// TestRepayMisadvertisePenaltyOnAuth_EmptyBanIPSkipsMirror verifies
// that an empty banIP (no live conn available) only moves the peer-
// level bucket and never touches s.bans — the peer-level accounting
// stays correct and the next successful auth with a known IP will
// try the mirror again.
func TestRepayMisadvertisePenaltyOnAuth_EmptyBanIPSkipsMirror(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	svc.bans = map[string]banEntry{
		"203.0.113.91": {Score: 2 * banIncrementAdvertiseMismatch},
	}
	const peerAddr domain.PeerAddress = "203.0.113.91:64646"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                      peerAddr,
		ForgivableMisadvertisePoints: banIncrementAdvertiseMismatch,
	}

	svc.mu.Lock()
	repaid := svc.repayMisadvertisePenaltyOnAuthLocked(peerAddr, "")
	svc.mu.Unlock()

	if repaid != banIncrementAdvertiseMismatch {
		t.Fatalf("peer-level repay must proceed with empty banIP: got %d want %d",
			repaid, banIncrementAdvertiseMismatch)
	}
	if got := svc.bans["203.0.113.91"].Score; got != 2*banIncrementAdvertiseMismatch {
		t.Fatalf("ban score must be untouched when banIP empty: got %d want %d",
			got, 2*banIncrementAdvertiseMismatch)
	}
	if pm := svc.persistedMeta[peerAddr]; pm.ForgivableMisadvertisePoints != 0 {
		t.Fatalf("peer-level bucket not drained: got %d",
			pm.ForgivableMisadvertisePoints)
	}
}

// TestHandleConnectionNotice_SetsOverride drives the outbound-side
// reaction: a well-formed observed-address-mismatch notice carrying a
// world-reachable observed IP must install a runtime override so the
// next outbound hello advertises the corrected IP.
func TestHandleConnectionNotice_SetsOverride(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")

	details, err := protocol.MarshalObservedAddressMismatchDetails("203.0.113.70")
	if err != nil {
		t.Fatalf("marshal details: %v", err)
	}
	svc.handleConnectionNotice(domain.PeerAddress("198.51.100.10:64646"), protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodeObservedAddressMismatch,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	})
	svc.mu.RLock()
	got := svc.trustedSelfAdvertiseIP
	svc.mu.RUnlock()
	if got != "203.0.113.70" {
		t.Fatalf("trustedSelfAdvertiseIP: got %q want %q", got, "203.0.113.70")
	}
}

// TestHandleConnectionNotice_IgnoresNonRoutable asserts that a peer
// reporting a private-range observed IP never downgrades our self
// advertise override. A hostile peer on the same LAN must not be able
// to make us announce 10.x / 192.168.x / 127.x to the rest of the
// network.
func TestHandleConnectionNotice_IgnoresNonRoutable(t *testing.T) {
	cases := []string{
		"10.0.0.5",
		"192.168.1.50",
		"127.0.0.1",
		"169.254.1.2",
		"100.64.0.9",
	}
	for _, observed := range cases {
		observed := observed
		t.Run(observed, func(t *testing.T) {
			svc := newAdvertiseTestService("198.51.100.10:64646")
			details, err := protocol.MarshalObservedAddressMismatchDetails(observed)
			if err != nil {
				t.Fatalf("marshal details: %v", err)
			}
			svc.handleConnectionNotice(domain.PeerAddress("198.51.100.10:64646"), protocol.Frame{
				Type:    protocol.FrameTypeConnectionNotice,
				Code:    protocol.ErrCodeObservedAddressMismatch,
				Details: details,
			})
			svc.mu.RLock()
			got := svc.trustedSelfAdvertiseIP
			svc.mu.RUnlock()
			if got != "" {
				t.Fatalf("non-routable observation should be ignored, got override %q", got)
			}
		})
	}
}

// TestHandleConnectionNotice_IgnoresWrongCode asserts that a notice
// with a different code never touches the self advertise override —
// only the observed-address-mismatch code may mutate it.
func TestHandleConnectionNotice_IgnoresWrongCode(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	details, _ := protocol.MarshalObservedAddressMismatchDetails("203.0.113.71")
	svc.handleConnectionNotice(domain.PeerAddress("198.51.100.10:64646"), protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    "unrelated-notice",
		Details: details,
	})
	svc.mu.RLock()
	got := svc.trustedSelfAdvertiseIP
	svc.mu.RUnlock()
	if got != "" {
		t.Fatalf("override set by unrelated code: got %q", got)
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
	svc := newAdvertiseTestService("198.51.100.10:64646")
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

	svc.mu.RLock()
	entry := svc.persistedMeta[peerAddr]
	ipBanCount := len(svc.remoteBannedIPs)
	svc.mu.RUnlock()
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
	svc := newAdvertiseTestService("198.51.100.10:64646")
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

	svc.mu.RLock()
	_, stillUnknown := svc.persistedMeta[peerAddr]
	svc.mu.RUnlock()
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
	svc := newAdvertiseTestService("198.51.100.10:64646")
	svc.persistedMeta[peerAddr] = &peerEntry{Address: peerAddr}

	// An Until field that is not a valid RFC3339 time.
	bad := json.RawMessage(`{"until":"not-a-timestamp","reason":"peer-ban"}`)

	svc.handleConnectionNotice(peerAddr, protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: bad,
	})

	svc.mu.RLock()
	entry := svc.persistedMeta[peerAddr]
	svc.mu.RUnlock()
	if entry != nil && entry.RemoteBannedUntil != nil {
		t.Fatalf("RemoteBannedUntil must remain unset when Details parsing fails, got %s",
			entry.RemoteBannedUntil.Format(time.RFC3339))
	}
}

// TestHandlePeerBannedNotice_BlacklistedPropagatesToSiblingIP is the
// regression guard for the blast-radius fix: a reason=blacklisted
// notice on one PeerAddress must populate the IP-wide record so every
// sibling PeerAddress behind the same egress IP is gated by the
// dialler — even siblings we have never heard of before. Without this
// the responder's IP-wide ban would only silence the single peer that
// happened to carry the notice and the dialler would keep hammering
// the other peers on the blacklisted IP.
func TestHandlePeerBannedNotice_BlacklistedPropagatesToSiblingIP(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.90:64646"
	svc := newAdvertiseTestService("198.51.100.10:64646")
	svc.remoteBannedIPs = make(map[string]remoteIPBanEntry)
	svc.persistedMeta[peerAddr] = &peerEntry{Address: peerAddr}

	until := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	details, err := protocol.MarshalPeerBannedDetails(until, protocol.PeerBannedReasonBlacklisted)
	if err != nil {
		t.Fatalf("marshal peer-banned details: %v", err)
	}

	svc.handleConnectionNotice(peerAddr, protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	})

	svc.mu.RLock()
	ipEntry, ok := svc.remoteBannedIPs["203.0.113.90"]
	svc.mu.RUnlock()
	if !ok {
		t.Fatal("reason=blacklisted must populate remoteBannedIPs keyed on the IP")
	}
	if !ipEntry.Until.Equal(until) {
		t.Fatalf("IP-wide entry Until: got %s want %s",
			ipEntry.Until.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	if ipEntry.Reason != string(protocol.PeerBannedReasonBlacklisted) {
		t.Fatalf("IP-wide entry Reason: got %q want %q",
			ipEntry.Reason, string(protocol.PeerBannedReasonBlacklisted))
	}

	// Sibling PeerAddress on the same IP — not even known yet — must be
	// suppressed via the IP-wide record.
	siblingUnknown := domain.PeerAddress("203.0.113.90:7070")
	svc.mu.RLock()
	sibBanned := svc.isPeerRemoteBannedLocked(siblingUnknown, time.Now().UTC())
	svc.mu.RUnlock()
	if !sibBanned {
		t.Fatal("unknown sibling on blacklisted IP must be suppressed by the dial gate")
	}

	// The sender's per-peer row must NOT be populated: reason=blacklisted
	// writes only the IP-wide record (the sender is still suppressed on
	// read through the IP-wide gate). A per-peer mirror would duplicate
	// state and force extra sync work on the recovery path.
	svc.mu.RLock()
	senderEntry := svc.persistedMeta[peerAddr]
	svc.mu.RUnlock()
	if senderEntry != nil && senderEntry.RemoteBannedUntil != nil {
		t.Fatalf("reason=blacklisted must not write a per-peer row on the sender, got RemoteBannedUntil=%s",
			senderEntry.RemoteBannedUntil.Format(time.RFC3339))
	}
	if senderEntry != nil && senderEntry.RemoteBanReason != "" {
		t.Fatalf("reason=blacklisted must not write RemoteBanReason on the sender, got %q",
			senderEntry.RemoteBanReason)
	}
}

// TestHandlePeerBannedNotice_PeerBanDoesNotPropagateToSiblings pins the
// symmetric contract: reason=peer-ban is scoped to one PeerAddress, so
// a sibling on the same IP MUST remain dialable when no IP-wide record
// exists. Confusing the two scopes would over-ban legitimate peers
// sharing an egress with one misbehaving node.
func TestHandlePeerBannedNotice_PeerBanDoesNotPropagateToSiblings(t *testing.T) {
	const peerAddr domain.PeerAddress = "203.0.113.91:64646"
	svc := newAdvertiseTestService("198.51.100.10:64646")
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

	// No IP-wide record must have been written.
	svc.mu.RLock()
	_, hasIPRecord := svc.remoteBannedIPs["203.0.113.91"]
	svc.mu.RUnlock()
	if hasIPRecord {
		t.Fatal("reason=peer-ban must NOT populate remoteBannedIPs")
	}

	// The original peer IS banned via the per-peer record.
	svc.mu.RLock()
	origBanned := svc.isPeerRemoteBannedLocked(peerAddr, time.Now().UTC())
	svc.mu.RUnlock()
	if !origBanned {
		t.Fatal("original peer must be suppressed by per-peer remote ban")
	}

	// A sibling on the same IP must remain dialable.
	sibling := domain.PeerAddress("203.0.113.91:7070")
	svc.mu.RLock()
	sibBanned := svc.isPeerRemoteBannedLocked(sibling, time.Now().UTC())
	svc.mu.RUnlock()
	if sibBanned {
		t.Fatal("sibling on same IP must stay dialable when reason was peer-ban")
	}
}

// TestObservedConsensusIPLocked_Threshold checks the consensus gate:
// below observedAddrConsensusThreshold distinct peers, no consensus IP
// is returned; at or above the threshold the most-voted IP wins.
func TestObservedConsensusIPLocked_Threshold(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")

	// One reporter: no consensus (threshold is 2).
	svc.mu.Lock()
	svc.observedAddrs[domain.PeerIdentity("peer-aaa")] = "203.0.113.80"
	_, ok := svc.observedConsensusIPLocked()
	svc.mu.Unlock()
	if ok {
		t.Fatalf("single reporter must not produce consensus")
	}

	// Two distinct reporters on the same IP: consensus.
	svc.mu.Lock()
	svc.observedAddrs[domain.PeerIdentity("peer-bbb")] = "203.0.113.80"
	ip, ok := svc.observedConsensusIPLocked()
	svc.mu.Unlock()
	if !ok {
		t.Fatalf("expected consensus at threshold")
	}
	if ip != "203.0.113.80" {
		t.Fatalf("consensus IP: got %q want %q", ip, "203.0.113.80")
	}

	// Two reporters disagreeing: no single IP crosses threshold even
	// though the aggregate count is >= threshold.
	svc.mu.Lock()
	svc.observedAddrs = map[domain.PeerIdentity]string{
		domain.PeerIdentity("peer-aaa"): "203.0.113.80",
		domain.PeerIdentity("peer-bbb"): "203.0.113.81",
	}
	_, ok = svc.observedConsensusIPLocked()
	svc.mu.Unlock()
	if ok {
		t.Fatalf("split-vote must not produce consensus")
	}
}

// TestSelfAdvertiseEndpoint_Priority exercises the override → consensus
// → config priority. The port always comes from cfg.AdvertiseAddress
// (or DefaultPeerPort) when no net.Listener is bound in tests.
func TestSelfAdvertiseEndpoint_Priority(t *testing.T) {
	t.Run("override_wins_over_consensus", func(t *testing.T) {
		svc := newAdvertiseTestService("198.51.100.10:64646")
		svc.trustedSelfAdvertiseIP = "203.0.113.90"
		// Consensus that should be ignored in favour of the override.
		svc.observedAddrs = map[domain.PeerIdentity]string{
			"peer-aaa": "203.0.113.91",
			"peer-bbb": "203.0.113.91",
		}
		if got := svc.selfAdvertiseEndpoint(); got != "203.0.113.90:64646" {
			t.Fatalf("endpoint: got %q want %q", got, "203.0.113.90:64646")
		}
	})

	t.Run("consensus_wins_over_config", func(t *testing.T) {
		svc := newAdvertiseTestService("198.51.100.10:64646")
		svc.observedAddrs = map[domain.PeerIdentity]string{
			"peer-aaa": "203.0.113.92",
			"peer-bbb": "203.0.113.92",
		}
		if got := svc.selfAdvertiseEndpoint(); got != "203.0.113.92:64646" {
			t.Fatalf("endpoint: got %q want %q", got, "203.0.113.92:64646")
		}
	})

	t.Run("config_fallback_when_no_override_no_consensus", func(t *testing.T) {
		svc := newAdvertiseTestService("198.51.100.10:64646")
		if got := svc.selfAdvertiseEndpoint(); got != "198.51.100.10:64646" {
			t.Fatalf("endpoint: got %q want %q", got, "198.51.100.10:64646")
		}
	})

	t.Run("empty_when_listener_disabled", func(t *testing.T) {
		svc := newAdvertiseTestService("198.51.100.10:64646")
		svc.cfg.ListenerEnabled = false
		svc.cfg.ListenerSet = true
		if got := svc.selfAdvertiseEndpoint(); got != "" {
			t.Fatalf("disabled listener must return empty, got %q", got)
		}
	})

	// Config-fallback branch must take the real local listener port and
	// only the HOST from cfg.AdvertiseAddress. Using the raw
	// cfg.AdvertiseAddress string in this branch would silently
	// re-introduce a stale port whenever the config drifts from the
	// bound port (operator edited config, listener picked a different
	// port on startup). The invariant is that hello.listen always
	// advertises the real listener port.
	t.Run("config_fallback_uses_listener_port", func(t *testing.T) {
		svc := newAdvertiseTestService("198.51.100.10:55555") // stale port
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen: %v", err)
		}
		defer func() { _ = ln.Close() }()
		svc.listener = ln

		_, listenerPort, ok := splitHostPort(ln.Addr().String())
		if !ok || listenerPort == "" || listenerPort == "0" {
			t.Fatalf("listener port unusable: %q", ln.Addr().String())
		}
		if listenerPort == "55555" {
			t.Skipf("OS handed out the stale port, cannot distinguish fallback branches")
		}

		want := net.JoinHostPort("198.51.100.10", listenerPort)
		if got := svc.selfAdvertiseEndpoint(); got != want {
			t.Fatalf("endpoint: got %q want %q (cfg port 55555 must be ignored in favour of listener port)",
				got, want)
		}
	})
}

// TestRecordObservedIPHistory_BoundAndDedup verifies both the
// bounded-history cap at observedIPHistoryMaxSize entries and the
// adjacent-duplicate suppression that stops the outbound convergence
// loop from ping-ponging on a repeated observation.
func TestRecordObservedIPHistory_BoundAndDedup(t *testing.T) {
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const peerAddr domain.PeerAddress = "203.0.113.100:64646"

	// Adjacent duplicates fold to a single entry.
	svc.mu.Lock()
	svc.recordObservedIPHintLocked(peerAddr, "203.0.113.200")
	svc.recordObservedIPHintLocked(peerAddr, "203.0.113.200")
	svc.recordObservedIPHintLocked(peerAddr, "203.0.113.200")
	svc.mu.Unlock()
	if history := svc.observedIPHistoryForPeer(peerAddr); len(history) != 1 {
		t.Fatalf("adjacent duplicates must be deduplicated: %v", history)
	}

	// Overflow past the cap retains only the last observedIPHistoryMaxSize
	// distinct entries.
	svc.mu.Lock()
	for i := 0; i < observedIPHistoryMaxSize+3; i++ {
		svc.recordObservedIPHintLocked(peerAddr, entryIP(i))
	}
	svc.mu.Unlock()
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
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const peerAddr domain.PeerAddress = "203.0.113.120:64646"
	svc.recordOutboundConfirmed(peerAddr, "203.0.113.120", "64646")

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
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const peerAddr domain.PeerAddress = "203.0.113.121:64646"
	svc.recordOutboundConfirmed(peerAddr, "::ffff:203.0.113.121", "64646")

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
	svc := newAdvertiseTestService("198.51.100.10:64646")
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
		svc.recordOutboundConfirmed(peerAddr, badIP, "64646")
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
	svc := newAdvertiseTestService("198.51.100.10:64646")
	const peerAddr domain.PeerAddress = "peer.example:64646"
	svc.persistedMeta[peerAddr] = &peerEntry{
		Address:                peerAddr,
		AnnounceState:          announceStateAnnounceable,
		TrustedAdvertiseIP:     "203.0.113.150", // canonical IP from a prior session
		TrustedAdvertiseSource: trustedAdvertiseSourceOutbound,
		TrustedAdvertisePort:   "64646",
	}

	svc.recordOutboundConfirmed(peerAddr, domain.PeerIP("peer.example"), "64646")

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
	svc := newAdvertiseTestService("198.51.100.10:64646")
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
	svc := newAdvertiseTestService("198.51.100.10:64646")
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
	svc := newAdvertiseTestService("198.51.100.10:64646")
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
	svc := newAdvertiseTestService("198.51.100.10:64646")

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
	svc := newAdvertiseTestService("198.51.100.10:64646")

	// Seed an IP-wide remote-ban entry for the dialled host.
	future := time.Now().UTC().Add(24 * time.Hour)
	svc.remoteBannedIPs[bannedIP] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}
	// persistedMeta is intentionally empty: the IP-wide record is
	// independent of the per-peer row and must be cleared on its own.

	svc.recordOutboundAuthSuccess(peerAddr, "203.0.113.181:64646")

	if _, ok := svc.remoteBannedIPs[bannedIP]; ok {
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
	svc := newAdvertiseTestService("203.0.113.10:64646")

	future := time.Now().UTC().Add(12 * time.Hour)
	svc.remoteBannedIPs[bannedIP] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}

	// Pre-gate: sibling2 must be suppressed before we recover via sibling1.
	if !svc.isPeerRemoteBannedLocked(sibling2, time.Now().UTC()) {
		t.Fatalf("pre-condition: sibling2 must be suppressed by the IP-wide ban")
	}

	svc.recordOutboundAuthSuccess(sibling1, "198.51.100.55:64646")

	if svc.isPeerRemoteBannedLocked(sibling2, time.Now().UTC()) {
		t.Fatalf("sibling2 must no longer be suppressed after sibling1's handshake")
	}
	if _, ok := svc.remoteBannedIPs[bannedIP]; ok {
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
	svc := newAdvertiseTestService("203.0.113.10:64646")

	future := time.Now().UTC().Add(12 * time.Hour)
	svc.remoteBannedIPs[bannedIP] = remoteIPBanEntry{
		Until:  future,
		Reason: string(protocol.PeerBannedReasonBlacklisted),
	}
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

	if _, ok := svc.remoteBannedIPs[bannedIP]; ok {
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

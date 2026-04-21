// Advertise-address convergence helpers.
//
// This file owns the decision logic that decides whether an inbound
// hello frame should be accepted, rejected with connection_notice, or
// downgraded to direct-only — plus the Service-owned persistence
// writers invoked from dispatchNetworkFrame / peerSessionRequest /
// routing_integration. It keeps the decision tree out of
// dispatchNetworkFrame so the inbound hello path stays readable and so
// the matrix from the advertise convergence contract lives in one place.
//
// See docs/protocol/handshake.md section "Advertise convergence" for
// the wire/behavioural contract this file implements.

package node

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// advertiseDecision is the machine-readable classification produced by
// validateAdvertisedAddress. Callers switch on this value rather than
// re-running the same string/IP checks inline.
//
// Phase 1 deprecation note (ProtocolVersion=11):
// advertiseDecisionWorldMismatch is no longer produced by the штатный
// runtime path — observed TCP IP is authoritative and no longer compared
// against hello.listen.host for reject decisions. The constant is kept so
// compat / legacy test call sites still compile; runtime logic must treat
// it as unreachable and must never emit RejectNotice on this path.
type advertiseDecision string

const (
	advertiseDecisionNonListener    advertiseDecision = "non_listener"
	advertiseDecisionLegacyDirect   advertiseDecision = "legacy_direct"
	advertiseDecisionMatch          advertiseDecision = "match"
	advertiseDecisionLocalException advertiseDecision = "local_exception"
	// advertiseDecisionWorldMismatch is deprecated in the phase 1
	// deprecation rollout (ProtocolVersion=11). validateAdvertisedAddress
	// never returns it; it survives only for legacy compat and tests that
	// exercise the dead-code downgrade sweep helper. Do not branch штатный
	// logic on it.
	advertiseDecisionWorldMismatch advertiseDecision = "world_mismatch"
	advertiseDecisionInvalid       advertiseDecision = "invalid"
)

// persistWriteMode is the closed enum for advertiseValidationResult.PersistWriteMode.
// Kept as a named string so the human-readable form stays unchanged
// (diagnostics/logs use the raw value), but any stray untyped string
// assignment is a compile error.
type persistWriteMode string

// Persist write modes returned by validateAdvertisedAddress. Callers
// must apply PersistAnnounceState only through the mode carried here —
// the same "direct_only" announce_state can be a create_or_update write
// (accept branches) or an update_existing_only write (downgrade from a
// previously announceable record), and those two operations must never
// be conflated.
const (
	persistWriteModeSkip           persistWriteMode = "skip_update"
	persistWriteModeCreateOrUpdate persistWriteMode = "create_or_update"
	persistWriteModeUpdateExisting persistWriteMode = "update_existing_only"
)

// advertiseValidationResult is the full machine-readable outcome of the
// inbound advertise check. No side effects — the caller is responsible
// for sending RejectNotice, closing the socket, and applying the
// persistence write through the dedicated Service helpers.
//
// AdvertisePort is the resolved self-reported listening port for this
// inbound frame. Phase 1 deprecation (ProtocolVersion=11) introduces it as
// the single authoritative port source for passive learning: value is
// frame.AdvertisePort when PeerPort.IsValid, else the PeerPort form of
// config.DefaultPeerPort. The inbound TCP source port is NEVER used as a
// listening port — that was the contract violation the phase 1 rollout
// eliminates. Textual conversion happens only at persisted / JoinHostPort
// boundaries; the runtime result carries the domain value.
//
// ShouldReject / RejectNotice survive only for compat with the deprecated
// world_mismatch reject path; the штатный v11 runtime never sets them.
type advertiseValidationResult struct {
	Decision               advertiseDecision
	NormalizedObservedIP   domain.PeerIP
	NormalizedAdvertisedIP domain.PeerIP
	ObservedIPHint         domain.PeerIP
	AdvertisePort          domain.PeerPort  // resolved listening port; DefaultPeerPort on absent/invalid wire value
	PersistAnnounceState   announceState    // announceStateDirectOnly | announceStateAnnounceable | announceStateUnset
	PersistWriteMode       persistWriteMode // persistWriteMode* constants
	AllowAnnounce          bool
	ShouldReject           bool
	RejectNotice           *protocol.Frame
}

// usableListen reports whether a self-reported hello.Listen string is
// structurally usable as an advertised listening endpoint: successfully
// parses as host:port, the host after normalisation is a concrete
// address/hostname (not a wildcard bind like 0.0.0.0 / :: / empty), and
// the port is not the zero placeholder.
func usableListen(listen string) (host, port string, ok bool) {
	host, port, splitOK := splitHostPort(strings.TrimSpace(listen))
	if !splitOK {
		return "", "", false
	}
	if port == "" || port == "0" {
		return "", "", false
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "", "", false
	}
	// Reject IPv4/IPv6 wildcard binds. These mean "bind to all" on the
	// peer's side, which is never a usable dial target for anyone else.
	if isUnspecifiedIPHost(host) {
		return "", "", false
	}
	return host, port, true
}

// buildObservedMismatchNotice constructs a connection_notice frame for
// ErrCodeObservedAddressMismatch carrying observed_address in details.
// Returns nil if observedIP is empty (no actionable hint to send).
func buildObservedMismatchNotice(observedIP domain.PeerIP) *protocol.Frame {
	details, err := protocol.MarshalObservedAddressMismatchDetails(string(observedIP))
	if err != nil {
		return nil
	}
	// Fallback: if MarshalObservedAddressMismatchDetails returned nil
	// (empty observed IP) we still emit the notice type, but with no
	// details — older behaviour where the peer only sees a disconnect
	// stays safe because ShouldReject is the authoritative gate.
	if details == nil {
		details = json.RawMessage(nil)
	}
	return &protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodeObservedAddressMismatch,
		Status:  protocol.ConnectionStatusClosing,
		Error:   "advertised address does not match observed remote address",
		Details: details,
	}
}

// extractAdvertisePort resolves the listening port the peer is announcing
// in an inbound hello frame under the phase 1 deprecation contract
// (ProtocolVersion=11). The only authoritative port source is
// frame.AdvertisePort: accepted when PeerPort.IsValid (inclusive 1..65535),
// collapsed to the PeerPort form of config.DefaultPeerPort on any other
// value (zero, missing, negative, out-of-range, or — at the decoder level
// — a non-integer wire payload which already arrives as zero after
// json.Unmarshal into PeerPort). The inbound TCP source port is
// intentionally never considered here; reusing it as a listening port is
// exactly the contract violation the phase 1 rollout eliminates.
func extractAdvertisePort(frame protocol.Frame) domain.PeerPort {
	if frame.AdvertisePort.IsValid() {
		return frame.AdvertisePort
	}
	value, err := strconv.Atoi(config.DefaultPeerPort)
	if err != nil {
		return 0
	}
	return domain.PeerPort(value)
}

// validateAdvertisedAddress is the pure decision helper invoked from
// dispatchNetworkFrame's case "hello". It does not touch shared state
// and does not write to the network — the caller is responsible for
// applying the persistence write. Phase 1 deprecation contract
// (ProtocolVersion=11): observed TCP IP is the sole authoritative source
// of the peer's IP, hello.listen.host is ignored as a truth input, and
// the штатный runtime path NEVER rejects on mismatch and NEVER emits
// connection_notice{observed-address-mismatch}. Legacy constants
// (advertiseDecisionWorldMismatch, advertiseDecisionInvalid) survive in
// the type for test/compat reasons but are unreachable from this helper.
//
// Ordering of the v11 decision matrix:
//  1. listener="0"                          → non_listener (accept, direct_only)
//  2. listener not "1" and listen unusable  → legacy_direct (accept, direct_only)
//  3. observed IP not parseable as IPv4/IPv6 → local_exception (accept, direct_only, no candidate)
//  4. observed IP non-routable / local       → local_exception (accept, direct_only, no candidate)
//  5. observed IP world-routable             → match (accept, announceable,
//                                              candidate = observed_IP:advertise_port)
//
// observedTCPAddr is the RemoteAddr() string of the inbound conn
// (host:port). frame is the parsed hello frame.
func validateAdvertisedAddress(observedTCPAddr string, frame protocol.Frame) advertiseValidationResult {
	result := advertiseValidationResult{
		PersistWriteMode: persistWriteModeSkip,
		AdvertisePort:    extractAdvertisePort(frame),
	}

	observedHost := remoteIPFromString(observedTCPAddr)
	result.NormalizedObservedIP = domain.PeerIP(canonicalIPFromHost(observedHost))

	// Branch 1: peer explicitly declared itself non-listener. Accept,
	// record as direct_only, never learn a candidate.
	if strings.TrimSpace(frame.Listener) == "0" {
		result.Decision = advertiseDecisionNonListener
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.AllowAnnounce = false
		return result
	}

	// Branch 2: legacy peer with no explicit listener flag AND no usable
	// listen string. A peer that sends listener="1" explicitly passes
	// through to the observed-IP branches below, even if its listen field
	// is empty — hello.listen is not a truth source under v11, only an
	// RFC-compatibility echo. Without an explicit listener="1" flag and
	// without a usable listen string we cannot claim the peer is a
	// listener, so we fall back to direct_only accept.
	if strings.TrimSpace(frame.Listener) != "1" {
		if _, _, ok := usableListen(frame.Listen); !ok {
			result.Decision = advertiseDecisionLegacyDirect
			result.PersistAnnounceState = announceStateDirectOnly
			result.PersistWriteMode = persistWriteModeCreateOrUpdate
			result.AllowAnnounce = false
			return result
		}
	}

	// hello.listen.host is intentionally NOT consulted as a truth input in
	// v11. We keep it only as a diagnostic echo for mixed-network compat:
	// the NormalizedAdvertisedIP field still surfaces in logs / tests, but
	// no branch below decides on it.
	if host, _, listenOK := usableListen(frame.Listen); listenOK && isIPHost(host) {
		result.NormalizedAdvertisedIP = domain.PeerIP(canonicalIPFromHost(host))
	}

	// Branch 3: observed side is not a parseable IP (e.g. non-IP
	// transport, or malformed RemoteAddr from a buggy transport wrapper).
	// We cannot learn a candidate from it, but v11 contract says we still
	// accept the handshake — never reject on advertise-learning grounds.
	if !isIPHost(observedHost) {
		result.Decision = advertiseDecisionLocalException
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.AllowAnnounce = false
		return result
	}

	// Branch 4: observed IP is local / private / CGNAT / link-local /
	// ULA / loopback / wildcard. Accept quietly, record direct_only, and
	// explicitly do NOT write any announce candidate — routing a
	// non-routable address through peer exchange would leak it to peers
	// that cannot reach it.
	if isNonRoutableIPHost(observedHost) {
		result.Decision = advertiseDecisionLocalException
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.ObservedIPHint = result.NormalizedObservedIP
		result.AllowAnnounce = false
		return result
	}

	// Branch 5: observed IP is world-routable. This is the sole announce
	// candidate source under v11 — hello.listen.host disagreement is not
	// a reject trigger and not a mismatch event. The candidate endpoint
	// is observed_IP:advertise_port (advertise_port already collapsed to
	// DefaultPeerPort on absent/invalid wire value). The persisted
	// TrustedAdvertiseIP follows the same rule: it mirrors observed IP,
	// NOT the advertised listen host.
	result.Decision = advertiseDecisionMatch
	result.NormalizedAdvertisedIP = result.NormalizedObservedIP
	result.ObservedIPHint = result.NormalizedObservedIP
	result.PersistAnnounceState = announceStateAnnounceable
	result.PersistWriteMode = persistWriteModeCreateOrUpdate
	result.AllowAnnounce = true
	return result
}

// downgradeAnnounceableByObservedIPLocked demotes any previously
// announceable peer whose trusted-advertise IP matches the current
// observed IP. Used on world_mismatch to catch the case where the peer
// keeps its real IP but changes the claimed port between sessions: the
// straight persistedMeta[peerAddress] lookup would miss the old
// (observed_ip, default_port) row and the announceable trust would
// survive the downgrade.
//
// The sweep also charges mismatch accounting on each demoted row —
// AdvertiseMismatchCount++ and ForgivableMisadvertisePoints +=
// banIncrementAdvertiseMismatch. Without this, a peer that rotates its
// claimed port between sessions would have its old announceable trust
// flipped but would never accumulate the persisted ranking penalty
// that rollout 5 depends on, effectively dodging the misbehaviour cost.
//
// The set of keys touched by the sweep is returned so the main switch
// in applyAdvertiseValidationResultLocked can avoid a double charge
// when peerAddress happens to coincide with a row already updated here.
//
// Scans are bounded by len(persistedMeta) which is capped at
// maxPersistedPeers, so cost is predictable.
// Must be called with s.mu held.
func (s *Service) downgradeAnnounceableByObservedIPLocked(observedIP domain.PeerIP) map[domain.PeerAddress]struct{} {
	if observedIP == "" {
		return nil
	}
	var affected map[domain.PeerAddress]struct{}
	now := time.Now().UTC()
	for _, pm := range s.persistedMeta {
		if pm == nil {
			continue
		}
		if pm.AnnounceState != announceStateAnnounceable {
			continue
		}
		// Match either the previously recorded trusted IP (strong
		// evidence — the observation directly contradicts a past
		// confirmed advertise) OR the host part of the persisted
		// peer address (covers entries never fully confirmed through
		// applyAdvertiseValidationResult / recordOutboundConfirmed).
		if pm.TrustedAdvertiseIP != observedIP {
			host, _, ok := splitHostPort(string(pm.Address))
			if !ok || domain.PeerIP(host) != observedIP {
				continue
			}
		}
		pm.AnnounceState = announceStateDirectOnly
		pm.LastObservedIP = observedIP
		pm.LastObservedAt = &now
		// World_mismatch accounting follows the demotion: the row
		// being swept IS the misbehaving peer (it changed its claimed
		// port while keeping the IP). Without this increment the
		// persisted ranking signal never reflects the mismatch on the
		// surviving row and the peer escapes the rollout-5 penalty.
		pm.AdvertiseMismatchCount++
		pm.ForgivableMisadvertisePoints += banIncrementAdvertiseMismatch
		if affected == nil {
			affected = make(map[domain.PeerAddress]struct{})
		}
		affected[pm.Address] = struct{}{}
	}
	return affected
}

// applyAdvertiseValidationResult is the single Service-owned writer that
// projects an advertiseValidationResult onto persistedMeta and the
// runtime observed-IP history. All writes happen under s.mu.
//
// peerAddress is the sanitised overlay address used for health keying —
// the caller must have already derived it from the verified TCP IP.
// observedIP is the canonical IP form extracted from the inbound TCP
// connection. authConfirmed=true is only legal for inbound match where
// observed == advertised and no auth_ok is required on inbound, per
// the RFC inbound path.
func (s *Service) applyAdvertiseValidationResult(peerAddress domain.PeerAddress, result advertiseValidationResult) {
	if peerAddress == "" {
		return
	}
	// Skip-mode decisions (e.g. advertiseDecisionInvalid) must not
	// touch persistence at all — the inbound was rejected as protocol
	// error and the peer should remain unknown/untracked.
	if result.PersistWriteMode == persistWriteModeSkip {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyAdvertiseValidationResultLocked(peerAddress, result)
}

// applyAdvertiseValidationResultLocked is the same writer without the
// lock acquisition. Used by callers that already hold s.mu (notably the
// outbound convergence path).
func (s *Service) applyAdvertiseValidationResultLocked(peerAddress domain.PeerAddress, result advertiseValidationResult) {
	// World-mismatch downgrade must also sweep any announceable entry
	// keyed by the same observed IP but a different port. Without this,
	// a peer that keeps its IP but rotates the claimed listen port
	// leaves its old announceable row untouched and continues leaking
	// through peer exchange. The sweep runs regardless of whether the
	// direct peerAddress lookup finds a row and returns the set of rows
	// it already charged with mismatch accounting so the main switch
	// below can skip a duplicate increment when peerAddress coincides
	// with one of those rows.
	var sweptMismatched map[domain.PeerAddress]struct{}
	if result.Decision == advertiseDecisionWorldMismatch && result.ObservedIPHint != "" {
		sweptMismatched = s.downgradeAnnounceableByObservedIPLocked(result.ObservedIPHint)
	}

	pm := s.persistedMeta[peerAddress]
	if pm == nil {
		if result.PersistWriteMode == persistWriteModeUpdateExisting {
			// Downgrade-only branch: do not create a new row just
			// because we saw a misbehaving peer for the first time.
			if result.ObservedIPHint != "" {
				// Keep a transient observation hint in runtime memory
				// so the outbound convergence loop can still use it.
				s.recordObservedIPHintLocked(peerAddress, result.ObservedIPHint)
			}
			return
		}
		now := time.Now().UTC()
		pm = &peerEntry{
			Address: peerAddress,
			AddedAt: &now,
			Source:  domain.PeerSourceAnnounce,
		}
		s.persistedMeta[peerAddress] = pm
	}

	// Sticky-state guard: a previously announceable peer downgrades to
	// direct_only only on explicit downgrade events (world_mismatch or
	// local_exception after announceable). We never overwrite
	// announceable with the same state twice, and we never auto-
	// downgrade just because the new decision happens to be direct_only
	// under a create_or_update write that is not a downgrade trigger.
	previouslyAnnounceable := pm.AnnounceState == announceStateAnnounceable

	switch result.Decision {
	case advertiseDecisionMatch:
		pm.AnnounceState = announceStateAnnounceable
		// Under v11 NormalizedAdvertisedIP mirrors the observed IP (see
		// validateAdvertisedAddress branch 5). TrustedAdvertisePort is the
		// self-reported hello.advertise_port, validated and collapsed to
		// DefaultPeerPort on absent/invalid wire value by extractAdvertisePort.
		pm.TrustedAdvertiseIP = result.NormalizedAdvertisedIP
		pm.TrustedAdvertiseSource = trustedAdvertiseSourceInbound
		// Defensive: synthesised results from legacy tests may leave
		// AdvertisePort at the zero PeerPort sentinel. Collapse to
		// DefaultPeerPort so the persisted row never stores an empty port.
		// Persisted schema keeps the port as string; domain→string conversion
		// happens here at the single persistence-writer boundary.
		if result.AdvertisePort.IsValid() {
			pm.TrustedAdvertisePort = strconv.Itoa(int(result.AdvertisePort))
		} else {
			pm.TrustedAdvertisePort = config.DefaultPeerPort
		}
	case advertiseDecisionNonListener, advertiseDecisionLegacyDirect:
		// Do not clobber announceable trust for an already-known
		// listener peer just because the current session declared
		// listener=0 or omitted listen. RFC sticky-state rule.
		if !previouslyAnnounceable {
			pm.AnnounceState = announceStateDirectOnly
		}
	case advertiseDecisionLocalException:
		if previouslyAnnounceable {
			// Explicit downgrade trigger.
			pm.AnnounceState = announceStateDirectOnly
		} else {
			pm.AnnounceState = announceStateDirectOnly
		}
	case advertiseDecisionWorldMismatch:
		if previouslyAnnounceable {
			pm.AnnounceState = announceStateDirectOnly
		}
		// Mismatch metadata — count only on world_mismatch. Skip when
		// the sweep above already charged this exact row, otherwise a
		// single world_mismatch event would bill the same row twice.
		if _, alreadyCharged := sweptMismatched[peerAddress]; !alreadyCharged {
			pm.AdvertiseMismatchCount++
			pm.ForgivableMisadvertisePoints += banIncrementAdvertiseMismatch
		}
	case advertiseDecisionInvalid:
		// Unreachable with PersistWriteMode != skip, but fall through
		// without mutating state just in case.
		return
	}

	if result.ObservedIPHint != "" {
		now := time.Now().UTC()
		pm.LastObservedIP = result.ObservedIPHint
		pm.LastObservedAt = &now
		s.recordObservedIPHintLocked(peerAddress, result.ObservedIPHint)
	}
}

// applyAdvertiseOnInboundAccept is the single call site both inbound
// accept branches (with and without identity fields) use to project the
// advertise decision onto persistedMeta. Keeps the dispatchNetworkFrame
// case "hello" body small and centralises the peerAddress derivation.
//
// tcpAddr is the verified RemoteAddr() of the inbound conn. frame is
// the parsed hello. result is the outcome of validateAdvertisedAddress.
// It is a no-op when the decision skips persistence.
func (s *Service) applyAdvertiseOnInboundAccept(tcpAddr string, frame protocol.Frame, result advertiseValidationResult) {
	if result.PersistWriteMode == persistWriteModeSkip {
		return
	}
	claimed := strings.TrimSpace(frame.Listen)
	if claimed == "" {
		claimed = strings.TrimSpace(frame.Address)
	}
	if claimed == "" {
		return
	}
	peerAddr := sanitizeInboundAddress(tcpAddr, claimed)
	if peerAddr == "" {
		return
	}
	s.applyAdvertiseValidationResult(domain.PeerAddress(peerAddr), result)
}

// recordOutboundConfirmed is the outbound success writer invoked by
// peerSessionRequest and routing_integration after a completed
// hello → welcome → auth_ok handshake. dialedIP MUST be a canonical IP
// — the caller is expected to derive it from the live TCP
// connection's RemoteAddr, not from peerAddress (which may be a
// hostname for DNS/manual peers). dialedPort is the real port used
// for the session — never 64646 as a default — expressed as a
// domain.PeerPort so the compiler catches any silent mix-up with
// generic ints the caller might have lying around.
//
// Contract: if dialedIP is not a parseable IP, or dialedPort is not a
// valid PeerPort (1..65535), the call is a no-op. TrustedAdvertiseIP
// is consumed as a canonical IP by the observed-IP downgrade sweep
// (downgradeAnnounceableByObservedIPLocked compares it against
// observed IPs extracted from real TCP RemoteAddrs). Storing a
// hostname here would permanently break that compare: a later
// world_mismatch targeting the same peer could not match its
// hostname-valued TrustedAdvertiseIP and the announceable row would
// survive the downgrade. Persisted schema keeps the port as string;
// the domain→string conversion happens here at the single writer
// boundary.
func (s *Service) recordOutboundConfirmed(peerAddress domain.PeerAddress, dialedIP domain.PeerIP, dialedPort domain.PeerPort) {
	if peerAddress == "" || dialedIP == "" || !dialedPort.IsValid() {
		return
	}
	canonIP := domain.PeerIP(canonicalIPFromHost(string(dialedIP)))
	if canonIP == "" {
		// Caller violated the contract: dialedIP is not a canonical
		// IP (e.g. hostname, .onion, malformed). Refuse the write —
		// the advertise-convergence layer must never persist a
		// non-IP value in TrustedAdvertiseIP.
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pm := s.persistedMeta[peerAddress]
	if pm == nil {
		now := time.Now().UTC()
		pm = &peerEntry{
			Address: peerAddress,
			AddedAt: &now,
			Source:  domain.PeerSourceAnnounce,
		}
		s.persistedMeta[peerAddress] = pm
	}
	pm.AnnounceState = announceStateAnnounceable
	pm.TrustedAdvertiseIP = canonIP
	pm.TrustedAdvertiseSource = trustedAdvertiseSourceOutbound
	pm.TrustedAdvertisePort = strconv.Itoa(int(dialedPort))
}

// recordObservedIPHintLocked updates the runtime-only history of
// observed IP hints for a single remote peer. Bounded at
// observedIPHistoryMaxSize. Must be called with s.mu held.
func (s *Service) recordObservedIPHintLocked(peerAddress domain.PeerAddress, observedIP domain.PeerIP) {
	if peerAddress == "" || observedIP == "" {
		return
	}
	history := s.observedIPHistoryByPeer[peerAddress]
	// De-duplicate adjacent entries: repeating the same observation is
	// noise, and a ping-pong detector wants distinct values.
	if n := len(history); n > 0 && history[n-1] == observedIP {
		return
	}
	history = append(history, observedIP)
	if len(history) > observedIPHistoryMaxSize {
		history = history[len(history)-observedIPHistoryMaxSize:]
	}
	s.observedIPHistoryByPeer[peerAddress] = history
}

// observedIPHistoryForPeer returns a defensive copy of the runtime
// observed IP history for a peer. Intended for tests and diagnostics.
func (s *Service) observedIPHistoryForPeer(peerAddress domain.PeerAddress) []domain.PeerIP {
	s.mu.RLock()
	defer s.mu.RUnlock()
	history := s.observedIPHistoryByPeer[peerAddress]
	if len(history) == 0 {
		return nil
	}
	out := make([]domain.PeerIP, len(history))
	copy(out, history)
	return out
}

// setTrustedSelfAdvertiseIP updates the runtime override used by the
// outbound hello frame. Empty string clears the override and falls back
// to observed consensus / config.
func (s *Service) setTrustedSelfAdvertiseIP(ip domain.PeerIP) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trustedSelfAdvertiseIP = domain.PeerIP(strings.TrimSpace(string(ip)))
}

// handleConnectionNotice applies the caller-side reaction to an inbound
// connection_notice frame on an outbound session. The concrete code
// governs the side effect:
//   - ErrCodeObservedAddressMismatch: update the trusted self advertise
//     IP runtime override (so subsequent outbound hellos self-correct).
//   - ErrCodePeerBanned: record the remote ban window — scope is driven
//     by details.Reason (peer-ban → this PeerAddress only; blacklisted
//     → every sibling PeerAddress behind the same egress IP). The dial
//     gate then suppresses the appropriate addresses until the window
//     elapses, ending the cm_session_setup_failed cascade that would
//     otherwise feed the ebus storm.
//
// peerAddress identifies the dial target — required for peer-banned so
// the handler can attach the per-peer record AND extract the server IP
// for the IP-wide record when reason=blacklisted. Ignored for
// observed-address-mismatch which only updates the local self-advertise
// override. The caller already holds the address as a domain value, so
// accepting it here keeps the remote-ban path off the network's
// RemoteAddr() string.
func (s *Service) handleConnectionNotice(peerAddress domain.PeerAddress, frame protocol.Frame) {
	switch frame.Code {
	case protocol.ErrCodeObservedAddressMismatch:
		s.handleObservedAddressMismatchNotice(frame)
	case protocol.ErrCodePeerBanned:
		s.handlePeerBannedNotice(peerAddress, frame)
	}
}

// handleObservedAddressMismatchNotice consumes a legacy
// connection_notice{code=observed-address-mismatch} hint produced by v10
// peers. Under the phase 1 deprecation contract (ProtocolVersion=11) this
// notice is no longer authoritative: the v11 truth source is passive
// learning from inbound observed IPs (see selfAdvertiseEndpoint /
// observedConsensusIPLocked). The hint is only applied as a weak fallback
// when the node has NOT yet accumulated an observed-IP consensus, so a
// legacy notice can never downgrade or override a stronger v11-derived
// advertise endpoint. Parser path and session survival are unconditional
// — the notice must never break a session or persisted state, per the
// mixed-network compatibility invariant.
func (s *Service) handleObservedAddressMismatchNotice(frame protocol.Frame) {
	details, err := protocol.ParseObservedAddressMismatchDetails(frame.Details)
	if err != nil {
		return
	}
	observed := strings.TrimSpace(details.ObservedAddress)
	if observed == "" {
		return
	}
	// Guard against a peer reporting a non-routable observation — the
	// override must never downgrade us to a private range.
	if isNonRoutableIPHost(observed) {
		return
	}
	// Weak-hint gate: if v11 passive learning already produced an observed
	// consensus IP, the legacy notice is advisory only and does not touch
	// the runtime override. This keeps mixed-network nodes from having a
	// stronger learned endpoint silently replaced by a v10 peer's claim.
	s.mu.RLock()
	_, hasObservedConsensus := s.observedConsensusIPLocked()
	s.mu.RUnlock()
	if hasObservedConsensus {
		return
	}
	s.setTrustedSelfAdvertiseIP(domain.PeerIP(canonicalIPFromHost(observed)))
}

// handlePeerBannedNotice records the remote ban window communicated by
// a connection_notice{code=peer-banned} frame. Scope is strictly driven
// by details.Reason, matching the wire contract in
// docs/protocol/errors.md: every notice lands in exactly one of two
// tables, never both. Keeping the scope single-source means the dial
// gate's read-side helpers stay authoritative and the recovery path on
// successful handshake has nothing to keep in sync across tables.
//
//   - reason=peer-ban     → per-peer record on peerEntry.RemoteBannedUntil.
//     The dial gate suppresses this PeerAddress only. Applies only to
//     peers we already know about; unknown addresses are ignored so a
//     hostile peer cannot grow our state by inventing names.
//
//   - reason=blacklisted  → IP-wide record on Service.remoteBannedIPs,
//     keyed on the canonical host part of peerAddress. The dial gate
//     suppresses every sibling PeerAddress behind that egress IP — NAT
//     gateway, VPN exit, Tor exit, multi-homed host — because the
//     responder's ban applies to the whole IP, not just the single
//     peer that happened to carry the notice. Recorded regardless of
//     whether the peer is known so a later discovery of a sibling peer
//     on the same IP still hits the gate. No per-peer row is written —
//     isPeerRemoteBannedLocked already consults the IP-wide table on
//     read, so the sender is suppressed through the IP-wide gate
//     without duplicating state into the per-peer map.
//
// Degenerate address form (peerAddress without a host:port split) is
// the one exception: reason=blacklisted falls back to the per-peer
// record because there is no IP to key on. The window comes from the
// responder verbatim (no local cap) — a peer that has decided to
// refuse us is authoritative about how long. A parse failure on
// Details is logged but does not record a ban, so a hostile or buggy
// responder cannot pin the dialler with a malformed payload.
// peerAddress empty is a programmer error from the caller (no address
// in scope to attach the ban to); the helper logs and returns rather
// than silently fabricating an entry.
func (s *Service) handlePeerBannedNotice(peerAddress domain.PeerAddress, frame protocol.Frame) {
	if strings.TrimSpace(string(peerAddress)) == "" {
		log.Debug().Msg("peer_banned_notice_missing_address")
		return
	}
	details, until, err := protocol.ParsePeerBannedDetails(frame.Details)
	if err != nil {
		log.Debug().Err(err).Str("peer", string(peerAddress)).Msg("peer_banned_notice_parse_failed")
		return
	}
	reason := details.Reason
	now := time.Now().UTC()

	ip, _, hasIP := splitHostPort(string(peerAddress))
	if hasIP {
		if canon := canonicalIPFromHost(ip); canon != "" {
			ip = canon
		}
	}

	recorded := false
	s.mu.Lock()
	switch {
	case reason == protocol.PeerBannedReasonBlacklisted && hasIP:
		if !s.recordRemoteIPBanLocked(ip, reason, until, now).IsZero() {
			recorded = true
		}
	case reason == protocol.PeerBannedReasonBlacklisted && !hasIP:
		// No host:port to hash on — we cannot express an IP-wide gate.
		// Fall back to the per-peer record so the sender at least is
		// suppressed. Logged so the degenerate path is observable.
		log.Debug().
			Str("peer", string(peerAddress)).
			Str("reason", string(reason)).
			Msg("peer_banned_notice_blacklisted_no_ip_fallback_per_peer")
		if !s.recordRemoteBanLocked(peerAddress, reason, until, now).IsZero() {
			recorded = true
		}
	default:
		// reason=peer-ban (and any forward-compatible reason that has
		// per-peer blast radius): write only the per-peer row.
		if !s.recordRemoteBanLocked(peerAddress, reason, until, now).IsZero() {
			recorded = true
		}
	}
	s.mu.Unlock()

	if !recorded {
		// Nothing recorded — reason=peer-ban on an unknown peer, or
		// reason=blacklisted with no IP to key on. recordRemoteBanLocked
		// already logged the unknown-peer case.
		return
	}
	// Flush immediately so the remote-ban gate survives a crash and the
	// dialler keeps skipping this peer instead of resurrecting the storm
	// that the notice was supposed to end. Matches the direct-flush
	// pattern used by manual peer registration.
	s.flushPeerState()
}

// selfAdvertiseEndpoint returns the host:port we should publish in an
// outbound hello.listen under the phase 1 deprecation contract
// (ProtocolVersion=11).
//
// Host selection priority:
//   1. learned observed IP (passive-learning consensus from inbound path);
//   2. runtime override set from legacy observed-address-mismatch notice
//      (deprecated compat hint, not an authoritative source);
//   3. host component of the deprecated CORSA_ADVERTISE_ADDRESS fallback.
//
// Port selection is a single rule: config.Node.EffectiveAdvertisePort(),
// which resolves CORSA_ADVERTISE_PORT or falls back to DefaultPeerPort.
// The real local listener port is NOT reused as the advertised port —
// operators commonly run behind a NAT/port-forward where the advertised
// port differs from the bind port, and the v11 contract makes the
// operator-declared advertise_port the single source of truth.
func (s *Service) selfAdvertiseEndpoint() string {
	if !s.cfg.EffectiveListenerEnabled() {
		return ""
	}
	s.mu.RLock()
	override := s.trustedSelfAdvertiseIP
	observedIP, _ := s.observedConsensusIPLocked()
	s.mu.RUnlock()

	// advertisePort is carried as a domain.PeerPort through the selection
	// logic; the textual form is derived once here at the JoinHostPort
	// boundary so the runtime path does not fan out stringly-typed copies.
	advertisePort := s.cfg.EffectiveAdvertisePort()
	advertisePortStr := strconv.Itoa(int(advertisePort))

	// Learned observed IP wins over the deprecated legacy override and the
	// deprecated CORSA_ADVERTISE_ADDRESS host. This is the v11 contract:
	// passive learning from inbound observed IP is the primary truth source.
	if observedIP != "" {
		return joinHostPort(string(observedIP), advertisePortStr)
	}
	// Override comes from handleObservedAddressMismatchNotice, which in v11
	// is weakened to a compat-only hint (never authoritative). Still better
	// than nothing when the node has not yet seen a v11 inbound.
	if override != "" {
		return joinHostPort(string(override), advertisePortStr)
	}
	// Deprecated fallback: host from CORSA_ADVERTISE_ADDRESS. Always paired
	// with EffectiveAdvertisePort, never with the stale port baked into the
	// config value.
	if cfgHost, _, ok := splitHostPort(s.cfg.AdvertiseAddress); ok && cfgHost != "" {
		return joinHostPort(cfgHost, advertisePortStr)
	}
	return s.cfg.AdvertiseAddress
}

// observedConsensusIPLocked returns the observed IP that crossed the
// consensus threshold, or ("", false) when there is no consensus yet.
// Must be called with s.mu held.
func (s *Service) observedConsensusIPLocked() (domain.PeerIP, bool) {
	if len(s.observedAddrs) < observedAddrConsensusThreshold {
		return "", false
	}
	votes := make(map[domain.PeerIP]int, len(s.observedAddrs))
	for _, obs := range s.observedAddrs {
		votes[domain.PeerIP(obs)]++
	}
	best, bestCount := domain.PeerIP(""), 0
	for ip, cnt := range votes {
		if cnt > bestCount {
			best, bestCount = ip, cnt
		}
	}
	if bestCount < observedAddrConsensusThreshold {
		return "", false
	}
	return best, true
}

// repayMisadvertisePenaltyOnAuthLocked decreases the forgivable
// misadvertise bucket for a peer after a successful authentication and
// mirrors that refund onto the per-IP ban score accumulated by
// addBanScore. Returns the number of points repaid in this call (may be
// zero). Must be called with s.mu held.
//
// banIP must be the verified TCP peer IP extracted from the live
// connection's RemoteAddr (the same source addBanScore keys under) —
// NOT derived from peerAddress. For DNS / manually-added bootstrap
// peers the host component of peerAddress is an unresolved hostname,
// while s.bans is always keyed by the canonical IP, so splitting
// peerAddress here would leave the IP-level ban score permanently
// un-refunded even though the peer-level bucket decayed. Pass empty
// banIP to skip the mirror (e.g. when no live conn is available);
// the peer-level bucket still moves.
//
// Invariant: the repay never removes more than was originally charged
// for misadvertise; ForgivableMisadvertisePoints tracks the remaining
// balance and MisadvertisePointsRepaid records the cumulative refund.
// Both the peer-level bookkeeping and the transport-level ban table
// must move together — otherwise repeated mismatches on an honest but
// flaky peer would permanently push s.bans[ip].Score toward banThreshold
// even though the peer keeps successfully authenticating afterwards.
func (s *Service) repayMisadvertisePenaltyOnAuthLocked(peerAddress domain.PeerAddress, banIP domain.PeerIP) int {
	if peerAddress == "" {
		return 0
	}
	pm := s.persistedMeta[peerAddress]
	if pm == nil {
		return 0
	}
	// Cap repay at banIncrementAdvertiseMismatch per successful auth.
	repay := banIncrementAdvertiseMismatch
	if repay > pm.ForgivableMisadvertisePoints {
		repay = pm.ForgivableMisadvertisePoints
	}
	if repay <= 0 {
		return 0
	}
	pm.ForgivableMisadvertisePoints -= repay
	pm.MisadvertisePointsRepaid += repay

	// Mirror the refund onto the IP-level ban table under the exact key
	// addBanScore used (the raw host from conn.RemoteAddr()). If the IP
	// is unknown (empty banIP, nil bans map, peer was never world-
	// mismatched), skip the mirror silently — the peer-level bookkeeping
	// is still correct and the next successful auth will try again.
	if s.bans == nil || banIP == "" {
		return repay
	}
	key := string(banIP)
	entry, exists := s.bans[key]
	if !exists || entry.Score <= 0 {
		return repay
	}
	if repay >= entry.Score {
		entry.Score = 0
	} else {
		entry.Score -= repay
	}
	// A previously-triggered blacklist must not be lifted by the repay
	// alone — Blacklisted is a wall-clock deadline and expires by itself.
	// We only adjust Score here so subsequent mismatches need to re-cross
	// the threshold from a lower baseline.
	s.bans[key] = entry
	return repay
}

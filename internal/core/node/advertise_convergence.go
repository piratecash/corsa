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
	"net"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// advertiseDecision is the machine-readable classification produced by
// validateAdvertisedAddress. Callers switch on this value rather than
// re-running the same string/IP checks inline.
type advertiseDecision string

const (
	advertiseDecisionNonListener    advertiseDecision = "non_listener"
	advertiseDecisionLegacyDirect   advertiseDecision = "legacy_direct"
	advertiseDecisionMatch          advertiseDecision = "match"
	advertiseDecisionLocalException advertiseDecision = "local_exception"
	advertiseDecisionWorldMismatch  advertiseDecision = "world_mismatch"
	advertiseDecisionInvalid        advertiseDecision = "invalid"
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
type advertiseValidationResult struct {
	Decision               advertiseDecision
	NormalizedObservedIP   domain.PeerIP
	NormalizedAdvertisedIP domain.PeerIP
	ObservedIPHint         domain.PeerIP
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
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
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

// validateAdvertisedAddress is the pure decision helper invoked from
// dispatchNetworkFrame's case "hello". It does not touch shared state
// and does not write to the network — the caller is responsible for
// applying the persistence write and sending RejectNotice.
//
// Ordering matches the inbound decision matrix:
//  1. listener="0"            → non_listener
//  2. no usable listen        → legacy_direct
//  3. observed == advertised  → match
//  4. observed local/private  → local_exception
//  5. observed world,
//     advertised != observed   → world_mismatch
//  6. invalid/malformed       → invalid
//
// observedTCPAddr is the RemoteAddr() string of the inbound conn
// (host:port). frame is the parsed hello frame.
func validateAdvertisedAddress(observedTCPAddr string, frame protocol.Frame) advertiseValidationResult {
	result := advertiseValidationResult{
		PersistWriteMode: persistWriteModeSkip,
	}

	observedHost := remoteIPFromString(observedTCPAddr)
	result.NormalizedObservedIP = domain.PeerIP(canonicalIPFromHost(observedHost))

	// Branch 1: peer explicitly declared itself non-listener.
	if strings.TrimSpace(frame.Listener) == "0" {
		result.Decision = advertiseDecisionNonListener
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.AllowAnnounce = false
		return result
	}

	// Branch 2: legacy peer without usable listen. Listener absent OR
	// listener="1" but listen field unusable both fall here — a peer
	// with listener="1" and usable listen but no explicit listener
	// string will be handled by the match / local_exception / mismatch
	// branches below.
	host, port, ok := usableListen(frame.Listen)
	if !ok {
		result.Decision = advertiseDecisionLegacyDirect
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.AllowAnnounce = false
		return result
	}
	_ = port // port is not consumed for the inbound decision; kept for clarity.

	// For the remaining branches we only act on IPv4 / IPv6 advertises.
	// .onion and other non-IP address families are explicitly out of
	// scope for the observed/advertised compare — fall through as
	// legacy direct so the non-IP branch below accepts them.
	advertisedIP := net.ParseIP(host)
	observedIP := net.ParseIP(observedHost)
	result.NormalizedAdvertisedIP = domain.PeerIP(canonicalIPFromHost(host))

	if advertisedIP == nil {
		// Non-IP advertise (e.g. .onion handled outside this RFC):
		// treat as legacy-direct so we accept but don't announce via
		// this path.
		result.Decision = advertiseDecisionLegacyDirect
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.AllowAnnounce = false
		return result
	}

	if observedIP == nil {
		// Observed side is not a parseable IP — treat the advertise
		// as invalid/malformed: we cannot compare, so closing as
		// protocol error (no connection_notice payload).
		result.Decision = advertiseDecisionInvalid
		result.ShouldReject = true
		result.PersistAnnounceState = announceStateUnset
		result.PersistWriteMode = persistWriteModeSkip
		return result
	}

	observedNonRoutable := isNonRoutableIP(observedIP)
	advertisedNonRoutable := isNonRoutableIP(advertisedIP)

	// Branch 3: match after canonical normalisation.
	if result.NormalizedObservedIP != "" && result.NormalizedObservedIP == result.NormalizedAdvertisedIP {
		result.Decision = advertiseDecisionMatch
		result.PersistAnnounceState = announceStateAnnounceable
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.AllowAnnounce = true
		return result
	}

	// Branch 4: observed is local/private, advertised is world-reachable.
	// Accept silently as direct-only knowledge with optimistic announce
	// allowed at <advertised>:DefaultPeerPort.
	if observedNonRoutable && !advertisedNonRoutable {
		result.Decision = advertiseDecisionLocalException
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.ObservedIPHint = result.NormalizedObservedIP
		// Announce is allowed via optimistic <advertised>:64646 form, but
		// this flag is authoritative only for the announce path — the
		// inbound source port is still never reused.
		result.AllowAnnounce = true
		return result
	}

	// Branches 5 and 6 both close the connection. When observed is
	// world-reachable but disagrees with the advertise, send machine-
	// readable notice; when the advertise itself is non-routable while
	// observed is world-reachable, behave identically (still mismatch).
	if !observedNonRoutable && (advertisedNonRoutable || result.NormalizedObservedIP != result.NormalizedAdvertisedIP) {
		result.Decision = advertiseDecisionWorldMismatch
		result.ShouldReject = true
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeUpdateExisting
		result.ObservedIPHint = result.NormalizedObservedIP
		result.RejectNotice = buildObservedMismatchNotice(result.NormalizedObservedIP)
		return result
	}

	// Anything that slips through — for example observed non-routable
	// AND advertised non-routable but unequal — is treated as a quiet
	// direct-only accept so loopback/test setups still work. We do NOT
	// learn announceable trust from this branch.
	result.Decision = advertiseDecisionLocalException
	result.PersistAnnounceState = announceStateDirectOnly
	result.PersistWriteMode = persistWriteModeCreateOrUpdate
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
		pm.TrustedAdvertiseIP = result.NormalizedAdvertisedIP
		pm.TrustedAdvertiseSource = trustedAdvertiseSourceInbound
		pm.TrustedAdvertisePort = config.DefaultPeerPort
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
// for the session — never 64646 as a default.
//
// Contract: if dialedIP is not a parseable IP, the call is a no-op.
// TrustedAdvertiseIP is consumed as a canonical IP by the observed-IP
// downgrade sweep (downgradeAnnounceableByObservedIPLocked compares it
// against observed IPs extracted from real TCP RemoteAddrs). Storing a
// hostname here would permanently break that compare: a later
// world_mismatch targeting the same peer could not match its
// hostname-valued TrustedAdvertiseIP and the announceable row would
// survive the downgrade.
func (s *Service) recordOutboundConfirmed(peerAddress domain.PeerAddress, dialedIP domain.PeerIP, dialedPort string) {
	if peerAddress == "" || dialedIP == "" || dialedPort == "" {
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
	pm.TrustedAdvertisePort = dialedPort
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
//
// The returned error is the sentinel for the notice code and is intended
// to bubble up to the caller that owns the session lifecycle.
func (s *Service) handleConnectionNotice(frame protocol.Frame) {
	if frame.Code != protocol.ErrCodeObservedAddressMismatch {
		return
	}
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
	if ip := net.ParseIP(observed); ip == nil || isNonRoutableIP(ip) {
		return
	}
	s.setTrustedSelfAdvertiseIP(domain.PeerIP(canonicalIPFromHost(observed)))
}

// selfAdvertiseEndpoint returns the host:port we should publish in an
// outbound hello frame. Priority: runtime override → observed
// consensus IP → configured advertise address. The port always comes
// from the real local listener so convergence never swaps our port to
// an unproven one.
func (s *Service) selfAdvertiseEndpoint() string {
	if !s.cfg.EffectiveListenerEnabled() {
		return ""
	}
	s.mu.RLock()
	override := s.trustedSelfAdvertiseIP
	observedIP, _ := s.observedConsensusIPLocked()
	s.mu.RUnlock()

	// Port preference: real local listener over config.AdvertiseAddress.
	listenerPort := s.localListenerPort()
	if listenerPort == "" {
		// Fallback: AdvertiseAddress port if listener port is unknown
		// (unit tests without a real net.Listener).
		if _, port, ok := splitHostPort(s.cfg.AdvertiseAddress); ok {
			listenerPort = port
		}
	}
	if listenerPort == "" {
		listenerPort = config.DefaultPeerPort
	}

	if override != "" {
		return net.JoinHostPort(string(override), listenerPort)
	}
	if observedIP != "" {
		return net.JoinHostPort(string(observedIP), listenerPort)
	}
	// Config fallback: use the host from cfg.AdvertiseAddress but always
	// pair it with listenerPort. The configured port may have drifted
	// from the real listener (operator edited config, listener picked a
	// different port on startup), and the contract is that hello.listen
	// must always advertise the real local listener port so the peer
	// dial target is dialable. Using the raw cfg.AdvertiseAddress here
	// would silently re-introduce a stale port for a correct host.
	if cfgHost, _, ok := splitHostPort(s.cfg.AdvertiseAddress); ok && cfgHost != "" {
		return net.JoinHostPort(cfgHost, listenerPort)
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

// localListenerPort returns the port from the real net.Listener, if any.
// Empty string when the listener is not yet bound.
//
// We intentionally parse listener.Addr().String() via splitHostPort
// instead of type-asserting to *net.TCPAddr: the listener type is opaque
// to this module (wrapped listeners, tls.Listener, test fakes), and the
// string form is already the canonical "host:port" everywhere else in
// this package.
func (s *Service) localListenerPort() string {
	s.mu.RLock()
	listener := s.listener
	s.mu.RUnlock()
	if listener == nil {
		return ""
	}
	addr := listener.Addr()
	if addr == nil {
		return ""
	}
	_, port, ok := splitHostPort(addr.String())
	if !ok {
		return ""
	}
	if port == "" || port == "0" {
		return ""
	}
	return port
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

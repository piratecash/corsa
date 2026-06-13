// Advertise-address convergence helpers.
//
// This file owns the decision logic that classifies an inbound hello
// frame into accept-only outcomes (non_listener / legacy_direct /
// match / local_exception) plus the Service-owned persistence writers
// invoked from dispatchNetworkFrame / peerSessionRequest. It keeps the
// decision tree out of dispatchNetworkFrame so the inbound hello path
// stays readable and so the matrix from the advertise convergence
// contract lives in one place. The legacy v10/v11 reject-and-correct
// path (observed-address-mismatch wire, trustedSelfAdvertiseIP
// override, misadvertise repay/forgiveness bookkeeping) was removed in
// the v12 cleanup phase and is no longer produced or consumed here.
//
// See docs/protocol/handshake.md section "Advertise convergence" for
// the wire/behavioural contract this file implements.

package node

import (
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
// v12 cleanup note: the legacy world_mismatch / invalid branches were
// removed together with the observed-address-mismatch wire path. The
// remaining decisions describe accept-only outcomes; validateAdvertisedAddress
// no longer rejects on advertise-learning grounds.
type advertiseDecision string

const (
	advertiseDecisionNonListener    advertiseDecision = "non_listener"
	advertiseDecisionLegacyDirect   advertiseDecision = "legacy_direct"
	advertiseDecisionMatch          advertiseDecision = "match"
	advertiseDecisionLocalException advertiseDecision = "local_exception"
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
// for applying the persistence write through the dedicated Service
// helpers.
//
// AdvertisePort is the resolved self-reported listening port for this
// inbound frame: the only authoritative port source for passive learning.
// Value is frame.AdvertisePort when PeerPort.IsValid, else the PeerPort
// form of config.DefaultPeerPort. The inbound TCP source port is NEVER
// used as a listening port. Textual conversion happens only at persisted /
// JoinHostPort boundaries; the runtime result carries the domain value.
//
// v12 cleanup note: there is no reject path on advertise-learning grounds.
// All decisions are accept-only outcomes; the legacy ShouldReject /
// RejectNotice fields were removed together with the observed-address-mismatch
// wire code.
type advertiseValidationResult struct {
	Decision               advertiseDecision
	NormalizedObservedIP   domain.PeerIP
	NormalizedAdvertisedIP domain.PeerIP
	ObservedIPHint         domain.PeerIP
	AdvertisePort          domain.PeerPort  // resolved listening port; DefaultPeerPort on absent/invalid wire value
	PersistAnnounceState   announceState    // announceStateDirectOnly | announceStateAnnounceable | announceStateUnset
	PersistWriteMode       persistWriteMode // persistWriteMode* constants
	AllowAnnounce          bool
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

// extractAdvertisePort resolves the listening port the peer is announcing
// in an inbound hello frame. The only authoritative port source is
// frame.AdvertisePort: accepted when PeerPort.IsValid (inclusive 1..65535),
// collapsed to the PeerPort form of config.DefaultPeerPort on any other
// value (zero, missing, negative, out-of-range, or — at the decoder level
// — a non-integer wire payload which already arrives as zero after
// json.Unmarshal into PeerPort). The inbound TCP source port is
// intentionally never considered here; reusing it as a listening port is
// exactly the contract violation the v11 rollout eliminated.
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
// applying the persistence write. Contract: observed TCP IP is the sole
// authoritative source of the peer's IP, hello.listen.host is ignored
// as a truth input, and the штатный runtime path NEVER rejects on
// advertise-learning grounds.
//
// Ordering of the v12 decision matrix:
//  1. listener="0"                          → non_listener (accept, direct_only)
//  2. listener != "1"                        → legacy_direct (accept, direct_only).
//     The listen field is NOT consulted here — under the v12 wire contract
//     the listener flag is the single source of truth for the "is this peer
//     reachable?" gate. A frame with listener="" / listener missing /
//     listener=anything-else is direct_only regardless of what host:port
//     the legacy listen string carries.
//  3. observed IP not parseable as IPv4/IPv6 → local_exception (accept, direct_only, no candidate)
//  4. observed IP non-routable / local       → local_exception (accept, direct_only, no candidate)
//  5. observed IP world-routable             → match (accept, announceable,
//     candidate = observed_IP:advertise_port)
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

	// Branch 2: any frame whose Listener flag is not exactly "1" is
	// classified as a non-listener under the v12 wire contract. The
	// listener flag is the single source of truth for the "is this peer
	// reachable?" gate; hello.Listen is no longer consulted as a
	// fallback signal even when a legacy frame still echoes a usable
	// host:port. Without an explicit listener="1" we cannot claim the
	// peer accepts inbound connections, so we fall back to direct_only
	// accept regardless of what listen carries.
	if strings.TrimSpace(frame.Listener) != "1" {
		result.Decision = advertiseDecisionLegacyDirect
		result.PersistAnnounceState = announceStateDirectOnly
		result.PersistWriteMode = persistWriteModeCreateOrUpdate
		result.AllowAnnounce = false
		return result
	}

	// hello.listen.host is intentionally NOT consulted as a truth input.
	// Under the v12 wire contract emitters do not produce a host in
	// Listen at all, but a legacy frame that still echoes one is parsed
	// only as a diagnostic — NormalizedAdvertisedIP still surfaces in
	// logs / tests, but no branch below decides on it.
	if host, _, listenOK := usableListen(frame.Listen); listenOK && isIPHost(host) {
		result.NormalizedAdvertisedIP = domain.PeerIP(canonicalIPFromHost(host))
	}

	// Branch 3: observed side is not a parseable IP (e.g. non-IP
	// transport, or malformed RemoteAddr from a buggy transport wrapper).
	// We cannot learn a candidate from it, but the contract says we still
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
	// candidate source under the v12 baseline — hello.listen.host
	// disagreement is not a reject trigger and not a mismatch event.
	// The candidate endpoint is observed_IP:advertise_port (advertise_port
	// already collapsed to DefaultPeerPort on absent/invalid wire value).
	// The persisted TrustedAdvertiseIP follows the same rule: it mirrors
	// observed IP, NOT the advertised listen host.
	result.Decision = advertiseDecisionMatch
	result.NormalizedAdvertisedIP = result.NormalizedObservedIP
	result.ObservedIPHint = result.NormalizedObservedIP
	result.PersistAnnounceState = announceStateAnnounceable
	result.PersistWriteMode = persistWriteModeCreateOrUpdate
	result.AllowAnnounce = true
	return result
}

// applyAdvertiseValidationResult is the single Service-owned writer that
// projects an advertiseValidationResult onto persistedMeta and the
// runtime observed-IP history. All writes happen under s.peerMu.
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
	// Skip-mode decisions must not touch persistence at all — the inbound
	// was either filtered out by the caller or has no row to project.
	if result.PersistWriteMode == persistWriteModeSkip {
		return
	}

	log.Trace().Str("site", "applyAdvertiseValidationResult").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "applyAdvertiseValidationResult").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "applyAdvertiseValidationResult").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	}()
	s.applyAdvertiseValidationResultLocked(peerAddress, result)
}

// applyAdvertiseValidationResultLocked is the same writer without the
// lock acquisition. Used by callers that already hold s.peerMu write
// lock (notably the outbound convergence path). Mutates peer-domain
// state (persistedMeta) under s.peerMu and, when an observed-IP hint is
// present, nests ipStateMu inside s.peerMu to update
// observedIPHistoryByPeer — matches the canonical peerMu → ipStateMu
// order.
func (s *Service) applyAdvertiseValidationResultLocked(peerAddress domain.PeerAddress, result advertiseValidationResult) {
	pm := s.persistedMeta[peerAddress]
	if pm == nil {
		if result.PersistWriteMode == persistWriteModeUpdateExisting {
			// Downgrade-only branch: do not create a new row just
			// because we saw a misbehaving peer for the first time.
			if result.ObservedIPHint != "" {
				// Keep a transient observation hint in runtime memory
				// so the outbound convergence loop can still use it.
				// observedIPHistoryByPeer is ipState-domain; nest
				// ipStateMu inside the already-held s.peerMu per the
				// canonical peerMu → ipStateMu order.
				s.ipStateMu.Lock()
				s.recordObservedIPHintLocked(peerAddress, result.ObservedIPHint)
				s.ipStateMu.Unlock()
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

	// Sticky-state guard: a previously announceable peer must not be
	// auto-downgraded just because the new decision happens to be a
	// direct_only outcome under a create_or_update write that is not an
	// explicit downgrade trigger.
	previouslyAnnounceable := pm.AnnounceState == announceStateAnnounceable

	switch result.Decision {
	case advertiseDecisionMatch:
		pm.AnnounceState = announceStateAnnounceable
		// NormalizedAdvertisedIP mirrors the observed IP (see
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
		// Always project as direct_only; if the row was previously
		// announceable, this is the explicit downgrade trigger that
		// flips it. The two arms collapse into one assignment because
		// the post-state matches in both cases.
		pm.AnnounceState = announceStateDirectOnly
	}

	if result.ObservedIPHint != "" {
		now := time.Now().UTC()
		pm.LastObservedIP = result.ObservedIPHint
		pm.LastObservedAt = &now
		// observedIPHistoryByPeer is ipState-domain; nest ipStateMu
		// inside the already-held s.peerMu per the canonical peerMu →
		// ipStateMu order.
		s.ipStateMu.Lock()
		s.recordObservedIPHintLocked(peerAddress, result.ObservedIPHint)
		s.ipStateMu.Unlock()
	}
}

// applyAdvertiseOnInboundAccept is the single call site both inbound
// accept branches (with and without identity fields) use to project the
// advertise decision onto persistedMeta. Keeps the dispatchNetworkFrame
// case "hello" body small and centralises the peerAddress derivation.
//
// tcpAddr is the verified RemoteAddr() of the inbound conn. result is
// the outcome of validateAdvertisedAddress and carries the resolved
// AdvertisePort. It is a no-op when the decision skips persistence.
//
// The peer address is derived strictly from the verified TCP IP plus
// the self-reported advertise_port (already collapsed to the validated
// PeerPort form by extractAdvertisePort). hello.Listen is intentionally
// not consulted: under the v12 wire contract it does not carry an
// authoritative host or port any more, so synthesising the address
// from inbound TCP + AdvertisePort keeps the projection consistent
// regardless of whether the legacy host:port form is still on the
// wire (e.g. tests wired by hand) or has been emptied (production
// emit path).
func (s *Service) applyAdvertiseOnInboundAccept(tcpAddr string, result advertiseValidationResult) {
	if result.PersistWriteMode == persistWriteModeSkip {
		return
	}
	peerAddr := peerAddressFromInbound(tcpAddr, result.AdvertisePort)
	if peerAddr == "" {
		return
	}
	s.applyAdvertiseValidationResult(domain.PeerAddress(peerAddr), result)
}

// peerAddressFromInbound builds the canonical peer address (host:port)
// for an inbound peer using ONLY the verified TCP IP and the validated
// advertise_port. When the TCP IP is not parseable as an IP literal
// (non-IP transport, malformed RemoteAddr that even
// remoteIPFromString's host:port split could not normalise into an
// IP) the helper returns an empty string and the caller skips
// persistence — there is no point keying state on an unverifiable
// host like "not-a-host". The host is taken from tcpAddr exactly as
// remoteIPFromString surfaces it (no canonicalisation here; that is
// the persistence writer's concern). The port is the resolved
// PeerPort form, falling back to config.DefaultPeerPort for the
// invalid sentinel — matching extractAdvertisePort's contract.
func peerAddressFromInbound(tcpAddr string, advertisePort domain.PeerPort) string {
	host := remoteIPFromString(tcpAddr)
	if host == "" || !isIPHost(host) {
		return ""
	}
	port := config.DefaultPeerPort
	if advertisePort.IsValid() {
		port = strconv.Itoa(int(advertisePort))
	}
	return joinHostPort(host, port)
}

// recordOutboundConfirmed is the outbound success writer invoked by
// peerSessionRequest (peer_management.go) after a completed
// hello → welcome → auth_ok handshake. dialedIP MUST be a canonical IP
// — the caller is expected to derive it from the live TCP
// connection's RemoteAddr, not from peerAddress (which may be a
// hostname for DNS/manual peers). dialedPort is the real port used
// for the session — never 64646 as a default — expressed as a
// domain.PeerPort so the compiler catches any silent mix-up with
// generic ints the caller might have lying around.
//
// Contract: if dialedIP is not a parseable IP, or dialedPort is not a
// valid PeerPort (1..65535), the call is a no-op. TrustedAdvertiseIP is
// consumed as a canonical IP by downstream callers that compare it
// against observed IPs extracted from real TCP RemoteAddrs. Storing a
// hostname here would permanently break that compare. Persisted schema
// keeps the port as string; the domain→string conversion happens here
// at the single writer boundary.
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
	log.Trace().Str("site", "recordOutboundConfirmed").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "recordOutboundConfirmed").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "recordOutboundConfirmed").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	}()
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
// observedIPHistoryMaxSize.
//
// observedIPHistoryByPeer is ipState-domain state. Caller MUST hold
// ipStateMu write lock. Cross-domain callers (peer-domain writers that
// also record a hint, e.g. applyAdvertiseValidationResultLocked) acquire
// ipStateMu nested inside s.peerMu per the canonical peerMu → ipStateMu
// lock order.
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
//
// Pure ipState-domain read: acquires ipStateMu read lock. Callers MUST
// NOT hold ipStateMu already (this helper takes it); cross-domain
// callers that already hold s.peerMu may call this safely because the
// canonical lock order is peerMu → ipStateMu (outer → inner).
func (s *Service) observedIPHistoryForPeer(peerAddress domain.PeerAddress) []domain.PeerIP {
	log.Trace().Str("site", "observedIPHistoryForPeer").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("ip_state_mu_reader")
	s.ipStateMu.RLock()
	log.Trace().Str("site", "observedIPHistoryForPeer").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("ip_state_mu_reader")
	defer func() {
		s.ipStateMu.RUnlock()
		log.Trace().Str("site", "observedIPHistoryForPeer").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("ip_state_mu_reader")
	}()
	history := s.observedIPHistoryByPeer[peerAddress]
	if len(history) == 0 {
		return nil
	}
	out := make([]domain.PeerIP, len(history))
	copy(out, history)
	return out
}

// handleConnectionNotice applies the caller-side reaction to an inbound
// connection_notice frame on an outbound session. The concrete code
// governs the side effect:
//   - ErrCodePeerBanned: record the remote ban window — scope is driven
//     by details.Reason (peer-ban → this PeerAddress only; blacklisted
//     → this PeerAddress per-peer PLUS an IP-wide escalation counter
//     that only promotes to a sibling-wide ban after enough distinct
//     live offenders behind the IP — see handlePeerBannedNotice). The
//     dial gate then suppresses the appropriate addresses until the
//     window elapses, ending the cm_session_setup_failed cascade that
//     would otherwise feed the ebus storm.
//
// peerAddress identifies the dial target — required for peer-banned so
// the handler can attach the per-peer record AND extract the server IP
// for the IP-wide record when reason=blacklisted. The caller already
// holds the address as a domain value, so accepting it here keeps the
// remote-ban path off the network's RemoteAddr() string.
func (s *Service) handleConnectionNotice(peerAddress domain.PeerAddress, frame protocol.Frame) {
	switch frame.Code {
	case protocol.ErrCodePeerBanned:
		s.handlePeerBannedNotice(peerAddress, frame)
	}
}

// handlePeerBannedNotice records the remote ban window communicated by
// a connection_notice{code=peer-banned} frame. Scope is driven by
// details.Reason, matching the wire contract in docs/protocol/errors.md:
// peer-ban / self-identity write a per-peer record; blacklisted writes a
// per-peer record for the offender AND feeds the IP-wide escalation
// counter, promoting to an IP-wide record only after enough distinct
// live offenders behind the egress accumulate. Only the per-peer and
// IP-wide records persist (the offender counter is volatile), so the
// recovery path on successful handshake has at most those two scopes to
// reconcile.
//
//   - reason=peer-ban     → per-peer record on peerEntry.RemoteBannedUntil.
//     The dial gate suppresses this PeerAddress only. Applies only to
//     peers we already know about; unknown addresses are ignored so a
//     hostile peer cannot grow our state by inventing names.
//
//   - reason=blacklisted  → per-peer record on the offender PLUS an
//     escalation counter toward an IP-wide record on
//     Service.remoteBannedIPs (keyed on the canonical host of
//     peerAddress). A SINGLE blacklisted notice suppresses only the
//     offending peer (per-peer row), exactly like reason=peer-ban —
//     it is not enough evidence to undial every innocent sibling
//     sharing the egress (NAT / VPN / Tor exit / multi-homed host).
//     Only once ipWideRemoteBanMinOffenders DISTINCT peers behind the
//     same IP have independently sent blacklisted does the IP-wide gate
//     engage, suppressing every sibling — including peers discovered
//     later via peer exchange. This trades the old "first notice = whole
//     IP" blast radius for a "the whole egress is hostile" signal; see
//     noteRemoteIPOffenderLocked.
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

	// Key the IP-wide scope the same way recovery clears it
	// (remoteIPBanKey): IP-literal hosts collapse to canonical form,
	// hostnames are kept verbatim. hasIP is "address has host:port", not
	// "host is a numeric IP" — a hostname still selects the IP-wide
	// escalation branch, keyed on the hostname.
	ip, hasIP := remoteIPBanKey(peerAddress)

	recorded := false
	// Cross-domain: peer-domain (persistedMeta via recordRemoteBanLocked)
	// under s.peerMu, ipState-domain (remoteBannedIPs via
	// recordRemoteIPBanLocked) under s.ipStateMu. Canonical lock order
	// peerMu → ipStateMu. Each branch acquires only the lock(s) it
	// needs, keeping the write windows narrow.
	log.Trace().Str("site", "handlePeerBannedNotice").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "handlePeerBannedNotice").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	// Resolve a generated fallback dial address to its canonical primary
	// (the address that owns the persistedMeta row). A connection_notice
	// can land on a fallback variant during the handshake; recording the
	// per-peer ban AND counting the offender against the canonical address
	// keeps the ban window from being lost (recordRemoteBanLocked ignores
	// unknown addresses) and stops multiple dial variants of ONE peer from
	// inflating the distinct-offender count toward IP-wide escalation. For
	// a known primary or an address with no alias, resolveHealthAddress is
	// the identity. IP extraction stays on the raw address (a fallback is a
	// port variant of the same host, so the IP key is unchanged).
	resolved := s.resolveHealthAddress(peerAddress)
	switch {
	case reason == protocol.PeerBannedReasonBlacklisted && hasIP:
		// Always suppress the specific offender immediately via a
		// per-peer record (known peers only — recordRemoteBanLocked
		// ignores unknown addresses to bound state growth).
		if !s.recordRemoteBanLocked(resolved, reason, until, now).IsZero() {
			recorded = true
		}
		// Escalate to the IP-WIDE gate — which ALSO suppresses innocent
		// siblings behind this egress (NAT / VPN / Tor exit) — only once
		// ipWideRemoteBanMinOffenders DISTINCT peers behind the IP have
		// banned us. A lone offender stays per-peer-only so a legitimate
		// neighbour sharing an egress with one hostile node is not
		// collaterally undiallable. Pure ipState write: nest ipStateMu
		// inside the already-held s.peerMu (canonical peerMu → ipStateMu
		// order).
		s.ipStateMu.Lock()
		// noteRemoteIPOffenderLocked returns the IP-wide horizon to record
		// (the instant the live-offender count would drop below the
		// escalation threshold), or zero when escalation is not warranted.
		// Recording THAT horizon — rather than this notice's own until —
		// keeps the IP-wide ban from outliving the "enough live offenders"
		// condition that justified it.
		if horizon := s.noteRemoteIPOffenderLocked(ip, resolved, until, now); !horizon.IsZero() {
			if !s.recordRemoteIPBanLocked(ip, reason, horizon, now).IsZero() {
				recorded = true
			}
		}
		s.ipStateMu.Unlock()
	case reason == protocol.PeerBannedReasonBlacklisted && !hasIP:
		// No host:port to hash on — we cannot express an IP-wide gate.
		// Fall back to the per-peer record so the sender at least is
		// suppressed. Logged so the degenerate path is observable.
		log.Debug().
			Str("peer", string(peerAddress)).
			Str("reason", string(reason)).
			Msg("peer_banned_notice_blacklisted_no_ip_fallback_per_peer")
		if !s.recordRemoteBanLocked(resolved, reason, until, now).IsZero() {
			recorded = true
		}
	default:
		// reason=peer-ban (and any forward-compatible reason that has
		// per-peer blast radius): write only the per-peer row.
		if !s.recordRemoteBanLocked(resolved, reason, until, now).IsZero() {
			recorded = true
		}
	}
	s.peerMu.Unlock()
	log.Trace().Str("site", "handlePeerBannedNotice").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("peer_mu_writer")

	if !recorded {
		// No PERSISTED mutation — reason=peer-ban on an unknown peer,
		// reason=blacklisted with no IP to key on, or a blacklisted
		// notice for an unknown peer that is still below the IP-wide
		// escalation threshold. recordRemoteBanLocked already logged the
		// unknown-peer case. Note `recorded` tracks only persisted state:
		// a sub-threshold blacklisted notice may still have updated the
		// VOLATILE remoteIPBanOffenders set (it counts toward a future
		// escalation), and that correctly needs no flushPeerState because
		// the offender set is never persisted.
		return
	}
	// Mark the remote-ban gate dirty so it survives a crash and the dialler
	// keeps skipping this peer instead of resurrecting the storm the notice
	// was supposed to end. The next bootstrapLoop tick coalesces it into a
	// debounced flush (markPeerStateDirty) — matches the deferred-flush
	// pattern used by manual peer registration.
	s.markPeerStateDirty()
}

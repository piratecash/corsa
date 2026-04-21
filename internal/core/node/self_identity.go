package node

import (
	"errors"
	"fmt"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// self_identity.go concentrates the defence against self-loopback at the
// cryptographic identity layer. The pre-existing isSelfAddress / isSelfDialIP
// helpers only catch the host:port tuple — they cannot see through NAT
// reflection, peer-exchange echoes, fallback-port aliases or onion/clearnet
// mirrors. In every such case the remote endpoint resolves to a different
// address but the hello/welcome carries our own Ed25519 address, and without
// an identity-level gate the node enters a reconnect loop against itself.
// The helpers below are the single source of truth for "that peer is us":
// handshake call sites consult isSelfIdentity before trusting the remote's
// identity and dialler-side call sites convert the decision into a
// structured selfIdentityError so the penalty / cooldown path can react.

// ---------------------------------------------------------------------------
// errSelfIdentity — package sentinel exposed through selfIdentityError.Unwrap
// ---------------------------------------------------------------------------

// errSelfIdentity is the package-local sentinel discriminated via errors.Is.
// Keep it wire-independent: the protocol-side wire form lives in
// protocol.ErrSelfIdentity / protocol.ErrCodeSelfIdentity. Callers that
// need to react to "peer is us" (ConnectionManager dial workers,
// onCMDialFailed, peer_management bootstrap loops) match against this
// sentinel so they stay decoupled from the transport envelope.
var errSelfIdentity = errors.New("peer advertised local identity")

// ---------------------------------------------------------------------------
// selfIdentityError — structured error carrying the remote evidence
// ---------------------------------------------------------------------------

// selfIdentityError wraps errSelfIdentity with the evidence captured from
// the rejected welcome/hello frame. Mirrors the incompatibleProtocolError
// pattern from version_policy.go so onCMDialFailed can pass structured
// context into the address-cooldown logic (no string parsing).
type selfIdentityError struct {
	// Address is the overlay address the dialler aimed at when the
	// collision surfaced. Populated on outbound paths; empty on the
	// inbound path because the remote opened the socket.
	Address domain.PeerAddress
	// PeerListen is the listen field the remote advertised in its
	// welcome. Diagnostic only — the address we punish is Address.
	// Typed as domain.PeerAddress (not raw string) so compile-time
	// checks catch accidental cross-assignments with unrelated string
	// fields (welcome.ObservedAddress, health keys, etc.). The wire
	// form is a plain string; the boundary conversion lives in
	// newSelfIdentityError.
	PeerListen domain.PeerAddress
	// LocalIdentity is the local node identity the remote matched.
	// Kept on the error so logs on the fail path can render the full
	// triple without re-reading Service state.
	LocalIdentity domain.PeerIdentity
}

func (e *selfIdentityError) Error() string {
	return fmt.Sprintf("self-identity detected: address=%q listen=%q local_identity=%q",
		e.Address, e.PeerListen, e.LocalIdentity)
}

func (e *selfIdentityError) Unwrap() error { return errSelfIdentity }

// newSelfIdentityError builds a structured self-identity error for the
// outbound paths (openPeerSession, openPeerSessionForCM, syncPeer). The
// LocalIdentity field is filled automatically from the Service so every
// call site emits a fully populated record regardless of whether the
// caller had the value in scope.
//
// peerListen arrives as a raw wire string (welcome.Listen). The
// TrimSpace + domain.PeerAddress conversion is the single boundary
// point — downstream consumers work with the domain-typed field.
//
// Returns the concrete *selfIdentityError so call sites can pass the
// record straight into applySelfIdentityCooldown (which needs the
// structured fields) without an errors.As dance. The concrete return
// still satisfies the error interface through implicit conversion at
// return sites that surface it as `error` (openPeerSession /
// openPeerSessionForCM bubble it up the handshake stack).
func (s *Service) newSelfIdentityError(address domain.PeerAddress, peerListen string) *selfIdentityError {
	return &selfIdentityError{
		Address:       address,
		PeerListen:    domain.PeerAddress(strings.TrimSpace(peerListen)),
		LocalIdentity: domain.PeerIdentity(s.identity.Address),
	}
}

// ---------------------------------------------------------------------------
// isSelfIdentity — handshake-layer identity equality
// ---------------------------------------------------------------------------

// isSelfIdentity reports whether the given identity matches the local
// node's Ed25519 address. Empty input is NOT self — callers that want to
// treat a missing identity as a failure must guard on emptiness separately
// (unauthenticated hello path). This keeps the helper monotone: adding
// an empty guard upstream never flips an existing true to false.
func (s *Service) isSelfIdentity(id domain.PeerIdentity) bool {
	if id == "" {
		return false
	}
	return string(id) == s.identity.Address
}

// ---------------------------------------------------------------------------
// tryApplySelfIdentityCooldown — shared failure-dispatch for all dial paths
// ---------------------------------------------------------------------------

// tryApplySelfIdentityCooldown recognises the two arrival shapes of a
// self-loopback error surfaced by ANY outbound dial path and routes them
// through applySelfIdentityCooldown on a hit. Returns true when the
// cooldown was applied so the caller can skip the generic disconnect
// penalty (markPeerDisconnected / penalizeOldProtocolPeer / retry
// backoff) — self-identity is a terminal failure against the address,
// not a transient disconnect.
//
// The two recognised shapes are:
//   - (*selfIdentityError) — the structured record produced locally on
//     the outbound side when openPeerSession / openPeerSessionForCM /
//     syncPeer / sendNoticeToPeer parses a welcome carrying our own
//     Ed25519 address. Carries Address, PeerListen, LocalIdentity.
//   - protocol.ErrSelfIdentity (bare sentinel) — the wire shape that
//     peerSessionRequest / syncPeer connection_notice branches resolve
//     via NoticeErrorFromFrame when the remote detected the self-loopback
//     first and answered with connection_notice{peer-banned,
//     reason=self-identity}. Has no structured fields, so we synthesise
//     them from the dial target and local identity.
//
// Every outbound failure hook (onCMDialFailed, runPeerSession) MUST
// consult this helper before dispatching markPeerDisconnected —
// otherwise a self-alias gets punished with the generic disconnect
// diagnostics and the 24h cooldown never lands, which reopens the
// reconnect-churn window the cooldown was designed to close.
func (s *Service) tryApplySelfIdentityCooldown(address domain.PeerAddress, err error) bool {
	var selfErr *selfIdentityError
	if errors.As(err, &selfErr) {
		s.applySelfIdentityCooldown(address, selfErr)
		return true
	}
	if errors.Is(err, protocol.ErrSelfIdentity) {
		s.applySelfIdentityCooldown(address, &selfIdentityError{
			Address:       address,
			LocalIdentity: domain.PeerIdentity(s.identity.Address),
		})
		return true
	}
	return false
}

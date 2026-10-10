package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// connectOnlyDNSTimeout bounds the one-shot DNS resolution performed when a
// connectOnly target is a plain hostname.
const connectOnlyDNSTimeout = 5 * time.Second

// connectOnlyBlockedSentinel is the pin stored when an invalid CORSA_CONNECT_ONLY
// seed forces a fail-closed startup. The leading NUL makes it impossible for any
// real "host:port" peer address (including one restored from peers.json) to equal
// it, so Candidates() admits NOTHING and egress stays blocked — and the pin's
// ForbiddenFn/private carve-outs in Candidates can never apply to a real peer,
// because no real peer is ever the pin in this state.
const connectOnlyBlockedSentinel = domain.PeerAddress("\x00connect-only-egress-blocked")

// connectOnlySelfRejection is the error text of the reply to a connect_only
// whose target is this node, however it was written: the literal listen
// address, or a hostname that resolves to it.
const connectOnlySelfRejection = "cannot connect-only to self"

// connectOnlyDisableTokens are the textual aliases that clear the egress pin
// instead of setting a target. An empty argument disables the pin too.
var connectOnlyDisableTokens = map[string]struct{}{
	"off":   {},
	"none":  {},
	"clear": {},
}

// connectOnlyTarget reports the active single-peer egress pin. ok=false means
// dialing is unrestricted. Wired into PeerProviderConfig.ConnectOnlyFn, so it
// is read on the Candidates() dial hot path — it must stay lock-free, which is
// why connectOnly is an atomic.Pointer rather than domain-mutex state.
func (s *Service) connectOnlyTarget() (domain.PeerAddress, bool) {
	if p := s.connectOnly.Load(); p != nil {
		return *p, true
	}
	return "", false
}

// ConnectOnly is the RPC entry point of the connect_only command: target pins
// egress to that peer, an empty target (or one of connectOnlyDisableTokens)
// clears the pin. ctx is the request's context; enabling waits on the
// ConnectionManager to drop the other outbound slots, and that wait ends when
// the caller abandons the request.
func (s *Service) ConnectOnly(ctx context.Context, target string) protocol.Frame {
	peers := []string{}
	if target != "" {
		peers = []string{target}
	}
	return s.connectOnlyFrame(ctx, protocol.Frame{Type: "connect_only", Peers: peers})
}

// connectOnlyFrame handles the local "connect_only" command (console / RPC).
// A target address pins egress to that single peer; an empty argument (or one
// of connectOnlyDisableTokens) clears the pin and restores normal dialing.
// Incoming connections are never affected — the pin governs outbound only.
func (s *Service) connectOnlyFrame(ctx context.Context, frame protocol.Frame) protocol.Frame {
	raw := ""
	if len(frame.Peers) > 0 {
		raw = strings.TrimSpace(frame.Peers[0])
	}

	if _, disable := connectOnlyDisableTokens[strings.ToLower(raw)]; raw == "" || disable {
		return s.disableConnectOnly()
	}
	return s.enableConnectOnly(ctx, raw)
}

// enableConnectOnly pins outbound dialing to a single peer: it normalises the
// address, rejects a self-pin, records the pin (atomic), registers the peer
// and dials it immediately via the operator add-peer path, then drops every
// other outbound slot and waits until they are gone. The pin is stored BEFORE
// the eviction so a fill() triggered by the dropped slots cannot re-select any
// other candidate — only the pinned address survives Candidates() while the
// pin is active.
func (s *Service) enableConnectOnly(ctx context.Context, rawAddress string) protocol.Frame {
	address := strings.TrimSpace(rawAddress)

	// Self-check the LITERAL target first, before any normalisation: catches an
	// exact listen-address match (e.g. a wildcard ":64646" bind) directly. This
	// must precede splitHostPort because splitHostPort rejects a host-less
	// ":port" form, which would otherwise be mis-normalised into a bogus host.
	if s.isSelfAddress(domain.PeerAddress(address)) {
		return protocol.Frame{Type: "error", Error: connectOnlySelfRejection}
	}

	host, port, ok := splitHostPort(address)
	if !ok {
		// No usable host:port split — treat the whole input as a bare host and
		// append the default port (covers "example.com", "1.2.3.4").
		host, port = address, config.DefaultPeerPort
	}

	// A plain DNS hostname is not dialable by the peer subsystem (classifyHost
	// maps it to NetGroupUnknown, which canReach rejects), so resolve it to an
	// IP once at pin time. IP literals and overlay (.onion/.b32.i2p) targets
	// pass through unchanged.
	dialHost, err := s.resolveConnectOnlyHost(ctx, host)
	if err != nil {
		return protocol.Frame{Type: "error", Error: err.Error()}
	}

	dialAddress := net.JoinHostPort(dialHost, port)
	peerAddress := domain.PeerAddress(dialAddress)

	// Every check that can reject the target runs HERE, before the pin is
	// written: a connect_only that fails must never make its target the live
	// pin, not even for a moment. A transiently live bad target is visible to
	// fill and to other commands — it evicts the old pin's session, and a
	// racing command's "restore the previous pin" can write it back for good
	// and strand egress at zero. After the write nothing can fail, so there
	// is nothing to restore.
	//
	// That includes the self-check on the RESOLVED address — a hostname can
	// resolve to our own IP, which can never establish a session
	// (isSelfIdentity rejects the handshake) and would strand egress at zero.
	// It is reported in connect_only's own words.
	target, err := s.validateAddPeerTarget(dialAddress)
	if err != nil {
		return protocol.Frame{Type: "error", Error: connectOnlyRejection(err)}
	}

	// Record the pin before the dial and the eviction, so Candidates() and the
	// manual dial (BuildDialAddresses suppresses the default-port fallback for
	// the pin) are already restricted when the slot churn below triggers a
	// refill. Racing commands that both pass validation leave the pin of the
	// one that writes last; each reply describes its own command.
	pin := target.address
	s.connectOnly.Store(&pin)

	// Register + immediately dial the target through the shared operator
	// add-peer admission (penalty reset, ManualPeerRequested bypass). It
	// cannot fail once validated, and its reply describes the add_peer side
	// of this command, which the connect_only reply below replaces.
	_ = s.admitAddPeerTarget(ctx, target, addPeerModeOperator)

	// Drop every other outbound connection. Incoming connections live in the
	// ipState domain, not the ConnectionManager, so they are untouched. The
	// result is only logged: an unconfirmed drop leaves the pin set, and the
	// next fill enforces it anyway.
	_ = s.retainOnlyPinnedOutbound(ctx, peerAddress)

	log.Info().
		Str("address", string(peerAddress)).
		Str("requested_host", host).
		Str("network", classifyAddress(peerAddress).String()).
		Msg("connect_only_enabled")

	status := "connect-only pinned to " + dialAddress
	if dialHost != host {
		status = fmt.Sprintf("connect-only pinned to %s (resolved %s)", dialAddress, host)
	}
	return protocol.Frame{
		Type:   "ok",
		Peers:  []string{dialAddress},
		Status: status,
	}
}

// connectOnlyRejection is the connect_only reply text for a target
// validateAddPeerTarget rejected: the add_peer reason, except that a self
// target reads as connectOnlySelfRejection, the same message the literal
// self check gives.
func connectOnlyRejection(err error) string {
	var rejected addPeerRejectedError
	if errors.As(err, &rejected) && rejected.kind == addPeerRejectedSelf {
		return connectOnlySelfRejection
	}
	return err.Error()
}

// retainOnlyPinnedOutbound asks the ConnectionManager to drop every outbound
// slot except the pinned one and waits until it has: the connect_only reply
// goes out only after the other outbound sessions are gone.
//
// RetainOnly blocks on the CM event loop, so this must run with no domain
// mutex, PeerProvider.mu or cm.mu held and never on the event loop itself.
// Both callers satisfy that: connectOnlyFrame runs on the RPC / console
// goroutine and applyStartupConnectOnly on the Run goroutine, and neither
// holds a lock across enableConnectOnly.
//
// Every write of a non-nil pin goes through here: enableConnectOnly and the
// fail-closed startup sentinel. The ConnectionManager applies the request
// against the live pin, and re-applies the same rule on every fill.
//
// Returns false when the eviction is not confirmed, and logs why: the event
// loop is not running — before Run there are no outbound slots, and shutdown
// clears them itself — or ctx ended first. The pin stays set either way; the
// next fill enforces it.
func (s *Service) retainOnlyPinnedOutbound(ctx context.Context, pin domain.PeerAddress) bool {
	if s.connManager == nil {
		return false
	}
	if s.connManager.RetainOnly(ctx, pin) {
		return true
	}
	log.Info().
		Str("address", connectOnlyPinLabel(pin)).
		Str("reason", string(retainUnconfirmedReasonFor(ctx))).
		Msg("connect_only_retain_unconfirmed")
	return false
}

// retainUnconfirmedReason says why a connect_only retention was not confirmed.
type retainUnconfirmedReason string

const (
	// retainUnconfirmedRequestEnded: the command's own context ended first;
	// the request may still be applied, and the next fill applies the rule.
	retainUnconfirmedRequestEnded retainUnconfirmedReason = "request_context_ended"
	// retainUnconfirmedCMNotRunning: the ConnectionManager's event loop is
	// not running — not started yet, or stopping — so there is nothing to
	// drop.
	retainUnconfirmedCMNotRunning retainUnconfirmedReason = "cm_not_running"
)

func retainUnconfirmedReasonFor(ctx context.Context) retainUnconfirmedReason {
	if ctx.Err() != nil {
		return retainUnconfirmedRequestEnded
	}
	return retainUnconfirmedCMNotRunning
}

// connectOnlyPinLabel is how a pin appears in logs. The fail-closed sentinel
// starts with a NUL byte so that no real address can equal it; logged raw it
// is an unreadable, terminal-unsafe string, so it is named instead.
func connectOnlyPinLabel(pin domain.PeerAddress) string {
	if pin == connectOnlyBlockedSentinel {
		return "<connect-only-egress-blocked>"
	}
	return string(pin)
}

// resolveConnectOnlyHost resolves a plain DNS hostname to a dialable IP. IP
// literals and overlay hosts (.onion / .b32.i2p) are returned unchanged — the
// peer subsystem dials those directly. A bare hostname maps to
// NetGroupUnknown (classifyHost) which canReach rejects, so it is resolved once
// here, preferring an IPv4 result then falling back to the first address.
//
// The lookup is I/O on behalf of the command, so it runs under the command's
// context — an abandoned request stops resolving — bounded further by
// connectOnlyDNSTimeout so a slow resolver cannot hang the command. The
// resolver is the Service's connectOnlyResolver dependency.
func (s *Service) resolveConnectOnlyHost(ctx context.Context, host string) (string, error) {
	if net.ParseIP(host) != nil || classifyHost(host) != domain.NetGroupUnknown {
		return host, nil
	}

	ctx, cancel := context.WithTimeout(ctx, connectOnlyDNSTimeout)
	defer cancel()

	ips, err := s.connectOnlyResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", fmt.Errorf("cannot resolve hostname %q: %w", host, err)
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("cannot resolve hostname %q: no addresses", host)
	}

	for _, ip := range ips {
		if ip.IP.To4() != nil {
			return ip.IP.String(), nil
		}
	}
	return ips[0].IP.String(), nil
}

// disableConnectOnly clears the egress pin and nudges the ConnectionManager to
// refill from the full candidate set again. Idempotent: clearing an already
// unset pin is a no-op success.
func (s *Service) disableConnectOnly() protocol.Frame {
	s.connectOnly.Store(nil)

	if s.connManager != nil {
		s.connManager.EmitHint(NewPeersDiscovered{Count: 1})
	}

	log.Info().Msg("connect_only_disabled")

	return protocol.Frame{Type: "ok", Status: "connect-only disabled"}
}

// applyStartupConnectOnly applies the CORSA_CONNECT_ONLY startup seed once the
// ConnectionManager is running. A blank seed is a no-op.
//
// On an invalid seed (typo, self, unresolvable host, forbidden/unreachable
// target) the startup path fails CLOSED, not open: the operator explicitly
// demanded a single-peer pin via CORSA_CONNECT_ONLY, so we must NOT silently
// fall back to unrestricted dialing. The pin is set to
// connectOnlyBlockedSentinel, which no peer address can equal, so Candidates()
// admits nothing, any outbound slot that already exists is dropped, and egress
// stays at zero until the operator fixes the value or clears the pin at
// runtime (connectOnly off). This runs before bootstrapLoop lets the manager
// fill (bootstrap priming only hints, and a pre-bootstrap hint is dropped), so
// the only slot that can exist here is one an operator add_peer opened in the
// window since the manager started. ctx is Run's: it bounds the wait for that
// slot to go.
func (s *Service) applyStartupConnectOnly(ctx context.Context) {
	address := strings.TrimSpace(s.cfg.ConnectOnly)
	if address == "" {
		return
	}

	reply := s.connectOnlyFrame(ctx, protocol.Frame{Type: "connect_only", Peers: []string{address}})
	if reply.Type != "error" {
		return
	}

	// Fail closed: pin to a sentinel that no real peer address can equal, so
	// Candidates() admits nothing and egress stays at zero. We must NOT pin to
	// the raw seed here — a seed that names a forbidden/private peer already in
	// peers.json would, via the pin's ForbiddenFn/private carve-outs, turn into
	// a live candidate and defeat the "block all egress" guarantee. The sentinel
	// sidesteps that: it matches nothing, so those carve-outs never fire.
	blocked := connectOnlyBlockedSentinel
	s.connectOnly.Store(&blocked)
	// The sentinel is a live pin like any other: a slot an early operator
	// add_peer opened must go. The result is only logged — an unconfirmed
	// drop is enforced by the first fill, and the manager does not fill
	// before bootstrap.
	_ = s.retainOnlyPinnedOutbound(ctx, blocked)

	log.Warn().
		Str("address", address).
		Str("error", reply.Error).
		Msg("connect_only_startup_failed_egress_blocked")
}

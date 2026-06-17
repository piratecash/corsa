package node

import (
	"context"
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

// connectOnlyFrame handles the local "connect_only" command (console / RPC).
// A target address pins egress to that single peer; an empty argument (or one
// of connectOnlyDisableTokens) clears the pin and restores normal dialing.
// Incoming connections are never affected — the pin governs outbound only.
func (s *Service) connectOnlyFrame(frame protocol.Frame) protocol.Frame {
	raw := ""
	if len(frame.Peers) > 0 {
		raw = strings.TrimSpace(frame.Peers[0])
	}

	if _, disable := connectOnlyDisableTokens[strings.ToLower(raw)]; raw == "" || disable {
		return s.disableConnectOnly()
	}
	return s.enableConnectOnly(raw)
}

// enableConnectOnly pins outbound dialing to a single peer: it normalises the
// address, rejects a self-pin, records the pin (atomic), registers the peer
// and dials it immediately via the operator add-peer path, then drops every
// other outbound slot. The pin is stored BEFORE the eviction so a fill()
// triggered by the dropped slots cannot re-select any other candidate — only
// the pinned address survives Candidates() while the pin is active.
func (s *Service) enableConnectOnly(rawAddress string) protocol.Frame {
	address := strings.TrimSpace(rawAddress)

	// Self-check the LITERAL target first, before any normalisation: catches an
	// exact listen-address match (e.g. a wildcard ":64646" bind) directly. This
	// must precede splitHostPort because splitHostPort rejects a host-less
	// ":port" form, which would otherwise be mis-normalised into a bogus host.
	if s.isSelfAddress(domain.PeerAddress(address)) {
		return protocol.Frame{Type: "error", Error: "cannot connect-only to self"}
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
	dialHost, err := resolveConnectOnlyHost(host)
	if err != nil {
		return protocol.Frame{Type: "error", Error: err.Error()}
	}

	dialAddress := net.JoinHostPort(dialHost, port)
	peerAddress := domain.PeerAddress(dialAddress)

	// Re-check self against the RESOLVED address: a hostname could resolve to
	// our own IP, which can never establish a session (isSelfIdentity rejects
	// the handshake) and would strand egress at zero.
	if s.isSelfAddress(peerAddress) {
		return protocol.Frame{Type: "error", Error: "cannot connect-only to self"}
	}

	// Record the pin first so Candidates() is already restricted before any
	// slot churn below can trigger a refill. Swap returns the PREVIOUS pin so
	// a failed re-pin can restore it instead of clobbering it: an operator who
	// is already pinned to A and fat-fingers an unreachable B must keep A, not
	// fall back to unrestricted egress.
	pin := peerAddress
	prev := s.connectOnly.Swap(&pin)

	// Register + immediately dial the target through the shared operator
	// add-peer path (validation, penalty reset, ManualPeerRequested bypass).
	reply := s.applyAddPeer(protocol.Frame{Type: "add_peer", Peers: []string{dialAddress}}, addPeerModeOperator)
	if reply.Type == "error" {
		// Admission failed (forbidden / unreachable). Restore the prior pin
		// (nil when there was none) so we neither strand egress at zero nor
		// silently drop an existing pin.
		s.connectOnly.Store(prev)
		return reply
	}

	// Drop every other outbound connection. Incoming connections live in the
	// ipState domain, not the ConnectionManager, so they are untouched.
	if s.connManager != nil {
		s.connManager.RetainOnly(peerAddress)
	}

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

// resolveConnectOnlyHost resolves a plain DNS hostname to a dialable IP. IP
// literals and overlay hosts (.onion / .b32.i2p) are returned unchanged — the
// peer subsystem dials those directly. A bare hostname maps to
// NetGroupUnknown (classifyHost) which canReach rejects, so it is resolved once
// here, preferring an IPv4 result then falling back to the first address.
//
// The lookup is bounded infrastructure I/O at an explicit operator action.
// HandleLocalFrame carries no request context, so a bounded background context
// is used deliberately rather than fabricating request scope; the timeout keeps
// a slow resolver from hanging the command.
func resolveConnectOnlyHost(host string) (string, error) {
	if net.ParseIP(host) != nil || classifyHost(host) != domain.NetGroupUnknown {
		return host, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectOnlyDNSTimeout)
	defer cancel()

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
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
// fall back to unrestricted dialing. The pin is set to the raw (normalised)
// target — which is not a registered/dialable candidate — so Candidates()
// admits nothing and egress stays at zero until the operator fixes the value
// or clears the pin at runtime (connectOnly off).
func (s *Service) applyStartupConnectOnly() {
	address := strings.TrimSpace(s.cfg.ConnectOnly)
	if address == "" {
		return
	}

	reply := s.connectOnlyFrame(protocol.Frame{Type: "connect_only", Peers: []string{address}})
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

	log.Warn().
		Str("address", address).
		Str("error", reply.Error).
		Msg("connect_only_startup_failed_egress_blocked")
}

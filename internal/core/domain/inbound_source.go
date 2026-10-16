package domain

import (
	"net/netip"
	"time"
)

// InboundSourceClass is the admission class of a connection the listener
// accepted (docs/protocol/network_security.md, "Loopback inbound class").
// It decides which bounds and which punishment apply to the connection:
//
//   - an external source is bounded and banned by its IP;
//   - a loopback source — every inbound onion connection arrives from the
//     local Tor daemon, and a local process on the same machine looks the
//     same — is one shared class with budgets of its own and no IP ban,
//     because banning 127.0.0.1 for one peer would cut every other onion
//     peer off.
//
// The zero value names no class: a classification always yields one of the
// two named values, so a zero reaching a decision is a bug, not a default.
type InboundSourceClass uint8

const (
	// InboundSourceExternal is a source with an address of its own: bounded
	// per IP, scored and banned per IP.
	InboundSourceExternal InboundSourceClass = iota + 1
	// InboundSourceLoopback is a source on 127.0.0.0/8 or ::1, an
	// IPv4-mapped loopback address included.
	InboundSourceLoopback
)

var inboundSourceClassNames = map[InboundSourceClass]string{
	InboundSourceExternal: "external",
	InboundSourceLoopback: "loopback",
}

func (c InboundSourceClass) String() string {
	if name, ok := inboundSourceClassNames[c]; ok {
		return name
	}
	return "unknown"
}

// ClassifyInboundSource is the class of a source host. A host that does not
// parse is external: the listener counts it per IP by its raw text, as it
// always has.
//
// The node has a single listener, so the address is the only thing the class
// can be read from: the local Tor daemon and any other local process both
// arrive from loopback and fall into the same class.
func ClassifyInboundSource(remoteHost string) InboundSourceClass {
	addr, err := netip.ParseAddr(remoteHost)
	if err == nil && addr.Unmap().IsLoopback() {
		return InboundSourceLoopback
	}
	return InboundSourceExternal
}

// LoopbackInboundBounds are the budgets of the loopback inbound class. Each
// is a bound on the whole class, never on one connection, so reconnecting
// does not open a fresh budget.
type LoopbackInboundBounds struct {
	// Enforced is false on a node whose per-IP limits are switched off (a
	// test or load-stand node): the class is then counted but not bounded.
	Enforced bool
	// MaxActive bounds the loopback connections held at once, v2 and legacy
	// together, from accept to close.
	MaxActive int
	// MaxNewPerWindow bounds the loopback connections admitted within any
	// Window (a sliding window, not a fixed one).
	MaxNewPerWindow int
	Window          time.Duration
	// MaxHandshakes bounds the loopback v2 handshakes in flight past their
	// ClientHello record — the loopback share of the node's handshake bound.
	MaxHandshakes int
}

// LoopbackInboundRefusals counts loopback connections refused or closed, by
// cause. Cumulative since the process started, never reset by reading.
type LoopbackInboundRefusals struct {
	// ConnectionRate counts connections over MaxNewPerWindow.
	ConnectionRate uint64
	// ActiveLimit counts connections over MaxActive.
	ActiveLimit uint64
	// ConnectionBudget counts loopback connections the node's shared
	// connection budget had no room for.
	ConnectionBudget uint64
	// InboundLimit counts loopback connections refused at registration by the
	// node's inbound connection limit.
	InboundLimit uint64
	// HandshakesLoopback counts loopback v2 handshakes over MaxHandshakes.
	HandshakesLoopback uint64
	// HandshakesNode counts loopback v2 handshakes over the node's own
	// handshake bound.
	HandshakesNode uint64
	// ViolationsClosed counts loopback connections closed for a violation
	// before any v2 proof: legacy connections, whose claimed identity is
	// never charged.
	ViolationsClosed uint64
	// IdentityBanned counts loopback v2 sessions closed because the identity
	// they proved is banned.
	IdentityBanned uint64
}

// LoopbackInboundStats is the state of the loopback inbound class for
// diagnostics (fetchRouteSummary, section loopback_inbound).
type LoopbackInboundStats struct {
	// StartedAt is when accumulation began; ReadAt is when the counters were
	// loaded.
	StartedAt time.Time
	ReadAt    time.Time

	Bounds LoopbackInboundBounds

	// Active is the loopback connections held now.
	Active int
	// HandshakesInFlight is the loopback v2 handshakes past their ClientHello
	// record now.
	HandshakesInFlight int
	// Admitted counts loopback connections admitted since StartedAt.
	Admitted uint64

	Refusals LoopbackInboundRefusals

	// IdentitiesScored is the proven identities with a penalty score now;
	// IdentitiesBanned is how many of them are banned.
	IdentitiesScored int
	IdentitiesBanned int
}

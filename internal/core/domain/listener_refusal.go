package domain

import "time"

// ListenerRefusalStats is how many accepted connections the listener refused
// before any session existed, by the bound that refused them, and how many of
// those refusals went out without the TLS alert because every alert sender
// was busy (docs/protocol/session_v2.md, "Listener refusals"). Cumulative
// since the process started and in memory only.
type ListenerRefusalStats struct {
	// StartedAt is when accumulation began.
	StartedAt time.Time
	// ReadAt is when the counters were loaded.
	ReadAt time.Time

	// ConnectionRate counts connections over the per-IP connection rate.
	ConnectionRate uint64
	// ConnectionsPerIP counts connections over the per-IP cap on
	// simultaneous connections.
	ConnectionsPerIP uint64
	// ConnectionBudget counts connections the shared connection budget had
	// no room for.
	ConnectionBudget uint64
	// Blacklisted counts connections from a blacklisted IP.
	Blacklisted uint64
	// InboundLimit counts connections refused at registration because the
	// inbound connection limit was reached.
	InboundLimit uint64
	// HandshakesNode counts v2 handshakes refused because the node already
	// ran its bound of concurrent inbound handshakes.
	HandshakesNode uint64
	// HandshakesPerIP counts v2 handshakes refused because their source —
	// an IPv4 address, or the /64 of an IPv6 one — already ran its bound of
	// concurrent inbound handshakes.
	HandshakesPerIP uint64
	// HandshakesLoopback counts v2 handshakes from a loopback source refused
	// because loopback sources already ran their share of the node bound.
	HandshakesLoopback uint64
	// LoopbackConnectionRate counts loopback connections over the loopback
	// class's rate of new connections.
	LoopbackConnectionRate uint64
	// LoopbackConnections counts loopback connections over the loopback
	// class's bound on connections held at once.
	LoopbackConnections uint64

	// ClosedWithoutAlert counts refusals closed at once, without the alert,
	// because every alert sender was busy, or the source's share of them. It
	// is part of the counts above, not in addition to them.
	ClosedWithoutAlert uint64
}

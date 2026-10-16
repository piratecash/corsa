package node

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// inbound_handshakes.go bounds the inbound v2 handshakes a node runs at once
// (docs/protocol/session_v2.md, "Listener refusals"): each costs a TLS
// handshake and a proof verification, and a source that opens them faster
// than they finish must not take the node's CPU or every slot.
//
// The bounds are taken in two steps, so that holding one costs the holder
// something:
//
//   - at the first byte 0x16, a slot of the connection's SOURCE (an IPv4
//     address, or the /64 of an IPv6 one), and the whole ClientHello record
//     must then arrive within clientHelloRecordTimeout. A socket that sends
//     nothing holds no slot at all, and one that trickles its ClientHello
//     holds only its own source's slot, and only for that deadline.
//   - once the record is whole, a slot of the NODE (with a share for loopback
//     sources), held to the end of the handshake and of the pin it stores.
//     Only a dialler that sent a complete ClientHello competes for it, so a
//     few addresses trickling bytes cannot close v2 to everybody.

// handshakeBounds are the bounds on inbound v2 handshakes in flight.
type handshakeBounds struct {
	// node bounds the handshakes past their ClientHello record on the node.
	node int
	// loopback bounds the part of node that loopback sources — the onion
	// entry, many clients behind 127.0.0.1 — may hold, so that they cannot
	// take the whole node bound from the clearnet entry.
	loopback int
	// perSource bounds the handshakes of one external source from the first
	// byte on. It is not lower because many honest clients share one address
	// behind CGNAT or a Tor exit.
	perSource int
}

// productionHandshakeBounds are the bounds a node runs with.
func productionHandshakeBounds() handshakeBounds {
	return handshakeBounds{node: 64, loopback: 16, perSource: 4}
}

// clientHelloRecordTimeout bounds the read of the whole ClientHello record
// after its first byte: about one round trip over Tor, ample for the ~1.5 KB
// a dialler sends in one write.
const clientHelloRecordTimeout = time.Second

// ipv6SourcePrefix is the prefix one IPv6 source is counted by: a single
// subscriber is routinely given a whole /64, so counting addresses would give
// one host 2^64 sources.
const ipv6SourcePrefix = 64

// handshakeSource is where an inbound handshake comes from.
type handshakeSource struct {
	// group is the source: an IPv4 address, or the /64 of an IPv6 one.
	group string
	// perSource says whether the per-source bound applies: not to a loopback
	// source and not on a node whose per-IP limits are off.
	perSource bool
	// loopback says whether the source takes the loopback share of the node
	// bound.
	loopback bool
}

// handshakeSourceOf names the source of an accepted connection.
func handshakeSourceOf(addr net.Addr, perIPLimits bool) handshakeSource {
	ip := net.ParseIP(remoteIP(addr))
	loopback := inboundSourceClassOf(addr) == domain.InboundSourceLoopback
	return handshakeSource{
		group:     sourceGroupOf(ip, remoteIP(addr)),
		perSource: perIPLimits && ip != nil && !loopback,
		loopback:  loopback,
	}
}

// sourceGroupOf is the source an IP is counted as. raw is kept for an address
// that does not parse, so that it is still counted as itself.
func sourceGroupOf(ip net.IP, raw string) string {
	if ip == nil {
		return raw
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	prefix := &net.IPNet{IP: ip.Mask(net.CIDRMask(ipv6SourcePrefix, 128)), Mask: net.CIDRMask(ipv6SourcePrefix, 128)}
	return prefix.String()
}

// inboundHandshakes counts the inbound v2 handshakes in flight. Its own leaf
// mutex: it is taken with no domain mutex held and nothing is called under
// it. loopbackGauge mirrors loopbackInFlight for the diagnostics, which read
// it without the mutex.
type inboundHandshakes struct {
	bounds handshakeBounds

	mu               sync.Mutex
	inFlight         int
	loopbackInFlight int
	bySource         map[string]int

	loopbackGauge atomic.Int64
}

func newInboundHandshakes(bounds handshakeBounds) *inboundHandshakes {
	return &inboundHandshakes{bounds: bounds, bySource: make(map[string]int)}
}

// admitSource takes a slot of source at the first byte 0x16, or refuses it
// with a *listenerRefusalError. A source the per-source bound does not apply
// to is admitted without a slot. The returned release gives the slot back;
// a second call gives nothing back.
func (h *inboundHandshakes) admitSource(source handshakeSource) (func(), error) {
	if !source.perSource {
		return func() {}, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bySource[source.group] >= h.bounds.perSource {
		return nil, &listenerRefusalError{reason: refusalHandshakesPerIP}
	}
	h.bySource[source.group]++
	var once sync.Once
	return func() { once.Do(func() { h.leaveSource(source.group) }) }, nil
}

func (h *inboundHandshakes) leaveSource(group string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bySource[group] > 1 {
		h.bySource[group]--
		return
	}
	delete(h.bySource, group)
}

// enter takes a node slot for a handshake whose ClientHello record is whole,
// or refuses it with a *listenerRefusalError naming the bound. The returned
// release gives the slot back on every outcome of the handshake; a second
// call gives nothing back.
func (h *inboundHandshakes) enter(source handshakeSource) (func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case h.inFlight >= h.bounds.node:
		return nil, &listenerRefusalError{reason: refusalHandshakesNode}
	case source.loopback && h.loopbackInFlight >= h.bounds.loopback:
		return nil, &listenerRefusalError{reason: refusalHandshakesLoopback}
	}
	h.inFlight++
	if source.loopback {
		h.loopbackInFlight++
		h.loopbackGauge.Store(int64(h.loopbackInFlight))
	}
	var once sync.Once
	return func() { once.Do(func() { h.leave(source.loopback) }) }, nil
}

func (h *inboundHandshakes) leave(loopback bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inFlight--
	if loopback {
		h.loopbackInFlight--
		h.loopbackGauge.Store(int64(h.loopbackInFlight))
	}
}

// loopbackRunning is the loopback handshakes past their ClientHello record
// now. Lock-free.
func (h *inboundHandshakes) loopbackRunning() int {
	return int(h.loopbackGauge.Load())
}

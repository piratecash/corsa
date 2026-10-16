package domain

import (
	"errors"
	"net"
	"strconv"
)

// EndpointTransport is how a dialled endpoint is reached. It is part of the
// endpoint's identity: the same host and port over another transport is a
// different endpoint.
type EndpointTransport string

const (
	// EndpointTransportTCP: a direct TCP dial — an IP literal or a DNS name.
	EndpointTransportTCP EndpointTransport = "tcp"
	// EndpointTransportTor: a v3 onion service, reached through the SOCKS5
	// proxy.
	EndpointTransportTor EndpointTransport = "tor"
	// EndpointTransportI2P: a .b32.i2p destination, reached through the
	// SOCKS5 proxy.
	EndpointTransportI2P EndpointTransport = "i2p"
)

// Valid reports whether t is one of the three transports.
func (t EndpointTransport) Valid() bool {
	switch t {
	case EndpointTransportTCP, EndpointTransportTor, EndpointTransportI2P:
		return true
	default:
		return false
	}
}

// ErrIncompleteCanonicalEndpoint is a canonical endpoint assembled without a
// valid transport, a host or a port.
var ErrIncompleteCanonicalEndpoint = errors.New("canonical endpoint: a transport, a host and a port 1-65535 are required")

// CanonicalEndpoint is a dialled endpoint in the one form an endpoint binding
// is keyed by: two spellings of the same endpoint — letter case, IPv6 zero
// compression, an IPv4-mapped IPv6 address, a trailing dot — are one value,
// so they share one binding. The fields are unexported so that a value is
// only made by NewCanonicalEndpoint from a host already in canonical form;
// node.canonicalEndpoint is where that form is computed.
type CanonicalEndpoint struct {
	transport EndpointTransport
	host      string
	port      uint16
}

// NewCanonicalEndpoint assembles an endpoint from a host the caller has
// already brought to canonical form.
func NewCanonicalEndpoint(transport EndpointTransport, host string, port uint16) (CanonicalEndpoint, error) {
	if !transport.Valid() || host == "" || port == 0 {
		return CanonicalEndpoint{}, ErrIncompleteCanonicalEndpoint
	}
	return CanonicalEndpoint{transport: transport, host: host, port: port}, nil
}

// Transport is how the endpoint is reached.
func (e CanonicalEndpoint) Transport() EndpointTransport { return e.transport }

// Host is the canonical host: an IP literal (an IPv6 one with its zone), a
// lower-case DNS name, onion or i2p name.
func (e CanonicalEndpoint) Host() string { return e.host }

// Port is the endpoint's port.
func (e CanonicalEndpoint) Port() uint16 { return e.port }

// Address is the endpoint as a dialable host:port. The transport follows from
// the host, so the address alone names the endpoint; it is the key the store
// file holds.
func (e CanonicalEndpoint) Address() PeerAddress {
	return PeerAddress(net.JoinHostPort(e.host, strconv.FormatUint(uint64(e.port), 10)))
}

// String is Address, for log lines.
func (e CanonicalEndpoint) String() string { return string(e.Address()) }

// SourceGroup is the network group a source belongs to, for quotas counted
// per group: an IPv4 /16, an IPv6 /32, all of loopback, the Tor and the I2P
// overlays. One function computes it for the whole tree (node.sourceGroup).
type SourceGroup string

const (
	// SourceGroupLoopback is the whole loopback range, 127.0.0.0/8 and ::1:
	// every inbound onion connection arrives from it, so it is one group with
	// its own quota instead of passing for many.
	SourceGroupLoopback SourceGroup = "loopback"
	// SourceGroupTor is every onion endpoint.
	SourceGroupTor SourceGroup = "tor"
	// SourceGroupI2P is every i2p endpoint.
	SourceGroupI2P SourceGroup = "i2p"
	// SourceGroupUnresolvedName is every DNS-name endpoint whose connected
	// address is not known — a binding written before groups existed, or a
	// socket without an IP peer. One shared group, so names whose address is
	// unknown never pass for many groups.
	SourceGroupUnresolvedName SourceGroup = "unresolved_name"
)

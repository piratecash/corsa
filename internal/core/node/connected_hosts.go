package node

// ConnectedHosts is one snapshot of the hosts (IP addresses or hostnames)
// this node holds a peer connection with, split by which side chose the
// connection. The split exists because the two dial filters that read it
// must not read the same set:
//
//   - the exact-host gate (PeerProvider.Candidates step 6d, peerDialCandidates)
//     skips a host this node already has any peer connection with — Holds;
//   - the subnet-diversity filter (step 9) spreads this node's OUTBOUND
//     slots over subnet groups, so only connections this node chose occupy a
//     group — ForEachOutbound. An inbound connection is chosen by the remote
//     side: counting it would let any remote that authenticates from an
//     address in a group stop this node from dialling every peer in that
//     group.
//
// Hosts are kept as strings rather than domain.PeerIP because outbound
// sessions may run to non-IP hosts (.onion, .i2p, hostnames). The zero value
// is an empty snapshot; a snapshot is read-only once built.
type ConnectedHosts struct {
	outbound             map[string]struct{}
	authenticatedInbound map[string]struct{}
}

// newConnectedHosts returns an empty snapshot ready to be filled.
func newConnectedHosts() ConnectedHosts {
	return ConnectedHosts{
		outbound:             make(map[string]struct{}),
		authenticatedInbound: make(map[string]struct{}),
	}
}

// addOutbound records a host this node dialled (an outbound session).
func (h ConnectedHosts) addOutbound(host string) {
	h.outbound[host] = struct{}{}
}

// addAuthenticatedInbound records the transport host of an inbound
// connection that completed authentication.
func (h ConnectedHosts) addAuthenticatedInbound(host string) {
	h.authenticatedInbound[host] = struct{}{}
}

// Holds reports whether this node has a peer connection with host in either
// direction.
func (h ConnectedHosts) Holds(host string) bool {
	if _, ok := h.outbound[host]; ok {
		return true
	}
	_, ok := h.authenticatedInbound[host]
	return ok
}

// ForEachOutbound calls fn for every host this node chose to connect to.
func (h ConnectedHosts) ForEachOutbound(fn func(host string)) {
	for host := range h.outbound {
		fn(host)
	}
}

package node

import (
	"github.com/piratecash/corsa/internal/core/netcore"
)

// connEntry is the single-source-of-truth record the Service keeps per
// registered net.Conn. It consolidates what checkpoint 9.3 previously
// spread across four parallel maps (inboundConns / inboundMetered /
// inboundTracked / inboundNetCores). The lifecycle invariant is pinned
// in doc §2.6.7: all fields are created in registerInboundConn /
// attachOutboundNetCore, mutated (tracked flag) by trackInboundConnect /
// trackInboundDisconnect, and invalidated atomically by
// unregisterInboundConn — there is exactly one delete call site.
//
// core is always non-nil for a registered conn.
// metered is nil for conns that are not wrapped in MeteredConn (e.g.
// outbound dials that do not measure bytes).
//
// tracked is set to true after the auth-complete / auth-not-required
// promotion in trackInboundConnect, and flipped back to false by
// trackInboundDisconnect. Deletion of the whole entry happens only in
// unregisterInboundConn, keeping the four legacy removals coherent.
type connEntry struct {
	core    *netcore.NetCore
	metered *netcore.MeteredConn
	tracked bool
}

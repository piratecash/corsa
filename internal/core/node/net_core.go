package node

import (
	"github.com/piratecash/corsa/internal/core/netcore"
)

// connEntry is the single-source-of-truth record the Service keeps per
// registered connection, stored in Service.conns under its netcore.ConnID.
// Its lifecycle is owned entirely by the helpers in conn_registry.go:
//
//   - registerInboundConnLocked creates the entry for an accepted inbound
//     conn, populating core and (optionally) metered.
//   - attachOutboundCoreLocked creates the entry for a dialed outbound
//     conn after the NetCore wrap completes.
//   - trackInboundConnect flips tracked to true once the peer has passed
//     auth-complete or auth-not-required promotion.
//   - trackInboundDisconnect flips tracked back to false on peer teardown.
//   - unregisterConnLocked is the single delete call site: it removes the
//     entry from both Service.conns and the secondary connIDByNetConn
//     index in one step so no accessor can observe a half-invalidated
//     state.
//
// Field semantics:
//   - core is always non-nil for a registered conn.
//   - metered is nil for conns that are not wrapped in MeteredConn (e.g.
//     outbound dials that do not measure bytes).
//   - tracked is the auth-promotion flag described above.
type connEntry struct {
	core    *netcore.NetCore
	metered *netcore.MeteredConn
	tracked bool
}

package ebus

// Topic constants for the node event bus.
//
// Naming convention: "<domain>.<entity>.<action>" (dot-separated, lowercase).
// Each topic documents its handler signature so subscribers can be type-checked
// at review time (Go reflection does not enforce signatures at compile time).

const (
	// TopicPeerConnected is emitted when a peer completes the handshake and
	// becomes active.
	//
	// Handler signature:
	//   func(address domain.PeerAddress, identity domain.PeerIdentity)
	TopicPeerConnected = "peer.connected"

	// TopicPeerDisconnected is emitted when a previously active peer session
	// terminates (clean close, error, or stale replacement).
	//
	// Handler signature:
	//   func(address domain.PeerAddress, identity domain.PeerIdentity)
	TopicPeerDisconnected = "peer.disconnected"

	// TopicAggregateStatusChanged is emitted when the node's aggregate
	// network health status changes (e.g. healthy → limited, peer counts
	// shift). Carries a full snapshot so subscribers don't need to query.
	//
	// Handler signature:
	//   func(snap domain.AggregateStatusSnapshot)
	TopicAggregateStatusChanged = "aggregate.status.changed"

	// TopicVersionPolicyChanged is emitted when the node's version policy
	// snapshot is recomputed (new incompatible-version reporter, peer
	// disconnect affecting build signal, periodic TTL expiry, lockout
	// change). Carries the full snapshot so the receiver can overwrite
	// the cached value without RPC.
	//
	// Handler signature:
	//   func(snap domain.VersionPolicySnapshot)
	TopicVersionPolicyChanged = "version.policy.changed"

	// TopicPeerHealthChanged is emitted when a peer's health record is
	// updated (state transition, score change, ping/pong, connect/disconnect).
	// Carries a PeerHealthDelta struct with all discretely-changing fields
	// so the receiver can apply the delta without RPC callback.
	//
	// Handler signature:
	//   func(delta ebus.PeerHealthDelta)
	TopicPeerHealthChanged = "peer.health.changed"

	// TopicMessageNew is emitted when a new direct message is stored in the
	// chatlog. Replaces the old LocalChangeNewMessage channel event.
	//
	// Handler signature:
	//   func(event protocol.LocalChangeEvent)
	TopicMessageNew = "message.new"

	// TopicReceiptUpdated is emitted when a delivery receipt status changes.
	// Replaces the old LocalChangeReceiptUpdate channel event.
	//
	// Handler signature:
	//   func(event protocol.LocalChangeEvent)
	TopicReceiptUpdated = "receipt.updated"

	// TopicMessageControl is emitted when a control DM
	// (message_delete, message_delete_ack, ...) arrives on the dedicated
	// control wire topic (protocol.TopicControlDM). Unlike TopicMessageNew,
	// the event carries no chatlog row — the wire-encrypted Body is delivered
	// straight to subscribers, who decrypt it via
	// DMCrypto.DecryptIncomingControlMessage and dispatch on the inner
	// DMCommand. See docs/dm-commands.md.
	//
	// Handler signature:
	//   func(event protocol.LocalChangeEvent)
	TopicMessageControl = "message.control"

	// TopicMessageDeleteCompleted is emitted by DMRouter when an
	// in-flight message_delete reaches a terminal state — either the
	// peer's message_delete_ack arrived with one of the four
	// MessageDeleteStatus values, or the sender's retry budget was
	// exhausted (Abandoned). Subscribers use this to differentiate a
	// successful deletion (deleted / not_found) from a peer rejection
	// (denied / immutable) or transport abandonment, all of which look
	// identical to the synchronous SendMessageDelete return.
	//
	// Handler signature:
	//   func(outcome ebus.MessageDeleteOutcome)
	TopicMessageDeleteCompleted = "message.delete.completed"

	// TopicIdentityAdded is emitted when a new identity is discovered and
	// added to the node's known set. Carries the peer identity so the
	// receiver can append it locally without an RPC round-trip.
	//
	// Handler signature:
	//   func(identity domain.PeerIdentity)
	TopicIdentityAdded = "identity.added"

	// TopicContactAdded is emitted when a trusted contact is added or
	// updated. Carries all contact fields so the receiver can upsert
	// locally without an RPC round-trip.
	//
	// Handler signature:
	//   func(c ebus.ContactAddedEvent)
	TopicContactAdded = "contact.added"

	// TopicContactRemoved is emitted when a trusted contact is deleted.
	// Carries the removed peer identity so the receiver can delete it
	// locally.
	//
	// Handler signature:
	//   func(identity domain.PeerIdentity)
	TopicContactRemoved = "contact.removed"

	// TopicMessageSent is emitted after a DM is successfully sent.
	// Emitted by DMRouter.SendMessage after the message is persisted.
	//
	// Handler signature:
	//   func(result ebus.MessageSentResult)
	TopicMessageSent = "message.sent"

	// TopicMessageSendFailed is emitted when a DM send attempt fails.
	// Emitted by DMRouter.SendMessage on SendDirectMessage error.
	//
	// Handler signature:
	//   func(result ebus.MessageSendFailedResult)
	TopicMessageSendFailed = "message.send.failed"

	// TopicFileSent is emitted after a file announce is successfully sent.
	// Emitted by DMRouter.SendFileAnnounce after PrepareAndSend succeeds.
	//
	// Handler signature:
	//   func(result ebus.FileSentResult)
	TopicFileSent = "file.sent"

	// TopicFileSendFailed is emitted when a file announce fails.
	// Emitted by DMRouter.SendFileAnnounce on PrepareAndSend error
	// or when the target peer is removed during in-flight send.
	//
	// Handler signature:
	//   func(result ebus.FileSendFailedResult)
	TopicFileSendFailed = "file.send.failed"

	// TopicSlotStateChanged is emitted by ConnectionManager when a peer
	// slot transitions between states (queued → dialing → active →
	// reconnecting → retry_wait). Carries the overlay address and new
	// state string. Published outside cm.mu so subscribers can safely
	// acquire other locks.
	//
	// Handler signature:
	//   func(address domain.PeerAddress, slotState string)
	TopicSlotStateChanged = "slot.state.changed"

	// TopicRouteTableChanged is emitted when the routing table is modified:
	// direct peer added/removed, incoming route announcement accepted, or
	// transit routes invalidated on disconnect. Carries a lightweight
	// summary so subscribers can decide whether to refresh their view.
	// For full table state, subscribers call RoutingSnapshot().
	//
	// Handler signature:
	//   func(summary ebus.RouteTableChange)
	TopicRouteTableChanged = "route.table.changed"

	// TopicPeerPendingChanged is emitted when the per-peer pending frame
	// queue mutates (frame queued, flushed, or expired). Carries the peer
	// address and the new queue length so subscribers can update the
	// displayed pending count without an RPC round-trip.
	//
	// Handler signature:
	//   func(delta ebus.PeerPendingDelta)
	TopicPeerPendingChanged = "peer.pending.changed"

	// TopicPeerTrafficUpdated is emitted periodically (every bootstrapLoop
	// tick, ~2s) with a batch of cumulative byte counters for all peers
	// whose traffic changed since the last tick. Combines persisted totals
	// (health.BytesSent/BytesReceived) with live session counters.
	// Published as a single batch so subscribers apply all deltas under
	// one lock acquisition and issue one UI notification.
	//
	// Handler signature:
	//   func(batch ebus.PeerTrafficBatch)
	TopicPeerTrafficUpdated = "peer.traffic.updated"

	// TopicCaptureSessionStarted is emitted when a traffic capture session
	// begins for a specific conn_id — either through an explicit
	// record_peer_traffic_* RPC or through a matching standing rule firing
	// on a new connection. Carries enough metadata for the receiver to
	// populate Recording* fields on the matching PeerHealth row without an
	// RPC round-trip.
	//
	// NodeStatusMonitor subscribes to this topic to keep the UI "recording"
	// dot and "Stop all recordings" banner live between startup probes —
	// the probe-driven contract was not enough once the monitor stopped
	// polling fetchPeerHealth.
	//
	// Handler signature:
	//   func(ev ebus.CaptureSessionStarted)
	TopicCaptureSessionStarted = "capture.session.started"

	// TopicCaptureSessionStopped is emitted when a traffic capture session
	// ends — stop_peer_traffic_recording RPC, writer error eviction, or
	// the owning connection closing. Carries the conn_id plus any terminal
	// diagnostic state (writer error, dropped event counter) so the UI can
	// surface failure information without losing visibility the moment the
	// session is torn down.
	//
	// Handler signature:
	//   func(ev ebus.CaptureSessionStopped)
	TopicCaptureSessionStopped = "capture.session.stopped"
)

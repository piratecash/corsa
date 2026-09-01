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

	// TopicMessageEmitted is emitted the first time a sink confirms taking
	// one of this node's own outgoing messages — the queued → sent
	// transition, and nothing else.
	//
	// It fires for EVERY outgoing DM, not only the ones the reachability
	// gate held back: the sender is shown "queued" from the moment the
	// message is stored, because at that point the sinks are still working
	// and nothing yet knows the frame went anywhere. It fires once per
	// message — a later re-send of the same envelope is not news — unless
	// the bus sheds the event, in which case the claim is returned and the
	// next confirmed emission repeats it.
	//
	// It is separate from TopicReceiptUpdated because no peer has spoken:
	// the fact being reported is entirely local. Subscribers apply it the
	// way they apply a receipt — as a monotonic delivery-status update —
	// and it loses to anything the recipient has actually confirmed.
	//
	// Handler signature:
	//   func(event protocol.LocalChangeEvent)
	TopicMessageEmitted = "message.emitted"

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
	// (denied / immutable) or an expired request, all of which look
	// identical on the wire.
	//
	// A route that owes the peer nothing — a local incoming delete, or a
	// recall of a message the node proved never went out — publishes
	// "deleted" here before SendMessageDelete returns. The scheduled
	// routes publish nothing then: the deletion is not finished, and the
	// outcome arrives with the peer's ack or with the intent's expiry,
	// possibly days later.
	//
	// Handler signature:
	//   func(outcome ebus.MessageDeleteOutcome)
	TopicMessageDeleteCompleted = "message.delete.completed"

	// TopicConversationDeleteCompleted is emitted by DMRouter TWICE for one
	// "delete chat for everyone", because the gesture finishes in two
	// places and the user has to be told about both:
	//
	//   - at click time (Settled == false): the local thread is gone and
	//     the peer has been asked to clear theirs. Deleted counts the rows
	//     removed here; Requested says a request now exists — true even
	//     for an empty thread, which is the case the repair path is for.
	//     LocalCleanupFailed says the wipe did not run at all: it is
	//     all-or-nothing, so the thread is untouched here AND nothing was
	//     recorded for the peer, and the user has to re-issue.
	//   - when the request settles (Settled == true): the peer's answer. A
	//     user who only ever saw the first event could not tell a finished
	//     wipe from one still waiting. There is no third, "gave up" event:
	//     the request is never written off, because erased-here-but-not-there
	//     with nobody left to ask is the state this must not produce.
	//
	// The payload contract lives on the struct — see
	// ebus.ConversationDeleteOutcome.
	//
	// Handler signature:
	//   func(outcome ebus.ConversationDeleteOutcome)
	TopicConversationDeleteCompleted = "conversation.delete.completed"

	// TopicReactionsChanged is emitted when a peer's reaction facts have been
	// merged into local state. It names the CONVERSATION and nothing else:
	// the reader already holds the whole conversation's reactions in memory
	// (one query per open chat, not one per bubble), so the useful message is
	// "reload that one conversation" rather than a delta it would have to
	// apply in the same order the database did.
	//
	// Handler signature:
	//   func(peer domain.PeerIdentity)
	TopicReactionsChanged = "reactions.changed"

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

	// TopicFileReceived is emitted by DMRouter.tryRegisterFileReceive
	// after a receiver-side mapping has been registered for an
	// incoming file_announce DM. This fires for EVERY inbound file
	// announce regardless of whether the recipient's chat is the
	// active conversation — the desktop file tab subscribes to keep
	// its history view current for background-conversation arrivals.
	//
	// The publisher de-duplicates by relying on
	// FileTransferManager.RegisterFileReceive's idempotency: a
	// re-registration of the same FileID still publishes the event,
	// so subscribers must treat it as "snapshot may have changed"
	// rather than "this is a brand-new transfer".
	//
	// Handler signature:
	//   func(result ebus.FileReceivedResult)
	TopicFileReceived = "file.received"

	// TopicFileDownloadCompleted is emitted exactly once per successful
	// receiver-side verification, immediately after the mapping has
	// transitioned into receiverWaitingAck and the verified file is
	// durably stored at its CompletedPath. Sourced from
	// FileTransferManager via the OnReceiverDownloadComplete callback
	// wired in node.Service.initFileTransfer.
	//
	// Subscribers use this to surface a "download finished" UI cue
	// (e.g. the desktop layer plays download-done.mp3). The event does
	// NOT fire on the symmetric sender-side completion (file_downloaded_ack
	// arrival) — that path is covered by inspecting AllTransfersSnapshot
	// state changes directly.
	//
	// Handler signature:
	//   func(result ebus.FileDownloadCompletedResult)
	TopicFileDownloadCompleted = "file.download.completed"

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
	// The mutation-time reasons fire while the cached routing snapshot is
	// still the PREVIOUS generation. A subscriber that reads
	// RoutingSnapshot() must reconcile on Reason == RouteChangeSnapshot —
	// emitted strictly after the fresh snapshot is stored — and on nothing
	// else, or it caches stale data until the next unrelated route event.
	//
	// Handler signature:
	//   func(summary ebus.RouteTableChange)
	TopicRouteTableChanged = "route.table.changed"

	// TopicIdentityPresenceChanged reports an observed final-route loss. Direct
	// peers are emitted by their attributed session-removal lifecycle; transit
	// identities are emitted after a fresh routing snapshot is stored. It is
	// offline-only: online reachability already belongs to TopicRouteTableChanged.
	// A Source field isolates observers that share one Bus.
	//
	// Handler signature:
	//   func(change ebus.IdentityPresenceChange)
	TopicIdentityPresenceChanged = "identity.presence.changed"

	// TopicIdentityPresenceObserved reports positive evidence that an identity
	// was up: today, a DM its sender delivered over its OWN authenticated
	// session, timestamped by the observing node's clock. It is deliberately
	// separate from TopicIdentityPresenceChanged, which means one specific
	// thing — the final route to an identity was lost — and whose contract the
	// routing documentation leans on. Subscribers apply the timestamp
	// monotonically and touch nothing else; reachability still has exactly one
	// writer in TopicRouteTableChanged.
	//
	// Handler signature:
	//   func(change ebus.IdentityPresenceChange)
	TopicIdentityPresenceObserved = "identity.presence.observed"

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

const (
	// TopicIdentityResolutionChanged is emitted by the identity lookup
	// engine on every state change of a resolution: lifecycle transitions,
	// the interactive_timeout / no_route progress flags, authority and
	// usable flips. Carries the full state; the node additionally retains
	// the last state per resolution for resolve_identity_status, so a lost
	// event is recoverable by polling.
	//
	// Handler signature:
	//   func(state ebus.IdentityResolutionState)
	TopicIdentityResolutionChanged = "identity.resolution.changed"
)

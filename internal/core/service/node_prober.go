package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// NodeProber is the read-side adapter between the desktop UI and the
// embedded node. It was split out of the former DesktopClient so the
// periodic "poll node, build NodeStatus" workflow has a dedicated surface
// that does not also own encryption, chatlog persistence, or configuration.
//
// Responsibilities:
//
//   - ProbeNode orchestrates the welcome + parallel data fetches that the
//     status monitor uses to build a NodeStatus snapshot every tick.
//
//   - FetchContacts / FetchKnownIDs / FetchPeerHealth / FetchMessage* are
//     explicit read endpoints that DMRouter and NodeStatusMonitor call when
//     the cached probe snapshot does not cover their need (e.g. a follow-up
//     fetch after a routing-table update).
//
//   - DeleteContact is the only write surface here — it forwards the
//     "delete_trusted_contact" command to the node. It belongs to the
//     prober because the contact-trust store is part of the same read/write
//     conversation that ProbeNode maintains; co-locating the delete keeps
//     the contact CRUD boundary single-purpose.
//
//   - BuildReachableIDs snapshots the routing table directly when the node
//     is embedded, or falls back to "fetch_reachable_ids" via RPC.
//
// NodeProber depends on DMCrypto only for the inbound DM-header contact
// import path (ProbeNode detects missing contacts for incoming DM senders
// and asks DMCrypto to import them). All other DM operations stay inside
// DMCrypto.
type NodeProber struct {
	rpc      *LocalRPCClient
	dmCrypto *DMCrypto
	info     AppInfo
}

// NewNodeProber wires the dependencies NodeProber needs: the RPC transport,
// DMCrypto for the header-import fallback inside ProbeNode, and AppInfo for
// identity + version metadata used in the hello frame.
func NewNodeProber(rpc *LocalRPCClient, dmCrypto *DMCrypto, info AppInfo) *NodeProber {
	return &NodeProber{rpc: rpc, dmCrypto: dmCrypto, info: info}
}

// ProbeNode performs a full status handshake against the embedded node and
// returns a populated NodeStatus snapshot.
//
// All RPC calls use the caller-supplied context so that context cancellation
// / deadline propagates. A single stuck handler cannot block the entire
// chain indefinitely — every fetch is bounded by ctx and a failure short-
// circuits the probe to preserve the previous cadence.
//
// The fetch set is intentionally minimal: only the data that the UI and
// DMRouter actually consume is fetched. Everything else (pending messages,
// notices, per-topic message IDs, peer lists) is requested on demand via
// the dedicated Fetch* helpers.
func (p *NodeProber) ProbeNode(ctx context.Context) NodeStatus {
	status := NodeStatus{
		Address: p.rpc.LocalAddress(),
	}

	if status.Address == "" {
		status.Error = "no target address configured"
		status.CheckedAt = time.Now()
		return status
	}

	id := p.info.Identity()
	welcome, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "desktop",
		ClientVersion: strings.ReplaceAll(p.info.Version(), " ", "-"),
		ClientBuild:   config.ClientBuild,
	})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	status.Welcome = welcome.Type
	status.NodeID = welcome.Address
	status.NodeType = welcome.NodeType
	status.ListenerEnabled = strings.TrimSpace(welcome.Listener) == "1"
	status.ListenerAddress = strings.TrimSpace(welcome.Listen)
	status.ClientVersion = welcome.ClientVersion
	status.Services = welcome.Services
	status.Capabilities = welcome.Capabilities

	idsReply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_identities"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	contactsReply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	peerHealthReply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_peer_health"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	aggregateStatusReply, aggregateStatusErr := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_aggregate_status"})
	dmHeadersReply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_dm_headers"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	receiptsReply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_delivery_receipts", Recipient: id.Address})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	ids := idsReply.Identities
	contacts := contactsFromFrame(contactsReply)
	dmHeaders := dmHeadersFromFrame(dmHeadersReply)
	deliveryReceipts := receiptRecordsFromFrames(receiptsReply.Receipts)

	// Check for missing contacts from DM headers (lightweight, no decryption needed).
	if missing := missingDMHeaderContacts(id.Address, contacts, dmHeaders); len(missing) > 0 {
		refreshedContactsReply, refreshErr := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_contacts"})
		if refreshErr == nil {
			networkContacts := contactsFromFrame(refreshedContactsReply)
			// Auto-import new contacts from DM headers via DMCrypto — the
			// encryption surface owns the contact-import conversation.
			if imported := p.dmCrypto.importIncomingDMHeaderContacts(contacts, networkContacts, dmHeaders); imported > 0 {
				trustedContactsReply, trustedErr := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_trusted_contacts"})
				if trustedErr == nil {
					contacts = contactsFromFrame(trustedContactsReply)
				}
			}
		}
	}

	status.Connected = true
	status.KnownIDs = ids
	status.Contacts = contacts
	status.PeerHealth = peerHealthFromFrame(peerHealthReply)
	status.CaptureSessions = captureSessionsFromFrame(peerHealthReply)
	resolvedStatus, resolveWarn := resolveAggregateStatus(aggregateStatusReply, aggregateStatusErr)
	if resolveWarn != "" {
		log.Warn().Msg(resolveWarn)
	}
	status.AggregateStatus = resolvedStatus
	status.ReachableIDs = p.BuildReachableIDs()
	status.DMHeaders = dmHeaders
	status.DeliveryReceipts = deliveryReceipts
	status.CheckedAt = time.Now()
	return status
}

// FetchContacts queries the node for the current trusted contacts map.
// Used by ProbeNode during startup and by DMRouter on TopicContactAdded
// / TopicContactRemoved when the cached snapshot needs a refresh.
func (p *NodeProber) FetchContacts(ctx context.Context) (map[string]Contact, error) {
	reply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return nil, err
	}
	return contactsFromFrame(reply), nil
}

// FetchKnownIDs queries the node for the current identity list.
// Used by ProbeNode during startup and by DMRouter on TopicIdentityAdded.
func (p *NodeProber) FetchKnownIDs(ctx context.Context) ([]string, error) {
	reply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_identities"})
	if err != nil {
		return nil, err
	}
	return reply.Identities, nil
}

// FetchPeerHealth queries the node for the current peer health snapshot.
// Used by ProbeNode during startup. Real-time updates use
// TopicPeerHealthChanged with per-peer deltas.
func (p *NodeProber) FetchPeerHealth(ctx context.Context) ([]PeerHealth, error) {
	reply, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_peer_health"})
	if err != nil {
		return nil, err
	}
	return peerHealthFromFrame(reply), nil
}

// FetchMessageIDs returns the message IDs stored for topic. Used by the
// console "list messages" command.
func (p *NodeProber) FetchMessageIDs(ctx context.Context, topic string) ([]string, error) {
	frame, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_message_ids", Topic: strings.TrimSpace(topic)})
	if err != nil {
		return nil, err
	}
	return frame.IDs, nil
}

// FetchMessage returns a single persisted message by (topic, id). The
// message body is raw ciphertext — decryption is the caller's
// responsibility (typically DMCrypto for dm-topic messages).
func (p *NodeProber) FetchMessage(ctx context.Context, topic, messageID string) (MessageRecord, error) {
	frame, err := p.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_message", Topic: strings.TrimSpace(topic), ID: strings.TrimSpace(messageID)})
	if err != nil {
		return MessageRecord{}, err
	}
	if frame.Item == nil {
		return MessageRecord{}, fmt.Errorf("message item is missing")
	}
	return messageRecordFromFrame(*frame.Item)
}

// SubscribeLocalChanges returns a receive-only channel of local-change
// events and a cancel func. When no embedded node is configured the
// returned channel is already closed, mirroring the legacy DesktopClient
// behaviour so DMRouter's subscribe loop terminates cleanly in
// standalone-RPC mode.
func (p *NodeProber) SubscribeLocalChanges() (<-chan protocol.LocalChangeEvent, func()) {
	node := p.rpc.LocalNode()
	if node == nil {
		ch := make(chan protocol.LocalChangeEvent)
		close(ch)
		return ch, func() {}
	}
	return node.SubscribeLocalChanges()
}

// DeleteContact removes a trusted contact from the node's trust store.
// The contact will no longer appear in Contacts on the next ProbeNode cycle.
func (p *NodeProber) DeleteContact(identity domain.PeerIdentity) error {
	_, err := p.rpc.LocalRequestFrame(protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: string(identity),
	})
	return err
}

// BuildReachableIDs returns the set of identities that have at least one
// live route in the routing table.
//
// Embedded mode: reads directly from node.RoutingSnapshot (no wire round
// trip). Remote-RPC mode: falls back to "fetch_reachable_ids" over RPC.
// Returns nil if the node is unreachable — callers must treat nil as
// "unknown" rather than "no reachable peers".
func (p *NodeProber) BuildReachableIDs() map[domain.PeerIdentity]bool {
	if node := p.rpc.LocalNode(); node != nil {
		return reachableFromSnapshot(node.RoutingSnapshot())
	}
	reply, err := p.rpc.LocalRequestFrame(protocol.Frame{Type: "fetch_reachable_ids"})
	if err != nil || reply.Type == "error" {
		return nil
	}
	reachable := make(map[domain.PeerIdentity]bool, len(reply.Identities))
	for _, id := range reply.Identities {
		reachable[domain.PeerIdentity(id)] = true
	}
	return reachable
}

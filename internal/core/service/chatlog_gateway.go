package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

// ChatlogGateway is the owner of the persistent chat history store. It was
// extracted from the former DesktopClient so the chatlog ownership and all
// reads/writes against it live behind a single typed surface.
//
// Responsibilities:
//
//   - Holds the *chatlog.Store built by the composition root. The database
//     itself is owned by internal/core/storage, so the gateway has no Close.
//   - Provides history read APIs consumed by the UI (FetchChatlog,
//     FetchChatlogPreviews, FetchConversations, HasEntryInConversation).
//   - Exposes the low-level store handle to sub-services that need it for
//     decryption / reconciliation (DMCrypto, NodeProber) via Store().
//
// ChatlogGateway does no network I/O. Anything that needs an RPC call lives
// on NodeProber / LocalRPCClient.
type ChatlogGateway struct {
	store    *chatlog.Store
	selfAddr domain.PeerIdentity
}

// NewChatlogGateway wraps an already built chatlog repository. The store is
// created by the composition root from the shared state database, so the
// gateway neither opens nor closes anything.
//
// A nil *chatlog.Store is allowed for code paths that run without local
// persistence (standalone RPC tests) — all gateway methods degrade to
// "chatlog not available" errors in that mode.
func NewChatlogGateway(store *chatlog.Store, selfAddr domain.PeerIdentity) *ChatlogGateway {
	return &ChatlogGateway{
		store:    store,
		selfAddr: selfAddr,
	}
}

// Store returns the raw chatlog store handle for sub-services that need
// direct access (e.g. DMCrypto calls HasEntryInConversation during reply
// validation). Returns nil when the gateway is unavailable.
func (g *ChatlogGateway) Store() *chatlog.Store {
	if g == nil {
		return nil
	}
	return g.store
}

// SelfAddress returns the identity under which the gateway was opened.
// Useful when sub-services need to filter incoming/outgoing messages.
func (g *ChatlogGateway) SelfAddress() domain.PeerIdentity {
	if g == nil {
		return domain.PeerIdentity{}
	}
	return g.selfAddr
}

// BackfillEstablished seeds the monotonic established facts from history the
// peer_established table predates. Idempotent; the composition root runs it
// once per start.
func (g *ChatlogGateway) BackfillEstablished(ctx context.Context, now time.Time) error {
	if g == nil || g.store == nil {
		return nil
	}
	return g.store.BackfillEstablishedFromHistory(ctx, now)
}

// HasEntryInConversation reports whether a message with the given ID exists
// in the conversation with peerAddress. Returns false when the gateway has
// no store (standalone node mode).
func (g *ChatlogGateway) HasEntryInConversation(ctx context.Context, peerAddress, messageID string) bool {
	if g == nil || g.store == nil {
		return false
	}
	return g.store.HasEntryInConversation(ctx, domain.PeerIdentityFromWire(peerAddress), domain.MessageID(messageID))
}

// LookupEntryInConversation is HasEntryInConversation for callers that must
// tell "the row is absent" from "the lookup failed" — the RPC validation of
// reply_to, which otherwise told the client their message did not exist
// whenever the context was cancelled or the database was unhealthy.
//
// No store (standalone node mode) is a definitive miss, not a failure: there
// is no conversation history to contradict.
func (g *ChatlogGateway) LookupEntryInConversation(ctx context.Context, peerAddress, messageID string) (bool, error) {
	if g == nil || g.store == nil {
		return false, nil
	}
	return g.store.LookupEntryInConversation(ctx, domain.PeerIdentityFromWire(peerAddress), domain.MessageID(messageID))
}

// DeletePeerHistory removes all chat messages for the given identity.
func (g *ChatlogGateway) DeletePeerHistory(ctx context.Context, identity domain.PeerIdentity) (int64, error) {
	if g == nil || g.store == nil {
		return 0, nil
	}
	return g.store.DeleteByPeer(ctx, identity)
}

// FetchChatlog reads the chat entries for a peer and returns a JSON payload
// ready for console / RPC consumption. Keeps the JSON-marshalling concern
// inside the gateway so the console command table need not know about the
// underlying store schema.
func (g *ChatlogGateway) FetchChatlog(ctx context.Context, topic, peerAddress string) (string, error) {
	if g == nil || g.store == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	if topic == "" {
		topic = "dm"
	}
	// Distinguish an omitted peer (empty string → zero identity, treated
	// as "no DM filter") from a malformed non-empty one. Best-effort
	// decoding would silently turn a bad peer into the zero identity and
	// fall through to the global topic; reject it explicitly instead.
	var peer domain.PeerIdentity
	if peerAddress != "" {
		parsed, err := domain.ParsePeerIdentity(peerAddress)
		if err != nil {
			return "", fmt.Errorf("invalid peer address %q: %w", peerAddress, err)
		}
		if parsed.IsZero() {
			return "", fmt.Errorf("invalid peer address %q: zero identity", peerAddress)
		}
		peer = parsed
	}
	entries, err := g.store.Read(ctx, topic, peer)
	if err != nil {
		return "", fmt.Errorf("chatlog read: %w", err)
	}
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format chatlog entries: %w", err)
	}
	return string(data), nil
}

// FetchChatlogPreviews reads the last entry per peer and returns a JSON
// payload with preview-sized fields.
func (g *ChatlogGateway) FetchChatlogPreviews(ctx context.Context) (string, error) {
	if g == nil || g.store == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	previews, err := g.store.ReadLastEntryPerPeer(ctx)
	if err != nil {
		return "", fmt.Errorf("chatlog previews: %w", err)
	}
	data, err := json.MarshalIndent(previews, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format chatlog previews: %w", err)
	}
	return string(data), nil
}

// FetchConversations lists all conversations with their message counts.
func (g *ChatlogGateway) FetchConversations(ctx context.Context) (string, error) {
	if g == nil || g.store == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	conversations, err := g.store.ListConversations(ctx)
	if err != nil {
		return "", fmt.Errorf("chatlog conversations: %w", err)
	}
	data, err := json.MarshalIndent(conversations, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format conversations: %w", err)
	}
	return string(data), nil
}

// UndeliveredOutgoing returns the locally-sent DM entries still in the
// "sent" delivery status — the durable source for the sender-side delivery
// retry scheduler.
func (g *ChatlogGateway) UndeliveredOutgoing(ctx context.Context, since time.Time) ([]chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.UndeliveredOutgoing(ctx, g.SelfAddress(), since)
}

// MarkNeverEmitted records that the locally-sent messages have not reached
// the wire, so a restart can still tell "the peer cannot have this" from
// "we cannot know".
func (g *ChatlogGateway) MarkNeverEmitted(ctx context.Context, ids []domain.MessageID) error {
	if g == nil || g.store == nil {
		return fmt.Errorf("chatlog not available")
	}
	return g.store.MarkNeverEmitted(ctx, ids)
}

// ClearNeverEmitted withdraws that claim once the messages go out.
func (g *ChatlogGateway) ClearNeverEmitted(ctx context.Context, ids []domain.MessageID) error {
	if g == nil || g.store == nil {
		return fmt.Errorf("chatlog not available")
	}
	return g.store.ClearNeverEmitted(ctx, ids)
}

// UnconfirmedSeen returns the inbound DM entries marked "seen" whose seen
// receipt the original sender has not confirmed yet (since bounds the scan).
func (g *ChatlogGateway) UnconfirmedSeen(ctx context.Context, since time.Time) ([]chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.UnconfirmedSeen(ctx, g.SelfAddress(), since)
}

// MarkDeliveryFailed durably journals an abandoned delivery retry.
func (g *ChatlogGateway) MarkDeliveryFailed(ctx context.Context, messageID string) error {
	if g == nil || g.store == nil {
		return fmt.Errorf("chatlog not available")
	}
	return g.store.MarkDeliveryFailed(ctx, messageID)
}

// MarkSeenConfirmed durably journals an arrived seen_ack.
func (g *ChatlogGateway) MarkSeenConfirmed(ctx context.Context, messageID string) error {
	if g == nil || g.store == nil {
		return fmt.Errorf("chatlog not available")
	}
	return g.store.MarkSeenConfirmed(ctx, messageID)
}

// Read returns the raw chatlog entries for a conversation using the
// caller's context to bound SQLite I/O. Used by DMCrypto when it needs the
// full history for on-demand decryption.
func (g *ChatlogGateway) Read(ctx context.Context, topic string, peer domain.PeerIdentity) ([]chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.Read(ctx, topic, peer)
}

// ReadLastEntry returns the most recent entry for a conversation or nil
// when the conversation is empty.
func (g *ChatlogGateway) ReadLastEntry(ctx context.Context, topic string, peer domain.PeerIdentity) (*chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.ReadLastEntry(ctx, topic, peer)
}

// ReadLastEntryPerPeer returns the most recent entry for each peer.
func (g *ChatlogGateway) ReadLastEntryPerPeer(ctx context.Context) (map[string]chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.ReadLastEntryPerPeer(ctx)
}

// ListConversations lists all conversations with unread counts bounded
// by the caller's context.
func (g *ChatlogGateway) ListConversations(ctx context.Context) ([]chatlog.ConversationSummary, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.ListConversations(ctx)
}

// AppendReportNew inserts an entry for the given topic and reports whether
// the write was a new record (as opposed to a duplicate ID). Used by
// MessageStoreAdapter when the node hands persistence to the desktop layer.
func (g *ChatlogGateway) AppendReportNew(ctx context.Context, topic string, owner domain.PeerIdentity, entry chatlog.Entry) (bool, error) {
	if g == nil || g.store == nil {
		return false, fmt.Errorf("chatlog not available")
	}
	return g.store.AppendReportNew(ctx, topic, owner, entry)
}

// UpdateStatus advances the delivery_status of a message persisted in the
// chatlog. Used by MessageStoreAdapter when the node forwards a delivery
// receipt.
func (g *ChatlogGateway) UpdateStatus(ctx context.Context, topic string, peer domain.PeerIdentity, messageID domain.MessageID, status string) (bool, error) {
	if g == nil || g.store == nil {
		return false, fmt.Errorf("chatlog not available")
	}
	return g.store.UpdateStatus(ctx, topic, peer, messageID, status)
}

// setStoreForTest replaces the underlying store. Test-only — production
// code must not mutate the store after construction.
func (g *ChatlogGateway) setStoreForTest(s *chatlog.Store) {
	if g == nil {
		return
	}
	g.store = s
}

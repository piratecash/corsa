package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

// ChatlogGateway is the owner of the persistent chat history store. It was
// extracted from the former DesktopClient so the chatlog ownership and all
// reads/writes against it live behind a single typed surface.
//
// Responsibilities:
//
//   - Owns *chatlog.Store (lifecycle: created with NewChatlogGateway, released
//     with Close).
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

// NewChatlogGateway opens (or attaches to) the SQLite-backed chatlog store
// located at chatLogDir for the given local identity. The listen address is
// recorded alongside each write for multi-device correlation.
//
// A nil *chatlog.Store is allowed for code paths that run without local
// persistence (standalone RPC tests) — all gateway methods degrade to
// "chatlog not available" errors in that mode.
func NewChatlogGateway(chatLogDir string, selfAddr domain.PeerIdentity, listenAddress domain.ListenAddress) *ChatlogGateway {
	return &ChatlogGateway{
		store:    chatlog.NewStore(chatLogDir, selfAddr, listenAddress),
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
		return ""
	}
	return g.selfAddr
}

// Close releases the underlying SQLite database and all prepared statements.
// Safe to call multiple times.
func (g *ChatlogGateway) Close() error {
	if g == nil || g.store == nil {
		return nil
	}
	return g.store.Close()
}

// HasEntryInConversation reports whether a message with the given ID exists
// in the conversation with peerAddress. Returns false when the gateway has
// no store (standalone node mode).
func (g *ChatlogGateway) HasEntryInConversation(peerAddress, messageID string) bool {
	if g == nil || g.store == nil {
		return false
	}
	return g.store.HasEntryInConversation(domain.PeerIdentity(peerAddress), domain.MessageID(messageID))
}

// DeletePeerHistory removes all chat messages for the given identity.
func (g *ChatlogGateway) DeletePeerHistory(identity domain.PeerIdentity) (int64, error) {
	if g == nil || g.store == nil {
		return 0, nil
	}
	return g.store.DeleteByPeer(identity)
}

// FetchChatlog reads the chat entries for a peer and returns a JSON payload
// ready for console / RPC consumption. Keeps the JSON-marshalling concern
// inside the gateway so the console command table need not know about the
// underlying store schema.
func (g *ChatlogGateway) FetchChatlog(topic, peerAddress string) (string, error) {
	if g == nil || g.store == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	if topic == "" {
		topic = "dm"
	}
	entries, err := g.store.Read(topic, domain.PeerIdentity(peerAddress))
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
func (g *ChatlogGateway) FetchChatlogPreviews() (string, error) {
	if g == nil || g.store == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	previews, err := g.store.ReadLastEntryPerPeer()
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
func (g *ChatlogGateway) FetchConversations() (string, error) {
	if g == nil || g.store == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	conversations, err := g.store.ListConversations()
	if err != nil {
		return "", fmt.Errorf("chatlog conversations: %w", err)
	}
	data, err := json.MarshalIndent(conversations, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format conversations: %w", err)
	}
	return string(data), nil
}

// ReadCtx returns the raw chatlog entries for a conversation using the
// caller's context to bound SQLite I/O. Used by DMCrypto when it needs the
// full history for on-demand decryption.
func (g *ChatlogGateway) ReadCtx(ctx context.Context, topic string, peer domain.PeerIdentity) ([]chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.ReadCtx(ctx, topic, peer)
}

// ReadLastEntryCtx returns the most recent entry for a conversation or nil
// when the conversation is empty.
func (g *ChatlogGateway) ReadLastEntryCtx(ctx context.Context, topic string, peer domain.PeerIdentity) (*chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.ReadLastEntryCtx(ctx, topic, peer)
}

// ReadLastEntryPerPeerCtx returns the most recent entry for each peer.
func (g *ChatlogGateway) ReadLastEntryPerPeerCtx(ctx context.Context) (map[string]chatlog.Entry, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.ReadLastEntryPerPeerCtx(ctx)
}

// ListConversationsCtx lists all conversations with unread counts bounded
// by the caller's context.
func (g *ChatlogGateway) ListConversationsCtx(ctx context.Context) ([]chatlog.ConversationSummary, error) {
	if g == nil || g.store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}
	return g.store.ListConversationsCtx(ctx)
}

// AppendReportNew inserts an entry for the given topic and reports whether
// the write was a new record (as opposed to a duplicate ID). Used by
// MessageStoreAdapter when the node hands persistence to the desktop layer.
func (g *ChatlogGateway) AppendReportNew(topic string, owner domain.PeerIdentity, entry chatlog.Entry) (bool, error) {
	if g == nil || g.store == nil {
		return false, fmt.Errorf("chatlog not available")
	}
	return g.store.AppendReportNew(topic, owner, entry)
}

// UpdateStatus advances the delivery_status of a message persisted in the
// chatlog. Used by MessageStoreAdapter when the node forwards a delivery
// receipt.
func (g *ChatlogGateway) UpdateStatus(topic string, peer domain.PeerIdentity, messageID domain.MessageID, status string) (bool, error) {
	if g == nil || g.store == nil {
		return false, fmt.Errorf("chatlog not available")
	}
	return g.store.UpdateStatus(topic, peer, messageID, status)
}

// setStoreForTest replaces the underlying store. Test-only — production
// code must not mutate the store after construction.
func (g *ChatlogGateway) setStoreForTest(s *chatlog.Store) {
	if g == nil {
		return
	}
	g.store = s
}

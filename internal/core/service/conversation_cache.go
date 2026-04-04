package service

import (
	"sync"
	"time"

	"corsa/internal/core/domain"
)

// ConversationCache holds the decrypted messages for the currently active
// conversation. Messages are loaded from SQLite once when switching peers,
// then updated incrementally via LocalChangeEvent (new messages appended,
// receipt statuses updated in-place).
type ConversationCache struct {
	mu          sync.RWMutex
	peerAddress domain.PeerIdentity
	messages    []DirectMessage
	index       map[string]int // message ID → index in messages slice
}

func NewConversationCache() *ConversationCache {
	return &ConversationCache{
		index: make(map[string]int),
	}
}

// Load is called when switching conversations or doing a full reload from SQLite.
func (c *ConversationCache) Load(peerAddress domain.PeerIdentity, messages []DirectMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peerAddress = peerAddress
	c.messages = make([]DirectMessage, len(messages))
	copy(c.messages, messages)
	c.index = make(map[string]int, len(messages))
	for i, m := range c.messages {
		c.index[m.ID] = i
	}
}

func (c *ConversationCache) PeerAddress() domain.PeerIdentity {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.peerAddress
}

func (c *ConversationCache) Messages() []DirectMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.messages) == 0 {
		return nil
	}
	out := make([]DirectMessage, len(c.messages))
	copy(out, c.messages)
	return out
}

func (c *ConversationCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.messages)
}

// AppendMessage ensures idempotency by message ID: only unique messages are stored.
// Returns true if the message was new, false if it was a duplicate.
func (c *ConversationCache) AppendMessage(msg DirectMessage) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[msg.ID]; exists {
		return false
	}

	c.index[msg.ID] = len(c.messages)
	c.messages = append(c.messages, msg)
	return true
}

// UpdateStatus enforces forward-only transitions (sent→delivered→seen) to maintain
// monotonic delivery status progression. Returns true if updated, false otherwise.
func (c *ConversationCache) UpdateStatus(messageID, status string, deliveredAt *time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx, exists := c.index[messageID]
	if !exists {
		return false
	}

	msg := &c.messages[idx]

	// Enforce monotonic transitions: sent < delivered < seen.
	newRank := statusRank(status)
	oldRank := statusRank(msg.ReceiptStatus)
	if newRank < oldRank {
		return false
	}

	if newRank == oldRank {
		// Same status — allow updating DeliveredAt if the incoming value is
		// a real (non-nil, non-zero) timestamp.  This covers the common case
		// where decryptDirectMessages() synthesized DeliveredAt from the
		// message Timestamp on restart and a real receipt arrives later with
		// the same rank — the synthetic value (which is non-nil, non-zero)
		// gets replaced by the actual receipt time.
		if deliveredAt == nil || deliveredAt.IsZero() {
			return false
		}
		msg.DeliveredAt = deliveredAt
		return true
	}

	msg.ReceiptStatus = status
	if deliveredAt != nil {
		msg.DeliveredAt = deliveredAt
	}
	return true
}

// Evict clears the cache if it currently holds the given identity's conversation.
func (c *ConversationCache) Evict(identity domain.PeerIdentity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.peerAddress == identity {
		c.peerAddress = ""
		c.messages = nil
		c.index = make(map[string]int)
	}
}

func (c *ConversationCache) MatchesPeer(peerAddress domain.PeerIdentity) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.peerAddress == peerAddress
}

func (c *ConversationCache) HasMessage(messageID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.index[messageID]
	return exists
}

func statusRank(status string) int {
	switch status {
	case "sent":
		return 1
	case "delivered":
		return 2
	case "seen":
		return 3
	default:
		return 0
	}
}

package service

import (
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
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

// Load installs a snapshot read from SQLite, RECONCILED against what is
// already in the cache.
//
// It is not a replace, and the difference is the whole point of this
// function. The read takes seconds; the live event stream keeps writing
// here while it runs. So the snapshot is not "the truth" and the cache is
// not "the truth" — each is authoritative about different things, and
// three rounds of review found three separate ways a wholesale replace
// loses:
//
//   - a status that moved forward during the read (queued → sent, or a
//     receipt) was rolled back;
//   - the exact DeliveredAt from a real receipt was replaced by the
//     snapshot's synthetic one at the same rank;
//   - a message stored during the read — including one the user had just
//     sent — vanished from the open conversation.
//
// The rule, stated once, in the order the reconcile applies it:
//
//   - a different conversation carries nothing over: same id, other peer,
//     different message;
//   - a message in both keeps the HIGHER status by rank, and at equal
//     rank the real DeliveredAt over a synthetic one — the same
//     comparison UpdateStatus makes, for the same reason;
//   - a message the snapshot does not carry is kept only if the read
//     COULD NOT HAVE SEEN IT: no row yet, or a row that appeared after the
//     read began. Anything else the read covered and did not return has
//     been deleted, and resurrecting it would be worse than losing it.
//
// authoritativeUpTo is that boundary: the highest Seq the CALLER knew
// before it started reading. It cannot be derived from the snapshot —
// deleting the newest rows and storing new ones both leave the snapshot
// short, and only the caller's own before-picture separates them. A fresh
// load (a different conversation, or an empty cache) reconciles nothing,
// so 0 is the right value there and the argument is ignored.
func (c *ConversationCache) Load(peerAddress domain.PeerIdentity, messages []DirectMessage, authoritativeUpTo int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	loaded := make([]DirectMessage, len(messages))
	copy(loaded, messages)
	if c.peerAddress == peerAddress && len(c.messages) > 0 {
		loaded = reconcileWithLive(c.messages, c.index, loaded, authoritativeUpTo)
	}

	c.peerAddress = peerAddress
	c.messages = loaded
	c.index = make(map[string]int, len(loaded))
	for i := range c.messages {
		c.index[c.messages[i].ID] = i
	}
}

// reconcileWithLive merges what the live stream has already applied into
// the snapshot the read returned. See Load for the rule.
func reconcileWithLive(prior []DirectMessage, priorIndex map[string]int, loaded []DirectMessage, authoritativeUpTo int64) []DirectMessage {
	inSnapshot := make(map[string]struct{}, len(loaded))
	for i := range loaded {
		inSnapshot[loaded[i].ID] = struct{}{}
	}

	for i := range loaded {
		at, known := priorIndex[loaded[i].ID]
		if !known || at >= len(prior) {
			continue
		}
		mergeDeliveryState(&loaded[i], prior[at])
	}

	// Messages the read could not have seen: stored after its snapshot was
	// taken, or not yet stored at all.
	for i := range prior {
		if _, carried := inSnapshot[prior[i].ID]; carried {
			continue
		}
		if prior[i].Seq != 0 && prior[i].Seq <= authoritativeUpTo {
			// Inside the range the read is authoritative about, and
			// absent from it: the row is gone.
			continue
		}
		loaded = append(loaded, prior[i])
	}
	return loaded
}

// mergeDeliveryState carries the live answer into the loaded one when the
// live answer is further along.
func mergeDeliveryState(into *DirectMessage, live DirectMessage) {
	liveRank, loadedRank := statusRank(live.ReceiptStatus), statusRank(into.ReceiptStatus)
	if liveRank > loadedRank {
		into.ReceiptStatus = live.ReceiptStatus
		if live.DeliveredAt.Valid() {
			into.DeliveredAt = live.DeliveredAt
			into.DeliveredAtFromReceipt = live.DeliveredAtFromReceipt
		}
		return
	}
	if liveRank < loadedRank {
		return
	}
	// Same rank, two timestamps of different KINDS. decryptDirectMessages
	// synthesises DeliveredAt from the message's own creation time when the
	// row carries no receipt, and that value is on the SENDER's clock while
	// a real receipt time is on the RECIPIENT's — so "the later one" is not
	// an answer: a sender running fast makes the synthetic one look newer.
	// The only real receipt time available wins, whichever is larger.
	if live.DeliveredAtFromReceipt && !into.DeliveredAtFromReceipt && live.DeliveredAt.Valid() {
		into.DeliveredAt = live.DeliveredAt
		into.DeliveredAtFromReceipt = true
		return
	}
	// Two REAL receipt times are on the same clock — the recipient's — so
	// here later does mean newer, and a second receipt that arrived during
	// the read must not be rolled back by the snapshot's first one.
	if live.DeliveredAtFromReceipt && into.DeliveredAtFromReceipt &&
		live.DeliveredAt.Valid() && (!into.DeliveredAt.Valid() || into.DeliveredAt.Time().Before(live.DeliveredAt.Time())) {
		into.DeliveredAt = live.DeliveredAt
	}
}

// HighestSeq is the newest row this cache knows about, and the boundary a
// caller passes to Load after a read: everything at or below it the read
// was in a position to see.
func (c *ConversationCache) HighestSeq() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var highest int64
	for i := range c.messages {
		if c.messages[i].Seq > highest {
			highest = c.messages[i].Seq
		}
	}
	return highest
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
// AppendForPeer appends msg only if the cache still belongs to peer, and
// reports whether it did. The pair has to be atomic: checking the owner and
// appending in two acquisitions leaves a window in which the cache is loaded
// for someone else, and the message is spliced into their thread.
func (c *ConversationCache) AppendForPeer(peer domain.PeerIdentity, msg DirectMessage) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.peerAddress != peer {
		return false
	}
	if _, exists := c.index[msg.ID]; exists {
		return true
	}
	c.index[msg.ID] = len(c.messages)
	c.messages = append(c.messages, msg)
	return true
}

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
//
// fromReceipt says whether deliveredAt came from the recipient's receipt
// rather than being synthesised from the message's own creation time. It
// is recorded on the message because a LATER reload has to choose between
// this value and the one the snapshot carries, and those two are on
// different clocks — see mergeDeliveryState. Without it every live receipt
// looked synthetic to that choice, which made the distinction useless
// exactly on the path that produces real ones.
func (c *ConversationCache) UpdateStatus(messageID, status string, deliveredAt domain.OptionalTime, fromReceipt bool) bool {
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
		// a real (valid, non-zero) timestamp. This covers the common case
		// where decryptDirectMessages() synthesized DeliveredAt from the
		// message Timestamp on restart and a real receipt arrives later
		// with the same rank — the synthetic value gets replaced by the
		// actual receipt time.
		if !deliveredAt.Valid() || deliveredAt.Time().IsZero() {
			return false
		}
		msg.DeliveredAt = deliveredAt
		msg.DeliveredAtFromReceipt = fromReceipt
		return true
	}

	msg.ReceiptStatus = status
	msg.DeliveredAtFromReceipt = fromReceipt
	if deliveredAt.Valid() {
		msg.DeliveredAt = deliveredAt
	}
	return true
}

// RemoveMessage drops the message with the given ID from the cache,
// keeping the index for the remaining messages contiguous. Returns
// true when the cache held the message and it was removed; false when
// the ID was not present (idempotent caller path).
//
// Used by the message_delete handlers in DMRouter — after chatlog has
// already removed the row, the live conversation cache must drop it
// too so the deleted bubble disappears from the UI without waiting
// for a conversation reload.
func (c *ConversationCache) RemoveMessage(messageID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx, exists := c.index[messageID]
	if !exists {
		return false
	}

	c.messages = append(c.messages[:idx], c.messages[idx+1:]...)
	delete(c.index, messageID)
	// Compact the index — every entry whose stored offset was beyond
	// the removed slot must shift down by one.
	for id, offset := range c.index {
		if offset > idx {
			c.index[id] = offset - 1
		}
	}
	return true
}

// Evict clears the cache if it currently holds the given identity's conversation.
func (c *ConversationCache) Evict(identity domain.PeerIdentity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.peerAddress == identity {
		c.peerAddress = domain.PeerIdentity{}
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
	case MessageStatusQueued:
		// Below sent on purpose: a queued message has not reached the
		// wire, so every other status is news. Named rather than left to
		// the default so nobody later "fixes" it up to sent's rank, which
		// would freeze the badge on a message that did go out. It shares
		// rank 0 with the unknown/empty case, which is why UpdateStatus
		// will not replace an empty status with queued — nothing does
		// that today, and a caller that wants to must say so explicitly.
		return 0
	case MessageStatusSent:
		return 1
	case MessageStatusDelivered:
		return 2
	case MessageStatusSeen:
		return 3
	default:
		return 0
	}
}

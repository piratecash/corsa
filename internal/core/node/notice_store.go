package node

import (
	"container/heap"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/gazeta"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const (
	maxNoticeTTLSeconds = 60 * 60
	maxNoticeCount      = 512
	// Leave room in fetch_notices for JSON escaping and per-entry fields.
	maxNoticeBytes = 1 << 20
)

// noticeStore is guarded by Service.gossipMu. Its zero value is ready to use.
// Each live entry has one deadline. A duplicate cannot extend that deadline.
type noticeStore struct {
	byID      map[string]gazeta.Notice
	deadlines noticeDeadlines
	bytes     int
}

type noticeDeadline struct {
	id string
	at time.Time
}

type noticeDeadlines []noticeDeadline

func (h noticeDeadlines) Len() int           { return len(h) }
func (h noticeDeadlines) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h noticeDeadlines) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *noticeDeadlines) Push(v any)        { *h = append(*h, v.(noticeDeadline)) }

func (h *noticeDeadlines) Pop() any {
	last := len(*h) - 1
	v := (*h)[last]
	(*h)[last] = noticeDeadline{}
	*h = (*h)[:last]
	return v
}

func (n *noticeStore) expire(now time.Time) {
	for len(n.deadlines) > 0 && !n.deadlines[0].at.After(now) {
		id := heap.Pop(&n.deadlines).(noticeDeadline).id
		n.bytes -= len(n.byID[id].Ciphertext)
		delete(n.byID, id)
	}
}

// put returns the original deadline for a duplicate, or a zero deadline when
// the store is full. Refusing a new entry preserves the existing notices.
func (n *noticeStore) put(now time.Time, id, ciphertext string, ttl time.Duration) (expiresAt time.Time, stored bool) {
	n.expire(now)
	if existing, ok := n.byID[id]; ok {
		return existing.ExpiresAt, false
	}
	if len(n.byID) >= maxNoticeCount || len(ciphertext) > maxNoticeBytes-n.bytes {
		return time.Time{}, false
	}
	if n.byID == nil {
		n.byID = make(map[string]gazeta.Notice)
	}
	expiresAt = now.Add(ttl)
	n.byID[id] = gazeta.Notice{ID: id, Ciphertext: ciphertext, ExpiresAt: expiresAt}
	n.bytes += len(ciphertext)
	heap.Push(&n.deadlines, noticeDeadline{id: id, at: expiresAt})
	return expiresAt, true
}

// acceptNotice is the admission path shared by local publication and gossip.
func (s *Service) acceptNotice(frame protocol.Frame) protocol.Frame {
	if frame.TTLSeconds <= 0 || frame.TTLSeconds > maxNoticeTTLSeconds ||
		len(frame.Ciphertext) > protocol.MaxFrameLine || strings.TrimSpace(frame.Ciphertext) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidPublishNotice}
	}
	ttl := time.Duration(frame.TTLSeconds) * time.Second
	ciphertext := frame.Ciphertext
	id := gazeta.ID(ciphertext)
	s.gossipMu.Lock()
	expiresAt, stored := s.notices.put(time.Now(), id, ciphertext, ttl)
	s.gossipMu.Unlock()
	if expiresAt.IsZero() {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeStoreDeferred}
	}
	if !stored {
		return protocol.Frame{Type: "notice_known", ID: id, ExpiresAt: expiresAt.Unix()}
	}
	if s.CanForward() {
		s.goBackground(func() { s.gossipNotice(ttl, ciphertext) })
	}
	return protocol.Frame{Type: "notice_stored", ID: id, ExpiresAt: expiresAt.Unix()}
}

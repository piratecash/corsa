// Package chatlog is the SQLite-backed repository of chat history.
//
// It owns SQL, row scanning and transactions for its own tables and nothing
// else. The database file, its schema and its lifecycle belong to
// internal/core/storage, which opens the shared state database once at the
// composition root and hands this package a non-owning executor. A schema
// change is therefore a new migration in storage's catalog — this package
// contains no DDL, no driver import and no Close.
//
// Messages are stored as-is: incoming DM bodies are already sealed envelopes,
// and outgoing DMs are encrypted with the sender's own key before storage.
// Reading a chatlog always requires decryption via the identity key.
package chatlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/storage"
)

// Message delivery statuses.
const (
	StatusSent      = "sent"      // outgoing message accepted locally
	StatusDelivered = "delivered" // delivery receipt received from recipient node
	StatusSeen      = "seen"      // recipient opened the conversation
)

// Message flags.
const (
	FlagNone          = ""                // default — no special behavior
	FlagImmutable     = "immutable"       // nobody may delete the message
	FlagSenderDelete  = "sender-delete"   // only the sender may delete it
	FlagAnyDelete     = "any-delete"      // any participant may delete it
	FlagAutoDeleteTTL = "auto-delete-ttl" // auto-deleted after ttl_seconds
)

// Entry is a single chatlog record.
type Entry struct {
	ID             string `json:"id"`
	Sender         string `json:"sender"`
	Recipient      string `json:"recipient"`
	Body           string `json:"body"`
	CreatedAt      string `json:"created_at"`
	Flag           string `json:"flag,omitempty"`
	DeliveryStatus string `json:"delivery_status,omitempty"`
	TTLSeconds     int    `json:"ttl_seconds,omitempty"`
	Metadata       string `json:"metadata,omitempty"` // arbitrary JSON for future extensibility
}

// ConversationSummary holds metadata for a single conversation peer.
type ConversationSummary struct {
	PeerAddress string    `json:"peer_address"`
	LastMessage time.Time `json:"last_message"`
	Count       int       `json:"count"`
	UnreadCount int       `json:"unread_count"`
}

// Store is the chatlog repository. It issues SQL against the shared state
// database but never owns it: opening, migrating and closing the file are the
// composition root's job through internal/core/storage.
type Store struct {
	db           storage.Executor
	identityAddr domain.PeerIdentity // full 40-char identity address
}

// NewStore builds the repository on an already opened and migrated shared
// database.
//
// db must not be nil. Running without persistence is a decision the
// composition root makes by not building a Store at all — a store that
// silently swallows writes is how a node used to come up looking healthy
// while losing every message.
func NewStore(db storage.Executor, identity domain.PeerIdentity) *Store {
	return &Store{db: db, identityAddr: identity}
}

// For DMs the peer is the other party; for global/broadcast use topic "global".
func (s *Store) Append(ctx context.Context, topic string, selfAddress domain.PeerIdentity, entry Entry) error {
	inserted, err := s.AppendReportNew(ctx, topic, selfAddress, entry)
	_ = inserted
	return err
}

// AppendReportNew works like Append but also reports whether the entry was
// actually inserted (true) or already existed (false). This allows callers
// to distinguish genuinely new messages from duplicates that were silently
// ignored by INSERT OR IGNORE, so they can suppress duplicate UI events.
func (s *Store) AppendReportNew(ctx context.Context, topic string, selfAddress domain.PeerIdentity, entry Entry) (bool, error) {
	status := entry.DeliveryStatus
	if status == "" {
		status = StatusSent
	}

	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO messages (id, topic, sender, recipient, body, flag, delivery_status, ttl_seconds, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID, topic, entry.Sender, entry.Recipient, entry.Body,
		entry.Flag, status, entry.TTLSeconds, entry.Metadata, entry.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("chatlog: insert %s: %w", entry.ID, err)
	}
	rows, _ := res.RowsAffected()
	return rows > 0, nil
}

// statusRank maps delivery statuses to their monotonic order.
// UpdateStatus only allows forward transitions: sent→delivered→seen.
var statusRank = map[string]int{
	StatusSent:      0,
	StatusDelivered: 1,
	StatusSeen:      2,
}

// UpdateStatus updates the delivery_status of a message by ID.
// The update is monotonic: a status can only move forward in the
// lifecycle (sent → delivered → seen). Attempts to regress
// (e.g. seen → delivered) are silently ignored and return false.
// Returns true if the message was found and actually updated.
func (s *Store) UpdateStatus(ctx context.Context, topic string, peerAddress domain.PeerIdentity, messageID domain.MessageID, status string) (bool, error) {
	newRank, ok := statusRank[status]
	if !ok {
		return false, fmt.Errorf("chatlog: invalid status %q", status)
	}

	// Build a list of statuses that the new status is allowed to replace.
	// e.g. "delivered" can only replace "sent"; "seen" can replace "sent" or "delivered".
	var allowedPrev []string
	for s, r := range statusRank {
		if r < newRank {
			allowedPrev = append(allowedPrev, s)
		}
	}
	if len(allowedPrev) == 0 {
		// "sent" cannot replace anything — it's the lowest rank.
		return false, nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Monotonic guard: only update if current status has a lower rank.
	// Using IN (?) with explicit values since SQLite doesn't support array params.
	query := `UPDATE messages SET delivery_status = ?, updated_at = ? WHERE id = ? AND delivery_status IN (`
	args := []interface{}{status, now, messageID}
	for i, prev := range allowedPrev {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, prev)
	}
	query += ")"

	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("chatlog: update status %s: %w", messageID, err)
	}

	n, _ := res.RowsAffected()
	return n > 0, nil
}

// Read returns the conversation entries in ascending time order. The context
// deadline is propagated to SQLite so callers can bound I/O time.
func (s *Store) Read(ctx context.Context, topic string, peerAddress domain.PeerIdentity) ([]Entry, error) {
	query, args := s.peerQuery(topic, peerAddress,
		`SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		 FROM messages WHERE `, ` ORDER BY created_at ASC`)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("chatlog: read: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanEntries(rows)
}

// UnconfirmedSeen returns the inbound DM entries this identity has marked
// "seen" whose seen receipt has not yet been confirmed by the original
// sender (no row in the seen_ack journal). since bounds the scan to
// recently-updated rows so a first run on a long history does not reseed
// the whole archive. Durable source for the seen half of the sender-side
// retry scheduler (node.SeenAckJournal).
func (s *Store) UnconfirmedSeen(ctx context.Context, self domain.PeerIdentity, since time.Time) ([]Entry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		 FROM messages
		 WHERE topic = 'dm' AND recipient = ? AND delivery_status = ?
		   AND updated_at >= ?
		   AND id NOT IN (SELECT id FROM seen_ack)
		 ORDER BY updated_at ASC`,
		self.String(), StatusSeen, since.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, fmt.Errorf("chatlog: unconfirmed seen: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanEntries(rows)
}

// MarkDeliveryFailed durably records that automatic retries for the
// locally-sent message were abandoned (TTL expiry / attempts cap), so
// UndeliveredOutgoing stops reseeding it. Idempotent.
func (s *Store) MarkDeliveryFailed(ctx context.Context, messageID string) error {
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO delivery_failed (id) VALUES (?)`, messageID); err != nil {
		return fmt.Errorf("chatlog: mark delivery failed %s: %w", messageID, err)
	}
	return nil
}

// MarkSeenConfirmed durably records that the original sender confirmed the
// seen receipt for the message (seen_ack arrived). Idempotent.
func (s *Store) MarkSeenConfirmed(ctx context.Context, messageID string) error {
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO seen_ack (id) VALUES (?)`, messageID); err != nil {
		return fmt.Errorf("chatlog: mark seen confirmed %s: %w", messageID, err)
	}
	return nil
}

// UndeliveredOutgoing returns the DM entries this identity SENT that are
// still in the "sent" delivery status — the durable source for the
// sender-side end-to-end delivery retry scheduler (node.DeliveryOutbox).
// since bounds the scan to recently-created rows so a restart does not
// reseed (and re-inject into the mesh) ancient undelivered DMs whose
// recipient never returned: the scheduler caps a single message at ~3.5h of
// attempts, so anything older than the horizon is already abandoned in
// practice. Symmetric with UnconfirmedSeen's `since`.
func (s *Store) UndeliveredOutgoing(ctx context.Context, self domain.PeerIdentity, since time.Time) ([]Entry, error) {
	// A recovery-superseded original must never re-enter the ordinary
	// retry path: its replacement is already in flight under a new id, and
	// re-sending the OLD ciphertext would race the replacement with the
	// very payload the receiver could not read. The exclusion reads the
	// JSON field itself (json_extract), mirroring decodeRecoveryMarks: a
	// null value, a non-object blob or the key nested inside some other
	// value all count as NOT superseded — a LIKE substring test would
	// wrongly drop those rows.
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		 FROM messages
		 WHERE topic = 'dm' AND sender = ? AND delivery_status = ?
		   AND created_at >= ?
		   AND id NOT IN (SELECT id FROM delivery_failed)
		   AND (metadata IS NULL OR NOT json_valid(metadata)
		        OR COALESCE(json_extract(metadata, '$.superseded_by'), '') = '')
		 ORDER BY created_at ASC`,
		self.String(), StatusSent, since.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, fmt.Errorf("chatlog: undelivered outgoing: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanEntries(rows)
}

// ReadLast returns the newest n entries of a conversation, oldest first.
func (s *Store) ReadLast(ctx context.Context, topic string, peerAddress domain.PeerIdentity, n int) ([]Entry, error) {
	// Use a subquery to get the last N, then re-order ascending.
	innerQuery, args := s.peerQuery(topic, peerAddress,
		`SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		 FROM messages WHERE `, ` ORDER BY created_at DESC, rowid DESC LIMIT ?`)
	args = append(args, n)

	query := fmt.Sprintf(`SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		FROM (%s) sub ORDER BY created_at ASC`, innerQuery)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("chatlog: read last: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanEntries(rows)
}

// ListConversations lists every DM conversation. Conversations with unread
// messages come first, then by last message time.
func (s *Store) ListConversations(ctx context.Context) ([]ConversationSummary, error) {
	selfAddr := s.identityAddr
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			CASE WHEN sender = ? THEN recipient ELSE sender END AS peer_address,
			MAX(created_at) AS last_message,
			COUNT(*) AS cnt,
			SUM(CASE WHEN sender != ? AND recipient = ? AND delivery_status != 'seen' THEN 1 ELSE 0 END) AS unread_count
		FROM messages
		WHERE topic = 'dm' AND (sender = ? OR recipient = ?)
		GROUP BY peer_address
		ORDER BY (unread_count > 0) DESC, last_message DESC`,
		selfAddr, selfAddr, selfAddr, selfAddr, selfAddr,
	)
	if err != nil {
		return nil, fmt.Errorf("chatlog: list conversations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var result []ConversationSummary
	for rows.Next() {
		var cs ConversationSummary
		var lastMsg string
		if err := rows.Scan(&cs.PeerAddress, &lastMsg, &cs.Count, &cs.UnreadCount); err != nil {
			continue
		}
		if t, err := time.Parse(time.RFC3339Nano, lastMsg); err == nil {
			cs.LastMessage = t
		} else if t, err := time.Parse(time.RFC3339, lastMsg); err == nil {
			cs.LastMessage = t
		}
		result = append(result, cs)
	}

	return result, rows.Err()
}

// ReadLastEntry returns the newest entry of a conversation, nil when empty.
func (s *Store) ReadLastEntry(ctx context.Context, topic string, peerAddress domain.PeerIdentity) (*Entry, error) {
	query, args := s.peerQuery(topic, peerAddress,
		`SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		 FROM messages WHERE `, ` ORDER BY created_at DESC, rowid DESC LIMIT 1`)

	row := s.db.QueryRowContext(ctx, query, args...)
	var e Entry
	err := row.Scan(&e.ID, &e.Sender, &e.Recipient, &e.Body, &e.CreatedAt, &e.Flag, &e.DeliveryStatus, &e.TTLSeconds, &e.Metadata)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("chatlog: read last entry: %w", err)
	}
	return &e, nil
}

// ReadLastEntryPerPeer returns the newest entry of every DM conversation,
// keyed by peer address.
func (s *Store) ReadLastEntryPerPeer(ctx context.Context) (map[string]Entry, error) {
	selfAddr := s.identityAddr
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		FROM (
			SELECT
				m.id, m.sender, m.recipient, m.body, m.created_at,
				m.flag, m.delivery_status, m.ttl_seconds, m.metadata,
				ROW_NUMBER() OVER (
					PARTITION BY CASE WHEN m.sender = ? THEN m.recipient ELSE m.sender END
					ORDER BY m.created_at DESC, m.rowid DESC
				) AS rn
			FROM messages m
			WHERE m.topic = 'dm' AND (m.sender = ? OR m.recipient = ?)
		) ranked
		WHERE rn = 1`,
		selfAddr, selfAddr, selfAddr,
	)
	if err != nil {
		return nil, fmt.Errorf("chatlog: read last per peer: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]Entry)
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.Sender, &e.Recipient, &e.Body, &e.CreatedAt, &e.Flag, &e.DeliveryStatus, &e.TTLSeconds, &e.Metadata); err != nil {
			continue
		}
		peer := e.Recipient
		if e.Recipient == selfAddr.String() {
			peer = e.Sender
		}
		result[peer] = e
	}

	return result, rows.Err()
}

// Returns the number of deleted rows.
func (s *Store) DeleteExpired(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM messages
		WHERE flag = 'auto-delete-ttl'
		  AND ttl_seconds > 0
		  AND datetime(created_at) < datetime('now', '-' || ttl_seconds || ' seconds')`)
	if err != nil {
		return 0, fmt.Errorf("chatlog: delete expired: %w", err)
	}

	n, _ := res.RowsAffected()
	return n, nil
}

// DeleteByPeer removes all messages for a conversation with the given identity.
// Returns the number of deleted rows.
func (s *Store) DeleteByPeer(ctx context.Context, identity domain.PeerIdentity) (int64, error) {
	id := identity.String()
	if strings.TrimSpace(id) == "" {
		return 0, fmt.Errorf("chatlog: empty identity")
	}

	res, err := s.db.ExecContext(ctx, `
		DELETE FROM messages
		WHERE topic = 'dm'
		  AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))`,
		s.identityAddr, id, id, s.identityAddr)
	if err != nil {
		return 0, fmt.Errorf("chatlog: delete identity %s: %w", id, err)
	}

	n, _ := res.RowsAffected()
	return n, nil
}

// UnreadCountFor returns the number of unread messages in the
// conversation with peerAddress. Mirrors the unread_count column
// computed by ListConversations for a single conversation, used by
// the DM-router delete path to refresh the in-memory sidebar badge
// after a row is removed (otherwise the badge would stay at the
// stale pre-delete count).
//
// Returns (0, nil) when the conversation has no unread messages or
// when the database is not available; the latter mirrors the contract
// of the surrounding chatlog Read* helpers (transient unavailability
// is not an error).
func (s *Store) UnreadCountFor(ctx context.Context, peerAddress domain.PeerIdentity) (int, error) {
	if peerAddress.IsZero() {
		return 0, nil
	}
	selfAddr := s.identityAddr
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM messages
		WHERE topic = 'dm'
		  AND sender = ?
		  AND recipient = ?
		  AND delivery_status != 'seen'`,
		peerAddress.String(), selfAddr,
	).Scan(&n)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("chatlog: unread count for %s: %w", peerAddress, err)
	}
	return n, nil
}

// EntryByID fetches a single chatlog entry by message ID across all
// conversations. Returns (entry, true, nil) when found,
// (zero, false, nil) when the row does not exist (idempotent caller
// path), and (zero, false, err) on database failure.
//
// Used by the message_delete handlers in DMRouter to look up the
// target message's Sender and Flag before authorizing a remote
// delete: the envelope sender of the inbound message_delete must
// match either the original Sender or the Recipient (depending on
// MessageFlag), and an immutable target is rejected outright.
func (s *Store) EntryByID(ctx context.Context, messageID domain.MessageID) (Entry, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, sender, recipient, body, created_at, flag, delivery_status, ttl_seconds, metadata
		 FROM messages WHERE id = ? LIMIT 1`,
		messageID,
	)

	var e Entry
	err := row.Scan(&e.ID, &e.Sender, &e.Recipient, &e.Body, &e.CreatedAt, &e.Flag, &e.DeliveryStatus, &e.TTLSeconds, &e.Metadata)
	switch {
	case err == sql.ErrNoRows:
		return Entry{}, false, nil
	case err != nil:
		return Entry{}, false, fmt.Errorf("chatlog: entry by id %s: %w", messageID, err)
	default:
		return e, true, nil
	}
}

// Returns true if a row was deleted.
func (s *Store) DeleteByID(ctx context.Context, messageID domain.MessageID) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM messages WHERE id = ?`, messageID)
	if err != nil {
		return false, fmt.Errorf("chatlog: delete %s: %w", messageID, err)
	}

	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) HasEntryID(ctx context.Context, topic string, peerAddress domain.PeerIdentity, id domain.MessageID) bool {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM messages WHERE id = ? LIMIT 1`, id).Scan(&exists)
	return err == nil
}

// HasEntryInConversation checks whether a message with the given ID exists
// within a specific DM conversation. Used to validate reply_to references
// before encrypting — prevents dangling or cross-conversation reply links.
// Collapses DB failures into false; callers that must distinguish "the row
// is genuinely absent" from "the lookup itself failed" (the reply-degrade
// path in DMCrypto.SendDirectMessage) use LookupEntryInConversation.
func (s *Store) HasEntryInConversation(ctx context.Context, peerAddress domain.PeerIdentity, id domain.MessageID) bool {
	found, err := s.LookupEntryInConversation(ctx, peerAddress, id)
	return err == nil && found
}

// LookupEntryInConversation reports whether a message with the given ID
// exists within a specific DM conversation, surfacing DB failures as a
// separate error instead of folding them into "not found". A nil store,
// zero peer or empty ID is a definitive miss (false, nil), matching the
// HasEntryInConversation contract.
func (s *Store) LookupEntryInConversation(ctx context.Context, peerAddress domain.PeerIdentity, id domain.MessageID) (bool, error) {
	if peerAddress.IsZero() || id == "" {
		return false, nil
	}
	query, params := s.peerQuery("dm", peerAddress,
		`SELECT 1 FROM messages WHERE id = ? AND `, ` LIMIT 1`)
	params = append([]interface{}{id}, params...)
	var exists int
	err := s.db.QueryRowContext(ctx, query, params...).Scan(&exists)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("chatlog: lookup entry %s: %w", id, err)
	}
}

// peerQuery builds a WHERE clause for messages in a specific conversation.
// For DMs it filters by (sender=self AND recipient=peer) OR (sender=peer AND recipient=self).
// For global it filters by topic='global'.
func (s *Store) peerQuery(topic string, peerAddress domain.PeerIdentity, prefix string, suffix string) (string, []interface{}) {
	if topic == "dm" && !peerAddress.IsZero() {
		return prefix +
			`topic = 'dm' AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))` +
			suffix, []interface{}{s.identityAddr, peerAddress, peerAddress, s.identityAddr}
	}
	return prefix + `topic = 'global'` + suffix, nil
}

func scanEntries(rows *sql.Rows) ([]Entry, error) {
	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.Sender, &e.Recipient, &e.Body, &e.CreatedAt, &e.Flag, &e.DeliveryStatus, &e.TTLSeconds, &e.Metadata); err != nil {
			continue
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

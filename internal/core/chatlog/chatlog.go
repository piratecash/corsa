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

	"github.com/rs/zerolog/log"

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

	// ONE transaction for the row and the refusal it lifts. Two statements in
	// sequence would have a state between them where the message is committed
	// and its refusal is not, and the caller cannot recover from it: the
	// returned error is read as "not stored", so no arrival event is published,
	// while the next copy of the message is a duplicate that publishes nothing
	// either. The message would sit in the database, invisible until the
	// conversation is loaded again.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("chatlog: begin insert %s: %w", entry.ID, err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO messages (id, topic, sender, recipient, body, flag, delivery_status, ttl_seconds, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID, topic, entry.Sender, entry.Recipient, entry.Body,
		entry.Flag, status, entry.TTLSeconds, entry.Metadata, entry.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("chatlog: insert %s: %w", entry.ID, err)
	}
	rows, _ := res.RowsAffected()
	if rows > 0 {
		// The message is here, so any standing refusal of its reactions is
		// wrong from this moment: the refusal means "not here and did not
		// come", and a re-delivery or a reseed days later is exactly the case
		// it must not outlive. Its author is still offering the fact, and the
		// next offer has to apply.
		//
		// Only on a real insert, so the duplicate arrivals that INSERT OR
		// IGNORE swallows do not pay for it, and only in THIS conversation:
		// two of them can hold the same id, and lifting id-wide would clear a
		// refusal the sweep could never record again — the id exists now.
		if scope, ok := dmScopeOf(topic, selfAddress, entry); ok {
			if err := dropReactionRefusalTx(ctx, tx, scope, domain.MessageID(entry.ID)); err != nil {
				return false, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("chatlog: commit insert %s: %w", entry.ID, err)
	}
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
		if t, ok := parseStoredTimestamp(lastMsg); ok {
			cs.LastMessage = t
		}
		result = append(result, cs)
	}

	return result, rows.Err()
}

// UnseenIncomingIDs returns, per conversation peer, the ids of the incoming
// messages that have not been marked seen.
//
// IDs rather than a count, because the consumer keeps a SET. A count has to be
// reconciled against a stream of events that arrive after it was taken — the
// database is ahead of that stream, so the same message can be in both and be
// counted twice — while adding an id to a set the message is already in
// changes nothing. Sets make the badge independent of ordering.
func (s *Store) UnseenIncomingIDs(ctx context.Context) (map[domain.PeerIdentity][]domain.MessageID, error) {
	selfAddr := s.identityAddr
	rows, err := s.db.QueryContext(ctx, `
		SELECT sender, id
		FROM messages
		WHERE topic = 'dm' AND recipient = ? AND sender != ? AND delivery_status != ?`,
		selfAddr, selfAddr, StatusSeen,
	)
	if err != nil {
		return nil, fmt.Errorf("chatlog: unseen incoming ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[domain.PeerIdentity][]domain.MessageID)
	for rows.Next() {
		var sender, id string
		if err := rows.Scan(&sender, &id); err != nil {
			continue
		}
		peer := domain.PeerIdentityFromWire(sender)
		if peer.IsZero() {
			continue
		}
		result[peer] = append(result[peer], domain.MessageID(id))
	}

	return result, rows.Err()
}

// storedStatusChunk keeps each IN (...) list clear of SQLite's bound
// parameter limit, which is 999 on the default build.
const storedStatusChunk = 500

// StoredMessageStatuses returns the delivery status of every one of these ids
// the database holds. An id absent from the result is one the database does
// not have at all.
//
// One query answers both questions the callers have, and answers them about
// the same moment. The header path needs "did the user already read this" —
// DMHeaders carry no delivery_status, and the node's in-memory topic outlives
// a desktop session, so on the first sync it is offered back every message of
// the previous session. The delete path needs "is this id merely not written
// yet", because a badge can be raised from a header before the row lands and
// re-deriving the badge from the database alone would read that as "read".
// Two separate queries would let one of them succeed while the other fails,
// and the caller would have to invent an answer for the half it did not get.
func (s *Store) StoredMessageStatuses(ctx context.Context, ids []domain.MessageID) (map[domain.MessageID]string, error) {
	statuses := make(map[domain.MessageID]string, len(ids))
	for start := 0; start < len(ids); start += storedStatusChunk {
		end := start + storedStatusChunk
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]

		args := make([]any, 0, len(chunk))
		for _, id := range chunk {
			args = append(args, string(id))
		}
		query := `SELECT id, delivery_status FROM messages WHERE id IN (?` + strings.Repeat(",?", len(chunk)-1) + `)`

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("chatlog: stored message statuses: %w", err)
		}
		for rows.Next() {
			var id, status string
			if err := rows.Scan(&id, &status); err != nil {
				continue
			}
			statuses[domain.MessageID(id)] = status
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, fmt.Errorf("chatlog: stored message statuses: %w", err)
		}
	}
	return statuses, nil
}

// UnseenIncomingIDsFor is the single-conversation form of UnseenIncomingIDs.
// The delete path uses it to re-derive one peer's badge from the database,
// which is the only place the set is reconciled against delivery_status:
// headers carry no status, so the event stream can only ever ADD ids.
func (s *Store) UnseenIncomingIDsFor(ctx context.Context, peerAddress domain.PeerIdentity) ([]domain.MessageID, error) {
	if peerAddress.IsZero() {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id
		FROM messages
		WHERE topic = 'dm' AND sender = ? AND recipient = ? AND delivery_status != ?`,
		peerAddress.String(), s.identityAddr, StatusSeen,
	)
	if err != nil {
		return nil, fmt.Errorf("chatlog: unseen incoming ids for %s: %w", peerAddress, err)
	}
	defer func() { _ = rows.Close() }()

	var ids []domain.MessageID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, domain.MessageID(id))
	}
	return ids, rows.Err()
}

// LastIncomingAtPerPeer returns, for every DM conversation, the creation time
// of the newest message the PEER wrote — never one of ours. The last row of a
// thread answers "when did anything happen here"; presence needs "when did this
// contact last do something", and in an ordinary conversation those differ:
// the peer writes, we reply, and the newest row is ours.
//
// Peers whose entire history is outgoing are absent from the map rather than
// present with a zero time — no incoming message is no evidence.
//
// Rows dated after `now` are skipped, and skipping one does NOT skip the
// conversation: the newest message a peer wrote is the one they chose the
// timestamp for, so a forged future date would otherwise hide the honest
// message behind it and leave the contact reading as never seen.
//
// The comparison happens in Go, over every candidate row, and deliberately
// not in SQL. created_at is RFC3339 text, and neither available SQL ordering
// is trustworthy here: text order is not time order ("…00Z" sorts ABOVE the
// later "…00.5Z" because 'Z' > '.', and a zone offset shifts the instant by
// hours while the digits keep their printed order), while julianday is a
// float that collapses timestamps inside the same microsecond, leaving the
// winner to a tie-break on rowid — insertion order, which for out-of-order
// arrivals is not time order either. RFC3339Nano stores nanoseconds, so
// nanoseconds decide. The scan stays inside idx_messages_peer, which covers
// every column this reads.
func (s *Store) LastIncomingAtPerPeer(ctx context.Context, now time.Time) (map[domain.PeerIdentity]time.Time, error) {
	selfAddr := s.identityAddr
	rows, err := s.db.QueryContext(ctx, `
		SELECT sender, created_at
		FROM messages
		WHERE topic = 'dm' AND recipient = ? AND sender != ?`,
		selfAddr, selfAddr,
	)
	if err != nil {
		return nil, fmt.Errorf("chatlog: last incoming per peer: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[domain.PeerIdentity]time.Time)
	for rows.Next() {
		var (
			sender string
			raw    string
		)
		if err := rows.Scan(&sender, &raw); err != nil {
			continue
		}
		at, ok := usableIncomingTimestamp(raw, now)
		if !ok {
			continue
		}
		peer := domain.PeerIdentityFromWire(sender)
		if peer.IsZero() {
			continue
		}
		if current, seen := result[peer]; seen && !at.After(current) {
			continue
		}
		result[peer] = at
	}

	return result, rows.Err()
}

// LastIncomingAtFor is the single-conversation form of
// LastIncomingAtPerPeer, used by the delete path to recompute presence
// evidence after rows leave the conversation. Returns the zero time when the
// peer has written nothing usable that is still stored. Same rules: future
// rows are skipped without hiding the ones behind them, and the winner is
// decided in Go at full precision.
func (s *Store) LastIncomingAtFor(ctx context.Context, peerAddress domain.PeerIdentity, now time.Time) (time.Time, error) {
	if peerAddress.IsZero() {
		return time.Time{}, nil
	}

	// One pass over the conversation, unordered, decided in Go. SQL cannot
	// help here: the stored text does not sort as time, and asking SQLite to
	// order by julianday(created_at) is ordering by a function of a column,
	// which costs a temp b-tree over the whole conversation — strictly more
	// than the scan it was meant to replace. Unordered, this is a covering
	// index scan of one peer's rows and the comparison is exact.
	rows, err := s.db.QueryContext(ctx, `
		SELECT created_at
		FROM messages
		WHERE topic = 'dm' AND sender = ? AND recipient = ?`,
		peerAddress.String(), s.identityAddr,
	)
	if err != nil {
		return time.Time{}, fmt.Errorf("chatlog: last incoming for %s: %w", peerAddress, err)
	}
	defer func() { _ = rows.Close() }()

	var newest time.Time
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		at, ok := usableIncomingTimestamp(raw, now)
		if !ok || !at.After(newest) {
			continue
		}
		newest = at
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("chatlog: last incoming for %s: %w", peerAddress, err)
	}
	return newest, nil
}

// usableIncomingTimestamp decodes a created_at that may serve as presence
// evidence. Unparsable values and values after now are refused: the column
// holds what the SENDER's node printed, and the sender is the one party who
// gains from appearing recently online.
func usableIncomingTimestamp(raw string, now time.Time) (time.Time, bool) {
	at, ok := parseStoredTimestamp(raw)
	if !ok {
		return time.Time{}, false
	}
	if !now.IsZero() && at.After(now) {
		return time.Time{}, false
	}
	return at, true
}

// parseStoredTimestamp decodes a created_at column. Rows written by older
// builds carry second-resolution RFC3339, so both layouts are accepted; an
// unparsable value reports failure instead of the zero time, which a caller
// would otherwise store as a real "January 1, year 1" observation.
func parseStoredTimestamp(raw string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, true
	}
	return time.Time{}, false
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
//
// It goes through deleteMessageTx per id rather than deleting the rows in one
// statement, and that is the point rather than an inefficiency: a message is
// never only its row. Its journals, its resend intents and its REACTIONS are the
// same message seen from other tables, there is no cascading foreign key to take
// them, and a bulk DELETE would leave a reaction whose message no longer exists
// — which the UI still draws and the re-offer still sends, forever, since both
// read reactions without joining the messages. The ids are read and deleted in
// ONE transaction so an expiry cannot half-happen.
func (s *Store) DeleteExpired(ctx context.Context) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("chatlog: begin delete expired: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM messages
		WHERE flag = 'auto-delete-ttl'
		  AND ttl_seconds > 0
		  AND datetime(created_at) < datetime('now', '-' || ttl_seconds || ' seconds')`)
	if err != nil {
		return 0, fmt.Errorf("chatlog: find expired: %w", err)
	}
	var expired []domain.MessageID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("chatlog: scan expired id: %w", err)
		}
		expired = append(expired, domain.MessageID(id))
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("chatlog: find expired: %w", err)
	}
	_ = rows.Close()

	var n int64
	for _, id := range expired {
		gone, err := deleteMessageTx(ctx, tx, s.identityAddr, id)
		if err != nil {
			return 0, fmt.Errorf("chatlog: delete expired: %w", err)
		}
		if gone {
			n++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("chatlog: commit delete expired: %w", err)
	}

	if n > 0 {
		// Same class of deletion as a user-issued one — a message whose
		// lifetime ran out is supposed to stop existing — so the pages
		// that held it leave the write-ahead log here rather than at the
		// next automatic checkpoint. Best-effort; see CheckpointWAL.
		if err := s.CheckpointWAL(ctx); err != nil {
			log.Debug().Err(err).Msg("chatlog: wal checkpoint after the TTL sweep did not complete")
		}
	}
	return n, nil
}

// DeleteByPeer removes all messages for a conversation with the given
// identity, along with every per-message trace those rows left in the other
// tables. Returns the number of deleted message rows.
//
// Everything else naming this peer goes with them: the delete requests
// still owed by them and the tombstones of the rows being removed. They are
// the last things left naming a conversation the user has erased,
// and a client that keeps phoning a peer about a thread its owner destroyed
// — or keeps a row with their address in it — is carrying exactly the
// metadata the deletion was for. The cost is stated plainly: peer-side
// deletions that had been scheduled and not yet acknowledged are abandoned,
// so the peer keeps whatever it still holds. Asking both sides to forget
// the thread first is what "Delete chat and ask the peer" is for; this is the
// user erasing their own side of it.
//
// What cannot be cleaned here is a tombstone for a row the peer's own wipe
// already removed: those name a message id and nothing else, no longer
// resolve to any conversation, and expire on their own hour.
//
// One transaction: a wipe that removed the rows but left their journals (or
// the reverse) is a half-erased conversation, and nothing downstream could
// tell which half it got.
func (s *Store) DeleteByPeer(ctx context.Context, identity domain.PeerIdentity) (int64, error) {
	id := identity.String()
	if strings.TrimSpace(id) == "" {
		return 0, fmt.Errorf("chatlog: empty identity")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("chatlog: begin delete identity %s: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()

	const conversationFilter = `
		SELECT id FROM messages
		WHERE topic = 'dm'
		  AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))`

	// Each occurrence of the sub-select needs its own copy of the four
	// conversation parameters, so the argument list is built per statement
	// rather than shared.
	conversationArgs := []any{s.identityAddr, id, id, s.identityAddr}
	twoFilters := append(append([]any{}, conversationArgs...), conversationArgs...)

	journalDeletes := []struct {
		statement string
		args      []any
	}{
		{`DELETE FROM seen_ack WHERE id IN (` + conversationFilter + `)`, conversationArgs},
		{`DELETE FROM delivery_failed WHERE id IN (` + conversationFilter + `)`, conversationArgs},
		{`DELETE FROM decrypt_resend_intents WHERE original_id IN (` + conversationFilter +
			`) OR replacement_id IN (` + conversationFilter + `)`, twoFilters},
		// Reactions go with the conversation for the reason the per-message
		// delete gives: they are metadata about messages that are being erased.
		//
		// Matched by SCOPE and not through the message rows. The join would
		// miss precisely the facts that are still WAITING for a message this
		// node never received — they have no row to join through — and those
		// are the ones that would outlive the wipe as "peer X reacted to
		// something in this conversation" on a conversation the user erased.
		{`DELETE FROM message_reactions WHERE scope = ?`, []any{id}},
		// And the refusals of this conversation, for the reason
		// DeleteConversationWithIntents gives at length: with the conversation
		// gone, offers from this peer are refused at the door, so a row per
		// erased id would spend a bounded table on ids nothing will ask about.
		//
		// Unconditional here, unlike there: this path removes EVERY message of
		// the peer, immutable ones included, so nothing can be left to keep the
		// conversation admitting offers. And by scope, which reaches the ids
		// deleted from it one at a time long before today.
		{`DELETE FROM reaction_refusals WHERE scope = ?`, []any{id}},
	}
	for _, deletion := range journalDeletes {
		if _, err := tx.ExecContext(ctx, deletion.statement, deletion.args...); err != nil {
			return 0, fmt.Errorf("chatlog: delete identity %s journals: %w", id, err)
		}
	}

	// Both halves of the deletion table go: the requests still owed by
	// this peer, and the refusals of the ids of the conversation being
	// erased. A refusal names no peer, so it is found through the rows it
	// belongs to — which are still here, this statement running before
	// they are removed.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM message_delete_intents WHERE peer = ? OR message_id IN (`+conversationFilter+`)`,
		append([]any{id}, conversationArgs...)...); err != nil {
		return 0, fmt.Errorf("chatlog: delete identity %s deletion rows: %w", id, err)
	}

	res, err := tx.ExecContext(ctx, `
		DELETE FROM messages
		WHERE topic = 'dm'
		  AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))`,
		s.identityAddr, id, id, s.identityAddr)
	if err != nil {
		return 0, fmt.Errorf("chatlog: delete identity %s: %w", id, err)
	}
	n, _ := res.RowsAffected()

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("chatlog: commit delete identity %s: %w", id, err)
	}

	// Same on-disk treatment as every other deletion: secure_delete
	// overwrites the freed page, but in WAL mode that overwrite is itself
	// a log frame, so the original bytes live in the -wal file until a
	// checkpoint. Erasing a whole conversation and leaving it readable
	// there would be the loudest place to skip this.
	if err := s.CheckpointWAL(ctx); err != nil {
		log.Debug().Err(err).Msg("chatlog: wal checkpoint after deleting an identity did not complete")
	}
	return n, nil
}

// CheckpointWAL folds the write-ahead log back into the database file and
// truncates it.
//
// DELETE with secure_delete on zeroes the freed pages, but in WAL mode the
// page that HELD the message is only overwritten in the log — the original
// bytes stay in the -wal file until a checkpoint retires them. For a routine
// write that is fine; for a deletion whose entire purpose is that the
// content stops existing, it is the difference between the promise and the
// file on disk.
//
// Best-effort by contract: TRUNCATE waits for readers and can report busy,
// which is not a failure of the deletion — the next checkpoint (automatic at
// ~4 MB of log, or at close) still retires the pages. Callers log and move
// on.
func (s *Store) CheckpointWAL(ctx context.Context) error {
	var busy, logFrames, checkpointed int
	if err := s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		return fmt.Errorf("chatlog: wal checkpoint: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("chatlog: wal checkpoint busy: %d frames left", logFrames)
	}
	return nil
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("chatlog: begin delete %s: %w", messageID, err)
	}
	defer func() { _ = tx.Rollback() }()

	removed, err := deleteMessageTx(ctx, tx, s.identityAddr, messageID)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("chatlog: commit delete %s: %w", messageID, err)
	}
	return removed, nil
}

// DeleteMessages removes a batch of messages in ONE transaction and reports
// which of them were actually there. Each row still goes through
// deleteMessageTx, so the journal traces leave with it; what changes is the
// number of commits.
//
// The receiver of a conversation wipe deletes a whole thread this way. One
// transaction per row meant a thread of a few thousand messages cost a few
// thousand commits, each an fsync the caller waits on while holding the
// locks the inbound path also needs — an order of magnitude more expensive
// than the sender's side of the same wipe, for the same work.
//
// All-or-nothing per batch: a failure rolls back the whole chunk and
// reports the error, so the caller can retry those ids (delete is
// idempotent) or fall back to one at a time to isolate the row that fails.
func (s *Store) DeleteMessages(ctx context.Context, ids []domain.MessageID, tombstoneUntil time.Time) ([]domain.MessageID, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("chatlog: begin batch delete of %d messages: %w", len(ids), err)
	}
	defer func() { _ = tx.Rollback() }()

	removed := make([]domain.MessageID, 0, len(ids))
	for _, id := range ids {
		gone, err := deleteMessageTx(ctx, tx, s.identityAddr, id)
		if err != nil {
			return nil, err
		}
		if gone {
			removed = append(removed, id)
		}
	}
	// Every id in the batch is refused from here on, not just the ones
	// that were still present: an id already gone can still be replayed,
	// and the caller asked for it to stay gone.
	if err := noteWipeTombstones(ctx, tx, ids, tombstoneUntil); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("chatlog: commit batch delete of %d messages: %w", len(ids), err)
	}
	return removed, nil
}

// DeleteMessageWithTombstone removes one message and plants the refusal of
// its id in the same commit. The pair is the point: a row deleted without
// its tombstone can be put straight back by a replay of the same envelope,
// and a tombstone written after the commit leaves that window open for as
// long as the second write takes — or forever, if the process dies in it.
func (s *Store) DeleteMessageWithTombstone(ctx context.Context, messageID domain.MessageID, tombstoneUntil time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("chatlog: begin delete %s: %w", messageID, err)
	}
	defer func() { _ = tx.Rollback() }()

	removed, err := deleteMessageTx(ctx, tx, s.identityAddr, messageID)
	if err != nil {
		return false, err
	}
	if err := noteWipeTombstones(ctx, tx, []domain.MessageID{messageID}, tombstoneUntil); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("chatlog: commit delete %s: %w", messageID, err)
	}
	return removed, nil
}

// deleteMessageTx removes the message row AND every per-message trace the
// other repositories keep under the same id. It takes an executor rather
// than the store handle so both the standalone delete and the
// delete-and-schedule transaction run it inside a transaction of their own:
// a row removed while its journals survived is exactly the half-erased
// state this function exists to prevent, and its name would be a lie if the
// two halves could commit apart.
//
// The journals are the point: `seen_ack` and `delivery_failed` (migration
// 0003) and the resend intents keyed on the id (migration 0004) outlive the
// row they describe, and each of them is a record that a message with this
// id existed and how it went. Leaving them behind after a user deletes the
// message keeps exactly the metadata the deletion was supposed to remove,
// and re-seeds retry schedulers with ids that no longer resolve.
//
// Per-PEER state (`decrypt_recovery_jobs`, `peer_established`,
// `decrypt_recovery_cycles`) is deliberately untouched: it describes the
// conversation, not this message, and survives it by design. The recovery
// marks of the row itself live in its `metadata` column and go with it.
func deleteMessageTx(ctx context.Context, db readWriteContext, self domain.PeerIdentity, messageID domain.MessageID) (bool, error) {
	res, err := db.ExecContext(ctx, `DELETE FROM messages WHERE id = ?`, messageID)
	if err != nil {
		return false, fmt.Errorf("chatlog: delete %s: %w", messageID, err)
	}
	n, _ := res.RowsAffected()

	for _, statement := range []string{
		`DELETE FROM seen_ack WHERE id = ?`,
		`DELETE FROM delivery_failed WHERE id = ?`,
	} {
		if _, err := db.ExecContext(ctx, statement, messageID); err != nil {
			return false, fmt.Errorf("chatlog: delete %s journals: %w", messageID, err)
		}
	}

	// Reactions describe THIS message and are metadata about it in exactly the
	// sense the paragraph above means: who responded to what, and when. Left
	// behind they would also resurrect the message on the next reconciliation,
	// since a fact naming an id is a claim that the id existed.
	//
	// By id alone, without the scope ReleaseHeldReactions insists on, and the
	// asymmetry is deliberate. Releasing makes rows VISIBLE, so it must not
	// reach a stranger's fact that happens to name the same id; deleting
	// destroys them, and over-deleting is the safe direction of a collision
	// that requires a message id to be reused on purpose.
	//
	// The conversations are read BEFORE the delete, and they are read from the
	// reaction rows themselves rather than from the message: this runs after
	// the message row is already gone, and a held fact never had one to begin
	// with. They are what the refusal below is keyed by.
	scopes, err := reactionScopesOfTx(ctx, db, messageID)
	if err != nil {
		return false, err
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM message_reactions WHERE message_id = ?`, messageID); err != nil {
		return false, fmt.Errorf("chatlog: delete %s journals: %w", messageID, err)
	}
	if len(scopes) > 0 {
		// Somebody reacted to this message, and their node offers the facts it
		// holds for as long as it holds them — longer than the tombstone that
		// refuses the id, which is sized for message re-delivery and expires
		// within the week. Recording the id here is what stops the next offer
		// after that from putting these rows back; see reaction_refusals.go.
		//
		// In the same transaction as the delete, for the reason the whole
		// function takes an executor: a deletion that committed without its
		// refusal is one that can be undone by the sender, one offer at a time.
		//
		// The stamp orders the trim and decides nothing, which is why the wall
		// clock is the right source for it.
		keys := make([]scopedID, 0, len(scopes))
		for _, scope := range scopes {
			keys = append(keys, scopedID{Scope: scope, MessageID: messageID})
		}
		if err := refuseReactionsForTx(ctx, db, self, keys, time.Now().UTC()); err != nil {
			return false, err
		}
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM decrypt_resend_intents WHERE original_id = ? OR replacement_id = ?`,
		messageID, messageID); err != nil {
		return false, fmt.Errorf("chatlog: delete %s resend intents: %w", messageID, err)
	}

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

// Package chatlog provides SQLite-backed storage for chat messages.
//
// Messages are stored in a single SQLite database file per node identity+port:
//
//	chatlog-<identity_short>-<port>.db
//
// The database uses WAL mode for concurrent read access. Messages are stored
// as-is: incoming DM bodies are already sealed envelopes, and outgoing DMs
// are encrypted with the sender's own key before storage. Reading a chatlog
// always requires decryption via the identity key.
package chatlog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
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

// Store manages the SQLite chatlog database for a single node identity.
type Store struct {
	db           *sql.DB
	identityAddr domain.PeerIdentity // full 40-char identity address
}

// NewStoreFromDB wraps an existing *sql.DB (may be nil) into a Store.
// Intended for tests that need a Store without filesystem side-effects.
func NewStoreFromDB(db *sql.DB, identity domain.PeerIdentity) *Store {
	return &Store{db: db, identityAddr: identity}
}

// NewStore creates a chatlog store backed by SQLite.
//   - dir:           base directory for the database file (e.g. ".corsa")
//   - identityAddr:  full 40-char hex identity address
//   - listenAddress: node listen address (e.g. ":64646") — port is extracted
//
// On startup the database file is checked with PRAGMA integrity_check.
// If corruption is detected the file is renamed to *.corrupt and a fresh
// database is created so the node can keep running.
func NewStore(dir string, identity domain.PeerIdentity, listenAddress domain.ListenAddress) *Store {
	identityAddr := identity
	short := string(identityAddr)
	if len(short) > 8 {
		short = short[:8]
	}
	port := portSuffix(string(listenAddress))

	if err := os.MkdirAll(dir, 0o700); err != nil {
		// Best effort — the database open will fail with a clear error.
		_ = err
	}

	dbPath := filepath.Join(dir, fmt.Sprintf("chatlog-%s-%s.db", short, port))

	db, result := openAndVerify(dbPath)
	switch result {
	case openOK:
		// Database is healthy — proceed.

	case openCorrupt:
		// Genuine corruption — move the bad file aside and start fresh.
		corruptPath := dbPath + ".corrupt"
		log.Warn().Str("db_path", dbPath).Str("corrupt_path", corruptPath).Msg("chatlog integrity check failed, renaming")
		_ = os.Rename(dbPath, corruptPath)
		// Also move WAL and SHM sidecar files if present.
		_ = os.Rename(dbPath+"-wal", corruptPath+"-wal")
		_ = os.Rename(dbPath+"-shm", corruptPath+"-shm")

		db, result = openAndVerify(dbPath)
		if result != openOK {
			// Even a fresh database failed — give up.
			log.Error().Str("db_path", dbPath).Msg("chatlog cannot create fresh database")
			return &Store{identityAddr: identityAddr}
		}
		log.Info().Str("db_path", dbPath).Msg("chatlog created fresh database after corruption recovery")

	case openError:
		// Transient error (permissions, disk full, locked file, etc.).
		// Do NOT rename the file — it may be perfectly healthy.
		log.Error().Str("db_path", dbPath).Msg("chatlog open failed (transient error), running without persistence")
		return &Store{identityAddr: identityAddr}
	}

	return &Store{
		db:           db,
		identityAddr: identityAddr,
	}
}

// openResult describes the outcome of openAndVerify.
type openResult int

const (
	openOK      openResult = iota // database opened, schema OK, integrity OK
	openCorrupt                   // PRAGMA integrity_check reported corruption
	openError                     // transient error (permissions, disk full, etc.)
)

// openAndVerify opens the SQLite database at path, runs schema migration,
// and performs an integrity check. It distinguishes true corruption (where
// renaming the file and starting fresh is appropriate) from transient I/O
// errors (where renaming a healthy file would cause data loss).
func openAndVerify(dbPath string) (*sql.DB, openResult) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)")
	if err != nil {
		log.Error().Err(err).Str("db_path", dbPath).Msg("chatlog sql.Open failed")
		return nil, openError
	}

	if err := initSchema(db); err != nil {
		log.Error().Err(err).Str("db_path", dbPath).Msg("chatlog initSchema failed")
		_ = db.Close()
		// If the file exists and does not start with the SQLite magic
		// header, it is genuinely corrupt (not a transient I/O error).
		if looksCorrupt(dbPath) {
			return nil, openCorrupt
		}
		return nil, openError
	}

	if err := checkIntegrity(db); err != nil {
		log.Error().Err(err).Str("db_path", dbPath).Msg("chatlog integrity check failed")
		_ = db.Close()
		return nil, openCorrupt
	}

	return db, openOK
}

// sqliteMagic is the first 16 bytes of any valid SQLite database file.
var sqliteMagic = []byte("SQLite format 3\x00")

// looksCorrupt returns true if the file at path exists, has content, and does
// not start with the SQLite magic header — indicating it is genuinely corrupt
// rather than missing or locked.
func looksCorrupt(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false // file missing or unreadable — not necessarily corrupt
	}
	defer func() { _ = f.Close() }()

	header := make([]byte, len(sqliteMagic))
	n, err := f.Read(header)
	if err != nil || n == 0 {
		return false
	}
	// If the file has content but the header doesn't match, it's corrupt.
	for i := 0; i < len(sqliteMagic) && i < n; i++ {
		if header[i] != sqliteMagic[i] {
			return true
		}
	}
	return false
}

// checkIntegrity runs PRAGMA integrity_check and returns an error if the
// database reports corruption.
func checkIntegrity(db *sql.DB) error {
	var result string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("integrity_check query failed: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("integrity_check: %s", result)
	}
	return nil
}

func initSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS messages (
		id              TEXT PRIMARY KEY,
		topic           TEXT NOT NULL DEFAULT 'dm' CHECK(topic IN ('dm','global')),
		sender          TEXT NOT NULL,
		recipient       TEXT NOT NULL,
		body            TEXT NOT NULL,
		flag            TEXT NOT NULL DEFAULT '' CHECK(flag IN ('','immutable','sender-delete','any-delete','auto-delete-ttl')),
		delivery_status TEXT NOT NULL DEFAULT 'sent' CHECK(delivery_status IN ('sent','delivered','seen')),
		ttl_seconds     INTEGER NOT NULL DEFAULT 0,
		metadata        TEXT NOT NULL DEFAULT '',
		created_at      TEXT NOT NULL,
		updated_at      TEXT NOT NULL DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS idx_messages_peer
		ON messages(topic, sender, recipient, created_at);

	CREATE INDEX IF NOT EXISTS idx_messages_status
		ON messages(recipient, delivery_status);

	CREATE INDEX IF NOT EXISTS idx_messages_created
		ON messages(created_at DESC);

	CREATE INDEX IF NOT EXISTS idx_messages_ttl
		ON messages(flag, created_at) WHERE flag = 'auto-delete-ttl';
	`
	_, err := db.Exec(schema)
	return err
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// For DMs the peer is the other party; for global/broadcast use topic "global".
func (s *Store) Append(topic string, selfAddress domain.PeerIdentity, entry Entry) error {
	inserted, err := s.AppendReportNew(topic, selfAddress, entry)
	_ = inserted
	return err
}

// AppendReportNew works like Append but also reports whether the entry was
// actually inserted (true) or already existed (false). This allows callers
// to distinguish genuinely new messages from duplicates that were silently
// ignored by INSERT OR IGNORE, so they can suppress duplicate UI events.
func (s *Store) AppendReportNew(topic string, selfAddress domain.PeerIdentity, entry Entry) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("chatlog: database not available")
	}

	status := entry.DeliveryStatus
	if status == "" {
		status = StatusSent
	}

	res, err := s.db.Exec(`
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
func (s *Store) UpdateStatus(topic string, peerAddress domain.PeerIdentity, messageID domain.MessageID, status string) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("chatlog: database not available")
	}

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

	res, err := s.db.Exec(query, args...)
	if err != nil {
		return false, fmt.Errorf("chatlog: update status %s: %w", messageID, err)
	}

	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) Read(topic string, peerAddress domain.PeerIdentity) ([]Entry, error) {
	return s.ReadCtx(context.Background(), topic, peerAddress)
}

// ReadCtx is the context-aware variant of Read.  The context deadline is
// propagated to the underlying SQLite query so callers can bound I/O time.
func (s *Store) ReadCtx(ctx context.Context, topic string, peerAddress domain.PeerIdentity) ([]Entry, error) {
	if s.db == nil {
		return nil, nil
	}

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

func (s *Store) ReadLast(topic string, peerAddress domain.PeerIdentity, n int) ([]Entry, error) {
	return s.ReadLastCtx(context.Background(), topic, peerAddress, n)
}

// ReadLastCtx is the context-aware variant of ReadLast.
func (s *Store) ReadLastCtx(ctx context.Context, topic string, peerAddress domain.PeerIdentity, n int) ([]Entry, error) {
	if s.db == nil {
		return nil, nil
	}

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

// Conversations with unread messages are sorted first, then by last message time.
func (s *Store) ListConversations() ([]ConversationSummary, error) {
	return s.ListConversationsCtx(context.Background())
}

// ListConversationsCtx is the context-aware variant of ListConversations.
func (s *Store) ListConversationsCtx(ctx context.Context) ([]ConversationSummary, error) {
	if s.db == nil {
		return nil, nil
	}

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

func (s *Store) ReadLastEntry(topic string, peerAddress domain.PeerIdentity) (*Entry, error) {
	return s.ReadLastEntryCtx(context.Background(), topic, peerAddress)
}

// ReadLastEntryCtx is the context-aware variant of ReadLastEntry.
func (s *Store) ReadLastEntryCtx(ctx context.Context, topic string, peerAddress domain.PeerIdentity) (*Entry, error) {
	if s.db == nil {
		return nil, nil
	}

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

func (s *Store) ReadLastEntryPerPeer() (map[string]Entry, error) {
	return s.ReadLastEntryPerPeerCtx(context.Background())
}

// ReadLastEntryPerPeerCtx is the context-aware variant of ReadLastEntryPerPeer.
func (s *Store) ReadLastEntryPerPeerCtx(ctx context.Context) (map[string]Entry, error) {
	if s.db == nil {
		return nil, nil
	}

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
		if e.Recipient == string(selfAddr) {
			peer = e.Sender
		}
		result[peer] = e
	}

	return result, rows.Err()
}

// Returns the number of deleted rows.
func (s *Store) DeleteExpired() (int64, error) {
	if s.db == nil {
		return 0, nil
	}

	res, err := s.db.Exec(`
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
func (s *Store) DeleteByPeer(identity domain.PeerIdentity) (int64, error) {
	if s.db == nil {
		return 0, fmt.Errorf("chatlog: database not available")
	}
	id := string(identity)
	if strings.TrimSpace(id) == "" {
		return 0, fmt.Errorf("chatlog: empty identity")
	}

	res, err := s.db.Exec(`
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

// Returns true if a row was deleted.
func (s *Store) DeleteByID(messageID domain.MessageID) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("chatlog: database not available")
	}

	res, err := s.db.Exec(`DELETE FROM messages WHERE id = ?`, messageID)
	if err != nil {
		return false, fmt.Errorf("chatlog: delete %s: %w", messageID, err)
	}

	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) HasEntryID(topic string, peerAddress domain.PeerIdentity, id domain.MessageID) bool {
	if s.db == nil {
		return false
	}
	var exists int
	err := s.db.QueryRow(`SELECT 1 FROM messages WHERE id = ? LIMIT 1`, id).Scan(&exists)
	return err == nil
}

// HasEntryInConversation checks whether a message with the given ID exists
// within a specific DM conversation. Used to validate reply_to references
// before encrypting — prevents dangling or cross-conversation reply links.
func (s *Store) HasEntryInConversation(peerAddress domain.PeerIdentity, id domain.MessageID) bool {
	if s.db == nil || peerAddress == "" || id == "" {
		return false
	}
	query, params := s.peerQuery("dm", peerAddress,
		`SELECT 1 FROM messages WHERE id = ? AND `, ` LIMIT 1`)
	params = append([]interface{}{id}, params...)
	var exists int
	err := s.db.QueryRow(query, params...).Scan(&exists)
	return err == nil
}

// peerQuery builds a WHERE clause for messages in a specific conversation.
// For DMs it filters by (sender=self AND recipient=peer) OR (sender=peer AND recipient=self).
// For global it filters by topic='global'.
func (s *Store) peerQuery(topic string, peerAddress domain.PeerIdentity, prefix string, suffix string) (string, []interface{}) {
	if topic == "dm" && peerAddress != "" {
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

func portSuffix(listenAddress string) string {
	port := "default"
	if idx := strings.LastIndex(listenAddress, ":"); idx >= 0 && idx < len(listenAddress)-1 {
		port = listenAddress[idx+1:]
	}
	return port
}

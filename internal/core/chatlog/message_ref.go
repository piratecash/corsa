package chatlog

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ErrAmbiguousMessage means an old ID-only request names multiple messages.
// Callers must request an exact reference rather than picking a row, updating
// every match, or interpreting this refusal as absence/delivery confirmation.
var ErrAmbiguousMessage = domain.ErrAmbiguousMessage

type messageRows interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// uniqueMessageRef is only a compatibility resolver. Its caller can operate
// on the returned exact reference, never on the unqualified id again.
func uniqueMessageRef(ctx context.Context, db messageRows, id string) (domain.MessageRef, bool, error) {
	return uniqueMessageRefQuery(ctx, db, `SELECT topic,id,sender,recipient FROM messages WHERE id = ? LIMIT 2`, id)
}

func uniqueMessageRefQuery(ctx context.Context, db messageRows, query string, args ...any) (domain.MessageRef, bool, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return domain.MessageRef{}, false, err
	}
	defer func() { _ = rows.Close() }()
	var ref domain.MessageRef
	count := 0
	for rows.Next() {
		if err := rows.Scan(&ref.Topic, &ref.ID, &ref.Sender, &ref.Recipient); err != nil {
			return domain.MessageRef{}, false, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return domain.MessageRef{}, false, err
	}
	if count > 1 {
		return domain.MessageRef{}, false, ErrAmbiguousMessage
	}
	return ref, count == 1, nil
}

// MessageRefByID resolves a legacy global selector without losing its topic.
// No row is selected when more than one full reference has that wire ID.
func (s *Store) MessageRefByID(ctx context.Context, id domain.MessageID) (domain.MessageRef, bool, error) {
	return uniqueMessageRef(ctx, s.db, string(id))
}

// EntryByRef is the exact lookup used by callers which know both endpoints.
func (s *Store) EntryByRef(ctx context.Context, ref domain.MessageRef) (Entry, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,sender,recipient,body,created_at,flag,delivery_status,ttl_seconds,metadata,rowid FROM messages WHERE topic=? AND id=? AND sender=? AND recipient=?`, ref.Topic, ref.ID, ref.Sender, ref.Recipient)
	var entry Entry
	err := row.Scan(&entry.ID, &entry.Sender, &entry.Recipient, &entry.Body, &entry.CreatedAt, &entry.Flag, &entry.DeliveryStatus, &entry.TTLSeconds, &entry.Metadata, &entry.RowID)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	return entry, err == nil, err
}

func (s *Store) MessageSeqByRef(ctx context.Context, ref domain.MessageRef) (int64, bool, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx, `SELECT rowid FROM messages WHERE topic=? AND id=? AND sender=? AND recipient=?`, ref.Topic, ref.ID, ref.Sender, ref.Recipient).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return seq, err == nil, err
}

func (s *Store) MarkDeliveryFailedRef(ctx context.Context, ref domain.MessageRef) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO delivery_failed(topic,id,sender,recipient) SELECT topic,id,sender,recipient FROM messages WHERE topic=? AND id=? AND sender=? AND recipient=? ON CONFLICT(topic,id,sender,recipient) DO NOTHING`, ref.Topic, ref.ID, ref.Sender, ref.Recipient)
	return err
}

func (s *Store) MarkSeenConfirmedRef(ctx context.Context, ref domain.MessageRef) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO seen_ack(topic,id,sender,recipient) SELECT topic,id,sender,recipient FROM messages WHERE topic=? AND id=? AND sender=? AND recipient=? ON CONFLICT(topic,id,sender,recipient) DO NOTHING`, ref.Topic, ref.ID, ref.Sender, ref.Recipient)
	return err
}

// SentMessageRefs seeds the bounded solicited-receipt set without merging
// messages this identity sent to different recipients under the same wire id.
func (s *Store) SentMessageRefs(ctx context.Context, self domain.PeerIdentity, limit int) ([]domain.MessageRef, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT topic,id,sender,recipient FROM messages WHERE topic='dm' AND sender=? ORDER BY created_at DESC LIMIT ?`, self.String(), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var refs []domain.MessageRef
	for rows.Next() {
		var ref domain.MessageRef
		if err := rows.Scan(&ref.Topic, &ref.ID, &ref.Sender, &ref.Recipient); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func (s *Store) EntryInConversation(ctx context.Context, peer domain.PeerIdentity, id domain.MessageID) (Entry, bool, error) {
	ref, found, err := s.RefInConversation(ctx, peer, id)
	if err != nil || !found {
		return Entry{}, false, err
	}
	return s.EntryByRef(ctx, ref)
}

func (s *Store) DeleteByRef(ctx context.Context, ref domain.MessageRef) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	removed, err := deleteMessageRefTx(ctx, tx, ref, s.identityAddr)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return removed, nil
}

func (s *Store) DeleteInConversation(ctx context.Context, peer domain.PeerIdentity, id domain.MessageID) (bool, error) {
	if peer.IsZero() || id == "" {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	query, args := s.peerQuery("dm", peer, `SELECT topic,id,sender,recipient FROM messages WHERE id=? AND `, ` LIMIT 2`)
	args = append([]interface{}{id}, args...)
	ref, found, err := uniqueMessageRefQuery(ctx, tx, query, args...)
	if err != nil || !found {
		return false, err
	}
	removed, err := deleteMessageRefTx(ctx, tx, ref, s.identityAddr)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return removed, nil
}

// StoredMessageRefStatuses reads a bounded batch without merging another
// author's or recipient's status into an ID-only result map.
func (s *Store) StoredMessageRefStatuses(ctx context.Context, refs []domain.MessageRef) (map[domain.MessageRef]string, error) {
	result := make(map[domain.MessageRef]string, len(refs))
	const batch = storedStatusChunk / 4
	for start := 0; start < len(refs); start += batch {
		end := min(start+batch, len(refs))
		terms := make([]string, 0, end-start)
		args := make([]any, 0, 4*(end-start))
		for _, ref := range refs[start:end] {
			terms = append(terms, "(?,?,?,?)")
			args = append(args, ref.Topic, ref.ID, ref.Sender, ref.Recipient)
		}
		rows, err := s.db.QueryContext(ctx, `SELECT topic,id,sender,recipient,delivery_status FROM messages WHERE (topic,id,sender,recipient) IN (`+strings.Join(terms, ",")+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var ref domain.MessageRef
			var status string
			if err := rows.Scan(&ref.Topic, &ref.ID, &ref.Sender, &ref.Recipient, &status); err != nil {
				_ = rows.Close()
				return nil, err
			}
			result[ref] = status
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Store) RefInConversation(ctx context.Context, peer domain.PeerIdentity, id domain.MessageID) (domain.MessageRef, bool, error) {
	if peer.IsZero() || id == "" {
		return domain.MessageRef{}, false, nil
	}
	query, args := s.peerQuery("dm", peer, `SELECT topic,id,sender,recipient FROM messages WHERE id=? AND `, ` LIMIT 2`)
	args = append([]interface{}{id}, args...)
	return uniqueMessageRefQuery(ctx, s.db, query, args...)
}

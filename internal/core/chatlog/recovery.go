package chatlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// recovery.go is the storage half of the §4.10 decrypt-recovery subsystem
// (docs/protocol/identity-lookup.md): per-row recovery marks, the durable
// per-peer job table and the monotonic "established" facts the quota
// scheduler reserves slots for.
//
// The per-row marks live in the existing `metadata` JSON column — the
// chatlog's documented forward-compatible path (docs/chatlog.md): the marks
// are exactly the kind of additive property the column exists for, and a
// rolled-back binary keeps reading the same rows. The job, cycle, intent and
// established tables are declared by the shared migration catalog
// (internal/core/storage/migrations); this file only queries them.

// Per-message decrypt states (§4.10): the lifecycle of one flagged row.
const (
	DecryptStatePendingNotice = "pending_notice"
	DecryptStateWaitingRetry  = "waiting_retry"
	DecryptStateRecovered     = "recovered"
	DecryptStateExpired       = "expired"
)

// entryRecoveryMetadata is the metadata-JSON projection of the recovery
// marks. Unknown keys already present in the column survive: the accessors
// re-marshal the FULL decoded map, not this struct.
type entryRecoveryMetadata struct {
	DecryptFailed bool   `json:"decrypt_failed,omitempty"`
	DecryptState  string `json:"decrypt_state,omitempty"`
	// DecryptFlaggedAt (RFC3339Nano) is when the failure was first
	// confirmed — the durable anchor of the §4.10 seven-day deadline: a
	// job re-admitted after an eviction must inherit the ORIGINAL clock,
	// not restart it.
	DecryptFlaggedAt string `json:"decrypt_flagged_at,omitempty"`
	SupersededBy     string `json:"superseded_by,omitempty"`
	RetryRootID      string `json:"retry_root_id,omitempty"`
}

// EntryRecoveryMarks reads the recovery marks of one row.
func (s *Store) EntryRecoveryMarks(ctx context.Context, id string) (entryRecoveryMetadata, bool, error) {
	var metadata sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT metadata FROM messages WHERE id = ?`, id).Scan(&metadata)
	if err == sql.ErrNoRows {
		return entryRecoveryMetadata{}, false, nil
	}
	if err != nil {
		return entryRecoveryMetadata{}, false, fmt.Errorf("read metadata %s: %w", id, err)
	}
	marks, err := decodeRecoveryMarks(metadata.String)
	if err != nil {
		return entryRecoveryMetadata{}, false, err
	}
	return marks, true, nil
}

func decodeRecoveryMarks(metadata string) (entryRecoveryMetadata, error) {
	if strings.TrimSpace(metadata) == "" {
		return entryRecoveryMetadata{}, nil
	}
	var marks entryRecoveryMetadata
	if err := json.Unmarshal([]byte(metadata), &marks); err != nil {
		// A metadata blob some other feature wrote in a non-object shape is
		// not a recovery failure; it simply carries no marks.
		return entryRecoveryMetadata{}, nil
	}
	return marks, nil
}

// mergeMetadata applies changes into the row's metadata JSON, preserving
// every unknown key.
func (s *Store) mergeMetadata(ctx context.Context, id string, apply func(map[string]any)) (bool, error) {
	var metadata sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT metadata FROM messages WHERE id = ?`, id).Scan(&metadata)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read metadata %s: %w", id, err)
	}
	fields := map[string]any{}
	if strings.TrimSpace(metadata.String) != "" {
		// A non-object blob is replaced: nothing in the codebase writes one,
		// and refusing the merge would wedge recovery forever.
		_ = json.Unmarshal([]byte(metadata.String), &fields)
		if fields == nil {
			fields = map[string]any{}
		}
	}
	apply(fields)
	encoded, err := json.Marshal(fields)
	if err != nil {
		return false, fmt.Errorf("encode metadata %s: %w", id, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE messages SET metadata = ?, updated_at = ? WHERE id = ?`,
		string(encoded), time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return false, fmt.Errorf("write metadata %s: %w", id, err)
	}
	return true, nil
}

// MarkDecryptFailed flags one row as a confirmed crypto-fail and puts it
// into the pending_notice state. ONE conditional UPDATE: the "not yet
// flagged, not superseded" check and the write must be a single atomic
// statement, or a late concurrent report could resurrect a row that
// recovery already superseded — flipping recovered history back into the
// live workset. Returns changed=false when the row does not exist, is
// already flagged (the §4.10 idempotency suppressor: UI renders must not
// multiply jobs) or is already superseded.
func (s *Store) MarkDecryptFailed(ctx context.Context, id string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// A metadata blob that is not a JSON OBJECT (empty, invalid, or a
	// valid array/string/number) is replaced wholesale: json_set on a
	// non-object silently returns it unchanged while still counting the
	// row as affected — the flag would be reported written without a
	// single recovery field landing.
	result, err := s.db.ExecContext(ctx, `
		UPDATE messages
		SET metadata = json_set(
			CASE WHEN metadata IS NULL OR NOT json_valid(metadata) OR json_type(metadata) <> 'object'
			     THEN '{}' ELSE metadata END,
			'$.decrypt_failed', json('true'),
			'$.decrypt_state', ?,
			'$.decrypt_flagged_at', ?),
		    updated_at = ?
		WHERE id = ?
		  AND (metadata IS NULL OR NOT json_valid(metadata) OR json_type(metadata) <> 'object'
		       OR (COALESCE(json_extract(metadata, '$.decrypt_failed'), 0) <> 1
		           AND COALESCE(json_extract(metadata, '$.superseded_by'), '') = ''))`,
		DecryptStatePendingNotice, now, now, id)
	if err != nil {
		return false, fmt.Errorf("mark decrypt-failed %s: %w", id, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark decrypt-failed %s: rows affected: %w", id, err)
	}
	return affected > 0, nil
}

// OldestDecryptFlaggedAt returns the earliest confirmed-failure time among
// the peer's LIVE flagged rows — the anchor the job deadline derives from.
// found=false when the peer has no live flagged rows.
func (s *Store) OldestDecryptFlaggedAt(ctx context.Context, peer, self string) (time.Time, bool, error) {
	var oldest sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT MIN(json_extract(metadata, '$.decrypt_flagged_at'))
		FROM messages
		WHERE topic = 'dm' AND sender = ? AND recipient = ?
		  AND `+recoveryLiveFlaggedCondition, peer, self).Scan(&oldest)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("oldest flagged-at %s: %w", peer, err)
	}
	if !oldest.Valid || oldest.String == "" {
		return time.Time{}, false, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, oldest.String)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse flagged-at %q: %w", oldest.String, err)
	}
	return parsed, true, nil
}

// ExpireDecryptFailed moves EVERY live flagged row of the peer to the
// terminal expired state in one statement — the job's hard deadline must
// reach all rows, not a workset-sized prefix of them.
func (s *Store) ExpireDecryptFailed(ctx context.Context, peer, self string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE messages
		SET metadata = json_set(metadata, '$.decrypt_state', ?), updated_at = ?
		WHERE topic = 'dm' AND sender = ? AND recipient = ?
		  AND `+recoveryLiveFlaggedCondition,
		DecryptStateExpired, time.Now().UTC().Format(time.RFC3339Nano), peer, self)
	if err != nil {
		return fmt.Errorf("expire decrypt-failed rows of %s: %w", peer, err)
	}
	return nil
}

// SetDecryptState moves one flagged row to the given state.
func (s *Store) SetDecryptState(ctx context.Context, id, state string) error {
	_, err := s.mergeMetadata(ctx, id, func(fields map[string]any) {
		fields["decrypt_state"] = state
	})
	return err
}

// mergeMetadataTx is mergeMetadata inside a caller-owned transaction —
// the building block of multi-row metadata terminals.
func mergeMetadataTx(ctx context.Context, tx *sql.Tx, id string, apply func(map[string]any)) (bool, error) {
	var metadata sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT metadata FROM messages WHERE id = ?`, id).Scan(&metadata)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read metadata %s: %w", id, err)
	}
	fields := map[string]any{}
	if strings.TrimSpace(metadata.String) != "" {
		_ = json.Unmarshal([]byte(metadata.String), &fields)
		if fields == nil {
			fields = map[string]any{}
		}
	}
	apply(fields)
	encoded, err := json.Marshal(fields)
	if err != nil {
		return false, fmt.Errorf("encode metadata %s: %w", id, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE messages SET metadata = ?, updated_at = ? WHERE id = ?`,
		string(encoded), time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return false, fmt.Errorf("write metadata %s: %w", id, err)
	}
	return true, nil
}

// MarkResendTerminal is the SENDER-side terminal in ONE transaction: the
// original row gets its supersede link (metadata only — its real
// delivery_status stays, 'seen' there would forge a peer confirmation)
// and the replacement row gets its chain stamp. Split writes would let a
// crash between them re-open the original for ordinary retry while the
// replacement is already on the wire, and an unstamped replacement would
// reset the §4.10 chain budget.
func (s *Store) MarkResendTerminal(ctx context.Context, originalID, replacementID, rootID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin resend terminal tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	changed, err := mergeMetadataTx(ctx, tx, originalID, func(fields map[string]any) {
		fields["decrypt_failed"] = false
		fields["decrypt_state"] = DecryptStateRecovered
		fields["superseded_by"] = replacementID
		fields["retry_root_id"] = rootID
	})
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("resend terminal: original %s not found", originalID)
	}
	changed, err = mergeMetadataTx(ctx, tx, replacementID, func(fields map[string]any) {
		fields["retry_root_id"] = rootID
	})
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("resend terminal: replacement %s not found", replacementID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit resend terminal %s: %w", originalID, err)
	}
	return nil
}

// MarkSupersededCollapsing is the RECEIVER-side supersede: the metadata
// link and the unread collapse (the undecryptable original stops counting;
// its replacement counts once) commit in ONE transaction, and the "still
// flagged, not yet superseded" precondition is re-checked INSIDE it —
// concurrent readers (the live event and a history load decrypting the
// same replacement, or two different replacements) race to this call, and
// only the first may write the link; the loser reports applied=false and
// must not touch the rows. applied=false with a nil error also covers a
// missing original.
func (s *Store) MarkSupersededCollapsing(ctx context.Context, originalID, replacementID, rootID string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin supersede tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var metadata sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT metadata FROM messages WHERE id = ?`, originalID).Scan(&metadata)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read metadata %s: %w", originalID, err)
	}
	marks, err := decodeRecoveryMarks(metadata.String)
	if err != nil {
		return false, err
	}
	if !marks.DecryptFailed || marks.SupersededBy != "" {
		return false, tx.Commit() // lost the race (or never flagged): leave the winner's link alone
	}
	fields := map[string]any{}
	if strings.TrimSpace(metadata.String) != "" {
		_ = json.Unmarshal([]byte(metadata.String), &fields)
		if fields == nil {
			fields = map[string]any{}
		}
	}
	fields["decrypt_failed"] = false
	fields["decrypt_state"] = DecryptStateRecovered
	fields["superseded_by"] = replacementID
	fields["retry_root_id"] = rootID
	encoded, err := json.Marshal(fields)
	if err != nil {
		return false, fmt.Errorf("encode metadata %s: %w", originalID, err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `UPDATE messages SET metadata = ?, delivery_status = 'seen', updated_at = ? WHERE id = ?`,
		string(encoded), now, originalID); err != nil {
		return false, fmt.Errorf("supersede %s: %w", originalID, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit supersede %s: %w", originalID, err)
	}
	return true, nil
}

// recoveryLiveFlaggedCondition is the SQL predicate for "this row is a
// LIVE decrypt failure": really flagged, and not in the terminal expired
// state (§4.10 — expired rows keep their flag for the UI but never
// re-enter a workset). The check runs on the JSON itself via json_extract,
// never on a LIKE substring: the filter interacts with LIMIT, so a false
// positive would consume a slot and a false negative would hide a live row
// behind the cap — both wrong in ways a post-scan Go filter cannot repair.
const recoveryLiveFlaggedCondition = `
	json_valid(metadata)
	AND json_extract(metadata, '$.decrypt_failed') = 1
	AND COALESCE(json_extract(metadata, '$.decrypt_state'), '') <> '` + DecryptStateExpired + `'`

// DecryptFailedEntries returns up to limit LIVE flagged rows of one peer's
// conversation — the bounded workset of the peer's recovery job (§4.10:
// accounting is unbounded via the flags, only the NETWORK work is
// slot-limited).
func (s *Store) DecryptFailedEntries(ctx context.Context, peer, self string, limit int) ([]Entry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, sender, recipient, body, created_at, flag, delivery_status, COALESCE(ttl_seconds, 0), COALESCE(metadata, '')
		FROM messages
		WHERE topic = 'dm' AND sender = ? AND recipient = ?
		  AND `+recoveryLiveFlaggedCondition+`
		ORDER BY created_at ASC
		LIMIT ?`, peer, self, limit)
	if err != nil {
		return nil, fmt.Errorf("select decrypt-failed rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Entry
	for rows.Next() {
		var entry Entry
		if err := rows.Scan(&entry.ID, &entry.Sender, &entry.Recipient, &entry.Body,
			&entry.CreatedAt, &entry.Flag, &entry.DeliveryStatus, &entry.TTLSeconds, &entry.Metadata); err != nil {
			return nil, fmt.Errorf("scan decrypt-failed row: %w", err)
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

// RecoveryOrphanPeers returns up to limit peers that have live flagged rows
// addressed to self but NO recovery job — the reconciliation feed: a
// refused admission (backlog full) or an eviction leaves the row flags in
// place, and this query is how those rows get their job back once slots
// free up. Without it a refused peer would wait forever: the row flag
// suppresses repeat Reports (changed=false), so no later report re-attempts
// admission.
func (s *Store) RecoveryOrphanPeers(ctx context.Context, self string, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT sender FROM messages
		WHERE topic = 'dm' AND recipient = ?
		  AND `+recoveryLiveFlaggedCondition+`
		  AND sender NOT IN (SELECT peer FROM decrypt_recovery_jobs)
		LIMIT ?`, self, limit)
	if err != nil {
		return nil, fmt.Errorf("select recovery orphans: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var peer string
		if err := rows.Scan(&peer); err != nil {
			return nil, fmt.Errorf("scan recovery orphan: %w", err)
		}
		out = append(out, peer)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// The per-peer recovery job table
// ---------------------------------------------------------------------------

// RecoveryJob is one peer's durable recovery task. Granularity is per-peer
// by §4.10: the key generation cannot be computed from an unreadable row.
// The job outlives a successful key resolve — restarts resume the notice
// retry, they never restart the lookup.
type RecoveryJob struct {
	Peer           string
	State          string
	NoticeAttempts int
	LastNoticeAt   time.Time
	WaitUntil      time.Time
	CreatedAt      time.Time
	ExpiresAt      time.Time
}

// ---------------------------------------------------------------------------
// Recovery cycle anchors
// ---------------------------------------------------------------------------

// EnsureRecoveryCycle returns the peer's IMMUTABLE recovery-cycle anchor,
// creating it from candidate on first call. The §4.10 seven-day deadline
// derives from this row, not from the flagged rows: rows recover one by
// one and jobs get evicted, but the cycle's clock must never restart until
// the cycle itself closes — otherwise a flood could roll the anchor
// forward (recover the oldest row, evict, re-admit) indefinitely.
func (s *Store) EnsureRecoveryCycle(ctx context.Context, peer string, candidate time.Time) (time.Time, error) {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO decrypt_recovery_cycles (peer, anchored_at)
		VALUES (?, ?)
		ON CONFLICT(peer) DO NOTHING`,
		peer, candidate.UTC().Format(time.RFC3339Nano)); err != nil {
		return time.Time{}, fmt.Errorf("ensure recovery cycle %s: %w", peer, err)
	}
	var anchored string
	if err := s.db.QueryRowContext(ctx, `SELECT anchored_at FROM decrypt_recovery_cycles WHERE peer = ?`, peer).Scan(&anchored); err != nil {
		return time.Time{}, fmt.Errorf("read recovery cycle %s: %w", peer, err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, anchored)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse recovery cycle anchor %q: %w", anchored, err)
	}
	return parsed, nil
}

// CloseRecoveryCycleIfIdle atomically closes the peer's job AND cycle
// anchor, but ONLY when no live flagged row remains — the check and both
// deletes are one transaction, or a fresh failure landing between a
// caller's own emptiness check and the close would lose the anchor while
// live work exists, handing the peer a brand-new seven-day clock through
// the next re-admission. Returns whether the close happened. Never called
// on an eviction, which must keep the original clock.
func (s *Store) CloseRecoveryCycleIfIdle(ctx context.Context, peer, self string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin cycle close %s: %w", peer, err)
	}
	defer func() { _ = tx.Rollback() }()

	var one int
	err = tx.QueryRowContext(ctx, `
		SELECT 1 FROM messages
		WHERE topic = 'dm' AND sender = ? AND recipient = ?
		  AND `+recoveryLiveFlaggedCondition+`
		LIMIT 1`, peer, self).Scan(&one)
	if err == nil {
		return false, tx.Commit() // live work remains: the cycle stays anchored
	}
	if err != sql.ErrNoRows {
		return false, fmt.Errorf("probe live rows %s: %w", peer, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM decrypt_recovery_jobs WHERE peer = ?`, peer); err != nil {
		return false, fmt.Errorf("close recovery job %s: %w", peer, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM decrypt_recovery_cycles WHERE peer = ?`, peer); err != nil {
		return false, fmt.Errorf("close recovery cycle %s: %w", peer, err)
	}
	return true, tx.Commit()
}

// StaleRecoveryCycles lists cycle anchors whose peer has no job — the
// candidates for the close sweep (their rows may have all recovered while
// the peer had no job to run the close path).
func (s *Store) StaleRecoveryCycles(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT peer FROM decrypt_recovery_cycles
		WHERE peer NOT IN (SELECT peer FROM decrypt_recovery_jobs)
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("select stale recovery cycles: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var peer string
		if err := rows.Scan(&peer); err != nil {
			return nil, fmt.Errorf("scan stale recovery cycle: %w", err)
		}
		out = append(out, peer)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Durable resend intents (the sender-leg crash insurance)
// ---------------------------------------------------------------------------

// ResendIntent is one durable "a replacement may be on the wire" record:
// written BEFORE the send with the PRE-MINTED replacement id, deleted
// after the terminal transaction. A crash in between leaves the intent,
// and the reconciliation checks the named replacement row directly — no
// search, no heuristics: the id was fixed before the send existed.
type ResendIntent struct {
	Root          string
	OriginalID    string
	Peer          string
	ReplacementID string
	CreatedAt     time.Time
}

// AdmitResendIntent is the bounded SENDER-side backlog admission (§4.10:
// overflow is never silently dropped — it lands in a bounded backlog with
// the established reservation). One intent per root; at most perPeerLimit
// intents per peer (refused beyond — the per-peer bound of the task). The
// global bound is SHARED with the receiver jobs: over globalLimit the
// oldest not-established row of either table is evicted (protected work —
// resend roots with a send possibly in flight — are never victims), and
// when every slot belongs to an established peer the newcomer is refused.
// Returns the CANONICAL intent for the root — the pre-existing one when
// the root was already admitted, so a retried notice reuses the SAME
// replacement id instead of minting a divergent one.
func (s *Store) AdmitResendIntent(ctx context.Context, intent ResendIntent, perPeerLimit, globalLimit int, protected RecoveryProtectedWork) (canonical ResendIntent, admitted bool, victim RecoveryEvictionVictim, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ResendIntent{}, false, RecoveryEvictionVictim{}, fmt.Errorf("begin resend admission: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, found, err := scanResendIntentRow(tx.QueryRowContext(ctx, `
		SELECT root, original_id, peer, replacement_id, created_at
		FROM decrypt_resend_intents WHERE root = ?`, intent.Root))
	if err != nil {
		return ResendIntent{}, false, RecoveryEvictionVictim{}, err
	}
	if found {
		return existing, true, RecoveryEvictionVictim{}, tx.Commit()
	}

	var proceed bool
	proceed, victim, err = admitRecoveryBacklogTx(ctx, tx, intent.Peer, perPeerLimit, globalLimit, true, protected)
	if err != nil {
		return ResendIntent{}, false, RecoveryEvictionVictim{}, err
	}
	if !proceed {
		return ResendIntent{}, false, RecoveryEvictionVictim{}, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO decrypt_resend_intents (root, original_id, peer, replacement_id, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		intent.Root, intent.OriginalID, intent.Peer, intent.ReplacementID,
		intent.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
		return ResendIntent{}, false, RecoveryEvictionVictim{}, fmt.Errorf("insert resend intent %s: %w", intent.Root, err)
	}
	return intent, true, victim, tx.Commit()
}

// DeleteResendIntent removes a settled intent (terminal written, or the
// send conclusively never happened).
func (s *Store) DeleteResendIntent(ctx context.Context, root string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM decrypt_resend_intents WHERE root = ?`, root); err != nil {
		return fmt.Errorf("delete resend intent %s: %w", root, err)
	}
	return nil
}

// resendIntentScanner abstracts sql.Row / sql.Rows for the shared scan.
type resendIntentScanner interface {
	Scan(dest ...any) error
}

func scanResendIntentRow(row resendIntentScanner) (ResendIntent, bool, error) {
	var intent ResendIntent
	var createdAt string
	err := row.Scan(&intent.Root, &intent.OriginalID, &intent.Peer, &intent.ReplacementID, &createdAt)
	if err == sql.ErrNoRows {
		return ResendIntent{}, false, nil
	}
	if err != nil {
		return ResendIntent{}, false, fmt.Errorf("scan resend intent: %w", err)
	}
	intent.CreatedAt = parseOptionalTime(createdAt)
	return intent, true, nil
}

// ResendIntentByRoot reads one durable intent — the pre-send re-check of
// tryResend: an intent evicted between activation and the send means the
// crash insurance is gone, and a replacement must never leave without it.
func (s *Store) ResendIntentByRoot(ctx context.Context, root string) (ResendIntent, bool, error) {
	return scanResendIntentRow(s.db.QueryRowContext(ctx, `
		SELECT root, original_id, peer, replacement_id, created_at
		FROM decrypt_resend_intents WHERE root = ?`, root))
}

// ResendIntents lists durable intents, oldest first, bounded.
func (s *Store) ResendIntents(ctx context.Context, limit int) ([]ResendIntent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT root, original_id, peer, replacement_id, created_at
		FROM decrypt_resend_intents
		ORDER BY created_at ASC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("select resend intents: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ResendIntent
	for rows.Next() {
		intent, _, err := scanResendIntentRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, rows.Err()
}

// UpsertRecoveryJob opens the peer's job if none exists. An existing job is
// left untouched — one peer, one job, whatever the number of flagged rows.
func (s *Store) UpsertRecoveryJob(ctx context.Context, peer string, now, expiresAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO decrypt_recovery_jobs (peer, state, created_at, expires_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(peer) DO NOTHING`,
		peer, DecryptStatePendingNotice, now.UTC().Format(time.RFC3339Nano), expiresAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("upsert recovery job %s: %w", peer, err)
	}
	return nil
}

// RecoveryEvictionVictim names the backlog row an admission displaced:
// a receiver job (Job=true, Key=peer) or a sender resend intent
// (Job=false, Key=root). Peer is always the row's peer — for a resend
// victim the caller needs it to release the peer's resolver work even
// when the intent was never restored into memory (a crashed
// predecessor's row evicted before any activation).
type RecoveryEvictionVictim struct {
	Job  bool
	Key  string
	Peer string
}

// None reports "nothing was evicted".
func (v RecoveryEvictionVictim) None() bool { return v.Key == "" }

// RecoveryProtectedWork names the backlog rows a live attempt depends on:
// resend roots with a send possibly in flight and job peers with a notice
// attempt running. Eviction never picks them — pulling durable state out
// from under a running attempt would strand a sent replacement without
// its crash insurance, or leave a notice updating a deleted job.
type RecoveryProtectedWork struct {
	ResendRoots []string
	JobPeers    []string
}

// admitRecoveryBacklogTx is the ONE §4.10 backlog gate both admissions run
// through — every quota counts jobs and resend intents TOGETHER, or each
// leg would get its own copy of the bound:
//
//   - per peer: at most perPeerLimit rows across both tables;
//   - globally: at most globalLimit rows across both tables;
//   - reservation: an unknown (not-established) newcomer may occupy at
//     most half of globalLimit — established peers can always claim the
//     other half, so 200 Sybil rows can never own the whole backlog.
//
// Over a cap the oldest not-established row of either table is evicted
// (LRU; protected work — in-flight sends and running notice attempts — is never a victim;
// established rows never leave for an unknown newcomer); with eviction
// disallowed, or nothing evictable, the newcomer is refused. Returns
// whether the caller may INSERT its row.
func admitRecoveryBacklogTx(ctx context.Context, tx *sql.Tx, peer string, perPeerLimit, globalLimit int, allowEvict bool, protected RecoveryProtectedWork) (bool, RecoveryEvictionVictim, error) {
	var perPeer int
	err := tx.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM decrypt_recovery_jobs WHERE peer = ?)
		     + (SELECT COUNT(*) FROM decrypt_resend_intents WHERE peer = ?)`, peer, peer).Scan(&perPeer)
	if err != nil {
		return false, RecoveryEvictionVictim{}, fmt.Errorf("count peer backlog %s: %w", peer, err)
	}
	if perPeer >= perPeerLimit {
		return false, RecoveryEvictionVictim{}, nil
	}

	var total int
	err = tx.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM decrypt_recovery_jobs)
		     + (SELECT COUNT(*) FROM decrypt_resend_intents)`).Scan(&total)
	if err != nil {
		return false, RecoveryEvictionVictim{}, fmt.Errorf("count recovery backlog: %w", err)
	}
	overCap := total >= globalLimit

	var newcomerEstablished int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM peer_established WHERE peer = ?`, peer).Scan(&newcomerEstablished)
	if err != nil {
		return false, RecoveryEvictionVictim{}, fmt.Errorf("probe established %s: %w", peer, err)
	}
	if newcomerEstablished == 0 && !overCap {
		var unknown int
		err = tx.QueryRowContext(ctx, `
			SELECT (SELECT COUNT(*) FROM decrypt_recovery_jobs j
			        LEFT JOIN peer_established e ON e.peer = j.peer WHERE e.peer IS NULL)
			     + (SELECT COUNT(*) FROM decrypt_resend_intents i
			        LEFT JOIN peer_established e ON e.peer = i.peer WHERE e.peer IS NULL)`).Scan(&unknown)
		if err != nil {
			return false, RecoveryEvictionVictim{}, fmt.Errorf("count unknown backlog: %w", err)
		}
		overCap = unknown >= globalLimit/recoveryBacklogUnknownShare
	}
	if !overCap {
		return true, RecoveryEvictionVictim{}, nil
	}
	if !allowEvict {
		return false, RecoveryEvictionVictim{}, nil
	}
	victim, err := evictRecoveryBacklogTx(ctx, tx, protected)
	if err != nil {
		return false, RecoveryEvictionVictim{}, err
	}
	if victim.None() {
		return false, RecoveryEvictionVictim{}, nil
	}
	return true, victim, nil
}

// recoveryBacklogUnknownShare mirrors the active-slot reservation on the
// backlog: unknown peers own at most 1/share of the global bound.
const recoveryBacklogUnknownShare = 2

// evictRecoveryBacklogTx removes the oldest NOT-established row across
// BOTH backlog tables (unknown peers go first, by LRU; established rows
// never leave for a newcomer). protected names the rows a live attempt
// depends on — resend roots with a send possibly in flight and job peers
// with a notice attempt running: evicting either would pull durable state
// out from under the attempt (a sent replacement stranded without its
// crash-reconciliation intent; a notice updating a deleted job).
func evictRecoveryBacklogTx(ctx context.Context, tx *sql.Tx, protected RecoveryProtectedWork) (RecoveryEvictionVictim, error) {
	query := `
		SELECT kind, key, peer FROM (
			SELECT 'job' AS kind, j.peer AS key, j.peer AS peer, j.created_at AS created_at
			FROM decrypt_recovery_jobs j
			LEFT JOIN peer_established e ON e.peer = j.peer
			WHERE e.peer IS NULL`
	args := make([]any, 0, len(protected.ResendRoots)+len(protected.JobPeers))
	if len(protected.JobPeers) > 0 {
		query += ` AND j.peer NOT IN (?` + strings.Repeat(", ?", len(protected.JobPeers)-1) + `)`
		for _, peer := range protected.JobPeers {
			args = append(args, peer)
		}
	}
	query += `
			UNION ALL
			SELECT 'resend', i.root, i.peer, i.created_at
			FROM decrypt_resend_intents i
			LEFT JOIN peer_established e ON e.peer = i.peer
			WHERE e.peer IS NULL`
	if len(protected.ResendRoots) > 0 {
		query += ` AND i.root NOT IN (?` + strings.Repeat(", ?", len(protected.ResendRoots)-1) + `)`
		for _, root := range protected.ResendRoots {
			args = append(args, root)
		}
	}
	query += `
		) ORDER BY created_at ASC LIMIT 1`

	var kind, key, peer string
	err := tx.QueryRowContext(ctx, query, args...).Scan(&kind, &key, &peer)
	if err == sql.ErrNoRows {
		return RecoveryEvictionVictim{}, nil
	}
	if err != nil {
		return RecoveryEvictionVictim{}, fmt.Errorf("pick backlog eviction victim: %w", err)
	}
	victim := RecoveryEvictionVictim{Job: kind == "job", Key: key, Peer: peer}
	if victim.Job {
		_, err = tx.ExecContext(ctx, `DELETE FROM decrypt_recovery_jobs WHERE peer = ?`, key)
	} else {
		_, err = tx.ExecContext(ctx, `DELETE FROM decrypt_resend_intents WHERE root = ?`, key)
	}
	if err != nil {
		return RecoveryEvictionVictim{}, fmt.Errorf("evict backlog row %s: %w", key, err)
	}
	return victim, nil
}

// AdmitRecoveryJob is the BOUNDED job admission — one caller of the §4.10
// backlog gate (see admitRecoveryBacklogTx for the quota model). Returns
// whether the job now exists and the eviction victim — the caller must
// release the victim's in-flight work; a job victim's row flags stay, so
// the orphan reconciliation re-admits it when a slot frees.
func (s *Store) AdmitRecoveryJob(ctx context.Context, peer string, now, expiresAt time.Time, perPeerLimit, limit int, protected RecoveryProtectedWork) (bool, RecoveryEvictionVictim, error) {
	return s.admitRecoveryJob(ctx, peer, now, expiresAt, perPeerLimit, limit, true, protected)
}

// AdmitRecoveryJobIfRoom admits only into a FREE slot — the reconciliation
// path. Orphans are by definition the jobs the eviction policy already
// judged against the current backlog: letting the sweep evict would make
// every pass rotate a full-of-unknowns backlog (evict one orphan's way in,
// orphan the victim, re-admit it next pass...), resetting created_at and
// the 7-day lifetime on each turn and starving everyone of attempts.
func (s *Store) AdmitRecoveryJobIfRoom(ctx context.Context, peer string, now, expiresAt time.Time, perPeerLimit, limit int) (bool, error) {
	admitted, _, err := s.admitRecoveryJob(ctx, peer, now, expiresAt, perPeerLimit, limit, false, RecoveryProtectedWork{})
	return admitted, err
}

func (s *Store) admitRecoveryJob(ctx context.Context, peer string, now, expiresAt time.Time, perPeerLimit, limit int, allowEvict bool, protected RecoveryProtectedWork) (bool, RecoveryEvictionVictim, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, RecoveryEvictionVictim{}, fmt.Errorf("begin job admission: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM decrypt_recovery_jobs WHERE peer = ?`, peer).Scan(&one)
	if err == nil {
		return true, RecoveryEvictionVictim{}, tx.Commit() // one peer, one job
	}
	if err != sql.ErrNoRows {
		return false, RecoveryEvictionVictim{}, fmt.Errorf("probe recovery job %s: %w", peer, err)
	}

	proceed, victim, err := admitRecoveryBacklogTx(ctx, tx, peer, perPeerLimit, limit, allowEvict, protected)
	if err != nil {
		return false, RecoveryEvictionVictim{}, err
	}
	if !proceed {
		return false, RecoveryEvictionVictim{}, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO decrypt_recovery_jobs (peer, state, created_at, expires_at)
		VALUES (?, ?, ?, ?)`,
		peer, DecryptStatePendingNotice, now.UTC().Format(time.RFC3339Nano), expiresAt.UTC().Format(time.RFC3339Nano)); err != nil {
		return false, RecoveryEvictionVictim{}, fmt.Errorf("insert recovery job %s: %w", peer, err)
	}
	return true, victim, tx.Commit()
}

// HasRecoveryJob probes the peer's receiver-side job.
func (s *Store) HasRecoveryJob(ctx context.Context, peer string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM decrypt_recovery_jobs WHERE peer = ?`, peer).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe recovery job %s: %w", peer, err)
	}
	return true, nil
}

// UpdateRecoveryJob persists one attempt/state transition.
func (s *Store) UpdateRecoveryJob(ctx context.Context, job RecoveryJob) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE decrypt_recovery_jobs
		SET state = ?, notice_attempts = ?, last_notice_at = ?, wait_until = ?
		WHERE peer = ?`,
		job.State, job.NoticeAttempts, formatOptionalTime(job.LastNoticeAt), formatOptionalTime(job.WaitUntil), job.Peer)
	if err != nil {
		return fmt.Errorf("update recovery job %s: %w", job.Peer, err)
	}
	return nil
}

// DeleteRecoveryJob removes a finished job.
func (s *Store) DeleteRecoveryJob(ctx context.Context, peer string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM decrypt_recovery_jobs WHERE peer = ?`, peer); err != nil {
		return fmt.Errorf("delete recovery job %s: %w", peer, err)
	}
	return nil
}

// RecoveryJobs lists every durable job, least-recently-served first: the
// scheduler admits a bounded batch per pass, so the listing order IS the
// fairness policy — an unordered read would hand the same first rows to
// every pass. last_notice_at is RFC3339Nano text, so the lexicographic
// ASC is chronological with never-served (”) jobs first.
func (s *Store) RecoveryJobs(ctx context.Context) ([]RecoveryJob, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT peer, state, notice_attempts, last_notice_at, wait_until, created_at, expires_at
		FROM decrypt_recovery_jobs
		ORDER BY last_notice_at ASC, created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("select recovery jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []RecoveryJob
	for rows.Next() {
		var job RecoveryJob
		var lastNotice, waitUntil, createdAt, expiresAt string
		if err := rows.Scan(&job.Peer, &job.State, &job.NoticeAttempts, &lastNotice, &waitUntil, &createdAt, &expiresAt); err != nil {
			return nil, fmt.Errorf("scan recovery job: %w", err)
		}
		job.LastNoticeAt = parseOptionalTime(lastNotice)
		job.WaitUntil = parseOptionalTime(waitUntil)
		job.CreatedAt = parseOptionalTime(createdAt)
		job.ExpiresAt = parseOptionalTime(expiresAt)
		out = append(out, job)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Established facts
// ---------------------------------------------------------------------------

// Established reasons (§4.10): the qualifying events. Set once, never
// refreshed, never revoked — a key rotation does not un-establish a peer.
const (
	EstablishedReasonOutgoing  = "user_outgoing"
	EstablishedReasonDecrypted = "decrypted_incoming"
	EstablishedReasonManual    = "manual_import"
)

// BackfillEstablishedFromHistory seeds the monotonic established facts
// from PRE-FEATURE history: every peer the user already messaged
// qualifies through the user-outgoing rule, and without this seed a
// database that predates the peer_established table would classify every
// long-standing real contact as unknown — exactly the peers the §4.10
// reservation exists to protect from Sybil eviction. Idempotent (ON
// CONFLICT DO NOTHING against the monotonic facts).
//
// This is a data backfill, not a schema step, so it stays in the repository
// rather than in a migration: it depends on which identity owns the rows, and
// the migration catalog is deliberately free of chatlog domain rules. The
// composition root runs it once per start, right after the store is built.
func (s *Store) BackfillEstablishedFromHistory(ctx context.Context, now time.Time) error {
	self := s.identityAddr.String()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO peer_established (peer, established_at, established_reason)
		SELECT DISTINCT recipient, ?, ? FROM messages
		WHERE topic = 'dm' AND sender = ? AND recipient <> ?
		ON CONFLICT(peer) DO NOTHING`,
		now.UTC().Format(time.RFC3339Nano), EstablishedReasonOutgoing, self, self)
	if err != nil {
		return fmt.Errorf("backfill established facts: %w", err)
	}
	return nil
}

// MarkEstablished records the monotonic established fact for a peer. The
// first qualifying event wins; later calls are no-ops, so an automatic
// import can never overwrite the reason a human action set.
func (s *Store) MarkEstablished(ctx context.Context, peer, reason string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO peer_established (peer, established_at, established_reason)
		VALUES (?, ?, ?)
		ON CONFLICT(peer) DO NOTHING`,
		peer, now.UTC().Format(time.RFC3339Nano), reason)
	if err != nil {
		return fmt.Errorf("mark established %s: %w", peer, err)
	}
	return nil
}

// IsEstablished reports whether the peer has a recorded established fact.
// Presence of a chatlog row or a header-derived contact is deliberately
// NOT enough (§4.10): both appear before decryption, and the first
// poison-DM would make a Sybil "known".
func (s *Store) IsEstablished(ctx context.Context, peer string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM peer_established WHERE peer = ?`, peer).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read established %s: %w", peer, err)
	}
	return true, nil
}

func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseOptionalTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

// CountRetryChain counts the rows stamped with the given chain root. The
// match is on the JSON field itself, not a LIKE substring: a root id
// appearing inside some other value must not inflate the chain budget.
func (s *Store) CountRetryChain(ctx context.Context, rootID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM messages
		WHERE json_valid(metadata) AND json_extract(metadata, '$.retry_root_id') = ?`,
		rootID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count retry chain %s: %w", rootID, err)
	}
	return count, nil
}

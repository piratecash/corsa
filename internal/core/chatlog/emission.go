package chatlog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

// emission.go is the durable half of one question the node asks about its
// own outgoing messages: did this envelope ever reach the wire?
//
// The node answers it from memory (node.deliveryRetryEntry.Emitted), and a
// deletion depends on the answer: "provably never emitted" is what lets a
// wipe drop the request instead of naming an id to a peer who has never
// seen it. Memory dies with the process, and until this mark existed a
// restart answered "emitted" for everything — safe in the sense that it
// never skipped a needed deletion, but it announced ids for messages that
// had never left the machine.
//
// The mark is the NEGATIVE and lives in the row's `metadata` JSON rather
// than in a column of its own, which is the chatlog's documented path for
// additive facts (docs/chatlog.md) and keeps a rolled-back binary reading
// the same rows. Both choices follow from the same cost argument:
//
//   - absent means EMITTED, so every row that predates the feature — and
//     every row of the ordinary path, where the message goes out the
//     moment it is stored — is correct while carrying nothing at all. The
//     common send pays zero writes;
//   - a write happens only for a message that was WITHHELD (recipient
//     unreachable) and once more if it later goes out. The cost is
//     proportional to exactly the case the mark exists for.

// metadataNeverEmitted is the JSON path of the mark. Its ABSENCE is the
// conservative answer, and the direction matters in both failure modes: a
// lost mark costs the peer one id they cannot resolve, while a stale mark
// would leave a delivered message with them and nothing left to ask.
const metadataNeverEmitted = "$.never_emitted"

// NeverEmitted reports whether a row's metadata claims the message never
// reached the wire. A blob that is missing, invalid or not an object
// carries no claim, so it reads as emitted.
func NeverEmitted(metadata string) bool {
	if strings.TrimSpace(metadata) == "" {
		return false
	}
	var fields struct {
		NeverEmitted bool `json:"never_emitted"`
	}
	if err := json.Unmarshal([]byte(metadata), &fields); err != nil {
		return false
	}
	return fields.NeverEmitted
}

// MarkNeverEmitted records that the messages have not reached the wire.
// Idempotent, and silently skips ids with no row: the caller marks from
// the node's delivery domain, where an entry can outlive the row a
// deletion has already removed.
func (s *Store) MarkNeverEmitted(ctx context.Context, ids []domain.MessageID) error {
	// A metadata blob that is not a JSON OBJECT is replaced wholesale:
	// json_set on a non-object returns it unchanged while still counting
	// the row as affected, so the mark would be reported written without
	// landing. Same guard as MarkDecryptFailed.
	return s.updateEmissionMarks(ctx, ids, `
		UPDATE messages
		SET metadata = json_set(
			CASE WHEN metadata IS NULL OR NOT json_valid(metadata) OR json_type(metadata) <> 'object'
			     THEN '{}' ELSE metadata END,
			'`+metadataNeverEmitted+`', json('true'))
		WHERE id IN (`, "mark never-emitted")
}

// ClearNeverEmitted withdraws the claim for the messages, and must be
// durable BEFORE the frame carrying any of them is written: a crash in
// between has to read as "may have gone out".
//
// The statement rewrites only rows that actually carry the mark. Without
// that predicate this would rewrite every id handed to it on a path that
// runs per emission, which is the one place the design promised not to
// pay for.
func (s *Store) ClearNeverEmitted(ctx context.Context, ids []domain.MessageID) error {
	return s.updateEmissionMarks(ctx, ids, `
		UPDATE messages
		SET metadata = json_remove(metadata, '`+metadataNeverEmitted+`')
		WHERE json_valid(metadata) AND json_type(metadata) = 'object'
		  AND json_extract(metadata, '`+metadataNeverEmitted+`') IS NOT NULL
		  AND id IN (`, "clear never-emitted")
}

func (s *Store) updateEmissionMarks(ctx context.Context, ids []domain.MessageID, prefix, what string) error {
	for start := 0; start < len(ids); start += emissionMarkBatch {
		end := min(start+emissionMarkBatch, len(ids))
		chunk := ids[start:end]

		placeholders := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk))
		for _, id := range chunk {
			placeholders = append(placeholders, "?")
			args = append(args, string(id))
		}
		if _, err := s.db.ExecContext(ctx, prefix+strings.Join(placeholders, ", ")+`)`, args...); err != nil {
			return fmt.Errorf("chatlog: %s for %d messages: %w", what, len(chunk), err)
		}
	}
	return nil
}

// emissionMarkBatch bounds one UPDATE so a backlog replay cannot build a
// statement with more placeholders than SQLite takes.
const emissionMarkBatch = 128

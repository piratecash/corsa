package chatlog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

// emission.go holds the durable answers to the TWO questions this node
// asks about its own outgoing messages. They are two, and not one, because
// they are asked by different readers with OPPOSITE safe answers:
//
//	                        | asked by            | when unsure, answer
//	------------------------+---------------------+---------------------
//	may a writer have       | deletion            | YES — ask the peer
//	taken this frame?       | (dm_router_delete)  |
//	------------------------+---------------------+---------------------
//	did a sink confirm it   | the sender's badge  | NO — show "queued"
//	on the wire?            | (outbox reseed)     |
//
// One flag cannot serve both. It was tried: `never_emitted` was cleared
// before the write to satisfy the first reader and put BACK on a refusal to
// satisfy the second, which made it non-monotone — and a non-monotone flag
// under concurrent sinks needs a queue, a per-attempt stamp, a re-read and
// a correction, each of which turned out to have its own interleaving. Six
// review rounds found six of them, always in different code, always the
// same defect.
//
// So each question gets its own bit, and each bit moves in ONE direction
// only:
//
//   - never_emitted — written ONCE, by the INSERT that creates an outgoing
//     row (NeverEmittedMetadata), and cleared ONCE, before the first frame
//     is handed to any writer. Never set again. A crash between the clear
//     and the write reads as "may have gone out": the deletion asks the
//     peer about an id they may not have, which costs them one unresolved
//     id and costs the user nothing.
//   - on_wire — written ONCE, after a sink reports it took the frame.
//     Never cleared. A crash between the confirmation and the write reads
//     as "not yet sent": the badge shows queued and the retry engine sends
//     again, which the recipient dedupes silently.
//
// Both failure directions are cheap, both bits are monotone, and neither
// needs to be corrected — which is why nothing in this file writes in the
// direction that would make it necessary.

// metadataNeverEmitted is the JSON path of the first bit. Its ABSENCE is
// the conservative answer: a lost mark costs the peer one id they cannot
// resolve, while a stale one would leave a delivered message with them and
// nothing left to ask.
const metadataNeverEmitted = "$.never_emitted"

// metadataOnWire is the JSON path of the second. Its ABSENCE is the
// conservative answer for ITS reader: a row with no stamp reads as not yet
// sent, so the badge says queued and the engine tries again.
const metadataOnWire = "$.on_wire"

// NeverEmittedMetadata is the whole metadata blob for a row that is born
// under this model — an outgoing message, written before anything has been
// handed to a writer. Using it at INSERT is what makes the durable answer
// true at every instant instead of only after a follow-up write.
//
// It carries on_wire EXPLICITLY as false, and that is not redundancy. The
// badge downgrades "sent" to "queued" on the ABSENCE of a confirmation, so
// it has to tell "this row is governed by the two-bit model and nothing
// confirmed it" from "this row predates the model and says nothing either
// way". Without the explicit false those look identical, and every
// still-unreceipted message in a user's history would flip to queued on
// the first launch after the upgrade.
const NeverEmittedMetadata = `{"never_emitted":true,"on_wire":false}`

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

// MarkNeverEmitted sets the first bit on rows that already exist.
//
// It is NOT part of the delivery path and the node cannot reach it: the
// journal interface the node is handed (node.DeliveryEmissionJournal)
// deliberately does not carry it, because a delivery path that can re-set
// this bit makes it non-monotone — the defect this file's header exists to
// describe. An outgoing row gets the bit from its INSERT
// (NeverEmittedMetadata) and loses it exactly once.
//
// What remains is row construction and repair: a test building a row that
// predates a feature, a migration backfilling one. Idempotent, and
// silently skips ids with no row.
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

// OnWire reports whether a sink has confirmed the message on the wire, and
// whether the row answers the question at all.
//
// known is false for a row written before this bit existed. Such a row
// says nothing about the wire, and the badge must leave its persisted
// status alone rather than reading the silence as "not sent" — otherwise
// an upgrade turns every unreceipted message in the history into "queued".
// A row born under the model always carries the key, false included.
func OnWire(metadata string) (onWire, known bool) {
	if strings.TrimSpace(metadata) == "" {
		return false, false
	}
	var fields struct {
		OnWire *bool `json:"on_wire"`
	}
	if err := json.Unmarshal([]byte(metadata), &fields); err != nil || fields.OnWire == nil {
		return false, false
	}
	return *fields.OnWire, true
}

// MarkOnWire records that a sink took the frame. Monotone: it is never
// unset, so it can be written whenever a confirmation happens without any
// ordering against the other bit.
//
// Idempotent, and silently skips ids with no row: the caller writes from
// the node's delivery domain, where an entry can outlive the row a
// deletion has already removed.
func (s *Store) MarkOnWire(ctx context.Context, ids []domain.MessageID) error {
	// A metadata blob that is not a JSON OBJECT is replaced wholesale:
	// json_set on a non-object returns it unchanged while still counting
	// the row as affected, so the stamp would be reported written without
	// landing. Same guard as MarkDecryptFailed.
	//
	// The predicate keeps this off the rows that already carry it, so a
	// re-send of a confirmed message pays no write.
	//
	// Compared against 0 and not against json('true'): json_extract
	// returns SQLite's INTEGER 1/0 for a JSON boolean, so
	// `json_extract(...) IS NOT json('true')` compares an integer with the
	// TEXT "true" and is therefore true for every row — the predicate
	// silently did nothing. COALESCE folds "absent" and "false" into the
	// same 0, which is exactly the set that still needs the write.
	return s.updateEmissionMarks(ctx, ids, `
		UPDATE messages
		SET metadata = json_set(
			CASE WHEN metadata IS NULL OR NOT json_valid(metadata) OR json_type(metadata) <> 'object'
			     THEN '{}' ELSE metadata END,
			'`+metadataOnWire+`', json('true'))
		WHERE COALESCE(json_extract(
			CASE WHEN json_valid(metadata) AND json_type(metadata) = 'object'
			     THEN metadata ELSE '{}' END,
			'`+metadataOnWire+`'), 0) = 0
		  AND id IN (`, "mark on-wire")
}

// ClearNeverEmitted withdraws the first bit, and must be durable BEFORE
// the frame carrying any of them is written: a crash in between has to read
// as "may have gone out".
//
// This is the ONLY writer of that bit after the insert, and it moves in one
// direction. Nothing puts it back — see the file header for why the version
// that did needed a queue and four rounds of interleaving fixes.
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

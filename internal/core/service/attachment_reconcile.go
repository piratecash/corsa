package service

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// attachment_reconcile.go closes the one window a deletion cannot commit
// through: the message row lives in SQLite and its attachment lives in a JSON
// file, and no transaction spans the two.
//
// The order is already the safe one — the cleanup intent is written to the JSON
// before any file is unlinked — but the two stores can still disagree. Delete a
// message with an attachment, and if the JSON write fails while the process
// then dies, the next start restores a mapping and a file for a message the
// database no longer has, with nothing left that knows they should be gone. The
// user was told the message was deleted; the file is still on their disk.
//
// So the disagreement is resolved at startup, from the side that is
// authoritative. A mapping whose message is not in chatlog any more describes a
// deletion that did not finish, and finishing it is exactly what
// CleanupTransferByMessageID does — idempotently, so a mapping that was already
// half-cleaned costs nothing.
//
// The direction is deliberate and only one way. A message with no mapping is
// ordinary (most messages carry no file); a mapping with no message is not.

// reconcileOrphanAttachments finishes the deletions whose file half never
// landed. Runs once at startup, after chatlog is available.
func (c *DesktopClient) reconcileOrphanAttachments(ctx context.Context) {
	if c == nil || c.localNode == nil || c.chatlog == nil {
		return
	}
	store := c.chatlog.Store()
	if store == nil {
		return
	}

	orphans := 0
	for _, transfer := range c.localNode.AllFileTransfersSnapshot() {
		if transfer.FileID == "" {
			continue
		}
		// The id of a file transfer IS the id of the DM that announced it.
		_, found, err := store.EntryByID(ctx, domain.MessageID(transfer.FileID))
		if err != nil {
			// A lookup that failed proves nothing, and erasing on a guess is
			// how a file the user still has a message for disappears. The next
			// start asks again.
			log.Warn().Err(err).
				Msg("service: could not check whether a transfer's message still exists; leaving it alone")
			continue
		}
		if found {
			continue
		}
		// Counted AFTER the call, and only when the manager actually let the
		// state go: CleanupTransferByMessageID defers the erasure when it
		// cannot persist the intent, and reporting "finished" for those would
		// be a count of work that is still owed.
		c.localNode.CleanupTransferByMessageID(transfer.FileID)
		if _, _, _, stillThere := c.localNode.FileTransferProgress(transfer.FileID, transfer.Direction == "send"); !stillThere {
			orphans++
		}
	}
	if orphans > 0 {
		// Behind the deletion gate: the line reports how many deletions this
		// user made whose file half was still outstanding, at startup, every
		// startup — a small durable census of past deletions in the log file.
		deletionLog().Info().
			Int("transfers", orphans).
			Msg("service: finished the file half of deletions whose message was already gone")
	}
}

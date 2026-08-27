package filetransfer

import (
	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/logid"
)

// logID renders a file-transfer identifier for a log line.
//
// A transfer is identified by the id of the DM that announced it, so a line
// naming it is a line saying which message was deleted and when — the same
// trace the database no longer keeps. See internal/core/logid.
func logID(value string) string { return logid.Of(value) }

// deletionLog is the gated logger for lines reporting that a deletion
// happened. See logid.DeletionLog for the rule and the reason.
func deletionLog() *zerolog.Logger { return logid.DeletionLog() }

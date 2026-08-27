package service

import (
	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/logid"
)

// logID renders an identifier for a delete-path log line. See
// internal/core/logid for what it protects and why the lines still correlate.
func logID(value string) string { return logid.Of(value) }

// logIDLength mirrors the shared length, for the test that pins the contract.
const logIDLength = logid.Length

// deletionDiagnosticsEnv names the variable that turns the deletion paths'
// own logging back on. Here so the tests of this package can set it by name.
const deletionDiagnosticsEnv = logid.DeletionDiagnosticsEnv

// deletionLog is the logger for lines reporting that a deletion happened. It
// writes nothing unless the diagnostics variable is set; the rule for what goes
// through it, and why, is on logid.DeletionLog.
//
// A variable rather than a plain call so a test can drive both sides of the
// gate without depending on which test ran first.
var deletionLog = func() *zerolog.Logger { return logid.DeletionLog() }

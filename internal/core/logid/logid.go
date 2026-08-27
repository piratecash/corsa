// Package logid renders identifiers for log lines without putting them there.
//
// A deletion is the one operation whose purpose is that something stops
// existing. A line saying "removed message 6c1f0a6e-… for peer 2222…" leaves,
// in a plain-text file nobody sweeps, exactly the record the database was just
// cleaned of: which conversation, which messages, and when they went. The WAL
// truncation, secure_delete and the keyed refusals all close one door; a log
// line naming the id leaves the other one open.
//
// Diagnosability is not what is traded away. What an investigation needs from
// these lines is that they can be tied to EACH OTHER — the dispatch, the answer
// and the sweep that gave up all being about one request — not that they can be
// tied back to a message. So an identifier is logged as a short digest under a
// salt this process generates at startup and never stores: stable within one
// run, meaningless in the file afterwards, and impossible to match against the
// database or against another node's logs.
package logid

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// salt is generated once per process and never leaves it. Without it a digest
// would be a stable pseudonym for the same message across every run and every
// install — enough to correlate two log files, which is the trace this exists
// to remove.
//
// crypto/rand.Read cannot fail on any platform this runs on; if it somehow did,
// the zero salt still yields per-value opaque digests and only the cross-run
// guarantee would be lost. That is not worth a panic this deep in startup.
var salt = func() []byte {
	value := make([]byte, 32)
	_, _ = rand.Read(value)
	return value
}()

// Length is how many hex characters of the digest are kept. Eight is plenty to
// tell apart the handful of operations one investigation looks at, and short
// enough that nobody mistakes the value for something to look up.
const Length = 8

// Of renders an identifier for a log line: a per-process digest, or the empty
// string for an empty value, which reads as "none" rather than as a digest of
// nothing.
func Of(value string) string {
	if value == "" {
		return ""
	}
	mac := hmac.New(sha256.New, salt)
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))[:Length]
}

// DeletionDiagnosticsEnv turns the deletion paths' own logging back on.
const DeletionDiagnosticsEnv = "CORSA_DELETION_DIAGNOSTICS"

// diagnostics answers whether those lines are wanted. Read once per process.
var diagnostics = sync.OnceValue(func() bool {
	return os.Getenv(DeletionDiagnosticsEnv) != ""
})

// nop swallows everything written to it.
var nop = zerolog.Nop()

// DeletionLog is the logger for lines that report a deletion HAPPENING. It
// writes nothing unless CORSA_DELETION_DIAGNOSTICS is set.
//
// Redacting the identifiers was not enough, which is why this lives beside
// them. "A wipe settled, three messages removed, 14:07:22" says that this user
// deleted something, how much of it, and when — in a plain-text file that no
// checkpoint, no secure_delete and no migration ever touches. The digests made
// those lines anonymous; they did not make them absent.
//
// The rule, since it is not obvious at a call site:
//
//   - a line saying a deletion SUCCEEDED goes through here. Nobody needs it in
//     ordinary operation; the outcome the user cares about is on their screen.
//   - a line saying something FAILED stays at its normal level. Those describe
//     a node that is not doing what it promised, and a support case with no way
//     to see them is worse for the user than the fact that something went
//     wrong once. Those lines still carry digests rather than identifiers.
//
// It spans packages on purpose: the deletion of a message reaches the node
// (frozen and cancelled deliveries) and the file-transfer manager (erased
// attachments), and a contract that held in only one of the three would be no
// contract at all.
func DeletionLog() *zerolog.Logger {
	if diagnostics() {
		return &log.Logger
	}
	return &nop
}

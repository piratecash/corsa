package overlaysim

// runstore_test.go is the journal the three final drivers (M4, M3-a, M6) write
// their runs into, and the only reason a sweep of this stand can be interrupted.
//
// Why it exists. One call of the execution environment is capped at ≈180 s
// (measured, not assumed: a 280 s sleep was killed at 177.985 s), while the
// agreed M3-a sweep is ≈10 min of graph building and the M6 grid is 520
// scenario runs of tens of seconds each. A sweep that only prints to the test
// log therefore cannot be finished at all — every call would restart it from
// zero and lose what the previous one computed. So a run has to be a FILE with
// a name derived from its configuration, and the sweep has to be able to look
// at the directory and say what is left.
//
// The four rules, each of which cost somebody something before it was a rule:
//
//  1. A CONFIGURATION HAS AN IDENTIFIER, and the identifier is derived from the
//     parameters — every one of them, in a fixed order — rather than from a
//     counter or a position in a loop. A position changes when the enumeration
//     changes, and then a resumed sweep skips a configuration it never ran.
//
//  2. A FINISHED RESULT IS NEVER OVERWRITTEN, and the refusal is the OPEN
//     ITSELF (O_CREATE|O_EXCL), not a check before it. This is the exact
//     lesson of 2026-09-19, where an accidental rerun replaced 160 of 180
//     presented M2 records: a stat-then-write leaves a window in which two runs
//     both see no file, and a sequential test cannot see that window at all.
//
//  3. A RUN IS SKIPPED ONLY AFTER ITS RECORD HAS BEEN VERIFIED — every
//     parameter, the sources stamp and the checksum of the body. A skip on the
//     strength of a file NAME would let a directory written by other parameters,
//     or a truncated file, pass for a result. Anything that does not verify is a
//     REFUSAL, never a silent rerun and never a silent skip.
//
//  4. THE FOUR STATES ARE COUNTED APART: expected, completed, failed, missing.
//     "Everything ran" and "everything that ran, ran" are different statements,
//     and a report that prints one total cannot make the second one.
//
// ⚠️ What this file does NOT do: it decides nothing about any measurement, holds
// no threshold, and never treats a result as good or bad. A driver fails on a
// stand defect; a measurement that came out badly is a result and is written
// down like any other.

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// runRecordFormat tags the file. It changes when the header or the checksum
// rule changes, and a reader that does not know the tag refuses the file rather
// than guessing what the lines mean.
const runRecordFormat = "overlaysim-run/v1"

// runParam is one named input of a configuration.
//
// ⚠️ Name AND value, as strings, in the order the driver declares them. A map
// would be tidier and would also make the identifier depend on Go's map
// iteration order — that is, on nothing — so two runs of one configuration
// could land in two files. The order is part of the canonical form.
type runParam struct {
	Name  string
	Value string
}

// runKey is what a run IS: which measurement, which configuration of it, under
// which parameters, from which sources.
type runKey struct {
	// Measurement is "M4", "M3-a" or "M6" — the driver that produced the run.
	Measurement string
	// Label is the configuration in words, for a human reading the directory:
	// "grid/A′ base", "control/omniscient", "delta/pair". It is part of the
	// canonical form, because two configurations that differ only in what they
	// are FOR (a control and a grid point with identical fields) are different
	// runs and must not share a file.
	Label string
	// Params is every input that changes the result.
	//
	// ⚠️ EVERY input. A parameter left out of this list is a parameter a
	// resumed sweep will not notice has changed — it would skip a run made
	// under different conditions and call it the same one.
	Params []runParam
	// Sources is the version of the sources the run was made with, passed in by
	// the operator. It is NOT part of the identifier: the identifier names the
	// configuration, and a run of the same configuration from other sources is
	// the SAME configuration measured again. It is compared on resume instead,
	// so a stale record shows up as a mismatch to be dealt with rather than as
	// a missing run to be silently redone.
	Sources string
}

// canonical renders the key in the one form the identifier and the checksum are
// taken over. ⚠️ It excludes Sources, by rule — see runKey.Sources.
func (k runKey) canonical() string {
	var out strings.Builder
	fmt.Fprintf(&out, "measurement=%s\n", k.Measurement)
	fmt.Fprintf(&out, "label=%s\n", k.Label)
	for _, param := range k.Params {
		fmt.Fprintf(&out, "param %s=%s\n", param.Name, param.Value)
	}
	return out.String()
}

// ID is the configuration's identifier: sixteen hex characters over the
// canonical form. Short enough to type into a command line, long enough that
// two configurations of this stand will not collide.
func (k runKey) ID() string {
	sum := sha256.Sum256([]byte("corsa/overlay/sim/run/v1\x00" + k.canonical()))
	return fmt.Sprintf("%x", sum[:8])
}

// slug renders a label as a file-name component: everything outside the small
// safe alphabet becomes a dash, so a label may carry ′, ×, spaces and slashes
// without deciding what a file is called.
func slug(text string) string {
	var out strings.Builder
	previousDash := false
	for _, symbol := range text {
		switch {
		case symbol >= 'a' && symbol <= 'z', symbol >= 'A' && symbol <= 'Z',
			symbol >= '0' && symbol <= '9':
			out.WriteRune(symbol)
			previousDash = false
		default:
			if !previousDash {
				out.WriteByte('-')
				previousDash = true
			}
		}
	}
	return strings.Trim(out.String(), "-")
}

// FileName is where the RESULT lives. The identifier is in the name, so a file
// can be found from a command line and a directory listing reads as an
// enumeration; the slug is there only so a human can see which is which.
func (k runKey) FileName() string {
	return fmt.Sprintf("%s.run", k.fileStem())
}

// FailureName is where ONE FAILED ATTEMPT lives. ⚠️ A different name from the
// result, and a fresh one per attempt: that is what removes the replace path
// writeRun used to take, and with it the window two retrying processes could
// lose (P2 of 2026-09-20).
func (k runKey) FailureName(attempt string) string {
	return fmt.Sprintf("%s.attempt-%s.failed", k.fileStem(), attempt)
}

// failureGlob matches every recorded attempt of one configuration.
func (k runKey) failureGlob() string {
	return fmt.Sprintf("%s.attempt-*.failed", k.fileStem())
}

func (k runKey) fileStem() string {
	return fmt.Sprintf("%s-%s-%s", slug(k.Measurement), slug(k.Label), k.ID())
}

func (k runKey) String() string {
	return fmt.Sprintf("%s %s [%s]", k.Measurement, k.Label, k.ID())
}

// Param reads one parameter back, so a driver and a test can ask what a key
// says without re-deriving it.
func (k runKey) Param(name string) (string, bool) {
	for _, param := range k.Params {
		if param.Name == name {
			return param.Value, true
		}
	}
	return "", false
}

// runOutcome is what happened to one configuration. ⚠️ A failure is a RECORDED
// state, not an absence: "it was tried and it broke" and "it was never tried"
// are the two things a resumed sweep must be able to tell apart, and only a
// written record can carry the first.
type runOutcome int

const (
	// runMissing — no file for this configuration.
	runMissing runOutcome = iota
	// runCompleted — the configuration ran and its result is in the file.
	runCompleted
	// runFailed — the configuration ran and the stand refused it; the reason is
	// in the file.
	runFailed
	// runUnreadable — a file exists under this identifier and does NOT verify:
	// wrong format, broken checksum, different parameters or different sources.
	//
	// ⚠️ Never skipped and never quietly redone. It means the directory holds
	// something other than what is being asked for, and only the operator can
	// say which of the two is wanted.
	runUnreadable
)

func (o runOutcome) String() string {
	switch o {
	case runCompleted:
		return "completed"
	case runFailed:
		return "failed"
	case runUnreadable:
		return "UNREADABLE"
	default:
		return "missing"
	}
}

// runRecord is one file: the key it was written under, the state, and the body.
type runRecord struct {
	Key     runKey
	Outcome runOutcome
	// Detail carries the failure reason for a failed run and the mismatch for an
	// unreadable one. Empty for a completed run.
	Detail string
	// Body is the result itself, line by line, exactly as the driver produced
	// it. The store does not interpret it — what a measurement means is the
	// driver's business — but it does checksum it.
	Body []string
}

// bodyChecksum digests the key and the body together.
//
// ⚠️ Together, and that is the point: a checksum over the body alone would pass
// on a file whose header was edited to claim other parameters, which is exactly
// the file a resumed sweep must not accept.
func bodyChecksum(key runKey, outcome runOutcome, detail string, body []string) string {
	var material strings.Builder
	material.WriteString("corsa/overlay/sim/run/body/v1\x00")
	material.WriteString(key.canonical())
	fmt.Fprintf(&material, "sources=%s\nstatus=%s\ndetail=%s\nlines=%d\n",
		key.Sources, outcome, detail, len(body))
	for _, line := range body {
		material.WriteString(line)
		material.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(material.String()))
	return fmt.Sprintf("%x", sum[:16])
}

// renderRun is the file's text. Header lines are comments, so the body stays a
// plain listing any tool can read.
func renderRun(record runRecord) string {
	var out strings.Builder
	key := record.Key
	fmt.Fprintf(&out, "# %s\n", runRecordFormat)
	fmt.Fprintf(&out, "# measurement=%s\n", key.Measurement)
	fmt.Fprintf(&out, "# label=%s\n", key.Label)
	fmt.Fprintf(&out, "# config_id=%s\n", key.ID())
	for _, param := range key.Params {
		fmt.Fprintf(&out, "# param %s=%s\n", param.Name, param.Value)
	}
	fmt.Fprintf(&out, "# sources=%s\n", key.Sources)
	fmt.Fprintf(&out, "# status=%s\n", record.Outcome)
	fmt.Fprintf(&out, "# detail=%s\n", record.Detail)
	fmt.Fprintf(&out, "# result_lines=%d\n", len(record.Body))
	fmt.Fprintf(&out, "# checksum=%s\n",
		bodyChecksum(key, record.Outcome, record.Detail, record.Body))
	for _, line := range record.Body {
		fmt.Fprintf(&out, "%s\n", line)
	}
	return out.String()
}

// errRunExists says a completed result is already on disk under this identifier.
var errRunExists = errors.New("a completed run is already recorded under this identifier")

// writeRun puts one run on disk.
//
// ⚠️ NOTHING IS EVER REPLACED, and that is a change of 2026-09-20 (P2). The
// previous version replaced a record whose status was `failed`, on the argument
// that a failure is not a result — and the replacement was an O_TRUNC of a name
// two processes could both have found failed a moment earlier. The first then
// wrote `completed` and the second truncated it: the initial O_EXCL did not
// guard that path at all.
//
// Now a success and a failure do not share a name:
//
//	<measurement>-<label>-<id>.run                    — the result, created once
//	<measurement>-<label>-<id>.attempt-<n>.failed     — one file per failed try
//
// A retry therefore never opens an existing name for writing: it tries to CREATE
// the result, and the kernel lets exactly one caller win. There is no
// check-then-write window left to lose, and a failure record is immutable like
// every other file here.
//
// ⚠️ A loser of that race is NOT an error. Both callers measured the same
// configuration from the same sources, so the record on disk is the record this
// one would have written; writeRun reports errRunExists and the caller counts
// the configuration as completed by whoever got there first. It does not
// overwrite, and it does not pretend it wrote anything.
func writeRun(dir string, record runRecord) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("run directory %s: %w", dir, err)
	}

	name := record.Key.FileName()
	if record.Outcome == runFailed {
		attempt, err := attemptToken()
		if err != nil {
			return "", err
		}
		name = record.Key.FailureName(attempt)
	}
	path := filepath.Join(dir, name)

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, fs.ErrExist) {
		// Only the result name can already exist: a failure name carries a
		// fresh random token, so a collision there is not a race but a defect.
		return "", fmt.Errorf("%s already holds a run of %s: %w (a completed result is never "+
			"replaced; a rerun writes into a SEPARATE directory)", path, record.Key, errRunExists)
	}
	if err != nil {
		return "", fmt.Errorf("creating %s: %w", path, err)
	}

	if _, writeErr := file.WriteString(renderRun(record)); writeErr != nil {
		// The close error is dropped here and nowhere else: the write already
		// failed, and reporting the close instead would hide why.
		_ = file.Close()
		return "", fmt.Errorf("writing %s: %w", path, writeErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return "", fmt.Errorf("closing %s: %w", path, closeErr)
	}
	return path, nil
}

// attemptToken names one try. ⚠️ From crypto/rand rather than from a counter or
// a clock: two processes retrying the same configuration in the same
// millisecond must not choose one name, and neither of them can see the other's
// counter.
func attemptToken() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("naming the attempt: %w", err)
	}
	return fmt.Sprintf("%x", buf), nil
}

// readRun parses one file back and verifies it against itself: format, header
// completeness, promised line count and checksum.
//
// ⚠️ It does NOT compare against an expected key — that is inspectRun's job,
// and keeping the two apart is what lets a directory be listed by a reader who
// does not know what was expected.
func readRun(path string) (runRecord, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // a path this test itself produced
	if err != nil {
		return runRecord{}, err
	}
	return parseRun(string(raw))
}

// runHeader is the header as read, kept apart from the record so the parser has
// one place to hold "what the file claims about itself" before any of it is
// believed.
type runHeader struct {
	sawFormat, sawStatus, sawChecksum bool
	promisedLines                     int
	checksum                          string
	declaredID                        string
}

func parseRun(text string) (runRecord, error) {
	record := runRecord{Outcome: runUnreadable}
	header := runHeader{promisedLines: -1}

	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "# ") {
			if !header.sawChecksum {
				return runRecord{}, fmt.Errorf(
					"a result line arrived before the checksum header: the body cannot be verified")
			}
			record.Body = append(record.Body, line)
			continue
		}

		field := strings.TrimPrefix(line, "# ")
		if !header.sawFormat {
			if field != runRecordFormat {
				return runRecord{}, fmt.Errorf(
					"this is not %s but %q — the header may mean anything, so it is refused",
					runRecordFormat, field)
			}
			header.sawFormat = true
			continue
		}
		name, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		if err := assignRunHeader(&record, &header, name, value); err != nil {
			return runRecord{}, err
		}
	}

	switch {
	case !header.sawFormat:
		return runRecord{}, fmt.Errorf("empty file: no %s tag", runRecordFormat)
	case !header.sawStatus:
		return runRecord{}, fmt.Errorf("the file does not say whether the run finished")
	case !header.sawChecksum:
		return runRecord{}, fmt.Errorf("the file carries no checksum, so its body proves nothing")
	case header.promisedLines >= 0 && header.promisedLines != len(record.Body):
		return runRecord{}, fmt.Errorf(
			"the header promises %d result lines and the body holds %d — the file is truncated",
			header.promisedLines, len(record.Body))
	}

	// ⚠️ The identifier is RE-DERIVED from the parameters and compared with the
	// one the file states. A header whose parameters were edited still carries
	// the old identifier, and a reader that believed the stated one would report
	// a run of other parameters under the expected name.
	if header.declaredID != "" && header.declaredID != record.Key.ID() {
		return runRecord{}, fmt.Errorf(
			"the file states configuration %s and its own parameters identify %s: the header was "+
				"edited", header.declaredID, record.Key.ID())
	}
	want := bodyChecksum(record.Key, record.Outcome, record.Detail, record.Body)
	if header.checksum != want {
		return runRecord{}, fmt.Errorf(
			"checksum %s does not match the file's own contents (%s): the record was truncated or "+
				"edited, and a measurement that cannot be verified is not evidence",
			header.checksum, want)
	}
	return record, nil
}

// assignRunHeader fills one header field. Unknown fields are ignored on purpose:
// a newer writer may add one, and refusing a file over a field this reader does
// not use would be strictness without a reason.
func assignRunHeader(record *runRecord, header *runHeader, name, value string) error {
	switch {
	case name == "measurement":
		record.Key.Measurement = value
	case name == "label":
		record.Key.Label = value
	case name == "config_id":
		header.declaredID = value
	case name == "sources":
		record.Key.Sources = value
	case name == "status":
		outcome, err := outcomeFromName(value)
		if err != nil {
			return err
		}
		record.Outcome, header.sawStatus = outcome, true
	case name == "detail":
		record.Detail = value
	case name == "result_lines":
		lines, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("result_lines=%q: %w", value, err)
		}
		header.promisedLines = lines
	case name == "checksum":
		header.checksum, header.sawChecksum = value, true
	case strings.HasPrefix(name, "param "):
		record.Key.Params = append(record.Key.Params,
			runParam{Name: strings.TrimPrefix(name, "param "), Value: value})
	}
	return nil
}

func outcomeFromName(name string) (runOutcome, error) {
	switch name {
	case "completed":
		return runCompleted, nil
	case "failed":
		return runFailed, nil
	case "missing":
		return runMissing, nil
	default:
		return runUnreadable, fmt.Errorf("%q is not a status this format knows", name)
	}
}

// inspectRun says what the directory holds for ONE expected configuration, and
// it is the only function a driver may ask "has this already been done".
//
// ⚠️ Every refusal below is a case where carrying on would produce a report
// that looks complete and describes something else.
func inspectRun(dir string, key runKey) (runOutcome, string) {
	path := filepath.Join(dir, key.FileName())
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		// No result. There may still be recorded attempts, and "it was tried
		// and it broke" is a different fact from "it was never tried" — the
		// whole reason runFailed exists.
		return inspectAttempts(dir, key)
	}
	record, err := readRun(path)
	if err != nil {
		return runUnreadable, err.Error()
	}
	// The identifier is in the name, so a mismatch here means the file's own
	// parameters do not hash to the name it is under: an edited header.
	if record.Key.ID() != key.ID() {
		return runUnreadable, fmt.Sprintf(
			"the file is named for configuration %s but its own parameters identify %s",
			key.ID(), record.Key.ID())
	}
	if difference := compareParams(key, record.Key); difference != "" {
		return runUnreadable, difference
	}
	if record.Key.Sources != key.Sources {
		return runUnreadable, fmt.Sprintf(
			"recorded from sources %s, this run is from %s: the same configuration measured on "+
				"other sources is a different measurement and is not skipped on the strength of the "+
				"old one",
			record.Key.Sources, key.Sources)
	}
	return record.Outcome, record.Detail
}

// inspectAttempts reads the failed attempts recorded for one configuration.
//
// ⚠️ Every attempt is checked against the expected key exactly as a result is:
// an attempt file of other parameters under this configuration's stem is a
// directory problem, not a failure to retry.
func inspectAttempts(dir string, key runKey) (runOutcome, string) {
	paths, err := filepath.Glob(filepath.Join(dir, key.failureGlob()))
	if err != nil || len(paths) == 0 {
		return runMissing, ""
	}
	sort.Strings(paths)

	// The last one is reported, because it is the most recent thing known about
	// the configuration; the others stay on disk, because an attempt that broke
	// differently is evidence of its own.
	last := paths[len(paths)-1]
	record, readErr := readRun(last)
	if readErr != nil {
		return runUnreadable, fmt.Sprintf("%s: %v", filepath.Base(last), readErr)
	}
	if record.Key.ID() != key.ID() {
		return runUnreadable, fmt.Sprintf(
			"%s is named for configuration %s and its own parameters identify %s",
			filepath.Base(last), key.ID(), record.Key.ID())
	}
	if difference := compareParams(key, record.Key); difference != "" {
		return runUnreadable, fmt.Sprintf("%s: %s", filepath.Base(last), difference)
	}
	if record.Key.Sources != key.Sources {
		return runUnreadable, fmt.Sprintf("%s: recorded from sources %s, this run is from %s",
			filepath.Base(last), record.Key.Sources, key.Sources)
	}
	if len(paths) == 1 {
		return runFailed, record.Detail
	}
	return runFailed, fmt.Sprintf("%s (%d attempts recorded)", record.Detail, len(paths))
}

// compareParams names the FIRST difference, with both values. "Parameters
// differ" would leave the operator to diff two files by eye.
func compareParams(want, got runKey) string {
	if want.Measurement != got.Measurement {
		return fmt.Sprintf("recorded for measurement %s, expected %s", got.Measurement, want.Measurement)
	}
	if want.Label != got.Label {
		return fmt.Sprintf("recorded as %q, expected %q", got.Label, want.Label)
	}
	if len(want.Params) != len(got.Params) {
		return fmt.Sprintf("recorded with %d parameters, expected %d", len(got.Params), len(want.Params))
	}
	for index, param := range want.Params {
		other := got.Params[index]
		if param.Name != other.Name {
			return fmt.Sprintf("parameter %d is %q in the file and %q here",
				index, other.Name, param.Name)
		}
		if param.Value != other.Value {
			return fmt.Sprintf("parameter %s is %q in the file and %q here",
				param.Name, other.Value, param.Value)
		}
	}
	return ""
}

// --- the ledger ---------------------------------------------------------------

// runLedger counts the four states apart and remembers which configuration was
// in which. ⚠️ Four lists, not one total: a sweep that ran nothing and a sweep
// whose every run broke both end with no new results, and they are not the same
// news.
type runLedger struct {
	Expected  []runKey
	Completed []runKey
	Failed    []runKey
	// Skipped is the part of Completed that was already on disk when this call
	// started — the work the resume saved.
	Skipped []runKey
	// Missing is what has no result anywhere: not on disk and not produced here.
	Missing []runKey
	// NotSelected is the part of the enumeration this CALL did not execute. It
	// is an annotation, not a fifth state: a configuration outside the
	// selection is completed if the journal holds its result and missing if it
	// does not.
	//
	// ⚠️ Keeping the two apart is the P2 of 2026-09-20. The previous version
	// marked everything outside the range missing WITHOUT looking at the
	// directory, so after running a grid batch by batch the last batch's
	// summary reported every earlier batch as missing — a ledger that got more
	// alarming the closer the work came to finished.
	NotSelected []runKey
	// Notes carries the failure reason per configuration, keyed by identifier.
	Notes map[string]string
}

func newRunLedger() *runLedger { return &runLedger{Notes: map[string]string{}} }

func (l *runLedger) expect(key runKey) { l.Expected = append(l.Expected, key) }

func (l *runLedger) note(key runKey, outcome runOutcome, detail string, alreadyOnDisk bool) {
	if detail != "" {
		l.Notes[key.ID()] = detail
	}
	switch outcome {
	case runCompleted:
		l.Completed = append(l.Completed, key)
		if alreadyOnDisk {
			l.Skipped = append(l.Skipped, key)
		}
	case runFailed:
		l.Failed = append(l.Failed, key)
	default:
		l.Missing = append(l.Missing, key)
	}
}

func (l *runLedger) noteNotSelected(key runKey) {
	l.NotSelected = append(l.NotSelected, key)
}

// Summary is the four numbers plus the arithmetic that has to hold.
//
// ⚠️ The four states describe THE WHOLE ENUMERATION as the journal holds it,
// not this call's slice of it. What the call did is the separate line below,
// because "how much is done" and "how much I just did" are different questions
// and the operator asks the first one.
func (l *runLedger) Summary() string {
	var out strings.Builder
	fmt.Fprintf(&out, "runs: expected %d = completed %d (of them already on disk %d) + failed %d + "+
		"missing %d\n",
		len(l.Expected), len(l.Completed), len(l.Skipped), len(l.Failed), len(l.Missing))
	if total := len(l.Completed) + len(l.Failed) + len(l.Missing); total != len(l.Expected) {
		fmt.Fprintf(&out, "  ⚠️ STAND DEFECT: the three states sum to %d and %d were expected — a "+
			"configuration was counted twice or not at all\n", total, len(l.Expected))
	}
	fmt.Fprintf(&out, "  this call executed %d configuration(s); %d were outside its selection and "+
		"are counted above by what the journal holds for them\n",
		len(l.Expected)-len(l.NotSelected), len(l.NotSelected))
	for _, key := range l.Failed {
		fmt.Fprintf(&out, "  FAILED  %s: %s\n", key, l.Notes[key.ID()])
	}
	if len(l.Missing) > 0 {
		fmt.Fprintf(&out, "  MISSING %d configuration(s) — no result on disk and none produced "+
			"here; they can be asked for by identifier\n", len(l.Missing))
		for _, key := range l.Missing {
			fmt.Fprintf(&out, "    %s\n", key)
		}
	}
	return out.String()
}

// Complete says whether every expected configuration has a result. ⚠️ It is
// reported, never asserted: an incomplete sweep is the normal state of a sweep
// that is being run in pieces, and a driver that failed on it could not be run
// in pieces at all.
func (l *runLedger) Complete() bool {
	return len(l.Completed) == len(l.Expected)
}

// --- the sources a batch was measured with -----------------------------------------

// sourceSnapshotFormat tags the snapshot manifest.
const sourceSnapshotFormat = "overlaysim-sources/v1"

// sourceFile is one source file, its digest, and THE BYTES THAT WERE HASHED.
//
// ⚠️ The content is carried rather than re-read at save time (P2 of
// 2026-09-20). The previous version hashed the files and then read them again
// to copy them, so an edit between the two reads produced a snapshot whose
// listing and whose files disagreed — and the listing is what the stamp comes
// from. One read, one set of bytes, one stamp.
type sourceFile struct {
	Name    string
	Digest  string
	Content []byte
}

// sourceSnapshot is WHICH SOURCES a batch was measured with — the files
// themselves, their digests, and the stamp derived from them.
//
// ⚠️ DERIVED, not typed in (P2 of 2026-09-20). The stamp used to be a string the
// operator passed on the command line, and in the round of 2026-09-19 it was
// computed once at the start and reused for every batch while the sources went
// on changing underneath: the records said which ROUND they came from and not
// which code. A stamp that does not come from the files it claims to identify
// verifies nothing — a rerun under the same typed stamp and different sources
// passed the comparison silently.
//
// So the driver takes the snapshot itself, derives the stamp from it, and SAVES
// the files beside the journal. The copy is what makes the binding exact after
// the fact: a result can be read together with the very code that produced it.
type sourceSnapshot struct {
	Stamp string
	Files []sourceFile
	// dir is where the files were read from.
	dir string
}

// takeSourceSnapshot digests every .go file of a package directory.
//
// ⚠️ Every .go file, with no exclusions. A snapshot that skipped "files that
// cannot affect the measurement" would be a judgement, and the judgement is
// exactly what a later reader cannot check.
func takeSourceSnapshot(dir string) (sourceSnapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return sourceSnapshot{}, fmt.Errorf("reading the sources at %s: %w", dir, err)
	}
	snapshot := sourceSnapshot{dir: dir}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		content, readErr := os.ReadFile(filepath.Join(dir, entry.Name())) //nolint:gosec // the package's own directory
		if readErr != nil {
			return sourceSnapshot{}, fmt.Errorf("reading %s: %w", entry.Name(), readErr)
		}
		sum := sha256.Sum256(content)
		snapshot.Files = append(snapshot.Files, sourceFile{
			Name: entry.Name(), Digest: fmt.Sprintf("%x", sum), Content: content,
		})
	}
	if len(snapshot.Files) == 0 {
		return sourceSnapshot{}, fmt.Errorf("no .go file at %s: the sources cannot be stamped", dir)
	}
	sort.Slice(snapshot.Files, func(i, j int) bool {
		return snapshot.Files[i].Name < snapshot.Files[j].Name
	})

	sum := sha256.Sum256([]byte(snapshot.manifest()))
	snapshot.Stamp = fmt.Sprintf("%x", sum[:8])
	return snapshot, nil
}

// manifest is the canonical listing the stamp is taken over.
func (s sourceSnapshot) manifest() string {
	var out strings.Builder
	fmt.Fprintf(&out, "# %s\n", sourceSnapshotFormat)
	for _, file := range s.Files {
		fmt.Fprintf(&out, "%s  %s\n", file.Digest, file.Name)
	}
	return out.String()
}

// save copies the snapshot beside the journal, under its own stamp.
//
// ⚠️ Idempotent by construction: the directory is named for the stamp, and the
// stamp is the content, so a second batch from the same sources finds its
// snapshot already there.
//
// ⚠️ AND WHEN IT DOES, THE FILES ARE CHECKED, not only the listing (P2 of
// 2026-09-20). A matching MANIFEST.sha256 says what the snapshot CLAIMS to
// hold; a snapshot whose .go file was deleted or edited afterwards claims
// exactly the same thing. The point of keeping the code beside the numbers is
// that it can be read back, so what is on disk is what gets verified.
//
// ⚠️ The copy is written from the bytes that were hashed, so nothing is read
// from the working tree twice — and the copy is verified BEFORE the manifest is
// published, so a half-written snapshot never gains the listing that would make
// a later batch accept it.
func (s sourceSnapshot) save(journalDir string) (string, error) {
	target := filepath.Join(journalDir, "sources", s.Stamp)
	manifestPath := filepath.Join(target, "MANIFEST.sha256")

	if _, err := os.Stat(manifestPath); err == nil {
		if verifyErr := s.verifySaved(target); verifyErr != nil {
			return "", verifyErr
		}
		return target, nil
	}

	if err := os.MkdirAll(target, 0o755); err != nil {
		return "", fmt.Errorf("snapshot directory %s: %w", target, err)
	}
	for _, file := range s.Files {
		if err := os.WriteFile(filepath.Join(target, file.Name), file.Content, 0o644); err != nil { //nolint:gosec // evidence, read by people
			return "", fmt.Errorf("writing %s into the snapshot: %w", file.Name, err)
		}
	}
	// ⚠️ Verified before the manifest exists: while there is no manifest the
	// directory is not a snapshot anybody will accept, so a failure here leaves
	// nothing that can be mistaken for one.
	if err := s.verifySavedFiles(target); err != nil {
		return "", fmt.Errorf("the snapshot did not land as it was read: %w", err)
	}
	if err := os.WriteFile(manifestPath, []byte(s.manifest()), 0o644); err != nil { //nolint:gosec // evidence, read by people
		return "", fmt.Errorf("writing the snapshot manifest: %w", err)
	}
	return target, nil
}

// verifySaved checks an existing snapshot directory: its listing AND its files.
func (s sourceSnapshot) verifySaved(target string) error {
	manifestPath := filepath.Join(target, "MANIFEST.sha256")
	existing, err := os.ReadFile(manifestPath) //nolint:gosec // a path this test produced
	if err != nil {
		return fmt.Errorf("reading %s: %w", manifestPath, err)
	}
	if string(existing) != s.manifest() {
		return fmt.Errorf("%s already holds a different listing under the stamp %s: the snapshot "+
			"directory is named for its own content, so this cannot happen without the directory "+
			"having been edited", manifestPath, s.Stamp)
	}
	return s.verifySavedFiles(target)
}

// verifySavedFiles re-hashes every file of a saved snapshot and refuses an extra
// one. ⚠️ Both halves matter: a missing or edited file breaks the first check,
// and a file nobody listed breaks the second — a snapshot that holds more than
// its listing says is not the code that was measured either.
func (s sourceSnapshot) verifySavedFiles(target string) error {
	listed := map[string]struct{}{"MANIFEST.sha256": {}}
	for _, file := range s.Files {
		listed[file.Name] = struct{}{}
		saved, err := os.ReadFile(filepath.Join(target, file.Name)) //nolint:gosec // a path this test produced
		if err != nil {
			return fmt.Errorf("the snapshot under stamp %s is missing %s, which its listing "+
				"names: %w", s.Stamp, file.Name, err)
		}
		if sum := sha256.Sum256(saved); fmt.Sprintf("%x", sum) != file.Digest {
			return fmt.Errorf("the snapshot copy of %s under stamp %s hashes to %x and its "+
				"listing says %s: the kept code is not the code that was measured",
				file.Name, s.Stamp, sum, file.Digest)
		}
	}

	entries, err := os.ReadDir(target)
	if err != nil {
		return fmt.Errorf("reading the snapshot directory %s: %w", target, err)
	}
	for _, entry := range entries {
		if _, ok := listed[entry.Name()]; !ok {
			return fmt.Errorf("the snapshot under stamp %s holds %s, which its listing does not "+
				"name", s.Stamp, entry.Name())
		}
	}
	return nil
}

// --- selecting what this call runs ---------------------------------------------

// runSelection is how one call of the environment is pointed at a part of an
// enumeration. Every driver takes the same three, under its own prefix, because
// an operator who has learned one sweep should not have to learn the next.
//
//	<PREFIX>_RUNS      — the directory the journal is written into (required to
//	                     keep anything; without it the sweep runs and prints, and
//	                     says in the header that nothing is being kept).
//	<PREFIX>_SOURCES   — OPTIONAL, and a cross-check rather than the stamp. The
//	                     stamp is DERIVED from a snapshot of the sources; when
//	                     this names a different one the call is refused, so an
//	                     operator cannot label a batch with a version it was not
//	                     measured under.
//	<PREFIX>_ONLY      — comma-separated configuration identifiers or labels;
//	                     only those run. This is what makes a single expensive
//	                     configuration runnable on its own.
//	<PREFIX>_RANGE     — "from-to", a half-open slice of the enumeration in its
//	                     declared order, which is how a grid is cut into calls.
//	<PREFIX>_LIST      — "1": print the enumeration and run nothing. The command
//	                     lines for a batch are built from this.
type runSelection struct {
	Prefix string
	Dir    string
	// Sources is the DERIVED stamp — Snapshot.Stamp, never an operator's string.
	Sources string
	// Snapshot is the code this batch measures with, and SnapshotPath where its
	// copy was saved beside the journal (empty when nothing is being kept).
	Snapshot     sourceSnapshot
	SnapshotPath string
	Keeping      bool
	Only         []string
	From         int
	To           int
	Listing      bool
}

// mustAbs renders a path for a message, falling back to the path itself.
func mustAbs(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return absolute
}

func runSelectionFrom(t *testing.T, prefix string) runSelection {
	t.Helper()

	selection := runSelection{Prefix: prefix, From: 0, To: -1}
	selection.Dir = strings.TrimSpace(os.Getenv(prefix + "_RUNS"))
	selection.Keeping = selection.Dir != ""

	// ⚠️ The stamp is taken from the SOURCES, here, at the start of the batch —
	// not from the command line. `go test` runs with the package directory as
	// its working directory, so "." is the code that is about to measure.
	snapshot, err := takeSourceSnapshot(".")
	if err != nil {
		t.Fatalf("stamping the sources: %v", err)
	}
	selection.Snapshot = snapshot
	selection.Sources = snapshot.Stamp

	if declared := strings.TrimSpace(os.Getenv(prefix + "_SOURCES")); declared != "" &&
		declared != snapshot.Stamp {
		t.Fatalf("%s_SOURCES=%s and the sources in %q stamp as %s. The stamp is derived from the "+
			"files, so this is a batch about to be labelled with a version it was not measured "+
			"under — which is the P2 of 2026-09-20. Drop the variable, or check out the sources "+
			"you meant.", prefix, declared, mustAbs("."), snapshot.Stamp)
	}

	if selection.Keeping {
		saved, saveErr := snapshot.save(selection.Dir)
		if saveErr != nil {
			t.Fatalf("saving the source snapshot: %v", saveErr)
		}
		selection.SnapshotPath = saved
	}
	if raw := strings.TrimSpace(os.Getenv(prefix + "_ONLY")); raw != "" {
		for _, field := range strings.Split(raw, ",") {
			if name := strings.TrimSpace(field); name != "" {
				selection.Only = append(selection.Only, name)
			}
		}
	}
	if raw := strings.TrimSpace(os.Getenv(prefix + "_RANGE")); raw != "" {
		from, to, ok := strings.Cut(raw, "-")
		if !ok {
			t.Fatalf("%s_RANGE=%q is not a range: it is written from-to, half-open, in the order "+
				"the enumeration prints", prefix, raw)
		}
		start, startErr := strconv.Atoi(strings.TrimSpace(from))
		end, endErr := strconv.Atoi(strings.TrimSpace(to))
		if startErr != nil || endErr != nil || start < 0 || end < start {
			// A malformed range is fatal for the same reason a malformed pair
			// count is: an operator who asked for one part of a sweep and
			// silently got another cannot tell from the report.
			t.Fatalf("%s_RANGE=%q: want from-to with 0 ≤ from ≤ to", prefix, raw)
		}
		selection.From, selection.To = start, end
	}
	selection.Listing = strings.TrimSpace(os.Getenv(prefix+"_LIST")) == "1"
	return selection
}

// bounded says whether this call was pointed at a part of the enumeration.
func (s runSelection) bounded() bool { return len(s.Only) > 0 || s.To >= 0 }

// requireACutOrAnExplicitAll is the gate on the sweeps that cannot finish inside
// one call, and it exists for the same reason `M1_SLOW` does: `go test ./...` is
// the project's completeness check, and a sweep of tens of hours makes it
// unrunnable rather than slow.
//
// ⚠️ It SKIPS with the enumeration in the message — it does not quietly measure
// a part. "The sweep ran and here are some of the numbers" is the one outcome a
// partial grid must never look like.
func (s runSelection) requireACutOrAnExplicitAll(t *testing.T, total int, cost string) bool {
	t.Helper()

	if s.bounded() || strings.TrimSpace(os.Getenv(s.Prefix+"_ALL")) == "1" {
		return true
	}
	t.Skipf("%d configurations, %s — more than one call of this environment can finish "+
		"(the cap is ≈180 s, measured). Ask for a part of the sweep with %s_RANGE=from-to or "+
		"%s_ONLY=<identifier>, print the enumeration with %s_LIST=1, or insist on the whole "+
		"sweep in one process with %s_ALL=1. The journal (%s_RUNS) makes the parts add up.",
		total, cost, s.Prefix, s.Prefix, s.Prefix, s.Prefix, s.Prefix)
	return false
}

// selects says whether one configuration, at position `index` of the
// enumeration, is run by this call.
func (s runSelection) selects(index int, key runKey) bool {
	if len(s.Only) > 0 {
		for _, name := range s.Only {
			if name == key.ID() || name == key.Label {
				return true
			}
		}
		return false
	}
	if index < s.From {
		return false
	}
	if s.To >= 0 && index >= s.To {
		return false
	}
	return true
}

// Header states what this call is doing, in the log, before it does it.
func (s runSelection) Header(total int) string {
	var out strings.Builder
	if s.Keeping {
		fmt.Fprintf(&out, "  journal: %s (format %s) — one file per configuration, named for its "+
			"identifier; a completed result is never overwritten\n", s.Dir, runRecordFormat)
		fmt.Fprintf(&out, "  sources: %s — DERIVED from %d .go files, whose copies are saved at %s\n",
			s.Sources, len(s.Snapshot.Files), s.SnapshotPath)
	} else {
		fmt.Fprintf(&out, "  ⚠️ NO JOURNAL KEPT (%s_RUNS unset): this call prints and forgets, so "+
			"it cannot be resumed and no snapshot of the sources is saved beside its numbers\n",
			s.Prefix)
		fmt.Fprintf(&out, "  sources: %s — derived from %d .go files, NOT saved\n",
			s.Sources, len(s.Snapshot.Files))
	}
	switch {
	case len(s.Only) > 0:
		fmt.Fprintf(&out, "  selection: %s_ONLY=%s — %d configuration(s) of %d named explicitly\n",
			s.Prefix, strings.Join(s.Only, ","), len(s.Only), total)
	case s.To >= 0:
		fmt.Fprintf(&out, "  selection: %s_RANGE=%d-%d of %d configurations, in enumeration order\n",
			s.Prefix, s.From, s.To, total)
	default:
		fmt.Fprintf(&out, "  selection: all %d configurations (one call of this environment is "+
			"capped at ≈180 s — use %s_RANGE or %s_ONLY to cut the sweep into calls)\n",
			total, s.Prefix, s.Prefix)
	}
	return out.String()
}

// --- the loop itself -------------------------------------------------------------

// sweepStep is what a driver does for ONE configuration: measure it and return
// the lines the journal stores plus the line the log shows.
//
// ⚠️ An error is a REFUSAL OF THIS CONFIGURATION, recorded as a failure with its
// reason — a stand defect, or a gate the environment cannot pass. It never stops
// the sweep: stopping the whole grid on one broken configuration would lose
// every configuration after it, and these drivers exist to be run in pieces.
type sweepStep func(index int, key runKey) (body []string, headline string, err error)

// runSweep is the loop all three drivers share: select, resume, run, record,
// account.
//
// ⚠️ It exists because the same thirty lines were written three times (M4,
// M3-a, M6), including the same refusal texts — and a rule that is spelled out
// three times is a rule that will soon be spelled out two ways. The part that
// differs between the drivers is the measurement, and that is the callback.
func runSweep(
	t *testing.T, selection runSelection, keys []runKey, out *strings.Builder, step sweepStep,
) *runLedger {
	t.Helper()

	ledger := newRunLedger()
	for _, key := range keys {
		ledger.expect(key)
	}

	for index, key := range keys {
		// ⚠️ THE JOURNAL IS CONSULTED FOR EVERY CONFIGURATION, selected or not
		// (P2 of 2026-09-20). What a call executes and what the whole set is
		// worth are two questions, and answering the second from the first made
		// a batch-by-batch grid report its finished batches as missing.
		onDisk, detail := runMissing, ""
		if selection.Keeping {
			onDisk, detail = inspectRun(selection.Dir, key)
			if onDisk == runUnreadable {
				// ⚠️ Fatal, and the only fatal case here: the directory holds
				// something under this identifier that is not this run. Only the
				// operator can say which of the two is wanted.
				t.Fatalf("%s: the journal already holds something under this identifier that is "+
					"not this run: %s\n⚠️ A rerun writes into a SEPARATE directory; nothing here "+
					"is overwritten and nothing is skipped on the strength of a file name.",
					key, detail)
			}
		}
		if onDisk == runCompleted {
			ledger.note(key, runCompleted, "", true)
			if selection.selects(index, key) {
				fmt.Fprintf(out, "%-64s already recorded\n", key.Label)
			} else {
				ledger.noteNotSelected(key)
			}
			continue
		}
		if !selection.selects(index, key) {
			// Not executed here, and the journal has no result: it keeps
			// whatever status the journal gives it — failed if attempts are
			// recorded, missing if nothing is.
			ledger.note(key, onDisk, detail, false)
			ledger.noteNotSelected(key)
			continue
		}

		body, headline, err := step(index, key)
		if err != nil {
			ledger.note(key, runFailed, err.Error(), false)
			if selection.Keeping {
				if writeErr := recordOutcome(selection.Dir, key, runFailed, err.Error(), nil); writeErr != nil {
					t.Fatalf("%s: recording the failure: %v", key, writeErr)
				}
			}
			fmt.Fprintf(out, "%-64s NOT MEASURED: %v\n", key.Label, err)
			continue
		}

		if selection.Keeping {
			switch writeErr := recordOutcome(selection.Dir, key, runCompleted, "", body); {
			case writeErr == nil:
				ledger.note(key, runCompleted, "", false)
			case errors.Is(writeErr, errRunExists):
				// ⚠️ Another process recorded this configuration while this one
				// was measuring it — and what it recorded is VERIFIED before it
				// is believed (P2 of 2026-09-20). It may have created the file
				// and not yet finished writing it, and the sources stamp is not
				// part of the file NAME, so the rival may also be another
				// version. "Somebody else made this name exist" is not a result.
				if confirmErr := confirmConcurrentRecord(selection.Dir, key); confirmErr != nil {
					t.Fatalf("%s: %v", key, confirmErr)
				}
				ledger.note(key, runCompleted, "", true)
				fmt.Fprintf(out, "%-64s recorded by a concurrent run; that record verifies and "+
					"is kept\n", key.Label)
				continue
			default:
				t.Fatalf("%s: keeping the run: %v", key, writeErr)
			}
		} else {
			ledger.note(key, runCompleted, "", false)
		}
		fmt.Fprintf(out, "%-64s %s\n", key.Label, headline)
	}

	fmt.Fprintf(out, "\n%s", ledger.Summary())
	if selection.Keeping {
		strangers, err := strangersInDirectory(selection.Dir, keys)
		if err != nil {
			t.Fatalf("listing the journal: %v", err)
		}
		if len(strangers) > 0 {
			fmt.Fprintf(out, "  ⚠️ %d file(s) in %s belong to no configuration of this "+
				"enumeration: %s\n", len(strangers), selection.Dir, strings.Join(strangers, ", "))
		}
	}
	return ledger
}

// confirmConcurrentRecord decides whether a record another process created
// under this configuration's name may be counted as this configuration's result.
//
// ⚠️ It exists because "the name is taken" says nothing about what is in it
// (P2 of 2026-09-20). The previous version counted the configuration completed
// on the strength of errRunExists alone, and that is wrong in two concrete ways:
// the rival may have created the file and not yet written it (a half-written
// record is not a result), and the sources stamp is not part of the file name,
// so the rival may be ANOTHER VERSION measuring the same configuration.
//
// ⚠️ It is a function returning an error rather than a t.Fatalf inside the loop,
// so the refusal can be exercised by a fixture. A refusal nobody can trigger is
// a refusal nobody has checked.
func confirmConcurrentRecord(dir string, key runKey) error {
	outcome, detail := inspectRun(dir, key)
	switch outcome {
	case runCompleted:
		return nil
	case runUnreadable:
		return fmt.Errorf("another run created the record for this configuration and it does NOT "+
			"verify: %s\n⚠️ Either that writer is still finishing (run this again and the record "+
			"will verify), or the file is damaged or belongs to another version — and neither is "+
			"a result this call may count as its own", detail)
	default:
		// The name exists — writeRun has just said so — yet the journal reports
		// no result under it. That is the half-written case with nothing
		// parseable in it yet, or a name taken by something this store did not
		// write.
		return fmt.Errorf("another run created the record for this configuration and the journal "+
			"reports it as %s: it is not a result, so this call does not count one", outcome)
	}
}

// recordOutcome writes one run, refusing a body a reader could not parse back.
//
// ⚠️ The one-line check is not pedantry. A body line carrying a newline, or
// starting with the header marker, produces a file parseRun refuses FOREVER —
// and the next call then reports an unreadable record under an expected
// identifier and stops the sweep. A failure detail comes from an error, and an
// error's text is not under this file's control, so it is flattened here rather
// than trusted.
func recordOutcome(dir string, key runKey, outcome runOutcome, detail string, body []string) error {
	lines := make([]string, 0, len(body))
	for _, line := range body {
		lines = append(lines, oneLine(line))
	}
	_, err := writeRun(dir, runRecord{
		Key: key, Outcome: outcome, Detail: oneLine(detail), Body: lines,
	})
	return err
}

// oneLine flattens a string into something a line-oriented record can hold.
func oneLine(text string) string {
	replaced := strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ", "\t", " ").Replace(text)
	if strings.HasPrefix(replaced, "# ") {
		// A body line that looks like a header would be read as one.
		replaced = "· " + replaced
	}
	return replaced
}

// enumerationListing prints the configurations with their identifiers and
// parameters and runs nothing. It is what a review reads and what a batch
// command line is built from.
func enumerationListing(measurement string, keys []runKey) string {
	var out strings.Builder
	fmt.Fprintf(&out, "\n%s — enumeration of %d configurations, in the order a range cuts them\n",
		measurement, len(keys))
	fmt.Fprintf(&out, "⚠️ A LISTING, not a run: no graph is built and no number is produced here.\n\n")
	for index, key := range keys {
		fmt.Fprintf(&out, "%4d  %s  %s\n", index, key.ID(), key.Label)
		for _, param := range key.Params {
			fmt.Fprintf(&out, "        %-22s %s\n", param.Name, param.Value)
		}
	}
	return out.String()
}

// requireDistinctKeys is the one assertion every enumeration owes: two
// configurations that hash to one identifier would share a file, and the second
// would either be refused as an overwrite or — worse, before this check existed
// — be skipped as "already done".
func requireDistinctKeys(keys []runKey) error {
	seen := map[string]runKey{}
	for _, key := range keys {
		if previous, clash := seen[key.ID()]; clash {
			return fmt.Errorf("configurations %q and %q share the identifier %s: they would share "+
				"one file, and the second would be taken for the first",
				previous.Label, key.Label, key.ID())
		}
		seen[key.ID()] = key
	}
	return nil
}

// existingRunFiles lists what a directory already holds, so a report can say
// whether anything in it is NOT part of the current enumeration — a directory
// carrying runs of an older grid is a directory whose totals mean nothing.
func existingRunFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".run") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// strangersInDirectory names the .run files that no expected configuration
// accounts for. ⚠️ Reported, never deleted: the store does not know whether they
// are an older grid worth keeping or a mistake, and only one of those two
// answers survives a deletion.
func strangersInDirectory(dir string, expected []runKey) ([]string, error) {
	present, err := existingRunFiles(dir)
	if err != nil {
		return nil, err
	}
	wanted := map[string]struct{}{}
	for _, key := range expected {
		wanted[key.FileName()] = struct{}{}
	}
	strangers := make([]string, 0, len(present))
	for _, name := range present {
		if _, ok := wanted[name]; !ok {
			strangers = append(strangers, name)
		}
	}
	return strangers, nil
}

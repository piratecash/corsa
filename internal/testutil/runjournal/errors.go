package runjournal

import (
	"errors"
	"fmt"
)

// Sentinels. Every refusal of this package wraps exactly one of them, so a
// driver decides what to do by errors.Is and never by the text of a message.
var (
	// ErrInvalidKey — a configuration key that cannot identify anything: no
	// measurement, a parameter without a name, or two parameters of one name.
	ErrInvalidKey = errors.New("runjournal: invalid configuration key")
	// ErrInvalidConfig — the journal itself was configured incompletely.
	ErrInvalidConfig = errors.New("runjournal: invalid journal configuration")
	// ErrDuplicateConfig — two configurations of one enumeration share an
	// identifier, so they would share one file and the second would be taken
	// for the first.
	ErrDuplicateConfig = errors.New("runjournal: two configurations share an identifier")
	// ErrAlreadyRecorded — a completed result already exists under this
	// identifier. It is never replaced.
	ErrAlreadyRecorded = errors.New("runjournal: a completed result is already recorded")
	// ErrNotRecorded — no completed result exists for this configuration.
	ErrNotRecorded = errors.New("runjournal: no completed result is recorded")
	// ErrCorruptRecord — a file under an expected name does not verify against
	// itself: unknown format, broken checksum, truncated body, edited header.
	ErrCorruptRecord = errors.New("runjournal: record does not verify")
	// ErrRecordMismatch — a record verifies against itself but describes
	// something other than what was asked for. Carried by *MismatchError.
	ErrRecordMismatch = errors.New("runjournal: record describes another run")
	// ErrNoSources — a source fingerprint matched no file, so it identifies
	// nothing.
	ErrNoSources = errors.New("runjournal: no source file matched the fingerprint")
	// ErrInvalidSourceSpec — a fingerprint specification that cannot be taken
	// faithfully: a root outside the base, a name the manifest cannot hold, a
	// file reached through two roots.
	ErrInvalidSourceSpec = errors.New("runjournal: invalid source specification")
	// ErrIrregularSource — a matching entry is not a regular file (a symlink, a
	// device). Its bytes are someone else's, so it is refused, not followed.
	ErrIrregularSource = errors.New("runjournal: source entry is not a regular file")
	// ErrSourcesChanged — the working tree no longer matches a snapshot taken of
	// it, so results recorded under that snapshot's stamp would be mislabelled.
	ErrSourcesChanged = errors.New("runjournal: sources changed since the snapshot")
	// ErrSnapshotMismatch — a kept copy of the sources does not match its own
	// manifest or the snapshot it is supposed to hold.
	ErrSnapshotMismatch = errors.New("runjournal: kept sources do not verify")
	// ErrNoHardLinks — the journal's filesystem cannot create a hard link, or
	// creates one over an existing name. The no-overwrite rule rests on link(2)
	// failing with EEXIST, so such a filesystem cannot hold a journal.
	ErrNoHardLinks = errors.New("runjournal: the journal filesystem has no no-replace hard links")
	// ErrEmptyEnumeration — a ledger or sweep over no configuration. "All of
	// nothing is done" is not a statement a report may make.
	ErrEmptyEnumeration = errors.New("runjournal: the enumeration is empty")
	// ErrQuarantined — results recorded while the sources changed await the
	// operator's decision (ReleaseQuarantine); a sweep holding any of them
	// does not succeed.
	ErrQuarantined = errors.New("runjournal: results await the operator's decision on changed sources")
	// ErrNotQuarantined — a release was asked for a configuration whose
	// result is not in quarantine.
	ErrNotQuarantined = errors.New("runjournal: the result is not quarantined")
	// ErrInconsistentJournal — a state the journal's own invariants rule out,
	// such as a taken result name that the journal does not report as a result.
	ErrInconsistentJournal = errors.New("runjournal: the journal contradicts its own invariants")
)

// MismatchField names which part of a record disagreed with the request.
type MismatchField string

const (
	MismatchMeasurement MismatchField = "measurement"
	MismatchLabel       MismatchField = "label"
	MismatchParams      MismatchField = "params"
	MismatchSources     MismatchField = "sources"
	MismatchOutcome     MismatchField = "outcome"
	MismatchConfigID    MismatchField = "config_id"
)

// MismatchError is a record that verified against itself and still is not the
// run that was asked for. It carries both values, because "parameters differ"
// would leave the operator to diff two files by eye.
type MismatchError struct {
	ConfigID ConfigID
	File     string
	Field    MismatchField
	Want     string
	Got      string
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf("runjournal: %s (configuration %s): %s is %s in the record, %s was asked for",
		e.File, e.ConfigID, e.Field, e.Got, e.Want)
}

// Is makes every MismatchError match ErrRecordMismatch, so a caller that only
// needs the category does not have to know the type.
func (e *MismatchError) Is(target error) bool { return target == ErrRecordMismatch }

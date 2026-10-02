package runjournal

import (
	"context"
	"time"
)

// FingerprintName names one source fingerprint of a journal ("measured",
// "stand"). It is a file-name component, so it is restricted to [a-z0-9-].
type FingerprintName string

// sweepRun names one call of Sweep: random, so two executors — even with
// equal fingerprints from different working directories — never share one.
// A result recorded by a run carries it, and only that run's check can vouch
// for the result.
type sweepRun string

// Stamp is the identifier of one source snapshot: hex over its manifest.
type Stamp string

// SourceStamp binds a fingerprint's name to the stamp its files produced.
type SourceStamp struct {
	Name  FingerprintName `json:"name"`
	Stamp Stamp           `json:"stamp"`
}

// FileFilter decides whether a file, given by its slash-separated path relative
// to the spec's Base, belongs to a fingerprint.
type FileFilter func(relPath string) bool

// SourceRoot is one directory a fingerprint reads.
type SourceRoot struct {
	// Dir is slash-separated and relative to SourceSpec.Base; "." is the base
	// itself. It must stay inside the base.
	Dir string
	// Recursive descends into subdirectories. Symlinked directories are never
	// followed.
	Recursive bool
	// Filter selects the files; it is required, so "every file" is a decision
	// written at the call site rather than a default nobody chose.
	Filter FileFilter
}

// SourceSpec describes one fingerprint: which files, from which directories.
type SourceSpec struct {
	Name  FingerprintName
	Base  string
	Roots []SourceRoot
}

// SourceFile is one file of a snapshot together with THE BYTES THAT WERE
// HASHED. Carrying the bytes is what lets the kept copy be written without
// reading the tree a second time, when it might already have changed.
type SourceFile struct {
	Path    string
	Digest  string
	Content []byte
}

// Snapshot is which sources a batch is measured with: the files, their
// digests, and the stamp derived from them. The stamp is never typed in.
type Snapshot struct {
	spec  SourceSpec
	files []SourceFile
	stamp Stamp
}

// Outcome is what one record says happened to its configuration.
type Outcome string

const (
	// OutcomeCompleted — the configuration ran and its result is the body.
	OutcomeCompleted Outcome = "completed"
	// OutcomeFailed — the stand refused the configuration; the reason is kept.
	OutcomeFailed Outcome = "failed"
	// OutcomeQuarantined — the result was recorded by a sweep whose sources
	// changed while it ran; the detail is the change. Written beside the
	// result, never instead of it.
	OutcomeQuarantined Outcome = "quarantined"
	// OutcomeReleased — the operator decided to keep a quarantined result; the
	// detail is that decision in the operator's words.
	OutcomeReleased Outcome = "released"
	// OutcomeVerified — the sweep run named in the mark re-read ITS sources
	// at the end and found them unchanged. It vouches only for a result
	// recorded by that same run.
	OutcomeVerified Outcome = "verified"
)

// Status is what the journal holds for one configuration. The zero value is
// deliberately not a status, so an unset field can never read as "missing".
type Status uint8

const (
	statusUnset Status = iota
	// StatusMissing — nothing is recorded for the configuration.
	StatusMissing
	// StatusCompleted — a verified result is recorded.
	StatusCompleted
	// StatusFailed — no result, and at least one verified failed attempt.
	StatusFailed
	// StatusQuarantined — a verified result recorded by a sweep whose sources
	// changed, not yet released by the operator. It is not done.
	StatusQuarantined
)

func (s Status) String() string {
	switch s {
	case StatusMissing:
		return "missing"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusQuarantined:
		return "quarantined"
	case statusUnset:
		return "unset"
	default:
		return "invalid"
	}
}

// Evidence is what a verified record says beyond its status.
type Evidence struct {
	// RecordedAt is when the result, or the latest failed attempt, was written.
	RecordedAt time.Time
	// Failure is nil for a completed configuration and set for a failed one, so
	// "no failure" is visible in the type rather than read from an empty string.
	Failure *Failure
	// Quarantine is set exactly for a quarantined configuration.
	Quarantine *Quarantine
}

// Quarantine is why a recorded result is not counted as done.
type Quarantine struct {
	// Detail is the source change the sweep reported.
	Detail string
}

// Failure is what the journal knows about a configuration that has only failed
// attempts.
type Failure struct {
	// Detail is the reason recorded by the latest attempt.
	Detail string
	// Attempts is how many failed attempts are recorded.
	Attempts int
}

// Inspection is the verified answer to "what does the journal hold for this
// configuration". Evidence is nil exactly when Status is StatusMissing.
type Inspection struct {
	Status   Status
	Evidence *Evidence
}

// Result is a verified completed record read back.
type Result struct {
	Key        ConfigKey
	Sources    []SourceStamp
	RecordedAt time.Time
	Body       []byte
}

// Config is everything a journal needs. All three fields are required.
type Config struct {
	// Dir is the journal directory; it is created on the first write.
	Dir string
	// Sources are the snapshots this batch measures with. Their stamps are
	// written into every record and compared on resume.
	Sources []Snapshot
	// Clock stamps every record.
	Clock Clock
}

// Journal is one directory of run records bound to one set of sources. Its
// fields are fixed by New, so a Journal is safe for concurrent use; the
// filesystem, not the value, arbitrates between writers.
type Journal struct {
	dir     string
	sources []Snapshot
	stamps  []SourceStamp
	clock   Clock
	// link publishes a finished file under its final name; os.Link outside
	// tests. It is a field so a test can stand in for a filesystem without hard
	// links, which no machine running the tests is likely to have.
	link linkFunc
}

// linkFunc has the contract of os.Link: create newname as another name of
// oldname, failing with fs.ErrExist if newname exists.
type linkFunc func(oldname, newname string) error

// Ledger is the whole enumeration as the journal holds it, in five states
// counted apart. Every expected configuration is in exactly one of Completed,
// Failed, Missing and Quarantined.
type Ledger struct {
	Expected  []ConfigID
	Completed []ConfigID
	Failed    []ConfigID
	Missing   []ConfigID
	// Quarantined are results recorded while the sources changed, awaiting
	// the operator's decision; QuarantineDetail is why each was quarantined.
	Quarantined      []ConfigID
	QuarantineDetail map[ConfigID]string
	// FailureDetail is the latest failure reason of every failed configuration.
	FailureDetail map[ConfigID]string
	// Strangers are record files that belong to no expected configuration. They
	// are reported, never deleted: whether they are an older grid worth keeping
	// or a mistake is the operator's call.
	Strangers []string
	// Pending are temporaries a writer left behind (a crash between creating
	// and publishing). They are never records and are reported, not deleted:
	// the directory may be shared with a writer that is still running.
	Pending []string
}

// Selection says whether a configuration, at position index of the
// enumeration, is executed by this call. It decides nothing about the ledger.
type Selection func(index int, key ConfigKey) bool

// Step measures one configuration and returns the body to record. An error is
// a refusal of THIS configuration and is recorded as a failure; it never stops
// the sweep, because these sweeps exist to be run in pieces.
type Step func(ctx context.Context, key ConfigKey) ([]byte, error)

// SweepReport separates "how much is done" (Ledger, the whole set) from "what
// this call did" (the other lists).
type SweepReport struct {
	Ledger Ledger
	// Executed are the configurations whose Step ran in this call.
	Executed []ConfigID
	// AlreadyRecorded were completed on disk before this call reached them.
	AlreadyRecorded []ConfigID
	// RecordedByRival were measured here, but another writer recorded them
	// first; its verified record is the one kept.
	RecordedByRival []ConfigID
	// Quarantined were found in quarantine by this call, or put there by it
	// because its sources changed. They are neither executed nor skipped as
	// done.
	Quarantined []ConfigID
}

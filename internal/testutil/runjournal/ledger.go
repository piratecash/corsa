package runjournal

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

// Ledger reads the WHOLE enumeration from the journal and sorts every
// configuration into exactly one of completed, failed, missing and
// quarantined.
//
// It takes the full set of keys and nothing about which of them a call
// executed: "how much is done" is a question about the journal, and answering
// it from one call's slice makes a grid run in batches report every finished
// batch as missing — a ledger that grows more alarming the closer the work
// comes to done.
func (j *Journal) Ledger(ctx context.Context, keys []ConfigKey) (Ledger, error) {
	if len(keys) == 0 {
		return Ledger{}, ErrEmptyEnumeration
	}
	if err := RequireDistinct(keys); err != nil {
		return Ledger{}, err
	}
	names, err := j.listNames(ctx)
	if err != nil {
		return Ledger{}, err
	}
	keptNames, err := listDir(ctx, filepath.Join(j.dir, keptSourcesDir))
	if err != nil {
		return Ledger{}, err
	}
	ledger := Ledger{FailureDetail: map[ConfigID]string{}, QuarantineDetail: map[ConfigID]string{}}
	for _, key := range keys {
		inspection, err := j.inspectAmong(ctx, key, names)
		if err != nil {
			return Ledger{}, err
		}
		if err := ledger.place(key.ID(), inspection); err != nil {
			return Ledger{}, err
		}
	}
	ledger.Strangers = strangers(names, keys)
	ledger.Pending = append(pendingNames(names, ""), pendingNames(keptNames, keptSourcesDir+"/")...)
	return ledger, nil
}

func (l *Ledger) place(id ConfigID, inspection Inspection) error {
	l.Expected = append(l.Expected, id)
	switch inspection.Status {
	case StatusCompleted:
		l.Completed = append(l.Completed, id)
	case StatusFailed:
		l.Failed = append(l.Failed, id)
		l.FailureDetail[id] = inspection.Evidence.Failure.Detail
	case StatusMissing:
		l.Missing = append(l.Missing, id)
	case StatusQuarantined:
		l.Quarantined = append(l.Quarantined, id)
		l.QuarantineDetail[id] = inspection.Evidence.Quarantine.Detail
	default:
		return fmt.Errorf("%w: configuration %s has status %s, which the ledger cannot place",
			ErrInconsistentJournal, id, inspection.Status)
	}
	return nil
}

// strangers names the journal entries that belong to no expected
// configuration. Pending temporaries and the kept sources are the journal's
// own and are not strangers.
func strangers(names []string, keys []ConfigKey) []string {
	stems := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		stems[key.fileStem()] = struct{}{}
	}
	var found []string
	for _, name := range names {
		if strings.HasPrefix(name, ".") || name == keptSourcesDir {
			continue
		}
		if _, owned := stems[stemOf(name)]; !owned {
			found = append(found, name)
		}
	}
	return found
}

// pendingNames picks the temporaries out of a listing, prefixed with where
// they were found.
func pendingNames(names []string, prefix string) []string {
	var pending []string
	for _, name := range names {
		if strings.HasPrefix(name, pendingPrefix) {
			pending = append(pending, prefix+name)
		}
	}
	return pending
}

// stemOf recovers the configuration stem from a record file name; for any
// other name it returns the name itself, which matches no stem.
func stemOf(name string) string {
	for _, suffix := range []string{resultSuffix, quarantineSuffix, releaseSuffix} {
		if stem, ok := strings.CutSuffix(name, suffix); ok {
			return stem
		}
	}
	if stem, _, ok := strings.Cut(name, verifiedInfix); ok {
		return stem
	}
	if stem, _, ok := strings.Cut(name, attemptInfix); ok && strings.HasSuffix(name, attemptSuffix) {
		return stem
	}
	return name
}

// Complete says whether every expected configuration has a result. It is for
// reporting: an incomplete ledger is the normal state of a sweep being run in
// pieces. A ledger of nothing is not complete — Ledger refuses to build one,
// and a zero Ledger must not read as "all done" either.
func (l Ledger) Complete() bool {
	return len(l.Expected) > 0 && len(l.Completed) == len(l.Expected)
}

// Summary is the four numbers and the configurations that need attention.
func (l Ledger) Summary() string {
	var out strings.Builder
	fmt.Fprintf(&out, "runs: expected %d = completed %d + failed %d + missing %d + quarantined %d\n",
		len(l.Expected), len(l.Completed), len(l.Failed), len(l.Missing), len(l.Quarantined))
	for _, id := range l.Quarantined {
		fmt.Fprintf(&out, "  QUARANTINED %s: %q — kept, not done until the operator releases it\n", id, l.QuarantineDetail[id])
	}
	for _, id := range l.Failed {
		fmt.Fprintf(&out, "  FAILED  %s: %q\n", id, l.FailureDetail[id])
	}
	for _, id := range l.Missing {
		fmt.Fprintf(&out, "  MISSING %s\n", id)
	}
	if len(l.Pending) > 0 {
		fmt.Fprintf(&out, "  %d temporary file(s) left by an interrupted writer: %s\n",
			len(l.Pending), strings.Join(l.Pending, ", "))
	}
	if len(l.Strangers) > 0 {
		fmt.Fprintf(&out, "  %d file(s) belong to no expected configuration: %s\n",
			len(l.Strangers), strings.Join(l.Strangers, ", "))
	}
	return out.String()
}

package runjournal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// --- the base of a fingerprint ------------------------------------------------

// TestABaseThatIsASymlinkIsRefusedByEveryWalk: a flat walk reads through a
// link with ReadDir while a tree walk refuses it, so before the base was
// checked the same spec stamped one tree or failed depending on Recursive.
func TestABaseThatIsASymlinkIsRefusedByEveryWalk(t *testing.T) {
	fsprobe.RequireSymlinks(t)
	cases := map[string]SourceRoot{
		"flat at the base":    {Dir: ".", Filter: ExactNames("go.mod")},
		"tree at the base":    {Dir: ".", Recursive: true, Filter: AnyGo},
		"flat below the base": {Dir: "internal/core", Filter: AnyGo},
		"tree below the base": {Dir: "internal/core", Recursive: true, Filter: AnyGo},
	}
	for name, root := range cases {
		t.Run(name, func(t *testing.T) {
			link := filepath.Join(t.TempDir(), "linked-base")
			if err := os.Symlink(measuredTree(t), link); err != nil {
				t.Fatalf("symlink: %v", err)
			}
			spec := SourceSpec{Name: "x", Base: link, Roots: []SourceRoot{root}}
			if _, err := TakeSnapshot(context.Background(), spec); !errors.Is(err, ErrIrregularSource) {
				t.Fatalf("TakeSnapshot over a symlinked base = %v, want ErrIrregularSource", err)
			}
		})
	}
}

func TestAFingerprintNameMustFitAKeptDirectoryName(t *testing.T) {
	spec := SourceSpec{
		Name: FingerprintName(strings.Repeat("a", maxFingerprintNameBytes+1)), Base: measuredTree(t),
		Roots: []SourceRoot{{Dir: ".", Filter: ExactNames("go.mod")}},
	}
	if _, err := TakeSnapshot(context.Background(), spec); !errors.Is(err, ErrInvalidSourceSpec) {
		t.Fatalf("an over-long fingerprint name = %v, want ErrInvalidSourceSpec", err)
	}
	spec.Name = FingerprintName(strings.Repeat("a", maxFingerprintNameBytes))
	if _, err := TakeSnapshot(context.Background(), spec); err != nil {
		t.Fatalf("a fingerprint name of the maximum length is refused: %v", err)
	}
	if got := len(KeptDir("", SourceStamp{Name: spec.Name, Stamp: "0011223344556677"})) -
		len(filepath.Join("", keptSourcesDir)) - 1; got > maxFileNameBytes {
		t.Fatalf("the longest kept directory name is %d bytes", got)
	}
}

func TestVerifyTreeCountsANewLinkAsAChange(t *testing.T) {
	fsprobe.RequireSymlinks(t)
	ctx := context.Background()
	base := measuredTree(t)
	snapshot, err := TakeSnapshot(ctx, measuredSpec(base))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := os.Symlink(filepath.Join(base, "go.mod"), filepath.Join(base, "internal/core/link.go")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := snapshot.VerifyTree(ctx); !errors.Is(err, ErrSourcesChanged) || !errors.Is(err, ErrIrregularSource) {
		t.Fatalf("VerifyTree after a link appeared = %v, want ErrSourcesChanged carrying ErrIrregularSource", err)
	}
}

// --- the hard-link probe names the right cause ---------------------------------

func TestTheProbeRefusesASecondLinkThatFailsWithoutEEXIST(t *testing.T) {
	calls := 0
	link := func(oldname, newname string) error {
		calls++
		if calls == 1 {
			return os.Link(oldname, newname)
		}
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: syscall.EPERM}
	}
	if err := probeHardLinks(context.Background(), t.TempDir(), link); !errors.Is(err, ErrNoHardLinks) {
		t.Fatalf("probe with a second link refused by EPERM = %v, want ErrNoHardLinks", err)
	}
}

func TestTheProbeDoesNotBlameLinksForAFullDisk(t *testing.T) {
	link := func(oldname, newname string) error {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: syscall.ENOSPC}
	}
	err := probeHardLinks(context.Background(), t.TempDir(), link)
	if err == nil || errors.Is(err, ErrNoHardLinks) || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("probe on a full disk = %v, want ENOSPC and not ErrNoHardLinks", err)
	}
}

func TestTheProbeDoesNotBlameLinksForACancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	link := func(oldname, newname string) error {
		cancel()
		return os.Link(oldname, newname)
	}
	dir := t.TempDir()
	err := probeHardLinks(ctx, dir, link)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrNoHardLinks) {
		t.Fatalf("probe cancelled between its links = %v, want context.Canceled and not ErrNoHardLinks", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("a cancelled probe left %v (%v)", entries, readErr)
	}
}

// --- sweep, ledger and record edges -------------------------------------------

func TestSweepNoticesAChangeInEveryFingerprint(t *testing.T) {
	for _, changed := range []FingerprintName{"measured", "stand"} {
		t.Run(string(changed), func(t *testing.T) {
			ctx := context.Background()
			bases := map[FingerprintName]string{}
			var sources []Snapshot
			for _, name := range []FingerprintName{"measured", "stand"} {
				bases[name] = t.TempDir()
				writeTree(t, bases[name], map[string]string{"pkg/code.go": "package pkg"})
				snapshot, err := TakeSnapshot(ctx, SourceSpec{
					Name: name, Base: bases[name], Roots: []SourceRoot{{Dir: "pkg", Filter: NonTestGo}},
				})
				if err != nil {
					t.Fatalf("snapshot %s: %v", name, err)
				}
				sources = append(sources, snapshot)
			}
			journal := newJournal(t, t.TempDir(), sources)
			_, err := journal.Sweep(ctx, gridKeys(1), SelectAll(), func(context.Context, ConfigKey) ([]byte, error) {
				writeTree(t, bases[changed], map[string]string{"pkg/code.go": "package pkg // edited"})
				return nil, nil
			})
			if !errors.Is(err, ErrSourcesChanged) {
				t.Fatalf("sweep with %s edited = %v, want ErrSourcesChanged", changed, err)
			}
		})
	}
}

func TestSweepOfNothingTouchesNoDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "journal")
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	_, err := journal.Sweep(context.Background(), nil, SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return nil, nil })
	if !errors.Is(err, ErrEmptyEnumeration) {
		t.Fatalf("Sweep(nil) = %v", err)
	}
	if _, statErr := os.Stat(dir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Sweep(nil) created the journal directory (%v)", statErr)
	}
}

func TestLedgerReportsTemporariesUnderTheKeptSources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	if err := journal.KeepSources(ctx); err != nil {
		t.Fatalf("keep: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, keptSourcesDir, ".pending-orphan"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	ledger, err := journal.Ledger(ctx, gridKeys(1))
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if !slices.Equal(ledger.Pending, []string{keptSourcesDir + "/.pending-orphan"}) {
		t.Fatalf("pending %v, want the orphan under the kept sources", ledger.Pending)
	}
}

func TestARecordNamingAnImpossibleStampIsCorrupt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	header := recordHeader{
		ConfigID: key.ID(), Measurement: key.Measurement, Label: key.Label, Params: key.canonicalParams(),
		Sources: []SourceStamp{{Name: "..", Stamp: "../../etc"}},
		Outcome: OutcomeCompleted,
	}
	raw, err := encodeRecord(header, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	writeTree(t, dir, map[string]string{key.resultName(): string(raw)})
	if _, err := journal.Inspect(ctx, key); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("inspect of a record with an impossible stamp = %v, want ErrCorruptRecord", err)
	}
}

func TestASlugCutAtADashLeavesNoDoubleDash(t *testing.T) {
	key := ConfigKey{Measurement: "m", Label: Label(strings.Repeat("a", maxSlugBytes-1) + " tail")}
	if stem := key.fileStem(); strings.Contains(stem, "--") {
		t.Fatalf("stem %q carries the dash the cut left behind", stem)
	}
}

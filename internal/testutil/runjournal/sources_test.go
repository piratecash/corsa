package runjournal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// measuredSpec is the shape of the stand's "measured code" fingerprint: every
// non-test Go file of a tree, recursively, plus two named files at the base.
func measuredSpec(base string) SourceSpec {
	return SourceSpec{
		Name: "measured",
		Base: base,
		Roots: []SourceRoot{
			{Dir: "internal/core", Recursive: true, Filter: NonTestGo},
			{Dir: ".", Recursive: false, Filter: ExactNames("go.mod", "go.sum")},
		},
	}
}

func measuredTree(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	writeTree(t, base, map[string]string{
		"go.mod":                             "module x",
		"go.sum":                             "",
		"README.md":                          "not code",
		"internal/core/node/service.go":      "package node",
		"internal/core/node/service_test.go": "package node",
		"internal/core/node/Zeta.go":         "package node",
		"internal/core/node/_hidden.go":      "package node",
		"internal/core/node/notes.txt":       "not code",
		"internal/core/a.go":                 "package core",
		"internal/core/a/b.go":               "package a",
		"internal/core/a-b.go":               "package core",
		"internal/other/x.go":                "package other",
	})
	return base
}

func TestSnapshotListsFilteredFilesFromSeveralRootsInByteOrder(t *testing.T) {
	snapshot, err := TakeSnapshot(context.Background(), measuredSpec(measuredTree(t)))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var paths []string
	for _, file := range snapshot.Files() {
		paths = append(paths, file.Path)
	}
	want := []string{
		"go.mod",
		"go.sum",
		"internal/core/a-b.go",
		"internal/core/a.go",
		"internal/core/a/b.go",
		"internal/core/node/Zeta.go",
		"internal/core/node/_hidden.go",
		"internal/core/node/service.go",
	}
	if !slices.Equal(paths, want) {
		t.Fatalf("files\n%v\nwant (byte order, test files and non-Go excluded)\n%v", paths, want)
	}
	manifest := string(snapshot.Manifest())
	if !strings.HasPrefix(manifest, "runjournal-sources/v1\n") {
		t.Fatalf("manifest does not start with its format tag:\n%s", manifest)
	}
	if strings.Index(manifest, "internal/core/a-b.go") > strings.Index(manifest, "internal/core/a.go") {
		t.Fatalf("manifest is not in byte order of names:\n%s", manifest)
	}
}

func TestNonRecursiveRootReadsOnlyItsOwnDirectory(t *testing.T) {
	base := measuredTree(t)
	snapshot, err := TakeSnapshot(context.Background(), SourceSpec{
		Name: "flat", Base: base, Roots: []SourceRoot{{Dir: "internal/core", Filter: AnyGo}},
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var paths []string
	for _, file := range snapshot.Files() {
		paths = append(paths, file.Path)
	}
	if !slices.Equal(paths, []string{"internal/core/a-b.go", "internal/core/a.go"}) {
		t.Fatalf("non-recursive root read %v", paths)
	}
}

func TestFilters(t *testing.T) {
	loadstandFiles, err := BaseGlob("loadstand_*_test.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if _, err := BaseGlob("["); !errors.Is(err, ErrInvalidSourceSpec) {
		t.Fatalf("a malformed pattern = %v, want ErrInvalidSourceSpec", err)
	}
	cases := []struct {
		name   string
		filter FileFilter
		path   string
		want   bool
	}{
		{"non-test go", NonTestGo, "a/b.go", true},
		{"non-test go refuses tests", NonTestGo, "a/b_test.go", false},
		{"non-test go refuses other", NonTestGo, "a/b.txt", false},
		{"any go takes tests", AnyGo, "a/b_test.go", true},
		{"test go takes tests", TestGo, "a/b_test.go", true},
		{"test go refuses code", TestGo, "a/b.go", false},
		{"test go refuses other", TestGo, "a/b_test.txt", false},
		{"exact names match base name", ExactNames("go.mod"), "go.mod", true},
		{"exact names refuse nested", ExactNames("go.mod"), "sub/go.mod", false},
		{"glob on base name", loadstandFiles, "internal/core/node/loadstand_plan_test.go", true},
		{"glob refuses others", loadstandFiles, "internal/core/node/service_test.go", false},
	}
	for _, testCase := range cases {
		if got := testCase.filter(testCase.path); got != testCase.want {
			t.Fatalf("%s: filter(%q) = %v, want %v", testCase.name, testCase.path, got, testCase.want)
		}
	}
}

func TestStampIsDerivedFromTheFiles(t *testing.T) {
	ctx := context.Background()
	base := measuredTree(t)
	spec := measuredSpec(base)
	stampNow := func() Stamp {
		t.Helper()
		snapshot, err := TakeSnapshot(ctx, spec)
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		return snapshot.Stamp().Stamp
	}

	original := stampNow()
	if again := stampNow(); again != original {
		t.Fatalf("the same tree stamps as %s and %s", original, again)
	}
	writeTree(t, base, map[string]string{"internal/core/a/b.go": "package a // one byte more"})
	edited := stampNow()
	if edited == original {
		t.Fatalf("an edit of a measured file did not change the stamp")
	}
	writeTree(t, base, map[string]string{"internal/core/node/extra_test.go": "package node"})
	if stampNow() != edited {
		t.Fatalf("a filtered-out file changed the stamp")
	}
	if err := os.Rename(filepath.Join(base, "internal/core/a.go"), filepath.Join(base, "internal/core/c.go")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if stampNow() == edited {
		t.Fatalf("a renamed file did not change the stamp")
	}
}

func TestSnapshotRefusals(t *testing.T) {
	base := measuredTree(t)
	cases := map[string]struct {
		spec SourceSpec
		want error
	}{
		"nothing matches": {SourceSpec{Name: "x", Base: base, Roots: []SourceRoot{
			{Dir: "internal/core", Filter: ExactNames("absent.go")}}}, ErrNoSources},
		"root escapes base": {SourceSpec{Name: "x", Base: base, Roots: []SourceRoot{
			{Dir: "../elsewhere", Filter: AnyGo}}}, ErrInvalidSourceSpec},
		"absolute root": {SourceSpec{Name: "x", Base: base, Roots: []SourceRoot{
			{Dir: base, Filter: AnyGo}}}, ErrInvalidSourceSpec},
		"overlapping roots": {SourceSpec{Name: "x", Base: base, Roots: []SourceRoot{
			{Dir: "internal/core", Recursive: true, Filter: AnyGo},
			{Dir: "internal/core/node", Filter: AnyGo}}}, ErrInvalidSourceSpec},
		"no filter": {SourceSpec{Name: "x", Base: base, Roots: []SourceRoot{{Dir: "."}}}, ErrInvalidSourceSpec},
		"bad name":  {SourceSpec{Name: "Has Space", Base: base, Roots: []SourceRoot{{Dir: ".", Filter: AnyGo}}}, ErrInvalidSourceSpec},
		"no roots":  {SourceSpec{Name: "x", Base: base}, ErrInvalidSourceSpec},
	}
	for name, testCase := range cases {
		if _, err := TakeSnapshot(context.Background(), testCase.spec); !errors.Is(err, testCase.want) {
			t.Fatalf("%s: TakeSnapshot = %v, want %v", name, err, testCase.want)
		}
	}
}

func TestSnapshotRefusesASymlinkedSource(t *testing.T) {
	fsprobe.RequireSymlinks(t)
	base := measuredTree(t)
	outside := filepath.Join(t.TempDir(), "outside.go")
	writeTree(t, filepath.Dir(outside), map[string]string{"outside.go": "package outside"})
	if err := os.Symlink(outside, filepath.Join(base, "internal/core/link.go")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := TakeSnapshot(context.Background(), measuredSpec(base)); !errors.Is(err, ErrIrregularSource) {
		t.Fatalf("TakeSnapshot over a symlinked source = %v, want ErrIrregularSource", err)
	}
}

func TestVerifyTreeNoticesAChangedSource(t *testing.T) {
	ctx := context.Background()
	base := measuredTree(t)
	snapshot, err := TakeSnapshot(ctx, measuredSpec(base))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := snapshot.VerifyTree(ctx); err != nil {
		t.Fatalf("an unchanged tree does not verify: %v", err)
	}
	writeTree(t, base, map[string]string{"internal/core/node/new.go": "package node"})
	if err := snapshot.VerifyTree(ctx); !errors.Is(err, ErrSourcesChanged) {
		t.Fatalf("VerifyTree after an added file = %v, want ErrSourcesChanged", err)
	}
}

func TestKeptSourcesAreVerifiedAgainstTheirFiles(t *testing.T) {
	tamperings := map[string]func(t *testing.T, kept string){
		"edited file": func(t *testing.T, kept string) {
			t.Helper()
			writeTree(t, kept, map[string]string{"internal/core/a.go": "package core // edited"})
		},
		"deleted file": func(t *testing.T, kept string) {
			t.Helper()
			if err := os.Remove(filepath.Join(kept, "go.mod")); err != nil {
				t.Fatalf("remove: %v", err)
			}
		},
		"extra file": func(t *testing.T, kept string) {
			t.Helper()
			writeTree(t, kept, map[string]string{"internal/core/unlisted.go": "package core"})
		},
		"edited manifest": func(t *testing.T, kept string) {
			t.Helper()
			path := filepath.Join(kept, "MANIFEST.sha256")
			raw, err := os.ReadFile(path) //nolint:gosec // a file this test wrote
			if err != nil {
				t.Fatalf("read manifest: %v", err)
			}
			edited := strings.Replace(string(raw), "go.sum", "go.sun", 1)
			if err := os.WriteFile(path, []byte(edited), 0o600); err != nil {
				t.Fatalf("write manifest: %v", err)
			}
		},
	}

	for name, tamper := range tamperings {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			snapshot, err := TakeSnapshot(ctx, measuredSpec(measuredTree(t)))
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			journal, err := New(Config{Dir: dir, Sources: []Snapshot{snapshot}, Clock: newSteppingClock()})
			if err != nil {
				t.Fatalf("journal: %v", err)
			}
			if err := journal.KeepSources(ctx); err != nil {
				t.Fatalf("keep: %v", err)
			}
			if err := journal.KeepSources(ctx); err != nil {
				t.Fatalf("keeping the same sources twice must be idempotent: %v", err)
			}
			if err := VerifyKept(ctx, dir, snapshot.Stamp()); err != nil {
				t.Fatalf("freshly kept sources do not verify: %v", err)
			}

			tamper(t, KeptDir(dir, snapshot.Stamp()))
			if err := VerifyKept(ctx, dir, snapshot.Stamp()); !errors.Is(err, ErrSnapshotMismatch) {
				t.Fatalf("VerifyKept after tampering = %v, want ErrSnapshotMismatch", err)
			}
			if err := journal.KeepSources(ctx); !errors.Is(err, ErrSnapshotMismatch) {
				t.Fatalf("KeepSources over tampered sources = %v, want ErrSnapshotMismatch", err)
			}
		})
	}
}

func TestSnapshotOfThisPackageIsTakenFromItsOwnFiles(t *testing.T) {
	snapshot, err := TakeSnapshot(context.Background(), SourceSpec{
		Name: "self", Base: ".", Roots: []SourceRoot{{Dir: ".", Filter: NonTestGo}},
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for _, file := range snapshot.Files() {
		if strings.HasSuffix(file.Path, "_test.go") {
			t.Fatalf("a test file %s entered a non-test fingerprint", file.Path)
		}
	}
	if len(snapshot.Files()) == 0 {
		t.Fatalf("this package's own sources were not found")
	}
}

// --- a fingerprint fails closed ----------------------------------------------

func TestEveryRootMustMatchAFile(t *testing.T) {
	base := measuredTree(t)
	spec := measuredSpec(base)
	spec.Roots = append(spec.Roots, SourceRoot{Dir: "internal/other", Filter: ExactNames("absent.go")})
	if _, err := TakeSnapshot(context.Background(), spec); !errors.Is(err, ErrNoSources) {
		t.Fatalf("a root that matched nothing = %v, want ErrNoSources even though other roots matched", err)
	}
}

func TestAMissingRootMatchesNothing(t *testing.T) {
	for _, recursive := range []bool{true, false} {
		spec := SourceSpec{Name: "x", Base: measuredTree(t), Roots: []SourceRoot{
			{Dir: "internal/core", Recursive: true, Filter: AnyGo},
			{Dir: "internal/absent", Recursive: recursive, Filter: AnyGo},
		}}
		if _, err := TakeSnapshot(context.Background(), spec); !errors.Is(err, ErrNoSources) {
			t.Fatalf("recursive=%v: a missing root = %v, want ErrNoSources", recursive, err)
		}
	}
}

func TestARootMustBeARealDirectory(t *testing.T) {
	for _, recursive := range []bool{true, false} {
		spec := SourceSpec{Name: "x", Base: measuredTree(t), Roots: []SourceRoot{
			{Dir: "go.mod", Recursive: recursive, Filter: ExactNames("go.mod")},
		}}
		if _, err := TakeSnapshot(context.Background(), spec); !errors.Is(err, ErrIrregularSource) {
			t.Fatalf("recursive=%v: a file as a root = %v, want ErrIrregularSource", recursive, err)
		}
	}
}

func TestSymlinksAreRefusedWhereverTheyAre(t *testing.T) {
	fsprobe.RequireSymlinks(t)
	outsideDir := t.TempDir()
	writeTree(t, outsideDir, map[string]string{"elsewhere.go": "package elsewhere"})

	cases := map[string]struct {
		link      string
		target    string
		root      SourceRoot
		rootIsDir bool
	}{
		"root is a symlink": {
			link: "linked", target: filepath.Join("internal", "core"),
			root: SourceRoot{Dir: "linked", Recursive: true, Filter: AnyGo},
		},
		"a component of the root is a symlink": {
			link: "via", target: "internal",
			root: SourceRoot{Dir: "via/core", Recursive: true, Filter: AnyGo},
		},
		"symlinked directory inside a recursive root": {
			link: "internal/core/vendored", target: outsideDir,
			root: SourceRoot{Dir: "internal/core", Recursive: true, Filter: AnyGo},
		},
		"symlinked directory inside a flat root": {
			link: "internal/core/vendored", target: outsideDir,
			root: SourceRoot{Dir: "internal/core", Filter: AnyGo},
		},
		"symlinked file the filter does not select, recursive": {
			link: "internal/core/node/notes.md", target: filepath.Join(outsideDir, "elsewhere.go"),
			root: SourceRoot{Dir: "internal/core", Recursive: true, Filter: NonTestGo},
		},
		"symlinked file the filter does not select, flat": {
			link: "internal/core/notes.md", target: filepath.Join(outsideDir, "elsewhere.go"),
			root: SourceRoot{Dir: "internal/core", Filter: NonTestGo},
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			base := measuredTree(t)
			target := testCase.target
			if !filepath.IsAbs(target) {
				target = filepath.Join(base, target)
			}
			if err := os.Symlink(target, filepath.Join(base, filepath.FromSlash(testCase.link))); err != nil {
				t.Fatalf("symlink: %v", err)
			}
			spec := SourceSpec{Name: "x", Base: base, Roots: []SourceRoot{testCase.root}}
			if _, err := TakeSnapshot(context.Background(), spec); !errors.Is(err, ErrIrregularSource) {
				t.Fatalf("TakeSnapshot = %v, want ErrIrregularSource", err)
			}
		})
	}
}

func TestVerifyTreeNoticesARemovedOrEmptiedRoot(t *testing.T) {
	removals := map[string]func(t *testing.T, base string){
		"root removed": func(t *testing.T, base string) {
			t.Helper()
			if err := os.RemoveAll(filepath.Join(base, "internal/core")); err != nil {
				t.Fatalf("remove: %v", err)
			}
		},
		"root emptied": func(t *testing.T, base string) {
			t.Helper()
			for _, rel := range []string{"go.mod", "go.sum"} {
				if err := os.Remove(filepath.Join(base, rel)); err != nil {
					t.Fatalf("remove: %v", err)
				}
			}
		},
	}
	for name, remove := range removals {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base := measuredTree(t)
			snapshot, err := TakeSnapshot(ctx, measuredSpec(base))
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			remove(t, base)
			if err := snapshot.VerifyTree(ctx); !errors.Is(err, ErrSourcesChanged) {
				t.Fatalf("VerifyTree = %v, want ErrSourcesChanged", err)
			}
		})
	}
}

// TestAKeptCopyHoldsFilesNotLinks: a listed file replaced by a link to a
// listed sibling of the same content hashes correctly — only the entry type
// shows that the copy is no longer the bytes that were measured.
func TestAKeptCopyHoldsFilesNotLinks(t *testing.T) {
	fsprobe.RequireSymlinks(t)
	ctx := context.Background()
	base := t.TempDir()
	writeTree(t, base, map[string]string{"pkg/a.go": "package pkg", "pkg/b.go": "package pkg"})
	snapshot, err := TakeSnapshot(ctx, SourceSpec{
		Name: "twins", Base: base, Roots: []SourceRoot{{Dir: "pkg", Filter: AnyGo}},
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dir := t.TempDir()
	journal := newJournal(t, dir, []Snapshot{snapshot})
	if err := journal.KeepSources(ctx); err != nil {
		t.Fatalf("keep: %v", err)
	}
	kept := KeptDir(dir, snapshot.Stamp())
	if err := os.Remove(filepath.Join(kept, "pkg/b.go")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := os.Symlink("a.go", filepath.Join(kept, "pkg/b.go")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := VerifyKept(ctx, dir, snapshot.Stamp()); !errors.Is(err, ErrSnapshotMismatch) {
		t.Fatalf("VerifyKept over a linked copy = %v, want ErrSnapshotMismatch", err)
	}
}

func TestVerifyKeptRefusesAStampThatIsNotAName(t *testing.T) {
	dir := t.TempDir()
	stamps := []SourceStamp{
		{Name: "..", Stamp: "0011223344556677"},
		{Name: "measured", Stamp: "../../etc"},
		{Name: "measured", Stamp: ""},
	}
	for _, stamp := range stamps {
		if err := VerifyKept(context.Background(), dir, stamp); !errors.Is(err, ErrInvalidSourceSpec) {
			t.Fatalf("VerifyKept(%+v) = %v, want ErrInvalidSourceSpec", stamp, err)
		}
	}
}

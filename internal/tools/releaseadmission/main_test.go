package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/release/admission"
	"github.com/piratecash/corsa/internal/release/admission/mocks"
)

const repoRoot = "../../.."

func committedManifest() string {
	return filepath.Join(repoRoot, filepath.FromSlash(admission.ManifestPath))
}

// TestClosedGateExitsNonZeroAndNamesWhatIsMissing is the contract the
// workflow relies on: a refusal is exit status 1, and the output says why.
func TestClosedGateExitsNonZeroAndNamesWhatIsMissing(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), nil, &stdout, &stderr, committedManifest(), unusedHistory(t))
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	for _, id := range admission.RequiredConditions() {
		if id == "owner_release_shape" { // decided; the gate pins this in its own test
			continue
		}
		if !strings.Contains(stdout.String(), "outstanding condition: "+string(id)) {
			t.Errorf("stdout does not name outstanding condition %q:\n%s", id, stdout.String())
		}
	}
	if !strings.Contains(stdout.String(), "no owner decision to release") {
		t.Errorf("stdout does not say the release decision is missing:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "release admission refused") {
		t.Errorf("stderr does not carry the refusal:\n%s", stderr.String())
	}
}

func TestMalformedTagIsRefused(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"-tag", "nightly"}, &stdout, &stderr, committedManifest(), unusedHistory(t))
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 for a tag that is not a release version", code)
	}
}

func TestMissingManifestIsRefused(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), nil, &stdout, &stderr, filepath.Join(t.TempDir(), "absent.json"), unusedHistory(t))
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 for a missing manifest", code)
	}
}

// openManifestAt writes a manifest that admits the current build, with the
// final candidate at commit.
func openManifestAt(t *testing.T, commit string) string {
	t.Helper()
	conditions := make([]map[string]any, 0, len(admission.RequiredConditions()))
	for _, id := range admission.RequiredConditions() {
		conditions = append(conditions, map[string]any{
			"id": string(id), "condition": "c", "executor": "developer", "check": "k",
			"evidence": map[string]any{"report": "r", "commit": commit}, "status": "done",
		})
	}
	raw, err := json.Marshal(map[string]any{
		"schema":                      1,
		"activation_protocol_version": config.ProtocolVersionSecureSession,
		"release_decision":            map[string]any{"number": 70, "date": "2026-11-02", "version": config.CorsaVersion},
		"conditions":                  conditions,
	})
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	path := filepath.Join(t.TempDir(), "admission.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

// unusedHistory fails the test if the gate consults git on a path that
// must be decided without it: the mock has no expectations, so any call to
// it is an unexpected call and fails the test.
func unusedHistory(t *testing.T) *mocks.MockReleaseTree {
	t.Helper()
	return mocks.NewMockReleaseTree(t)
}

// candidateHistory reports the given files as changed since any commit and
// the release notes of the build version as present. The gate may consult
// it or not, so neither call is required.
func candidateHistory(t *testing.T, files []string) *mocks.MockReleaseTree {
	t.Helper()
	history := mocks.NewMockReleaseTree(t)
	history.EXPECT().FilesChangedSince(mock.Anything, mock.Anything).Return(files, nil).Maybe()
	history.EXPECT().IsRegularFile(mock.Anything, "docs/release-notes/RELEASE-v"+config.CorsaVersion+".md").Return(true, nil).Maybe()
	history.EXPECT().IsRegularFile(mock.Anything, mock.Anything).Return(false, nil).Maybe()
	return history
}

const candidateCommit = "0123456789abcdef0123456789abcdef01234567"

// TestOpenGateExitsZero runs the whole path an admitted release takes.
func TestOpenGateExitsZero(t *testing.T) {
	path := openManifestAt(t, candidateCommit)
	var stdout, stderr bytes.Buffer
	history := candidateHistory(t, []string{admission.ManifestPath})
	code := execute(context.Background(), []string{"-tag", "v" + config.CorsaVersion}, &stdout, &stderr, path, history)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "release admitted") {
		t.Fatalf("stdout does not report the admission:\n%s", stdout.String())
	}
}

func TestOpenManifestWithAnotherTagIsRefused(t *testing.T) {
	path := openManifestAt(t, candidateCommit)
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"-tag", "v0.0.1"}, &stdout, &stderr, path, candidateHistory(t, nil))
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 for a tag that is not the build version", code)
	}
}

func TestCodeChangedAfterTheCandidateIsRefused(t *testing.T) {
	path := openManifestAt(t, candidateCommit)
	var stdout, stderr bytes.Buffer
	history := candidateHistory(t, []string{"internal/core/node/service.go"})
	code := execute(context.Background(), nil, &stdout, &stderr, path, history)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 when code changed after the final candidate", code)
	}
}

// TestGitHistoryReadsTheRepository exercises the real history source with
// read-only git: the commit under test is known, a made-up one is an error
// rather than "nothing changed".
func TestGitHistoryReadsTheRepository(t *testing.T) {
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Skipf("no git history to read here: %v", err)
	}
	history := gitTree{root: repoRoot}
	if _, err := history.FilesChangedSince(context.Background(), admission.CommitHash(strings.TrimSpace(string(head)))); err != nil {
		t.Fatalf("FilesChangedSince(HEAD) error = %v", err)
	}
	if _, err := history.FilesChangedSince(context.Background(), "0000000000000000000000000000000000000000"); err == nil {
		t.Fatal("FilesChangedSince(unknown commit) returned no error; an unknown candidate would read as unchanged")
	}
}

// TestGitTreeRefusesACandidateOffTheHistory: the candidate must stay
// reachable from the released commit; a commit elsewhere is not "the same
// code with a manifest edit".
func TestGitTreeRefusesACandidateOffTheHistory(t *testing.T) {
	out, err := exec.Command("git", "rev-list", "--all", "--not", "HEAD", "--max-count=1").Output()
	if err != nil {
		t.Skipf("no git history to read here: %v", err)
	}
	other := strings.TrimSpace(string(out))
	if other == "" {
		t.Skip("every commit in this clone is an ancestor of HEAD; no off-history commit to test with")
	}
	if _, err := (gitTree{root: repoRoot}).FilesChangedSince(context.Background(), admission.CommitHash(other)); err == nil {
		t.Fatalf("FilesChangedSince(%s, not an ancestor of HEAD) returned no error", other)
	}
}

func TestIndexEntriesHiddenFromDiffAreReported(t *testing.T) {
	got := hiddenIndexEntries([]string{"H a.go", "h b.go", "S c.go", "s d.go", "M e.go"})
	want := []string{"h b.go", "S c.go", "s d.go", "M e.go"}
	if !slices.Equal(got, want) {
		t.Fatalf("hiddenIndexEntries = %v, want %v", got, want)
	}
}

func TestOnlyBuildCachesMayBeUntracked(t *testing.T) {
	got := unexpectedUntracked([]string{".gocache/ab/cd", ".gomodcache/cache/x", "internal/core/evil.go", ".gocache", "dist/corsa"})
	want := []string{"internal/core/evil.go", ".gocache", "dist/corsa"}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpectedUntracked = %v, want %v", got, want)
	}
}

func TestSplitNulKeepsNamesWithNewlines(t *testing.T) {
	got := splitNul([]byte("a.go\x00odd\nname.go\x00"))
	if !slices.Equal(got, []string{"a.go", "odd\nname.go"}) {
		t.Fatalf("splitNul = %q", got)
	}
}

func TestIsRegularFileRefusesLinks(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notes.md"), []byte("notes"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	tree := gitTree{root: dir}
	if ok, err := tree.IsRegularFile(context.Background(), "notes.md"); err != nil || !ok {
		t.Fatalf("IsRegularFile(regular) = %v, %v; want true", ok, err)
	}
	if ok, err := tree.IsRegularFile(context.Background(), "absent.md"); err != nil || ok {
		t.Fatalf("IsRegularFile(absent) = %v, %v; want false, nil", ok, err)
	}
	if err := os.Symlink("notes.md", filepath.Join(dir, "link.md")); err != nil {
		t.Skipf("cannot create a symlink here: %v", err)
	}
	if ok, err := tree.IsRegularFile(context.Background(), "link.md"); err != nil || ok {
		t.Fatalf("IsRegularFile(symlink) = %v, %v; want false, nil", ok, err)
	}
}

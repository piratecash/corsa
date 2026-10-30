package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/release/admission"
	"github.com/piratecash/corsa/internal/release/admission/mocks"
)

const repoRoot = "../../.."

func committedManifest() string {
	return filepath.Join(repoRoot, filepath.FromSlash(admission.ManifestPath))
}

// committedConditions reads the conditions of the committed manifest.
func committedConditions(t *testing.T) []admission.Condition {
	t.Helper()
	file, err := os.Open(committedManifest())
	if err != nil {
		t.Fatalf("open the committed manifest: %v", err)
	}
	defer func() {
		// A read-only file: a close error cannot lose data, and the decode
		// below has already reported anything that went wrong reading it.
		_ = file.Close()
	}()
	manifest, err := admission.DecodeManifest(file)
	if err != nil {
		t.Fatalf("decode the committed manifest: %v", err)
	}
	return manifest.Conditions
}

// TestClosedGateExitsNonZeroAndNamesWhatIsMissing is the contract the
// workflow relies on: a refusal is exit status 1, and the output says why.
func TestClosedGateExitsNonZeroAndNamesWhatIsMissing(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), nil, &stdout, &stderr, committedManifest(), unusedHistory(t))
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	// The expectation comes from the committed manifest itself: closing a
	// condition there is legitimate work and must not break this test, while
	// a condition the output forgets or wrongly lists still does.
	for _, condition := range committedConditions(t) {
		named := strings.Contains(stdout.String(), "outstanding condition: "+string(condition.ID)+"\n")
		outstanding := condition.Status != admission.ConditionDone && condition.Status != admission.ConditionDeferred
		if named != outstanding {
			t.Errorf("condition %q (status %q): named as outstanding = %t, want %t:\n%s",
				condition.ID, condition.Status, named, outstanding, stdout.String())
		}
	}
	if !strings.Contains(stdout.String(), "no owner decision to release") {
		t.Errorf("stdout does not say the release decision is missing:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "release admission refused") {
		t.Errorf("stderr does not carry the refusal:\n%s", stderr.String())
	}
}

// An explicit release tag must report the ordinary closed S6 gate.
func TestExplicitReleaseTagUsesClosedS6Gate(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"-tag", "v" + config.CorsaVersion},
		&stdout, &stderr, committedManifest(), unusedHistory(t))
	if code != 1 {
		t.Fatalf("explicit release tag bypassed the S6 gate: code=%d; stdout:\n%s\nstderr:\n%s",
			code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "outstanding condition:") ||
		!strings.Contains(stdout.String(), "no owner decision to release") ||
		!strings.Contains(stderr.String(), "release admission refused") {
		t.Fatalf("explicit release tag did not report the ordinary closed S6 gate; stdout:\n%s\nstderr:\n%s",
			stdout.String(), stderr.String())
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
		"schema":                      2,
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

func TestTorDeferralIsReportedWithoutClaimingSuccessfulMeasurement(t *testing.T) {
	for _, open := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "open"}[open], func(t *testing.T) {
			path := openManifestAt(t, candidateCommit)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err = json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			for _, row := range doc["conditions"].([]any) {
				c := row.(map[string]any)
				if c["id"] == "tor_phase_measurement" {
					c["status"] = "deferred"
					c["evidence"] = map[string]any{"report": string(admission.TorDeferralDecision), "commit": nil}
				}
			}
			if !open {
				doc["release_decision"] = nil
			}
			raw, err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := execute(context.Background(), nil, &stdout, &stderr, path, candidateHistory(t, nil))
			wantCode := 1
			if open {
				wantCode = 0
			}
			if code != wantCode || !strings.Contains(stdout.String(), "deferred condition: tor_phase_measurement (UNVERIFIED; owner decision 2026-10-23") || strings.Contains(stdout.String(), "every condition is done") || strings.Contains(stdout.String(), "outstanding condition: tor_phase_measurement") {
				t.Fatalf("deferral misreported: code%d stdout=%s stderr=%s", code, &stdout, &stderr)
			}
		})
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

// gitHistoryFixture creates a real, clean repository owned by this test.
// Both commits have the same tree but no parents: equal files are not proof
// that a candidate belongs to HEAD's history. No CORSA checkout is consulted.
func gitHistoryFixture(t *testing.T) (context.Context, gitTree, admission.CommitHash, admission.CommitHash) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	root := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", append([]string{
			"-c", "core.hooksPath=" + filepath.Join(root, ".git", "disabled-hooks"),
			"-c", "commit.gpgSign=false", "-c", "gc.auto=0", "-c", "maintenance.auto=false",
		}, args...)...)
		cmd.WaitDelay = time.Second
		cmd.Dir = root
		cmd.Env = append(gitIsolatedEnv(),
			"GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.invalid",
			"GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.invalid",
			"GIT_AUTHOR_DATE=2000-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2000-01-01T00:00:00Z")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("own Git fixture %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "--quiet", "--template=", "--initial-branch=fixture-main", "--object-format=sha1")
	if err := os.WriteFile(filepath.Join(root, "fixture.txt"), []byte("same fixture tree\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run("add", "--force", "--", "fixture.txt")
	tree := run("write-tree")
	head := run("commit-tree", tree, "-m", "fixture A")
	other := run("commit-tree", tree, "-m", "fixture B")
	if head == other || len(head) != 40 || len(other) != 40 {
		t.Fatalf("fixture roots are not distinct SHA-1 commits: %q / %q", head, other)
	}
	run("update-ref", "refs/heads/fixture-main", head)
	run("update-ref", "refs/heads/fixture-other", other)
	if run("rev-parse", "HEAD") != head || run("rev-list", "--parents", "-n", "1", head) != head ||
		run("rev-list", "--parents", "-n", "1", other) != other ||
		run("rev-parse", head+"^{tree}") != tree || run("rev-parse", other+"^{tree}") != tree ||
		run("rev-list", "--all", "--not", "HEAD", "--max-count=1") != other ||
		run("status", "--porcelain", "--untracked-files=all") != "" {
		t.Fatal("own Git fixture does not have clean HEAD and unrelated same-tree root")
	}
	return ctx, gitTree{root: root}, admission.CommitHash(head), admission.CommitHash(other)
}

// TestGitHistoryReadsTheRepository exercises the real history source against
// a known current commit and a nonexistent commit, without clone-dependent skips.
func TestGitHistoryReadsTheRepository(t *testing.T) {
	ctx, history, head, _ := gitHistoryFixture(t)
	changed, err := history.FilesChangedSince(ctx, head)
	if err != nil || len(changed) != 0 {
		t.Fatalf("FilesChangedSince(HEAD) = %v, %v; want clean fixture", changed, err)
	}
	if _, err := history.FilesChangedSince(ctx, "0000000000000000000000000000000000000000"); err == nil {
		t.Fatal("FilesChangedSince(unknown commit) returned no error; an unknown candidate would read as unchanged")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("history fixture deadline, not evidence of commit refusal: %v", err)
	}
}

// TestGitTreeRefusesACandidateOffTheHistory rejects an existing root with
// identical files: it must be reachable from HEAD, not merely have equal bytes.
func TestGitTreeRefusesACandidateOffTheHistory(t *testing.T) {
	ctx, history, _, other := gitHistoryFixture(t)
	_, err := history.FilesChangedSince(ctx, other)
	var exit *exec.ExitError
	if err == nil || !errors.As(err, &exit) || exit.ExitCode() != 1 || ctx.Err() != nil {
		t.Fatalf("FilesChangedSince(%s, not an ancestor of HEAD) = %v; want real ancestry rejection, context=%v", other, err, ctx.Err())
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

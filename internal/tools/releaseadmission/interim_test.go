package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/release/admission"
)

func TestInterimTreeDigestHasCanonicalRawFraming(t *testing.T) {
	entries := []interimEntry{
		{path: "tools/run.sh", mode: "100755", body: []byte("#!/bin/sh\n")},
		{path: admission.InterimApprovalPath, mode: "100644", body: []byte("approval one")},
		{path: "a.go", mode: "100644", body: []byte("A\x00B\n")},
	}
	// Independent vector: domain prefix, then sorted path/mode/body, each
	// preceded by its unsigned 64-bit big-endian byte length. Only approval
	// content is excluded to avoid requiring a hash fixed point.
	const expected = "38e2cacd4aaad36afc6b106d8c3eab61b07a5076d786a61359a82643feb91414"
	got, err := hashInterimEntries(entries)
	if err != nil || got != expected {
		t.Fatalf("canonical digest = %q, %v; want %s", got, err, expected)
	}
	if entries[0].path != "tools/run.sh" || entries[2].path != "a.go" {
		t.Fatal("digest sorted the caller's slice in place")
	}
	entries[1].body = []byte("different committed approval")
	got, err = hashInterimEntries([]interimEntry{entries[2], entries[1], entries[0]})
	if err != nil || got != expected {
		t.Fatalf("order/approval-self changed source digest: %q, %v", got, err)
	}
	for _, mutate := range []struct {
		name  string
		entry interimEntry
	}{
		{"raw-byte", interimEntry{path: "a.go", mode: "100644", body: []byte("A\x00C\n")}},
		{"line-ending", interimEntry{path: "a.go", mode: "100644", body: []byte("A\x00B\r\n")}},
		{"rename", interimEntry{path: "b.go", mode: "100644", body: []byte("A\x00B\n")}},
		{"executable-mode", interimEntry{path: "a.go", mode: "100755", body: []byte("A\x00B\n")}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			got, err := hashInterimEntries([]interimEntry{entries[0], mutate.entry})
			if err != nil || got == expected {
				t.Fatalf("source mutation did not change digest: %q, %v", got, err)
			}
		})
	}
	left, err := hashInterimEntries([]interimEntry{{path: "ab", mode: "100644", body: []byte("c")}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hashInterimEntries([]interimEntry{{path: "a", mode: "100644", body: []byte("bc")}})
	if err != nil || left == right {
		t.Fatalf("path/body boundaries collided: %s / %s, %v", left, right, err)
	}
}

func TestInterimTreeDigestRefusesAmbiguousEntries(t *testing.T) {
	for _, path := range []string{"", ".", "/absolute", "..", "../escape", "a/../b", "a//b", "./a", "nul\x00path"} {
		t.Run("path-"+path, func(t *testing.T) {
			if _, err := hashInterimEntries([]interimEntry{{path: path, mode: "100644"}}); err == nil {
				t.Fatalf("invalid path %q accepted", path)
			}
		})
	}
	for _, mode := range []string{"", "100600", "120000", "160000"} {
		t.Run("mode-"+mode, func(t *testing.T) {
			if _, err := hashInterimEntries([]interimEntry{{path: "a", mode: mode}}); err == nil {
				t.Fatalf("unsupported mode %q accepted", mode)
			}
		})
	}
	for _, path := range []string{"a", admission.InterimApprovalPath} {
		if _, err := hashInterimEntries([]interimEntry{{path: path, mode: "100644"}, {path: path, mode: "100644"}}); err == nil {
			t.Fatalf("duplicate path %q accepted, including the excluded approval", path)
		}
	}
	if _, err := hashInterimEntries(nil); err == nil {
		t.Fatal("empty source inventory accepted")
	}
}

// Only this test's temporary Git repository is mutated. Production Git reads
// retain their real implementation, isolated environment and real blob I/O.
func interimFixtureGit(t *testing.T, ctx context.Context, tree gitTree, args ...string) string {
	t.Helper()
	options := []string{"-c", "core.hooksPath=" + filepath.Join(tree.root, ".git", "disabled-hooks"),
		"-c", "commit.gpgSign=false", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid"}
	out, err := tree.git(ctx, append(options, args...)...)
	if err != nil {
		t.Fatalf("own interim fixture Git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}

func interimSnapshotFixture(t *testing.T) (context.Context, gitTree, admission.CommitHash, admission.CommitHash, []interimEntry) {
	t.Helper()
	t.Setenv("GITHUB_SHA", "")
	t.Setenv("GITHUB_REF", "")
	ctx, tree, _, unrelated := gitHistoryFixture(t)
	entries := []interimEntry{
		{path: "fixture.txt", mode: "100644", body: []byte("same fixture tree\n")},
		{path: admission.InterimApprovalPath, mode: "100644", body: []byte("{\"fixture\":\"opaque committed approval bytes\"}\n")},
		{path: "tools/run.sh", mode: "100755", body: []byte("#!/bin/sh\n")},
		{path: "inputs/regular.txt", mode: "100644", body: []byte("raw\x00input\r\n")},
		{path: ".gitignore", mode: "100644", body: []byte("/ignored.go\n")},
		{path: "docs/release-notes/RELEASE-v2.5.71.md", mode: "100644", body: []byte("Owned fixture release notes; not a product release claim.\n")},
	}
	for _, entry := range entries {
		path := filepath.Join(tree.root, filepath.FromSlash(entry.path))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		mode := fs.FileMode(0o600)
		if entry.mode == "100755" {
			mode = 0o700
		}
		if err := os.WriteFile(path, entry.body, mode); err != nil {
			t.Fatal(err)
		}
	}
	interimFixtureGit(t, ctx, tree, "add", "--force", "--", ".")
	interimFixtureGit(t, ctx, tree, "commit", "--quiet", "-m", "owned interim fixture")
	head := admission.CommitHash(interimFixtureGit(t, ctx, tree, "rev-parse", "HEAD"))
	interimFixtureGit(t, ctx, tree, "tag", "v2.5.71")
	return ctx, tree, head, unrelated, entries
}

func TestInterimSnapshotBindsRealTagRawFilesAndCommittedApproval(t *testing.T) {
	for _, annotated := range []bool{false, true} {
		name := "lightweight"
		if annotated {
			name = "annotated"
		}
		t.Run(name, func(t *testing.T) {
			ctx, tree, head, _, entries := interimSnapshotFixture(t)
			eventSHA := string(head)
			if annotated {
				interimFixtureGit(t, ctx, tree, "tag", "-d", "v2.5.71")
				interimFixtureGit(t, ctx, tree, "tag", "-a", "-m", "owned annotated tag", "v2.5.71")
				eventSHA = interimFixtureGit(t, ctx, tree, "rev-parse", "refs/tags/v2.5.71")
				if eventSHA == string(head) {
					t.Fatal("annotated event fixture did not produce a tag object")
				}
			}
			t.Setenv("GITHUB_SHA", eventSHA)
			t.Setenv("GITHUB_REF", "refs/tags/v2.5.71")
			digest, approval, commit, err := tree.interimSnapshot(ctx, "v2.5.71")
			want, hashErr := hashInterimEntries(entries)
			if err != nil || hashErr != nil || digest != want || commit != head || !bytes.Equal(approval, entries[1].body) {
				t.Fatalf("snapshot = %q, %q, %q, %v; want digest %q, exact committed approval and %s (hash error %v)", digest, approval, commit, err, want, head, hashErr)
			}
		})
	}
}

func TestInterimSnapshotRefusesDirtyHiddenAndRetargetedInputs(t *testing.T) {
	for _, name := range []string{"dirty-source", "dirty-approval", "staged-source", "staged-index-only", "untracked-go", "ignored-go", "hidden-index", "skip-worktree", "mode-hidden-by-config", "group-only-executable", "missing-notes", "empty-notes", "missing-tag", "tag-off-head", "wrong-event-sha", "wrong-event-object", "wrong-event-ref", "other-tag"} {
		t.Run(name, func(t *testing.T) {
			ctx, tree, _, unrelated, _ := interimSnapshotFixture(t)
			tag := "v2.5.71"
			write := func(path string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(tree.root, filepath.FromSlash(path)), []byte("changed\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "dirty-source", "staged-source", "staged-index-only":
				write("fixture.txt")
				if name != "dirty-source" {
					interimFixtureGit(t, ctx, tree, "add", "--", "fixture.txt")
				}
				if name == "staged-index-only" {
					if err := os.WriteFile(filepath.Join(tree.root, "fixture.txt"), []byte("same fixture tree\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			case "dirty-approval":
				write(admission.InterimApprovalPath)
			case "untracked-go":
				write("extra.go")
			case "ignored-go":
				write("ignored.go")
			case "hidden-index":
				interimFixtureGit(t, ctx, tree, "update-index", "--assume-unchanged", "fixture.txt")
			case "skip-worktree":
				interimFixtureGit(t, ctx, tree, "update-index", "--skip-worktree", "fixture.txt")
			case "mode-hidden-by-config", "group-only-executable":
				interimFixtureGit(t, ctx, tree, "config", "core.filemode", "false")
				mode := fs.FileMode(0o600)
				if name == "group-only-executable" {
					mode = 0o610
				}
				if err := os.Chmod(filepath.Join(tree.root, "tools/run.sh"), mode); err != nil {
					t.Fatal(err)
				}
			case "missing-notes", "empty-notes":
				const notes = "docs/release-notes/RELEASE-v2.5.71.md"
				if name == "missing-notes" {
					interimFixtureGit(t, ctx, tree, "rm", "--", notes)
				} else {
					if err := os.WriteFile(filepath.Join(tree.root, filepath.FromSlash(notes)), nil, 0o600); err != nil {
						t.Fatal(err)
					}
					interimFixtureGit(t, ctx, tree, "add", "--", notes)
				}
				interimFixtureGit(t, ctx, tree, "commit", "--quiet", "-m", "invalid owned notes fixture")
				interimFixtureGit(t, ctx, tree, "tag", "-f", tag)
			case "missing-tag":
				interimFixtureGit(t, ctx, tree, "tag", "-d", tag)
			case "tag-off-head":
				interimFixtureGit(t, ctx, tree, "tag", "-f", tag, string(unrelated))
			case "wrong-event-sha":
				t.Setenv("GITHUB_SHA", strings.Repeat("0", 40))
			case "wrong-event-object":
				interimFixtureGit(t, ctx, tree, "tag", "-a", "-m", "unrelated event fixture", "other-event", string(unrelated))
				t.Setenv("GITHUB_SHA", interimFixtureGit(t, ctx, tree, "rev-parse", "refs/tags/other-event"))
			case "wrong-event-ref":
				t.Setenv("GITHUB_REF", "refs/heads/dev")
			case "other-tag":
				tag = "v2.5.72"
				interimFixtureGit(t, ctx, tree, "tag", tag)
			}
			if _, _, _, err := tree.interimSnapshot(ctx, tag); err == nil {
				t.Fatal("invalid interim snapshot was admitted")
			}
			if ctx.Err() != nil {
				t.Fatalf("fixture deadline, not evidence of refusal: %v", ctx.Err())
			}
		})
	}
}

func TestInterimFileVerificationRefusesRawMismatchAndSymlinkParents(t *testing.T) {
	for _, name := range []string{"raw-bytes", "final-symlink", "parent-symlink"} {
		t.Run(name, func(t *testing.T) {
			ctx, tree, _, _, entries := interimSnapshotFixture(t)
			entry := entries[3]
			path := filepath.Join(tree.root, filepath.FromSlash(entry.path))
			if err := tree.verifyInterimFile(entry); err != nil {
				t.Fatalf("positive regular input refused: %v", err)
			}
			switch name {
			case "raw-bytes":
				if err := os.WriteFile(path, []byte("not the committed raw bytes"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "final-symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("../fixture.txt", path); err != nil {
					t.Fatal(err)
				}
			case "parent-symlink":
				parent := filepath.Dir(path)
				if err := os.Rename(parent, parent+"-target"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(parent+"-target", parent); err != nil {
					t.Fatal(err)
				}
			}
			if err := tree.verifyInterimFile(entry); err == nil {
				t.Fatal("raw mismatch or redirected input accepted")
			}
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestInterimSnapshotDoesNotFollowGitEnvironmentAndRequiresCommittedApproval(t *testing.T) {
	ctx, tree, head, _, entries := interimSnapshotFixture(t)
	want, err := hashInterimEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_CONFIG_GLOBAL"} {
		t.Setenv(name, filepath.Join(t.TempDir(), "not-a-repository"))
	}
	t.Setenv("CORSA_ALLOW_INTERIM_RELEASE", "1")
	t.Setenv("SKIP_RELEASE_ADMISSION", "1")
	digest, approval, commit, err := tree.interimSnapshot(ctx, "v2.5.71")
	if err != nil || digest != want || commit != head || !bytes.Equal(approval, entries[1].body) {
		t.Fatalf("Git environment changed the snapshot: %q, %q, %q, %v", digest, approval, commit, err)
	}
	if err := os.Remove(filepath.Join(tree.root, admission.InterimApprovalPath)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := tree.interimSnapshot(ctx, "v2.5.71"); !errors.Is(err, errNoInterimApproval) {
		t.Fatalf("missing approval did not retain the absent-profile fallback signal: %v", err)
	}
}

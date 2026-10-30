// Command releaseadmission is the release gate: it exits non-zero unless the
// build in the working tree may be published. The release workflow and the
// signed Android targets run it (make release-admission) before anything is
// built for publication. It must run from the repository root.
//
//	releaseadmission [-tag vMAJOR.MINOR.BUILD]
//
// -tag is the release tag being published; the workflow passes the pushed
// tag, and a tag that is not the build's version is refused.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/release/admission"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := execute(ctx, os.Args[1:], os.Stdout, os.Stderr, admission.ManifestPath, gitTree{root: "."})
	stop()
	os.Exit(code)
}

// execute runs the gate and returns the process exit status: 0 admitted,
// 1 refused for any reason.
func execute(ctx context.Context, args []string, stdout, stderr io.Writer, manifestPath string, history admission.ReleaseTree) int {
	err := run(ctx, args, stdout, manifestPath, history)
	if err == nil {
		return 0
	}
	// The exit status is the verdict; a failed write to stderr has no
	// channel left to be reported on and does not change it.
	_, _ = fmt.Fprintf(stderr, "release admission refused: %v\n", err)
	return 1
}

func run(ctx context.Context, args []string, out io.Writer, manifestPath string, history admission.ReleaseTree) error {
	req, err := parseRequest(args)
	if err != nil {
		return err
	}
	verdict, err := admission.Admit(ctx, manifestPath, req, history)
	if reportErr := reportDeferred(out, verdict); reportErr != nil {
		return errors.Join(err, reportErr)
	}
	if errors.Is(err, admission.ErrAdmissionNotGranted) {
		if reportErr := reportRefusal(out, req.Build, verdict, manifestPath); reportErr != nil {
			return errors.Join(err, reportErr)
		}
		return err
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "release admitted: %s, applicable checks complete with any deferrals listed above; owner release decision %d of %s\n",
		req.Build.Release, verdict.ReleaseDecision.Number, verdict.ReleaseDecision.Date)
	return err
}

func reportDeferred(out io.Writer, verdict admission.Admission) error {
	for _, id := range verdict.Deferred {
		if _, err := fmt.Fprintf(out, "  deferred condition: %s (UNVERIFIED; owner decision 2026-10-23, %s)\n", id, admission.TorDeferralDecision); err != nil {
			return err
		}
	}
	return nil
}

func parseRequest(args []string) (admission.Request, error) {
	flags := flag.NewFlagSet("releaseadmission", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	tag := flags.String("tag", "", "release tag being published (vMAJOR.MINOR.BUILD)")
	if err := flags.Parse(args); err != nil {
		return admission.Request{}, err
	}
	if flags.NArg() != 0 {
		return admission.Request{}, fmt.Errorf("unexpected arguments %q", flags.Args())
	}
	req := admission.Request{Build: admission.CurrentBuild()}
	if *tag == "" {
		return req, nil
	}
	version, err := domain.ParseReleaseVersion(*tag)
	if err != nil {
		return admission.Request{}, fmt.Errorf("tag: %w", err)
	}
	req.Tag = &version
	return req, nil
}

func reportRefusal(out io.Writer, build admission.BuildVersions, verdict admission.Admission, manifestPath string) error {
	if _, err := fmt.Fprintf(out, "protocol %d activates the secure session (activation at %d); its release conditions apply.\n",
		build.Protocol, build.Activation); err != nil {
		return err
	}
	for _, id := range verdict.Outstanding {
		if _, err := fmt.Fprintf(out, "  outstanding condition: %s\n", id); err != nil {
			return err
		}
	}
	if verdict.ReleaseDecision == nil {
		if _, err := fmt.Fprintln(out, "  no owner decision to release is recorded"); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(out, "see %s\n", manifestPath)
	return err
}

// gitTree reads the repository at root with read-only git commands. It
// does not trust local git configuration: the global and system config, the
// excludes files, replace refs and index flags could each hide a change from
// a plain `git diff`.
type gitTree struct {
	root string
}

// buildCaches are the only untracked paths a release build may leave in the
// tree: the Makefile keeps GOCACHE and GOMODCACHE inside the repository.
// Everything else untracked or ignored counts as a change, whatever
// .gitignore says — an ignored .go file is still compiled.
var buildCaches = []string{".gocache/", ".gomodcache/"}

// FilesChangedSince lists tracked files that differ between commit and the
// working tree, plus every untracked or ignored file outside the build
// caches: the build is made from the working tree, not from HEAD.
// --no-renames lists both sides of a rename, so moving code under a release
// input path still shows the code path. commit must be an ancestor of HEAD,
// and no index entry may be marked assume-unchanged or skip-worktree.
func (g gitTree) FilesChangedSince(ctx context.Context, commit admission.CommitHash) ([]string, error) {
	if _, err := g.git(ctx, "merge-base", "--is-ancestor", string(commit)+"^{commit}", "HEAD"); err != nil {
		return nil, fmt.Errorf("commit %s is not an ancestor of HEAD: %w", commit, err)
	}
	index, err := g.git(ctx, "ls-files", "-v", "-z", "--full-name", "--", ":/")
	if err != nil {
		return nil, err
	}
	if hidden := hiddenIndexEntries(splitNul(index)); len(hidden) > 0 {
		return nil, fmt.Errorf("index entries hidden from diff (assume-unchanged, skip-worktree or unmerged): %q", hidden)
	}
	tracked, err := g.git(ctx, "diff", "--no-renames", "--no-relative", "--no-ext-diff", "--no-textconv",
		"--name-only", "-z", string(commit)+"^{commit}", "--")
	if err != nil {
		return nil, err
	}
	untracked, err := g.git(ctx, "ls-files", "--others", "-z", "--full-name", "--", ":/")
	if err != nil {
		return nil, err
	}
	return append(splitNul(tracked), unexpectedUntracked(splitNul(untracked))...), nil
}

// IsRegularFile reports whether path under root is a regular file, without
// following a final link.
func (g gitTree) IsRegularFile(_ context.Context, path string) (bool, error) {
	info, err := os.Lstat(filepath.Join(g.root, filepath.FromSlash(path)))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Mode().IsRegular(), nil
}

// hiddenIndexEntries returns the `git ls-files -v` entries whose tag is not
// "H": lowercase tags are assume-unchanged, "S" is skip-worktree, "M" is
// unmerged — each a way for the working tree to differ unseen by diff.
func hiddenIndexEntries(entries []string) []string {
	var hidden []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry, "H ") {
			hidden = append(hidden, entry)
		}
	}
	return hidden
}

func unexpectedUntracked(paths []string) []string {
	var unexpected []string
	for _, path := range paths {
		inCache := slices.ContainsFunc(buildCaches, func(cache string) bool { return strings.HasPrefix(path, cache) })
		if !inCache {
			unexpected = append(unexpected, path)
		}
	}
	return unexpected
}

// splitNul splits -z output; names may contain any byte but NUL.
func splitNul(out []byte) []string {
	var names []string
	for _, name := range strings.Split(string(out), "\x00") {
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// gitIsolatedEnv drops every GIT_* variable that could redirect the
// repository, index or object store, and ignores global and system config.
func gitIsolatedEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") {
			env = append(env, kv)
		}
	}
	return append(env,
		"GIT_NO_REPLACE_OBJECTS=1",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_OPTIONAL_LOCKS=0",
	)
}

func (g gitTree) git(ctx context.Context, args ...string) ([]byte, error) {
	full := append([]string{
		"-c", "core.excludesFile=",
		"-c", "core.fsmonitor=false",
		"-c", "core.untrackedCache=false",
		"-c", "diff.relative=false",
	}, args...)
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.Dir = g.root
	cmd.Env = gitIsolatedEnv()
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

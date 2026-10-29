package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/release/admission"
)

type interimReportHistory struct {
	admission.ReleaseTree
	digest string
	raw    []byte
	err    error
	calls  int
}

func (h *interimReportHistory) interimSnapshot(context.Context, string) (string, []byte, admission.CommitHash, error) {
	h.calls++
	return h.digest, h.raw, admission.CommitHash(strings.Repeat("a", 40)), h.err
}

func interimReportFixture(t *testing.T) *interimReportHistory {
	t.Helper()
	conditions := committedConditions(t)
	a := admission.InterimApproval{
		Schema: 1, Version: "2.5.71", SourceSHA256: strings.Repeat("b", 64),
		Decision:              admission.InterimDecision{Date: "2026-10-30", Authority: "Fixture owner reviewed the interim publication scope"},
		OutstandingConditions: []admission.ConditionID{}, DeferredConditions: []admission.ConditionID{},
	}
	for _, c := range conditions {
		switch c.Status {
		case admission.ConditionDeferred:
			a.DeferredConditions = append(a.DeferredConditions, c.ID)
		case admission.ConditionDone:
		default:
			a.OutstandingConditions = append(a.OutstandingConditions, c.ID)
		}
	}
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	return &interimReportHistory{ReleaseTree: unusedHistory(t), digest: a.SourceSHA256, raw: raw}
}

func TestInterimReportKeepsS6Outstanding(t *testing.T) {
	history := interimReportFixture(t)
	var out, stderr bytes.Buffer
	if code := execute(context.Background(), []string{"-tag", "v2.5.71"}, &out, &stderr, committedManifest(), history); code != 0 {
		t.Fatalf("exit %d: %s", code, &stderr)
	}
	for _, text := range []string{"INTERIM release admitted: 2.5.71", "S6 admitted: false", strings.Repeat("a", 40), history.digest, "unverified S6 condition: crypto_review", "UNVERIFIED"} {
		if !strings.Contains(out.String(), text) {
			t.Errorf("missing %q in %s", text, &out)
		}
	}
	if strings.Contains(out.String(), "applicable checks complete") || strings.Contains(out.String(), "owner release decision") {
		t.Fatalf("ordinary S6 success claimed: %s", &out)
	}
}

func TestInterimReportRefusesInvalidInputs(t *testing.T) {
	for _, name := range []string{"snapshot", "source", "approval", "manifest", "output"} {
		t.Run(name, func(t *testing.T) {
			history := interimReportFixture(t)
			manifest := committedManifest()
			stdout := io.Discard
			switch name {
			case "snapshot":
				history.err = os.ErrNotExist // Not the absence-only sentinel.
			case "source":
				history.digest = strings.Repeat("c", 64)
			case "approval":
				history.raw = []byte(`{"schema":1,"schema":1}`)
			case "manifest":
				manifest = t.TempDir() + "/absent.json"
			case "output":
				stdout = interimFailWriter{}
			}
			if code := execute(context.Background(), []string{"-tag", "v2.5.71"}, stdout, io.Discard, manifest, history); code != 1 {
				t.Fatal("invalid interim input admitted")
			}
		})
	}
}

type interimFailWriter struct{}

func (interimFailWriter) Write([]byte) (int, error) { return 0, errors.New("fixture output failure") }

func TestInterimDoesNotApplyToTaglessOrFutureRelease(t *testing.T) {
	for _, args := range [][]string{nil, {"-tag", "v2.5.72"}} {
		history := interimReportFixture(t)
		if code := execute(context.Background(), args, io.Discard, io.Discard, committedManifest(), history); code != 1 {
			t.Fatal("closed normal gate unexpectedly passed")
		}
		if history.calls != 0 {
			t.Fatal("interim policy consulted for noneligible release")
		}
	}
}

func TestAbsentInterimApprovalUsesNormalS6Gate(t *testing.T) {
	history := interimReportFixture(t)
	history.err = errNoInterimApproval
	var out bytes.Buffer
	if code := execute(context.Background(), []string{"-tag", "v2.5.71"}, &out, io.Discard, committedManifest(), history); code != 1 ||
		!strings.Contains(out.String(), "outstanding condition: crypto_review") {
		t.Fatalf("missing approval did not retain normal gate: code %d: %s", code, &out)
	}
}

func TestInterimCLIAdmitsOnlyTheExactCommittedFixture(t *testing.T) {
	ctx, tree, _, _, _ := interimSnapshotFixture(t)
	rawManifest, err := os.ReadFile(committedManifest())
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(tree.root, admission.ManifestPath)
	if err := os.WriteFile(manifestPath, rawManifest, 0o600); err != nil {
		t.Fatal(err)
	}
	interimFixtureGit(t, ctx, tree, "add", "--", admission.ManifestPath)
	interimFixtureGit(t, ctx, tree, "commit", "--quiet", "-m", "fixture manifest")
	interimFixtureGit(t, ctx, tree, "tag", "-f", "v2.5.71")
	digest, _, _, err := tree.interimSnapshot(ctx, "v2.5.71")
	if err != nil {
		t.Fatal(err)
	}
	history := interimReportFixture(t)
	var approval admission.InterimApproval
	if err := json.Unmarshal(history.raw, &approval); err != nil {
		t.Fatal(err)
	}
	approval.SourceSHA256 = digest
	raw, err := json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree.root, admission.InterimApprovalPath), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	interimFixtureGit(t, ctx, tree, "add", "--", admission.InterimApprovalPath)
	interimFixtureGit(t, ctx, tree, "commit", "--quiet", "-m", "fixture exact approval")
	interimFixtureGit(t, ctx, tree, "tag", "-f", "v2.5.71")
	var out, stderr bytes.Buffer
	if code := execute(ctx, []string{"-tag", "v2.5.71"}, &out, &stderr, manifestPath, tree); code != 0 ||
		!strings.Contains(out.String(), digest) || !strings.Contains(out.String(), "S6 admitted: false") {
		t.Fatalf("exact real Git fixture refused: exit %d; %s; %s", code, &out, &stderr)
	}
	if err := os.WriteFile(filepath.Join(tree.root, "fixture.txt"), []byte("changed after approval\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := execute(ctx, []string{"-tag", "v2.5.71"}, io.Discard, io.Discard, manifestPath, tree); code != 1 {
		t.Fatal("modified approved Git fixture admitted")
	}
}

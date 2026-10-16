package admission

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// The gate only protects a release if nothing that publishes can run
// without it. These tests read the workflows and the Makefile the way a
// reviewer would and fail when a publishing path loses the gate — the gate
// itself cannot notice that it was never called.

const (
	admissionJobName    = "admission"
	admissionMakeTarget = "release-admission"
	releaseWorkflowFile = "release.yml"
	// admissionCommand is compared whole, not as a substring: `|| true`,
	// `; exit 0` or a wrapper would keep the substring and lose the verdict.
	// `:?` fails the step when the tag name is empty instead of running the
	// gate without the tag check.
	admissionCommand = `make release-admission RELEASE_TAG="${GITHUB_REF_NAME:?}"`
)

// admissionSetupActions are the only actions the admission job may use
// besides running the gate; anything else could publish or tamper with the
// tree before the verdict.
var admissionSetupActions = []string{"actions/checkout", "actions/setup-go"}

// publishingActions are the actions that put build output where people
// outside the run can fetch it: a GitHub release, a workflow artifact, a
// container registry, Pages.
var publishingActions = []string{
	"actions/upload-artifact",
	"actions/upload-pages-artifact",
	"actions/deploy-pages",
	"softprops/action-gh-release",
	"ncipollo/release-action",
	"docker/build-push-action",
	"docker/login-action",
}

// publishingCommands are shell commands with the same effect.
var publishingCommands = []string{"gh release", "docker push", "--push"}

// writeScopes are the token scopes that let a job publish without any of
// the actions above.
var writeScopes = []string{"contents", "packages", "deployments", "pages", "id-token", "attestations"}

type workflowStep struct {
	Uses            string         `yaml:"uses"`
	Run             string         `yaml:"run"`
	If              yaml.Node      `yaml:"if"`
	With            map[string]any `yaml:"with"`
	ContinueOnError yaml.Node      `yaml:"continue-on-error"`
}

type workflowJob struct {
	Needs           yaml.Node      `yaml:"needs"`
	If              yaml.Node      `yaml:"if"`
	ContinueOnError yaml.Node      `yaml:"continue-on-error"`
	Permissions     yaml.Node      `yaml:"permissions"`
	Uses            string         `yaml:"uses"`
	Steps           []workflowStep `yaml:"steps"`
}

type workflow struct {
	Permissions yaml.Node              `yaml:"permissions"`
	Jobs        map[string]workflowJob `yaml:"jobs"`
}

func workflowsDir() string {
	return filepath.Join(repoRoot, ".github", "workflows")
}

func readWorkflowText(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(workflowsDir(), name))
	if err != nil {
		t.Fatalf("read workflow %s: %v", name, err)
	}
	return string(raw)
}

func parseWorkflow(text string) (workflow, error) {
	var w workflow
	if err := yaml.Unmarshal([]byte(text), &w); err != nil {
		return workflow{}, err
	}
	if len(w.Jobs) == 0 {
		return workflow{}, fmt.Errorf("no jobs")
	}
	return w, nil
}

func needsOf(job workflowJob) ([]string, error) {
	switch job.Needs.Kind {
	case 0:
		return nil, nil
	case yaml.ScalarNode:
		return []string{job.Needs.Value}, nil
	case yaml.SequenceNode:
		var needs []string
		err := job.Needs.Decode(&needs)
		return needs, err
	default:
		return nil, fmt.Errorf("needs has an unexpected YAML shape (kind %d)", job.Needs.Kind)
	}
}

// mayWrite reports whether the token a job runs with may write to a scope a
// release could go through. No permissions block anywhere means the
// repository default, which can be write — so it counts as write.
func mayWrite(w workflow, job workflowJob) (bool, error) {
	permissions := job.Permissions
	if permissions.Kind == 0 {
		permissions = w.Permissions
	}
	switch permissions.Kind {
	case 0:
		return true, nil
	case yaml.ScalarNode:
		return permissions.Value != "read-all" && permissions.Value != "{}", nil
	case yaml.MappingNode:
		var scopes map[string]string
		if err := permissions.Decode(&scopes); err != nil {
			return false, err
		}
		for _, scope := range writeScopes {
			if scopes[scope] == "write" {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("permissions has an unexpected YAML shape (kind %d)", permissions.Kind)
	}
}

func usesAction(step workflowStep, action string) bool {
	return step.Uses == action || strings.HasPrefix(step.Uses, action+"@")
}

func stepPublishes(step workflowStep) bool {
	for _, action := range publishingActions {
		if usesAction(step, action) {
			return true
		}
	}
	for _, command := range publishingCommands {
		if strings.Contains(step.Run, command) {
			return true
		}
	}
	return false
}

func publishes(w workflow, job workflowJob) (bool, error) {
	// A reusable workflow call runs steps this file cannot see; treat it as
	// publishing rather than assume it does not.
	if job.Uses != "" {
		return true, nil
	}
	writes, err := mayWrite(w, job)
	if err != nil || writes {
		return writes, err
	}
	return slices.ContainsFunc(job.Steps, stepPublishes), nil
}

// gatingProblems lists why job is not held back by a refused admission.
func gatingProblems(file, name string, w workflow, job workflowJob) []string {
	if _, ok := w.Jobs[admissionJobName]; !ok {
		return []string{fmt.Sprintf("%s: job %q publishes, but the workflow has no %q job", file, name, admissionJobName)}
	}
	var problems []string
	needs, err := needsOf(job)
	if err != nil {
		problems = append(problems, fmt.Sprintf("%s: job %q: %v", file, name, err))
	}
	if !slices.Contains(needs, admissionJobName) {
		problems = append(problems, fmt.Sprintf("%s: job %q does not list %q in needs directly; a link through "+
			"another job is lost the moment that job is edited", file, name, admissionJobName))
	}
	// Any job condition can be written to hold after a failed dependency
	// (`always()`, `success() || true`, `!cancelled()`); none is allowed.
	if job.If.Kind != 0 {
		problems = append(problems, fmt.Sprintf("%s: job %q has `if: %s`; a gated job takes no condition", file, name, job.If.Value))
	}
	return problems
}

// admissionJobProblems lists why the admission job could pass without
// running the gate, or do something before it.
func admissionJobProblems(file string, w workflow, job workflowJob) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf("%s: %q job: ", file, admissionJobName)+fmt.Sprintf(format, args...))
	}
	if job.If.Kind != 0 {
		add("has `if: %s`", job.If.Value)
	}
	if job.ContinueOnError.Kind != 0 {
		add("sets continue-on-error")
	}
	if job.Uses != "" {
		add("calls a reusable workflow")
	}
	if writes, err := mayWrite(w, job); err != nil || writes {
		add("its token may write (err %v); the gate needs read access only", err)
	}
	if len(job.Steps) == 0 {
		add("has no steps")
		return problems
	}
	for i, step := range job.Steps {
		if step.If.Kind != 0 {
			add("step %d has `if: %s`", i, step.If.Value)
		}
		if step.ContinueOnError.Kind != 0 {
			add("step %d sets continue-on-error", i)
		}
		last := i == len(job.Steps)-1
		switch {
		case last && step.Run != "" && strings.TrimSpace(step.Run) == admissionCommand && step.Uses == "":
		case last:
			add("last step is not exactly `%s`", admissionCommand)
		case step.Run != "":
			add("step %d runs a command before the gate", i)
		case !slices.ContainsFunc(admissionSetupActions, func(a string) bool { return usesAction(step, a) }):
			add("step %d uses %q; only %v may precede the gate", i, step.Uses, admissionSetupActions)
		}
		if usesAction(step, "actions/checkout") && fmt.Sprint(step.With["fetch-depth"]) != "0" {
			add("checkout without fetch-depth: 0 cannot compare the tag with the final candidate commit")
		}
	}
	return problems
}

// Key allow-lists for a workflow that carries the gate. Anything not listed
// can change what the gate runs or where: env (GOFLAGS, MAKEFLAGS, GIT_DIR),
// defaults.run.shell or a step `shell: "true {0}"` that never runs the
// command, working-directory, a container or another runner, a checkout of
// another ref, repository or path.
var (
	gatedWorkflowKeys  = []string{"name", "run-name", "on", "permissions", "concurrency", "jobs"}
	admissionJobKeys   = []string{"name", "runs-on", "steps", "timeout-minutes"}
	admissionRunner    = "ubuntu-latest"
	maxAdmissionMinute = 60
)

// admissionStep is one allowed step of the admission job, in order.
type admissionStep struct {
	describe string
	keys     []string
	check    func(step map[string]*yaml.Node) []string
}

var admissionSteps = []admissionStep{
	{
		describe: "actions/checkout with only fetch-depth: 0",
		keys:     []string{"name", "uses", "with"},
		check: func(step map[string]*yaml.Node) []string {
			return append(scalarPrefix(step, "uses", "actions/checkout@"),
				exactWith(step, map[string]string{"fetch-depth": "0"}, nil)...)
		},
	},
	{
		describe: "actions/setup-go with go-version-file: go.mod",
		keys:     []string{"name", "uses", "with"},
		check: func(step map[string]*yaml.Node) []string {
			return append(scalarPrefix(step, "uses", "actions/setup-go@"),
				exactWith(step, map[string]string{"go-version-file": "go.mod"}, []string{"cache"})...)
		},
	},
	{
		describe: "run: " + admissionCommand,
		keys:     []string{"name", "run"},
		check: func(step map[string]*yaml.Node) []string {
			if run := step["run"]; run == nil || strings.TrimSpace(run.Value) != admissionCommand {
				return []string{fmt.Sprintf("the gate step is not exactly `%s`", admissionCommand)}
			}
			return nil
		},
	},
}

func mappingOf(node *yaml.Node) (map[string]*yaml.Node, bool) {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, false
	}
	m := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		m[node.Content[i].Value] = node.Content[i+1]
	}
	return m, true
}

func unexpectedKeys(where string, m map[string]*yaml.Node, allowed []string) []string {
	var problems []string
	for key := range m {
		if !slices.Contains(allowed, key) {
			problems = append(problems, fmt.Sprintf("%s: key %q is not allowed (allowed %v)", where, key, allowed))
		}
	}
	slices.Sort(problems)
	return problems
}

func scalarPrefix(step map[string]*yaml.Node, key, prefix string) []string {
	if n := step[key]; n == nil || n.Kind != yaml.ScalarNode || !strings.HasPrefix(n.Value, prefix) {
		return []string{fmt.Sprintf("%s does not start with %q", key, prefix)}
	}
	return nil
}

// exactWith requires the step's `with` to hold exactly the required values,
// plus optional boolean keys.
func exactWith(step map[string]*yaml.Node, required map[string]string, optionalBools []string) []string {
	with, ok := mappingOf(step["with"])
	if !ok {
		return []string{"with is missing or not a mapping"}
	}
	var problems []string
	for key, want := range required {
		if n := with[key]; n == nil || n.Kind != yaml.ScalarNode || n.Value != want {
			problems = append(problems, fmt.Sprintf("with.%s is not %q", key, want))
		}
	}
	for key, n := range with {
		if _, isRequired := required[key]; isRequired {
			continue
		}
		if !slices.Contains(optionalBools, key) {
			problems = append(problems, fmt.Sprintf("with.%s is not allowed", key))
			continue
		}
		if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" {
			problems = append(problems, fmt.Sprintf("with.%s is not a literal boolean", key))
		}
	}
	slices.Sort(problems)
	return problems
}

// admissionKeyProblems checks the workflow and the admission job against the
// allow-lists, key by key.
func admissionKeyProblems(file, text string) []string {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil || len(doc.Content) == 0 {
		return []string{fmt.Sprintf("%s: not a YAML document", file)}
	}
	root, ok := mappingOf(doc.Content[0])
	if !ok {
		return []string{fmt.Sprintf("%s: top level is not a mapping", file)}
	}
	problems := unexpectedKeys(file, root, gatedWorkflowKeys)
	jobs, _ := mappingOf(root["jobs"])
	job, ok := mappingOf(jobs[admissionJobName])
	if !ok {
		return append(problems, fmt.Sprintf("%s: %q job is not a mapping", file, admissionJobName))
	}
	where := fmt.Sprintf("%s: %q job", file, admissionJobName)
	problems = append(problems, unexpectedKeys(where, job, admissionJobKeys)...)
	if n := job["runs-on"]; n == nil || n.Kind != yaml.ScalarNode || n.Value != admissionRunner {
		problems = append(problems, fmt.Sprintf("%s: runs-on is not %q", where, admissionRunner))
	}
	if n := job["timeout-minutes"]; n != nil {
		var minutes int
		if n.Kind != yaml.ScalarNode || n.Tag != "!!int" || n.Decode(&minutes) != nil || minutes < 1 || minutes > maxAdmissionMinute {
			problems = append(problems, fmt.Sprintf("%s: timeout-minutes is not a literal integer in 1..%d", where, maxAdmissionMinute))
		}
	}
	steps := job["steps"]
	if steps == nil || steps.Kind != yaml.SequenceNode || len(steps.Content) != len(admissionSteps) {
		return append(problems, fmt.Sprintf("%s: steps are not exactly %d", where, len(admissionSteps)))
	}
	for i, want := range admissionSteps {
		step, ok := mappingOf(steps.Content[i])
		stepWhere := fmt.Sprintf("%s step %d (%s)", where, i, want.describe)
		if !ok {
			problems = append(problems, stepWhere+": not a mapping")
			continue
		}
		problems = append(problems, unexpectedKeys(stepWhere, step, want.keys)...)
		for _, problem := range want.check(step) {
			problems = append(problems, stepWhere+": "+problem)
		}
	}
	return problems
}

// releaseNotesBodyPath is what the release job publishes as the release
// body: the notes file the gate required for this version.
const releaseNotesBodyPath = "docs/release-notes/RELEASE-${{ github.ref_name }}.md"

// releaseBodyProblems requires every GitHub release step to publish the
// committed notes file, never generated notes.
func releaseBodyProblems(file string, w workflow) []string {
	var problems []string
	found := false
	for name, job := range w.Jobs {
		for _, step := range job.Steps {
			if !usesAction(step, "softprops/action-gh-release") {
				continue
			}
			found = true
			if fmt.Sprint(step.With["body_path"]) != releaseNotesBodyPath {
				problems = append(problems, fmt.Sprintf("%s: job %q publishes body_path %v, want %q",
					file, name, step.With["body_path"], releaseNotesBodyPath))
			}
			if notes, ok := step.With["generate_release_notes"]; ok && fmt.Sprint(notes) != "false" {
				problems = append(problems, fmt.Sprintf("%s: job %q may publish generated notes", file, name))
			}
			if _, ok := step.With["body"]; ok {
				problems = append(problems, fmt.Sprintf("%s: job %q sets an inline body", file, name))
			}
		}
	}
	if !found {
		problems = append(problems, fmt.Sprintf("%s: no GitHub release step found", file))
	}
	return problems
}

func workflowProblems(file, text string, everyJobGated bool) []string {
	w, err := parseWorkflow(text)
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", file, err)}
	}
	var problems []string
	admissionJob, hasAdmission := w.Jobs[admissionJobName]
	if everyJobGated && !hasAdmission {
		problems = append(problems, fmt.Sprintf("%s has no %q job", file, admissionJobName))
	}
	if hasAdmission {
		problems = append(problems, admissionJobProblems(file, w, admissionJob)...)
		problems = append(problems, admissionKeyProblems(file, text)...)
	}
	if everyJobGated {
		problems = append(problems, releaseBodyProblems(file, w)...)
	}
	for name, job := range w.Jobs {
		if name == admissionJobName {
			continue
		}
		gate := everyJobGated
		if !gate {
			p, err := publishes(w, job)
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s: job %q: %v", file, name, err))
			}
			gate = p || err != nil
		}
		if gate {
			problems = append(problems, gatingProblems(file, name, w, job)...)
		}
	}
	return problems
}

// TestReleaseWorkflowRunsNothingBeforeAdmission: every job of the release
// workflow needs the admission job directly, so a refusal stops the build
// and the upload as well as the release itself.
func TestReleaseWorkflowRunsNothingBeforeAdmission(t *testing.T) {
	for _, problem := range workflowProblems(releaseWorkflowFile, readWorkflowText(t, releaseWorkflowFile), true) {
		t.Error(problem)
	}
}

// TestEveryPublishingJobIsGated covers every workflow, including ones added
// later: a nightly or a push-to-dev publisher must carry the same gate.
func TestEveryPublishingJobIsGated(t *testing.T) {
	entries, err := os.ReadDir(workflowsDir())
	if err != nil {
		t.Fatalf("read workflows directory: %v", err)
	}
	checked := 0
	for _, entry := range entries {
		ext := filepath.Ext(entry.Name())
		if entry.IsDir() || (ext != ".yml" && ext != ".yaml") {
			continue
		}
		everyJobGated := entry.Name() == releaseWorkflowFile
		for _, problem := range workflowProblems(entry.Name(), readWorkflowText(t, entry.Name()), everyJobGated) {
			t.Error(problem)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no workflow files found; the check would pass vacuously")
	}
}

// TestWiringChecksCatchBypasses applies each known way around the gate to
// the real release workflow; the checks must report every one of them.
func TestWiringChecksCatchBypasses(t *testing.T) {
	original := readWorkflowText(t, releaseWorkflowFile)
	const gateRun = "        run: " + admissionCommand + "\n"
	cases := []struct {
		name, old, replacement string
	}{
		{
			name:        "gate step disabled by if",
			old:         "      - name: Check release admission\n",
			replacement: "      - name: Check release admission\n        if: false\n",
		},
		{
			name:        "gate failure swallowed by the shell",
			old:         gateRun,
			replacement: "        run: " + admissionCommand + " || true\n",
		},
		{
			name:        "gate step continue-on-error",
			old:         gateRun,
			replacement: gateRun + "        continue-on-error: true\n",
		},
		{
			name: "publishing step in the admission job before the gate",
			old:  "      - name: Check release admission\n",
			replacement: "      - name: Publish early\n        uses: softprops/action-gh-release@v2\n\n" +
				"      - name: Check release admission\n",
		},
		{
			name:        "write token in the admission job",
			old:         "    name: Release admission\n",
			replacement: "    name: Release admission\n    permissions:\n      contents: write\n",
		},
		{
			name:        "checkout without full history",
			old:         "          fetch-depth: 0\n",
			replacement: "          fetch-depth: 1\n",
		},
		{
			name:        "release job runs after a failed gate",
			old:         "    name: Create Release\n",
			replacement: "    name: Create Release\n    if: success() || true\n",
		},
		{
			name:        "release job drops the direct need",
			old:         "    needs:\n      - admission\n      - build-linux\n",
			replacement: "    needs:\n      - build-linux\n",
		},
		{
			name:        "default token everywhere",
			old:         "permissions:\n  contents: read\n",
			replacement: "",
		},
		{
			name:        "gate step shell that never runs the command",
			old:         gateRun,
			replacement: gateRun + "        shell: \"true {0}\"\n",
		},
		{
			name:        "gate step in another directory",
			old:         gateRun,
			replacement: gateRun + "        working-directory: packaging\n",
		},
		{
			name:        "gate step environment",
			old:         gateRun,
			replacement: gateRun + "        env:\n          GOFLAGS: -mod=mod\n",
		},
		{
			name:        "admission job environment",
			old:         "    name: Release admission\n",
			replacement: "    name: Release admission\n    env:\n      MAKEFLAGS: -n\n",
		},
		{
			name:        "admission job default shell",
			old:         "    name: Release admission\n",
			replacement: "    name: Release admission\n    defaults:\n      run:\n        shell: \"true {0}\"\n",
		},
		{
			name:        "workflow environment",
			old:         "permissions:\n  contents: read\n",
			replacement: "permissions:\n  contents: read\n\nenv:\n  GIT_DIR: /tmp/elsewhere\n",
		},
		{
			name:        "workflow default shell",
			old:         "permissions:\n  contents: read\n",
			replacement: "permissions:\n  contents: read\n\ndefaults:\n  run:\n    shell: \"true {0}\"\n",
		},
		{
			name:        "admission timeout as an expression",
			old:         "    name: Release admission\n",
			replacement: "    name: Release admission\n    timeout-minutes: ${{ 0 }}\n",
		},
		{
			name:        "admission job on another runner",
			old:         "    name: Release admission\n    runs-on: ubuntu-latest\n",
			replacement: "    name: Release admission\n    runs-on: self-hosted\n",
		},
		{
			name:        "admission job in a container",
			old:         "    name: Release admission\n",
			replacement: "    name: Release admission\n    container: golang:1.20\n",
		},
		{
			name:        "checkout of another ref",
			old:         "          fetch-depth: 0\n",
			replacement: "          fetch-depth: 0\n          ref: refs/heads/master\n",
		},
		{
			name:        "checkout of another repository",
			old:         "          fetch-depth: 0\n",
			replacement: "          fetch-depth: 0\n          repository: someone/fork\n",
		},
		{
			name:        "checkout into another path",
			old:         "          fetch-depth: 0\n",
			replacement: "          fetch-depth: 0\n          path: other\n",
		},
		{
			name:        "setup-go with another Go",
			old:         "          go-version-file: go.mod\n          cache: true\n\n      - name: Check release admission\n",
			replacement: "          go-version-file: go.mod\n          go-version: \"1.20\"\n          cache: true\n\n      - name: Check release admission\n",
		},
		{
			name:        "release published with generated notes",
			old:         "          body_path: " + releaseNotesBodyPath + "\n",
			replacement: "          body_path: " + releaseNotesBodyPath + "\n          generate_release_notes: true\n",
		},
		{
			name:        "release published from another notes file",
			old:         "          body_path: " + releaseNotesBodyPath + "\n",
			replacement: "          body_path: README.md\n",
		},
	}
	if problems := workflowProblems(releaseWorkflowFile, original, true); len(problems) != 0 {
		t.Fatalf("the unmodified workflow already has problems: %v", problems)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(original, tc.old) {
				t.Fatalf("%s no longer contains %q; update the mutation", releaseWorkflowFile, tc.old)
			}
			mutated := strings.Replace(original, tc.old, tc.replacement, 1)
			if problems := workflowProblems(releaseWorkflowFile, mutated, true); len(problems) == 0 {
				t.Fatal("the wiring checks accept the mutated workflow")
			}
		})
	}
}

// TestPublishingDetectionSeesTheReleaseJob keeps the detector honest: if it
// stopped recognising the jobs that actually publish, the sweep over the
// other workflows would pass over everything.
func TestPublishingDetectionSeesTheReleaseJob(t *testing.T) {
	w, err := parseWorkflow(readWorkflowText(t, releaseWorkflowFile))
	if err != nil {
		t.Fatalf("parse %s: %v", releaseWorkflowFile, err)
	}
	for _, name := range []string{"build-linux", "build-platforms", "release"} {
		p, err := publishes(w, w.Jobs[name])
		if err != nil || !p {
			t.Errorf("job %q not detected as publishing (err %v)", name, err)
		}
	}
	silent := workflow{Jobs: map[string]workflowJob{"lint": {}}}
	if writes, _ := mayWrite(silent, silent.Jobs["lint"]); !writes {
		t.Error("a job with no permissions block anywhere runs with the default token and must count as writing")
	}
}

// makeRulePrerequisites returns the prerequisites of a Makefile rule.
func makeRulePrerequisites(t *testing.T, target string) ([]string, bool) {
	t.Helper()
	f, err := os.Open(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("open Makefile: %v", err)
	}
	defer func() { _ = f.Close() }() // read-only; a close error loses nothing

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		head, prerequisites, ok := strings.Cut(scanner.Text(), ":")
		if !ok || head != target || strings.HasPrefix(prerequisites, "=") {
			continue
		}
		return strings.Fields(prerequisites), true
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	return nil, false
}

// TestSignedAndroidReleaseTargetsRequireAdmission: the release-signed APK
// and bundle are store artifacts, built outside any workflow.
func TestSignedAndroidReleaseTargetsRequireAdmission(t *testing.T) {
	if _, ok := makeRulePrerequisites(t, admissionMakeTarget); !ok {
		t.Fatalf("Makefile has no %q target", admissionMakeTarget)
	}
	for _, target := range []string{"build-android-release", "build-android-aab"} {
		prerequisites, ok := makeRulePrerequisites(t, target)
		if !ok {
			t.Errorf("Makefile has no %q target", target)
			continue
		}
		if !slices.Contains(prerequisites, admissionMakeTarget) {
			t.Errorf("Makefile target %q does not depend on %q (prerequisites %v)", target, admissionMakeTarget, prerequisites)
		}
	}
}

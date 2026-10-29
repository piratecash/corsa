package admission

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const candidateWorkflowFile = "candidate-build.yml"

// A candidate run supplies build evidence, never externally retrievable binaries.
// Every publishing job continues to be subject to the original gate tests.
func candidateBuildProblems(text string) []string {
	problems := workflowProblems(candidateWorkflowFile, text, false)
	w, err := parseWorkflow(text)
	if err != nil {
		return append(problems, err.Error())
	}
	for _, job := range w.Jobs {
		if publishes, err := publishes(w, job); err != nil || publishes {
			problems = append(problems, "candidate validation must not publish")
		}
		for _, step := range job.Steps {
			if step.Uses != "" && !slices.ContainsFunc([]string{"actions/checkout", "actions/setup-go", "actions/setup-python"}, func(action string) bool { return usesAction(step, action) }) {
				problems = append(problems, "unexpected candidate action")
			}
		}
	}
	buildCalls, reportCalls := 0, 0
	for _, step := range w.Jobs["build"].Steps {
		switch strings.TrimSpace(step.Run) {
		case "${{ matrix.commands }}":
			buildCalls++
		case `python scripts/report-candidate-artifacts.py --platform "${{ matrix.platform }}" --source-sha "$GITHUB_SHA"`:
			reportCalls++
		}
	}
	if buildCalls != 1 || reportCalls != 1 {
		problems = append(problems, "candidate must execute its build matrix and artifact reporter exactly once")
	}
	var doc struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Jobs        map[string]struct {
			Timeout  int `yaml:"timeout-minutes"`
			Strategy struct {
				Matrix struct {
					Include []struct {
						Platform string `yaml:"platform"`
						Runner   string `yaml:"runner"`
						Commands string `yaml:"commands"`
					} `yaml:"include"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		return append(problems, err.Error())
	}
	if len(doc.Permissions) != 1 || doc.Permissions["contents"] != "read" || strings.Contains(text, "secrets.") {
		problems = append(problems, "candidate validation must use read-only access without secrets")
	}
	push, hasPush := doc.On["push"]
	_, manual := doc.On["workflow_dispatch"]
	var trigger map[string][]string
	if err := push.Decode(&trigger); err != nil || !hasPush || !manual || len(doc.On) != 2 || len(trigger) != 1 || !slices.Equal(trigger["branches"], []string{"dev"}) {
		problems = append(problems, "candidate validation triggers must be manual and dev pushes only")
	}
	build, exists := doc.Jobs["build"]
	if !exists || len(doc.Jobs) != 1 || build.Timeout <= 0 || build.Timeout > 30 {
		problems = append(problems, "unexpected candidate job or timeout")
	}
	want := map[string][3]string{
		"linux-amd64":   {"ubuntu-latest", "build-node-linux-amd64", "build-desktop-linux-amd64"},
		"darwin-arm64":  {"macos-latest", "build-node-macos-arm64", "build-desktop-macos-arm64"},
		"darwin-amd64":  {"macos-15-intel", "build-node-macos-amd64", "build-desktop-macos-amd64"},
		"windows-amd64": {"windows-latest", "build-node-windows-amd64", "build-desktop-windows-amd64"},
		"windows-arm64": {"windows-latest", "build-node-windows-arm64", "build-desktop-windows-arm64"},
	}
	seen := map[string]bool{}
	for _, row := range build.Strategy.Matrix.Include {
		expected, ok := want[row.Platform]
		if !ok || seen[row.Platform] || row.Runner != expected[0] || strings.TrimSpace(row.Commands) != fmt.Sprintf("make %s\nmake %s", expected[1], expected[2]) {
			problems = append(problems, "candidate platform coverage or build command changed")
		}
		seen[row.Platform] = true
	}
	if len(seen) != len(want) {
		problems = append(problems, "candidate validation must build all five platforms")
	}
	return problems
}

func TestCandidateBuildChecksEveryReleasePlatformWithoutPublishing(t *testing.T) {
	for _, problem := range candidateBuildProblems(readWorkflowText(t, candidateWorkflowFile)) {
		t.Error(problem)
	}
}

func TestReleaseCIKeepsTheApprovedTestScopeAndCGOChecks(t *testing.T) {
	w, err := parseWorkflow(readWorkflowText(t, "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	job, ok := w.Jobs["test"]
	if !ok {
		t.Fatal("release test job is missing")
	}
	selected, cgo, controls := 0, 0, 0
	for _, step := range job.Steps {
		switch strings.TrimSpace(step.Run) {
		case "make test-release":
			selected++
		case "make test-cgo":
			cgo++
		case "python scripts/test-test-release.py\npython scripts/test-report-candidate-artifacts.py":
			controls++
		}
	}
	if selected != 1 || cgo != 1 || controls != 1 {
		t.Fatal("CI must run the approved package selection, tagged CGO suite and preflight refusal controls")
	}
}

func TestCandidateBuildRefusesPublishingAndCoverageRegressions(t *testing.T) {
	original := readWorkflowText(t, candidateWorkflowFile)
	for _, mutation := range []struct{ name, old, replacement string }{
		{"write-token", "contents: read", "contents: write"},
		{"tag-trigger", "branches: [dev]", "tags: ['v*']"},
		{"duplicate-platform", "platform: windows-arm64", "platform: windows-amd64"},
		{"wrong-build", "make build-desktop-windows-arm64", "make build-desktop-windows-amd64"},
		{"missing-daemon", "make build-node-linux-amd64", "echo skipped"},
		{"skipped-build-step", "run: ${{ matrix.commands }}", "run: echo skipped"},
		{"skipped-report-step", "run: python scripts/report-candidate-artifacts.py", "run: echo scripts/report-candidate-artifacts.py"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			if strings.Count(original, mutation.old) != 1 {
				t.Fatal("update mutation to match the current workflow")
			}
			if len(candidateBuildProblems(strings.Replace(original, mutation.old, mutation.replacement, 1))) == 0 {
				t.Fatal("candidate policy regression was accepted")
			}
		})
	}
	for _, action := range []string{"actions/upload-artifact@v4", "actions/download-artifact@v4", "softprops/action-gh-release@v2"} {
		t.Run(action, func(t *testing.T) {
			if len(candidateBuildProblems(original+"\n      - uses: "+action+"\n")) == 0 {
				t.Fatal("candidate publication or promotion action was accepted")
			}
		})
	}
}

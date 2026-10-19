package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

func loadStandFunctionalLimits() loadStandExecutionLimits {
	return loadStandExecutionLimits{RouteReady: 35 * time.Second, Readiness: 8 * time.Second, Action: 4 * time.Second, Cleanup: 12 * time.Second, ReceiptDrain: 8 * time.Second, AllowedLateness: 3 * time.Second, MaxActions: 256, MaxSamples: 128, MaxTraceBytes: 4 << 20}
}
func loadStandFunctionalExecutor(t *testing.T, opts loadStandPlanOpts, binary string) *loadStandExecutor {
	t.Helper()
	p, err := buildLoadStandPlan(opts)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	ports := newLoadStandPortRegistry(reserveLoopbackAddress)
	clock := runjournal.SystemClock{}
	members := map[loadStandNodeID]loadstand.Member{}
	for _, n := range p.Nodes() {
		if n.role != loadStandRoleFull {
			continue
		}
		fixture, err := newLoadStandNode(n.id, n.role, filepath.Join(root, string(n.id)), ports, loadStandNodeOpts{Hooks: loadStandLoopbackHooks()})
		if err != nil {
			t.Fatal(err)
		}
		members[n.id] = &loadStandFullMember{node: fixture, clock: clock}
	}
	for _, n := range p.Nodes() {
		bootstrap := make([]domain.PeerAddress, 0, len(n.dialTargets))
		for _, target := range n.dialTargets {
			endpoint, ok := members[target].Endpoint()
			if !ok {
				t.Fatal("plan dials non-full target")
			}
			bootstrap = append(bootstrap, endpoint)
		}
		if m, ok := members[n.id].(*loadStandFullMember); ok {
			m.node.cfg.BootstrapPeers = nil
			for _, a := range bootstrap {
				m.node.cfg.BootstrapPeers = append(m.node.cfg.BootstrapPeers, string(a))
			}
			continue
		}
		credentials, err := loadstand.NewCredentials()
		if err != nil {
			t.Fatal(err)
		}
		listen, err := ports.Reserve()
		if err != nil {
			t.Fatal(err)
		}
		rpc, err := ports.Reserve()
		if err != nil {
			t.Fatal(err)
		}
		members[n.id] = &loadStandSDKMember{id: loadstand.MemberID(n.id), clock: clock, options: loadstand.ProcessOptions{Binary: binary, Credentials: credentials, Config: loadstand.ChildConfig{DataDir: filepath.Join(root, string(n.id)), Listen: listen, RPC: rpc, Bootstrap: bootstrap, Listener: false, Procs: 1}}}
	}
	return &loadStandExecutor{plan: p, members: members, wall: clock, schedule: func() loadstand.ScheduleClock { return loadstand.NewMonotonicSchedule() }, limits: loadStandFunctionalLimits()}
}
func loadStandFullOnlyOptions() loadStandPlanOpts {
	return loadStandPlanOpts{Seed: 13, FullNodes: 2, Hubs: 2, EdgeUplinks: 1, Duration: 200 * time.Millisecond, SampleInterval: 100 * time.Millisecond, Churn: loadStandChurnOpts{Profile: loadStandChurnQuiet}}
}
func TestLoadStandExecutorRealFullLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
	report, err := e.run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !report.FunctionalLifecycleComplete || report.AcceptanceEligible || len(report.Samples) != 8 {
		t.Fatalf("invalid functional lifecycle report: complete=%v acceptance=%v samples=%d", report.FunctionalLifecycleComplete, report.AcceptanceEligible, len(report.Samples))
	}
	for _, ended := range report.Ended {
		if len(ended) != 1 || ended[0].Finality != loadstand.FinalExact || !ended[0].Final.Available() {
			t.Fatal("lost exact inprocess final reading")
		}
	}
	for _, m := range e.members {
		if m.Owned() {
			t.Fatal("owned member remained after completed run")
		}
	}
}

func TestLoadStandExecutorSDKDMRestartJournal(t *testing.T)       { runLoadStandSDKTimeline(t, false) }
func TestLoadStandExecutorSDKGlobalDMRestartJournal(t *testing.T) { runLoadStandSDKTimeline(t, true) }
func runLoadStandSDKTimeline(t *testing.T, gossip bool) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("real SDK loopback child requires macOS sandbox; Linux namespace runner remains unavailable")
	}
	if testing.Short() {
		t.Skip("bounded real SDK child functional build/lifecycle")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "sdkedge")
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	build := exec.CommandContext(ctx, goBinary, "build", "-o", binary, "../../testutil/loadstand/sdkedge")
	if raw, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build SDK fixture: %v: %s", err, raw)
	}
	opts := loadStandPlanOpts{Seed: 1, FullNodes: 2, Hubs: 2, EdgeNodes: 2, EdgeUplinks: 2, Contacts: 1, Duration: 4500 * time.Millisecond, Settle: 200 * time.Millisecond, SampleInterval: 500 * time.Millisecond, Churn: loadStandChurnOpts{Profile: loadStandChurnFlap, Flap: &loadStandFlapOpts{Percent: 50, Up: 4 * time.Second, Down: 500 * time.Millisecond}}, Traffic: loadStandTrafficOpts{DirectMessages: &loadStandDMOpts{MeanInterval: 500 * time.Millisecond}}}
	if gossip {
		opts.Traffic.Gossip = &loadStandGossipOpts{Interval: time.Second}
	}
	e := loadStandFunctionalExecutor(t, opts, binary)
	journal := loadStandExecutorJournal(t, ctx)
	report, err := e.runJournal(ctx, journal)
	if err != nil {
		t.Fatalf("functional SDK executor: %v; expected=%+v received=%+v", err, e.expected, report.Receipts)
	}
	if gossip {
		planned := 0
		for _, event := range e.plan.Timeline() {
			if event.kind == loadStandEventGossip {
				planned++
			}
		}
		if planned == 0 || len(report.GlobalObservations) != planned*2 {
			t.Fatalf("global replica observations=%d planned=%d", len(report.GlobalObservations), planned)
		}
		for _, observation := range report.GlobalObservations {
			if observation.Provenance != "unsigned_claim_runtime_store" || observation.Topic != domain.GlobalMessageTopic {
				t.Fatal("overstated global provenance")
			}
		}
	}
	if !report.FunctionalLifecycleComplete || report.AcceptanceEligible || !report.FullSampleAvailable {
		t.Fatalf("overstated/missing lifecycle flags: %+v", report)
	}
	// Current SDK profiles are authoritatively configured off, so this family
	// is complete as an explicit refusal with no fabricated zero metrics.
	// That does not grant resource/profile acceptance. Stopped tails require the separate post-join final report.
	sdkSamples := 0
	for _, observation := range report.Samples {
		if e.members[loadStandNodeID(observation.Node)].Role() != loadstand.MemberSDK {
			continue
		}
		sdkSamples++
		if observation.Sample.Datagram.State != loadstand.ObservationDisabled || observation.Sample.Datagram.Value != nil {
			t.Fatal("SDK configured-off observation was unavailable or fabricated metrics")
		}
	}
	if sdkSamples == 0 {
		t.Fatal("configured-off SDK family was never actually sampled")
	}
	if len(report.Receipts) == 0 || len(report.Receipts) != len(e.expected) {
		t.Fatalf("pending ACK was treated as delivery: receipts=%d expected=%d", len(report.Receipts), len(e.expected))
	}
	restarted := false
	afterRestartDM := false
	beforeRestartDM := false
	for id, m := range e.members {
		ended := m.EndedIncarnations()
		if len(ended) == 2 {
			restarted = true
			for _, action := range report.Actions {
				if action.Node == loadStandNodeID(id) && action.Kind == "dm" {
					beforeRestartDM = beforeRestartDM || action.Incarnation.Ordinal == 1
					afterRestartDM = afterRestartDM || action.Incarnation.Ordinal == 2
				}
			}
		}
		if m.Role() == loadstand.MemberSDK {
			for _, life := range ended {
				if life.Finality != loadstand.FinalExact || !life.Final.Available() {
					t.Fatal("joined SDK final transport tail missing")
				}
			}
			child := m.(*loadStandSDKMember)
			header, err := os.ReadFile(filepath.Join(child.options.Config.DataDir, "state.db"))
			if err != nil || !slices.Equal(header[:min(len(header), 16)], []byte("SQLite format 3\x00")) {
				t.Fatal("SDK member did not persist real SQLite")
			}
		}
	}
	if !restarted || !beforeRestartDM || !afterRestartDM {
		t.Fatal("fixture failed to exercise restarted SDK as normal sender in both incarnations")
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > e.limits.MaxTraceBytes {
		t.Fatal("unbounded report")
	}
	// Resume must read verified results without starting any stopped member.
	again, err := e.runJournal(ctx, journal)
	if err != nil || len(again.Actions) != len(report.Actions) {
		t.Fatal("journal resume did not preserve verified lifecycle")
	}
}
func loadStandExecutorJournal(t *testing.T, ctx context.Context) *runjournal.Journal {
	t.Helper()
	base, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]runjournal.Snapshot, 0, 2)
	for _, spec := range []runjournal.SourceSpec{
		{Name: "measured", Base: base, Roots: []runjournal.SourceRoot{{Dir: "internal/core", Recursive: true, Filter: func(p string) bool { return filepath.Ext(p) == ".go" && !isLoadStandTestGo(p) }}}},
		{Name: "stand", Base: base, Roots: []runjournal.SourceRoot{{Dir: "internal/core/node", Recursive: false, Filter: isLoadStandTestGo}, {Dir: "internal/testutil/loadstand", Recursive: true, Filter: func(p string) bool { return filepath.Ext(p) == ".go" }}, {Dir: "sdk", Recursive: true, Filter: func(p string) bool { return filepath.Ext(p) == ".go" }}}},
	} {
		snapshot, err := runjournal.TakeSnapshot(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, snapshot)
	}
	j, err := runjournal.New(runjournal.Config{Dir: filepath.Join(t.TempDir(), "journal"), Clock: runjournal.SystemClock{}, Sources: sources})
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func isLoadStandTestGo(p string) bool {
	return filepath.Ext(p) == ".go" && len(p) >= 8 && p[len(p)-8:] == "_test.go"
}

type loadStandControlledSchedule struct {
	elapsed    time.Duration
	overshoot  time.Duration
	waits      []time.Duration
	beforeWait func()
}

func (c *loadStandControlledSchedule) Elapsed() time.Duration { return c.elapsed }
func (c *loadStandControlledSchedule) WaitUntil(ctx context.Context, at time.Duration) error {
	if c.beforeWait != nil {
		c.beforeWait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.waits = append(c.waits, at)
	c.elapsed = at + c.overshoot
	return nil
}

type loadStandJumpClock struct {
	at    time.Time
	delta time.Duration
}

func (c *loadStandJumpClock) Now() time.Time {
	c.at = c.at.Add(c.delta)
	c.delta = -c.delta
	return c.at
}
func TestLoadStandExecutorWallJumpsAndFixedSchedule(t *testing.T) {
	for _, jump := range []time.Duration{24 * time.Hour, -24 * time.Hour} {
		t.Run(jump.String(), func(t *testing.T) {
			e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
			clock := &loadStandControlledSchedule{}
			e.schedule = func() loadstand.ScheduleClock { return clock }
			e.wall = &loadStandJumpClock{at: time.Now(), delta: jump}
			report, err := e.run(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(clock.waits, []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond}) || !report.FunctionalLifecycleComplete {
				t.Fatal("UTC wall jump rebased monotonic schedule")
			}
		})
	}
}
func TestLoadStandExecutorCancellationAndOverrunCleanup(t *testing.T) {
	for _, kind := range []string{"cancel", "overrun"} {
		t.Run(kind, func(t *testing.T) {
			e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			clock := &loadStandControlledSchedule{}
			if kind == "cancel" {
				clock.beforeWait = cancel
			} else {
				clock.overshoot = e.limits.AllowedLateness + time.Nanosecond
			}
			e.schedule = func() loadstand.ScheduleClock { return clock }
			report, err := e.run(ctx)
			if err == nil || report.FunctionalLifecycleComplete {
				t.Fatal("failed/cancelled schedule marked complete")
			}
			if kind == "overrun" && !errors.Is(err, loadstand.ErrScheduleMissed) {
				t.Fatal(err)
			}
			for _, m := range e.members {
				if m.Owned() {
					t.Fatal("cancellation leaked owned member")
				}
			}
		})
	}
}
func TestLoadStandExecutorPreflightBeforeStart(t *testing.T) {
	for _, kind := range []string{"old-version", "gossip", "unknown-role", "bootstrap-empty", "bad-digest", "bound"} {
		t.Run(kind, func(t *testing.T) {
			e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
			switch kind {
			case "old-version":
				for i := range e.plan.params {
					if e.plan.params[i].Name == "plan_version" {
						e.plan.params[i].Value = "2"
					}
				}
			case "gossip":
				for i := range e.plan.params {
					if e.plan.params[i].Name == "gossip_interval" {
						e.plan.params[i].Value = "1s"
					}
				}
			case "unknown-role":
				e.plan.nodes[0].role = "unsupported"
			case "bootstrap-empty":
				e.members[e.plan.nodes[0].id].(*loadStandFullMember).node.cfg.BootstrapPeers = nil
			case "bad-digest":
				e.plan.digest = "mismatch"
			case "bound":
				e.limits.MaxSamples = 2
			}
			if kind != "bad-digest" {
				e.plan.digest = e.plan.computeDigest()
			}
			if _, err := e.run(t.Context()); err == nil {
				t.Fatal("unsafe plan accepted")
			}
			for _, m := range e.members {
				if m.Owned() || len(m.EndedIncarnations()) != 0 {
					t.Fatal("unsafe plan reached Start")
				}
			}
		})
	}
}

func TestLoadStandSDKFailedStartsHaveNoPhantomOrHistoricalEndedReceipt(t *testing.T) {
	if runtime.GOOS != "darwin" || testing.Short() {
		t.Skip("real SDK sandboxed child lifecycle")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "sdkedge")
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.CommandContext(ctx, goBinary, "build", "-o", binary, "../../testutil/loadstand/sdkedge").CombinedOutput(); err != nil {
		t.Fatalf("build SDK child: %v %s", err, out)
	}
	ports := newLoadStandPortRegistry(reserveLoopbackAddress)
	listen, err := ports.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	rpc, err := ports.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := ports.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := loadstand.NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	m := &loadStandSDKMember{id: "failed-start-controls", clock: runjournal.SystemClock{}, options: loadstand.ProcessOptions{Binary: binary, Credentials: credentials, Config: loadstand.ChildConfig{DataDir: filepath.Join(t.TempDir(), "profile"), Listen: listen, RPC: rpc, Bootstrap: []domain.PeerAddress{domain.PeerAddress(bootstrap)}, Procs: 1}}}
	t.Cleanup(func() {
		if m.Owned() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_, _ = m.Stop(cleanup)
		}
	})
	failStart := func(wantHistory int) {
		t.Helper()
		readiness, stop := context.WithCancel(ctx)
		stop()
		if _, err := m.Start(ctx, readiness); err == nil {
			t.Fatal("canceled readiness start succeeded")
		}
		if !m.Owned() {
			t.Fatal("failed readiness lost spawned owner")
		}
		receipt, _ := m.Stop(ctx)
		if !receipt.Terminated || receipt.Ended != nil || m.Owned() || len(m.EndedIncarnations()) != wantHistory {
			t.Fatal("failed Start reused/invented completed receipt", receipt, len(m.EndedIncarnations()))
		}
	}
	failStart(0)
	first, err := m.Start(ctx, ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := m.Stop(ctx)
	if err != nil || receipt.Ended == nil || receipt.Ended.Finality != loadstand.FinalExact || !receipt.Ended.Final.Available() {
		t.Fatal("actual completed Start lacked exact end", receipt, err)
	}
	if receipt.Ended.Incarnation != first.Incarnation || first.Incarnation.Ordinal != 1 {
		t.Fatal("failed attempt changed completed ordinal", receipt)
	}
	receipt.Ended.Final.Value.BytesSent++
	history := m.EndedIncarnations()
	if history[0].Final.Value.BytesSent == receipt.Ended.Final.Value.BytesSent {
		t.Fatal("StopReceipt aliases history")
	}
	history[0].Final.Value.BytesSent++
	if m.EndedIncarnations()[0].Final.Value.BytesSent == history[0].Final.Value.BytesSent {
		t.Fatal("ended projection aliases history")
	}
	failStart(1)
}

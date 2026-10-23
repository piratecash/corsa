package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

func loadStandC1Options() loadStandPlanOpts {
	return loadStandPlanOpts{Seed: 1, FullNodes: 2, Hubs: 2, EdgeNodes: 1, EdgeUplinks: 2, Contacts: 0,
		Duration: 30 * time.Second, Settle: 2 * time.Second, SampleInterval: time.Second,
		Churn:   loadStandChurnOpts{Profile: loadStandChurnQuiet},
		Traffic: loadStandTrafficOpts{Gossip: &loadStandGossipOpts{Interval: 250 * time.Millisecond, PayloadProfile: loadStandC1Global32K96}}}
}

// Uses the SAME original SDK Runtime/Desktop/SQLite and Full Services as the
// existing executor. No raw traffic, returned-counter mutation or extra sweep.
func TestLoadStandExecutorC1KnownGlobalLoadJournal(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("real SDK loopback child uses the existing Darwin sandbox")
	}
	if testing.Short() {
		t.Skip("bounded real SDK known-load lifecycle")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "sdkedge")
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	build := exec.CommandContext(ctx, goBinary, "build", "-o", binary, "../../testutil/loadstand/sdkedge")
	if raw, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build original SDK fixture: %v: %s", err, raw)
	}
	e := loadStandFunctionalExecutor(t, loadStandC1Options(), binary)
	actions, err := e.compile()
	if err != nil || len(actions) != 127 {
		t.Fatal("C1 compile/schedule", err, len(actions))
	}
	// Observe original exit channels at the existing post-start schedule frontier;
	// return the untouched genuine clock. No lifecycle/member/metric adapter.
	done := map[loadstand.MemberID]<-chan struct{}{}
	knownPublisherReady := false
	knownPublisherReason := "not_reached"
	originalSchedule := e.schedule
	e.schedule = func() loadstand.ScheduleClock {
		for _, m := range e.members {
			done[m.ID()] = m.Done()
		}
		publisher, publisherErr := e.c1OriginalPublisher()
		if publisherErr != nil {
			knownPublisherReason = "original_publisher_custody"
			return nil
		}
		fullCount := 0
		for _, member := range e.members {
			full, ok := member.(*loadStandFullMember)
			if !ok {
				continue
			}
			if !full.Owned() {
				knownPublisherReason = "full_not_owned"
				return nil
			}
			original := full.node.current.running.svc
			original.knowledgeMu.RLock()
			publicKey, present := original.pubKeys[publisher.Identity.String()]
			original.knowledgeMu.RUnlock()
			if !present || publicKey != publisher.PublicKey || !original.isVerifiedSender(publisher.Identity.String(), domain.PeerIdentity{}) {
				knownPublisherReason = "original_full_sender_not_known"
				return nil
			}
			fullCount++
		}
		if fullCount != 2 {
			knownPublisherReason = "original_full_count"
			return nil
		}
		knownPublisherReady = true
		knownPublisherReason = "verified_original_sdk_key_on_two_fulls"
		return originalSchedule()
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), e.limits.Cleanup)
		defer stop()
		if err := e.cleanup(cleanup); err != nil {
			t.Error("fallback original cleanup", err)
		}
	})
	journal := loadStandExecutorJournal(t, ctx)
	report, runErr := e.runJournal(ctx, journal)
	if runErr != nil {
		t.Logf("C1_FAILURE_METADATA %s", loadStandC1FailureMetadata(e, report, runErr, len(actions), knownPublisherReady, knownPublisherReason))
	}
	// This is checked on both positive and expected-mutant paths before any
	// arithmetic label can count as the intended cause.
	if len(report.OwnedAfterCleanup) != 0 || len(e.ownedStarts) != 0 {
		t.Fatal("C1 original owners retained")
	}
	for _, m := range e.members {
		if m.Owned() {
			t.Fatal("original member still owned", m.ID())
		}
		originalDone := done[m.ID()]
		if originalDone == nil {
			t.Fatal("original post-start channel absent", m.ID(), runErr)
		}
		select {
		case <-originalDone:
		default:
			t.Fatal("original Run/child not joined", m.ID())
		}
		ended := m.EndedIncarnations()
		if len(ended) != 1 || len(report.Ended[m.ID()]) != 1 || ended[0].Finality != loadstand.FinalExact || !ended[0].Final.Available() || ended[0].Incarnation != e.starts[loadStandNodeID(m.ID())].Incarnation || !loadStandSameTraffic(*ended[0].Final.Value, *report.Ended[m.ID()][0].Final.Value) {
			t.Fatal("original final custody lost", m.ID())
		}
		if sdk, ok := m.(*loadStandSDKMember); ok {
			header, readErr := os.ReadFile(filepath.Join(sdk.options.Config.DataDir, "state.db"))
			if readErr != nil || !slices.Equal(header[:min(len(header), 16)], []byte("SQLite format 3\x00")) {
				t.Fatal("real SDK SQLite missing", readErr)
			}
		}
	}
	if !knownPublisherReady {
		t.Fatal("C1 known-publisher precondition not established", knownPublisherReason, runErr)
	}
	if len(done) != 3 || report.PlanVersion != "4" || report.AcceptanceEligible || report.ByteBalanceScope != "declared_functional_closed_fixture" || report.ByteBalanceTerminal != "terminal_tail_unjudged" {
		t.Fatal("C1 scope/custody metadata", runErr)
	}
	inspection, inspectErr := journal.Inspect(ctx, e.journalKey())
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	if runErr != nil {
		for _, check := range report.RuntimeChecks {
			if check.Verdict != "pass" {
				t.Fatal("arithmetic candidate also has runtime/floor refusal", check.Verdict)
			}
		}
		for _, check := range report.ByteBalanceChecks {
			if check.Status == "source_invalid" {
				t.Fatal("arithmetic candidate also has source refusal", check.Reason)
			}
		}
		for _, other := range []error{context.Canceled, context.DeadlineExceeded, loadstand.ErrScheduleMissed, loadstand.ErrStopUnconfirmed, loadstand.ErrFinalAccountingPending, loadstand.ErrInvalidSample, errLoadStandByteBalanceSource} {
			if errors.Is(runErr, other) {
				if other == context.DeadlineExceeded && e.diagnosticStage == "receipt_drain" {
					t.Fatal("C1_INCONCLUSIVE_DELIVERY: original receipt drain expired; no load/time/tolerance retry", runErr)
				}
				t.Fatal("arithmetic candidate has another execution failure", other, runErr)
			}
		}
		var defect *loadStandDefect
		if !errors.Is(runErr, loadstand.ErrMemberState) || !errors.Is(runErr, errLoadStandDefect) || !errors.As(runErr, &defect) || defect.Kind != loadStandDefectByteBalance {
			t.Fatalf("C1 execution failed WITHOUT qualified arithmetic cause: %v", runErr)
		}
		if report.FunctionalLifecycleComplete || inspection.Status != runjournal.StatusFailed || inspection.Evidence == nil || inspection.Evidence.Failure == nil || inspection.Evidence.Failure.Attempts != 1 || inspection.Evidence.Failure.Detail != loadstand.ErrMemberState.Error() {
			t.Fatal("arithmetic failure escaped original sanitized journal", inspection)
		}
		if _, err := journal.Result(ctx, e.journalKey()); !errors.Is(err, runjournal.ErrNotRecorded) {
			t.Fatal("arithmetic failure published completed result", err)
		}
		var causal *loadStandByteBalanceCheck
		for i := range report.ByteBalanceChecks {
			check := &report.ByteBalanceChecks[i]
			if check.Phase == "periodic" && check.Status == "defect" && check.Reason == string(loadStandDefectByteBalance) {
				causal = check
				break
			}
		}
		if causal == nil || !loadStandC1Judged(causal, "defect") {
			t.Fatal("C1 arithmetic error lacks judged periodic evidence")
		}
		if err := loadStandC1Coverage(e, report, *causal.Earlier, *causal.Later, false); err != nil {
			t.Fatal("arithmetic failure lacks genuine known-load prefix", err)
		}
		// ROOT's distinct Source variant doubles ONLY real addSent at the socket.
		// This label is reachable only after all exact typed/power/journal/cleanup
		// assertions above. Early RED need not finish all96/192 future actions.
		t.Fatalf("C1_ARITHMETIC_DEFECT: %v; volume=%d allowance=%d imbalance=%d", runErr, causal.Computed.Volume, causal.Computed.Allowance, causal.Computed.Imbalance)
	}
	if inspection.Status != runjournal.StatusCompleted || !report.FunctionalLifecycleComplete || !report.FullSampleAvailable || len(report.Actions) != 127 || len(report.Samples) != 97 || len(report.GlobalObservations) != 192 || len(report.TrafficSweeps) != 31 {
		t.Fatal("C1 positive incomplete", inspection.Status, len(report.Actions), len(report.Samples), len(report.GlobalObservations))
	}
	later := len(report.TrafficSweeps) - 1
	if report.TrafficSweeps[later].At != 30*time.Second {
		t.Fatal("C1 final scheduled edge changed")
	}
	if err := loadStandC1Coverage(e, report, 0, later, true); err != nil {
		t.Fatal("C1 positive not covered before final edge", err)
	}
	judged := false
	for i := range report.ByteBalanceChecks {
		check := &report.ByteBalanceChecks[i]
		if check.Phase == "periodic" && check.Earlier != nil && check.Later != nil && *check.Earlier == 0 && *check.Later == later {
			judged = loadStandC1Judged(check, "judged_no_defect")
		}
	}
	if !judged {
		t.Fatal("C1_INCONCLUSIVE: final original interval unjudged; no load/time/tolerance retry")
	}
	t.Log("C1 known added load:96 SDK publications/3145728 body bytes/192 exact Full replicas; final periodic C3 judged; terminal tail unjudged")
}

func loadStandC1Judged(check *loadStandByteBalanceCheck, status string) bool {
	c := check.Computed
	if check.Status != status || check.Earlier == nil || check.Later == nil || c == nil || c.Nodes != 3 || c.Stops != 0 || c.Volume == 0 || c.AllowanceShare == nil || math.IsNaN(float64(*c.AllowanceShare)) || math.IsInf(float64(*c.AllowanceShare), 0) || *c.AllowanceShare < 0 || *c.AllowanceShare > 0.10 || check.Tolerance != loadStandDefaultBalanceTolerance() {
		return false
	}
	if status == "defect" {
		return c.Imbalance > c.Allowance
	}
	return status == "judged_no_defect" && c.Imbalance <= c.Allowance
}

// Prefix selection is the original sequential report append order, NOT any
// scheduled time or wall clock. Only prior first periodic observations count.
func loadStandC1Coverage(e *loadStandExecutor, report loadStandExecutionReport, earlier, later int, complete bool) error {
	if earlier < 0 || later <= earlier || later >= len(report.TrafficSweeps) {
		return errors.New("missing original C1 edge")
	}
	first, last := report.TrafficSweeps[earlier], report.TrafficSweeps[later]
	if first.CompletedActions < 0 || last.CompletedActions <= first.CompletedActions || last.CompletedActions > len(report.Actions) || first.SampleCount != 3 || last.SampleCount != 3 {
		return errors.New("invalid sequential C1 edge")
	}
	dispatched := map[domain.MessageID]loadStandActionTrace{}
	for index := first.CompletedActions; index < last.CompletedActions; index++ {
		trace := report.Actions[index]
		if trace.Kind != "gossip" {
			continue
		}
		body, err := e.globalBody(index)
		if err != nil {
			return err
		}
		digest := sha256.Sum256([]byte(body))
		start, ok := e.starts[trace.Node]
		if !ok || trace.Incarnation != start.Incarnation || !trace.Pending || trace.BodyBytes != 32768 || !trace.MessageID.IsValid() || trace.BodySHA256 != hex.EncodeToString(digest[:]) {
			return errors.New("unbound actual C1 dispatch")
		}
		if _, duplicate := dispatched[trace.MessageID]; duplicate {
			return errors.New("duplicate C1 publication")
		}
		dispatched[trace.MessageID] = trace
	}
	if len(dispatched) == 0 || (complete && len(dispatched) != 96) {
		return fmt.Errorf("actual C1 publication prefix=%d", len(dispatched))
	}
	observed := map[string]bool{}
	for _, observation := range report.GlobalObservations {
		trace, ok := dispatched[observation.MessageID]
		if !ok {
			return errors.New("C1 observation outside actual dispatched prefix")
		}
		start, ok := e.starts[loadStandNodeID(observation.Observer)]
		if !ok || observation.Incarnation != start.Incarnation || observation.ObserverIdentity != start.Public.Identity || e.members[loadStandNodeID(observation.Observer)].Role() != loadstand.MemberFull || observation.ClaimedSender != e.starts[trace.Node].Public.Identity || observation.BodySHA256 != trace.BodySHA256 || observation.Topic != domain.GlobalMessageTopic || observation.Provenance != loadstand.UnsignedGlobalRuntimeStore {
			return errors.New("unbound C1 replica")
		}
		// A replica first found after the selected traffic edge or only in drain
		// remains valid delivery evidence, but cannot cover C1 inside this window.
		if observation.ObservedSweep == nil || *observation.ObservedSweep < earlier || *observation.ObservedSweep >= later {
			if complete {
				return errors.New("C1 replica not observed before selected edge")
			}
			continue
		}
		key := string(observation.MessageID) + "/" + string(observation.Observer)
		if observed[key] {
			return errors.New("duplicate C1 observer receipt")
		}
		observed[key] = true
	}
	if complete && len(observed) != 192 {
		return fmt.Errorf("pre-edge C1 observations=%d", len(observed))
	}
	return nil
}

// Isolated metadata control, not a traffic/balance qualification. It proves
// first-success custody cannot be manufactured by a subsequent drain poll.
func TestLoadStandC1FirstPeriodicObservationNeverRetagged(t *testing.T) {
	for _, periodicFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(periodicFirst), func(t *testing.T) {
			message := domain.GlobalMessage{ID: "11111111-1111-4111-8111-111111111111", Topic: domain.GlobalMessageTopic, ClaimedSender: idPeerB, Body: "real-protocol-metadata-probe", CreatedAt: time.Now().UTC()}
			digest := sha256.Sum256([]byte(message.Body))
			incarnation := loadstand.Incarnation{Ordinal: 1, StartedAt: time.Now().UTC()}
			probe := &globalObserverProbe{message: message}
			e := &loadStandExecutor{starts: map[loadStandNodeID]loadstand.StartReceipt{"full": {Incarnation: incarnation, Public: loadstand.PublicIdentity{Identity: idNodeA}}}, members: map[loadStandNodeID]loadstand.Member{"full": probe}, globals: []loadStandExpectedGlobal{{Message: message, BodySHA256: hex.EncodeToString(digest[:]), Observer: "full", ObserverIdentity: idNodeA, Incarnation: incarnation}}, report: loadStandExecutionReport{TrafficSweeps: make([]loadStandTrafficSweep, 2)}}
			var err error
			if periodicFirst {
				err = e.observeGlobals(t.Context(), 0)
			} else {
				err = e.observeGlobals(t.Context())
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := e.observeGlobals(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
			if err := e.observeGlobals(t.Context()); err != nil {
				t.Fatal(err)
			}
			if probe.calls != 1 || len(e.report.GlobalObservations) != 1 {
				t.Fatal("first success was repeated")
			}
			first := e.report.GlobalObservations[0].ObservedSweep
			if (periodicFirst && (first == nil || *first != 0)) || (!periodicFirst && first != nil) {
				t.Fatal("drain/late periodic retagged original receipt", first)
			}
		})
	}
}

// Bounded scalar/receipt diagnostics are printed BEFORE any failure assertions,
// not lost with t.TempDir journal cleanup. No raw bodies, keys, env or SQL rows.
func loadStandC1FailureMetadata(e *loadStandExecutor, report loadStandExecutionReport, runErr error, plannedActions int, knownReady bool, knownReason string) string {
	type missingReplica struct {
		ID            domain.MessageID             `json:"id"`
		Observer      loadStandNodeID              `json:"observer"`
		Hash          string                       `json:"body_sha256"`
		DispatchIndex int                          `json:"dispatch_index"`
		Ordinal       loadstand.IncarnationOrdinal `json:"ordinal"`
	}
	dispatches := map[domain.MessageID]int{}
	publications := 0
	for i, action := range report.Actions {
		if action.Kind == "gossip" && action.Pending {
			publications++
			dispatches[action.MessageID] = i
		}
	}
	missing := []missingReplica{}
	for _, expected := range e.globals {
		if !expected.Observed {
			index, ok := dispatches[expected.Message.ID]
			if !ok {
				index = -1
			}
			missing = append(missing, missingReplica{expected.Message.ID, expected.Observer, expected.BodySHA256, index, expected.Incarnation.Ordinal})
		}
	}
	firstObserved := map[int]int{}
	drainObserved := 0
	for _, observation := range report.GlobalObservations {
		if observation.ObservedSweep == nil {
			drainObserved++
		} else {
			firstObserved[*observation.ObservedSweep]++
		}
	}
	var firstJudged *loadStandByteBalanceCheck
	for i := range report.ByteBalanceChecks {
		check := &report.ByteBalanceChecks[i]
		if check.Status == "judged_no_defect" || check.Status == "defect" {
			firstJudged = check
			break
		}
	}
	members := map[loadStandNodeID]map[string]any{}
	for id, start := range e.starts {
		members[id] = map[string]any{"identity": start.Public.Identity.String(), "ordinal": start.Incarnation.Ordinal, "started_at": start.Incarnation.StartedAt, "ended_count": len(report.Ended[loadstand.MemberID(id)])}
	}
	diagnostic := map[string]any{
		"execution_id": report.ExecutionID, "plan_digest": report.PlanDigest,
		"failure_stage": e.diagnosticStage, "failure_action_index": e.diagnosticActionIndex,
		"deadline": errors.Is(runErr, context.DeadlineExceeded), "cancelled": errors.Is(runErr, context.Canceled),
		"arithmetic": errors.Is(runErr, errLoadStandDefect), "source_invalid": errors.Is(runErr, errLoadStandByteBalanceSource),
		"known_publisher_ready": knownReady, "known_publisher_reason": knownReason,
		"planned_actions": plannedActions, "recorded_actions": len(report.Actions), "completed_publications": publications,
		"member_samples": len(report.Samples), "traffic_edges": len(report.TrafficSweeps),
		"expected_replicas": len(e.globals), "observed_replicas": len(report.GlobalObservations), "missing_replicas": missing,
		"first_observed_sweeps": firstObserved, "drain_only_observations": drainObserved,
		"last_actions": report.Actions[max(0, len(report.Actions)-8):],
		"last_checks":  report.ByteBalanceChecks[max(0, len(report.ByteBalanceChecks)-8):], "first_judged": firstJudged,
		"members": members, "owned_after_cleanup": report.OwnedAfterCleanup, "owned_start_count": len(e.ownedStarts),
		"functional_complete": report.FunctionalLifecycleComplete, "acceptance_eligible": report.AcceptanceEligible,
		"terminal_scope": report.ByteBalanceTerminal,
	}
	if e.diagnosticStage == "receipt_drain" && len(missing) > 0 {
		diagnostic["inconclusive_reason"] = "expected_replica_absent_after_schedule"
	}
	raw, err := json.Marshal(diagnostic)
	if err != nil {
		return `{"diagnostic_error":"json_encode","qualification":"none"}`
	}
	if len(raw) > 64<<10 {
		// Diagnostics only; never relax any actual load/time/trace/acceptance bound.
		small, _ := json.Marshal(map[string]any{"diagnostic_error": "64KiB_diagnostic_cap", "failure_stage": e.diagnosticStage, "failure_action_index": e.diagnosticActionIndex, "expected_replicas": len(e.globals), "observed_replicas": len(report.GlobalObservations), "first_judged": firstJudged, "qualification": "none"})
		return string(small)
	}
	return string(raw)
}

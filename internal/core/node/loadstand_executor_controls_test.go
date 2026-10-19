package node

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

func TestLoadStandExecutorPartialStartStillOwnedCleanup(t *testing.T) {
	e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
	second := e.members[e.plan.nodes[1].id].(*loadStandFullMember)
	listener, err := net.Listen("tcp", second.node.cfg.ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
	}()
	report, err := e.run(t.Context())
	if err == nil || report.FunctionalLifecycleComplete {
		t.Fatal("failed startup marked complete")
	}
	for _, m := range e.members {
		if m.Owned() {
			t.Fatal("readiness failure erased cleanup ownership")
		}
	}
}
func TestLoadStandExecutorReceiptControls(t *testing.T) {
	sender := domain.PeerIdentityFromWire(strings.Repeat("1", 40))
	recipient := domain.PeerIdentityFromWire(strings.Repeat("2", 40))
	other := domain.PeerIdentityFromWire(strings.Repeat("3", 40))
	good := loadstand.Receipt{ID: "11111111-1111-4111-8111-111111111111", Sender: sender, Recipient: recipient, BodySHA256: strings.Repeat("a", 64)}
	cases := []string{"valid", "wrong-sender", "wrong-recipient", "wrong-digest", "duplicate-id", "duplicate-event", "stale-event", "invalid-id"}
	for _, kind := range cases {
		t.Run(kind, func(t *testing.T) {
			receipt := good
			rows := []loadstand.Receipt{receipt}
			switch kind {
			case "wrong-sender":
				rows[0].Sender = other
			case "wrong-recipient":
				rows[0].Recipient = other
			case "wrong-digest":
				rows[0].BodySHA256 = strings.Repeat("b", 64)
			case "duplicate-id":
				rows = append(rows, receipt)
			case "duplicate-event":
				receipt.ID = "22222222-2222-4222-8222-222222222222"
				rows = append(rows, receipt)
			case "stale-event":
				rows[0].BodySHA256 = strings.Repeat("c", 64)
			case "invalid-id":
				rows[0].ID = ""
			}
			dir := t.TempDir()
			file, err := os.OpenFile(filepath.Join(dir, "receipts.jsonl"), os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				if err := json.NewEncoder(file).Encode(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			member := &loadStandSDKMember{id: "edge", options: loadstand.ProcessOptions{Config: loadstand.ChildConfig{DataDir: dir}}, receipt: loadstand.StartReceipt{Public: loadstand.PublicIdentity{Identity: recipient}}}
			e := &loadStandExecutor{plan: loadStandPlan{nodes: []loadStandPlanNode{{id: "edge", role: loadStandRoleEdge}}}, members: map[loadStandNodeID]loadstand.Member{"edge": member}, limits: loadStandFunctionalLimits(), expected: []loadStandExpectedReceipt{{Sender: sender, Recipient: recipient, BodySHA256: good.BodySHA256}}}
			err = e.observeReceipts(t.Context())
			if kind == "valid" {
				if err != nil || !e.expected[0].Delivered || len(e.report.Receipts) != 1 {
					t.Fatal("valid decrypted receipt lost")
				}
			} else if err == nil {
				t.Fatal("forged/duplicate/stale receipt accepted")
			}
		})
	}
}
func TestLoadStandExecutorWaitDetectsUnexpectedExit(t *testing.T) {
	e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
	e.starts = map[loadStandNodeID]loadstand.StartReceipt{}
	e.report.Ended = map[loadstand.MemberID][]loadstand.EndedIncarnation{}
	lifetime, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := e.start(t.Context(), lifetime, e.plan.nodes[0].id); err != nil {
		t.Fatal(err)
	}
	cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
	defer stop()
	defer func() {
		if err := e.cleanup(cleanup); err != nil && !errors.Is(err, errServiceExitedBeforeReady) {
			t.Error(err)
		}
	}()
	e.clock = loadstand.NewMonotonicSchedule()
	member := e.members[e.plan.nodes[0].id].(*loadStandFullMember)
	member.node.current.running.cancel()
	before := time.Now()
	err := e.waitUntil(t.Context(), time.Hour)
	if !errors.Is(err, loadstand.ErrMemberState) || time.Since(before) > time.Second {
		t.Fatal("dead child did not interrupt monotonic wait")
	}
}

func TestLoadStandExecutorFullJournalAndRefusal(t *testing.T) {
	e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
	journal := loadStandExecutorJournal(t, t.Context())
	report, err := e.runJournal(t.Context(), journal)
	if err != nil {
		t.Fatal(err)
	}
	if !report.FunctionalLifecycleComplete || report.AcceptanceEligible {
		t.Fatal("invalid verified functional lifecycle")
	}
	if err := journal.RecordCompleted(t.Context(), e.journalKey(), []byte("replacement")); err == nil {
		t.Fatal("journal overwrote verified completed result")
	}
	again, err := e.runJournal(t.Context(), journal)
	if err != nil || len(again.Actions) != len(report.Actions) || again.ExecutionID != report.ExecutionID {
		t.Fatal("resume relaunched an existing execution")
	}
	failed := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
	for i := range failed.plan.params {
		if failed.plan.params[i].Name == "plan_version" {
			failed.plan.params[i].Value = "2"
		}
	}
	failed.plan.digest = failed.plan.computeDigest()
	refusal, err := failed.runJournal(t.Context(), journal)
	if err == nil || refusal.FunctionalLifecycleComplete {
		t.Fatal("unsupported plan recorded as completed")
	}
	inspection, err := journal.Inspect(t.Context(), failed.journalKey())
	if err != nil || inspection.Status != runjournal.StatusFailed {
		t.Fatal("journal lost the refused configuration")
	}
}

type loadStandStopOrderMember struct {
	loadstand.Member
	stopped *[]loadstand.MemberID
}

func (m *loadStandStopOrderMember) Stop(ctx context.Context) (loadstand.StopReceipt, error) {
	*m.stopped = append(*m.stopped, m.ID())
	return m.Member.Stop(ctx)
}
func TestLoadStandExecutorCleanupReversesCurrentIncarnationStartOrder(t *testing.T) {
	e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
	e.starts = map[loadStandNodeID]loadstand.StartReceipt{}
	e.report.Ended = map[loadstand.MemberID][]loadstand.EndedIncarnation{}
	var stopped []loadstand.MemberID
	for id, member := range e.members {
		e.members[id] = &loadStandStopOrderMember{Member: member, stopped: &stopped}
	}
	lifetime, cancel := context.WithCancel(t.Context())
	defer cancel()
	cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 12*time.Second)
	defer stop()
	defer func() {
		if err := e.cleanup(cleanup); err != nil {
			t.Error(err)
		}
	}()
	first, second := e.plan.nodes[0].id, e.plan.nodes[1].id
	for _, id := range []loadStandNodeID{first, second} {
		if err := e.start(t.Context(), lifetime, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.stop(t.Context(), first, 0); err != nil {
		t.Fatal(err)
	}
	if err := e.start(t.Context(), lifetime, first); err != nil {
		t.Fatal(err)
	}
	stopped = nil
	// Cancellation must not cancel the distinct bounded cleanup context.
	cancel()
	if err := e.cleanup(cleanup); err != nil && !errors.Is(err, errServiceExitedBeforeReady) {
		t.Fatal(err)
	}
	if len(stopped) != 2 || stopped[0] != loadstand.MemberID(first) || stopped[1] != loadstand.MemberID(second) {
		t.Fatalf("cleanup order=%v, want newest incarnation %s then %s", stopped, first, second)
	}
}

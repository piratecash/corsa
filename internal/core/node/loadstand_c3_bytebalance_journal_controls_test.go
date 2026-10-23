package node

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// Real Services/Run/Stop remain original. Only one returned source projection
// is deliberately invalidated. These are integration/source-refusal controls,
// NOT a judged arithmetic byte_balance RED or a resource measurement.
type loadStandByteSourceMember struct {
	*loadStandFullMember
	mode            string
	changes         int
	started         loadstand.StartReceipt
	exited          <-chan struct{}
	original        loadstand.MemberSample
	heldTraffic     *domain.TransportTrafficStats
	suppliedTraffic []domain.TransportTrafficStats
}

func (m *loadStandByteSourceMember) Start(lifetime, readiness context.Context) (loadstand.StartReceipt, error) {
	r, err := m.loadStandFullMember.Start(lifetime, readiness)
	if err == nil {
		m.started = r
		m.exited = m.Done()
		if m.mode == "max-live-ordinal" {
			r.Incarnation.Ordinal = loadstand.IncarnationOrdinal(int(^uint(0) >> 1))
			m.changes++
		}
	}
	return r, err
}
func (m *loadStandByteSourceMember) Sample(ctx context.Context) (loadstand.MemberSample, error) {
	s, err := m.loadStandFullMember.Sample(ctx)
	if err == nil && m.mode == "max-live-ordinal" {
		s.Incarnation.Ordinal = loadstand.IncarnationOrdinal(int(^uint(0) >> 1))
		return s, nil
	}
	if err == nil && m.mode == "reused-traffic-pointer" && s.Traffic.Available() {
		if m.heldTraffic == nil {
			m.heldTraffic = new(domain.TransportTrafficStats)
		}
		*m.heldTraffic = *s.Traffic.Value
		m.suppliedTraffic = append(m.suppliedTraffic, *m.heldTraffic)
		s.Traffic.Value = m.heldTraffic
		return s, nil
	}
	if err != nil || m.mode != "foreign-traffic-start" || m.changes != 0 {
		return s, err
	}
	m.original = s
	if !s.Traffic.Available() {
		return s, loadstand.ErrInvalidSample
	}
	changed := *s.Traffic.Value
	changed.StartedAt = changed.StartedAt.Add(-time.Second)
	s.Traffic = loadstand.Observed(changed, nil)
	m.changes++
	return s, nil
}
func (m *loadStandByteSourceMember) EndedIncarnations() []loadstand.EndedIncarnation {
	original := m.loadStandFullMember.EndedIncarnations()
	if m.mode == "missing-ended" && len(original) != 0 {
		m.changes++
		return nil
	}
	return original
}

func TestLoadStandC3ByteBalanceOriginalJournalAndSourceRefusals(t *testing.T) {
	for _, mode := range []string{"unchanged", "reused-traffic-pointer", "foreign-traffic-start", "missing-ended", "max-live-ordinal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
			var originals []*loadStandByteSourceMember
			for i, n := range e.plan.nodes {
				m := &loadStandByteSourceMember{loadStandFullMember: e.members[n.id].(*loadStandFullMember), mode: "unchanged"}
				if i == 0 {
					m.mode = mode
				}
				originals = append(originals, m)
				e.members[n.id] = m
			}
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), e.limits.Cleanup)
				defer stop()
				// The intentionally invalid source may refuse again; original owners
				// must already have been joined by the real execution cleanup.
				if err := e.cleanup(cleanup); err != nil && !errors.Is(err, errLoadStandByteBalanceSource) {
					t.Error(err)
				}
			})
			journal := loadStandExecutorJournal(t, ctx)
			report, err := e.runJournal(ctx, journal)
			inspection, inspectErr := journal.Inspect(ctx, e.journalKey())
			if inspectErr != nil {
				t.Fatal(inspectErr)
			}
			if mode == "unchanged" || mode == "reused-traffic-pointer" {
				if err != nil || !report.FunctionalLifecycleComplete || report.AcceptanceEligible || inspection.Status != runjournal.StatusCompleted || len(report.Samples) != 9 || len(report.TrafficSweeps) != 3 || report.ByteBalanceTerminal != "terminal_tail_unjudged" || report.ByteBalanceScope != "declared_functional_closed_fixture" {
					t.Fatal("original functional byte composition failed", err, inspection.Status)
				}
				periodic, partial, reconciled := 0, 0, 0
				for _, check := range report.ByteBalanceChecks {
					if check.Tolerance != loadStandDefaultBalanceTolerance() {
						t.Fatal("changed byte tolerance", check)
					}
					switch check.Phase {
					case "prestop":
						partial++
						if check.Status != "undefined" || check.Reason != "partial_network_sweep" || check.PreStopOf == "" || check.Computed != nil {
							t.Fatal("partial process group masqueraded as network edge", check)
						}
					case "periodic", "reconcile":
						if check.Phase == "periodic" {
							periodic++
						} else {
							reconciled++
						}
						if check.Later == nil {
							t.Fatal("lost exact byte edge", check)
						}
						if *check.Later == 0 {
							if check.Status != "undefined" || check.Reason != "awaiting_second_edge" || check.Earlier != nil || check.Computed != nil {
								t.Fatal("first edge was judged", check)
							}
							continue
						}
						if check.Earlier == nil || *check.Earlier != 0 {
							t.Fatal("reanchored to hide interval", check)
						}
						if check.Status == "undefined" {
							if check.Reason != "zero_volume" && check.Reason != "allowance_blind" && check.Reason != "overlapping_network_edges" {
								t.Fatal("unclassified undefined", check)
							}
							// Undefined explicitly remains UNJUDGED, never counted as PASS.
						} else if check.Status != "judged_no_defect" {
							t.Fatal("functional check concealed defect", check)
						}
						if check.Computed != nil {
							b := check.Computed
							if b.Volume == 0 && (b.AllowanceShare != nil || check.Reason != "zero_volume") {
								t.Fatal("zero-volume share invented", check)
							}
							if check.Status == "judged_no_defect" && (b.Volume == 0 || b.AllowanceShare == nil || *b.AllowanceShare > check.Tolerance.MaxAllowanceShare || b.Imbalance > b.Allowance) {
								t.Fatal("undefined/defect became judged PASS", check)
							}
						}
					default:
						t.Fatal("unexpected byte judgement", check)
					}
				}
				if periodic != 3 || partial != 2 || reconciled != 1 {
					t.Fatal("missing actual periodic/partial/final scope", periodic, partial, reconciled)
				}
				for _, edge := range report.TrafficSweeps {
					if edge.SampleCount != 2 || edge.End.Before(edge.Begin) {
						t.Fatal("incomplete original network edge", edge)
					}
					for _, reading := range report.Samples[edge.SampleBegin : edge.SampleBegin+edge.SampleCount] {
						if reading.PreStopOf != "" {
							t.Fatal("preStop promoted to complete edge")
						}
					}
				}
			} else {
				if !errors.Is(err, errLoadStandByteBalanceSource) || !errors.Is(err, loadstand.ErrMemberState) || report.FunctionalLifecycleComplete || report.AcceptanceEligible || originals[0].changes == 0 {
					t.Fatal("invalid original source was harmless undefined", mode, err)
				}
				want := "traffic_original_lifetime"
				if mode == "missing-ended" {
					want = "stop_receipt_missing_history"
				}
				ordinal := originals[0].started.Incarnation.Ordinal
				if mode == "max-live-ordinal" {
					want = "live_ordinal_prefix"
					ordinal = loadstand.IncarnationOrdinal(int(^uint(0) >> 1))
					if len(report.TrafficSweeps) != 0 {
						t.Fatal("invalid huge ordinal reached checker edge")
					}
				}
				found := false
				for _, check := range report.ByteBalanceChecks {
					if check.Status == "source_invalid" && check.Reason == want && check.Node == originals[0].ID() && check.Ordinal == ordinal {
						found = true
					}
				}
				if !found {
					t.Fatal("source refusal lost exact node/life attribution", report.ByteBalanceChecks)
				}
				if inspection.Status != runjournal.StatusFailed || inspection.Evidence == nil || inspection.Evidence.Failure == nil || inspection.Evidence.Failure.Attempts != 1 || inspection.Evidence.Failure.Detail != loadstand.ErrMemberState.Error() {
					t.Fatal("failed source published success journal", inspection)
				}
				if _, resultErr := journal.Result(ctx, e.journalKey()); !errors.Is(resultErr, runjournal.ErrNotRecorded) {
					t.Fatal("source failure has completed result", resultErr)
				}
			}
			if mode == "reused-traffic-pointer" {
				actor := originals[0]
				if len(actor.suppliedTraffic) < 2 || !actor.suppliedTraffic[len(actor.suppliedTraffic)-1].ReadAt.After(actor.suppliedTraffic[0].ReadAt) {
					t.Fatal("provider did not reuse/advance the real scalar cell")
				}
				index := 0
				for _, reading := range report.Samples {
					if reading.Node != actor.ID() {
						continue
					}
					if index >= len(actor.suppliedTraffic) || !reading.Sample.Traffic.Available() || !loadStandSameTraffic(*reading.Sample.Traffic.Value, actor.suppliedTraffic[index]) {
						t.Fatal("later provider write altered original anchor/report", index)
					}
					index++
				}
				if index != len(actor.suppliedTraffic) {
					t.Fatal("lost original provider projection", index)
				}
			}
			// Marshaling an undefined zero-volume share must not fail with JSON Inf.
			if _, err := json.Marshal(report); err != nil {
				t.Fatal("nonfinite balance report", err)
			}
			if len(report.OwnedAfterCleanup) != 0 || len(e.ownedStarts) != 0 {
				t.Fatal("source refusal abandoned original owners")
			}
			for _, m := range originals {
				if m.Owned() || m.exited == nil {
					t.Fatal("original owner not released", m.ID())
				}
				select {
				case <-m.exited:
				default:
					t.Fatal("original Run not joined", m.ID())
				}
				// Read the genuine adapter directly even when the negative projection
				// deliberately hid its Ended rows from the executor.
				ended := m.loadStandFullMember.EndedIncarnations()
				if len(ended) != 1 || ended[0].Incarnation != m.started.Incarnation || ended[0].Finality != loadstand.FinalExact || !ended[0].Final.Available() {
					t.Fatal("original final accounting lost", m.ID())
				}
				if (mode == "unchanged" || mode == "reused-traffic-pointer") && (len(report.Ended[m.ID()]) != 1 || !loadStandSameTraffic(*report.Ended[m.ID()][0].Final.Value, *ended[0].Final.Value)) {
					t.Fatal("whole original Ended snapshot not retained", m.ID())
				}
			}
		})
	}
}

func TestLoadStandC3ByteBalanceRangeAndUndefinedDTO(t *testing.T) {
	// Supplemental range/serialization controls, not measured byte observations.
	dto := loadStandByteBalanceDTO(loadStandBalance{AllowanceShare: loadStandShare(math.Inf(1))})
	if dto.AllowanceShare != nil {
		t.Fatal("undefined infinity became finite share")
	}
	if _, err := json.Marshal(dto); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	node := func(id loadStandNodeID, read time.Time, sent uint64) loadStandNodeSample {
		return loadStandNodeSample{Node: id, Incarnation: loadStandIncarnationID{Life: 1, StartedAt: at}, Traffic: domain.TransportTrafficStats{StartedAt: at, ReadAt: read, BytesSent: sent}}
	}
	first := loadStandSweep{Begin: at, End: at.Add(time.Second), Nodes: []loadStandNodeSample{node("a", at.Add(time.Second), 0), node("b", at.Add(time.Second), 0)}}
	later := loadStandSweep{Begin: at.Add(2 * time.Second), End: at.Add(3 * time.Second), Nodes: []loadStandNodeSample{node("a", at.Add(3*time.Second), ^uint64(0)), node("b", at.Add(3*time.Second), 1)}}
	if err := loadStandByteBalanceRange(first, later, nil, loadStandDefaultBalanceTolerance()); err == nil {
		t.Fatal("network uint64 overflow accepted")
	}
	later.Nodes = later.Nodes[:1]
	first.Nodes = first.Nodes[:1]
	// A safe uint sum with an unrepresentable float allowance must refuse before
	// the old checker's float->uint conversion. No threshold is changed.
	first.End = at.Add(time.Second)
	first.Nodes[0].Traffic.ReadAt = at.Add(time.Second)
	later.Begin = at.Add(time.Second + time.Nanosecond)
	later.Nodes[0].Traffic.ReadAt = at.Add(3 * time.Second)
	// Make a real spread in this supplemental input without outside peers.
	later.Nodes = append(later.Nodes, node("c", at.Add(time.Second+time.Nanosecond), 0))
	first.Nodes = append(first.Nodes, node("c", at.Add(time.Second), 0))
	if err := loadStandByteBalanceRange(first, later, nil, loadStandDefaultBalanceTolerance()); err == nil {
		t.Fatal("unrepresentable allowance accepted")
	}
}

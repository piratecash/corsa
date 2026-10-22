package node

import (
	"context"
	"errors"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// Only returned observations are changed. The actual executor, two original
// Full Services, resources, sockets, identity, Run/Stop and final accounting
// are retained. Controlled heap copies are not resource measurements.
type loadStandC3Control struct {
	mode         string
	normalSweeps int
	members      []*loadStandC3Member
	changes      int
	floor        uint64
	changed      loadstand.MemberSample
}
type loadStandC3Member struct {
	*loadStandFullMember
	control  *loadStandC3Control
	calls    int
	started  loadstand.StartReceipt
	process  loadstand.RuntimeProcess
	exited   <-chan struct{}
	original loadstand.MemberSample
}

func (m *loadStandC3Member) Start(lifetime, readiness context.Context) (loadstand.StartReceipt, error) {
	r, err := m.loadStandFullMember.Start(lifetime, readiness)
	if err == nil {
		m.started = r
		m.process = m.RuntimeProcess()
		m.exited = m.Done()
	}
	return r, err
}
func (m *loadStandC3Member) Sample(ctx context.Context) (loadstand.MemberSample, error) {
	s, err := m.loadStandFullMember.Sample(ctx)
	m.calls++
	m.original = s
	c := m.control
	if err != nil || c.changes != 0 || c.mode == "unchanged" || m != c.members[len(c.members)-1] {
		return s, err
	}
	preStop := m.calls > c.normalSweeps
	if (c.mode == "prestop-floor") != preStop {
		return s, err
	}
	if c.mode == "owner-custody" {
		s.Process = loadstand.RuntimeProcess{}
	} else {
		var total uint64
		for _, original := range c.members {
			if original.calls != m.calls || !original.original.Resources.Available() {
				return s, loadstand.ErrInvalidSample
			}
			floor, floorErr := loadStandCheckedFloor(*original.original.Resources.Value)
			if floorErr != nil || floor > ^uint64(0)-total {
				return s, loadstand.ErrInvalidSample
			}
			total += floor
		}
		if total == 0 || !s.Runtime.Available() {
			return s, loadstand.ErrInvalidSample
		}
		// Copy the DTO and the changed scalar, never edit the original observation.
		runtime := *s.Runtime.Value
		heap := total - 1
		runtime.HeapObjectsBytes = &heap
		s.Runtime = loadstand.Observed(runtime, nil)
		c.floor = total
	}
	c.changes++
	c.changed = s
	return s, nil
}

func TestLoadStandC3RuntimeMutationsFailOriginalJournalAndJoin(t *testing.T) {
	for _, mode := range []string{"unchanged", "periodic-floor", "owner-custody", "prestop-floor"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
			actions, err := e.compile()
			if err != nil {
				t.Fatal(err)
			}
			c := &loadStandC3Control{mode: mode}
			for _, action := range actions {
				if action.Sample {
					c.normalSweeps++
				}
			}
			for _, n := range e.plan.nodes {
				m := &loadStandC3Member{loadStandFullMember: e.members[n.id].(*loadStandFullMember), control: c}
				c.members = append(c.members, m)
				e.members[n.id] = m
			}
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), e.limits.Cleanup)
				defer stop()
				if err := e.cleanup(cleanup); err != nil {
					t.Error("fallback original cleanup", err)
				}
			})
			journal := loadStandExecutorJournal(t, ctx)
			report, err := e.runJournal(ctx, journal)
			inspection, inspectErr := journal.Inspect(ctx, e.journalKey())
			if inspectErr != nil {
				t.Fatal(inspectErr)
			}
			if len(c.members) != 2 || !c.members[0].process.Valid() || c.members[0].process != c.members[1].process || c.members[0].started.Public.Identity == c.members[1].started.Public.Identity {
				t.Fatal("not two original services in the same physical Go heap")
			}
			if mode == "unchanged" {
				if err != nil || !report.FunctionalLifecycleComplete || !report.FullSampleAvailable || report.AcceptanceEligible || inspection.Status != runjournal.StatusCompleted || c.changes != 0 || len(report.Samples) != 9 {
					t.Fatal("genuine floor/runtime execution did not complete", err, inspection.Status)
				}
				periodic, final := 0, 0
				for _, check := range report.RuntimeChecks {
					if check.Verdict != "pass" || check.Group != 1 || check.HeapObjectsBytes == nil || check.FloorBytes > *check.HeapObjectsBytes || check.RuntimeReadAt.IsZero() {
						t.Fatal("lost genuine per-group floor result", check)
					}
					if check.PreStopOf == "" {
						periodic++
						if len(check.Members) != 2 || check.Provider != c.members[1].ID() || check.ProviderIncarnation != c.members[1].started.Incarnation {
							t.Fatal("heap was counted per node or provider was not last", check)
						}
					} else {
						final++
					}
				}
				if periodic != c.normalSweeps || final != 2 {
					t.Fatal("periodic/final group coverage changed", periodic, final)
				}
			} else {
				want := string(loadStandDefectFloorAboveHeap)
				if mode == "owner-custody" {
					want = "runtime_owner_mismatch"
				}
				var defect *loadStandDefect
				if !errors.Is(err, loadstand.ErrMemberState) || !errors.As(err, &defect) || string(defect.Kind) != want || report.FunctionalLifecycleComplete || report.AcceptanceEligible || c.changes != 1 {
					t.Fatal("directed observation bypassed typed executor refusal", mode, err, c.changes)
				}
				if inspection.Status != runjournal.StatusFailed || inspection.Evidence == nil || inspection.Evidence.Failure == nil || inspection.Evidence.Failure.Attempts != 1 || inspection.Evidence.Failure.Detail != loadstand.ErrMemberState.Error() {
					t.Fatal("journal lost exact failed attempt", inspection)
				}
				if _, resultErr := journal.Result(ctx, e.journalKey()); !errors.Is(resultErr, runjournal.ErrNotRecorded) {
					t.Fatal("failed floor published a completed result", resultErr)
				}
				found, periodic := 0, 0
				for _, check := range report.RuntimeChecks {
					if check.PreStopOf == "" && check.Verdict == "pass" {
						periodic++
					}
					if check.Verdict != want {
						continue
					}
					found++
					if check.Provider != c.members[1].ID() || check.ProviderIncarnation != c.members[1].started.Incarnation || check.Group != 1 {
						t.Fatal("defect reassigned original provider", check)
					}
					if mode == "prestop-floor" {
						if check.PreStopOf != c.members[1].ID() || len(check.Members) != 2 {
							t.Fatal("preStop omitted live shared-heap sibling", check)
						}
					} else if check.PreStopOf != "" {
						t.Fatal("periodic defect moved to cleanup", check)
					}
					if mode != "owner-custody" && (check.FloorBytes != c.floor || check.HeapObjectsBytes == nil || *check.HeapObjectsBytes != c.floor-1) {
						t.Fatal("floor/heap mutation or attribution was lost", check)
					}
				}
				if found != 1 || (mode == "prestop-floor" && periodic != c.normalSweeps) {
					t.Fatal("mutation did not reach exact periodic/final check", found, periodic)
				}
				actor := c.members[1]
				if c.changed.Identity != actor.started.Public.Identity || c.changed.Incarnation != actor.started.Incarnation || !actor.original.Runtime.Available() {
					t.Fatal("mutation replaced actual member lifetime or original heap read")
				}
			}
			if len(report.OwnedAfterCleanup) != 0 || len(e.ownedStarts) != 0 {
				t.Fatal("C3 refusal abandoned original owners")
			}
			for _, m := range c.members {
				if m.Owned() || m.exited == nil {
					t.Fatal("original owner not released", m.ID())
				}
				select {
				case <-m.exited:
				default:
					t.Fatal("original Run not joined", m.ID())
				}
				ended := report.Ended[m.ID()]
				if len(ended) != 1 || ended[0].Incarnation != m.started.Incarnation || ended[0].Finality != loadstand.FinalExact || !ended[0].Final.Available() {
					t.Fatal("original final accounting dropped", m.ID())
				}
			}
		})
	}
}

func TestLoadStandC3RuntimeValidationAndCheckedFloor(t *testing.T) {
	started := time.Now().UTC()
	genuine := readRuntimeMetrics(started, runtimeMetricSpecs, metrics.Read)
	genuine.ReadAt = time.Now().UTC()
	if err := loadstand.ValidateMemberRuntime(genuine, started); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"foreign-start", "missing-heap", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			changed := genuine
			want := loadstand.ErrFamilyUnavailable
			switch mode {
			case "foreign-start":
				changed.StartedAt = started.Add(-time.Second)
				want = loadstand.ErrInvalidSample
			case "missing-heap":
				changed.HeapObjectsBytes = nil
			case "unsupported":
				changed.Unsupported = []domain.RuntimeMetricName{"/memory/classes/heap/objects:bytes"}
			}
			if err := loadstand.ValidateMemberRuntime(changed, started); !errors.Is(err, want) {
				t.Fatal("runtime refusal lost", mode, err)
			}
		})
	}
	owner := domain.ResourceSubsystems()[0]
	for _, gauges := range [][]domain.ResourceGauge{
		{domain.NewResourceGauge("multiply", 2, ^uint64(0))},
		{domain.NewResourceGauge("first", 1, ^uint64(0)), domain.NewResourceGauge("second", 1, 1)},
	} {
		b := domain.NewResourceBreakdown(started, domain.NewSubsystemUsage(owner, gauges...))
		if _, err := loadStandCheckedFloor(b); !errors.Is(err, loadstand.ErrInvalidSample) {
			t.Fatal("floor overflow accepted", err)
		}
	}
	b := domain.NewResourceBreakdown(started, domain.NewSubsystemUsage(owner, domain.NewResourceGauge("actual", 2, 3), domain.NewSaturationGauge("occupancy", 99)))
	if floor, err := loadStandCheckedFloor(b); err != nil || floor != 6 {
		t.Fatal("saturation charged twice or valid floor lost", floor, err)
	}
}

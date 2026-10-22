package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

var errLoadStandT6SampleRead = errors.New("T6 controlled original member sample read failure")

// Only the first actual sample projection is changed. Start/Stop, sockets,
// identity, background joins and final readings remain the original member's.
// This is a functional journal control, not a Linux measurement or T6 closure.
type loadStandT6SampleMember struct {
	*loadStandFullMember
	mutation  string
	mutations int
	started   loadstand.StartReceipt
	exited    <-chan struct{}
	original  loadstand.MemberSample
	changed   loadstand.MemberSample
}

func (m *loadStandT6SampleMember) Start(lifetime, readiness context.Context) (loadstand.StartReceipt, error) {
	r, err := m.loadStandFullMember.Start(lifetime, readiness)
	if err == nil {
		m.started = r
		m.exited = m.Done()
	}
	return r, err
}

func (m *loadStandT6SampleMember) Sample(ctx context.Context) (loadstand.MemberSample, error) {
	s, err := m.loadStandFullMember.Sample(ctx)
	if err != nil || m.mutation == "unchanged" || m.mutations != 0 {
		return s, err
	}
	m.original = s
	m.mutations++
	switch m.mutation {
	case "foreign-identity":
		s.Identity[0] ^= 1
	case "foreign-incarnation":
		s.Incarnation.Ordinal++
	case "sample-read-error":
		s.Bans = loadstand.Observation[[]loadstand.BanFinding]{State: loadstand.ObservationUnavailable}
		err = errLoadStandT6SampleRead
	default:
		panic("unknown T6 test mutation")
	}
	m.changed = s
	return s, err
}

func TestLoadStandT6SampleMutationFailsOriginalJournalAndJoinsMembers(t *testing.T) {
	for _, mutation := range []string{"unchanged", "foreign-identity", "foreign-incarnation", "sample-read-error"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
			originals := make([]*loadStandT6SampleMember, 0, len(e.plan.nodes))
			var changed *loadStandT6SampleMember
			for i, node := range e.plan.nodes {
				member := &loadStandT6SampleMember{loadStandFullMember: e.members[node.id].(*loadStandFullMember), mutation: "unchanged"}
				if i == 0 {
					member.mutation = mutation
					changed = member
				}
				originals = append(originals, member)
				e.members[node.id] = member
			}
			// Unexpected assertion/start failures must not strand original owners.
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), e.limits.Cleanup)
				defer stop()
				if err := e.cleanup(cleanup); err != nil {
					t.Error("fallback original member cleanup failed", err)
				}
			})
			journal := loadStandExecutorJournal(t, ctx)
			report, err := e.runJournal(ctx, journal)
			inspection, inspectErr := journal.Inspect(ctx, e.journalKey())
			if inspectErr != nil {
				t.Fatal(inspectErr)
			}
			if mutation == "unchanged" {
				if err != nil || !report.FunctionalLifecycleComplete || report.AcceptanceEligible || len(report.Samples) != 9 || !report.RoutesReady || len(report.Actions) == 0 || inspection.Status != runjournal.StatusCompleted || changed.mutations != 0 {
					t.Fatal("unchanged original execution did not complete its journal", err, inspection.Status)
				}
			} else {
				want := loadstand.ErrMemberState
				if mutation == "sample-read-error" {
					want = errLoadStandT6SampleRead
				}
				if !errors.Is(err, want) || report.FunctionalLifecycleComplete || report.AcceptanceEligible || changed.mutations != 1 {
					t.Fatal("changed sample bypassed exact refusal or never reached original execution", mutation, err)
				}
				if inspection.Status != runjournal.StatusFailed || inspection.Evidence == nil || inspection.Evidence.Failure == nil || inspection.Evidence.Failure.Attempts != 1 || inspection.Evidence.Failure.Detail != loadstand.ErrMemberState.Error() {
					t.Fatal("journal did not retain the one failed functional attempt", inspection)
				}
				if _, resultErr := journal.Result(ctx, e.journalKey()); !errors.Is(resultErr, runjournal.ErrNotRecorded) {
					t.Fatal("failed sample published a completed journal result")
				}
				if changed.original.Identity != changed.started.Public.Identity || changed.original.Incarnation != changed.started.Incarnation || changed.original.ReadAt.IsZero() {
					t.Fatal("mutation did not begin at a genuine original sample")
				}
				observed := 0
				for _, reading := range report.Samples {
					if reading.Node != changed.ID() || reading.PreStopOf != "" {
						continue
					}
					observed++
					if reading.Sample.Identity != changed.changed.Identity || reading.Sample.Incarnation != changed.changed.Incarnation || reading.Sample.ReadAt != changed.changed.ReadAt || reading.Sample.Bans.State != changed.changed.Bans.State {
						t.Fatal("report lost or reassigned the exact failed member observation")
					}
				}
				if observed != 1 {
					t.Fatal("failed sample observation was missing or duplicated", observed)
				}
				if mutation == "sample-read-error" && (report.FullSampleAvailable || changed.changed.Bans.Value != nil) {
					t.Fatal("unavailable read was converted into complete/zero metrics")
				}
			}
			if len(originals) != 2 || len(report.OwnedAfterCleanup) != 0 || len(e.ownedStarts) != 0 {
				t.Fatal("execution lost original cleanup scope")
			}
			for _, member := range originals {
				if member.started.Public.Identity.IsZero() || member.started.Incarnation.Ordinal != 1 || member.Owned() || member.exited == nil {
					t.Fatal("original member was not genuinely started and released", member.ID())
				}
				select {
				case <-member.exited:
				default:
					t.Fatal("original Service.Run owner was not joined", member.ID())
				}
				ended := report.Ended[member.ID()]
				if len(ended) != 1 || ended[0].Incarnation != member.started.Incarnation || ended[0].Finality != loadstand.FinalExact || !ended[0].Final.Available() {
					t.Fatal("cleanup invented or dropped original final accounting", member.ID())
				}
			}
		})
	}
}

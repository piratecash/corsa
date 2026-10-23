package node

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

var errLoadStandC2Attribution = errors.New("C2 original local quota attribution invalid")
var errLoadStandC2Epoch = errors.New("C2 controlled send epoch unavailable or interfered")

// This expectation is installed only by the explicit functional control. It
// observes a closed sequence of original calls, not a new fairness policy.
// Ordinary executor profiles retain nil and keep their prior acceptance rules.
type loadStandC2Expectation struct {
	Node                         loadstand.MemberID
	Mutation                     string
	Ordinal                      loadstand.IncarnationOrdinal
	Service                      *Service
	Before, After                *datagram.LocalSendersSnapshot
	BeforeRefusals               uint64
	Cap                          int
	Executed, Attempted, Checked bool
}
type loadStandC2Check struct {
	Node                            loadstand.MemberID
	Ordinal                         loadstand.IncarnationOrdinal
	DType, Holder, Refused, Verdict string
	Capacity                        int
}

func (c *loadStandC2Expectation) requests() int {
	if c.Mutation == "unsaturated" {
		return 1
	}
	return 64
}
func (c *loadStandC2Expectation) refusal() uint64 {
	if c.Mutation == "unsaturated" {
		return 0
	}
	return 1
}
func (c *loadStandC2Expectation) params() []runjournal.Param {
	return []runjournal.Param{loadStandParam("c2_profile", "original-local-get-identity-quota-v1"), loadStandParam("c2_node", string(c.Node)), loadStandParam("c2_mutation", c.Mutation), loadStandParam("c2_capacity", "64"), loadStandParam("c2_requests", fmt.Sprint(c.requests())), loadStandParam("c2_refusal_expected", fmt.Sprint(c.refusal())), loadStandParam("c2_holder", "presence_prober"), loadStandParam("c2_refused", "identity_resolver")}
}
func (e *loadStandExecutor) checkC2Sample(id loadstand.MemberID, sample loadstand.MemberSample) error {
	c := e.c2
	if c == nil || id != c.Node || c.Attempted {
		return nil
	}
	c.Attempted = true
	check := loadStandC2Check{Node: id, Ordinal: sample.Incarnation.Ordinal, DType: domain.DTypeGetIdentity.String(), Holder: datagram.LocalSenderPresenceProber.String(), Refused: datagram.LocalSenderIdentityResolver.String(), Capacity: c.Cap}
	record := func(reason string) error {
		check.Verdict = reason
		e.report.C2Checks = append(e.report.C2Checks, check)
		if reason == "attributed-original-refusal" || reason == "original-unsaturated-request" {
			c.Checked = true
			return nil
		}
		return errors.Join(loadstand.ErrMemberState, errLoadStandC2Attribution)
	}
	if !c.Executed || c.Before == nil || c.After == nil || sample.Incarnation.Ordinal != c.Ordinal {
		return record("original-epoch-missing")
	}
	if !sample.Datagram.Available() || sample.Datagram.Value.LocalSenders == nil {
		return record("local-senders-unavailable")
	}
	d := sample.Datagram.Value
	local := d.LocalSenders
	if local.StartedAt == nil || local.ReadAt == nil || !local.StartedAt.Equal(*c.Before.StartedAt) || local.ReadAt.Before(*c.After.ReadAt) || sample.ReadAt.Before(*local.ReadAt) {
		return record("original-period-invalid")
	}
	if !reflect.DeepEqual(local.BySender, c.After.BySender) {
		return record("sender-attribution-changed")
	}
	if d.Reverse.LocalSlots != c.requests() || d.Reverse.LocalRefusals[domain.DTypeGetIdentity.String()] != c.BeforeRefusals+c.refusal() {
		return record("dtype-refusal-changed")
	}
	if c.refusal() == 0 {
		return record("original-unsaturated-request")
	}
	return record("attributed-original-refusal")
}

type loadStandC2Member struct {
	*loadStandFullMember
	target               *loadStandFullMember
	expect               *loadStandC2Expectation
	started              loadstand.StartReceipt
	exited, targetExited <-chan struct{}
	observed             bool
	original             *datagram.LocalSendersSnapshot
}

func (m *loadStandC2Member) Start(lifetime, readiness context.Context) (loadstand.StartReceipt, error) {
	r, err := m.loadStandFullMember.Start(lifetime, readiness)
	if err == nil {
		m.started = r
		m.exited = m.Done()
	}
	return r, err
}
func (m *loadStandC2Member) Sample(ctx context.Context) (loadstand.MemberSample, error) {
	c := m.expect
	if m.observed {
		return m.loadStandFullMember.Sample(ctx)
	}
	if !c.Executed {
		if err := m.CheckRunning(ctx); err != nil {
			return loadstand.MemberSample{}, err
		}
		svc := m.node.current.running.svc
		layer := svc.datagramLayer()
		if layer == nil || m.target.node.current == nil || layer.limits.Reverse.PerUpstreamCap != 64 || layer.reverse.LocalSlots() != 0 {
			return loadstand.MemberSample{}, errLoadStandC2Epoch
		}
		if err := m.target.CheckRunning(ctx); err != nil {
			return loadstand.MemberSample{}, err
		}
		m.targetExited = m.target.Done()
		c.Service = svc
		c.Cap = layer.limits.Reverse.PerUpstreamCap
		before := layer.metrics.LocalSendersSnapshot()
		c.Before = loadstand.CloneLocalSendersSnapshot(&before)
		c.BeforeRefusals = layer.reverse.LocalRefusals()[domain.DTypeGetIdentity]
		// A real valid future requirement obliges the original target handler to
		// remain silent. No answer/proof is fabricated and no queue/clock changes.
		payload, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: domain.IdentityLookupSchemaVersion, Required: []string{"c2_unknown_requirement"}})
		if err != nil {
			return loadstand.MemberSample{}, err
		}
		dst := m.target.receipt.Public.Identity
		if dst.IsZero() {
			return loadstand.MemberSample{}, errLoadStandC2Epoch
		}
		for i := 0; i < c.requests()+int(c.refusal()); i++ {
			var label domain.PeerIdentity
			label[0], label[1], label[2] = 0xc2, 0x13, byte(i+1)
			frame := protocol.DatagramFrame{Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRequest, Class: domain.DatagramClassControl, Src: label, Dst: dst, TTL: domain.DatagramDefaultMaxHops, RoutePolicy: domain.RoutePolicyBest, DType: domain.DTypeGetIdentity, Payload: payload}
			if err := frame.Validate(); err != nil {
				return loadstand.MemberSample{}, err
			}
			sender, want := datagram.LocalSenderPresenceProber, datagram.SendQueued
			if i == c.requests() {
				sender, want = datagram.LocalSenderIdentityResolver, datagram.SendRejected
			}
			if layer.pipeline.SendLocal(ctx, sender, datagram.LocalSendOpts{Frame: frame}).Kind() != want {
				return loadstand.MemberSample{}, errLoadStandC2Epoch
			}
		}
		after := layer.metrics.LocalSendersSnapshot()
		c.After = loadstand.CloneLocalSendersSnapshot(&after)
		// Only this controlled epoch may use exact equalities. Unexpected original
		// background sends or lifetime churn refuse; no sleeps/tolerance/retry.
		want := loadstand.CloneLocalSendersSnapshot(c.Before)
		holder := want.BySender[datagram.LocalSenderPresenceProber.String()]
		refused := want.BySender[datagram.LocalSenderIdentityResolver.String()]
		if holder.Attempts > ^uint64(0)-64 || holder.Queued > ^uint64(0)-64 || refused.Attempts == ^uint64(0) || refused.Rejected == ^uint64(0) || refused.QuotaCapped == ^uint64(0) || c.BeforeRefusals == ^uint64(0) {
			return loadstand.MemberSample{}, errLoadStandC2Epoch
		}
		holder.Attempts += uint64(c.requests())
		holder.Queued += uint64(c.requests())
		refused.Attempts += c.refusal()
		refused.Rejected += c.refusal()
		refused.QuotaCapped += c.refusal()
		want.BySender[datagram.LocalSenderPresenceProber.String()] = holder
		want.BySender[datagram.LocalSenderIdentityResolver.String()] = refused
		if !reflect.DeepEqual(want.BySender, c.After.BySender) || layer.reverse.LocalSlots() != c.requests() || layer.reverse.LocalRefusals()[domain.DTypeGetIdentity] != c.BeforeRefusals+c.refusal() {
			return loadstand.MemberSample{}, errLoadStandC2Epoch
		}
		c.Executed = true
	}
	sample, err := m.loadStandFullMember.Sample(ctx)
	if err != nil || c.Checked {
		return sample, err
	}
	if m.node.current == nil || m.node.current.running.svc != c.Service || sample.Incarnation != m.started.Incarnation || sample.Identity != m.started.Public.Identity || !sample.Datagram.Available() {
		return sample, errLoadStandC2Epoch
	}
	d := *sample.Datagram.Value
	m.original = loadstand.CloneLocalSendersSnapshot(d.LocalSenders)
	d.LocalSenders = loadstand.CloneLocalSendersSnapshot(d.LocalSenders)
	switch c.Mutation {
	case "unsaturated", "unchanged":
	case "hide":
		d.LocalSenders = nil
	case "misattribute":
		if d.LocalSenders == nil {
			return sample, errLoadStandC2Epoch
		}
		a := d.LocalSenders.BySender[datagram.LocalSenderPresenceProber.String()]
		b := d.LocalSenders.BySender[datagram.LocalSenderIdentityResolver.String()]
		d.LocalSenders.BySender[datagram.LocalSenderPresenceProber.String()] = b
		d.LocalSenders.BySender[datagram.LocalSenderIdentityResolver.String()] = a
	default:
		return sample, errLoadStandC2Epoch
	}
	sample.Datagram.Value = &d
	m.observed = true
	return sample, nil
}

func TestLoadStandC2OriginalQuotaAttributionReachesJournal(t *testing.T) {
	for _, mutation := range []string{"unsaturated", "unchanged", "hide", "misattribute"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			e := loadStandFunctionalExecutor(t, loadStandFullOnlyOptions(), "")
			first, second := e.plan.nodes[0].id, e.plan.nodes[1].id
			a, b := e.members[first].(*loadStandFullMember), e.members[second].(*loadStandFullMember)
			e.c2 = &loadStandC2Expectation{Node: a.ID(), Mutation: mutation, Ordinal: 1}
			m := &loadStandC2Member{loadStandFullMember: a, target: b, expect: e.c2}
			e.members[first] = m
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), e.limits.Cleanup)
				defer stop()
				if err := e.cleanup(cleanup); err != nil {
					t.Error(err)
				}
			})
			key := e.journalKey()
			if value, ok := key.Param("c2_mutation"); !ok || string(value) != mutation {
				t.Fatal("C2 input absent from original journal key")
			}
			journal := loadStandExecutorJournal(t, ctx)
			report, err := e.runJournal(ctx, journal)
			inspection, inspectErr := journal.Inspect(ctx, key)
			if inspectErr != nil {
				t.Fatal(inspectErr)
			}
			if !e.c2.Executed || e.c2.Service == nil || m.original == nil || !reflect.DeepEqual(m.original.BySender, e.c2.After.BySender) {
				t.Fatal("not an unchanged original metric projection/closed epoch", err)
			}
			if len(report.C2Checks) != 1 || report.C2Checks[0].Node != a.ID() || report.C2Checks[0].Ordinal != 1 || report.C2Checks[0].Capacity != 64 {
				t.Fatal("C2 observation lost attribution", report.C2Checks, err)
			}
			if mutation == "unsaturated" || mutation == "unchanged" {
				want := "attributed-original-refusal"
				if mutation == "unsaturated" {
					want = "original-unsaturated-request"
				}
				if err != nil || !report.FunctionalLifecycleComplete || report.AcceptanceEligible || !e.c2.Checked || inspection.Status != runjournal.StatusCompleted || report.C2Checks[0].Verdict != want {
					t.Fatal("original C2 did not complete journal", err, inspection.Status)
				}
				if _, resultErr := journal.Result(ctx, key); resultErr != nil {
					t.Fatal(resultErr)
				}
			} else {
				want := "local-senders-unavailable"
				if mutation == "misattribute" {
					want = "sender-attribution-changed"
				}
				if !errors.Is(err, errLoadStandC2Attribution) || report.FunctionalLifecycleComplete || report.AcceptanceEligible || e.c2.Checked || report.C2Checks[0].Verdict != want || inspection.Status != runjournal.StatusFailed {
					t.Fatal("C2 defect did not refuse original execution", err, report.C2Checks)
				}
				if inspection.Evidence == nil || inspection.Evidence.Failure == nil || inspection.Evidence.Failure.Attempts != 1 || inspection.Evidence.Failure.Detail != loadstand.ErrMemberState.Error() {
					t.Fatal("failed journal lost sanitized original attempt", inspection)
				}
				if _, resultErr := journal.Result(ctx, key); !errors.Is(resultErr, runjournal.ErrNotRecorded) {
					t.Fatal("C2 defect acquired completed journal result", resultErr)
				}
			}
			// The exact sample, including optional absence, survives journal result
			// decode or failed report retention without borrowing source map storage.
			found := false
			for _, observation := range report.Samples {
				if observation.Node != a.ID() || observation.PreStopOf != "" {
					continue
				}
				if found {
					continue
				}
				found = true
				if !observation.Sample.Datagram.Available() {
					t.Fatal("real datagram source unavailable")
				}
				got := observation.Sample.Datagram.Value.LocalSenders
				want := loadstand.CloneLocalSendersSnapshot(m.original)
				switch mutation {
				case "hide":
					want = nil
				case "misattribute":
					a := want.BySender[datagram.LocalSenderPresenceProber.String()]
					b := want.BySender[datagram.LocalSenderIdentityResolver.String()]
					want.BySender[datagram.LocalSenderPresenceProber.String()] = b
					want.BySender[datagram.LocalSenderIdentityResolver.String()] = a
				}
				// Success is already original journal.Result JSON-decoded by runJournal.
				// Compare every row and BOTH original stamps, including causal changes.
				if !reflect.DeepEqual(got, want) {
					t.Fatal("journal changed original sender rows or period", got, want)
				}
			}
			if !found || len(report.OwnedAfterCleanup) != 0 || len(e.ownedStarts) != 0 {
				t.Fatal("missing sample or original cleanup")
			}
			for _, member := range []*loadStandFullMember{a, b} {
				if member.Owned() || len(report.Ended[member.ID()]) != 1 {
					t.Fatal("original member owner not released")
				}
				ended := report.Ended[member.ID()][0]
				if ended.Finality != loadstand.FinalExact || !ended.Final.Available() || ended.Incarnation.Ordinal != 1 {
					t.Fatal("original final accounting missing")
				}
			}
			select {
			case <-m.exited:
			default:
				t.Fatal("original source Run was not joined")
			}
			select {
			case <-m.targetExited:
			default:
				t.Fatal("original target Run was not joined")
			}
		})
	}
}

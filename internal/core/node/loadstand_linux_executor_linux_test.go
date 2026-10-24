//go:build linux

package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// This is the existing executor's Linux composition, not a second scheduler.
// The complete plan/address/Member map is compiled before network setup.
func prepareLoadStandLinuxExecutor(ctx context.Context, plan loadStandPlan, binaries loadstand.LinuxMemberBinaries, specs []loadstand.LinuxMemberSpec, probes loadstand.LinuxIsolationProbes, wall runjournal.Clock, schedule func() loadstand.ScheduleClock, limits loadStandExecutionLimits) (*loadStandExecutor, *loadstand.LinuxMemberGroup, error) {
	if err := validateLoadStandLinuxSpecs(plan, specs); err != nil {
		return nil, nil, err
	}
	group, err := loadstand.PrepareLinuxMemberGroup(ctx, netip.MustParsePrefix("240.0.0.0/4"), binaries, specs, limits.Cleanup, probes)
	if err != nil {
		return nil, nil, err
	}
	members := make(map[loadStandNodeID]loadstand.Member, len(specs))
	for id, member := range group.Members() {
		members[loadStandNodeID(id)] = member
	}
	executor := &loadStandExecutor{plan: plan, members: members, wall: wall, schedule: schedule, limits: limits}
	if _, err = executor.compile(); err != nil {
		return nil, group, err
	}
	if err = group.Setup(ctx); err != nil {
		return nil, group, err
	}
	return executor, group, nil
}
func validateLoadStandLinuxSpecs(plan loadStandPlan, specs []loadstand.LinuxMemberSpec) error {
	candidates := make([]loadStandPlannedAddress, 0, len(specs))
	roles := make(map[loadStandNodeID]loadstand.MemberRole, len(specs))
	for _, spec := range specs {
		candidates = append(candidates, loadStandPlannedAddress{Node: loadStandNodeID(spec.ID), Address: domain.PeerAddress(spec.Config.Listen), Bootstrap: append([]domain.PeerAddress(nil), spec.Config.Bootstrap...)})
		if _, exists := roles[loadStandNodeID(spec.ID)]; exists {
			return loadstand.ErrUnsupportedPlan
		}
		roles[loadStandNodeID(spec.ID)] = spec.Role
	}
	if _, err := validateLoadStandPrimaryIPv4(plan, candidates); err != nil {
		return err
	}
	for _, node := range plan.Nodes() {
		want := loadstand.MemberFull
		if node.role == loadStandRoleEdge {
			want = loadstand.MemberSDK
		}
		if roles[node.id] != want {
			return loadstand.ErrUnsupportedPlan
		}
	}
	return nil
}
func runLoadStandLinuxPlan(ctx context.Context, plan loadStandPlan, binaries loadstand.LinuxMemberBinaries, specs []loadstand.LinuxMemberSpec, probes loadstand.LinuxIsolationProbes, journal *runjournal.Journal, wall runjournal.Clock, schedule func() loadstand.ScheduleClock, limits loadStandExecutionLimits) (report loadStandExecutionReport, result error) {
	if journal == nil {
		return report, loadstand.ErrInvalidConfiguration
	}
	executor, group, err := prepareLoadStandLinuxExecutor(ctx, plan, binaries, specs, probes, wall, schedule, limits)
	// Even failed setup may own exact partial resources; always use a fresh
	// caller-bounded cleanup context and retain any failure in the run result.
	if group != nil {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), limits.Cleanup)
			defer cancel()
			result = errors.Join(result, group.Close(cleanup))
		}()
	}
	if err != nil {
		return report, err
	}
	report, result = executor.runJournal(ctx, journal)
	if result != nil {
		return report, result
	}
	for _, reading := range report.Samples {
		if reading.Sample.LinuxProcess == nil || !reading.Sample.LinuxProcess.Available() {
			return report, loadstand.ErrInvalidSample
		}
	}
	return report, nil
}

// loadStandLinuxRetainedFailureSummary formats only already retained evidence.
// It never identifies a last action as the cause, subtracts clock domains, or
// treats the unset runtime-unavailable floor as a measured zero.
func loadStandLinuxRetainedFailureSummary(report loadStandExecutionReport) string {
	const limit = 4 << 10
	finish := func(text string) string {
		const tail = "...TRUNCATED"
		if len(text) > limit {
			return text[:limit-len(tail)] + tail
		}
		return text
	}
	label := func(text string) string {
		if len(text) > 128 {
			return text[:128]
		}
		return text
	}
	wall := func(at time.Time) string {
		if at.IsZero() {
			return "UNKNOWN"
		}
		return at.UTC().Format(time.RFC3339Nano)
	}
	text := "retained_failure_v1 raw_wall_not_schedule_clock actual_deadline=UNKNOWN first_failed_read=UNKNOWN physical_clock=UNKNOWN"
	if len(report.Actions) == 0 {
		text += " last_retained_action=UNKNOWN"
	} else {
		last := report.Actions[len(report.Actions)-1]
		text += fmt.Sprintf(" last_retained_action={index=%d kind=%+q node=%+q scheduled_ns=%d begin_ns=%d end_ns=%d wall_at=%s}", len(report.Actions)-1, label(last.Kind), label(string(last.Node)), last.Scheduled, last.Begin, last.End, wall(last.WallAt))
	}
	failed := -1
	for i, check := range report.RuntimeChecks {
		if check.PreStopOf == "" && check.Verdict != "pass" {
			failed = i
			break
		}
	}
	if failed < 0 {
		return finish(text + " first_regular_failed_runtime_check=UNKNOWN sample_join=UNKNOWN prefix=UNKNOWN")
	}
	check := report.RuntimeChecks[failed]
	text += fmt.Sprintf(" first_regular_failed_runtime_check={index=%d at_ns=%d group=%d provider=%+q ordinal=%d started_at=%s verdict=%+q}", failed, check.At, check.Group, label(string(check.Provider)), check.ProviderIncarnation.Ordinal, wall(check.ProviderIncarnation.StartedAt), label(check.Verdict))
	if check.Verdict != "runtime_unavailable" {
		return finish(text + " sample_join=UNKNOWN(reason=other-first-verdict) prefix=UNKNOWN")
	}
	text += " floor=UNKNOWN(uncomputed-before-runtime-availability) runtime_read_at=UNKNOWN"
	if check.At < 0 || check.Group == 0 || check.Provider == "" || check.ProviderIncarnation.Ordinal == 0 || check.ProviderIncarnation.StartedAt.IsZero() || len(check.Members) != 1 || check.Members[0] != check.Provider {
		return finish(text + " sample_join=UNKNOWN(reason=malformed-check) prefix=UNKNOWN")
	}
	matched, matches := -1, 0
	for i, row := range report.Samples {
		if !row.PreStop && row.PreStopOf == "" && row.At == check.At && row.Node == check.Provider && row.Sample.Incarnation == check.ProviderIncarnation {
			matched, matches = i, matches+1
		}
	}
	if matches != 1 {
		return finish(text + fmt.Sprintf(" sample_join=UNKNOWN(matches=%d) prefix=UNKNOWN", matches))
	}
	if report.Samples[matched].Sample.Runtime.State != loadstand.ObservationUnavailable || report.Samples[matched].Sample.Runtime.Value != nil {
		return finish(text + " sample_join=UNKNOWN(reason=runtime-state-conflict) prefix=UNKNOWN")
	}
	// compile() admits strictly increasing periodic sample At values. The
	// collector appends a row before its check and stops at the first refusal.
	// Cleanup rows have a nonempty PreStopOf and cannot fill this prefix.
	var prefix [4]int
	count := 0
	for i, row := range report.Samples {
		if row.PreStop || row.PreStopOf != "" || row.At != check.At {
			continue
		}
		if i > matched || count == len(prefix) || row.Node == "" || row.Sample.Incarnation.Ordinal == 0 || row.Sample.Incarnation.StartedAt.IsZero() {
			return finish(text + " sample_join=UNKNOWN(reason=malformed-or-overbound-prefix) prefix=UNKNOWN")
		}
		for j := 0; j < count; j++ {
			prior := report.Samples[prefix[j]]
			if prior.Node == row.Node {
				return finish(text + " sample_join=UNKNOWN(reason=duplicate-or-reused-member-prefix) prefix=UNKNOWN")
			}
		}
		prefix[count] = i
		count++
	}
	correlated := len(report.Actions) > 0 && report.Actions[len(report.Actions)-1].Kind == "sample" && report.Actions[len(report.Actions)-1].Scheduled == check.At
	text += fmt.Sprintf(" sample_join=unique sample_index=%d last_retained_action_matches_sample_at=%t regular_prefix_count=%d", matched, correlated, count)
	for i := 0; i < count; i++ {
		row := report.Samples[prefix[i]]
		s := row.Sample
		osState, osBegin, osEnd, osValid := "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"
		if s.LinuxProcess != nil {
			osState = string(s.LinuxProcess.State)
			if s.LinuxProcess.Available() {
				value := s.LinuxProcess.Value
				osBegin, osEnd = wall(value.ReadStartedAt), wall(value.ReadCompletedAt)
				osValid = fmt.Sprint(!value.ReadStartedAt.IsZero() && !value.ReadCompletedAt.IsZero() && !value.ReadCompletedAt.Before(value.ReadStartedAt))
			}
		}
		text += fmt.Sprintf(" prefix[%d]={sample_index=%d node=%+q ordinal=%d started_at=%s read_at=%s states={Runtime=%+q LinuxProcess=%+q Resources=%+q Traffic=%+q Sessions=%+q Modes=%+q Neighbours=%+q RouteEntries=%+q Datagram=%+q Bans=%+q} os_after_begin=%s os_after_end=%s os_after_valid=%s}", i, prefix[i], label(string(row.Node)), s.Incarnation.Ordinal, wall(s.Incarnation.StartedAt), wall(s.ReadAt), label(string(s.Runtime.State)), label(osState), label(string(s.Resources.State)), label(string(s.Traffic.State)), label(string(s.Sessions.State)), label(string(s.Modes.State)), label(string(s.Neighbours.State)), label(string(s.RouteEntries.State)), label(string(s.Datagram.State)), label(string(s.Bans.State)), osBegin, osEnd, osValid)
	}
	return finish(text)
}

// This is an explicitly admitted native entry. Ordinary/short tests never
// create namespaces, alter links, launch targets or send isolation probes.
func TestLoadStandLinuxPlannedFullSDKTimelineNative(t *testing.T) {
	if os.Getenv("LOADSTAND_LINUX_NATIVE") != "1" || testing.Short() {
		t.Skip("requires separately admitted own-container planned Linux fixture")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
	defer cancel()
	opts := loadStandPlanOpts{Seed: 1, FullNodes: 2, Hubs: 2, EdgeNodes: 2, EdgeUplinks: 2, Contacts: 1, Duration: 4500 * time.Millisecond, Settle: 200 * time.Millisecond, SampleInterval: 500 * time.Millisecond, Churn: loadStandChurnOpts{Profile: loadStandChurnFlap, Flap: &loadStandFlapOpts{Percent: 50, Up: 4 * time.Second, Down: 500 * time.Millisecond}}, Traffic: loadStandTrafficOpts{DirectMessages: &loadStandDMOpts{MeanInterval: 500 * time.Millisecond}, Gossip: &loadStandGossipOpts{Interval: time.Second}}}
	plan, err := buildLoadStandPlan(opts)
	if err != nil {
		t.Fatal(err)
	}
	binaries := loadstand.LinuxMemberBinaries{Helper: os.Getenv("LOADSTAND_NATIVE_HELPER"), HelperSHA: loadstand.ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_HELPER_SHA")), SDK: os.Getenv("LOADSTAND_NATIVE_TARGET"), SDKSHA: loadstand.ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_TARGET_SHA")), Full: os.Getenv("LOADSTAND_NATIVE_FULL"), FullSHA: loadstand.ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_FULL_SHA")), NFT: os.Getenv("LOADSTAND_NATIVE_NFT"), NFTSHA: loadstand.ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_NFT_SHA"))}
	var probes loadstand.LinuxIsolationProbes
	for key, dst := range map[string]*netip.AddrPort{"LOADSTAND_NATIVE_PRODUCTION_PROBE": &probes.ProductionBootstrap, "LOADSTAND_NATIVE_FALLBACK_PROBE": &probes.FallbackPublic, "LOADSTAND_NATIVE_IPV6_PROBE": &probes.PublicIPv6} {
		value, parseErr := netip.ParseAddrPort(os.Getenv(key))
		if parseErr != nil {
			t.Fatal("explicit numeric isolation target required", key)
		}
		*dst = value
	}
	nodes := plan.Nodes()
	endpoints := make(map[loadStandNodeID]domain.PeerAddress, len(nodes))
	for i, node := range nodes {
		endpoints[node.id] = domain.PeerAddress(fmt.Sprintf("240.%d.0.1:64646", i))
	}
	specs := make([]loadstand.LinuxMemberSpec, 0, len(nodes))
	for _, node := range nodes {
		var nonce loadstand.LaunchNonce
		if _, err = rand.Read(nonce[:]); err != nil {
			t.Fatal(err)
		}
		parent, createErr := loadstand.CreateAuditedProfileParent(ctx, "/profiles", nonce)
		if createErr != nil {
			t.Fatal("original profile parent refused", createErr)
		}
		defer func() {
			if closeErr := parent.Close(); closeErr != nil {
				t.Error("original profile descriptor close failed", closeErr)
			}
		}()
		credentials, credentialErr := loadstand.NewCredentials()
		if credentialErr != nil {
			t.Fatal(credentialErr)
		}
		role := loadstand.MemberFull
		if node.role == loadStandRoleEdge {
			role = loadstand.MemberSDK
		}
		bootstrap := make([]domain.PeerAddress, 0, len(node.dialTargets))
		for _, target := range node.dialTargets {
			bootstrap = append(bootstrap, endpoints[target])
		}
		specs = append(specs, loadstand.LinuxMemberSpec{ID: loadstand.MemberID(node.id), Role: role, Parent: parent, Credentials: credentials, UID: 65532, GID: 65532, Config: loadstand.ChildConfig{DataDir: parent.MemberDirectory(), Listen: domain.ListenAddress(endpoints[node.id]), RPC: "127.0.0.1:42051", Bootstrap: bootstrap, Listener: role == loadstand.MemberFull, Procs: 1}})
	}
	report, err := runLoadStandLinuxPlan(ctx, plan, binaries, specs, probes, loadStandExecutorJournal(t, ctx), runjournal.SystemClock{}, func() loadstand.ScheduleClock { return loadstand.NewMonotonicSchedule() }, loadStandFunctionalLimits())
	if err != nil {
		t.Fatalf("actual planned Full/SDK executor failed %v; %s", err, loadStandLinuxRetainedFailureSummary(report))
	}
	if !report.FunctionalLifecycleComplete || !report.FullSampleAvailable || report.AcceptanceEligible || len(report.Receipts) == 0 || len(report.GlobalObservations) == 0 {
		t.Fatal("missing actual functional receipts/OS sample/global/finality or overstated resource acceptance")
	}
	restarted := false
	for _, ended := range report.Ended {
		if len(ended) > 1 {
			restarted = true
		}
		for _, incarnation := range ended {
			if incarnation.Finality != loadstand.FinalExact || !incarnation.Final.Available() {
				t.Fatal("unconfirmed original final incarnation")
			}
		}
	}
	if !restarted {
		t.Fatal("planned restart never completed")
	}
	processes := make(map[loadstand.MemberID]map[loadstand.IncarnationOrdinal]loadstand.ProcessIncarnation)
	for _, reading := range report.Samples {
		if reading.Sample.LinuxProcess == nil || !reading.Sample.LinuxProcess.Available() {
			t.Fatal("native sample lacks original OS process")
		}
		value := reading.Sample.LinuxProcess.Value.Provenance.Incarnation
		if processes[reading.Node] == nil {
			processes[reading.Node] = make(map[loadstand.IncarnationOrdinal]loadstand.ProcessIncarnation)
		}
		old, exists := processes[reading.Node][reading.Sample.Incarnation.Ordinal]
		if exists && old != value {
			t.Fatal("process identity changed within an incarnation")
		}
		processes[reading.Node][reading.Sample.Incarnation.Ordinal] = value
	}
	for id, ended := range report.Ended {
		if len(ended) < 2 {
			continue
		}
		first, firstOK := processes[id][ended[0].Incarnation.Ordinal]
		last, lastOK := processes[id][ended[len(ended)-1].Incarnation.Ordinal]
		if !firstOK || !lastOK || first == last {
			t.Fatal("restart did not retain distinct actual process incarnations")
		}
	}
	// Each successful Start itself rechecks the original namespace pointer and
	// kernel identity; process restart does not create a new network namespace.

}
func TestLoadStandLinuxCompositionValidatesOriginalAddressesBeforeEffects(t *testing.T) {
	plan, err := buildLoadStandPlan(loadStandFullOnlyOptions())
	if err != nil {
		t.Fatal(err)
	}
	nodes := plan.Nodes()
	specs := make([]loadstand.LinuxMemberSpec, len(nodes))
	for i, node := range nodes {
		specs[i] = loadstand.LinuxMemberSpec{ID: loadstand.MemberID(node.id), Role: loadstand.MemberFull, Config: loadstand.ChildConfig{Listen: domain.ListenAddress(fmt.Sprintf("240.%d.0.1:64646", i))}}
	}
	for i, node := range nodes {
		for _, target := range node.dialTargets {
			for j, other := range nodes {
				if other.id == target {
					specs[i].Config.Bootstrap = append(specs[i].Config.Bootstrap, domain.PeerAddress(specs[j].Config.Listen))
				}
			}
		}
	}
	if err = validateLoadStandLinuxSpecs(plan, specs); err != nil {
		t.Fatal("exact pure plan refused", err)
	}
	specs[1].Config.Listen = "240.0.0.2:64646"
	if err = validateLoadStandLinuxSpecs(plan, specs); err == nil {
		t.Fatal("same-source-group plan reached kernel constructor")
	}
}

// Pure retained-data controls: no original member, clock, RPC or kernel work.
func TestLoadStandLinuxRetainedFailureSummary(t *testing.T) {
	fixture := func() loadStandExecutionReport {
		incarnation := loadstand.Incarnation{Ordinal: 1, StartedAt: time.Unix(11, 0).UTC()}
		sample := loadstand.MemberSample{Incarnation: incarnation, ReadAt: time.Unix(0, 0).UTC()}
		sample.Runtime.State = loadstand.ObservationUnavailable
		os := loadstand.Observed(loadstand.LinuxProcessSample{ReadStartedAt: time.Unix(0, 0).UTC(), ReadCompletedAt: time.Unix(0, 0).UTC()}, nil)
		sample.LinuxProcess = &os
		return loadStandExecutionReport{
			Actions:       []loadStandActionTrace{{Kind: "sample", Scheduled: 0, Begin: 0, End: 7}},
			RuntimeChecks: []loadStandRuntimeCheck{{At: 0, Group: 3, Provider: "edge-000", Members: []loadstand.MemberID{"edge-000"}, ProviderIncarnation: incarnation, Verdict: "runtime_unavailable"}},
			Samples:       []loadStandMemberObservation{{At: 0, Node: "edge-000", Sample: sample}},
		}
	}
	t.Run("known-zero-and-existing-wall-metadata", func(t *testing.T) {
		text := loadStandLinuxRetainedFailureSummary(fixture())
		for _, expected := range []string{"sample_join=unique", "scheduled_ns=0 begin_ns=0", "at_ns=0", "read_at=1970-01-01T00:00:00Z", "os_after_begin=1970-01-01T00:00:00Z", "os_after_end=1970-01-01T00:00:00Z", "os_after_valid=true", "floor=UNKNOWN(uncomputed-before-runtime-availability)", "actual_deadline=UNKNOWN", "physical_clock=UNKNOWN"} {
			if !strings.Contains(text, expected) {
				t.Fatal("missing original scalar/UNKNOWN distinction", expected, text)
			}
		}
	})
	t.Run("missing-ambiguous-and-cleanup-are-unknown", func(t *testing.T) {
		for _, change := range []string{"missing", "duplicate", "cleanup", "different-incarnation", "runtime-conflict"} {
			report := fixture()
			switch change {
			case "missing":
				report.Samples = nil
			case "duplicate":
				report.Samples = append(report.Samples, report.Samples[0])
			case "cleanup":
				report.Samples[0].PreStop, report.Samples[0].PreStopOf = true, "edge-000"
			case "different-incarnation":
				report.Samples[0].Sample.Incarnation.StartedAt = time.Unix(12, 0).UTC()
			case "runtime-conflict":
				report.Samples[0].Sample.Runtime = loadstand.Observed(domain.RuntimeMetrics{}, nil)
			}
			text := loadStandLinuxRetainedFailureSummary(report)
			if !strings.Contains(text, "sample_join=UNKNOWN") || strings.Contains(text, "prefix[0]=") {
				t.Fatal("fabricated original sample join", change, text)
			}
		}
	})
	t.Run("first-regular-failure-and-retained-action-label", func(t *testing.T) {
		report := fixture()
		cleanup := report.RuntimeChecks[0]
		cleanup.PreStopOf = "edge-000"
		report.RuntimeChecks = append([]loadStandRuntimeCheck{cleanup}, report.RuntimeChecks...)
		report.Actions[0].Kind = "direct_message"
		text := loadStandLinuxRetainedFailureSummary(report)
		if !strings.Contains(text, "index=1 at_ns=0 group=3") || !strings.Contains(text, "last_retained_action_matches_sample_at=false") || !strings.Contains(text, "sample_join=unique") {
			t.Fatal("cleanup displaced regular check or action misidentified", text)
		}
		report.RuntimeChecks[1].Verdict = "resource_floor_unavailable"
		report.RuntimeChecks = append(report.RuntimeChecks, fixture().RuntimeChecks[0])
		text = loadStandLinuxRetainedFailureSummary(report)
		if !strings.Contains(text, "reason=other-first-verdict") || strings.Contains(text, "sample_join=unique") {
			t.Fatal("later check replaced first regular failure", text)
		}
	})
	t.Run("prefix-is-ordered-bounded-and-not-incarnation-only", func(t *testing.T) {
		report := fixture()
		prior := report.Samples[0]
		prior.Node = "full-000" // The same scalar incarnation on another member is not a duplicate tuple.
		report.Samples = append([]loadStandMemberObservation{prior}, report.Samples...)
		text := loadStandLinuxRetainedFailureSummary(report)
		if !strings.Contains(text, "regular_prefix_count=2") || !strings.Contains(text, "prefix[0]={sample_index=0 node=\"full-000\"") || !strings.Contains(text, "prefix[1]={sample_index=1 node=\"edge-000\"") {
			t.Fatal("lost retained order or conflated member incarnations", text)
		}
		for _, change := range []string{"duplicate-prefix", "overbound", "later-regular"} {
			report := fixture()
			prior := report.Samples[0]
			prior.Node = "full-000"
			switch change {
			case "duplicate-prefix":
				report.Samples = append([]loadStandMemberObservation{prior, prior}, report.Samples...)
			case "overbound":
				for i := 0; i < 4; i++ {
					prior.Node = loadstand.MemberID(fmt.Sprintf("full-%03d", i))
					report.Samples = append([]loadStandMemberObservation{prior}, report.Samples...)
				}
			case "later-regular":
				report.Samples = append(report.Samples, prior)
			}
			text := loadStandLinuxRetainedFailureSummary(report)
			if !strings.Contains(text, "prefix=UNKNOWN") || strings.Contains(text, "prefix[0]=") {
				t.Fatal("ambiguous/overbound prefix was published", change, text)
			}
		}
	})
	t.Run("malformed-and-post-format-byte-bound", func(t *testing.T) {
		if text := loadStandLinuxRetainedFailureSummary(loadStandExecutionReport{}); !strings.Contains(text, "sample_join=UNKNOWN") || len(text) > 4<<10 {
			t.Fatal("empty report was not bounded UNKNOWN", text)
		}
		report := fixture()
		check := report.RuntimeChecks[0]
		check.Members = nil
		report.RuntimeChecks[0] = check
		if text := loadStandLinuxRetainedFailureSummary(report); !strings.Contains(text, "reason=malformed-check") {
			t.Fatal("malformed provider tuple accepted", text)
		}
		report = fixture()
		row := report.Samples[0]
		long := strings.Repeat("\x00", 4096)
		row.Sample.Resources.State, row.Sample.Traffic.State = loadstand.ObservationState(long), loadstand.ObservationState(long)
		row.Sample.Sessions.State, row.Sample.Modes.State = loadstand.ObservationState(long), loadstand.ObservationState(long)
		row.Sample.Neighbours.State, row.Sample.RouteEntries.State = loadstand.ObservationState(long), loadstand.ObservationState(long)
		row.Sample.Datagram.State, row.Sample.Bans.State = loadstand.ObservationState(long), loadstand.ObservationState(long)
		report.Samples[0] = row
		text := loadStandLinuxRetainedFailureSummary(report)
		if len(text) != 4<<10 || !strings.HasSuffix(text, "...TRUNCATED") {
			t.Fatal("post-quote byte bound not applied", len(text))
		}
	})
}

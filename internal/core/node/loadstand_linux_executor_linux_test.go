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

// Formats two retained full-ELF read/SHA-update spans, never hash CPU.
func loadStandLinuxExecutableReadBracketSummary(pair loadstand.ProcessExecutableReadBrackets, outer int64, outerKnown bool) (string, int64, bool) {
	const max = int64(1<<63 - 1)
	text := ""
	total, all := int64(0), true
	for i, bracket := range []loadstand.ProcessExecutableReadBracket{pair.First, pair.Last} {
		name := "first"
		if i == 1 {
			name = "last"
		}
		ns, bytes := "UNKNOWN", "UNKNOWN"
		bytesKnown := bracket.VerifiedBytes >= 4 && bracket.VerifiedBytes <= 128<<20
		if bytesKnown {
			bytes = fmt.Sprint(bracket.VerifiedBytes)
		}
		if bytesKnown && bracket.ElapsedKnown && bracket.ElapsedNS >= 0 && bracket.ElapsedNS < max {
			ns = fmt.Sprint(bracket.ElapsedNS)
			if bracket.ElapsedNS > max-total {
				all = false
			} else {
				total += bracket.ElapsedNS
			}
		} else {
			all = false
		}
		if i != 0 {
			text += " "
		}
		text += fmt.Sprintf("%s_ns=%s %s_bytes=%s", name, ns, name, bytes)
	}
	sum, uncovered, contained := "UNKNOWN", "UNKNOWN", "UNKNOWN"
	if all {
		sum = fmt.Sprint(total)
		if outerKnown && outer >= 0 && outer < max && total <= outer {
			uncovered = fmt.Sprint(outer - total)
			contained = "true"
		}
	}
	text += fmt.Sprintf(" sum_ns=%s contained=%s uncovered_ns=%s", sum, contained, uncovered)
	return text, total, all
}

// Existing helper sampler read brackets only: not CPU, hash cost or parent IPC.
func loadStandLinuxReadBracketSummary(report loadStandExecutionReport) string {
	const unknown = "sampler_read_brackets_v1=UNKNOWN"
	if len(report.Actions) == 0 {
		return unknown
	}
	index := len(report.Actions) - 1
	action := report.Actions[index]
	if action.Kind != "sample" || action.Scheduled < 0 || action.Begin < action.Scheduled || action.End < action.Begin {
		return unknown
	}
	for i, a := range report.Actions {
		if i != index && a.Kind == "sample" && a.Scheduled == action.Scheduled {
			return unknown
		}
	}
	matched, matches := -1, 0
	for i, edge := range report.TrafficSweeps {
		if edge.CompletedActions == index && edge.At == action.Scheduled {
			matched = i
			matches++
		}
	}
	if matches != 1 {
		return unknown
	}
	edge := report.TrafficSweeps[matched]
	if edge.SampleCount < 1 || edge.SampleCount > 4 || edge.SampleBegin < 0 || edge.SampleBegin > len(report.Samples)-edge.SampleCount || edge.Begin.IsZero() || edge.End.Before(edge.Begin) {
		return unknown
	}
	end := edge.SampleBegin + edge.SampleCount
	for i, row := range report.Samples {
		if i < edge.SampleBegin || i >= end {
			if !row.PreStop && row.PreStopOf == "" && row.At == edge.At {
				return unknown
			}
			continue
		}
		if row.PreStop || row.PreStopOf != "" || row.At != edge.At || row.Node == "" || row.Sample.Incarnation.Ordinal == 0 || row.Sample.Incarnation.StartedAt.IsZero() {
			return unknown
		}
		for j := edge.SampleBegin; j < i; j++ {
			if report.Samples[j].Node == row.Node {
				return unknown
			}
		}
	}
	const max = int64(1<<63 - 1)
	beforeTotal, afterTotal := int64(0), int64(0)
	beforeAll, afterAll := true, true
	elfBeforeTotal, elfAfterTotal := int64(0), int64(0)
	elfBeforeAll, elfAfterAll := true, true
	text := fmt.Sprintf("sampler_read_brackets_v1=unique action_index=%d sweep_index=%d rows=%d", index, matched, edge.SampleCount)
	for i := edge.SampleBegin; i < end; i++ {
		row := report.Samples[i]
		s := row.Sample
		before, after := "UNKNOWN", "UNKNOWN"
		if s.LinuxBeforeElapsedKnown && s.LinuxBeforeElapsedNS >= 0 && s.LinuxBeforeElapsedNS < max {
			before = fmt.Sprint(s.LinuxBeforeElapsedNS)
			if s.LinuxBeforeElapsedNS > max-beforeTotal {
				beforeAll = false
			} else {
				beforeTotal += s.LinuxBeforeElapsedNS
			}
		} else {
			beforeAll = false
		}
		if s.LinuxProcess != nil && s.LinuxProcess.Available() && s.LinuxProcess.Value.ReadElapsedKnown && s.LinuxProcess.Value.ReadElapsedNS >= 0 && s.LinuxProcess.Value.ReadElapsedNS < max {
			n := s.LinuxProcess.Value.ReadElapsedNS
			after = fmt.Sprint(n)
			if n > max-afterTotal {
				afterAll = false
			} else {
				afterTotal += n
			}
		} else {
			afterAll = false
		}
		elfBefore, beforeStreamTotal, beforeStreamKnown := loadStandLinuxExecutableReadBracketSummary(s.LinuxBeforeExecutableReadBrackets, s.LinuxBeforeElapsedNS, s.LinuxBeforeElapsedKnown)
		afterPair := loadstand.ProcessExecutableReadBrackets{}
		afterOuter, afterOuterKnown := int64(0), false
		if s.LinuxProcess != nil && s.LinuxProcess.Available() {
			afterPair = s.LinuxProcess.Value.ExecutableReadBrackets
			afterOuter = s.LinuxProcess.Value.ReadElapsedNS
			afterOuterKnown = s.LinuxProcess.Value.ReadElapsedKnown
		}
		elfAfter, afterStreamTotal, afterStreamKnown := loadStandLinuxExecutableReadBracketSummary(afterPair, afterOuter, afterOuterKnown)
		if beforeStreamKnown && beforeStreamTotal <= max-elfBeforeTotal {
			elfBeforeTotal += beforeStreamTotal
		} else {
			elfBeforeAll = false
		}
		if afterStreamKnown && afterStreamTotal <= max-elfAfterTotal {
			elfAfterTotal += afterStreamTotal
		} else {
			elfAfterAll = false
		}
		label := string(row.Node)
		if len(label) > 128 {
			label = label[:128]
		}
		text += fmt.Sprintf(" read_bracket[%d]={sample_index=%d node=%+q ordinal=%d started_at=%s before_ns=%s after_ns=%s elf_before={%s} elf_after={%s}}", i-edge.SampleBegin, i, label, s.Incarnation.Ordinal, s.Incarnation.StartedAt.UTC().Format(time.RFC3339Nano), before, after, elfBefore, elfAfter)
	}
	beforeSum, afterSum, total := "UNKNOWN", "UNKNOWN", "UNKNOWN"
	if beforeAll {
		beforeSum = fmt.Sprint(beforeTotal)
	}
	if afterAll {
		afterSum = fmt.Sprint(afterTotal)
	}
	if beforeAll && afterAll && beforeTotal <= max-afterTotal {
		total = fmt.Sprint(beforeTotal + afterTotal)
	}
	text += fmt.Sprintf(" before_sum_ns=%s after_sum_ns=%s combined_sum_ns=%s scope=successful_sampler_read_brackets_not_cpu_or_hash", beforeSum, afterSum, total)
	elfBeforeSum, elfAfterSum, elfSum := "UNKNOWN", "UNKNOWN", "UNKNOWN"
	if elfBeforeAll {
		elfBeforeSum = fmt.Sprint(elfBeforeTotal)
	}
	if elfAfterAll {
		elfAfterSum = fmt.Sprint(elfAfterTotal)
	}
	if elfBeforeAll && elfAfterAll && elfBeforeTotal <= max-elfAfterTotal {
		elfSum = fmt.Sprint(elfBeforeTotal + elfAfterTotal)
	}
	text += fmt.Sprintf(" elf_before_sum_ns=%s elf_after_sum_ns=%s elf_combined_sum_ns=%s elf_scope=full_elf_read_sha_update_not_cpu", elfBeforeSum, elfAfterSum, elfSum)
	if len(text) > 3000 {
		return unknown
	}
	return text
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
	text += " " + loadStandLinuxReadBracketSummary(report)
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

func TestLoadStandLinuxSamplerReadBracketSummary(t *testing.T) {
	fixture := func() loadStandExecutionReport {
		r := loadStandExecutionReport{Actions: []loadStandActionTrace{{Kind: "sample"}, {Kind: "gossip"}, {Kind: "sample", Scheduled: 500 * time.Millisecond, Begin: 2495027680, End: 5239379760}}, TrafficSweeps: []loadStandTrafficSweep{{Begin: time.Unix(1, 0), End: time.Unix(2, 0), SampleCount: 4, CompletedActions: 0}, {At: 500 * time.Millisecond, Begin: time.Unix(3, 0), End: time.Unix(4, 0), SampleBegin: 4, SampleCount: 4, CompletedActions: 2}}}
		var prior, current []loadStandMemberObservation
		ids := []loadstand.MemberID{"full-000", "full-001", "edge-000", "edge-001"}
		for i := 0; i < 4; i++ {
			bytes := int64(20709639)
			if i >= 2 {
				bytes = 22675719
			}
			first := int64(i*4 + 1)
			beforePair := loadstand.ProcessExecutableReadBrackets{First: loadstand.ProcessExecutableReadBracket{ElapsedNS: first, ElapsedKnown: true, VerifiedBytes: bytes}, Last: loadstand.ProcessExecutableReadBracket{ElapsedNS: first + 1, ElapsedKnown: true, VerifiedBytes: bytes}}
			afterPair := loadstand.ProcessExecutableReadBrackets{First: loadstand.ProcessExecutableReadBracket{ElapsedNS: first + 2, ElapsedKnown: true, VerifiedBytes: bytes}, Last: loadstand.ProcessExecutableReadBracket{ElapsedNS: first + 3, ElapsedKnown: true, VerifiedBytes: bytes}}
			osSample := loadstand.LinuxProcessSample{ReadElapsedNS: int64(i+1) * 200, ReadElapsedKnown: true, ExecutableReadBrackets: afterPair}
			o := loadstand.Observed(osSample, nil)
			row := loadStandMemberObservation{At: 500 * time.Millisecond, Node: ids[i], Sample: loadstand.MemberSample{Incarnation: loadstand.Incarnation{Ordinal: 1, StartedAt: time.Unix(int64(11+i), 0)}, LinuxBeforeElapsedNS: int64(i+1) * 100, LinuxBeforeElapsedKnown: true, LinuxBeforeExecutableReadBrackets: beforePair, LinuxProcess: &o}}
			current = append(current, row)
			previous := row
			previous.At = 0
			previous.Sample.LinuxBeforeElapsedNS = 10000
			previous.Sample.LinuxBeforeExecutableReadBrackets.First.ElapsedNS = 999
			osSample.ReadElapsedNS = 20000
			osSample.ExecutableReadBrackets.First.ElapsedNS = 888
			previousOS := loadstand.Observed(osSample, nil)
			previous.Sample.LinuxProcess = &previousOS
			prior = append(prior, previous)
			heap := uint64(100)
			r.RuntimeChecks = append(r.RuntimeChecks, loadStandRuntimeCheck{At: row.At, Group: uint64(i + 1), Members: []loadstand.MemberID{row.Node}, Provider: row.Node, ProviderIncarnation: row.Sample.Incarnation, RuntimeReadAt: time.Unix(int64(21+i), 0), FloorBytes: 1, HeapObjectsBytes: &heap, Verdict: "pass"})
		}
		r.Samples = append(prior, current...)
		return r
	}

	t.Run("completed-without-runtime-failure", func(t *testing.T) {
		r := fixture()
		text := loadStandLinuxRetainedFailureSummary(r)
		for _, want := range []string{"sampler_read_brackets_v1=unique action_index=2 sweep_index=1 rows=4", "before_ns=100 after_ns=200", "before_ns=400 after_ns=800", "before_sum_ns=1000 after_sum_ns=2000 combined_sum_ns=3000", "first_regular_failed_runtime_check=UNKNOWN", "elf_before={first_ns=1 first_bytes=20709639 last_ns=2 last_bytes=20709639 sum_ns=3 contained=true uncovered_ns=97}", "elf_before_sum_ns=60 elf_after_sum_ns=76 elf_combined_sum_ns=136"} {
			if !strings.Contains(text, want) {
				t.Fatal("completed exact sweep missing", want, text)
			}
		}
		if strings.Count(text, " read_bracket[") != 4 {
			t.Fatal("completed current sweep not four rows", text)
		}
		r.RuntimeChecks = nil
		if withoutChecks := loadStandLinuxRetainedFailureSummary(r); withoutChecks != text {
			t.Fatal("read bracket depends on runtime check failure", withoutChecks, text)
		}
	})
	t.Run("ambiguous-edges-and-stops", func(t *testing.T) {
		for _, kind := range []string{"missing", "duplicate", "wrong-action", "same-time-action", "bounds", "overlap", "overbound", "duplicate-node", "reused-node", "prestop", "sibling", "outside-regular", "wrong-at", "missing-incarnation"} {
			r := fixture()
			switch kind {
			case "missing":
				r.TrafficSweeps = nil
			case "duplicate":
				r.TrafficSweeps = append(r.TrafficSweeps, r.TrafficSweeps[1])
			case "wrong-action":
				r.TrafficSweeps[1].CompletedActions = 1
			case "same-time-action":
				r.Actions[0] = r.Actions[2]
			case "bounds":
				r.TrafficSweeps[1].SampleBegin = int(^uint(0) >> 1)
			case "overlap":
				r.TrafficSweeps[1].SampleBegin = 3
			case "overbound":
				r.TrafficSweeps[1].SampleCount = 5
			case "duplicate-node":
				r.Samples[5] = r.Samples[4]
			case "reused-node":
				r.Samples[5].Node = r.Samples[4].Node
			case "prestop":
				r.Samples[5].PreStop = true
			case "sibling":
				r.Samples[5].PreStopOf = "member-0"
			case "outside-regular":
				r.Samples = append(r.Samples, r.Samples[4])
			case "wrong-at":
				r.Samples[5].At++
			case "missing-incarnation":
				r.Samples[5].Sample.Incarnation.StartedAt = time.Time{}
			}
			text := loadStandLinuxRetainedFailureSummary(r)
			if !strings.Contains(text, "sampler_read_brackets_v1=UNKNOWN") || strings.Contains(text, "read_bracket[0]=") {
				t.Fatal("ambiguous elapsed join", kind, text)
			}
		}
	})
	t.Run("zero-unavailable-and-overflow", func(t *testing.T) {
		r := fixture()
		r.Samples[4].Sample.LinuxBeforeElapsedNS = 0
		r.Samples[4].Sample.LinuxProcess.Value.ReadElapsedNS = 0
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "before_ns=0 after_ns=0") {
			t.Fatal("known zero lost", text)
		}
		r.Samples[4].Sample.LinuxBeforeElapsedKnown = false
		r.Samples[5].Sample.LinuxProcess.Value.ReadElapsedNS = -1
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "before_ns=UNKNOWN") || !strings.Contains(text, "after_ns=UNKNOWN") || !strings.Contains(text, "combined_sum_ns=UNKNOWN") {
			t.Fatal("missing metric became zero", text)
		}
		r = fixture()
		r.Samples[4].Sample.LinuxBeforeElapsedNS = 1<<63 - 2
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "before_sum_ns=UNKNOWN") || !strings.Contains(text, "combined_sum_ns=UNKNOWN") {
			t.Fatal("total overflow", text)
		}
		r = fixture()
		r.Samples[4].Sample.LinuxProcess.Value.ReadElapsedNS = 1<<63 - 2
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "before_sum_ns=1000 after_sum_ns=UNKNOWN combined_sum_ns=UNKNOWN") {
			t.Fatal("AFTER sum overflow", text)
		}
		r = fixture()
		for i := range r.Samples {
			r.Samples[i].Sample.LinuxBeforeElapsedNS = 0
			r.Samples[i].Sample.LinuxProcess.Value.ReadElapsedNS = 0
		}
		r.Samples[4].Sample.LinuxBeforeElapsedNS = 1<<63 - 2
		r.Samples[4].Sample.LinuxProcess.Value.ReadElapsedNS = 2
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "before_sum_ns=9223372036854775806 after_sum_ns=2 combined_sum_ns=UNKNOWN") {
			t.Fatal("combined-only overflow", text)
		}
		r = fixture()
		r.Samples[4].Sample.LinuxBeforeExecutableReadBrackets.First.ElapsedKnown = false
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "elf_before={first_ns=UNKNOWN first_bytes=20709639") || !strings.Contains(text, "elf_before_sum_ns=UNKNOWN elf_after_sum_ns=76 elf_combined_sum_ns=UNKNOWN") {
			t.Fatal("unknown elapsed lost valid bytes", text)
		}
		r = fixture()
		r.Samples[4].Sample.LinuxProcess.Value.ExecutableReadBrackets.First.ElapsedNS = 1<<63 - 2
		r.Samples[4].Sample.LinuxProcess.Value.ExecutableReadBrackets.Last.ElapsedNS = 0
		r.Samples[4].Sample.LinuxProcess.Value.ReadElapsedNS = 1<<63 - 2
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "elf_before_sum_ns=60 elf_after_sum_ns=UNKNOWN elf_combined_sum_ns=UNKNOWN") {
			t.Fatal("stream AFTER side overflow", text)
		}
		r = fixture()
		for i := 4; i < 8; i++ {
			r.Samples[i].Sample.LinuxBeforeExecutableReadBrackets.First.ElapsedNS = 0
			r.Samples[i].Sample.LinuxBeforeExecutableReadBrackets.Last.ElapsedNS = 0
			r.Samples[i].Sample.LinuxProcess.Value.ExecutableReadBrackets.First.ElapsedNS = 0
			r.Samples[i].Sample.LinuxProcess.Value.ExecutableReadBrackets.Last.ElapsedNS = 0
		}
		r.Samples[4].Sample.LinuxBeforeExecutableReadBrackets.First.ElapsedNS = 1<<63 - 2
		r.Samples[4].Sample.LinuxBeforeElapsedNS = 1<<63 - 2
		r.Samples[4].Sample.LinuxProcess.Value.ExecutableReadBrackets.First.ElapsedNS = 2
		r.Samples[4].Sample.LinuxProcess.Value.ReadElapsedNS = 2
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "elf_before_sum_ns=9223372036854775806 elf_after_sum_ns=2 elf_combined_sum_ns=UNKNOWN") {
			t.Fatal("stream combined-only overflow", text)
		}
	})
	t.Run("bounded-labels-and-cleanup", func(t *testing.T) {
		r := fixture()
		r.Samples[4].Node = loadstand.MemberID(strings.Repeat("\x00", 4096))
		r.Samples[4].Sample.LinuxBeforeExecutableReadBrackets.First.VerifiedBytes = 128 << 20
		r.Samples[4].Sample.LinuxBeforeExecutableReadBrackets.First.ElapsedNS = 1<<63 - 2
		r.Samples[4].Sample.LinuxBeforeExecutableReadBrackets.Last.ElapsedNS = 0
		r.Samples[4].Sample.LinuxBeforeElapsedNS = 1<<63 - 2
		text := loadStandLinuxRetainedFailureSummary(r)
		if len(text) > 4<<10 || strings.Count(text, " read_bracket[") > 4 {
			t.Fatal("elapsed output bound", len(text), text)
		}
		r = fixture()
		cleanup := r.Samples[4]
		cleanup.PreStopOf = "member-0"
		r.Samples = append(r.Samples, cleanup)
		if text := loadStandLinuxReadBracketSummary(r); !strings.Contains(text, "sampler_read_brackets_v1=unique") {
			t.Fatal("cleanup replaced exact completed edge", text)
		}
	})
}

func TestLoadStandLinuxExecutableReadBrackets(t *testing.T) {
	pair := func(a, b int64) loadstand.ProcessExecutableReadBrackets {
		return loadstand.ProcessExecutableReadBrackets{First: loadstand.ProcessExecutableReadBracket{ElapsedNS: a, ElapsedKnown: true, VerifiedBytes: 4}, Last: loadstand.ProcessExecutableReadBracket{ElapsedNS: b, ElapsedKnown: true, VerifiedBytes: 8}}
	}
	for _, tc := range []struct {
		name       string
		value      loadstand.ProcessExecutableReadBrackets
		outer      int64
		outerKnown bool
		sum        int64
		known      bool
		want       string
	}{
		{"zero", pair(0, 0), 0, true, 0, true, "sum_ns=0 contained=true uncovered_ns=0"},
		{"positive", pair(2, 3), 7, true, 5, true, "sum_ns=5 contained=true uncovered_ns=2"},
		{"outer-unavailable", pair(2, 3), 0, false, 5, true, "sum_ns=5 contained=UNKNOWN uncovered_ns=UNKNOWN"},
		{"outside-read-bracket", pair(2, 3), 4, true, 5, true, "sum_ns=5 contained=UNKNOWN uncovered_ns=UNKNOWN"},
		{"negative", pair(-1, 3), 7, true, 3, false, "first_ns=UNKNOWN"},
		{"saturated", pair(1<<63-1, 3), 7, true, 3, false, "sum_ns=UNKNOWN"},
		{"overflow", pair(1<<63-2, 3), 1<<63 - 2, true, 1<<63 - 2, false, "sum_ns=UNKNOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, total, known := loadStandLinuxExecutableReadBracketSummary(tc.value, tc.outer, tc.outerKnown)
			if total != tc.sum || known != tc.known || !strings.Contains(text, tc.want) {
				t.Fatal("stream diagnostic", text, total, known)
			}
		})
	}
	t.Run("missing-and-malformed", func(t *testing.T) {
		for _, kind := range []string{"missing-first", "missing-last", "invalid-bytes", "oversized-bytes", "missing-mono"} {
			value := pair(2, 3)
			switch kind {
			case "missing-first":
				value.First = loadstand.ProcessExecutableReadBracket{}
			case "missing-last":
				value.Last = loadstand.ProcessExecutableReadBracket{}
			case "invalid-bytes":
				value.First.VerifiedBytes = -1
			case "oversized-bytes":
				value.First.VerifiedBytes = 1 << 30
			case "missing-mono":
				value.First.ElapsedKnown = false
			}
			text, _, known := loadStandLinuxExecutableReadBracketSummary(value, 7, true)
			if known || !strings.Contains(text, "sum_ns=UNKNOWN") {
				t.Fatal("invalid stream metric became measured", kind, text)
			}
		}
	})
}

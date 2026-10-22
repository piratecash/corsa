//go:build linux

package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/netip"
	"os"
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
		t.Fatal("actual planned Full/SDK executor failed", err)
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

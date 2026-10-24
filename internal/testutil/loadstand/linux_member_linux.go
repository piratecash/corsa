//go:build linux

package loadstand

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"golang.org/x/sys/unix"
)

// Each Member is driven by the existing single executor. The group owns only
// its plan's network; original audited profile parents remain caller-owned.
type LinuxMemberSpec struct {
	ID          MemberID
	Role        MemberRole
	Config      ChildConfig
	Credentials *Credentials
	Parent      *AuditedProfileParent
	UID         ProcessUID
	GID         uint32
}
type LinuxMemberBinaries struct {
	Helper, Full, SDK, NFT             string
	HelperSHA, FullSHA, SDKSHA, NFTSHA ExecutableSHA256
}
type LinuxMemberGroup struct {
	membership  *linuxMemberMembership
	plan        plannedNetworkPlan
	binaries    LinuxMemberBinaries
	setupCalled bool
	probes      LinuxIsolationProbes
	network     *plannedNetwork
	backend     *linuxPlannedNetworkBackend
	members     []Member
	owners      []*linuxMember
}
type linuxMember struct {
	id                          MemberID
	role                        MemberRole
	config                      ChildConfig
	planned                     PlannedChildConfig
	physical                    plannedNetworkMember
	group                       *LinuxMemberGroup
	spec                        LinuxFixtureSpec
	parent                      *AuditedProfileParent
	cleanup                     time.Duration
	fixture                     *LinuxFixture
	previousFixture             *LinuxFixture
	originalNamespace           *OwnedNetNS
	originalNamespaceIdentity   linuxNamespaceIdentity
	attempted                   bool
	current                     *StartReceipt
	previous                    PublicIdentity
	ended                       []EndedIncarnation
	highWater                   TransportHighWater
	profileDevice, profileInode uint64
	stopWatch                   chan struct{}
	watchDone                   chan struct{}
}
type linuxSDKMember struct{ *linuxMember }
type linuxFullMember struct{ *linuxMember }

func PrepareLinuxMemberGroup(ctx context.Context, prefix netip.Prefix, binaries LinuxMemberBinaries, specs []LinuxMemberSpec, cleanup time.Duration, probes LinuxIsolationProbes) (*LinuxMemberGroup, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrLinuxOwnerBound
	}
	if cleanup <= 0 || !probes.valid() || len(specs) == 0 || !plannedApprovedPrefix(prefix) {
		return nil, ErrInvalidConfiguration
	}
	nonce, err := newLaunchBinding(ChildConfig{}, string(binaries.HelperSHA))
	if err != nil {
		return nil, err
	}
	plan := plannedNetworkPlan{nonce: nonce.nonce, bridge: "br" + nonce.nonce.String()[:8], prefix: prefix}
	projected := make([]PlannedChildConfig, len(specs))
	copied := make([]LinuxMemberSpec, len(specs))
	for i, spec := range specs {
		if spec.Parent == nil || !spec.Credentials.valid() || spec.UID == 0 || spec.GID == 0 || spec.Config.DataDir != spec.Parent.MemberDirectory() {
			return nil, ErrInvalidConfiguration
		}
		address, e := netip.ParseAddrPort(string(spec.Config.Listen))
		if e != nil {
			return nil, e
		}
		for _, old := range specs[:i] {
			if old.ID == spec.ID || old.Parent == spec.Parent {
				return nil, ErrInvalidConfiguration
			}
		}
		p := PlannedChildConfig{ID: spec.ID, Role: spec.Role, Prefix: prefix, Peers: []PlannedChildPeer{{ID: spec.ID, Role: spec.Role, Endpoint: domain.PeerAddress(spec.Config.Listen)}}}
		for _, seed := range spec.Config.Bootstrap {
			found := false
			for _, candidate := range specs {
				if candidate.ID != spec.ID && candidate.Role == MemberFull && domain.PeerAddress(candidate.Config.Listen) == seed {
					p.Peers = append(p.Peers, PlannedChildPeer{ID: candidate.ID, Role: candidate.Role, Endpoint: seed})
					found = true
				}
			}
			if !found {
				return nil, ErrInvalidConfiguration
			}
		}
		config, p, e := copyPlannedChild(spec.Config, p)
		if e != nil {
			return nil, e
		}
		spec.Config = config
		copied[i] = spec
		projected[i] = p
		plan.members = append(plan.members, plannedNetworkMember{id: spec.ID, role: spec.Role, endpoint: domain.PeerAddress(spec.Config.Listen), hostLink: "h" + fmtMemberIndex(i), peerLink: "p" + fmtMemberIndex(i), address: address.Addr()})
	}
	// All input tuples and exact bootstrap roles are checked before kernel work.
	plan, err = copyPlannedNetwork(plan)
	if err != nil {
		return nil, plannedNetworkPhaseError("prepare-plan-copy", err)
	}
	group := &LinuxMemberGroup{plan: plan, binaries: binaries, probes: probes}
	for i, spec := range copied {
		target, hash := binaries.SDK, binaries.SDKSHA
		if spec.Role == MemberFull {
			target, hash = binaries.Full, binaries.FullSHA
		}
		owner := &linuxMember{id: spec.ID, role: spec.Role, config: spec.Config, planned: projected[i], physical: plan.members[i], parent: spec.Parent, cleanup: cleanup, group: group, spec: LinuxFixtureSpec{HelperBinary: binaries.Helper, HelperSHA256: binaries.HelperSHA, TargetBinary: target, TargetSHA256: hash, Config: spec.Config, Credentials: spec.Credentials, UID: spec.UID, GID: spec.GID}}
		group.owners = append(group.owners, owner)
		if spec.Role == MemberFull {
			group.members = append(group.members, &linuxFullMember{owner})
		} else {
			group.members = append(group.members, &linuxSDKMember{owner})
		}
	}
	return group, nil
}
func CreateLinuxMemberGroup(ctx context.Context, prefix netip.Prefix, binaries LinuxMemberBinaries, specs []LinuxMemberSpec, cleanup time.Duration, probes LinuxIsolationProbes) (*LinuxMemberGroup, error) {
	group, err := PrepareLinuxMemberGroup(ctx, prefix, binaries, specs, cleanup, probes)
	if err != nil {
		return nil, err
	}
	return group, group.Setup(ctx)
}

// The existing executor compiles this group's exact actual Member map before
// Setup. This is only the effectful half of the same concrete constructor.
func (g *LinuxMemberGroup) Setup(ctx context.Context) error {
	if g == nil || g.setupCalled || g.network != nil {
		return plannedNetworkPhaseError("setup-group-state", ErrMemberState)
	}
	g.setupCalled = true
	binaries, plan, probes := g.binaries, g.plan, g.probes
	for _, target := range []struct {
		path string
		hash ExecutableSHA256
	}{{binaries.Helper, binaries.HelperSHA}, {binaries.Full, binaries.FullSHA}, {binaries.SDK, binaries.SDKSHA}} {
		if err := auditLinuxExecutable(ctx, target.path, target.hash); err != nil {
			return plannedNetworkPhaseError("setup-artifact-audit", err)
		}
	}
	membership, err := newLinuxMemberMembership(ctx, plan)
	if err != nil {
		return plannedNetworkPhaseError("setup-membership", err)
	}
	g.membership = membership
	network, err := newLinuxPlannedNetwork(ctx, plan, binaries.NFT, binaries.NFTSHA)
	if err != nil {
		return plannedNetworkPhaseError("setup-network-constructor", err)
	}
	g.network = network
	g.backend = network.backend.(*linuxPlannedNetworkBackend)
	if err = g.backend.originalUnreachable(ctx, probes); err != nil {
		return plannedNetworkPhaseError("setup-original-preprobe", err)
	}
	if err = network.setup(ctx); err != nil {
		return plannedNetworkPhaseError("setup-network", err)
	}
	for _, member := range plan.members {
		row := g.backend.resource(plannedNamespace, string(member.id))
		if row == nil || row.namespace == nil {
			return plannedNetworkPhaseError(fmt.Sprintf("setup-namespace-owner member=%s", member.id), ErrMemberState)
		}
		if err = row.namespace.plannedProbe(ctx, probes); err != nil {
			return fmt.Errorf("planned-network stage=setup-namespace-postprobe member=%s: %w", member.id, err)
		}
	}
	return nil
}
func (g *LinuxMemberGroup) Members() map[MemberID]Member {
	out := make(map[MemberID]Member, len(g.members))
	for _, member := range g.members {
		out[member.ID()] = member
	}
	return out
}
func (g *LinuxMemberGroup) Close(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if g.network == nil || g.network.closed {
		return plannedNetworkPhaseError("close-membership", g.membership.close())
	}
	var result error
	for _, member := range g.owners {
		if member.Owned() {
			_, err := member.Stop(ctx)
			result = errors.Join(result, plannedNetworkPhaseError(fmt.Sprintf("close-member member=%s", member.id), err))
		}
	}
	for _, member := range g.owners {
		if member.Owned() {
			return errors.Join(result, ErrStopUnconfirmed)
		}
	}
	postErr := g.backend.originalUnreachable(ctx, g.probes)
	return errors.Join(result, plannedNetworkPhaseError("close-original-postprobe", postErr), plannedNetworkPhaseError("close-network", g.network.close(ctx)), plannedNetworkPhaseError("close-membership", g.membership.close()))
}
func (m *linuxMember) ID() MemberID     { return m.id }
func (m *linuxMember) Role() MemberRole { return m.role }
func (m *linuxMember) Endpoint() (domain.PeerAddress, bool) {
	if m.role != MemberFull {
		return "", false
	}
	return domain.PeerAddress(m.config.Listen), true
}
func (m *linuxMember) Bootstrap() []domain.PeerAddress {
	return append([]domain.PeerAddress(nil), m.config.Bootstrap...)
}
func (m *linuxMember) Owned() bool { return m.fixture != nil || m.attempted }
func (m *linuxMember) Done() <-chan struct{} {
	if m.fixture == nil {
		return nil
	}
	return m.fixture.helperWaitDone
}
func (m *linuxMember) Start(lifetime, readiness context.Context) (StartReceipt, error) {
	if m.Owned() || lifetime.Err() != nil || m.group.network == nil || !m.group.network.ready {
		return StartReceipt{}, m.startFailure("precondition", ErrMemberState)
	}
	m.attempted = true
	m.current = nil
	m.highWater = TransportHighWater{}
	row := m.group.backend.resource(plannedNamespace, string(m.id))
	if row == nil || row.namespace == nil {
		return StartReceipt{}, m.startFailure("namespace-owner", ErrMemberState)
	}
	row.namespace.mu.Lock()
	namespaceIdentity := row.namespace.identity
	row.namespace.mu.Unlock()
	if m.originalNamespace == nil {
		m.originalNamespace = row.namespace
		m.originalNamespaceIdentity = namespaceIdentity
	} else if m.originalNamespace != row.namespace || m.originalNamespaceIdentity != namespaceIdentity {
		return StartReceipt{}, m.startFailure("namespace-identity", ErrMemberState)
	}
	ordinal := IncarnationOrdinal(len(m.ended) + 1)
	binding, err := newPlannedLaunchBinding(m.config, m.planned, m.spec.TargetSHA256, ordinal)
	if err != nil {
		return StartReceipt{}, m.startFailure("launch-binding", err)
	}
	m.spec.planned = &linuxPlannedLaunch{network: m.group.network, member: m.physical, plan: m.planned, ordinal: ordinal, binding: binding, profileDevice: m.profileDevice, profileInode: m.profileInode, previous: m.previousFixture, membership: m.group.membership}
	fixture, err := CreateLinuxFixture(readiness, m.spec, m.parent, row.namespace)
	m.fixture = fixture
	if err != nil {
		return StartReceipt{}, m.startFailure("create-fixture", err)
	}
	m.stopWatch = make(chan struct{})
	m.watchDone = make(chan struct{})
	go m.watchLifetime(lifetime, fixture, m.stopWatch, m.watchDone)
	if err = fixture.Start(readiness); err != nil {
		return StartReceipt{}, m.startFailure("fixture-start", err)
	}
	receipt, err := fixture.completePlannedMemberStart(readiness)
	if err != nil {
		return StartReceipt{}, m.startFailure("member-bind", err)
	}
	if !m.previous.Identity.IsZero() && m.previous != receipt.Public {
		return StartReceipt{}, m.startFailure("receipt-identity", ErrMemberState)
	}
	m.parent.mu.Lock()
	var profile unix.Stat_t
	statErr := unix.Fstatat(int(m.parent.file.Fd()), "member-1", &profile, unix.AT_SYMLINK_NOFOLLOW)
	m.parent.mu.Unlock()
	if statErr != nil || profile.Uid != uint32(m.spec.UID) || profile.Mode&07777 != 0700 || profile.Mode&unix.S_IFMT != unix.S_IFDIR {
		return StartReceipt{}, m.startFailure("profile-stat", ErrMemberState)
	}
	if m.profileInode != 0 && (m.profileDevice != uint64(profile.Dev) || m.profileInode != profile.Ino) {
		return StartReceipt{}, m.startFailure("profile-identity", ErrMemberState)
	}
	m.profileDevice, m.profileInode = uint64(profile.Dev), profile.Ino
	m.previous = receipt.Public
	m.current = &receipt
	m.highWater = NewTransportHighWater(RuntimeBinding{Identity: receipt.Public.Identity, StartedAt: receipt.Incarnation.StartedAt})
	return receipt, nil
}

// startFailure reads only already retained owner state. The observation can be
// later than the failure decision and must not be treated as its causal origin.
func (m *linuxMember) startFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	fixture := m.fixture
	if fixture == nil {
		return fmt.Errorf("planned-member-start member=%q stage=%s fixture=none: %w", m.id, stage, err)
	}
	fixture.mu.Lock()
	initResult, bindResult := fixture.initResult, fixture.memberBindDiagnostic
	startCalled, running, stopping, workBusy := fixture.startCalled, fixture.running, fixture.stopping, fixture.workBusy
	refused, poisoned := fixture.beforeTargetRefused, fixture.correlation.poisoned
	helperState, helperJoined := fixture.helperState, fixture.helperJoined
	helperKnown, helperExit := fixture.helperWaitKnown, fixture.helperWait
	targetKnown, targetExit := fixture.targetKnown, fixture.targetExit
	fixture.mu.Unlock()
	return fmt.Errorf("planned-member-start member=%q stage=%s observation=after_failure_decision start_called=%t running=%t stopping=%t work_busy=%t before_target_refused=%t poisoned=%t init_known=%t init_error=%q bind_known=%t bind_error=%q bind_context=%q helper_state=%d helper_joined=%t helper_wait_known=%t helper_exit=%q target_known=%t target_exit=%q: %w", m.id, stage, startCalled, running, stopping, workBusy, refused, poisoned, initResult.known, plannedMemberDiagnosticError(initResult.known, initResult.err), bindResult.known, plannedMemberDiagnosticError(bindResult.known, bindResult.err), plannedMemberBindContext(bindResult), helperState, helperJoined, helperKnown, plannedMemberDiagnosticExit(helperKnown, helperExit), targetKnown, plannedMemberDiagnosticExit(targetKnown, targetExit), err)
}

// Text only: diagnostic errors never extend the returned error's Is/As chain.
// Error() and formatting run outside every fixture/profile/namespace mutex.
func plannedMemberDiagnosticError(known bool, err error) string {
	if !known {
		return "UNKNOWN"
	}
	if err == nil {
		return "<nil>"
	}
	text := err.Error()
	if len(text) > 2048 {
		return text[:2045] + "..."
	}
	return text
}

func plannedMemberBindContext(result linuxMemberBindDiagnostic) string {
	if !result.known {
		return "UNKNOWN"
	}
	helper := "UNKNOWN"
	if result.helperKnown {
		helper = linuxBindHelperContext(result.helper)
	}
	replyCode := "UNKNOWN"
	if result.replyKnown {
		replyCode = fmt.Sprint(result.replyCode)
	}
	return linuxBindDiagnosticText(fmt.Sprintf("phase=%s reply_known=%t reply_code=%q mismatch_known=%t mismatch_bits=%#x public_checked=%t helper_known=%t helper=%q", result.phase, result.replyKnown, replyCode, result.replyKnown, result.mismatch, result.publicChecked, result.helperKnown, helper))
}

func plannedMemberDiagnosticExit(known bool, result ProcessExitResult) string {
	if !known {
		return "UNKNOWN"
	}
	if result.Kind == WaitUnknown {
		return fmt.Sprintf("UNKNOWN(kind=%d wait_failed=%t)", result.Kind, result.WaitFailed)
	}
	return fmt.Sprintf("kind=%d code=%d signal=%d wait_failed=%t", result.Kind, result.Code, result.Signal, result.WaitFailed)
}

func (m *linuxMember) watchLifetime(lifetime context.Context, fixture *LinuxFixture, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	select {
	case <-stop:
		return
	case <-lifetime.Done():
		ctx, cancel := context.WithTimeout(context.WithoutCancel(lifetime), m.cleanup)
		defer cancel()
		_ = fixture.Stop(ctx)
	}
}
func (m *linuxMember) CheckRunning(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.fixture == nil || m.current == nil || m.group.network == nil || !m.group.network.ready {
		return ErrMemberState
	}
	state := m.fixture.Snapshot()
	if state.Poisoned || state.HelperState != LinuxHelperRunning || state.TargetKnown || state.HelperWaitKnown {
		return ErrMemberState
	}
	return nil
}
func (m *linuxMember) Stop(ctx context.Context) (StopReceipt, error) {
	if !m.Owned() {
		return StopReceipt{}, ErrMemberState
	}
	if m.fixture == nil {
		m.attempted = false
		return StopReceipt{Terminated: true}, nil
	} // no child escaped; group retains original network cleanup
	stopErr := m.fixture.Stop(ctx)
	state := m.fixture.Snapshot()
	m.fixture.mu.Lock()
	refused := m.fixture.beforeTargetRefused
	initResult, bindResult := m.fixture.initResult, m.fixture.memberBindDiagnostic
	m.fixture.mu.Unlock()
	if stopErr != nil {
		stopErr = fmt.Errorf("planned-member-stop member=%q observation=post_stop_not_primary_cause helper_state=%d helper_joined=%t helper_wait_known=%t helper_exit=%q target_known=%t target_exit=%q poisoned=%t before_target_refused=%t init_known=%t init_error=%q bind_known=%t bind_error=%q bind_context=%q: %w", m.id, state.HelperState, state.HelperJoined, state.HelperWaitKnown, plannedMemberDiagnosticExit(state.HelperWaitKnown, state.HelperWait), state.TargetKnown, plannedMemberDiagnosticExit(state.TargetKnown, state.TargetExit), state.Poisoned, refused, initResult.known, plannedMemberDiagnosticError(initResult.known, initResult.err), bindResult.known, plannedMemberDiagnosticError(bindResult.known, bindResult.err), plannedMemberBindContext(bindResult), stopErr)
	}
	if !plannedMemberTerminationConfirmed(state, refused, m.current != nil) {
		return StopReceipt{}, errors.Join(stopErr, ErrStopUnconfirmed)
	}
	if m.stopWatch != nil {
		close(m.stopWatch)
		m.stopWatch = nil
	}
	select {
	case <-m.watchDone:
	case <-ctx.Done():
		return StopReceipt{Terminated: true}, errors.Join(stopErr, ctx.Err())
	}
	var ended *EndedIncarnation
	var finalErr error
	if m.current != nil {
		final, err := m.fixture.plannedMemberFinal(*m.current, m.highWater)
		finalErr = err
		m.fixture.mu.Lock()
		disk := m.fixture.memberFinal
		m.fixture.mu.Unlock()
		if disk == nil || !disk.PeersKnown || m.group.membership == nil || disk.MembershipSHA256 != m.group.membership.descriptor.SHA256 || disk.PeerCount < 0 || disk.PeerCount > m.group.membership.descriptor.Count {
			finalErr = errors.Join(finalErr, ErrInvalidSample)
		}
		e := EndedIncarnation{Incarnation: m.current.Incarnation, Finality: FinalUnavailable, Final: Observation[domain.TransportTrafficStats]{State: ObservationUnavailable}}
		if finalErr == nil && final.Finality == FinalExact {
			e.Finality = FinalExact
			e.Final = Observed(final.Traffic, nil)
		}
		ended = &e
	}
	postErr := m.group.backend.memberPostflight(ctx, m.physical)
	if ns := m.group.backend.resource(plannedNamespace, string(m.id)); ns != nil && ns.namespace != nil {
		postErr = errors.Join(postErr, ns.namespace.plannedProbe(ctx, m.group.probes))
	} else {
		postErr = errors.Join(postErr, ErrMemberState)
	}
	if postErr != nil {
		m.group.network.ready = false
	}
	closeErr := m.fixture.Close(ctx)
	m.fixture.mu.Lock()
	released := m.fixture.plannedReleased
	m.fixture.mu.Unlock()
	if !released || m.fixture.parent.file != nil {
		return StopReceipt{Terminated: true}, errors.Join(stopErr, finalErr, postErr, closeErr, ErrStopUnconfirmed)
	}
	stopErr = errors.Join(stopErr, closeErr)
	m.previousFixture = m.fixture
	m.fixture = nil
	m.attempted = false
	m.current = nil
	if ended != nil {
		m.ended = append(m.ended, ended.Clone())
		copy := ended.Clone()
		return StopReceipt{Terminated: true, Ended: &copy}, errors.Join(stopErr, finalErr, postErr)
	}
	return StopReceipt{Terminated: true}, errors.Join(stopErr, postErr)
}
func (m *linuxMember) EndedIncarnations() []EndedIncarnation {
	out := make([]EndedIncarnation, len(m.ended))
	for i, e := range m.ended {
		out[i] = e.Clone()
	}
	return out
}

var _ Member = (*linuxSDKMember)(nil)
var _ Member = (*linuxFullMember)(nil)

func fmtMemberIndex(index int) string { return strconv.Itoa(index) }

func plannedMemberTerminationConfirmed(state LinuxFixtureResult, refused, successful bool) bool {
	if (!state.HelperWaitKnown && state.HelperState != LinuxHelperNotStarted) || (!state.HelperJoined && !refused) {
		return false
	}
	return state.TargetKnown || !successful && refused
}

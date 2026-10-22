//go:build linux

package loadstand

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func linuxMemberTestSpecs(t *testing.T) ([]LinuxMemberSpec, LinuxIsolationProbes) {
	t.Helper()
	probes := LinuxIsolationProbes{ProductionBootstrap: netip.MustParseAddrPort("192.0.2.1:1"), FallbackPublic: netip.MustParseAddrPort("198.51.100.1:1"), PublicIPv6: netip.MustParseAddrPort("[2001:db8::1]:1")}
	specs := make([]LinuxMemberSpec, 2)
	for i := range specs {
		credentials, err := NewCredentials()
		if err != nil {
			t.Fatal(err)
		}
		parent := &AuditedProfileParent{path: []string{"/profiles/first", "/profiles/second"}[i]}
		specs[i] = LinuxMemberSpec{ID: []MemberID{"full-a", "sdk-b"}[i], Role: []MemberRole{MemberFull, MemberSDK}[i], Parent: parent, Credentials: credentials, UID: 65532, GID: 65532, Config: ChildConfig{DataDir: parent.MemberDirectory(), Listen: []domain.ListenAddress{"240.0.0.1:42050", "240.1.0.1:42050"}[i], RPC: "127.0.0.1:42051", Listener: i == 0, Procs: 1}}
	}
	// Metadata constructor requires a nonempty exact original Full bootstrap;
	// use two Full members here. SDK projection is tested below with a third.
	specs[1].Role = MemberFull
	specs[1].Config.Listener = true
	specs[0].Config.Bootstrap = []domain.PeerAddress{domain.PeerAddress(specs[1].Config.Listen)}
	specs[1].Config.Bootstrap = []domain.PeerAddress{domain.PeerAddress(specs[0].Config.Listen)}
	return specs, probes
}
func TestLinuxMemberPrepareCopiesWholeOriginalPlanWithoutEffects(t *testing.T) {
	specs, probes := linuxMemberTestSpecs(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	group, err := PrepareLinuxMemberGroup(ctx, netip.MustParsePrefix("240.0.0.0/4"), LinuxMemberBinaries{}, specs, time.Second, probes)
	if err != nil {
		t.Fatal(err)
	}
	if group.network != nil || group.backend != nil || group.setupCalled || len(group.Members()) != 2 {
		t.Fatal("prepare acquired network authority")
	}
	original := group.owners[0].Bootstrap()[0]
	specs[0].Config.Bootstrap[0] = "240.9.0.1:1"
	copy := group.owners[0].Bootstrap()
	copy[0] = "240.8.0.1:1"
	if group.owners[0].Bootstrap()[0] != original {
		t.Fatal("bootstrap alias escaped")
	}
	for _, member := range group.Members() {
		if member.Owned() || len(member.EndedIncarnations()) != 0 || member.Done() != nil {
			t.Fatal("metadata minted a successful incarnation")
		}
	}
	if err = group.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestLinuxMemberPrepareRefusesForeignGraphAndProfileAliases(t *testing.T) {
	for _, reason := range []string{"unknown-bootstrap", "sdk-bootstrap", "shared-parent", "duplicate-id", "outside240", "sdk-listener"} {
		t.Run(reason, func(t *testing.T) {
			specs, probes := linuxMemberTestSpecs(t)
			switch reason {
			case "unknown-bootstrap":
				specs[0].Config.Bootstrap[0] = "240.9.0.1:42050"
			case "sdk-bootstrap":
				specs[1].Role = MemberSDK
				specs[1].Config.Listener = false
			case "shared-parent":
				specs[1].Parent = specs[0].Parent
				specs[1].Config.DataDir = specs[0].Config.DataDir
			case "duplicate-id":
				specs[1].ID = specs[0].ID
			case "outside240":
				specs[1].Config.Listen = "10.0.0.1:42050"
			case "sdk-listener":
				specs[1].Role = MemberSDK
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			group, err := PrepareLinuxMemberGroup(ctx, netip.MustParsePrefix("240.0.0.0/4"), LinuxMemberBinaries{}, specs, time.Second, probes)
			if err == nil || group != nil {
				t.Fatal("foreign/invalid metadata accepted")
			}
		})
	}
}
func TestLinuxMemberBindUsesFreshWorkOwnerNotHistoricalObservation(t *testing.T) {
	owner, _, facts := linuxRuntimeTestWire(t)
	owner.runtimeObservation = LinuxOwnedRuntimeObservation{known: true, nonce: owner.correlation.nonce, ordinal: 1, facts: facts}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := owner.completePlannedMemberStart(ctx); !errors.Is(err, ErrMemberState) {
		t.Fatal("old observation became Member receipt", err)
	}
}
func TestLinuxMemberBindRequiresExactOriginalReply(t *testing.T) {
	for _, reason := range []string{"valid", "nonce", "request", "ordinal", "provenance", "signature", "zero-start"} {
		t.Run(reason, func(t *testing.T) {
			owner, work, facts := linuxRuntimeTestWire(t)
			owner.spec.planned = &linuxPlannedLaunch{ordinal: 2}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			result := make(chan linuxMemberStartOutcome, 1)
			go func() {
				receipt, err := owner.completePlannedMemberStart(ctx)
				result <- linuxMemberStartOutcome{receipt, err}
			}()
			request, err := work.read(time.Now().Add(time.Second))
			if err != nil || request.kind != linuxMemberBind {
				t.Fatal("missing original bind request", err)
			}
			body, err := decodeLinuxBody[linuxMemberBindRequest](request)
			if err != nil || body.Ordinal != 2 || body.Nonce != owner.correlation.nonce {
				t.Fatal("wrong bind input", err)
			}
			reply := linuxMemberBindReply{Nonce: body.Nonce, Code: linuxResultReady, Provenance: facts.Provenance, Receipt: StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: body.Ordinal, StartedAt: facts.StartedAt}}}
			switch reason {
			case "nonce":
				reply.Nonce = LaunchNonce{9}
			case "request":
				request.id++
			case "ordinal":
				reply.Receipt.Incarnation.Ordinal++
			case "provenance":
				reply.Provenance.Incarnation.StartTicks++
			case "signature":
				reply.Receipt.Public.BoxSignature = "invalid"
			case "zero-start":
				reply.Receipt.Incarnation.StartedAt = time.Time{}
			}
			raw, err := encodeLinuxPacket(linuxMemberBindResult, request.id, 1, reply)
			if err != nil {
				t.Fatal(err)
			}
			if err = work.write(raw, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			got := <-result
			if reason == "valid" {
				if got.err != nil || got.receipt.Public != facts.Public || got.receipt.Incarnation.Ordinal != 2 {
					t.Fatal("original reply refused", got.err)
				}
			} else if got.err == nil || got.receipt != (StartReceipt{}) {
				t.Fatal("foreign/partial receipt published")
			}
		})
	}
}
func TestLinuxMemberFinalRequiresJoinedOriginalAndHighWater(t *testing.T) {
	facts := linuxRuntimeTestFacts(t)
	receipt := StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}}
	traffic := domain.TransportTrafficStats{StartedAt: facts.StartedAt, ReadAt: facts.EndedAt, BytesSent: 9, BytesReceived: 11}
	for _, reason := range []string{"valid", "not-joined", "foreign-start", "foreign-ordinal", "foreign-public", "prior-binding", "rollback", "poison", "unclean", "unknown-finality", "unknown-unavailable-reason", "unavailable"} {
		t.Run(reason, func(t *testing.T) {
			owner := &LinuxFixture{targetExit: ProcessExitResult{Kind: WaitNormal}, helperWait: ProcessExitResult{Kind: WaitNormal}, helperJoined: true, helperWaitKnown: true, targetKnown: true, memberFinal: &linuxMemberFinal{Receipt: receipt, Finality: FinalExact, Traffic: traffic}}
			// Exact finality requires both original normal wait results, not zero values.
			prior := NewTransportHighWater(RuntimeBinding{Identity: receipt.Public.Identity, StartedAt: receipt.Incarnation.StartedAt})
			prior.Observe(traffic, nil)
			switch reason {
			case "not-joined":
				owner.helperJoined = false
			case "foreign-start":
				owner.memberFinal.Receipt.Incarnation.StartedAt = facts.StartedAt.Add(time.Second)
			case "foreign-ordinal":
				owner.memberFinal.Receipt.Incarnation.Ordinal++
			case "foreign-public":
				owner.memberFinal.Receipt.Public = PublicIdentity{}
			case "prior-binding":
				prior.binding.StartedAt = prior.binding.StartedAt.Add(time.Second)
			case "unknown-finality":
				owner.memberFinal.Finality = "invented"
			case "unknown-unavailable-reason":
				owner.memberFinal.Finality = FinalUnavailable
				owner.memberFinal.Reason = "invented"
			case "unavailable":
				owner.memberFinal.Finality = FinalUnavailable
				owner.memberFinal.Reason = FinalReportRejected
			case "rollback":
				owner.memberFinal.Traffic.BytesSent--
			case "poison":
				owner.correlation.poisoned = true
			case "unclean":
				owner.targetExit = ProcessExitResult{Kind: WaitNormal, Code: 1}
			}
			value, err := owner.plannedMemberFinal(receipt, prior)
			if reason == "valid" {
				if err != nil || value.Finality != FinalExact {
					t.Fatal("exact final refused", err)
				}
			} else if reason == "unavailable" {
				if err != nil || value.Finality != FinalUnavailable || value.Reason != FinalReportRejected {
					t.Fatal("honest unavailable reduction refused", err)
				}
			} else if err == nil {
				t.Fatal("unconfirmed/foreign/rolled-back final accepted")
			}
		})
	}
}
func TestLinuxMemberProbeInputsNeverSelectApprovedNetworkOrFallback(t *testing.T) {
	_, valid := linuxMemberTestSpecs(t)
	if !valid.valid() {
		t.Fatal("valid explicit probes refused")
	}
	for _, target := range []string{"240.0.0.1:1", "10.0.0.1:1", "127.0.0.1:1"} {
		copy := valid
		copy.FallbackPublic = netip.MustParseAddrPort(target)
		if copy.valid() {
			t.Fatal("invalid negative probe accepted")
		}
	}
	copy := valid
	copy.PublicIPv6 = netip.MustParseAddrPort("[::ffff:192.0.2.1]:1")
	if copy.valid() {
		t.Fatal("mapped IPv4 accepted as IPv6 test")
	}
}

func TestLinuxMemberFailedStartRetainsCleanupWithoutInventedTargetExit(t *testing.T) {
	refused := LinuxFixtureResult{HelperState: LinuxHelperNotStarted}
	if !plannedMemberTerminationConfirmed(refused, true, false) {
		t.Fatal("confirmed pre-target refusal cannot clean up")
	}
	if plannedMemberTerminationConfirmed(refused, true, true) {
		t.Fatal("successful start accepted without original target exit")
	}
	if plannedMemberTerminationConfirmed(refused, false, false) {
		t.Fatal("unconfirmed refusal dropped ownership")
	}
	started := LinuxFixtureResult{TargetKnown: true, HelperJoined: true, HelperWaitKnown: true}
	if !plannedMemberTerminationConfirmed(started, false, true) {
		t.Fatal("complete original joins refused")
	}
	started.HelperJoined = false
	if plannedMemberTerminationConfirmed(started, false, true) {
		t.Fatal("helper join fabricated")
	}
}

func TestLinuxMemberSDKUsesOrdinaryTrafficWithoutFullPublisherPromotion(t *testing.T) {
	specs, probes := linuxMemberTestSpecs(t)
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	parent := &AuditedProfileParent{path: "/profiles/sdk"}
	specs = append(specs, LinuxMemberSpec{ID: "sdk-c", Role: MemberSDK, Parent: parent, Credentials: credentials, UID: 65532, GID: 65532, Config: ChildConfig{DataDir: parent.MemberDirectory(), Listen: "240.2.0.1:42050", RPC: "127.0.0.1:42051", Bootstrap: []domain.PeerAddress{domain.PeerAddress(specs[0].Config.Listen), domain.PeerAddress(specs[1].Config.Listen)}, Procs: 1}})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	group, err := PrepareLinuxMemberGroup(ctx, netip.MustParsePrefix("240.0.0.0/4"), LinuxMemberBinaries{}, specs, time.Second, probes)
	if err != nil {
		t.Fatal(err)
	}
	sdk := group.Members()["sdk-c"]
	if _, ok := sdk.(UserTraffic); !ok {
		t.Fatal("SDK lost ordinary DM port")
	}
	if _, ok := sdk.(GlobalPublisher); !ok {
		t.Fatal("SDK lost ordinary global port")
	}
	if _, ok := sdk.(GlobalObserver); ok {
		t.Fatal("SDK acquired Full observer role")
	}
	full := group.Members()["full-a"]
	if _, ok := full.(UserTraffic); ok {
		t.Fatal("headless Full became DM recipient/publisher")
	}
	if _, ok := full.(GlobalObserver); !ok {
		t.Fatal("Full lost ordinary replica port")
	}
	if _, err = sdk.Start(ctx, ctx); !errors.Is(err, ErrMemberState) || sdk.Owned() {
		t.Fatal("metadata-only member started without actual prepared network", err)
	}
}
func TestLinuxMemberRestartConsumesOnlyReleasedOriginalNamespace(t *testing.T) {
	for _, reason := range []string{"valid", "unreleased", "copied-predecessor", "foreign-namespace", "foreign-group", "foreign-member", "wrong-ordinal", "wrong-profile", "foreign-credentials", "legacy-public"} {
		t.Run(reason, func(t *testing.T) {
			specs, _ := linuxMemberTestSpecs(t)
			spec := LinuxFixtureSpec{Config: specs[0].Config, UID: specs[0].UID, GID: specs[0].GID, Credentials: specs[0].Credentials, TargetSHA256: "original-target"}
			network := &plannedNetwork{}
			member := plannedNetworkMember{id: specs[0].ID, address: netip.MustParseAddr("240.0.0.1")}
			namespace := newOwnedNetNSState()
			oldPlan := &linuxPlannedLaunch{network: network, member: member, ordinal: 1}
			spec.planned = oldPlan
			previous := &LinuxFixture{spec: spec, namespace: namespace, plannedReleased: true}
			namespace.plannedOwner = previous
			nextPlan := &linuxPlannedLaunch{network: network, member: member, ordinal: 2, previous: previous}
			next := spec
			next.planned = nextPlan
			switch reason {
			case "unreleased":
				previous.plannedReleased = false
			case "copied-predecessor":
				copy := &LinuxFixture{spec: spec, namespace: namespace, plannedReleased: true}
				nextPlan.previous = copy
			case "foreign-namespace":
				other := newOwnedNetNSState()
				other.plannedOwner = previous
				namespace = other
			case "foreign-group":
				nextPlan.network = &plannedNetwork{}
			case "foreign-member":
				nextPlan.member.id = "other"
			case "wrong-ordinal":
				nextPlan.ordinal = 3
			case "wrong-profile":
				next.Config.DataDir += "-other"
			case "foreign-credentials":
				other, err := NewCredentials()
				if err != nil {
					t.Fatal(err)
				}
				next.Credentials = other
			case "legacy-public":
				next.planned = nil
			}
			namespace.mu.Lock()
			allowed := consumePlannedNamespaceReuseLocked(next, namespace)
			namespace.mu.Unlock()
			if reason != "valid" {
				if allowed {
					t.Fatal("foreign/unreleased/public namespace reuse accepted")
				}
				return
			}
			if !allowed || nextPlan.previous.namespace != namespace || next.Config.Listen != spec.Config.Listen || next.Config.DataDir != spec.Config.DataDir {
				t.Fatal("same original namespace/address/profile handoff refused")
			}
			namespace.mu.Lock()
			again := consumePlannedNamespaceReuseLocked(next, namespace)
			namespace.mu.Unlock()
			if again {
				t.Fatal("released predecessor replayed twice")
			}
		})
	}
}

func TestLinuxMemberWorkCompletionReleasesBeforePublication(t *testing.T) {
	for _, operation := range []string{"bind", "receipts"} {
		t.Run(operation, func(t *testing.T) {
			owner, work, facts := linuxRuntimeTestWire(t)
			owner.spec.planned = &linuxPlannedLaunch{ordinal: 1}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			owner.mu.Lock()
			id, err := owner.correlation.allocate()
			owner.workBusy = true
			owner.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			x := linuxSampleExchange{ctx: ctx, id: id, memberResult: make(chan linuxMemberStartOutcome), receiptsResult: make(chan linuxMemberReceiptsOutcome), receiptLimit: 1}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if operation == "bind" {
					owner.bindMemberOnWorkOwner(x)
				} else {
					owner.receiptsOnWorkOwner(x)
				}
			}()
			request, err := work.read(time.Now().Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if operation == "bind" {
				raw, err = encodeLinuxPacket(linuxMemberBindResult, request.id, 1, linuxMemberBindReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Provenance: facts.Provenance, Receipt: StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}}})
			} else {
				raw, err = encodeLinuxPacket(linuxMemberReceiptsResult, request.id, 1, linuxMemberReceiptsReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Sequence: 1, Receipts: []Receipt{}})
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = work.write(raw, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if operation == "bind" {
				if result := <-x.memberResult; result.err != nil {
					t.Fatal(result.err)
				}
			} else {
				if result := <-x.receiptsResult; result.err != nil {
					t.Fatal(result.err)
				}
			}
			owner.mu.Lock()
			busy := owner.workBusy
			owner.mu.Unlock()
			if busy {
				t.Fatal("completed original work still owns busy slot")
			}
			<-done
			// Immediately use the real next Sample API/owner, rather than retrying Busy.
			sampleDone := make(chan error, 1)
			go func() { _, err := owner.Sample(ctx); sampleDone <- err }()
			next, err := work.read(time.Now().Add(time.Second))
			if err != nil || next.kind != linuxSample {
				t.Fatal("next operation did not acquire original owner", err)
			}
			raw, err = encodeLinuxPacket(linuxSampleResult, next.id, 1, linuxSampleReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Sample: LinuxProcessSample{Provenance: facts.Provenance}})
			if err != nil {
				t.Fatal(err)
			}
			if err = work.write(raw, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if err = <-sampleDone; err != nil {
				t.Fatal("completed bind/receipt spuriously refused next Sample", err)
			}
		})
	}
}

func TestLinuxMemberNamespaceCompletionAllowsImmediateNextOperation(t *testing.T) {
	n := newOwnedNetNSState()
	n.busy = true
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	first := linuxNamespaceOperation{ctx: ctx, result: make(chan error)}
	finished := make(chan struct{})
	go func() { defer close(finished); n.finishOperation(first, nil) }()
	if err := <-first.result; err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	busy := n.busy
	n.mu.Unlock()
	if busy {
		t.Fatal("completed namespace operation retained busy ownership")
	}
	<-finished
	next := linuxNamespaceOperation{ctx: ctx, result: make(chan error)}
	joined := make(chan struct{})
	go func() { defer close(joined); operation := <-n.operations; n.finishOperation(operation, nil) }()
	if err := n.submit(ctx, next); err != nil {
		t.Fatal("completed operation spuriously refused sequential successor", err)
	}
	if err := <-next.result; err != nil {
		t.Fatal(err)
	}
	<-joined
}

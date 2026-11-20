package node

import (
	"bufio"
	"context"
	"maps"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// This is the functional T7 seam, not a load/latency qualification: both real
// get_identity producers complete while the original F1 target holds a request.
func TestOwnedFindNodeT7RealIdentityAndPresenceWhilePending(t *testing.T) {
	relay, address := newOwnedBareV2PilotRelay(t, 2, 5, true)
	target := dialOwnedBareV2PilotClient(t, relay, address, true)
	f1, f1Wire := dialOwnedBareV2PilotWire(t, relay, address, true, net.Dialer{}, nil)
	t.Cleanup(func() { _ = f1Wire.Close() })

	ordinaryIdentity, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := deriveTestAdvertisePort(configtest.WithTempDataDirs(t, config.Node{
		ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: true,
		BootstrapPeers: []string{address}, Type: domain.NodeTypeFull,
		EnableDatagramV1: true, AllowPrivatePeers: true,
	}))
	ordinary := NewService(cfg, ordinaryIdentity, nil)
	if ordinary.localFindNodeRole != findNodeRoleOff || ordinary.disableRateLimiting || relay.disableRateLimiting {
		t.Fatal("T7 must retain ordinary Off construction and original rate limits")
	}
	// The real sealed presence question needs the target's address book to
	// recognise its contact. The relay is deliberately not given this contact
	// until AFTER resolution, so resolution cannot supply the later proof ledger.
	rememberLivenessContact(t, ordinary, relay.identity)
	ctx, cancel := context.WithCancel(t.Context())
	_, stop := startTestService(t, ctx, cancel, ordinary)
	t.Cleanup(stop)
	ordinaryID := domain.PeerIdentityFromWire(ordinaryIdentity.Address)
	waitFor(t, 2*time.Second, "ordinary CM did not publish a genuine non-F1 proof", func() bool {
		relay.peerMu.RLock()
		defer relay.peerMu.RUnlock()
		for _, entry := range relay.conns {
			if entry.core == nil || entry.core.HasCapability(domain.CapOverlayFindNodeV1) {
				continue
			}
			proof, ok := entry.core.Auth().ProvenIdentity()
			if id, valid := proof.Identity(); ok && valid && id == ordinaryID {
				return true
			}
		}
		return false
	})
	if relay.datagramLayer() == nil || relay.identityResolver == nil || relay.presenceProber == nil || relay.presenceProjector == nil {
		t.Fatal("original running discovery components are absent")
	}
	// Direct datagram reachability can precede the original resolver's routing
	// snapshot (documented 2–2.5 s structural publish lag). Complete SETUP using
	// that actual read-only precondition before creating a held FIND_NODE. This
	// bounded readiness guard does not enlarge either proof or intake deadline.
	waitFor(t, 5*time.Second, "original resolver get_identity route is not ready", func() bool {
		return relay.identityResolver.canAttempt(ordinaryID) &&
			datagramReachableNow(t, relay, ordinaryID, domain.DTypeGetIdentity)
	})
	f1ID := domain.PeerIdentityFromWire(f1.Address)
	originals := make(map[domain.PeerIdentity]findnode.Session)
	for _, member := range bareV2PilotOriginalMembers(t, relay, target.id, f1ID) {
		originals[member.Identity()] = member
	}
	beforeRefusals := ownedAnswerDatagramReverse(t, relay).LocalRefusals[domain.DTypeGetIdentity.String()]
	counts := func(sender datagram.LocalSender) datagram.LocalSendCounts {
		return relay.datagramLayer().metrics.LocalSendersSnapshot().BySender[sender.String()]
	}
	resolverBefore := counts(datagram.LocalSenderIdentityResolver)
	presenceBefore := counts(datagram.LocalSenderPresenceProber)
	request := protocol.FindNodeRequest{ID: 71, Target: target.id, Challenge: domain.FindNodeChallenge{71}}
	raw, err := protocol.MarshalFindNodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := f1Wire.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := f1Wire.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
	var forwarded protocol.FindNodeRequest
	select {
	case forwarded = <-target.requests:
		if forwarded.Target != request.Target || forwarded.Challenge != request.Challenge {
			t.Fatal("original pending FIND_NODE request changed")
		}
	case err := <-target.errors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("FIND_NODE did not reach the held target")
	}
	pending := func() {
		t.Helper()
		v := relay.bridgeCapture.intake.ResourceSnapshot()
		if v.Intake.Taken != 1 || v.Intake.Answered != 0 || v.Pending != 1 {
			t.Fatal("discovery did not coexist with a retained FIND_NODE request", v)
		}
	}
	pending()

	// Cached direct push_identity keys do not terminate StartResolution: this
	// exact fresh attempt must receive a target_proof to become authoritative.
	watermark := relay.identityResolver.CurrentAttemptGen()
	relay.identityResolver.mu.Lock()
	joinedExisting := relay.identityResolver.resolutions[ordinaryID] != nil
	relay.identityResolver.mu.Unlock()
	resolution, err := relay.identityResolver.StartResolution(ordinaryID, identityIntentReason{Type: identityIntentReasonUIChat})
	if err != nil {
		t.Fatal(err)
	}
	// Scalar-only readbacks preserve the original failure rather than guessing
	// that a cache, routing gate or coalesced attempt caused a generic timeout.
	t.Logf("T7 resolver initial: joined=%t lifecycle=%s authority=%s answer_gen=%d watermark=%d current_gen=%d",
		joinedExisting, resolution.Lifecycle, resolution.Authority, resolution.AnswerAttemptGen, watermark, relay.identityResolver.CurrentAttemptGen())
	diagnostic := func(stage string) {
		r := relay.identityResolver
		state, found := r.StateByID(resolution.ResolutionID)
		r.mu.Lock()
		owner := r.resolutions[ordinaryID]
		closed, started, currentGen := r.closed, r.ownerContext != nil, r.attemptGen
		owned, due, cooling, open := false, false, false, 0
		if owner != nil {
			owned, due, open = owner.id == resolution.ResolutionID, !r.clock().Before(owner.nextSendAt), owner.openAttempts
		}
		if until, exists := r.cooldownUntil[ordinaryID]; exists {
			cooling = until.After(r.clock())
		}
		var total, accepted, refused, queuePending, proofRequired int
		var minGen, maxGen uint64
		for _, attempt := range r.attempts {
			if attempt.dst != ordinaryID {
				continue
			}
			total++
			if minGen == 0 || attempt.gen < minGen {
				minGen = attempt.gen
			}
			if attempt.gen > maxGen {
				maxGen = attempt.gen
			}
			if attempt.proofRequired {
				proofRequired++
			}
			switch attempt.queue {
			case identityAttemptQueueAccepted:
				accepted++
			case identityAttemptQueueRefused:
				refused++
			default:
				queuePending++
			}
		}
		r.mu.Unlock()
		t.Logf("T7 resolver %s: state_found=%t lifecycle=%s authority=%s no_route=%t answer_gen=%d watermark=%d current_gen=%d started=%t closed=%t exact_owner=%t due=%t cooling=%t open=%d attempts=%d accepted=%d refused=%d queue_pending=%d proof_required=%d min_gen=%d max_gen=%d can_attempt=%t producer=%+v pending=%d refusals=%d",
			stage, found, state.Lifecycle, state.Authority, state.NoRoute, state.AnswerAttemptGen, watermark, currentGen, started, closed, owned, due, cooling, open, total, accepted, refused, queuePending, proofRequired, minGen, maxGen, r.canAttempt(ordinaryID), counts(datagram.LocalSenderIdentityResolver), relay.bridgeCapture.intake.ResourceSnapshot().Pending, ownedAnswerDatagramReverse(t, relay).LocalRefusals[domain.DTypeGetIdentity.String()])
		for _, endpoint := range []struct {
			name string
			svc  *Service
		}{{"relay", relay}, {"ordinary", ordinary}} {
			m := endpoint.svc.datagramLayer().metrics.Snapshot()
			t.Logf("T7 %s datagram: observed=%d delivered=%d answered=%d dropped=%d refused_answers=%d drops=%v send_refusals=%v reverse=%v",
				endpoint.name, m.Observed, m.Delivered, m.Answered, m.Dropped, m.RefusedAnswers, m.DropsByReason, m.SendRefusals, m.ReverseEvents)
		}
	}
	diagnostic("initial")
	defer func() {
		if t.Failed() {
			diagnostic("failed")
		}
	}()
	waitFor(t, 2*time.Second, "real identity resolver did not verify a fresh target proof", func() bool {
		state, ok := relay.identityResolver.StateByID(resolution.ResolutionID)
		return ok && state.Target == ordinaryID && state.Lifecycle == domain.IdentityResolutionSucceeded &&
			state.Authority == domain.IdentityAuthorityAuthoritative && state.AnswerAttemptGen > watermark
	})
	pending()
	rememberLivenessContact(t, relay, ordinaryIdentity)
	relay.presenceProjector.mu.Lock()
	prior := relay.presenceProjector.records[ordinaryID]
	relay.presenceProjector.mu.Unlock()
	if prior.aliveSource == domain.PresenceSourceProof {
		t.Fatal("presence proof predates the real probe; fixture would be vacuous")
	}
	sentAt := relay.presenceNow()
	if !relay.presenceProber.sendProbe(t.Context(), ordinaryID, sentAt) {
		t.Fatal("real presence producer did not queue its sealed target-proof question")
	}
	waitFor(t, 2*time.Second, "real presence answer did not verify its target proof", func() bool {
		relay.presenceProjector.mu.Lock()
		record := relay.presenceProjector.records[ordinaryID]
		relay.presenceProjector.mu.Unlock()
		if record.aliveSource != domain.PresenceSourceProof || record.aliveFor <= 0 || !record.aliveSince.ObservedAfter(sentAt) {
			return false
		}
		relay.presenceProber.mu.Lock()
		defer relay.presenceProber.mu.Unlock()
		for _, attempt := range relay.presenceProber.attempts {
			if attempt.target == ordinaryID && attempt.sentAt == sentAt {
				return false
			}
		}
		return true
	})
	pending()
	for _, producer := range []struct {
		sender datagram.LocalSender
		before datagram.LocalSendCounts
	}{{datagram.LocalSenderIdentityResolver, resolverBefore}, {datagram.LocalSenderPresenceProber, presenceBefore}} {
		after := counts(producer.sender)
		if after.Attempts <= producer.before.Attempts || after.Queued <= producer.before.Queued ||
			after.QuotaCapped != producer.before.QuotaCapped || after.GateRefused != producer.before.GateRefused {
			t.Fatal("T7 producer was vacuous or refused", producer.sender, producer.before, after)
		}
	}
	if after := ownedAnswerDatagramReverse(t, relay).LocalRefusals[domain.DTypeGetIdentity.String()]; after != beforeRefusals {
		t.Fatal("get_identity local refusal delta is not zero", beforeRefusals, after)
	}
	if err := f1Wire.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	target.replyTo(t, forwarded)
	line, err := readFrameLine(bufio.NewReader(f1Wire), protocol.MaxFrameLine)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := protocol.ParseFindNodeAnswer([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil {
		t.Fatal("signed FIND_NODE return failed after identity/presence exchange", err)
	}
	after := make(map[domain.PeerIdentity]findnode.Session)
	for _, member := range bareV2PilotOriginalMembers(t, relay, target.id, f1ID) {
		after[member.Identity()] = member
	}
	if !maps.Equal(originals, after) {
		t.Fatal("T7 exchange changed original FIND_NODE membership/ConnIDs")
	}
	t.Log("T7 functional: resolver+presence genuine target proofs; get_identity local refusal delta=0; original F1 members retained; signed answer returned")
}

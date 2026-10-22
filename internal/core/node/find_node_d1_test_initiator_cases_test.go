package node

import (
	"bufio"
	"context"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Explicit test capability enters the ORIGINAL TLS transcript. No running
// Service advertises responder capability or becomes construction Ready here.
func d1InitiatorPair(t *testing.T, a, r *Service, findCap, responder bool) f1ProofFixture {
	t.Helper()
	owner := newF1FixtureOwner()
	t.Cleanup(func() { owner.stopAndJoin(t) })
	server, client := tcpPair(t)
	owner.rawServer, owner.rawClient = server, client
	var extra []domain.Capability
	if responder {
		extra = []domain.Capability{domain.CapOverlayResponderV1}
	}
	pair := f1PairOnRaw(t, findCap, nil, a, r, owner, server, client, extra...)
	d1ProjectOriginalNegotiatedCaps(t, pair)
	a.publishAcceptedProofTransport(pair.inbound)
	pair.inbound.life.mu.Lock()
	retired := pair.inbound.life.evidence == nil && !pair.inbound.life.ended
	published := pair.inbound.life.findNode != nil && pair.inbound.life.findNode.published
	pair.inbound.life.mu.Unlock()
	if !retired || (findCap && !published) {
		t.Fatal("genuine accepted publication did not retire staging evidence/publish original")
	}
	return pair
}

// This inactive test Service does not advertise overlay capabilities. Complete
// only this fixture's negotiated metadata from BOTH actual authenticated TLS
// intros, before either original is published to its relevant send registry.
// No capability is manufactured and no staging evidence is retained or read.
func d1ProjectOriginalNegotiatedCaps(t *testing.T, pair f1ProofFixture) {
	t.Helper()
	if pair.accepted == nil || pair.dialer == nil || pair.inbound == nil || pair.inbound.life == nil || pair.outbound == nil || pair.outbound.common == nil || pair.outbound.proven == nil || pair.fixtureOwner == nil {
		t.Fatal("missing exact original fixture tuple")
	}
	remote, remoteErr := pair.acceptedProof.Peer()
	local, localErr := pair.dialedProof.Peer()
	remoteProof, remoteOK := remote.Proof()
	localProof, localOK := local.Proof()
	acceptedRole, acceptedRoleErr := pair.acceptedProof.Role()
	dialedRole, dialedRoleErr := pair.dialedProof.Role()
	if remoteErr != nil || localErr != nil || !remoteOK || !localOK || acceptedRoleErr != nil || dialedRoleErr != nil || acceptedRole != sessionv2.RoleListener || dialedRole != sessionv2.RoleDialer || remote.Identity != domain.PeerIdentityFromWire(pair.dialer.identity.Address) || local.Identity != domain.PeerIdentityFromWire(pair.accepted.identity.Address) {
		t.Fatal("fixture lost original mutually proven intro pair")
	}
	a := pair.accepted
	a.peerMu.Lock()
	entry := a.connEntryByIDLocked(pair.inbound.id)
	if entry == nil || entry != pair.fixtureOwner.inboundEntry || entry.common != pair.inbound.life || entry.core != pair.fixtureOwner.inboundCore {
		a.peerMu.Unlock()
		t.Fatal("fixture capability projection rebound original accepted core")
	}
	entry.common.mu.Lock()
	auth := entry.core.Auth()
	proof, proven := auth.ProvenIdentity()
	valid := auth != nil && proven && proof == remoteProof && reflect.DeepEqual(auth.Hello, remote.Intro) && entry.core.Identity() == remote.Identity && entry.common.token == a.proofToken && entry.common.role == sessionv2.RoleListener && !entry.common.outbound && !entry.common.ended && (entry.common.findNode == nil || !entry.common.findNode.published)
	if valid {
		entry.core.SetCapabilities(intersectCapabilities(domain.ParseCapabilities(local.Intro.Capabilities), remote.Intro.Capabilities))
	}
	entry.common.mu.Unlock()
	a.peerMu.Unlock()
	if !valid {
		t.Fatal("original Auth/proof/common/core mismatch before fixture publication")
	}
	// The paired outbound original has not entered CM/sendable publication.
	// Assert its same genuine peer/proof before completing its mirror metadata.
	r := pair.dialer
	r.peerMu.Lock()
	outbound := pair.outbound
	outbound.common.mu.Lock()
	outProof, outProven := outbound.proven.Proof()
	valid = outProven && outProof == localProof && reflect.DeepEqual(outbound.proven.Intro, local.Intro) && outbound.netCore == pair.fixtureOwner.outboundCore && outbound.common.peer == outbound && outbound.common.token == r.proofToken && outbound.common.role == sessionv2.RoleDialer && outbound.common.outbound && !outbound.common.ended && (outbound.common.findNode == nil || !outbound.common.findNode.published)
	if valid {
		caps := intersectCapabilities(domain.ParseCapabilities(remote.Intro.Capabilities), local.Intro.Capabilities)
		outbound.capabilities = slices.Clone(caps)
		outbound.netCore.SetCapabilities(caps)
	}
	outbound.common.mu.Unlock()
	r.peerMu.Unlock()
	if !valid {
		t.Fatal("fixture capability projection rebound original outbound proof/core")
	}
}
func d1InitiatorFixture(t *testing.T) (*Service, *Service, *atomic.Int64, f1ProofFixture, *d1TestInitiator, domain.PeerIdentity) {
	t.Helper()
	a, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	r, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	pair := d1InitiatorPair(t, a, r, true, true)
	target := domain.PeerIdentityFromWire(r.identity.Address)
	clock := new(atomic.Int64)
	clock.Store(int64(3 * time.Second))
	driver := newD1TestInitiator(a, []domain.PeerIdentity{target}, func() time.Duration { return time.Duration(clock.Load()) })
	return a, r, clock, pair, driver, target
}
func d1StartOriginal(t *testing.T, driver *d1TestInitiator, target domain.PeerIdentity) (*d1TestLookup, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	lookup, result := driver.Start(ctx, target)
	if result.status != "" || result.err != nil || lookup == nil || lookup.count != 1 {
		t.Fatal("test initiator did not retain genuine responder original", lookup, result)
	}
	return lookup, cancel
}

// The D1 test initiator is observation-only; it owns no executing intake.
// Read through the retained ORIGINAL protected core and the same bounded read
// plus command admission used by handleConn, then let Receive verify the saved
// request/challenge. An owned execution receipt would be inapplicable here.
func d1ReadOriginalAnswer(t *testing.T, driver *d1TestInitiator, pair f1ProofFixture, original d1TestOriginal) string {
	t.Helper()
	if driver.service != pair.accepted || original.core == nil || original.core != pair.fixtureOwner.inboundCore || original.common != pair.inbound.life || original.session.ConnID() != pair.inbound.id || !driver.sameOriginal(original) {
		t.Fatal("test initiator reader lost retained original transport")
	}
	core := original.core
	if err := core.Conn().SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(core.Conn())
	for n := 0; n < 32; n++ {
		line, err := driver.service.readInboundCommandLineOwned(reader, pair.inbound.id, pair.inbound)
		if err != nil {
			t.Fatal("test initiator original bounded answer read", err)
		}
		if !driver.service.admitInboundCommandLine(pair.inbound.id, core.Conn().RemoteAddr().String(), line) {
			t.Fatal("test initiator original command budget exhausted")
		}
		// The real loop dispatches other command kinds separately. Classify
		// only AFTER admission; strict answer parse/Verify stays in Receive.
		kind, class := classifyFrameLine(line)
		if class == frameLineNamed && kind == protocol.FindNodeAnswerFrameType {
			return line
		}
	}
	t.Fatal("strict answer absent from bounded original ingress frames")
	return ""
}
func d1PendingCount(driver *d1TestInitiator) int {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	n := 0
	for _, lookup := range driver.pending {
		if lookup != nil {
			n++
		}
	}
	return n
}

func TestD1InitiatorOriginalResponderEnumerationAndThreeRelayBound(t *testing.T) {
	a, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	expected := map[domain.PeerIdentity]*proofTransportLifetime{}
	for i := 0; i < 4; i++ {
		r, _, _ := ownedCaptureTestService(t, 8, 8, 16)
		pair := d1InitiatorPair(t, a, r, true, true)
		expected[domain.PeerIdentityFromWire(r.identity.Address)] = pair.inbound.life
	}
	noResponder, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	d1InitiatorPair(t, a, noResponder, true, false)
	noFindNode, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	d1InitiatorPair(t, a, noFindNode, false, true)
	peers, err := d1TestOriginals(t.Context(), a)
	if err != nil || len(peers) != 3 {
		t.Fatal("exact original responder enumeration", len(peers), err)
	}
	seen := map[domain.PeerIdentity]bool{}
	for _, peer := range peers {
		if expected[peer.session.Identity()] != peer.common || seen[peer.session.Identity()] {
			t.Fatal("unproven/capability-missing/rebound/duplicate relay selected")
		}
		seen[peer.session.Identity()] = true
		if peer.core == nil || peer.session.ConnID() == 0 {
			t.Fatal("original writer/session not retained")
		}
	}
	// Canonical order is stable, including map traversal; no arbitrary alternate
	// connection or fourth relay enters a second traversal.
	again, err := d1TestOriginals(t.Context(), a)
	if err != nil || len(again) != 3 {
		t.Fatal(err)
	}
	for i := range peers {
		if peers[i] != again[i] {
			t.Fatal("original enumeration order drift")
		}
	}
}
func TestD1InitiatorRejectsAdvertisedIdentityRebindAndEndedOriginal(t *testing.T) {
	a, _, _, pair, driver, _ := d1InitiatorFixture(t)
	peers, err := d1TestOriginals(t.Context(), a)
	if err != nil || len(peers) != 1 {
		t.Fatal(peers, err)
	}
	original := peers[0]
	claimed := original.session.Identity()
	claimed[0] ^= 1
	a.peerMu.Lock()
	pair.fixtureOwner.inboundCore.SetIdentity(claimed)
	a.peerMu.Unlock()
	rebound, err := d1TestOriginals(t.Context(), a)
	a.peerMu.Lock()
	pair.fixtureOwner.inboundCore.SetIdentity(original.session.Identity())
	a.peerMu.Unlock()
	if err != nil || len(rebound) != 0 {
		t.Fatal("declared identity replaced original ProvenSession", rebound, err)
	}
	pair.inbound.end()
	if driver.sameOriginal(original) {
		t.Fatal("ended original remained sendable")
	}
	ended, err := d1TestOriginals(t.Context(), a)
	if err != nil || len(ended) != 0 {
		t.Fatal("ended original was replaced by numeric registry key", ended, err)
	}
}
func TestD1InitiatorRejectsAdvertisedResponderWithoutOriginalProofCapability(t *testing.T) {
	a, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	r, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	pair := d1InitiatorPair(t, a, r, true, false)
	core := pair.fixtureOwner.inboundCore
	auth := core.Auth()
	if auth == nil || slices.Contains(auth.Hello.Capabilities, string(domain.CapOverlayResponderV1)) {
		t.Fatal("negative fixture unexpectedly proved responder capability")
	}
	// Deliberately corrupt ONLY advertised metadata: the canonical enumerator
	// sees a sendable candidate, but the retained original proof must reject it.
	a.peerMu.Lock()
	core.SetCapabilities(append(core.Capabilities(), domain.CapOverlayResponderV1))
	candidates := a.peerSendableConnectionsLocked(domain.PeerIdentityFromWire(r.identity.Address), domain.CapOverlayResponderV1, time.Now().UTC())
	a.peerMu.Unlock()
	if len(candidates) != 1 || !candidates[0].proven || candidates[0].outbound != nil || candidates[0].inboundID != pair.inbound.id {
		t.Fatal("negative did not reach exact proven original candidate filter")
	}
	peers, err := d1TestOriginals(t.Context(), a)
	if err != nil || len(peers) != 0 {
		t.Fatal("advertised responder replaced original authenticated intro", peers, err)
	}
}
func TestD1InitiatorFoundOverOriginalProtectedStreamAndRetainedChallenge(t *testing.T) {
	_, r, _, pair, driver, target := d1InitiatorFixture(t)
	lookup, _ := d1StartOriginal(t, driver, target)
	request := ownedExecutingReadForward(t, pair, target)
	if request != lookup.request {
		t.Fatal("actual request lost retained ID/target/challenge")
	}
	answer := ownedAnswerSignedBody(t, r, request)
	raw, err := protocol.MarshalFindNodeAnswer(answer)
	if err != nil {
		t.Fatal(err)
	}
	if status := pair.outbound.netCore.SendRaw(append(raw, '\n')); status != netcore.SendOK {
		t.Fatal(status)
	}
	original := lookup.peers[0]
	line := d1ReadOriginalAnswer(t, driver, pair, original)
	result := driver.Receive(lookup, original.common, []byte(line))
	if result.err != nil || result.status != d1TestFound || d1PendingCount(driver) != 0 {
		t.Fatal("original verified lookup did not complete", result)
	}
	if _, err := protocol.MarshalFindNodeAnswer(result.answer); err != nil {
		t.Fatal(err)
	}
	if replay := driver.Receive(lookup, original.common, []byte(line)); !errors.Is(replay.err, findnode.ErrStaleWork) {
		t.Fatal("one original answer completed twice", replay)
	}
}
func TestD1InitiatorNoNeighboursTimeoutAndCancellation(t *testing.T) {
	t.Run("no-neighbours", func(t *testing.T) {
		a, _, _ := ownedCaptureTestService(t, 8, 8, 16)
		var target domain.PeerIdentity
		target[0] = 7
		driver := newD1TestInitiator(a, []domain.PeerIdentity{target}, func() time.Duration { return 0 })
		lookup, result := driver.Start(t.Context(), target)
		if lookup != nil || result.status != d1TestNoNeighbours || result.err != nil || d1PendingCount(driver) != 0 {
			t.Fatal(result)
		}
	})
	t.Run("timeout-equality", func(t *testing.T) {
		_, _, clock, pair, driver, target := d1InitiatorFixture(t)
		lookup, _ := d1StartOriginal(t, driver, target)
		ownedExecutingReadForward(t, pair, target)
		clock.Store(int64(lookup.started + peerRequestTimeout - time.Nanosecond))
		if result := driver.Poll(lookup); result.status != "" || result.err != nil {
			t.Fatal("early timeout", result)
		}
		clock.Store(int64(lookup.started + peerRequestTimeout))
		if result := driver.Poll(lookup); result.status != d1TestTimeout || result.err != nil || d1PendingCount(driver) != 0 {
			t.Fatal("12s original timeout failed", result)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		_, _, _, pair, driver, target := d1InitiatorFixture(t)
		lookup, cancel := d1StartOriginal(t, driver, target)
		ownedExecutingReadForward(t, pair, target)
		cancel()
		if result := driver.Poll(lookup); !errors.Is(result.err, context.Canceled) || result.status != "" || d1PendingCount(driver) != 0 {
			t.Fatal("cancellation invented a wire status or leaked pending", result)
		}
		events := make(chan bool, 1)
		pair.fixtureOwner.inboundCore.SetCaptureSink(ownedAnswerCapture{events})
		cancelled, stop := context.WithCancel(t.Context())
		stop()
		if pending, result := driver.Start(cancelled, target); pending != nil || !errors.Is(result.err, context.Canceled) {
			t.Fatal("cancelled start accepted", result)
		}
		select {
		case <-events:
			t.Fatal("cancelled start emitted another request")
		default:
		}
	})
}
func TestD1InitiatorInvalidAnswerCannotReplaceOriginalOrChallenge(t *testing.T) {
	for _, kind := range []string{"signature", "challenge", "target", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			_, r, _, pair, driver, target := d1InitiatorFixture(t)
			lookup, _ := d1StartOriginal(t, driver, target)
			request := ownedExecutingReadForward(t, pair, target)
			answer := ownedAnswerSignedBody(t, r, request)
			if foreign := driver.Receive(lookup, pair.outbound.common, []byte("{}")); !errors.Is(foreign.err, findnode.ErrUnauthenticated) || d1PendingCount(driver) != 1 {
				t.Fatal("foreign original resolved lookup", foreign)
			}
			stray := answer
			stray.ID++
			raw, err := protocol.MarshalFindNodeAnswer(stray)
			if err != nil {
				t.Fatal(err)
			}
			if result := driver.Receive(lookup, pair.inbound.life, raw); result.err != nil || result.status != "" || d1PendingCount(driver) != 1 {
				t.Fatal("unmatched envelope resolved lookup", result)
			}
			switch kind {
			case "signature":
				answer.Signature[0] ^= 1
			case "challenge":
				other := request
				other.Challenge[0] ^= 1
				answer = ownedAnswerSignedBody(t, r, other)
			case "target":
				other, _, _ := ownedCaptureTestService(t, 8, 8, 16)
				wrong := request
				wrong.Target = domain.PeerIdentityFromWire(other.identity.Address)
				answer = ownedAnswerSignedBody(t, other, wrong)
			}
			raw, err = protocol.MarshalFindNodeAnswer(answer)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "malformed" {
				raw = []byte("{}")
			}
			result := driver.Receive(lookup, pair.inbound.life, raw)
			if result.status != d1TestInvalid || result.err != nil || d1PendingCount(driver) != 0 {
				t.Fatal("invalid retained response did not retire local attempt", result)
			}
		})
	}
}
func TestD1InitiatorInvalidRelayKeepsAnotherOriginalResponseEligible(t *testing.T) {
	a, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	targetService, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	relay, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	first := d1InitiatorPair(t, a, targetService, true, true)
	second := d1InitiatorPair(t, a, relay, true, true)
	target := domain.PeerIdentityFromWire(targetService.identity.Address)
	driver := newD1TestInitiator(a, []domain.PeerIdentity{target}, func() time.Duration { return 3 * time.Second })
	lookup, result := driver.Start(t.Context(), target)
	if lookup == nil || lookup.count != 2 || result.status != "" || result.err != nil {
		t.Fatal("parallel original relay setup", result)
	}
	request := ownedExecutingReadForward(t, first, target)
	other := ownedExecutingReadForward(t, second, target)
	if request != other || request != lookup.request {
		t.Fatal("parallel relays changed the retained challenge")
	}
	valid := ownedAnswerSignedBody(t, targetService, request)
	invalid := valid
	invalid.Signature[0] ^= 1
	raw, err := protocol.MarshalFindNodeAnswer(invalid)
	if err != nil {
		t.Fatal(err)
	}
	// Directed verification input; real protected-stream ownership is exercised
	// in the Found test above. This case isolates two original response states.
	if result := driver.Receive(lookup, lookup.peers[0].common, raw); result.status != "" || result.err != nil || d1PendingCount(driver) != 1 {
		t.Fatal("one invalid relay cancelled another original", result)
	}
	raw, err = protocol.MarshalFindNodeAnswer(valid)
	if err != nil {
		t.Fatal(err)
	}
	if result := driver.Receive(lookup, lookup.peers[1].common, raw); result.status != d1TestFound || result.err != nil || d1PendingCount(driver) != 0 {
		t.Fatal("remaining original valid answer lost", result)
	}
}
func TestD1InitiatorLimitedPoolAndPerTargetWindowDoNotRefundOnCancel(t *testing.T) {
	t.Run("target-window", func(t *testing.T) {
		_, _, clock, pair, driver, target := d1InitiatorFixture(t)
		var previous protocol.FindNodeRequest
		for n := 0; n < 3; n++ {
			lookup, cancel := d1StartOriginal(t, driver, target)
			request := ownedExecutingReadForward(t, pair, target)
			if n > 0 && (request.ID == previous.ID || request.Challenge == previous.Challenge) {
				t.Fatal("new lookup reused ID or retained challenge")
			}
			previous = request
			cancel()
			driver.Poll(lookup)
		}
		if lookup, result := driver.Start(t.Context(), target); lookup != nil || result.status != d1TestLimited {
			t.Fatal("cancel refunded per-target original window", result)
		}
		clock.Add(int64(30 * time.Second))
		lookup, result := driver.Start(t.Context(), target)
		if lookup == nil || result.status != "" || result.err != nil {
			t.Fatal("exact30s window did not reopen", result)
		}
		ownedExecutingReadForward(t, pair, target)
	})
	t.Run("pending16", func(t *testing.T) {
		a, _, clock, pair, _, _ := d1InitiatorFixture(t)
		targets := make([]domain.PeerIdentity, 17)
		for i := range targets {
			targets[i][0] = byte(i + 1)
		}
		driver := newD1TestInitiator(a, targets, func() time.Duration { return time.Duration(clock.Load()) })
		for i := 0; i < 16; i++ {
			lookup, result := driver.Start(t.Context(), targets[i])
			if lookup == nil || result.status != "" || result.err != nil {
				t.Fatal("legal original pending refused", i, result)
			}
			ownedExecutingReadForward(t, pair, targets[i])
		}
		if lookup, result := driver.Start(t.Context(), targets[16]); lookup != nil || result.status != d1TestLimited || d1PendingCount(driver) != 16 {
			t.Fatal("pending pool exceeded16", result)
		}
		clock.Add(int64(peerRequestTimeout))
		lookup, result := driver.Start(t.Context(), targets[16])
		if lookup == nil || result.status != "" || result.err != nil || d1PendingCount(driver) != 1 {
			t.Fatal("timeout failed to release original pool", result)
		}
		ownedExecutingReadForward(t, pair, targets[16])
	})
}

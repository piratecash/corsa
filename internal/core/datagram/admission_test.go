package datagram

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// admission_test.go covers the two-stage per-neighbour budget of §5 and the
// three properties §9 names by hand: the crypto budget runs out before the
// byte budget on small signed frames, the class does not widen the budget,
// and the budget is keyed per neighbour.
//
// It also pins what the KEY is allowed to be. AdmissionKey carries its
// namespace, so a bucket opened for an identity the peer proved and a bucket
// opened for an address this node dialled are two buckets and cannot be talked
// into being one — and the key that names nobody opens neither.

// limitsClock is the injectable clock of the M8 tests. It is deliberately not
// the package's manual clock fixture: these tests move time in seconds to
// watch buckets refill, and sharing a clock with the pipeline fixtures would
// make one test's advance another test's flake.
type limitsClock struct {
	mu  sync.Mutex
	now time.Time
}

func newLimitsClock() *limitsClock {
	return &limitsClock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *limitsClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *limitsClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// A stream of small SIGNED control frames must run out of verification tokens
// while the byte budget is still nearly untouched, and it must be turned away
// BEFORE ed25519.Verify — which is what ChargeVerify returning false means
// (§4.1 step 8, §5, §9).
func TestAdmissionCryptoBudgetRunsOutBeforeBytes(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	budget := DefaultLimits().Peer
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: budget})
	peer := domaintest.ID("noisy-signer")

	// A minimum-size signed routed frame on this wire format.
	const smallSignedFrame = 480

	admitted, verified := 0, 0
	for i := 0; i < budget.FrameBurst; i++ {
		if !admission.Admit(ProvenIdentityKey(peer), smallSignedFrame) {
			break
		}
		admitted++
		if !admission.ChargeVerifyFor(ProvenIdentityKey(peer)) {
			break
		}
		verified++
	}

	if verified != budget.VerifyBurst {
		t.Fatalf("verified %d frames, want the whole verify burst of %d", verified, budget.VerifyBurst)
	}
	if admitted != verified+1 {
		t.Fatalf("admitted %d frames for %d verifications: the refusal must happen at the "+
			"crypto stage, after the frame was admitted", admitted, verified)
	}

	stats := admission.Stats()
	if stats.VerifiesRefused != 1 {
		t.Fatalf("VerifiesRefused = %d, want exactly the one refusal", stats.VerifiesRefused)
	}
	if stats.RefusedBytes != 0 || stats.RefusedFrames != 0 {
		t.Fatalf("stage one refused something: %+v", stats)
	}
	// The byte budget must still be nearly whole — that is the point of the
	// two stages.
	spent := admitted * smallSignedFrame
	if spent*4 >= budget.ByteBurst {
		t.Fatalf("the signed flood spent %d B of the %d B byte burst; the crypto budget "+
			"is supposed to bite long before that", spent, budget.ByteBurst)
	}
	// And the byte budget is genuinely still usable: a full-size bulk frame
	// still fits.
	if !admission.Admit(ProvenIdentityKey(peer), MaxFrameBytes(domain.DatagramClassBulk)) {
		t.Fatal("the byte budget was exhausted after all")
	}
}

// The budget is per neighbour and the classes DIVIDE it. Switching class must
// not hand the sender a second budget — otherwise the limit is bypassed by
// writing a different string into one header field.
func TestAdmissionClassDoesNotWidenThePeerBudget(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Peer: PeerBudget{
		BytesPerSecond:  1_000,
		ByteBurst:       10_000,
		FramesPerSecond: 4,
		FrameBurst:      8,
	}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: limits.Peer})
	peer := domaintest.ID("class-switcher")

	// Spend the whole frame burst on control-sized frames.
	const controlBytes = 500
	for i := 0; i < limits.Peer.FrameBurst; i++ {
		if !admission.Admit(ProvenIdentityKey(peer), controlBytes) {
			t.Fatalf("control frame %d refused inside the burst", i)
		}
	}
	// A bulk frame now finds the SAME empty bucket. Its size is irrelevant:
	// nothing in the controller reads the class at all.
	if admission.Admit(ProvenIdentityKey(peer), MaxFrameBytes(domain.DatagramClassBulk)) {
		t.Fatal("a bulk frame was admitted on an exhausted per-neighbour budget: the class widened it")
	}
	if admission.Admit(ProvenIdentityKey(peer), controlBytes) {
		t.Fatal("a second control frame was admitted on an exhausted budget")
	}

	// The mirror case: exhausting the BYTE budget with bulk frames must not
	// leave control traffic a private allowance either.
	clock.advance(time.Hour)
	other := domaintest.ID("bulk-then-control")
	for admission.Admit(ProvenIdentityKey(other), 2_000) {
		// Drain the byte bucket; the frame bucket is deeper in bytes terms.
	}
	if admission.Admit(ProvenIdentityKey(other), controlBytes) {
		t.Fatal("a control frame was admitted after bulk drained the shared budget")
	}
}

// The budget is keyed by the AUTHENTICATED identity: two neighbours never
// share one, and one neighbour never gets two.
func TestAdmissionBudgetIsPerAuthenticatedIdentity(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Peer: PeerBudget{FramesPerSecond: 1, FrameBurst: 2}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: limits.Peer})
	loud, quiet := domaintest.ID("loud"), domaintest.ID("quiet")

	for i := 0; i < limits.Peer.FrameBurst; i++ {
		if !admission.Admit(ProvenIdentityKey(loud), 100) {
			t.Fatalf("frame %d of the burst refused", i)
		}
	}
	if admission.Admit(ProvenIdentityKey(loud), 100) {
		t.Fatal("the loud neighbour kept spending past its burst")
	}
	if !admission.Admit(ProvenIdentityKey(quiet), 100) {
		t.Fatal("the quiet neighbour was charged for its neighbour's flood")
	}
	if !admission.ChargeVerifyFor(ProvenIdentityKey(quiet)) {
		t.Fatal("the quiet neighbour lost its verification budget too")
	}
}

// Buckets refill in real time; a neighbour that waits gets its budget back,
// and never more than the burst.
func TestAdmissionBucketsRefillAndCapAtTheBurst(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Peer: PeerBudget{
		FramesPerSecond:   2,
		FrameBurst:        2,
		VerifiesPerSecond: 1,
		VerifyBurst:       1,
	}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: limits.Peer})
	peer := domaintest.ID("patient")

	for i := 0; i < 2; i++ {
		if !admission.Admit(ProvenIdentityKey(peer), 10) {
			t.Fatalf("frame %d refused inside the burst", i)
		}
	}
	if admission.Admit(ProvenIdentityKey(peer), 10) {
		t.Fatal("the burst did not end")
	}

	clock.advance(time.Second)
	if !admission.Admit(ProvenIdentityKey(peer), 10) {
		t.Fatal("a second of silence bought no frames back")
	}

	// An hour of silence must not bank an hour of frames.
	clock.advance(time.Hour)
	for i := 0; i < 2; i++ {
		if !admission.Admit(ProvenIdentityKey(peer), 10) {
			t.Fatalf("refill %d refused", i)
		}
	}
	if admission.Admit(ProvenIdentityKey(peer), 10) {
		t.Fatal("the bucket banked more than its burst while idle")
	}
}

// A clock that jumps backwards must not drain the buckets: an NTP correction
// is not a reason to refuse a neighbour's traffic.
func TestAdmissionSurvivesABackwardClock(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: DefaultLimits().Peer})
	peer := domaintest.ID("time-traveller")

	if !admission.Admit(ProvenIdentityKey(peer), 100) {
		t.Fatal("the first frame was refused")
	}
	clock.advance(-time.Hour)
	if !admission.Admit(ProvenIdentityKey(peer), 100) {
		t.Fatal("a backward clock refused a frame")
	}
	if !admission.ChargeVerifyFor(ProvenIdentityKey(peer)) {
		t.Fatal("a backward clock refused a verification")
	}
}

// Both dimensions are charged atomically: a frame refused for lack of frame
// tokens must not have eaten bytes on its way out.
func TestAdmissionRefusalChargesNothing(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Peer: PeerBudget{
		BytesPerSecond:  1_000,
		ByteBurst:       10_000,
		FramesPerSecond: 1,
		FrameBurst:      1,
	}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: limits.Peer})
	peer := domaintest.ID("refused")

	if !admission.Admit(ProvenIdentityKey(peer), 1_000) {
		t.Fatal("the first frame was refused")
	}
	for i := 0; i < 5; i++ {
		if admission.Admit(ProvenIdentityKey(peer), 1_000) {
			t.Fatalf("frame %d passed an empty frame bucket", i)
		}
	}
	// Five refused frames must have cost nothing: after one second exactly
	// one frame's worth of budget is back, and the byte bucket still holds
	// the 9 000 bytes the refusals never spent.
	clock.advance(time.Second)
	if !admission.Admit(ProvenIdentityKey(peer), 9_000) {
		t.Fatal("the refused frames were charged bytes anyway")
	}

	stats := admission.Stats()
	if stats.RefusedFrames != 5 {
		t.Fatalf("RefusedFrames = %d, want 5", stats.RefusedFrames)
	}
	if stats.AdmittedBytes != 10_000 {
		t.Fatalf("AdmittedBytes = %d, want the 10 000 actually admitted", stats.AdmittedBytes)
	}
}

// The bucket map is bounded. Buckets that have fully refilled are forgotten
// for free; the eviction that forgives debt is counted, because it is the
// only one worth watching.
func TestAdmissionBoundsTheBucketMap(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Peer: PeerBudget{
		FramesPerSecond: 1,
		FrameBurst:      4,
		TrackedPeers:    8,
		IdleRetention:   time.Minute,
	}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: limits.Peer})

	for i := 0; i < 64; i++ {
		admission.Admit(ProvenIdentityKey(domaintest.ID(string(rune('a'+i%26))+string(rune('0'+i/26)))), 10)
	}
	if tracked := admission.TrackedPeers(); tracked > limits.Peer.TrackedPeers {
		t.Fatalf("tracked %d buckets, cap is %d", tracked, limits.Peer.TrackedPeers)
	}

	// Forget is the session-close path and must be safe for an unknown peer.
	admission.Forget(ProvenIdentityKey(domaintest.ID("never-seen")))
	admission.Forget(ProvenIdentityKey(domaintest.ID("a0")))
	// And for a key that names nobody at all.
	admission.Forget(AdmissionKey{})
}

// The two namespaces are separate budgets. A key built from a proven identity
// and a key built from a dialled address never meet on one bucket, so the
// controller cannot be talked into treating "what the peer says it is" and
// "what this node dialled" as the same neighbour.
//
// The mutation this kills: dropping the space discriminator from AdmissionKey,
// or having one constructor fall back to the other's namespace.
func TestAdmissionKeyNamespacesDoNotCollide(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Peer: PeerBudget{FramesPerSecond: 1, FrameBurst: 2}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: limits.Peer})

	dialed := DialedAddressKey(domain.PeerAddress("10.0.0.9:64646"))
	proven := ProvenIdentityKey(domaintest.ID("10.0.0.9:64646"))

	for i := 0; i < limits.Peer.FrameBurst; i++ {
		if !admission.Admit(dialed, 100) {
			t.Fatalf("frame %d of the dialled neighbour's burst refused", i)
		}
	}
	if admission.Admit(dialed, 100) {
		t.Fatal("the dialled neighbour kept spending past its burst")
	}
	if !admission.Admit(proven, 100) {
		t.Fatal("a PROVEN identity was charged for a dialled address's flood: the two namespaces share a bucket")
	}
	if got := admission.TrackedPeers(); got != 2 {
		t.Fatalf("tracked %d buckets for two namespaces, want 2", got)
	}
	// Forgetting one namespace leaves the other alone.
	clock.advance(limits.Peer.IdleRetention + time.Hour)
	admission.Forget(proven)
	if got := admission.TrackedPeers(); got != 1 {
		t.Fatalf("tracked %d buckets after forgetting one key, want 1", got)
	}
}

// A key that names nobody is refused rather than opening a bucket. One shared
// bucket for every unidentified arrival is a budget any of them drains for all
// the others — and, worse, an admission that returned true would make the zero
// key the cheapest way past the limit.
func TestAdmissionRefusesTheZeroKey(t *testing.T) {
	t.Parallel()

	admission := NewPeerAdmission(AdmissionConfig{Budget: DefaultLimits().Peer})

	if admission.Admit(AdmissionKey{}, 100) {
		t.Fatal("the zero key was admitted")
	}
	if admission.ChargeVerifyFor(AdmissionKey{}) {
		t.Fatal("the zero key bought a verification")
	}
	if admission.Admit(ProvenIdentityKey(domain.PeerIdentity{}), 100) {
		t.Fatal("a zero identity produced a usable key")
	}
	if admission.Admit(DialedAddressKey(domain.PeerAddress("   ")), 100) {
		t.Fatal("a blank address produced a usable key")
	}
	if got := admission.TrackedPeers(); got != 0 {
		t.Fatalf("the zero key opened %d buckets", got)
	}

	// The unkeyed refusals land on their OWN counter: folded into
	// RefusedFrames they would read as a neighbour hitting its limit, which is
	// ordinary load, while this is a receive path that reached the budget
	// without knowing whose it was.
	stats := admission.Stats()
	if stats.RefusedUnkeyed != 4 {
		t.Fatalf("RefusedUnkeyed = %d, want 4 (three charges and one verification)", stats.RefusedUnkeyed)
	}
	if stats.RefusedFrames != 0 || stats.RefusedBytes != 0 || stats.VerifiesRefused != 0 {
		t.Fatalf("an unkeyed refusal was counted as a budget refusal: %+v", stats)
	}

	// The negative control: a real key on the same controller still works, so
	// the assertions above cannot pass because the controller refuses
	// everything.
	if !admission.Admit(ProvenIdentityKey(domaintest.ID("real")), 100) {
		t.Fatal("a well-formed key was refused")
	}
}

// A neighbour whose budget is keyed by the address THIS node dialled keeps one
// bucket across reconnects, whatever it renames itself to. That is the whole
// point of the namespace: the identity of an outbound session is the peer's own
// claim, so a budget keyed on it is a budget the peer resets at will.
func TestDialedAddressKeyIsStableAcrossRenames(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Peer: PeerBudget{FramesPerSecond: 1, FrameBurst: 3}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: limits.Peer})

	address := domain.PeerAddress("198.51.100.7:64646")
	for i := 0; i < limits.Peer.FrameBurst; i++ {
		if !admission.Admit(DialedAddressKey(address), 100) {
			t.Fatalf("frame %d of the burst refused", i)
		}
	}
	// The peer reconnects and presents a brand new identity. The key is ours,
	// so nothing about the bucket changes.
	if admission.Admit(DialedAddressKey(address), 100) {
		t.Fatal("a renamed peer on the same dialled address got its budget back")
	}
	if got := admission.TrackedPeers(); got != 1 {
		t.Fatalf("tracked %d buckets for one dialled address, want 1", got)
	}
}

// The controller satisfies the ONE seam the pipeline declares, and the two
// stages it serves through it and through Admit share one bucket.
//
// Stage one has no seam: it is charged by the owner of the receive path, which
// calls Admit directly. So the property worth stating here is not "both
// interfaces are implemented" — there is one — but that a charge taken through
// the seam and a charge taken directly land on the SAME neighbour, which is the
// whole content of "the crypto budget is part of the neighbour's budget" (§5).
func TestAdmissionServesBothStagesFromOneBucket(t *testing.T) {
	t.Parallel()

	limits := Limits{Peer: PeerBudget{
		FramesPerSecond: 1, FrameBurst: 2,
		VerifiesPerSecond: 1, VerifyBurst: 1,
	}}.Normalized()
	admission := NewPeerAdmission(AdmissionConfig{Budget: limits.Peer})
	var crypto cryptoBudget = admission

	key := ProvenIdentityKey(domaintest.ID("seams"))
	if !admission.Admit(key, 100) {
		t.Fatal("Admit refused the first frame")
	}
	if !crypto.ChargeVerifyFor(key) {
		t.Fatal("ChargeVerifyFor refused the first verification")
	}
	if crypto.ChargeVerifyFor(key) {
		t.Fatal("the verify burst of one bought two verifications")
	}
	// One neighbour, one bucket — not one per stage.
	if got := admission.TrackedPeers(); got != 1 {
		t.Fatalf("the two stages opened %d buckets for one neighbour, want 1", got)
	}
}

// The end-to-end shape of §9: a flood of small signed control frames reaching
// a real pipeline is turned away with DropCryptoBudget — before Verify, and
// while the byte budget is still open.
func TestPipelineRefusesSignedFloodOnTheCryptoBudget(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	private, sender := newSigner(t)
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registerType(t, receiver, routedType(dtypePush, handler))

	clock := newLimitsClock()
	budget := PeerBudget{
		BytesPerSecond:    1 << 20,
		ByteBurst:         4 << 20,
		FramesPerSecond:   64,
		FrameBurst:        64,
		VerifiesPerSecond: 4,
		VerifyBurst:       4,
	}
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: budget})
	// The fixture wires its own counting stub for stage two; swapping the seam
	// in place is what lets the REAL budget run against the real conveyor
	// without duplicating the fixture's wiring. Stage one is charged by the
	// loop below, because in production it is charged by the owner of the
	// receive path and not by the conveyor — and it is charged on the SAME
	// controller and the SAME key, which is what makes the AdmittedBytes
	// assertion at the end an assertion about one neighbour's budget.
	receiver.pipeline.crypto = admission
	key := ProvenIdentityKey(sender)

	var frameBytes int
	delivered, refused := 0, 0
	for i := 0; i < budget.VerifyBurst*2; i++ {
		frame := signedRouted(t, routedOpts{
			now:     receiver.clock(),
			private: private,
			src:     sender,
			dst:     receiver.id,
			payload: []byte{byte(i)},
		})
		line, err := protocol.MarshalDatagramFrameLine(frame)
		if err != nil {
			t.Fatalf("MarshalDatagramFrameLine: %v", err)
		}
		frameBytes = len(line)
		if !admission.Admit(key, frameBytes) {
			t.Fatalf("frame %d was refused by stage one: this test is about stage two", i)
		}
		result := receiver.deliverBilledTo(t, sender, key, frame)
		switch {
		case result.Outcome() == InboundDelivered:
			delivered++
		case result.Reason() == DropCryptoBudget:
			refused++
		default:
			t.Fatalf("frame %d: unexpected %s / %s", i, result.Outcome(), result.Reason())
		}
	}

	if delivered != budget.VerifyBurst {
		t.Fatalf("delivered %d frames, want the verify burst of %d", delivered, budget.VerifyBurst)
	}
	if refused != budget.VerifyBurst {
		t.Fatalf("refused %d frames on the crypto budget, want %d", refused, budget.VerifyBurst)
	}
	if handler.callCount() != delivered {
		t.Fatalf("the handler ran %d times for %d delivered frames", handler.callCount(), delivered)
	}

	stats := admission.Stats()
	if stats.RefusedBytes != 0 || stats.RefusedFrames != 0 {
		t.Fatalf("stage one refused something on a small-frame flood: %+v", stats)
	}
	if int(stats.AdmittedBytes) != (delivered+refused)*frameBytes {
		t.Fatalf("AdmittedBytes = %d, want %d: the budget is charged on the SERIALIZED frame",
			stats.AdmittedBytes, (delivered+refused)*frameBytes)
	}
	if stats.AdmittedBytes*4 >= uint64(budget.ByteBurst) {
		t.Fatalf("the flood spent %d B of the %d B byte burst", stats.AdmittedBytes, budget.ByteBurst)
	}
}

// TestVerifyBudgetIsChargedToTheKeyAndNotToTheClaimedIdentity is the second
// half of the key defect, on the stage that still derived its neighbour from
// the frame: stage two used to charge arrival.peer, which on an outbound
// session is the fingerprint the REMOTE side wrote into its welcome.
//
// Three consequences followed from that, and this test is shaped to show the
// worst one. A neighbour on a session THIS node dialled could name any node it
// liked — a fingerprint is public — and burn that node's verification tokens,
// after which the real owner's own signed frames came back refused on a budget
// it never spent. The fixture therefore needs TWO neighbours: with one, "the
// tokens were taken from the wrong bucket" and "the tokens were taken" are the
// same observation.
//
// The mutation this kills: restoring `p.chargeVerify(arrival.peer)` in
// verifyRouted, or deriving the key inside chargeVerify. Either one puts both
// batches below on ProvenIdentityKey(owner), and the owner's batch — the
// positive control — is then refused in full.
func TestVerifyBudgetIsChargedToTheKeyAndNotToTheClaimedIdentity(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	private, signer := newSigner(t)
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	// The type declares that it needs no proven neighbour. This test is about
	// which BUDGET KEY a verification is charged to, and the strict default
	// would refuse the dialled arrival at the sender-proof gate — one gate ABOVE
	// the crypto budget — so the invariant under test would never be reached and
	// the test would pass while measuring nothing.
	billed := routedType(dtypePush, handler)
	billed.SenderProof = SenderProvenInPayload
	registerType(t, receiver, billed)

	clock := newLimitsClock()
	budget := PeerBudget{
		BytesPerSecond:    1 << 20,
		ByteBurst:         4 << 20,
		FramesPerSecond:   64,
		FrameBurst:        64,
		VerifiesPerSecond: 1,
		VerifyBurst:       2,
	}
	// The clock never advances in this test, so nothing refills and every
	// refusal below is a spent bucket rather than a timing accident.
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: budget})
	receiver.pipeline.crypto = admission

	// Neighbour one: an ACCEPTED connection, identity proven by the handshake.
	// This is the node whose budget must not be spendable by anybody else.
	owner := domaintest.ID("proven-owner")
	ownerKey := ProvenIdentityKey(owner)
	// Neighbour two: a session THIS node dialled, which proves nothing about
	// the remote — so it is billed to the host:port we dialled — and which
	// claims to be `owner` in its welcome.
	impostorKey := DialedAddressKey(domain.PeerAddress("203.0.113.9:64646"))

	sent := 0
	nextFrame := func() protocol.DatagramFrame {
		sent++
		// A distinct payload per frame, so each has its own transcript and no
		// frame dies as a replay before it reaches the crypto stage.
		return signedRouted(t, routedOpts{
			now:     receiver.clock(),
			private: private,
			src:     signer,
			dst:     receiver.id,
			payload: []byte{byte(sent)},
		})
	}

	// The impostor floods past the burst under the borrowed name.
	const overspend = 2
	impostorDelivered, impostorRefused := 0, 0
	for i := 0; i < budget.VerifyBurst+overspend; i++ {
		result := receiver.deliverBilledTo(t, owner, impostorKey, nextFrame())
		switch {
		case result.Outcome() == InboundDelivered:
			impostorDelivered++
		case result.Reason() == DropCryptoBudget:
			impostorRefused++
		default:
			t.Fatalf("impostor frame %d: unexpected %s / %s", i, result.Outcome(), result.Reason())
		}
	}
	if impostorDelivered != budget.VerifyBurst || impostorRefused != overspend {
		t.Fatalf("the impostor got %d verifications and %d refusals, want %d and %d",
			impostorDelivered, impostorRefused, budget.VerifyBurst, overspend)
	}

	// THE ASSERTION: the node whose fingerprint was borrowed still has its whole
	// verification burst.
	for i := 0; i < budget.VerifyBurst; i++ {
		result := receiver.deliverBilledTo(t, owner, ownerKey, nextFrame())
		requireOutcome(t, result, InboundDelivered)
	}
	// And the owner's own budget is a real budget: the next frame is refused,
	// so the loop above cannot have passed because nothing is being charged.
	requireDrop(t, receiver.deliverBilledTo(t, owner, ownerKey, nextFrame()), DropCryptoBudget)

	if got := admission.TrackedPeers(); got != 2 {
		t.Fatalf("tracked %d buckets for two neighbours, want 2: the claim and the dial address met on one", got)
	}
	stats := admission.Stats()
	if stats.VerifiesCharged != uint64(2*budget.VerifyBurst) {
		t.Fatalf("VerifiesCharged = %d, want %d", stats.VerifiesCharged, 2*budget.VerifyBurst)
	}
	if stats.VerifiesRefused != uint64(overspend+1) {
		t.Fatalf("VerifiesRefused = %d, want %d", stats.VerifiesRefused, overspend+1)
	}
	if stats.RefusedUnkeyed != 0 {
		t.Fatalf("RefusedUnkeyed = %d: a charge reached the budget without a key", stats.RefusedUnkeyed)
	}
	if handler.callCount() != impostorDelivered+budget.VerifyBurst {
		t.Fatalf("the handler ran %d times for %d verified frames",
			handler.callCount(), impostorDelivered+budget.VerifyBurst)
	}
}

// TestForgetDoesNotForgiveSpentBudget pins the hole the second review found:
// Forget deleted the neighbour's buckets and bucketsLocked recreates them
// FULL, so a peer that tears its session down and reconnects — a moment it
// picks itself — got a fresh ByteBurst, FrameBurst and VerifyBurst every time.
//
// The eviction path already treats handing budget back as something that costs
// something and counts it (PeersEvicted). Forget must not do it unconditionally
// and silently: an idle bucket is dropped only once it has REFILLED, at which
// point it is byte-for-byte a fresh one and forgetting it forgives nothing.
func TestForgetDoesNotForgiveSpentBudget(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	budget := DefaultLimits().Peer
	admission := NewPeerAdmission(AdmissionConfig{Clock: clock.Now, Budget: budget})
	peer := domaintest.ID("reconnecting-peer")

	spent := 0
	for admission.Admit(ProvenIdentityKey(peer), budget.ByteBurst/8) {
		spent++
		if spent > budget.FrameBurst*4 {
			t.Fatal("the byte budget never ran out")
		}
	}

	// The session ends and comes straight back — the attacker's own timing.
	admission.Forget(ProvenIdentityKey(peer))
	if admission.Admit(ProvenIdentityKey(peer), budget.ByteBurst/8) {
		t.Fatal("a reconnect handed the neighbour its spent byte budget back")
	}

	// The debt is a rate limit, not a life sentence: once the bucket has
	// refilled on its own the peer is admitted again.
	clock.advance(time.Duration(budget.ByteBurst/budget.BytesPerSecond+1) * time.Second)
	if !admission.Admit(ProvenIdentityKey(peer), budget.ByteBurst/8) {
		t.Fatal("a refilled bucket must admit again")
	}

	// A bucket that is idle AND completely refilled costs nothing to forget,
	// so Forget still frees the memory in the ordinary case.
	clock.advance(budget.IdleRetention + time.Second)
	admission.Forget(ProvenIdentityKey(peer))
	if admission.TrackedPeers() != 0 {
		t.Fatalf("a fully refilled idle bucket must be dropped, tracked = %d", admission.TrackedPeers())
	}
}

package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func f1Ended(l *proofTransportLifetime) bool { l.mu.Lock(); defer l.mu.Unlock(); return l.ended }

const (
	f1DiagnosticSetup uint32 = iota
	f1DiagnosticArmed
	f1DiagnosticReleased
)

// Test-only phased observer. The mutex coordinates this writer's phase and
// attribution, NOT production Logger readers. Global safety is pre-worker
// installation and post-owner-join restoration.
type f1ArmedDiagnostic struct {
	needle, address              []byte
	phase                        atomic.Uint32
	mu                           sync.Mutex
	triggerGID                   uint64
	setupMatches, foreignMatches uint64
	entered, release             chan struct{}
	enterOnce, releaseOnce       sync.Once
}

func (b *f1ArmedDiagnostic) Write(p []byte) (int, error) {
	if !bytes.Contains(p, b.needle) || !bytes.Contains(p, b.address) {
		return len(p), nil
	}
	gid := relationGateCurrentGID()
	held := false
	b.mu.Lock()
	switch b.phase.Load() {
	case f1DiagnosticSetup:
		b.setupMatches++
	case f1DiagnosticArmed:
		if gid == b.triggerGID {
			b.enterOnce.Do(func() { close(b.entered) })
			held = true
		} else {
			b.foreignMatches++
		}
	case f1DiagnosticReleased:
		// Release stops holding writes, not fault accounting. The logger is
		// still installed until ALL exact owner jobs have actually joined.
		if b.triggerGID == 0 {
			b.setupMatches++
		} else if gid != b.triggerGID {
			b.foreignMatches++
		}
	}
	b.mu.Unlock()
	if held {
		<-b.release
	}
	return len(p), nil
}

// The result helpers let the observer's own negative controls inspect the SAME
// path without turning an expected infrastructure refusal into t.Fatal/RED.
func (b *f1ArmedDiagnostic) tryBeginTrigger(f f1ProofFixture) error {
	p, owner := f.outbound, f.fixtureOwner
	if p == nil || p.common == nil || owner == nil || f.dialer == nil || owner.outbound != p || owner.serviceJobs[1] != f.dialer {
		return errors.New("diagnostic trigger has no exact original fixture owner")
	}
	gid := relationGateCurrentGID()
	l := p.common
	l.mu.Lock()
	b.mu.Lock()
	phase, setup, foreign := b.phase.Load(), b.setupMatches, b.foreignMatches
	live := !l.ended && l.peer == p && l.id != 0 && l.id == p.connID && l.token != nil && l.token == f.dialer.proofToken
	select {
	case <-b.entered:
		live = false
	default:
	}
	valid := phase == f1DiagnosticSetup && setup == 0 && foreign == 0 && live && gid != 0
	if valid {
		b.triggerGID = gid
		b.phase.Store(f1DiagnosticArmed)
	}
	b.mu.Unlock()
	l.mu.Unlock()
	if !valid {
		return fmt.Errorf("diagnostic trigger setup refused: phase%d original_live%t setup%d foreign%d", phase, live, setup, foreign)
	}
	return nil
}
func (b *f1ArmedDiagnostic) beginTrigger(t *testing.T, f f1ProofFixture) {
	t.Helper()
	if err := b.tryBeginTrigger(f); err != nil {
		t.Fatal(err) // after both locks; a setup fault is never semantic evidence
	}
}
func (b *f1ArmedDiagnostic) triggerError() error {
	b.mu.Lock()
	phase, gid, setup, foreign := b.phase.Load(), b.triggerGID, b.setupMatches, b.foreignMatches
	entered := false
	select {
	case <-b.entered:
		entered = true
	default:
	}
	b.mu.Unlock()
	if phase != f1DiagnosticArmed || gid == 0 || !entered || setup != 0 || foreign != 0 {
		return fmt.Errorf("diagnostic receipt is not the clean explicit trigger: phase%d entered%t setup%d foreign%d", phase, entered, setup, foreign)
	}
	return nil
}
func (b *f1ArmedDiagnostic) assertTrigger(t *testing.T) {
	t.Helper()
	if err := b.triggerError(); err != nil {
		t.Fatal(err)
	}
}
func (b *f1ArmedDiagnostic) faultError() error {
	b.mu.Lock()
	setup, foreign := b.setupMatches, b.foreignMatches
	b.mu.Unlock()
	if setup != 0 || foreign != 0 {
		return fmt.Errorf("diagnostic setup/foreign matches are infrastructure faults: setup%d foreign%d", setup, foreign)
	}
	return nil
}
func (b *f1ArmedDiagnostic) checkFaults(t *testing.T) {
	t.Helper()
	if err := b.faultError(); err != nil {
		t.Error(err)
	}
}
func (b *f1ArmedDiagnostic) unblock() {
	b.mu.Lock()
	b.phase.Store(f1DiagnosticReleased)
	b.releaseOnce.Do(func() { close(b.release) })
	b.mu.Unlock()
}
func f1HeldDiagnostic(t *testing.T, p f1PreparedDiagnosticPair, needle, address string) *f1ArmedDiagnostic {
	t.Helper()
	owner := p.owner
	if owner == nil || owner.serviceJobs != [2]*Service{p.accepted, p.dialer} || p.accepted.inboundPins != nil || p.dialer.inboundPins != nil || owner.rawServer != p.server || owner.rawClient != p.client || owner.acceptDone != nil || owner.inboundCore != nil || owner.outboundCore != nil || owner.serverDone != nil || owner.readerDone != nil {
		t.Fatal("diagnostic logger must be installed before its exact raw pair starts any worker")
	}
	b := &f1ArmedDiagnostic{needle: []byte(needle), address: []byte(address), entered: make(chan struct{}), release: make(chan struct{})}
	owner.diagnostic = b
	old := log.Logger
	oldLevel := zerolog.GlobalLevel()
	t.Cleanup(func() {
		b.unblock()
		if !owner.stopAndJoin(t) {
			return // never restore Logger while ownership remains unconfirmed
		}
		b.checkFaults(t) // even Fatal before beginTrigger cannot hide setup matches
		log.Logger = old
		zerolog.SetGlobalLevel(oldLevel)
	})
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	log.Logger = zerolog.New(b).Level(zerolog.TraceLevel)
	return b
}

func f1Await(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded fixture wait: " + what)
	}
}

// f1AwaitAuthBackground joins finite auth jobs without waiting for the actual
// CM serve loop. A post-CM WaitBackground would also join runLoopsWg and wait
// for a live transport that the following push/capsule oracle still needs.
func f1AwaitAuthBackground(t *testing.T, f f1ProofFixture) {
	t.Helper()
	owner := f.fixtureOwner
	if owner.authDone != nil {
		t.Fatal("auth completion waiter already owned")
	}
	done := make(chan struct{})
	owner.authDone = done
	// Registered before the waiter starts. On failure the SAME owner performs
	// all blocking shutdown/join work, including this exact waiter receipt.
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
			owner.stopAndJoin(t)
		}
	})
	go func() {
		defer close(done)
		f.dialer.backgroundWg.Wait()
		f.accepted.backgroundWg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded finite auth background completion")
	}
}

// These small controls test only the TEST observer. The in-memory tuple here
// is NOT TLS/proof/CM evidence; the existing genuine fixtures retain those
// independent assertions. Actual GIDs are obtained internally, never supplied.
func TestFindNodeDiagnosticObserverPhases(t *testing.T) {
	fixture := func(t *testing.T) (f1ProofFixture, *f1ArmedDiagnostic) {
		t.Helper()
		d := &Service{proofToken: new(proofTransportServiceToken)}
		p := &peerSession{connID: 1, address: "observer-only"}
		p.common = &proofTransportLifetime{token: d.proofToken, id: p.connID, peer: p}
		o := &f1FixtureOwner{outbound: p, serviceJobs: [2]*Service{nil, d}}
		b := &f1ArmedDiagnostic{needle: []byte("needle"), address: []byte("address"), entered: make(chan struct{}), release: make(chan struct{})}
		t.Cleanup(b.unblock)
		return f1ProofFixture{dialer: d, outbound: p, fixtureOwner: o}, b
	}
	assertUnentered := func(t *testing.T, b *f1ArmedDiagnostic) {
		t.Helper()
		select {
		case <-b.entered:
			t.Fatal("setup/foreign/released write invented entered receipt")
		default:
		}
	}
	payload := []byte("needle address")
	t.Run("setup-match", func(t *testing.T) {
		f, b := fixture(t)
		if n, err := b.Write([]byte("unrelated")); err != nil || n != len("unrelated") || b.faultError() != nil {
			t.Fatal("nonmatching write affected observer", n, err)
		}
		if n, err := b.Write(payload); err != nil || n != len(payload) {
			t.Fatal("setup match did not return immediately", n, err)
		}
		assertUnentered(t, b)
		if b.setupMatches != 1 || b.tryBeginTrigger(f) == nil || b.phase.Load() != f1DiagnosticSetup || b.faultError() == nil {
			t.Fatal("pre-arm setup fault was not sticky/refused")
		}
	})
	t.Run("foreign-before-arm", func(t *testing.T) {
		f, b := fixture(t)
		done := make(chan struct{})
		t.Cleanup(func() { b.unblock(); f1Await(t, done, "foreign-before-arm joined") })
		go func() { defer close(done); _, _ = b.Write(payload) }()
		f1Await(t, done, "foreign write before armed commit")
		assertUnentered(t, b)
		if b.setupMatches != 1 || b.tryBeginTrigger(f) == nil || b.faultError() == nil {
			t.Fatal("foreign-before-arm became a clean trigger")
		}
	})
	t.Run("foreign-after-arm-stays-sticky", func(t *testing.T) {
		f, b := fixture(t)
		if err := b.tryBeginTrigger(f); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		t.Cleanup(func() { b.unblock(); f1Await(t, done, "foreign-after-arm joined") })
		go func() { defer close(done); _, _ = b.Write(payload) }()
		f1Await(t, done, "foreign write after armed commit")
		assertUnentered(t, b)
		if b.foreignMatches != 1 || b.faultError() == nil {
			t.Fatal("foreign armed write was silently accepted")
		}
		checked := make(chan struct{})
		t.Cleanup(func() {
			b.releaseOnce.Do(func() { close(b.release) })
			b.unblock()
			f1Await(t, checked, "sticky receipt observer joined")
		})
		go func() {
			defer close(checked)
			select {
			case <-b.entered:
			case <-b.release:
				return
			}
			if b.triggerError() == nil {
				t.Error("same-GID entry erased previous foreign match")
			}
			b.releaseOnce.Do(func() { close(b.release) })
		}()
		_, _ = b.Write(payload) // SAME actual GID which committed begin
		f1Await(t, checked, "sticky fault observed while held")
		if b.triggerError() == nil || b.faultError() == nil {
			t.Fatal("foreign fault disappeared after selected write")
		}
		b.unblock()
		late := make(chan struct{})
		t.Cleanup(func() { f1Await(t, late, "released foreign writer joined") })
		go func() { defer close(late); _, _ = b.Write(payload) }()
		f1Await(t, late, "foreign write after release before join")
		if b.foreignMatches != 2 || b.faultError() == nil {
			t.Fatal("release hid late foreign fault before owner join")
		}
	})
	t.Run("ended-original-current-live", func(t *testing.T) {
		f, b := fixture(t)
		original := f.outbound
		replacement := &peerSession{connID: original.connID, address: original.address}
		replacement.common = &proofTransportLifetime{token: f.dialer.proofToken, id: replacement.connID, peer: replacement}
		f.dialer.sessions = map[domain.PeerAddress]*peerSession{original.address: replacement}
		original.common.end()
		if b.tryBeginTrigger(f) == nil || b.phase.Load() != f1DiagnosticSetup || f1Ended(replacement.common) || f.dialer.sessions[original.address] != replacement {
			t.Fatal("live current B replaced ended original A trigger authority")
		}
		assertUnentered(t, b)
	})
	t.Run("same-gid-held-without-mutex", func(t *testing.T) {
		f, b := fixture(t)
		done, lockDone := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() {
			// Also breaks a mutant which wrongly holds writer.mu across Write.
			b.releaseOnce.Do(func() { close(b.release) })
			b.unblock()
			f1Await(t, done, "selected writer joined")
			f1Await(t, lockDone, "writer mutex observer joined")
		})
		go func() {
			defer close(done)
			if err := b.tryBeginTrigger(f); err != nil {
				t.Error(err)
				return
			}
			f.outbound.common.end() // separate End oracle, not a receipt claim
			_, _ = b.Write(payload)
		}()
		// This second finite observer is registered before go, even if a
		// failure occurs before entered. It never touches global Logger.
		go func() {
			defer close(lockDone)
			select {
			case <-b.entered:
			case <-b.release:
				return
			}
			if err := b.triggerError(); err != nil {
				t.Error("writer mutex observer did not see the clean explicit trigger", err)
			}
		}()
		f1Await(t, b.entered, "selected same-GID Write")
		b.assertTrigger(t)
		if !f1Ended(f.outbound.common) || f.outbound.common.peer != nil {
			t.Fatal("receipt substituted for original End assertion")
		}
		f1Await(t, lockDone, "writer.mu available while Write held")
		select {
		case <-done:
			t.Fatal("selected Write did not remain held")
		default:
		}
		b.unblock()
		f1Await(t, done, "released selected writer")
		if _, err := b.Write(payload); err != nil || b.tryBeginTrigger(f) == nil || b.triggerError() == nil {
			t.Fatal("released write/repeated begin became a new qualified receipt")
		}
	})
	t.Run("owned-pair-release-and-join", func(t *testing.T) {
		prepared := f1PrepareDiagnosticPair(t)
		b := &f1ArmedDiagnostic{entered: make(chan struct{}), release: make(chan struct{})}
		prepared.owner.diagnostic = b // no global Logger assignment in this control
		gate := n1NewHeldWrite()
		f := prepared.finish(t, gate)
		done := make(chan struct{})
		f.fixtureOwner.caseDone = done
		go func() { defer close(done); <-b.release; <-gate.release }()
		if !f.fixtureOwner.stopAndJoin(t) {
			t.Fatal("same once-owner did not confirm actual pair shutdown")
		}
		if !f.fixtureOwner.stopAndJoin(t) {
			t.Fatal("same once-owner did not confirm actual pair shutdown")
		}
		select {
		case <-done:
		default:
			t.Fatal("owned worker not joined after barrier releases")
		}
		if b.phase.Load() != f1DiagnosticReleased {
			t.Fatal("diagnostic release did not precede actual owner join")
		}
	})
}

func TestFindNodeOutboundReadAndAdmissionEndBeforeDiagnostics(t *testing.T) {
	for _, kind := range []string{"read-error", "admission", "contacts-count"} {
		t.Run(kind, func(t *testing.T) {
			needle := "peer_session_admission_session_closed"
			if kind == "read-error" {
				needle = "peer_session_read: remote closed connection"
			}
			prepared := f1PrepareDiagnosticPair(t)
			barrier := f1HeldDiagnostic(t, prepared, needle, prepared.address())
			f := prepared.finish(t, nil)
			p := f.outbound
			done := make(chan struct{})
			f.fixtureOwner.caseDone = done
			t.Cleanup(func() {
				barrier.unblock()
				f.fixtureOwner.stopAndJoin(t)
			})
			go func() {
				defer close(done)
				barrier.beginTrigger(t, f)
				switch kind {
				case "read-error":
					f.dialer.readPeerSession(bufio.NewReader(strings.NewReader("")), p)
				case "admission":
					for n := 0; n <= peerSessionViolationBudget; n++ {
						_ = f.dialer.punishSessionAdmission(p, "wide_budget_refused", "contacts", 1)
					}
				case "contacts-count":
					for n := 0; n <= peerSessionViolationBudget; n++ {
						_ = f.dialer.refuseOversizeContactsReply(p, maxContactsPerResponse+1)
					}
				}
			}()
			f1Await(t, barrier.entered, "actual terminal diagnostic")
			barrier.assertTrigger(t)
			if !f1Ended(p.common) {
				t.Fatal("terminal diagnostic preceded original transport End")
			}
			if p.common.evidence != nil || p.common.peer != nil {
				t.Fatal("terminal retained original TLS/session")
			}
			barrier.unblock()
			f1Await(t, done, "terminal completion")
		})
	}
}
func TestFindNodeOutboundNonterminalAdmissionAndLocalDropsRemainLive(t *testing.T) {
	f := f1Pair(t, true)
	p := f.outbound
	if err := f.dialer.punishSessionAdmission(p, "wide_budget_refused", "contacts", 1); !errors.Is(err, errPeerSessionLineRefused) {
		t.Fatal(err)
	}
	if f1Ended(p.common) {
		t.Fatal("first admission drop ended transport")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if owner, err := f.dialer.Network().(*networkBridge).sendFrameBound(ctx, p.connID, []byte("unused"), false); !errors.Is(err, context.Canceled) || owner != nil {
		t.Fatal("cancelled send invented terminal ownership", err)
	}
	if f1Ended(p.common) {
		t.Fatal("cancelled send ended transport")
	}
}
func TestFindNodeOutboundActualPushPipelineOriginalOwner(t *testing.T) {
	prepared := f1PrepareDiagnosticPair(t)
	barrier := f1HeldDiagnostic(t, prepared, "push_identity_rate_limit_repeat", prepared.accepted.identity.Address)
	f := prepared.finish(t, nil)
	p := f.outbound
	f1StartActualCM(t, f, 11)
	waitFor(t, 2*time.Second, "published actual CM", func() bool { return len(f1Members(t, f.dialer).sessions) == 1 })
	f1AwaitAuthBackground(t, f)
	// Genuine outer datagram signatures and verified inner own records traverse
	// the real outbound dispatcher and registered synchronous push handler.
	reversed := n1ConnectionFixture{n1ServiceFixture: n1ServiceFixture{service: f.dialer, dialer: f.accepted}}
	for n := 0; n < 2; n++ {
		f.dialer.dispatchSessionDatagramLine(p, n1PushWire(t, reversed))
		if f1Ended(p.common) {
			t.Fatal("valid push/first rate drop became terminal", n)
		}
	}
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	t.Cleanup(func() {
		barrier.unblock()
		f.fixtureOwner.stopAndJoin(t)
	})
	go func() {
		defer close(done)
		barrier.beginTrigger(t, f)
		f.dialer.dispatchSessionDatagramLine(p, n1PushWire(t, reversed))
	}()
	f1Await(t, barrier.entered, "actual signed repeat-close")
	barrier.assertTrigger(t)
	if !f1Ended(p.common) {
		t.Fatal("outbound synchronous capsule lost before repeat diagnostic")
	}
	if len(f1Members(t, f.dialer).sessions) != 0 {
		t.Fatal("terminal push retained canonical member")
	}
	barrier.unblock()
	f1Await(t, done, "push dispatcher completion")
}
func TestFindNodeOutboundInitialPushFaultEndsProvisionalOriginal(t *testing.T) {
	prepared := f1PrepareDiagnosticPair(t)
	barrier := f1HeldDiagnostic(t, prepared, "push_identity_enqueue_failed_closing_session", prepared.accepted.identity.Address)
	f := prepared.finish(t, nil)
	p := f.outbound
	// CONTROLLED routing-liveness premise after genuine auth/registration.
	// This is not completed CM setup or realistic ConnID reuse. The separate
	// actual generation/setup positive owns that claim; F1 stays unpublished.
	if !f.dialer.registerCMSession(p, p.address) {
		t.Fatal("registration refused")
	}
	f.dialer.markPeerConnected(p.address, peerDirectionOutbound)
	f.dialer.WaitBackground()
	f1AssertUnpublished(t, p)
	probe := protocol.DatagramFrame{Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRouted, Class: domain.DatagramClassControl, Src: domain.PeerIdentityFromWire(f.dialer.identity.Address), Dst: p.peerIdentity, TTL: 1, RoutePolicy: domain.RoutePolicyBest, DType: domain.DTypePushIdentity}
	f.dialer.peerMu.RLock()
	targets := f.dialer.datagramFrameSendTargetsLocked(probe, p.peerIdentity, time.Now().UTC())
	f.dialer.peerMu.RUnlock()
	if len(targets) != 1 || targets[0].session != p || targets[0].connID != p.connID {
		t.Fatal("controlled routing premise has no exact canonical direct target")
	}
	layer := f.dialer.datagramLayer()
	full := false
	for n := 0; n <= layer.limits.Queue.ControlFrames; n++ {
		if !layer.queue.Enqueue(datagram.QueuedFrame{Class: domain.DatagramClassControl, Frame: protocol.DatagramFrame{Class: domain.DatagramClassControl}, Bytes: 1}) {
			full = true
			break
		}
	}
	if !full {
		t.Fatal("actual bounded queue did not become full")
	}
	original := captureSessionTransportOwner(f.dialer, p)
	if original == nil {
		t.Fatal("original initial-push capsule missing")
	}
	// Controlled same-key B has copied non-owning routing metadata and the same
	// selected physical core solely to isolate late-owner lookup. It is NOT a
	// second genuine proof transport or a production ConnID-reuse scenario.
	replacement := f1ControlledReplacement(f.dialer, p)
	f.dialer.peerMu.Lock()
	f.dialer.sessions[p.address] = replacement
	targets = f.dialer.datagramFrameSendTargetsLocked(probe, p.peerIdentity, time.Now().UTC())
	f.dialer.peerMu.Unlock()
	if len(targets) != 1 || targets[0].session != replacement || targets[0].connID != p.connID {
		t.Fatal("controlled replacement lost canonical direct target")
	}
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	t.Cleanup(func() {
		barrier.unblock()
		f.fixtureOwner.stopAndJoin(t)
	})
	// The production capture-before-goBackground placement is separately AST
	// pinned. This actual typed send proves enqueue-fault -> original End-before-
	// diagnostic; moving capture to a DIFFERENT current owner is an ABA mutant.
	go func() {
		defer close(done)
		barrier.beginTrigger(t, f)
		f.dialer.sendInitialIdentityPushOwned(t.Context(), p.peerIdentity, func() { _ = p.Close() }, original)
	}()
	f1Await(t, barrier.entered, "actual initial enqueue fault")
	barrier.assertTrigger(t)
	if !f1Ended(p.common) || f1Ended(replacement.common) {
		t.Fatal("initial enqueue fault lost A or retired current B")
	}
	f1AssertUnpublished(t, p)
	barrier.unblock()
	f1Await(t, done, "initial callback completion")
}
func TestFindNodeCMStaleGenerationAndDeactivationFenceOriginal(t *testing.T) {
	// Independent deactivation evidence is completed and JOINED before the
	// held global diagnostic for the separate stale-session oracle is installed.
	g := f1DiagnosticPair(t, nil)
	q := g.outbound
	c := g.dialer.connManager
	selected := &slot{Address: q.address, State: domain.SlotStateInitializing, Session: q}
	c.mu.Lock()
	teardown := c.deactivateSlotLocked(selected, slotDeactivationLocalEviction)
	c.mu.Unlock()
	if teardown == nil || !f1Ended(q.common) {
		t.Fatal("CM deactivation deferred End until physical teardown")
	}
	if !g.fixtureOwner.stopAndJoin(t) {
		t.Fatal("independent deactivation fixture not joined before diagnostic installation")
	}
	prepared := f1PrepareDiagnosticPair(t)
	barrier := f1HeldDiagnostic(t, prepared, "onCMStaleSession", prepared.address())
	f := prepared.finish(t, nil)
	cm := f.dialer.connManager
	p := f.outbound
	cm.mu.Lock()
	cm.slots = []*slot{{Address: p.address, State: domain.SlotStateDialing, Generation: 19}}
	cm.mu.Unlock()
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	t.Cleanup(func() {
		barrier.unblock()
		f.fixtureOwner.stopAndJoin(t)
	})
	go func() {
		defer close(done)
		barrier.beginTrigger(t, f)
		cm.handleDialSucceeded(t.Context(), DialSucceeded{Address: p.address, SlotGeneration: 18, Session: p})
	}()
	f1Await(t, barrier.entered, "actual stale refusal before physical close")
	barrier.assertTrigger(t)
	if !f1Ended(p.common) || p.findNodeReceipt != nil {
		t.Fatal("stale dial callback preceded End or acquired receipt")
	}
	barrier.unblock()
	f1Await(t, done, "stale close completion")

}

func TestFindNodeServeSelectedTerminalBeforeDiagnostic(t *testing.T) {
	for _, kind := range []string{"contact-sync", "heartbeat", "ordinary-request"} {
		t.Run(kind, func(t *testing.T) {
			needle := "peer session send failed"
			request := "get_peers"
			if kind == "contact-sync" {
				needle = "owned_contact_sync_failed"
				request = "fetch_contacts"
			}
			if kind == "heartbeat" {
				needle = "heartbeat failed, peer stalled"
				request = "ping"
			}
			prepared := f1PrepareDiagnosticPair(t)
			barrier := f1HeldDiagnostic(t, prepared, needle, prepared.address())
			f := prepared.finish(t, nil)
			p := f.outbound
			if kind == "heartbeat" {
				schedule := productionLiveness()
				schedule.diallerPingInterval = time.Millisecond
				schedule.maxPingJitter = 0
				f.dialer.liveness = &schedule
			}
			done, observed := make(chan struct{}), make(chan struct{})
			f.fixtureOwner.caseDone, f.fixtureOwner.observerDone = done, observed
			p.contactSyncCh = make(chan chan int, 1)
			reply := make(chan int, 1)
			if kind == "contact-sync" {
				p.contactSyncCh <- reply
			}
			if kind == "ordinary-request" {
				p.sendCh <- peerSendItem{Frame: protocol.Frame{Type: request}}
			}
			f.accepted.peerMu.RLock()
			core := f.accepted.coreForIDLocked(f.inbound.id)
			f.accepted.peerMu.RUnlock()
			t.Cleanup(func() {
				barrier.unblock()
				f.fixtureOwner.stopAndJoin(t)
			})
			go func() {
				defer close(observed)
				_ = core.Conn().SetReadDeadline(time.Now().Add(2 * time.Second))
				line, err := bufio.NewReader(core.Conn()).ReadString('\n')
				if err != nil {
					t.Error("actual request not observed", err)
					return
				}
				frame, err := protocol.ParseFrameLine(line)
				if err != nil || frame.Type != request {
					t.Error("unexpected actual request", line, err)
					return
				}
				p.errCh <- errors.New("fixture observed request then terminal reply failure")
			}()
			go func() { defer close(done); barrier.beginTrigger(t, f); _ = f.dialer.servePeerSession(t.Context(), p) }()
			f1Await(t, barrier.entered, "selected fatal diagnostic")
			barrier.assertTrigger(t)
			if !f1Ended(p.common) {
				t.Fatal("serve fatal diagnostic preceded End", kind)
			}
			if kind == "contact-sync" {
				select {
				case n := <-reply:
					if n != 0 {
						t.Fatal("fatal sync returned imports", n)
					}
				default:
					t.Fatal("fatal sync response not completed before diagnostic")
				}
			}
			barrier.unblock()
			f1Await(t, done, "selected fatal return")
		})
	}
}
func TestFindNodeServeContextAndMissingCoreAreTerminal(t *testing.T) {
	for _, kind := range []string{"context", "missing-core"} {
		t.Run(kind, func(t *testing.T) {
			f := f1Pair(t, true)
			p := f.outbound
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			core := p.netCore
			if kind == "context" {
				cancel()
			} else {
				p.netCore = nil
				p.sendCh <- peerSendItem{Frame: protocol.Frame{Type: "relay_message"}}
			}
			t.Cleanup(func() { core.Close() })
			done := make(chan struct{})
			go func() { defer close(done); _ = f.dialer.servePeerSession(ctx, p) }()
			t.Cleanup(func() { cancel(); _ = p.Close(); f1Await(t, done, "serve context/missing core") })
			f1Await(t, done, "selected terminal return")
			if !f1Ended(p.common) {
				t.Fatal("selected terminal return retained lifetime", kind)
			}
		})
	}
}

func TestFindNodeActualFireAndForgetClosedGateEndsBeforeDeliveryLog(t *testing.T) {
	prepared := f1PrepareDiagnosticPair(t)
	barrier := f1HeldDiagnostic(t, prepared, "delivery_refused_by_writer", "f1-foreign-diagnostic")
	f := prepared.finish(t, nil)
	p := f.outbound
	p.netCore.ShutSendQueue() // actual gate closed, actual writer still alive
	select {
	case <-p.netCore.WriterDone():
		t.Fatal("fixture writer already exited")
	default:
	}
	// Foreign sender is an explicit diagnostic-only dispatch fixture: its gate
	// has no local outbox side effects and creates no R/message provenance.
	item := deliveryPeerSendItem(protocol.Frame{Type: "relay_message"}, protocol.Envelope{ID: "f1-foreign-diagnostic", Sender: f.accepted.identity.Address}, time.Now())
	p.sendCh <- item
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	t.Cleanup(func() {
		barrier.unblock()
		f.fixtureOwner.stopAndJoin(t)
	})
	go func() { defer close(done); barrier.beginTrigger(t, f); _ = f.dialer.servePeerSession(t.Context(), p) }()
	f1Await(t, barrier.entered, "actual FF closed-gate diagnostic")
	barrier.assertTrigger(t)
	if !f1Ended(p.common) {
		t.Fatal("FF selected terminal logged before original End")
	}
	barrier.unblock()
	f1Await(t, done, "FF terminal completion")
}
func TestFindNodeActualFireAndForgetMarshalAndFullKeepOwnerLive(t *testing.T) {
	for _, kind := range []string{"marshal", "full"} {
		t.Run(kind, func(t *testing.T) {
			gate := n1NewHeldWrite()
			needle := "fire_and_forget_marshal_error"
			if kind == "full" {
				needle = "fire_and_forget_buffer_full"
			}
			prepared := f1PrepareDiagnosticPair(t)
			barrier := f1HeldDiagnostic(t, prepared, needle, prepared.address())
			f := prepared.finish(t, gate)
			p := f.outbound
			f.accepted.WaitBackground()
			f.dialer.WaitBackground()
			frame := protocol.Frame{Type: "relay_message"}
			if kind == "marshal" {
				frame.Details = []byte("{")
			} else {
				close(gate.arm)
				if p.netCore.SendRaw([]byte("{\"type\":\"ping\"}\n")) != netcore.SendOK {
					t.Fatal("cannot prime actual TLS writer")
				}
				f1Await(t, gate.entered, "actual outbound TLS write hold")
				_, capacity := p.netCore.WriterQueue()
				for range capacity {
					if p.netCore.SendRaw([]byte("{\"type\":\"ping\"}\n")) != netcore.SendOK {
						t.Fatal("cannot fill actual writer queue")
					}
				}
			}
			p.sendCh <- peerSendItem{Frame: frame}
			done := make(chan struct{})
			f.fixtureOwner.caseDone = done
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(func() {
				gate.unblock()
				barrier.unblock()
				cancel()
				f.fixtureOwner.stopAndJoin(t)
			})
			go func() { defer close(done); barrier.beginTrigger(t, f); _ = f.dialer.servePeerSession(ctx, p) }()
			f1Await(t, barrier.entered, "actual FF nonterminal diagnostic")
			barrier.assertTrigger(t)
			if f1Ended(p.common) {
				t.Fatal("best effort FF refusal became terminal", kind)
			}
			gate.unblock()
			barrier.unblock()
			cancel()
			f1Await(t, done, "FF context completion")
		})
	}
}
func TestFindNodeNinOffActualSendEvictionEndsBeforeDiagnostic(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		name := "async_full"
		if synchronous {
			name = "sync_timeout"
		}
		t.Run(name, func(t *testing.T) {
			gate := n1NewHeldWrite()
			needle := "protocol_trace"
			if synchronous {
				needle = "sync flush timeout, disconnecting peer"
			}
			prepared := f1PrepareDiagnosticPair(t)
			barrier := f1HeldDiagnostic(t, prepared, needle, prepared.remoteAddress())
			f := prepared.finish(t, gate)
			p := f.outbound
			if f.dialer.inboundPins != nil || p.common.nin != nil {
				t.Fatal("fixture activated N-in")
			}
			f.accepted.WaitBackground()
			f.dialer.WaitBackground()
			close(gate.arm)
			payload := []byte("{\"type\":\"ping\"}\n")
			if p.netCore.SendRaw(payload) != netcore.SendOK {
				t.Fatal("writer prime refused")
			}
			f1Await(t, gate.entered, "actual TLS writer held")
			if !synchronous {
				_, capacity := p.netCore.WriterQueue()
				for range capacity {
					if p.netCore.SendRaw(payload) != netcore.SendOK {
						t.Fatal("fill refused")
					}
				}
			}
			done := make(chan struct{})
			f.fixtureOwner.caseDone = done
			t.Cleanup(func() {
				gate.unblock()
				barrier.unblock()
				f.fixtureOwner.stopAndJoin(t)
			})
			go func() {
				defer close(done)
				barrier.beginTrigger(t, f)
				if synchronous {
					_ = f.dialer.sendFrameBytesViaNetworkSync(t.Context(), p.connID, payload)
				} else {
					_ = f.dialer.sendFrameViaNetwork(t.Context(), p.connID, protocol.Frame{Type: "ping"})
				}
			}()
			limit := 2 * time.Second
			if synchronous {
				limit = 7 * time.Second
			}
			select {
			case <-barrier.entered:
			case <-time.After(limit):
				t.Fatal("actual full/timeout diagnostic not entered")
			}
			barrier.assertTrigger(t)
			if !f1Ended(p.common) {
				t.Fatal("NinOff actual send eviction retained original authority")
			}
			gate.unblock()
			barrier.unblock()
			f1Await(t, done, "NinOff eviction return")
		})
	}
}

func f1ControlledReplacement(s *Service, p *peerSession) *peerSession {
	q := &peerSession{connID: p.connID, address: p.address, peerIdentity: p.peerIdentity, proven: p.proven, netCore: p.netCore, conn: p.conn, metered: p.metered, authOK: p.authOK, version: p.version, capabilities: p.capabilities, declarations: p.declarations}
	q.common = &proofTransportLifetime{token: s.proofToken, id: p.connID, role: sessionv2.RoleDialer, outbound: true, peer: q}
	return q
}
func TestFindNodeOutboundCapturedArrivalKeepsAWhileCurrentKeyBecomesB(t *testing.T) {
	prepared := f1PrepareDiagnosticPair(t)
	barrier := f1HeldDiagnostic(t, prepared, "push_identity_rate_limit_repeat", prepared.accepted.identity.Address)
	f := prepared.finish(t, nil)
	p := f.outbound
	f.dialer.markPeerStateIntervalTest = -1 // existing fixture-only throttle control, before actual reader/session workers
	f1StartActualCM(t, f, 31)
	waitFor(t, 2*time.Second, "actual CM published", func() bool { return len(f1Members(t, f.dialer).sessions) == 1 })
	f1AwaitAuthBackground(t, f)
	reversed := n1ConnectionFixture{n1ServiceFixture: n1ServiceFixture{service: f.dialer, dialer: f.accepted}}
	for range 2 {
		f.dialer.dispatchSessionDatagramLine(p, n1PushWire(t, reversed))
	}
	wire := n1PushWire(t, reversed)
	replacement := f1ControlledReplacement(f.dialer, p)
	gid := make(chan uint64, 1)
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	held := false
	t.Cleanup(func() {
		if held {
			f.dialer.peerMu.Unlock()
			held = false
		}
		barrier.unblock()
		f.fixtureOwner.stopAndJoin(t)
	})
	f.dialer.peerMu.Lock()
	held = true
	go func() {
		defer close(done)
		barrier.beginTrigger(t, f)
		gid <- relationGateCurrentGID()
		f.dialer.dispatchSessionDatagramLine(p, wire)
	}()
	// Exact target waiter: capture already completed, markPeerRead is the first
	// actual peer-domain wait. No new production hook/queue/callback is added.
	var actualGID uint64
	handoffFailure := ""
	handoffTimer := time.NewTimer(2 * time.Second)
	select {
	case actualGID = <-gid:
	case <-done:
		handoffFailure = "captured-arrival worker completed before GID handoff"
	case <-handoffTimer.C:
		handoffFailure = "bounded captured-arrival GID handoff"
	}
	handoffTimer.Stop()
	if handoffFailure != "" {
		t.Fatal(handoffFailure) // registered cleanup unlocks peerMu, then joins owner
	}
	n1WaitMutexFrame(t, actualGID, "node.(*Service).markPeerRead")
	old := f.dialer.conns[p.connID]
	shadow := &connEntry{core: old.core, metered: old.metered, common: replacement.common, receive: old.receive}
	replacement.common.entry = shadow
	f.dialer.conns[p.connID] = shadow
	f.dialer.sessions[p.address] = replacement
	f.dialer.peerMu.Unlock()
	held = false
	f1Await(t, barrier.entered, "actual repeat warning after A->B")
	barrier.assertTrigger(t)
	if !f1Ended(p.common) || f1Ended(replacement.common) {
		t.Fatal("handler re-resolved current B instead of captured arrival A")
	}
	barrier.unblock()
	f1Await(t, done, "arrival handler completion")
}

func TestFindNodeReaderErrorPublicationFollowsOriginalTerminalFence(t *testing.T) {
	f := f1Pair(t, true)
	p := f.outbound
	f.dialer.WaitBackground()
	f.accepted.WaitBackground()
	// Explicit decoded-reader fixture. Its authenticated, genuine original owner
	// is real, but these plaintext framing bytes are not claimed as TLS I/O.
	p.inboxCh <- protocol.Frame{Type: "fixture_occupied"}
	l := p.common
	l.mu.Lock()
	held := true
	done := make(chan struct{})
	gid := make(chan uint64, 1)
	defer func() {
		if held {
			l.mu.Unlock()
		}
		f1Await(t, done, "reader terminal-fence cleanup join")
	}()
	go func() {
		defer close(done)
		gid <- relationGateCurrentGID()
		f.dialer.readPeerSession(bufio.NewReader(strings.NewReader("{\"type\":\"ping\"}\n")), p)
	}()
	// Observe the exact reader parked inside End while the owner mutex is held.
	// Good code parks BEFORE publishing errCh. Defer-only code publishes first,
	// then parks in deferred End: the error observer distinguishes those paths.
	n1WaitMutexFrame(t, <-gid, "node.(*proofTransportLifetime).end(")
	select {
	case err := <-p.errCh:
		t.Fatal("reader published error before original terminal fence", err)
	default:
	}
	l.mu.Unlock()
	held = false
	f1Await(t, done, "reader terminal-fence completion")
	select {
	case err := <-p.errCh:
		if !errors.Is(err, errPeerSessionInboxOverflow) {
			t.Fatal("unexpected actual reader error", err)
		}
	default:
		t.Fatal("terminal reader did not publish its actual overflow")
	}
	if !f1Ended(l) {
		t.Fatal("error observer saw a nonterminal original owner")
	}
}

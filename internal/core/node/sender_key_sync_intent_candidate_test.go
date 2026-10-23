package node

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Only new connections are redirected. The original X TLS/proof/session remains
// alive in the actual CM while the original fresh-dial consumer can meet Y.
type intentHopFixture struct {
	owner, x, y             *Service
	address                 domain.PeerAddress
	selected                senderKeySyncCandidate
	journal                 *storeJournal
	redirect, armed, stopCM atomic.Bool
	dials                   atomic.Int32
}

func newIntentHopFixture(t *testing.T, contact bool) *intentHopFixture {
	t.Helper()
	f := &intentHopFixture{}
	x, address, stopX := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopX)
	y, other, stopY := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopY)
	f.x, f.y, f.address = x, y, domain.PeerAddress(normalizeAddress(address))
	owner, stop := dialerNode(t, address, func(s *Service) {
		s.connManager.config.MaxSlotsFn = func() int {
			if f.stopCM.Load() {
				return 0
			}
			return 1
		}
		f.journal = journalledStore(s.secureSessions.store)
		if contact {
			trustAsContact(t, s, peerIdentityOf(t, x))
		}
		original := s.peerTCPDial
		s.peerTCPDial = func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
			if f.armed.Load() {
				f.dials.Add(1)
				if f.redirect.Load() && domain.PeerAddress(normalizeAddress(address)) == f.address {
					address = other
				}
			}
			return original(ctx, address, timeout)
		}
	})
	t.Cleanup(stop)
	f.owner = owner
	waitForConditionMsg(t, 10*time.Second, "original proven X hop not published", func() bool {
		candidates := owner.senderKeySyncCandidateSnapshot("", senderKeySyncProvenFanout)
		for _, c := range candidates {
			if c.address == f.address && c.named && owner.senderKeySyncCandidateCurrent(c) {
				f.selected = c
				return true
			}
		}
		return false
	})
	if f.selected.identity != peerIdentityOf(t, x) {
		t.Fatal("selected hop is not actual original X")
	}
	f.armed.Store(true)
	return f
}

func intentHopOffer(t *testing.T, s *Service) *identity.Identity {
	t.Helper()
	sender, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	s.addKnownPubKey(sender.Address, identity.PublicKeyBase64(sender.PublicKey))
	s.addKnownBoxKey(sender.Address, identity.BoxPublicKeyBase64(sender.BoxPublicKey))
	s.addKnownBoxSig(sender.Address, identity.SignBoxKeyBinding(sender))
	return sender
}
func intentHopOfferSame(s *Service, sender *identity.Identity) {
	s.addKnownPubKey(sender.Address, identity.PublicKeyBase64(sender.PublicKey))
	s.addKnownBoxKey(sender.Address, identity.BoxPublicKeyBase64(sender.BoxPublicKey))
	s.addKnownBoxSig(sender.Address, identity.SignBoxKeyBinding(sender))
}
func intentHopPassFinished(t *testing.T, s *Service, sender string) {
	t.Helper()
	waitForConditionMsg(t, 10*time.Second, "original admitted recovery pass not released", func() bool {
		s.senderKeySyncMu.Lock()
		defer s.senderKeySyncMu.Unlock()
		_, done := s.senderKeySyncLastRun[sender]
		return done && len(s.senderKeySyncInFlight) == 0 && len(s.senderKeySyncHopInFlight) == 0
	})
}

func TestInactiveIntentHopActualFanoutRecoversDifferentSender(t *testing.T) {
	for _, contact := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "contact"}[contact], func(t *testing.T) {
			f := newIntentHopFixture(t, contact)
			sender := intentHopOffer(t, f.x)
			if sender.Address == f.selected.identity.String() || f.owner.hasSenderPubKey(sender.Address) {
				t.Fatal("missing S must differ from original hop X")
			}
			before := f.owner.connBudget.Snapshot().Used
			f.owner.triggerSenderKeySyncIntentCandidate("", sender.Address, nil)
			intentHopPassFinished(t, f.owner, sender.Address)
			if !f.owner.hasSenderPubKey(sender.Address) || f.dials.Load() != 1 {
				t.Fatal("actual original named fanout did not recover S through X with one fresh dial")
			}
			if got := f.owner.connBudget.Snapshot().Used; got != before {
				t.Fatalf("fresh dial retained physical reservation: %d != %d", got, before)
			}
			binding, ok := bindingOf(f.owner.secureSessions.store, f.address)
			if !ok || binding.identity != f.selected.identity {
				t.Fatal("original matching X binding missing")
			}
			if contact {
				if binding.protection == nil || binding.protection.by != f.selected.identity || binding.protection.pool != domain.ProtectedBindingPoolContact {
					t.Fatal("original contact-only K protection missing")
				}
			} else if binding.tier() != bindingTierGeneral {
				t.Fatal("noncontact named hop acquired protection")
			}
		})
	}
}

func TestInactiveIntentHopMismatchKeepsOriginalOwnerAndNeverFallsBack(t *testing.T) {
	f := newIntentHopFixture(t, false)
	sender := intentHopOffer(t, f.x)
	intentHopOfferSame(f.y, sender)
	if f.owner.hasSenderPubKey(sender.Address) {
		t.Fatal("S not initially missing")
	}
	before, bound := bindingOf(f.owner.secureSessions.store, f.address)
	if !bound {
		t.Fatal("original X endpoint not bound")
	}
	used := f.owner.connBudget.Snapshot().Used
	f.redirect.Store(true)
	f.owner.triggerSenderKeySyncIntentCandidate("", sender.Address, nil)
	intentHopPassFinished(t, f.owner, sender.Address)
	if f.owner.hasSenderPubKey(sender.Address) || f.dials.Load() != 1 {
		t.Fatal("mismatching Y imported S or retried as address")
	}
	after, ok := bindingOf(f.owner.secureSessions.store, f.address)
	if !ok || !sameBinding(before, after) {
		t.Fatal("mismatch changed original X binding")
	}
	if f.owner.secureSessions.store.identityPinned(peerIdentityOf(t, f.y)) {
		t.Fatal("refused Y acquired a pin")
	}
	rows := f.journal.entries(t, "endpoint_owner_mismatch")
	if len(rows) != 1 || rows[0]["expected_identity"] != f.selected.identity.String() || rows[0]["identity"] != peerIdentityOf(t, f.y).String() {
		t.Fatal("missing exact original X/Y mismatch evidence", rows)
	}
	if got := f.owner.connBudget.Snapshot().Used; got != used {
		t.Fatal("mismatch retained socket budget", got, used)
	}
	if !f.owner.senderKeySyncCandidateCurrent(f.selected) {
		t.Fatal("test lost original X wire before mismatch; stale refusal is not handshake proof")
	}
}

func TestInactiveIntentHopStaleTupleNeverOpensSocket(t *testing.T) {
	for _, name := range []string{"owner", "session", "address", "connID", "peer", "proof", "identity", "generation"} {
		t.Run(name, func(t *testing.T) {
			f := newIntentHopFixture(t, false)
			c := f.selected
			switch name {
			case "owner":
				c.owner = f.y
			case "session":
				c.session = &peerSession{}
			case "address":
				c.address = "127.0.0.1:1"
			case "connID":
				c.connID++
			case "peer":
				c.peer = &sessionv2.Peer{}
			case "proof":
				c.proof = sessionv2.ProvenIdentity{}
			case "identity":
				c.identity = peerIdentityOf(t, f.y)
			case "generation":
				c.generation++
			}
			got := f.owner.syncSenderKeyCandidate(t.Context(), c)
			if f.dials.Load() != 0 {
				t.Fatal("stale named tuple opened socket or became address fallback")
			}
			if got != 0 {
				t.Fatal("stale tuple returned contacts")
			}
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		f := newIntentHopFixture(t, false)
		// A genuinely cancelled original action is not replaced with Background.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		f.owner.syncSenderKeyCandidate(ctx, f.selected)
		if f.dials.Load() != 0 {
			t.Fatal("cancelled candidate opened socket")
		}
	})
	t.Run("closed", func(t *testing.T) {
		f := newIntentHopFixture(t, false)
		// Never reset a terminal eligibility flag.
		f.stopCM.Store(true) // Original CM cannot refill after the terminal case.
		if err := f.selected.session.Close(); err != nil {
			t.Error(err)
		}
		f.owner.syncSenderKeyCandidate(t.Context(), f.selected)
		if f.dials.Load() != 0 {
			t.Fatal("closed original candidate opened socket")
		}
	})
}

func TestInactiveIntentHopSnapshotPreservesLegacyUnknownAndSelectionBounds(t *testing.T) {
	f := newIntentHopFixture(t, false)
	projected := f.owner.senderKeySyncCandidates("")
	if len(projected) != 1 || projected[0] != f.address {
		t.Fatal("original address projection changed", projected)
	}
	ordinary := f.owner.senderKeySyncCandidateSnapshot("", senderKeySyncAddress)
	for _, c := range ordinary {
		if c.named {
			t.Fatal("default candidate became named")
		}
	}
	if got := f.owner.senderKeySyncCandidateSnapshot(f.address, senderKeySyncProvenFanout); len(got) != 0 {
		t.Fatal("previous hop was not excluded")
	}
	s := newHarnessProbeService(t, domain.NodeTypeClient)
	s.peerMu.Lock()
	s.sessions = map[domain.PeerAddress]*peerSession{}
	for i, addr := range []domain.PeerAddress{"unknown-a", "unknown-b", "legacy-a", "legacy-alias", "fourth"} {
		id := domain.PeerIdentity{}
		if i >= 2 {
			id = domain.PeerIdentity{7}
		}
		s.sessions[addr] = &peerSession{address: addr, peerIdentity: id}
		s.health[addr] = &peerHealth{Connected: true}
	}
	s.peerMu.Unlock()
	// No forged v2 proof is minted: all these descriptions remain address-only.
	selected := s.senderKeySyncCandidateSnapshot("", senderKeySyncProvenFanout)
	if len(selected) != senderKeySyncFanout {
		t.Fatal("original cap/known-ID dedup/unknown treatment changed", len(selected))
	}
	for _, c := range selected {
		if c.named {
			t.Fatal("legacy or unknown description acquired named proof")
		}
	}
}

func TestInactiveIntentHopSharesOriginalAdmissionAndCooldown(t *testing.T) {
	s := newHarnessProbeService(t, domain.NodeTypeFull)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); s.WaitBackground() })
	var calls atomic.Int32
	s.peerTCPDial = func(ctx context.Context, _ string, _ time.Duration) (net.Conn, error) {
		calls.Add(1)
		select {
		case <-release:
			return nil, errors.New("fixture refused socket")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ids := make([]string, 5)
	for i := range ids {
		id, err := identity.Generate()
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id.Address
	}
	s.triggerSenderKeySyncIntentCandidate("127.0.0.1:41001", ids[0], nil)
	waitForConditionMsg(t, 3*time.Second, "first original pass not started", func() bool { return calls.Load() == 1 })
	s.triggerSenderKeySyncIntentCandidate("127.0.0.1:41001", ids[1], nil)
	s.senderKeySyncMu.Lock()
	n := len(s.senderKeySyncInFlight)
	s.senderKeySyncMu.Unlock()
	if n != 1 {
		t.Fatal("candidate bypassed original per-hop slot", n)
	}
	s.triggerSenderKeySyncIntentCandidate("127.0.0.1:41002", ids[2], nil)
	s.triggerSenderKeySyncIntentCandidate("127.0.0.1:41003", ids[3], nil)
	waitForConditionMsg(t, 3*time.Second, "three original passes not started", func() bool { return calls.Load() == 3 })
	s.triggerSenderKeySyncIntentCandidate("127.0.0.1:41004", ids[4], nil)
	s.senderKeySyncMu.Lock()
	n = len(s.senderKeySyncInFlight)
	s.senderKeySyncMu.Unlock()
	if n != maxConcurrentSenderKeySyncPasses {
		t.Fatal("candidate bypassed original global3", n)
	}
	unblock()
	s.WaitBackground()
	s.senderKeySyncMu.Lock()
	inflight, hops := len(s.senderKeySyncInFlight), len(s.senderKeySyncHopInFlight)
	s.senderKeySyncMu.Unlock()
	if inflight != 0 || hops != 0 {
		t.Fatal("original release-once failed", inflight, hops)
	}
	s.triggerSenderKeySyncIntentCandidate("127.0.0.1:41001", ids[0], nil)
	s.WaitBackground()
	if calls.Load() != 3 {
		t.Fatal("candidate bypassed original cooldown", calls.Load())
	}
}

func TestInactiveIntentHopDefaultContactRemainsAddress(t *testing.T) {
	f := newIntentHopFixture(t, true)
	sender := intentHopOffer(t, f.x)
	f.owner.triggerSenderKeySyncAsync("", sender.Address, nil)
	intentHopPassFinished(t, f.owner, sender.Address)
	binding, ok := bindingOf(f.owner.secureSessions.store, f.address)
	if !f.owner.hasSenderPubKey(sender.Address) || !ok || binding.tier() != bindingTierGeneral || binding.protection != nil {
		t.Fatal("default address recovery acquired named protection or lost compatibility")
	}
	if len(f.journal.entries(t, "endpoint_binding_protected")) != 0 {
		t.Fatal("default contact dial raised endpoint")
	}
}

func TestInactiveIntentHopTakeoverDoesNotRaisePreviousY(t *testing.T) {
	f := newIntentHopFixture(t, true)
	sender := intentHopOffer(t, f.x)
	y := peerIdentityOf(t, f.y)
	// Original store API seeds the historical endpoint owner, as in existing
	// dial-intent takeover fixtures; the original actual X wire remains live.
	if _, err := f.owner.secureSessions.store.noteProvenOutbound(dialledAt(f.address), y, endpointProtectionNone()); err != nil {
		t.Fatal(err)
	}
	f.owner.triggerSenderKeySyncIntentCandidate("", sender.Address, nil)
	intentHopPassFinished(t, f.owner, sender.Address)
	binding, ok := bindingOf(f.owner.secureSessions.store, f.address)
	if !f.owner.hasSenderPubKey(sender.Address) || !ok || binding.identity != f.selected.identity || binding.tier() != bindingTierGeneral || binding.protection != nil {
		t.Fatal("matching X failed takeover or raised an endpoint last owned by Y")
	}
}

func TestInactiveIntentHopPreviousHopRemainsAddress(t *testing.T) {
	f := newIntentHopFixture(t, false)
	sender := intentHopOffer(t, f.y)
	f.redirect.Store(true)
	// The private option selects EXTRA fanout only. Previous-hop fresh fallback
	// remains address-purpose even though the original X identity is known.
	f.owner.triggerSenderKeySyncIntentCandidate(f.address, sender.Address, nil)
	intentHopPassFinished(t, f.owner, sender.Address)
	if !f.owner.hasSenderPubKey(sender.Address) || f.dials.Load() != 1 {
		t.Fatal("previous-hop address compatibility changed")
	}
	if len(f.journal.entries(t, "endpoint_owner_mismatch")) != 0 {
		t.Fatal("previous hop incorrectly acquired X intent")
	}
}

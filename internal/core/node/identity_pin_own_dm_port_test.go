package node

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/stretchr/testify/mock"
)

func ownDMCandidateManaged(t *testing.T, setup func(*Service)) (*Service, OwnDMProvenancePort, r2aHopPublication, *r2aWireGate) {
	t.Helper()
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopListener)
	cfg := deriveTestAdvertisePort(configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), BootstrapPeers: []string{normalizeAddress(address)}, AllowPrivatePeers: true, Type: domain.NodeTypeFull}))
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	s, port := NewInactiveOwnDMProvenanceCandidate(cfg, id, nil)
	gate := &r2aWireGate{entered: make(chan struct{}), release: make(chan struct{})}
	actualDial := s.peerTCPDial
	s.peerTCPDial = func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
		c, err := actualDial(ctx, address, timeout)
		if err != nil {
			return nil, err
		}
		return &r2aGatedConn{Conn: c, gate: gate, stopped: make(chan struct{})}, nil
	}
	if setup != nil {
		setup(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, stop := startTestService(t, ctx, cancel, s)
	t.Cleanup(stop)
	t.Cleanup(gate.unblock)
	var publication r2aHopPublication
	waitForConditionMsg(t, 10*time.Second, "candidate real CM capsule never became ready", func() bool {
		s.peerMu.RLock()
		defer s.peerMu.RUnlock()
		for _, session := range s.sessions {
			if hop := session.r2OwnDMHop; hop != nil && hop.r2PublishedReady && hop.session == session && session.selectedGeneration != 0 {
				publication = r2aHopPublication{sealed: hop}
				return true
			}
		}
		return false
	})
	return s, port, publication, gate
}
func ownDMWaitEvidence(t *testing.T, receipt OwnDMSubmission) OwnDMWriteEvidence {
	t.Helper()
	var evidence OwnDMWriteEvidence
	waitForConditionMsg(t, 3*time.Second, "candidate genuine first write did not settle", func() bool { var err error; evidence, err = receipt.TrySettle(t.Context()); return err == nil })
	return evidence
}
func ownDMRecipient(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestOwnDMPortCandidateActualWriteAndOneShot(t *testing.T) {
	s, port, publication, gate := ownDMCandidateManaged(t, nil)
	recipient := ownDMRecipient(t)
	e := r2aEnvelope(t, s, recipient)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
	gate.armed.Store(true)
	reply, receipt, err := port.Submit(t.Context(), e)
	if err != nil || reply.Type != "message_stored" || reply.ID != string(e.ID) || receipt.r2Submission == nil {
		t.Fatalf("genuine candidate admission failed: %+v %v", reply, err)
	}
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("candidate actual writer never reached gate")
	}
	for i := 0; i < 3; i++ {
		if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMNotWritten) {
			t.Fatal("queue acceptance replaced real writer ACK", err)
		}
	}
	gate.unblock()
	evidence := ownDMWaitEvidence(t, receipt)
	if evidence.RecipientIdentity().String() != recipient.Address || evidence.NearestHopIdentity() != publication.sealed.binding.identity || evidence.RecipientIdentity() == evidence.NearestHopIdentity() {
		t.Fatal("candidate recipient X conflated with actual hop Y")
	}
	if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMProvenanceUnavailable) {
		t.Fatal("candidate receipt consumed twice", err)
	}
	if s.secureSessions.store.stats().RelationAllocations != 0 {
		t.Fatal("provenance-only candidate staged R")
	}
}

func TestOwnDMPortOriginalOwnersSurviveNoRebinding(t *testing.T) {
	for _, which := range []string{"recipient", "hop", "cancel", "disconnect", "failed_write"} {
		t.Run(which, func(t *testing.T) {
			s, port, publication, gate := ownDMCandidateManaged(t, nil)
			recipient, address, stop := listenerNode(t, withoutOwnDials(nil))
			t.Cleanup(stop)
			if err := dialWithIntent(t, s, domain.PeerAddress(address), domain.DialToAddress()); err != nil {
				t.Fatal(err)
			}
			e := r2aEnvelope(t, s, recipient.identity)
			r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
			gate.armed.Store(true)
			_, receipt, err := port.Submit(t.Context(), e)
			if err != nil || receipt.r2Submission == nil {
				t.Fatal("original candidate missing", err)
			}
			select {
			case <-gate.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("original writer not gated")
			}
			if which == "recipient" {
				r2aRemoveActualPin(t, s, domain.PeerIdentityFromWire(e.Recipient))
			}
			if which == "hop" {
				r2aRemoveActualPin(t, s, publication.sealed.binding.identity)
			}
			if which == "cancel" {
				if _, err := s.CancelOutgoingDelivery(e.ID, domain.PeerIdentityFromWire(e.Recipient)); err != nil {
					t.Fatal(err)
				}
			}
			if which == "failed_write" {
				if err := publication.sealed.session.Close(); err != nil {
					t.Fatal(err)
				}
			}
			gate.unblock()
			if which == "cancel" || which == "failed_write" {
				if _, err := receipt.TrySettle(t.Context()); err == nil {
					t.Fatal("cancelled/failed actual writer yielded evidence")
				}
				return
			}
			candidate := receipt.r2Submission.r2Result.stamp.candidate
			select {
			case <-candidate.ack:
			case <-time.After(3 * time.Second):
				t.Fatal("real write did not complete")
			}
			if which == "disconnect" {
				if err := publication.sealed.session.Close(); err != nil {
					t.Fatal(err)
				}
				ownDMWaitEvidence(t, receipt)
			} else if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMProvenanceUnavailable) {
				t.Fatal("original X/Y/cancel rebound after actual write", err)
			}
		})
	}
}

func TestOwnDMPortStorageCancellationAndOriginalAbsentX(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "C3_during_storage", true: "cancel_after_storage"}[cancelled], func(t *testing.T) {
			recipient, address, stop := listenerNode(t, withoutOwnDials(nil))
			t.Cleanup(stop)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			store := NewMockMessageStore(t)
			var owner *Service
			store.EXPECT().StoreMessage(mock.Anything, true).RunAndReturn(func(e protocol.Envelope, _ bool) StoreResult {
				if cancelled {
					cancel()
				} else {
					if err := dialWithIntent(t, owner, domain.PeerAddress(address), domain.DialToAddress()); err != nil {
						t.Fatal(err)
					}
					r2aRemoveActualPin(t, owner, domain.PeerIdentityFromWire(e.Recipient))
				}
				return StoreInserted
			}).Once()
			s, port, publication, _ := ownDMCandidateManaged(t, func(s *Service) { owner = s; s.RegisterMessageStore(store) })
			e := r2aEnvelope(t, s, recipient.identity)
			r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
			reply, receipt, err := port.Submit(ctx, e)
			if err != nil || reply.Type != "message_stored" || reply.ID != string(e.ID) {
				t.Fatalf("performed storage hidden by optional provenance: %+v %v", reply, err)
			}
			if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMProvenanceUnavailable) {
				t.Fatal("original absent X/cancel observation recaptured after storage", err)
			}
		})
	}
}

func TestOwnDMPortRequiresGenuineCMReadyAndOriginalSession(t *testing.T) {
	for _, change := range []string{"not_ready", "wrong_session", "closed"} {
		t.Run(change, func(t *testing.T) {
			s, port, publication, _ := ownDMCandidateManaged(t, nil)
			e := r2aEnvelope(t, s, ownDMRecipient(t))
			r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
			s.peerMu.Lock()
			hop := publication.sealed
			if change == "not_ready" {
				hop.r2PublishedReady = false
			}
			if change == "wrong_session" {
				hop.session = &peerSession{}
			}
			s.peerMu.Unlock()
			if change == "closed" {
				if err := publication.sealed.session.Close(); err != nil {
					t.Fatal(err)
				}
			}
			reply, receipt, err := port.Submit(t.Context(), e)
			if err != nil || reply.Type != "message_stored" {
				t.Fatal("ordinary send was suppressed", reply, err)
			}
			s.deliveryMu.RLock()
			captured := receipt.r2Submission != nil && receipt.r2Submission.r2Result.stamp != nil && receipt.r2Submission.r2Result.stamp.candidate != nil
			s.deliveryMu.RUnlock()
			if captured {
				t.Fatal("unpublished/replaced/closed CM session acquired first-item candidate")
			}
			if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMNotWritten) {
				t.Fatal("unpublished/replaced/closed CM session lent evidence", err)
			}
		})
	}
}

func TestOwnDMPortDefaultRawDuplicateAndForeignRemainNoOrigin(t *testing.T) {
	s, port, _, _ := ownDMCandidateManaged(t, nil)
	other := NewService(configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t)}), ownDMRecipient(t), nil)
	if port.ValidFor(other, s.identity) || port.ValidFor(s, other.identity) || (OwnDMProvenancePort{}).ValidFor(s, s.identity) {
		t.Fatal("foreign/zero port validated")
	}
	if other.r2OwnDMOwner != nil {
		t.Fatal("normal node constructor minted own-DM origin")
	}
	e := r2aEnvelope(t, s, ownDMRecipient(t))
	reply := s.storeMessageFrame(ownDMFrame(e))
	if reply.Type != "message_stored" {
		t.Fatal(reply)
	}
	_, receipt, err := port.Submit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMProvenanceUnavailable) {
		t.Fatal("generic duplicate promoted to typed authority", err)
	}
	s.deliveryMu.RLock()
	entry := s.awaitingDelivered[e.ID]
	origin := entry != nil && entry.outgoingProvenance != nil
	s.deliveryMu.RUnlock()
	if origin {
		t.Fatal("raw stored owner gained provenance")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := port.Submit(ctx, r2aEnvelope(t, s, ownDMRecipient(t))); !errors.Is(err, context.Canceled) {
		t.Fatal("pre-store cancellation not returned", err)
	}
}

func TestOwnDMPortResourceInventoryNoAggregateAcceptance(t *testing.T) {
	t.Logf("relative fixed values only: port=%d owner=%d receipt=%d retained_submission=%d evidence=%d session_hop_pointer=%d; excludes allocation/backing/ACK channel/existing outbox and is not RSS admission", unsafe.Sizeof(OwnDMProvenancePort{}), unsafe.Sizeof(ownDMProvenanceOwner{}), unsafe.Sizeof(OwnDMSubmission{}), unsafe.Sizeof(ownDMSubmission{}), unsafe.Sizeof(OwnDMWriteEvidence{}), unsafe.Sizeof((*r2aPublishedHop)(nil)))
	if unsafe.Sizeof(OwnDMProvenancePort{}) != unsafe.Sizeof(uintptr(0)) || unsafe.Sizeof(OwnDMSubmission{}) != unsafe.Sizeof(uintptr(0)) {
		t.Fatal("opaque handles grew an index/queue")
	}
}

func TestOwnDMPortActualCMRegisterBarrierCannotPublishEarly(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "actual_Ready", true: "closed_before_register"}[closed], func(t *testing.T) {
			_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
			t.Cleanup(stopListener)
			cfg := deriveTestAdvertisePort(configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), BootstrapPeers: []string{normalizeAddress(address)}, AllowPrivatePeers: true, Type: domain.NodeTypeFull}))
			s, port := NewInactiveOwnDMProvenanceCandidate(cfg, ownDMRecipient(t), nil)
			ctx, cancel := context.WithCancel(context.Background())
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			s.cmSessionRegisterBarrier = func() {
				enteredOnce.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			_, stop := startTestService(t, ctx, cancel, s)
			t.Cleanup(stop)
			t.Cleanup(unblock)
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("actual CM never reached pre-registration barrier")
			}
			var session *peerSession
			s.connManager.mu.RLock()
			for _, slot := range s.connManager.slots {
				if slot.Session != nil {
					session = slot.Session
					break
				}
			}
			s.connManager.mu.RUnlock()
			if session == nil || session.r2OwnDMHop == nil {
				t.Fatal("original actual transport capsule missing before CM wait")
			}
			s.peerMu.RLock()
			early := session.r2OwnDMHop.r2PublishedReady
			s.peerMu.RUnlock()
			if early {
				t.Fatal("capsule published before actual CM registration")
			}
			s.publishR2OwnDMHop(session)
			s.peerMu.RLock()
			early = session.r2OwnDMHop.r2PublishedReady
			s.peerMu.RUnlock()
			if early {
				t.Fatal("manual publication bypassed actual CM membership")
			}
			e := r2aEnvelope(t, s, ownDMRecipient(t))
			r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), r2aHopPublication{sealed: session.r2OwnDMHop})
			_, receipt, err := port.Submit(t.Context(), e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMNotWritten) {
				t.Fatal("pre-Ready ordinary queue fabricated first write", err)
			}
			if closed {
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
			}
			unblock()
			if !closed {
				waitForConditionMsg(t, 3*time.Second, "actual CM Ready did not publish original capsule", func() bool {
					s.peerMu.RLock()
					defer s.peerMu.RUnlock()
					return session.r2OwnDMHop.r2PublishedReady && s.sessions[session.address] == session
				})
			}
			if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMNotWritten) {
				t.Fatal("later Ready/retry repaired an uncaptured first item", err)
			}
			if closed {
				s.peerMu.RLock()
				published := session.r2OwnDMHop.r2PublishedReady
				s.peerMu.RUnlock()
				if published {
					t.Fatal("closed original CM capsule published")
				}
			}
		})
	}
}

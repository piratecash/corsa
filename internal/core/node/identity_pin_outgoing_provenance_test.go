package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type r2aAdmissionProbeRouter struct {
	service      *Service
	observed     bool
	beforeReturn func(protocol.Envelope)
}

func (r *r2aAdmissionProbeRouter) Route(envelope protocol.Envelope) RoutingDecision {
	r.service.deliveryMu.RLock()
	r.observed = r.service.awaitingDelivered[messageIdentityOf(envelope)] != nil
	r.service.deliveryMu.RUnlock()
	if r.beforeReturn != nil {
		r.beforeReturn(envelope)
	}
	return NewGossipRouter(r.service).Route(envelope)
}
func r2aEnvelope(t *testing.T, s *Service, recipient *identity.Identity) protocol.Envelope {
	t.Helper()
	id, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{ID: id, Topic: "dm", Sender: s.Address(), Recipient: recipient.Address, Flag: protocol.MessageFlagSenderDelete, CreatedAt: time.Now().UTC(), Payload: []byte(sealDMBody(t, s.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey)))}
}
func TestR2aAcceptedOriginPrecedesActualRoute(t *testing.T) {
	s := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	envelope := r2aEnvelope(t, s, recipient)
	probe := &r2aAdmissionProbeRouter{service: s}
	s.router = probe
	prepared, err := s.prepareR2aFixtureOwnDM(t.Context(), envelope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.submitR2aFixtureOwnDM(t.Context(), prepared, r2aHopPublication{})
	if err != nil || !result.stored || result.code != "" {
		t.Fatalf("real signed message refused: %+v %v", result, err)
	}
	if !probe.observed {
		t.Fatal("accepted own-message origin escaped to router before its original delivery owner existed")
	}
	if domain.PeerIdentityFromWire(envelope.Recipient).IsZero() {
		t.Fatal("fixture recipient absent")
	}
}

func TestR2aOwnedPayloadAndOrdinaryNoOrigin(t *testing.T) {
	s := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	original := append([]byte(nil), e.Payload...)
	prepared, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	for i := range e.Payload {
		e.Payload[i] = 'z'
	}
	store := NewMockMessageStore(t)
	store.EXPECT().StoreMessage(mock.Anything, true).RunAndReturn(func(stored protocol.Envelope, _ bool) StoreResult {
		if !bytes.Equal(stored.Payload, original) {
			t.Error("caller payload altered admitted signature/body")
		}
		for i := range stored.Payload {
			stored.Payload[i] = 'q'
		}
		return StoreInserted
	}).Once()
	// This Service is not running; install immutable constructor dependency.
	s.messageStore = store
	result, err := s.submitR2aFixtureOwnDM(t.Context(), prepared, r2aHopPublication{})
	if err != nil || !result.stored || result.stamp == nil {
		t.Fatalf("private body admission %+v %v", result, err)
	}
	if result.stamp.prepared.digest != sha256.Sum256(original) {
		t.Fatal("mutable StoreMessage payload became authority")
	}
	s.deliveryMu.RLock()
	entry := s.awaitingDelivered[messageIdentityOf(e)]
	s.deliveryMu.RUnlock()
	if !bytes.Equal(entry.Envelope.Payload, original) {
		t.Fatal("store payload corrupted ordinary delivery bytes")
	}
	// A real generic local send still has no origin, even from the same author.
	s.messageStore = nil
	next := r2aEnvelope(t, s, recipient)
	_, _, code := s.storeIncomingMessage(incomingMessage{ID: next.ID, Topic: next.Topic, Sender: next.Sender, Recipient: next.Recipient, Flag: next.Flag, CreatedAt: next.CreatedAt, Body: string(next.Payload)}, true)
	if code != "" {
		t.Fatal(code)
	}
	s.deliveryMu.RLock()
	ordinary := s.awaitingDelivered[messageIdentityOf(next)].outgoingProvenance
	s.deliveryMu.RUnlock()
	if ordinary != nil {
		t.Fatal("generic send minted provenance")
	}
}
func TestR2aGenericDuplicateAndIdempotentTypedOwner(t *testing.T) {
	s := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.submitR2aFixtureOwnDM(t.Context(), p, r2aHopPublication{})
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.submitR2aFixtureOwnDM(t.Context(), p, r2aHopPublication{})
	if err != nil || again.stored || again.stamp != first.stamp {
		t.Fatalf("typed owner changed: %+v %v", again, err)
	}
	generic := r2aEnvelope(t, s, recipient)
	_, _, code := s.storeIncomingMessage(incomingMessage{ID: generic.ID, Topic: generic.Topic, Sender: generic.Sender, Recipient: generic.Recipient, Flag: generic.Flag, CreatedAt: generic.CreatedAt, Body: string(generic.Payload)}, true)
	if code != "" {
		t.Fatal(code)
	}
	gp, err := s.prepareR2aFixtureOwnDM(t.Context(), generic)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := s.submitR2aFixtureOwnDM(t.Context(), gp, r2aHopPublication{})
	if err != nil || duplicate.stamp != nil {
		t.Fatal("generic duplicate retroactively promoted", err)
	}
	if _, err := s.CancelOutgoingDelivery(e.ID, domain.PeerIdentityFromWire(e.Recipient)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.settleR2aOwnHop(t.Context(), first); !errors.Is(err, errR2aOrigin) {
		t.Fatal("withdrawn owner revived", err)
	}
}
func TestR2aCancellationAfterStorageRetainsHonestOrdinaryEffect(t *testing.T) {
	s := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := NewMockMessageStore(t)
	store.EXPECT().StoreMessage(mock.Anything, true).RunAndReturn(func(protocol.Envelope, bool) StoreResult { cancel(); return StoreInserted }).Once()
	s.messageStore = store
	result, err := s.submitR2aFixtureOwnDM(ctx, p, r2aHopPublication{})
	if !result.stored || result.stamp != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("wrong applied/canceled axes: %+v %v", result, err)
	}
	s.deliveryMu.RLock()
	entry := s.awaitingDelivered[messageIdentityOf(e)]
	s.deliveryMu.RUnlock()
	if entry == nil || entry.outgoingProvenance != nil {
		t.Fatal("ordinary committed delivery lost or minted canceled origin")
	}
}
func TestR2aIncomingTenThousandAndForeignPreparedNeverOptIn(t *testing.T) {
	s := newTestService(t, config.NodeTypeFull)
	foreign := newTestService(t, config.NodeTypeFull)
	e := r2aEnvelope(t, foreign, s.identity)
	// Identical signed ciphertext can legitimately be carried under many unsigned
	// IDs. Neither incoming content nor the caller's self-looking strings grants
	// the private local submission capability.
	for i := 0; i < 10000; i++ {
		if _, err := s.prepareR2aFixtureOwnDM(t.Context(), e); !errors.Is(err, errR2aOrigin) {
			t.Fatal("incoming DM minted local origin")
		}
	}
	prepared, err := foreign.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.submitR2aFixtureOwnDM(t.Context(), prepared, r2aHopPublication{}); !errors.Is(err, errR2aOrigin) {
		t.Fatal("foreign runtime preparation accepted")
	}
	if len(s.awaitingDelivered) != 0 {
		t.Fatal("rejected incoming admission allocated owner")
	}
}

func TestR2aCancellationDuringOriginalOwnerLockWait(t *testing.T) {
	s := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	files := s.secureSessions.store
	store := NewMockMessageStore(t)
	store.EXPECT().StoreMessage(mock.Anything, true).RunAndReturn(func(protocol.Envelope, bool) StoreResult { files.mu.Lock(); close(entered); return StoreInserted }).Once()
	s.messageStore = store
	type answer struct {
		result r2aSubmissionResult
		err    error
	}
	done := make(chan answer, 1)
	go func() { r, e := s.submitR2aFixtureOwnDM(ctx, p, r2aHopPublication{}); done <- answer{r, e} }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("real storage callback not entered")
	}
	cancel()
	files.mu.Unlock()
	select {
	case out := <-done:
		if !out.result.stored || out.result.stamp != nil || !errors.Is(out.err, context.Canceled) {
			t.Fatalf("canceled lock wait minted origin: %+v %v", out.result, out.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled origin registration did not drain")
	}
}
func TestR2aCancelAtActualRouteCannotRecreateOriginalOwner(t *testing.T) {
	s := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	probe := &r2aAdmissionProbeRouter{service: s, beforeReturn: func(e protocol.Envelope) {
		if _, err := s.CancelOutgoingDelivery(e.ID, domain.PeerIdentityFromWire(e.Recipient)); err != nil {
			t.Error(err)
		}
	}}
	s.router = probe
	result, err := s.submitR2aFixtureOwnDM(t.Context(), p, r2aHopPublication{})
	if err != nil || !result.stored || !probe.observed {
		t.Fatalf("accept/cancel ordering: %+v %v", result, err)
	}
	s.deliveryMu.RLock()
	entry := s.awaitingDelivered[messageIdentityOf(e)]
	s.deliveryMu.RUnlock()
	if entry != nil {
		t.Fatal("late normal registration recreated canceled own-message owner")
	}
	if _, _, err := s.settleR2aOwnHop(t.Context(), result); !errors.Is(err, errR2aOrigin) {
		t.Fatal("old result survived original owner cancellation", err)
	}
}
func TestR2aStructuralCostIsPerExistingOwnerNotGlobalAdmission(t *testing.T) {
	t.Logf("relative bytes: pointer=%d stamp=%d prepared=%d candidate=%d hop=%d; excludes channel allocation, strings/body backing, allocator overhead and existing outbox working set", unsafe.Sizeof((*r2aOwnSendStamp)(nil)), unsafe.Sizeof(r2aOwnSendStamp{}), unsafe.Sizeof(r2aPreparedEnvelope{}), unsafe.Sizeof(r2aFirstWrite{}), unsafe.Sizeof(r2aPublishedHop{}))
}

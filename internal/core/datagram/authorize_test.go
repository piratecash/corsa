package datagram

import (
	"context"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// authorize_test.go covers the §7 hook contract itself; its pipeline
// consequences — "reject does not commit the key", "an unknown dtype never
// reaches it", "it runs on all three planes" — are asserted in the pipeline
// tests, where the commit is observable.

// TestTypeWithoutAHookIsAuthorizedTrivially is the §7 default, expressed once
// so no call site has to remember it.
func TestTypeWithoutAHookIsAuthorizedTrivially(t *testing.T) {
	registry := NewTypeRegistry()
	if err := registry.Register(routedType(dtypePush, acceptingHandler())); err != nil {
		t.Fatalf("Register: %v", err)
	}
	entry, _ := registry.Lookup(dtypePush)
	if _, present := entry.Authorizer(); present {
		t.Fatal("a type without a hook must report absence, not a null object")
	}
	decision := authorizeLocalDelivery(context.Background(), entry, DeliveryContext{}, nil)
	if !decision.Accepted() {
		t.Fatalf("a type without a hook authorizes trivially, got %s", decision.Outcome())
	}
}

// TestZeroDecisionIsAReject pins the zero value: "accepted by omission" must
// never be inferrable.
func TestZeroDecisionIsAReject(t *testing.T) {
	if (AuthorizationDecision{}).Accepted() {
		t.Fatal("an unset decision must not admit a frame")
	}
	if got := (AuthorizationDecision{}).Outcome(); got != AuthorizationUnset {
		t.Fatalf("outcome %s", got)
	}
	if !Accept().Accepted() || Reject(errors.New("no")).Accepted() {
		t.Fatal("the two constructors disagree with their names")
	}
}

// TestDeliveryContextCarriesTheContextFields pins the §7 ctx: where the frame
// came from and our own address — the two facts push_identity's rule cannot be
// checked without.
//
// The ingress is built with ProvenIngress and not with the weakest constructor,
// because Identity() is the PROVEN accessor: a claimed ingress answers false
// there by design, and a fixture that used one would be asserting the absence of
// a fact instead of the presence of one.
func TestDeliveryContextCarriesTheContextFields(t *testing.T) {
	header, err := NewDeliveryHeader(requestFrame(t, requestOpts{
		label: newLabel(t, "ctx"), dst: domaintest.ID("dst"),
	}))
	if err != nil {
		t.Fatalf("NewDeliveryHeader: %v", err)
	}
	peer := domaintest.ID("neighbour")
	delivery, err := NewDeliveryContext(DeliveryContextOpts{
		Header:        header,
		IncomingPeer:  ProvenIngress(testChannel("neighbour"), peer),
		LocalIdentity: domaintest.ID("me"),
	})
	if err != nil {
		t.Fatalf("NewDeliveryContext: %v", err)
	}
	got, remote := delivery.IncomingPeer().Identity()
	if !remote || got != peer {
		t.Fatalf("incoming peer %v/%v", got, remote)
	}
	if delivery.LocalIdentity() != domaintest.ID("me") {
		t.Fatalf("local identity %v", delivery.LocalIdentity())
	}

	// A context without an incoming peer or without our own address is refused
	// at construction: a hook that cannot tell who sent the frame would have to
	// fall back to header.src, which on this plane is a label.
	if _, err := NewDeliveryContext(DeliveryContextOpts{
		Header: header, LocalIdentity: domaintest.ID("me"),
	}); err == nil {
		t.Fatal("a context without an incoming peer must be refused")
	}
	if _, err := NewDeliveryContext(DeliveryContextOpts{
		Header: header, IncomingPeer: LocalIngress(),
	}); err == nil {
		t.Fatal("a context without the local identity must be refused")
	}
}

// TestRegistryGateRefusesForeignModesAndClasses is the gate that stands
// immediately before the hook, and the reason mode demotion (§3.6) is harmless.
func TestRegistryGateRefusesForeignModesAndClasses(t *testing.T) {
	registry := NewTypeRegistry()
	if err := registry.Register(TypeRegistration{
		DType:   dtypeUnrelated,
		Modes:   []domain.DatagramMode{domain.DatagramModeRouted},
		Classes: []domain.DatagramClass{domain.DatagramClassBulk},
		Handler: acceptingHandler(),
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	entry, _ := registry.Lookup(dtypeUnrelated)

	if got := admitRegisteredFrame(entry, domain.DatagramModeRouted, domain.DatagramClassBulk); got != DropReasonUnset {
		t.Fatalf("the declared pair must be admitted, got %s", got)
	}
	if got := admitRegisteredFrame(entry, domain.DatagramModeRequest, domain.DatagramClassControl); got != DropModeNotAllowedForType {
		t.Fatalf("an undeclared mode must be refused, got %s", got)
	}
	if got := admitRegisteredFrame(entry, domain.DatagramModeRouted, domain.DatagramClassControl); got != DropClassNotAllowedForType {
		t.Fatalf("an undeclared class must be refused, got %s", got)
	}
}

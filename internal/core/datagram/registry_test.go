package datagram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// registry_test.go covers §7 and the release invariant of §10.

func TestRegistryRejectsPairingOutsideResponseTypes(t *testing.T) {
	registry := NewTypeRegistry()

	err := registry.Register(TypeRegistration{
		DType:     dtypeQuery,
		Modes:     []domain.DatagramMode{domain.DatagramModeRequest},
		Classes:   []domain.DatagramClass{domain.DatagramClassControl},
		AnswersTo: []domain.DType{dtypeAnswer},
		Handler:   acceptingHandler(),
	})
	if !errors.Is(err, ErrTypePairingForbidden) {
		t.Fatalf("want ErrTypePairingForbidden, got %v", err)
	}

	err = registry.Register(TypeRegistration{
		DType:   dtypeAnswer,
		Modes:   []domain.DatagramMode{domain.DatagramModeResponse},
		Classes: []domain.DatagramClass{domain.DatagramClassControl},
		Handler: acceptingHandler(),
	})
	if !errors.Is(err, ErrTypePairingRequired) {
		t.Fatalf("want ErrTypePairingRequired, got %v", err)
	}
}

func TestRegistryRefusesModeClassCombinationOutsideTheMatrix(t *testing.T) {
	registry := NewTypeRegistry()

	// §2.1 admits only `control` on the request plane, so a request type that
	// declares bulk alone could never receive a lawful frame.
	err := registry.Register(TypeRegistration{
		DType:   dtypeQuery,
		Modes:   []domain.DatagramMode{domain.DatagramModeRequest},
		Classes: []domain.DatagramClass{domain.DatagramClassBulk},
		Handler: acceptingHandler(),
	})
	if !errors.Is(err, ErrTypeRegistrationInvalid) {
		t.Fatalf("want ErrTypeRegistrationInvalid, got %v", err)
	}
}

func TestRegistryHoldsASetOfClasses(t *testing.T) {
	registry := NewTypeRegistry()
	if err := registry.Register(TypeRegistration{
		DType:   dtypeUnrelated,
		Modes:   []domain.DatagramMode{domain.DatagramModeRouted},
		Classes: []domain.DatagramClass{domain.DatagramClassControl, domain.DatagramClassBulk},
		Handler: acceptingHandler(),
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	entry, ok := registry.Lookup(dtypeUnrelated)
	if !ok {
		t.Fatal("registered type not found")
	}
	// §7: file_transfer is accepted both as control and as bulk, because its
	// small progress frames must not queue behind its own large answers.
	if !entry.AllowsClass(domain.DatagramClassControl) || !entry.AllowsClass(domain.DatagramClassBulk) {
		t.Fatal("both declared classes must be admitted")
	}
	if entry.AllowsMode(domain.DatagramModeRequest) {
		t.Fatal("an undeclared mode must not be admitted")
	}
}

// TestRegistryRefusesATypeSpanningBothPlanes is §3.6 made constructive: a type
// that accepts the same meaning authenticated AND unauthenticated hands the
// demotion path to any relay, and the registry is the last place that can
// notice before the type ships.
func TestRegistryRefusesATypeSpanningBothPlanes(t *testing.T) {
	registry := NewTypeRegistry()
	err := registry.Register(TypeRegistration{
		DType:   dtypeUnrelated,
		Modes:   []domain.DatagramMode{domain.DatagramModeRouted, domain.DatagramModeRequest},
		Classes: []domain.DatagramClass{domain.DatagramClassControl},
		Handler: acceptingHandler(),
	})
	if !errors.Is(err, ErrTypeMixesAuthenticatedPlanes) {
		t.Fatalf("want ErrTypeMixesAuthenticatedPlanes, got %v", err)
	}
	// The two unauthenticated modes together are fine: neither is signed, so
	// there is no guarantee to demote.
	if err := registry.Register(TypeRegistration{
		DType:     dtypeCached,
		Modes:     []domain.DatagramMode{domain.DatagramModeRequest, domain.DatagramModeResponse},
		Classes:   []domain.DatagramClass{domain.DatagramClassControl},
		AnswersTo: []domain.DType{dtypeQuery},
		Handler:   acceptingHandler(),
	}); err != nil {
		t.Fatalf("two unauthenticated planes must be allowed: %v", err)
	}
}

func TestRegistryRefusesASecondOwnerOfOneDType(t *testing.T) {
	registry := NewTypeRegistry()
	if err := registry.Register(routedType(dtypePush, acceptingHandler())); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := registry.Register(routedType(dtypePush, acceptingHandler())); !errors.Is(err, ErrTypeAlreadyRegistered) {
		t.Fatalf("want ErrTypeAlreadyRegistered, got %v", err)
	}
}

// TestRegistryDoesNotStoreAuthRequirement pins §7: whether auth is mandatory
// follows from the mode and is identical for every type, so there is no field
// for it — and the registry admits a routed type without saying anything about
// signatures.
func TestRegistryDoesNotStoreAuthRequirement(t *testing.T) {
	registry := NewTypeRegistry()
	if err := registry.Register(routedType(dtypePush, acceptingHandler())); err != nil {
		t.Fatalf("Register: %v", err)
	}
	entry, _ := registry.Lookup(dtypePush)
	rule, ok := domain.DatagramModeRuleFor(domain.DatagramModeRouted)
	if !ok || !rule.AuthRequired {
		t.Fatal("the mode matrix is the only source of the auth requirement")
	}
	if !entry.AllowsMode(domain.DatagramModeRouted) {
		t.Fatal("the routed type must accept its own mode")
	}
}

// TestPayloadSchemaIgnoresUnknownFieldsUnlikeTheHeader is the contract
// difference of §7 and §3.4 in one test: an unknown field in the PAYLOAD is
// ignored, an unknown field in the HEADER is a reject.
func TestPayloadSchemaIgnoresUnknownFieldsUnlikeTheHeader(t *testing.T) {
	schema := PayloadSchema{Name: "identity_record", Version: 2}
	var decoded struct {
		Address string `json:"address"`
	}
	if err := schema.Decode([]byte(`{"address":"abc","future_field":42}`), &decoded); err != nil {
		t.Fatalf("an additive payload field must be ignored, got %v", err)
	}
	if decoded.Address != "abc" {
		t.Fatalf("known payload fields must still decode, got %q", decoded.Address)
	}

	// The version is taken from the constant rather than written as a literal:
	// this test is about an unknown FIELD, so a frame that is stale in its
	// VERSION would report the wrong refusal and hide the contract under test.
	frame := map[string]any{
		"type": protocol.DatagramFrameType, "v": domain.DatagramHeaderVersion,
		"mode": "request", "class": "control",
		"src": domaintest.ID("label").String(), "dst": domaintest.ID("dst").String(),
		"ttl": 10, "route_policy": "best", "dtype": "get_identity", "payload": "",
		"future_field": 42,
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := protocol.ParseDatagramFrame(raw); !errors.Is(err, protocol.ErrDatagramUnknownField) {
		t.Fatalf("an unknown HEADER field must be rejected, got %v", err)
	}
}

// TestDeliveryHeaderHidesSrcInUnauthenticatedModes is the type-level statement
// of §7: in request and response there is no accessor that hands src over as a
// sender, so a hook cannot build a decision on it even by mistake.
func TestDeliveryHeaderHidesSrcInUnauthenticatedModes(t *testing.T) {
	label := newLabel(t, "one")
	target := domaintest.ID("target")

	request, err := NewDeliveryHeader(requestFrame(t, requestOpts{label: label, dst: target}))
	if err != nil {
		t.Fatalf("NewDeliveryHeader: %v", err)
	}
	if _, ok := request.SignedSrc(); ok {
		t.Fatal("a request has no authenticated src")
	}
	if got, ok := request.Label(); !ok || got != label {
		t.Fatalf("a request must expose its label, got %v/%v", got, ok)
	}
	if got, ok := request.Destination(); !ok || got != target {
		t.Fatalf("a request must expose its destination, got %v/%v", got, ok)
	}

	response, err := NewDeliveryHeader(responseFrame(t, responseOpts{label: label, subject: target}))
	if err != nil {
		t.Fatalf("NewDeliveryHeader: %v", err)
	}
	if _, ok := response.SignedSrc(); ok {
		t.Fatal("a response has no authenticated src")
	}
	if _, ok := response.Destination(); ok {
		t.Fatal("the dst of a response is an echoed label, not a destination")
	}
	subject, ok := response.Subject()
	if !ok || subject != target {
		t.Fatalf("a response must expose the subject that was asked, got %v/%v", subject, ok)
	}

	private, signer := newSigner(t)
	routed, err := NewDeliveryHeader(signedRouted(t, routedOpts{
		private: private, src: signer, dst: target, now: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatalf("NewDeliveryHeader: %v", err)
	}
	src, ok := routed.SignedSrc()
	if !ok || src != signer {
		t.Fatalf("a routed frame must expose its verified signer, got %v/%v", src, ok)
	}
	if _, ok := routed.Label(); ok {
		t.Fatal("the routed plane has no label")
	}
}

// TestTypeRegistryIsSafeUnderConcurrentRegistration pins what a bare map made
// impossible: Lookup and DTypes run on every session's receive
// goroutine, while Register may run at any time — the moment anything is
// registered lazily or behind a feature flag, a plain map is a data race.
//
// It is a `-race` test by construction: without the copy-on-write snapshot the
// detector reports the write against the concurrent reads.
func TestTypeRegistryIsSafeUnderConcurrentRegistration(t *testing.T) {
	t.Parallel()

	registry := NewTypeRegistry()
	names := make([]domain.DType, 0, 64)
	for i := 0; i < 64; i++ {
		names = append(names, domain.DType(fmt.Sprintf("probe_type_%02d", i)))
	}

	var readers sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, name := range names {
					registry.Lookup(name)
				}
				registry.DTypes()
			}
		}()
	}

	for _, name := range names {
		if err := registry.Register(TypeRegistration{
			DType:   name,
			Modes:   []domain.DatagramMode{domain.DatagramModeRouted},
			Classes: []domain.DatagramClass{domain.DatagramClassControl},
			Handler: acceptingTestHandler{},
		}); err != nil {
			t.Fatalf("Register(%s): %v", name, err)
		}
	}
	close(stop)
	readers.Wait()

	if got := len(registry.DTypes()); got != len(names) {
		t.Fatalf("registered %d types, want %d", got, len(names))
	}
}

// acceptingTestHandler is the minimal handler this test needs.
type acceptingTestHandler struct{}

func (acceptingTestHandler) Handle(context.Context, DeliveryContext, []byte) HandlerResult {
	return AcceptDelivery()
}

package node

import (
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/findnode"
	"testing"
	"time"
)

type readyRegisteredConstructor struct {
	t        *testing.T
	ready    *findNodeReadyConstruction
	original *findNodeReadyRegistered
	mode     string
}

func (c *readyRegisteredConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	r := c.ready
	if !r.registeredCaptured || r.originalRegisteredService != s || r.originalRegisteredSource != s.k3Registered || s.k3Registered == nil || r.finalized {
		c.t.Fatal("registered source was not captured in original firstService before callback")
	}
	c.original.OwnerCapacity = 1 // caller's tuple cannot mutate the copied request/source.
	if r.originalRegisteredValue.OwnerCapacity != 4 || s.k3Registered.ownerCapacity != 4 {
		c.t.Fatal("mutable caller request escaped")
	}
	switch c.mode {
	case "replace":
		s.k3Registered = newK3RegisteredSource(*r.registeredRequest)
	case "recapture":
		s.k3Registered = newK3RegisteredSource(*r.registeredRequest)
		r.captureRegisteredSource(s)
	case "nil-option":
		r.options.registered = nil
	case "tuple":
		r.options.registered.Window[0] ^= 1
	case "source-window":
		s.k3Registered.routeWireScope[0] ^= 1
	case "source-capacity":
		s.k3Registered.ownerCapacity--
	case "identity":
		copied := *s.identity
		s.identity = &copied
	case "caller-clock":
		r.registeredRequest.elapsed = func() (time.Duration, error) { return 0, nil }

	}
	return nil, nil
}
func TestOwnedReadyRegisteredCopiedBeforeCallbacks(t *testing.T) {
	cfg, id := readyTestInputs(t)
	r := readyTestPrepare(t, cfg, id)
	tuple := findNodeReadyRegistered{OwnerCapacity: 4, HistoryCapacity: 8, Instance: [16]byte{1}, Window: [32]byte{2}}
	cb := &readyRegisteredConstructor{t: t, ready: r, original: &tuple}
	s, err := r.buildWithOptions(cfg, id, nil, cb, findNodeReadyOptions{registered: &tuple})
	if err != nil || s == nil || !r.finalized || !r.bindings(s) || tuple.OwnerCapacity != 1 || s.k3Registered.ownerCapacity != 4 || r.registeredRequest.elapsed != nil {
		t.Fatal("copied trusted-clock Ready composition unavailable", err)
	}
	if r.originalRegisteredSource != s.k3Registered || r.originalRegisteredService != s {
		t.Fatal("another source/service was adopted")
	}
}
func TestOwnedReadyRegisteredRejectsReplacementAndRecapture(t *testing.T) {
	for _, mode := range []string{"replace", "recapture", "nil-option", "tuple", "source-window", "source-capacity", "caller-clock", "identity"} {
		t.Run(mode, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			r := readyTestPrepare(t, cfg, id)
			tuple := findNodeReadyRegistered{OwnerCapacity: 4, HistoryCapacity: 8, Instance: [16]byte{1}, Window: [32]byte{2}}
			_, err := r.buildWithOptions(cfg, id, nil, &readyRegisteredConstructor{t: t, ready: r, original: &tuple, mode: mode}, findNodeReadyOptions{registered: &tuple})
			if err == nil || r.finalized || r.originalContext.Err() == nil {
				t.Fatal("foreign/re-adopted source finalized", mode, err)
			}
			if mode == "recapture" && !r.registeredCaptureFailed {
				t.Fatal("recapture refusal lost after original tail installers")
			}
		})
	}
	if (*findNodeReadyConstruction)(nil).registeredBindings(nil) {
		t.Fatal("nil construction bound")
	}
}
func TestOwnedReadyRegisteredOrdinaryReadyRemainsAbsent(t *testing.T) {
	cfg, id := readyTestInputs(t)
	r := readyTestPrepare(t, cfg, id)
	s, err := r.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil || s.k3Registered != nil || r.registeredRequest != nil || r.originalRegisteredSource != nil || r.originalRegisteredService != nil || !r.bindings(s) {
		t.Fatal("default Ready acquired registered observer", err)
	}
}
func TestOwnedReadyRegisteredInvalidTupleRefusedBeforeFirstService(t *testing.T) {
	for _, mode := range []string{"zero-window", "zero-instance", "history", "capacity", "target-role"} {
		t.Run(mode, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			profile := ownedCutProfile()
			if mode == "target-role" {
				profile.LocalRole = findNodeRoleTarget
			}
			r, err := prepareFindNodeReadyConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(r.cleanup)
			tuple := findNodeReadyRegistered{OwnerCapacity: 4, HistoryCapacity: 8, Instance: [16]byte{1}, Window: [32]byte{2}}
			switch mode {
			case "zero-window":
				tuple.Window = [32]byte{}
			case "zero-instance":
				tuple.Instance = [16]byte{}
			case "history":
				tuple.HistoryCapacity = k3RegisteredMaxHistory + 1
			case "capacity":
				tuple.OwnerCapacity = 0
			}
			s, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, findNodeReadyOptions{registered: &tuple})
			if err == nil || s != nil || r.service != nil || r.finalized {
				t.Fatal("invalid observer tuple reached firstService", mode, err)
			}
		})
	}
}

// An unrequested source existed at the original pre-callback capture point.
// Restoring nil in a later callback cannot erase that construction refusal.
type readyUnrequestedRegisteredConstructor struct {
	t     *testing.T
	ready *findNodeReadyConstruction
}

func (c *readyUnrequestedRegisteredConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	if s != c.ready.service || s.k3Registered == nil || !c.ready.registeredCaptured || !c.ready.registeredCaptureFailed {
		c.t.Fatal("unrequested source not observed/refused before callback")
	}
	s.k3Registered = nil
	return nil, nil
}
func TestOwnedReadyRegisteredUnrequestedSourceRefusalStaysSticky(t *testing.T) {
	cfg, id := readyTestInputs(t)
	r := readyTestPrepare(t, cfg, id)
	request := &k3RegisteredRequest{OwnerCapacity: 4, HistoryCapacity: 8, Instance: [16]byte{1}, routeWireScope: [32]byte{2}}
	callback := &readyUnrequestedRegisteredConstructor{t: t, ready: r}
	s := newServiceWithConstruction(cfg, id, nil, nil, callback, serviceConstruction{bridge: r.bridge, ready: r, localFindNodeRole: r.localRole, k3Registered: request})
	if s == nil || s.k3Registered != nil || !r.registeredCaptureFailed || r.failure == nil || r.checked || r.finalized || r.bindings(s) || r.registeredBindings(s) {
		t.Fatal("unrequested source refusal was erased by restored nil/tail installer")
	}
}

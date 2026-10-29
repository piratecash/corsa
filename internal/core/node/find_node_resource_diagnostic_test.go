//go:build d1_resource_probe

package node

import (
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
)

func newD1ResourceForTest(t *testing.T) (*Service, *D1FindNodeResourceReadout) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", ListenerSet: true, ListenerEnabled: true, Type: domain.NodeTypeFull})
	s, r, err := NewD1FindNodeResourceDiagnostic(cfg, id, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s, r
}

func TestD1FindNodeResourceDiagnosticActualConstruction(t *testing.T) {
	s, r := newD1ResourceForTest(t)
	v := r.SnapshotForService(s)
	if !v.Available || v.UnknownReason != "" || v.Identity != s.Address() || !v.Owner.Enabled || !v.Owner.LocalEnabled || !v.IndependentPublications || v.ExternalClockBound || v.PhysicalTimeBoundsProven {
		t.Fatalf("missing actual runtime readout: %+v", v)
	}
	if !s.bridgeCapture.consumer.IsRuntime() || s.bridgeCapture.localLoop.signingGate != nil || s.inboundPins != nil || s.r2OwnDMOwner != nil {
		t.Fatal("wrong runtime construction options")
	}
	if v.MaxActive != 2 || v.MaxMembers != 2 || v.MaxRegistryVisits != 4 || v.TargetBudget != 2*time.Second || v.NetworkBudget != 2*time.Second || v.RuntimeBudget != time.Second || v.Round != 100*time.Millisecond || v.Slack != 100*time.Millisecond || v.InitialTokens != 4 || v.TargetServiceBound != 2*time.Second || v.TargetRuntimeReserve != 500*time.Millisecond || v.SigningSpacing != 250*time.Millisecond || v.SigningReservations != 8 {
		t.Fatal("fixed hypothesis changed", v)
	}
	f1 := 0
	for _, name := range s.nodeHelloFrame().Capabilities {
		if name == string(domain.CapOverlayFindNodeV1) {
			f1++
		}
	}
	if f1 != 1 {
		t.Fatal("pre-handshake diagnostic selection missing/duplicate", f1)
	}
	v.Owner.Intake.Offered = 999
	if r.SnapshotForService(s).Owner.Intake.Offered == 999 {
		t.Fatal("value copy mutated owner")
	}
	var valueOnly func(reflect.Type) bool
	valueOnly = func(typ reflect.Type) bool {
		switch typ.Kind() {
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				if !valueOnly(typ.Field(i).Type) {
					return false
				}
			}
			return true
		case reflect.Array:
			return valueOnly(typ.Elem())
		case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return true
		default:
			return false
		}
	}
	if !valueOnly(reflect.TypeOf(v)) {
		t.Fatal("DTO exposes reference/authority graph")
	}
}

func TestD1FindNodeResourceDiagnosticWrongOwnerAndClosed(t *testing.T) {
	s, r := newD1ResourceForTest(t)
	other, foreign := newD1ResourceForTest(t)
	check := func(name string, handle *D1FindNodeResourceReadout, expected *Service) {
		t.Helper()
		v := handle.SnapshotForService(expected)
		if v.Available || v.UnknownReason == "" || v.Owner != (findnode.ResourceSnapshot{}) {
			t.Fatal(name, "unknown binding read owner", v)
		}
	}
	check("nil", nil, s)
	check("nil expected", r, nil)
	check("foreign service", r, other)
	copied := *r
	copied.capture = foreign.capture
	check("cross runtime", &copied, s)
	copied = *r
	copied.captureValue.intake = findnode.DormantIntake{}
	check("intake mismatch", &copied, s)
	copied = *r
	copied.address = "foreign"
	check("identity mismatch", &copied, s)
	// A foreign empty Service has no coordinator. Refusal must precede reads.
	check("unbound coordinator", r, &Service{})
	s.runCancel()
	check("closed", r, s)
}

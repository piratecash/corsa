package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/testutil/loadstand/fullwitness"
)

func TestRouteWireCensusWitnessBindsRegistryToLaunch(t *testing.T) {
	_, binding, expected := routeWireBoundedFixture(t, RouteWireWindowBounds{Begin: 100, End: 200})
	plan := PlannedRouteWireObservation{Window: expected.Window, Bounds: expected.Bounds, MaxEndpoints: len(expected.Endpoints)}
	table := rpc.NewCommandTable()
	identity := binding.RuntimeBinding().Identity
	slot := &ChildFinalSlot{binding: binding.launch}
	var rows []RouteWireEndpointKey
	read := func() ([]RouteWireEndpointKey, error) { return rows, nil }
	clock := RouteWireClockDomain{BootID: "c481cc22-adf3-4d99-a4fe-de7f204e0ad6", Device: 4, Inode: 17}
	current := clock
	witness, err := NewRouteWireCensusWitness(slot, identity, plan, clock, func() (RouteWireClockDomain, error) { return current, nil }, read)
	if err != nil {
		t.Fatal(err)
	}
	if err := fullwitness.RegisterRouteWireCensus(table, RouteWireCensusCommand, witness); err != nil {
		t.Fatal(err)
	}
	if policy, ok := table.TransportPolicyFor(RouteWireCensusCommand); !ok || policy != rpc.TransportLoopbackOnly {
		t.Fatal("unexpected transport policy", policy)
	}
	if err := fullwitness.RegisterRouteWireCensus(table, RouteWireCensusCommand, witness); err == nil {
		t.Fatal("duplicate command registration", err)
	}
	request := rpc.CommandRequest{Name: RouteWireCensusCommand, Ctx: t.Context()}
	if response := table.Execute(request); response.ErrorKind != rpc.ErrUnavailable || response.Error == nil || response.Data != nil {
		t.Fatal("missing registry census was accepted", response)
	}
	rows = expected.Endpoints
	response := table.Execute(request)
	got, err := decodeRouteWireCensusWitness(response.Data, binding, plan, clock)
	want, commitErr := commitRouteWireWindow(expected, identity)
	if response.Error != nil || err != nil || commitErr != nil || got != want {
		t.Fatalf("witness %+v, want %+v, errors %v/%v/%v", got, want, response.Error, err, commitErr)
	}
	for _, tc := range []struct {
		name   string
		change func(*routeWireCensusWitness)
	}{
		{"other launch", func(w *routeWireCensusWitness) { w.Launch[0]++ }},
		{"other fingerprint", func(w *routeWireCensusWitness) { w.Fingerprint[0]++ }},
		{"other identity", func(w *routeWireCensusWitness) { w.Identity[0]++ }},
		{"other window", func(w *routeWireCensusWitness) { w.Census.Window[0]++ }},
		{"other bounds", func(w *routeWireCensusWitness) { w.Census.Bounds.End++ }},
		{"other boot", func(w *routeWireCensusWitness) { w.Clock.BootID = "d481cc22-adf3-4d99-a4fe-de7f204e0ad6" }},
		{"other time namespace", func(w *routeWireCensusWitness) { w.Clock.Inode++ }},
		{"over endpoint budget", func(w *routeWireCensusWitness) { w.Census.Count++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			witness := routeWireCensusWitness{Version: 1, Launch: binding.launch.nonce, Fingerprint: binding.launch.fingerprint, Identity: identity, Clock: clock, Census: want}
			tc.change(&witness)
			raw, err := json.Marshal(witness)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeRouteWireCensusWitness(raw, binding, plan, clock); !errors.Is(err, ErrFinalReportInvalid) {
				t.Fatal("accepted inconsistent witness", err)
			}
		})
	}
	modified := strings.Replace(string(response.Data), `"Census":`, `"census":`, 1)
	if _, err := decodeRouteWireCensusWitness([]byte(modified), binding, plan, clock); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatal("accepted noncanonical witness", err)
	}
	current.Inode++
	if response := table.Execute(request); response.ErrorKind != rpc.ErrUnavailable || response.Error == nil || response.Data != nil {
		t.Fatal("accepted changed clock domain", response)
	}
	current = clock
	request.Args = map[string]any{"window": "override"}
	if response := table.Execute(request); response.ErrorKind != rpc.ErrValidation || response.Error == nil || response.Data != nil {
		t.Fatal("accepted caller-selected window", response)
	}
	request.Args = nil
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request.Ctx = ctx
	if response := table.Execute(request); !errors.Is(response.Error, context.Canceled) || response.Data != nil {
		t.Fatal("accepted canceled query", response)
	}
}

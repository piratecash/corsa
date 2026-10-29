//go:build d1_resource_probe

package node

import (
	"testing"

	corenode "github.com/piratecash/corsa/internal/core/node"
)

func newD1ResourceAppForTest(t *testing.T) (*App, *D1FindNodeResourceReadout) {
	t.Helper()
	isolatePendingApp(t)
	t.Setenv("CORSA_LISTENER", "1")
	t.Setenv("CORSA_LISTEN_ADDRESS", "127.0.0.1:0")
	t.Setenv("CORSA_BOOTSTRAP_PEERS", "127.0.0.1:0")
	t.Setenv("CORSA_BOOTSTRAP_PEER", "")
	t.Setenv("CORSA_CONNECT_ONLY", "")
	t.Setenv("CORSA_PROXY", "")
	a, r, err := NewD1FindNodeResourceDiagnostic()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.service.WaitBackground)
	return a, r
}

func TestD1FindNodeResourceDiagnosticAppBinding(t *testing.T) {
	a, r := newD1ResourceAppForTest(t)
	v := r.SnapshotForApp(a)
	if !v.Available || v.Identity != a.service.Address() || r.service != a.service || r.core == nil || a.PendingObservation() != nil || a.TerminalObservation() != nil || a.GuardObservation() != nil {
		t.Fatal("App did not retain its original closed construction", v)
	}
	check := func(name string, handle *D1FindNodeResourceReadout, expected *App) {
		t.Helper()
		v := handle.SnapshotForApp(expected)
		if v.Available || v.UnknownReason == "" {
			t.Fatal(name, "unbound App read owner", v)
		}
	}
	check("nil", nil, a)
	check("nil App", r, nil)
	check("foreign App", r, &App{service: a.service})
	copyHandle := *r
	copyHandle.service = &corenode.Service{}
	check("substituted Service", &copyHandle, a)
	original := a.service
	a.service = &corenode.Service{}
	check("changed App Service", r, a)
	a.service = original
	ordinary := New()
	t.Cleanup(ordinary.service.WaitBackground)
	check("ordinary App", r, ordinary)
}

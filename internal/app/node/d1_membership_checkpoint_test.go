//go:build d1_resource_probe

package node

import (
	"strings"
	"testing"

	corenode "github.com/piratecash/corsa/internal/core/node"
)

func readD1AppCheckpoint(r *D1FindNodeResourceReadout, a *App) corenode.D1FindNodeCheckpoint {
	return r.CheckpointForApp(a)
}

func TestD1MembershipCheckpointAppBinding(t *testing.T) {
	a, r := newD1ResourceAppForTest(t)
	v := readD1AppCheckpoint(r, a)
	if !v.MembershipComplete || v.ServiceIdentity != a.service.Address() || v.ActiveCount != 0 || v.F1Count != 0 || !strings.HasPrefix(v.RetirementStatus, "UNKNOWN:") || !strings.HasPrefix(v.DrainStatus, "UNKNOWN:") {
		t.Fatal("original App membership unavailable", v)
	}
	for _, v := range []corenode.D1FindNodeCheckpoint{readD1AppCheckpoint(nil, a), readD1AppCheckpoint(r, nil), readD1AppCheckpoint(r, &App{service: a.service})} {
		if v.MembershipComplete || v.UnknownReason == "" {
			t.Fatal("foreign App checkpoint escaped", v)
		}
	}
	copyHandle := *r
	copyHandle.service = &corenode.Service{}
	if readD1AppCheckpoint(&copyHandle, a).MembershipComplete {
		t.Fatal("changed service accepted")
	}
}

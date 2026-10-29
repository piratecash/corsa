//go:build d1_resource_probe

package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	corenode "github.com/piratecash/corsa/internal/core/node"
)

// D1FindNodeResourceReadout is bound to the actual App and its original Service.
// It does not expose a Service, clock, profile setter or admission authority.
type D1FindNodeResourceReadout struct {
	app     *App
	service *corenode.Service
	core    *corenode.D1FindNodeResourceReadout
}

// NewD1FindNodeResourceDiagnostic is absent without d1_resource_probe. The
// stock headless config/identity/bus ordering is retained without first building
// and replacing an ordinary Service. No S6 observer is selected implicitly.
func NewD1FindNodeResourceDiagnostic() (*App, *D1FindNodeResourceReadout, error) {
	cfg := config.Default()
	cfg.Node.DisableDirectMessages = !config.AcceptDirectMessagesFromEnv()
	id, err := identity.LoadOrCreate(cfg.Node.IdentityPath)
	if err != nil {
		return nil, nil, err
	}
	bus := ebus.New()
	service, core, err := corenode.NewD1FindNodeResourceDiagnostic(cfg.Node, id, bus)
	if err != nil {
		bus.Shutdown()
		return nil, nil, err
	}
	a := &App{cfg: cfg, service: service}
	return a, &D1FindNodeResourceReadout{app: a, service: service, core: core}, nil
}

// SnapshotForApp returns UNKNOWN before owner reads for any foreign App.
func (r *D1FindNodeResourceReadout) SnapshotForApp(expected *App) corenode.D1FindNodeResourceSnapshot {
	if r == nil || expected == nil || r.app != expected || r.service == nil || expected.service != r.service || r.core == nil {
		return corenode.D1FindNodeResourceSnapshot{UnknownReason: "UNKNOWN: App/service binding unavailable"}
	}
	return r.core.SnapshotForService(r.service)
}

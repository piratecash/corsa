//go:build d1_resource_probe

package node

import corenode "github.com/piratecash/corsa/internal/core/node"

// CheckpointForApp preserves the exact original App/Service membership source.
// Retirement/drain remain UNKNOWN even when complete membership is empty.
func (r *D1FindNodeResourceReadout) CheckpointForApp(expected *App) corenode.D1FindNodeCheckpoint {
	if r == nil || expected == nil || r.app != expected || r.service == nil || expected.service != r.service || r.core == nil {
		return corenode.D1FindNodeCheckpoint{UnknownReason: "UNKNOWN: App/service binding unavailable", Scope: "registered-original-listener-small2", RetirementStatus: "UNKNOWN:no original completion barrier", DrainStatus: "UNKNOWN:not a drain barrier"}
	}
	return r.core.CheckpointForService(r.service)
}

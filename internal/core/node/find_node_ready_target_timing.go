package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"sync/atomic"
)

// Private close-only synchronization, never a configurable callback or result.
// Both channel identities are immutable before escape and checked at Seal/wait.
type ownedFindNodeSigningTestGate struct {
	arrived         chan struct{}
	release         <-chan struct{}
	originalArrived chan struct{}
	originalRelease <-chan struct{}
	entered         atomic.Bool
}

func (g *ownedFindNodeSigningTestGate) bindings() bool {
	return g != nil && g.arrived != nil && g.release != nil && cap(g.arrived) == 0 && cap(g.release) == 0 && g.arrived == g.originalArrived && g.release == g.originalRelease
}
func (g *ownedFindNodeSigningTestGate) wait(done <-chan struct{}) bool {
	if !g.bindings() {
		return false
	}
	if !g.entered.CompareAndSwap(false, true) {
		return true
	}
	close(g.arrived)
	select {
	case <-g.release:
		return true
	case <-done:
		return false
	}
}

// Explicit normal timed candidate: no test gate, config, public factory or capability.
func newFindNodeReadyTimedLoadedInboundCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile findNodeBridgeProfile, signing findnode.SigningProfile, timing findnode.OwnedTargetTimingProfile) (*Service, error) {
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, signing, timing)
	if err != nil {
		return nil, err
	}
	return r.buildWithOptions(cfg, id, bus, productionDatagramConstructor{}, findNodeReadyOptions{loadedInbound: true})
}

// Only this dedicated test construction can install the gate, before any callback/Seal.
// Supplied channels carry close signals only; no authority/result is transferred.
func prepareFindNodeReadyTimedTestConstruction(cfg config.Node, id *identity.Identity, profile findNodeBridgeProfile, signing findnode.SigningProfile, timing findnode.OwnedTargetTimingProfile, arrived chan struct{}, release <-chan struct{}) (*findNodeReadyConstruction, findNodeReadyOptions, error) {
	gate := &ownedFindNodeSigningTestGate{arrived: arrived, release: release, originalArrived: arrived, originalRelease: release}
	if !gate.bindings() {
		return nil, findNodeReadyOptions{}, findnode.ErrInvalid
	}
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, signing, timing)
	if err != nil {
		return nil, findNodeReadyOptions{}, err
	}
	return r, findNodeReadyOptions{loadedInbound: true, signingGate: gate}, nil
}

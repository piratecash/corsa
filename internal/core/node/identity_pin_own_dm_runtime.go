package node

import (
	"context"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Explicit internal candidate parameters, never configuration or defaults.
// Every limit/timing is supplied by the construction owner before escape.
type OwnDMRuntimeProfile struct {
	Residence         OwnDMResidenceProfile
	MaxActive         int
	MaxMembers        int
	MaxRegistryVisits int
	Intake            findnode.DormantIntakeProfile
	Signing           findnode.SigningProfile
}

type ownDMRuntimeConstruction struct {
	profile   OwnDMResidenceProfile
	owner     *ownDMProvenanceOwner
	resolver  *identityResolver
	admission *ownDMRecordAdmission
	port      OwnDMProvenancePort
}

// One original Service, runtime domain, store, publisher and cancellation.
// Cleanup cancels the EXISTING Ready lifecycle and closes its existing
// background gate before an unstarted owner joins; it creates no new scope.
// Normal SDK/Desktop construction does not select this candidate.
func NewInactiveOwnDMRuntimeCandidate(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile OwnDMRuntimeProfile) (*Service, OwnDMProvenancePort, context.CancelFunc, error) {
	if !profile.Residence.valid() || id == nil {
		return nil, OwnDMProvenancePort{}, nil, errOwnDMResidence
	}
	bridge := findNodeBridgeProfile{Capture: findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: profile.MaxActive}, Priority: findNodePriorityProfile{MaxMembers: profile.MaxMembers, MaxRegistryVisits: profile.MaxRegistryVisits}}, Intake: profile.Intake}
	ready, err := prepareFindNodeReadyConstruction(cfg, id, bridge, profile.Signing)
	if err != nil {
		return nil, OwnDMProvenancePort{}, nil, err
	}
	own := &ownDMRuntimeConstruction{profile: profile.Residence}
	s, err := ready.buildWithOwnDM(cfg, id, bus, productionDatagramConstructor{}, own)
	if err != nil {
		return nil, OwnDMProvenancePort{}, nil, err
	}
	return s, own.port, newUnstartedRuntimeCleanup(s, ready.originalCancel), nil
}

// Called once by the original Service builder before Ready's first check.
func (o *ownDMRuntimeConstruction) install(s *Service) {
	if o == nil || o.owner != nil || o.resolver != nil || o.admission != nil {
		panic("own-DM construction reused")
	}
	o.port = newOwnDMAdmissionPort(s, o.profile)
	o.owner, o.resolver, o.admission = s.r2OwnDMOwner, s.identityResolver, s.r2OwnDMOwner.r2Admission
}
func (o *ownDMRuntimeConstruction) bindings(s *Service) bool {
	return o != nil && s != nil && o.owner != nil && o.resolver != nil && o.admission != nil && o.profile.valid() && o.port.ValidFor(s, s.identity) && s.r2OwnDMOwner == o.owner && s.identityResolver == o.resolver && o.owner.r2Admission == o.admission && o.admission.service == s && o.admission.resolver == o.resolver && o.admission.admission == o.resolver.recordAdmission && o.admission.profile == o.profile && o.resolver.recordAdmission.classes == recordAdmissionChargedR
}

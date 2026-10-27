package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Explicit scalar allocation request only; there is no public clock provider,
// RNG-generated instance, deployment capacity or activation policy. Invalid
// capacities/zero Instance retain the original engine's UNKNOWN semantics.
type RegisteredProvenV2ObservationRequest struct {
	OwnerCapacity, HistoryCapacity int
	Instance                       [16]byte
}

// Instance is a copied caller label, never process/boot/artifact attribution.
type RegisteredProvenV2ObservationKey struct{ Instance [16]byte }

// These aliases expose only the engine's already-copied value DTOs, not its
// private owners/maps/pointers or a second interpretation/serialization format.
type RegisteredProvenV2ObservationClock = k3RegisteredClock
type RegisteredProvenV2ObservationOwnerRef = k3RegisteredOwnerRef
type RegisteredProvenV2ObservationTransition = k3RegisteredTransition
type RegisteredProvenV2ObservationSnapshot = k3RegisteredSnapshot

// Bound to one original Service and its pre-publication source. No installer,
// mutable request pointer, App worker, resource or Close obligation is added.
type RegisteredProvenV2Observation struct {
	service  *Service
	source   *k3RegisteredSource
	identity domain.PeerIdentity
	key      RegisteredProvenV2ObservationKey
}

func (o *RegisteredProvenV2Observation) Key() RegisteredProvenV2ObservationKey {
	if o == nil {
		return RegisteredProvenV2ObservationKey{}
	}
	return o.key
}

func (o *RegisteredProvenV2Observation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}

func (o *RegisteredProvenV2Observation) Snapshot() RegisteredProvenV2ObservationSnapshot {
	unknown := RegisteredProvenV2ObservationSnapshot{Population: "RegisteredProvenV2", Status: "UNKNOWN", Reason: "source_unavailable", Qualification: "UNKNOWN", PastOnly: true}
	if o == nil {
		return unknown
	}
	if o.service == nil || o.source == nil || o.service.k3Registered != o.source || o.source.instance != o.key.Instance || o.identity.IsZero() || o.service.identity == nil || domain.PeerIdentityFromWire(o.service.Address()) != o.identity {
		unknown.Reason = "source_binding_unavailable"
		return unknown
	}
	// Exactly one original Snapshot call. Its own clock/cut and immutable
	// prefix-copy behavior are unchanged; no extra observer clock or reducer.
	return o.service.k3RegisteredSnapshot()
}

func NewServiceWithRegisteredProvenV2Observation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, request RegisteredProvenV2ObservationRequest) (*Service, *RegisteredProvenV2Observation, error) {
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{RegisteredProvenV2: &request})
	return s, readouts.RegisteredProvenV2, err
}

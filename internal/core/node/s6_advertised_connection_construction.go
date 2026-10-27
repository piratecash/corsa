package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Explicit scalar allocation request only. Instance is an unverified caller
// label; there is no public clock, generated instance or deployment policy.
// Invalid requests retain the original source UNKNOWN/startup semantics.
type AdvertisedConnectionObservationRequest struct {
	ConnectionCapacity, SlotCapacity, HistoryCapacity int
	Instance                                          [16]byte
}

type AdvertisedConnectionObservationKey struct{ Instance [16]byte }

// Project existing value facts without another reducer or serialization.
// Raw zeros/conflicts/context uncertainty and Completed/Covered stay distinct.
type AdvertisedConnectionObservationClock = k3AdvertisedClock
type AdvertisedConnectionObservationPeerFact = k3AdvertisedPeerFact
type AdvertisedConnectionObservationSlotFact = k3AdvertisedSlotFact
type AdvertisedConnectionObservationFact = k3AdvertisedFact
type AdvertisedConnectionObservationSnapshot = k3AdvertisedSnapshot

// Bound to the one original Service/source/CM, installed before publication.
// No resource, Close obligation, worker or post-construction installer is added.
type AdvertisedConnectionObservation struct {
	service  *Service
	source   *k3AdvertisedSource
	manager  *ConnectionManager
	identity domain.PeerIdentity
	key      AdvertisedConnectionObservationKey
}

func (o *AdvertisedConnectionObservation) Key() AdvertisedConnectionObservationKey {
	if o == nil {
		return AdvertisedConnectionObservationKey{}
	}
	return o.key
}

func (o *AdvertisedConnectionObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}

func (o *AdvertisedConnectionObservation) Snapshot() AdvertisedConnectionObservationSnapshot {
	unknown := AdvertisedConnectionObservationSnapshot{Population: "AdvertisedConnections", Status: "UNKNOWN", Reason: "source_unavailable", Qualification: "UNKNOWN", PastOnly: true}
	if o == nil {
		return unknown
	}
	// These construction references and Instance are immutable. Mutable owner
	// maps/slots are never inspected here, and the secure store is unrelated.
	if o.service == nil || o.source == nil || o.manager == nil || o.service.k3Advertised != o.source || o.service.connManager != o.manager || o.manager.config.advertised != o.source || o.source.instance != o.key.Instance || o.identity.IsZero() || o.service.identity == nil || domain.PeerIdentityFromWire(o.service.Address()) != o.identity {
		unknown.Reason = "source_binding_unavailable"
		return unknown
	}
	// Exactly one existing Snapshot delegation. The source itself decides
	// whether idle coverage can sample its clock or pending/fault must refuse.
	return o.service.k3AdvertisedSnapshot()
}

func NewServiceWithAdvertisedConnectionObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, request AdvertisedConnectionObservationRequest) (*Service, *AdvertisedConnectionObservation, error) {
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{AdvertisedConnections: &request})
	return s, readouts.AdvertisedConnections, err
}

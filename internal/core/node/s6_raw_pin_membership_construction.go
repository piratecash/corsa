package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Explicit scalar observation allocation request only. Instance is an
// unverified caller label, not pin proof, artifact, boot or run attribution.
// No public duration callback, RNG, deployment limit or activation is added.
// Invalid caps/zero Instance retain the original store engine's UNKNOWN state.
type RawPinMembershipObservationRequest struct {
	BaselineCapacity, HistoryCapacity int
	Instance                          [16]byte
}

type RawPinMembershipObservationKey struct{ Instance [16]byte }

// The engine owns the only baseline/history/clock algorithm. These are its
// already-copied value DTOs, including CountAtCut and separately CURRENT
// PointOnly flags; no durable/current-pin interpretation or new format.
type RawPinMembershipObservationClock = pinMembershipClock
type RawPinMembershipObservationTransition = pinMembershipTransition
type RawPinMembershipObservationPointOnly = pinMembershipPointOnly
type RawPinMembershipObservationSnapshot = pinMembershipSnapshot

// One original Service, actual secure store and immutable source attachment.
// No post-construction installer, mutable request, worker or Close resource.
type RawPinMembershipObservation struct {
	service  *Service
	store    *secureSessionStore
	source   *pinMembershipSource
	identity domain.PeerIdentity
	key      RawPinMembershipObservationKey
}

func (o *RawPinMembershipObservation) Key() RawPinMembershipObservationKey {
	if o == nil {
		return RawPinMembershipObservationKey{}
	}
	return o.key
}

func (o *RawPinMembershipObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}

func (o *RawPinMembershipObservation) Snapshot() RawPinMembershipObservationSnapshot {
	unknown := RawPinMembershipObservationSnapshot{Population: "RawPinMembership", Status: "UNKNOWN", Reason: "source_unavailable", Qualification: "UNKNOWN", PastOnly: true}
	if o == nil {
		return unknown
	}
	if o.service == nil || o.store == nil || o.source == nil || o.service.secureSessions == nil || o.service.secureSessions.store != o.store || o.store.pinMembership != o.source || o.source.instance != o.key.Instance || o.identity.IsZero() || o.service.identity == nil || domain.PeerIdentityFromWire(o.service.Address()) != o.identity {
		unknown.Reason = "source_binding_unavailable"
		return unknown
	}
	// Exactly one original owner Snapshot, with its original clock/cut and
	// detached immutable prefix. No wrapper sampling, reducer or FS call.
	return o.store.pinMembershipSnapshot()
}

func NewServiceWithRawPinMembershipObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, request RawPinMembershipObservationRequest) (*Service, *RawPinMembershipObservation, error) {
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{RawPinMembership: &request})
	return s, readouts.RawPinMembership, err
}

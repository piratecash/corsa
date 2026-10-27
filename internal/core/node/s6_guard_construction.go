package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// GuardObservationKey contains unverified caller references. ClockInstance is
// the original K9 instance token, copied by value, not an identity attestation.
type GuardObservationKey struct {
	Anchor          string
	ArtifactSHA256  string
	Boot            string
	ProcessInstance string
	ClockDomain     string
	ClockInstance   [16]byte
}

// GuardObservationSnapshot retains every original duration field verbatim.
// Guarded=false in an UNKNOWN/IN_FLIGHT placeholder is not evidence that the
// security Clock is unguarded; inspect Status before interpreting any bounds.
type GuardObservationSnapshot struct {
	Available, AttributionBound bool
	SchemaVersion               uint32
	trustedclock.GuardDurationSnapshot
}

type GuardObservation struct {
	service  *Service
	key      GuardObservationKey
	identity domain.PeerIdentity
	clock    *trustedclock.Clock
}

func (o *GuardObservation) Key() GuardObservationKey {
	if o == nil {
		return GuardObservationKey{}
	}
	return o.key
}
func (o *GuardObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}
func (o *GuardObservation) Snapshot() GuardObservationSnapshot {
	result := GuardObservationSnapshot{SchemaVersion: 1, GuardDurationSnapshot: trustedclock.GuardDurationSnapshot{Status: "INACTIVE", Reason: "source_unavailable"}}
	if o == nil {
		return result
	}
	if o.clock == nil {
		result.Status, result.Reason = "UNKNOWN", "secure_store_clock_unavailable"
		return result
	}
	result.Available = true
	result.GuardDurationSnapshot = o.clock.GuardDurationSnapshot()
	return result
}

type guardObservationConstruction struct {
	key     GuardObservationKey
	readout *GuardObservation
}

func validGuardObservationKey(key GuardObservationKey) bool {
	for _, ref := range []string{key.Anchor, key.Boot, key.ProcessInstance, key.ClockDomain} {
		if !validObservationReference(ref) {
			return false
		}
	}
	return validObservationArtifact(key.ArtifactSHA256) && key.ClockInstance != ([16]byte{})
}
func prepareGuardObservation(key GuardObservationKey, owner domain.PeerIdentity) *guardObservationConstruction {
	return &guardObservationConstruction{key: key, readout: &GuardObservation{key: key, identity: owner}}
}
func (c *guardObservationConstruction) install(s *Service) { c.readout.service = s }

// This selection is evaluated at the original trustedclock.New expression,
// before openSecureSessionStore can load or evaluate Now. Exactly one clock is
// constructed in either branch. Certificate and address-mark clocks stay raw.
func (c *guardObservationConstruction) newClock(sources trustedclock.Sources) *trustedclock.Clock {
	if c == nil {
		return trustedclock.New(sources)
	}
	clock := trustedclock.NewWithGuardObservation(sources, c.key.ClockInstance)
	c.readout.clock = clock // private construction only, before load/publication
	return clock
}

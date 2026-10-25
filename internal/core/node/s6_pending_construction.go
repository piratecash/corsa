package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// PendingObservationKey records caller assertions, not verified provenance.
// Strings are bounded references, never evidence that the named artifact,
// process, boot or duration domain was independently verified.
type PendingObservationKey struct {
	Anchor          string
	ArtifactSHA256  string
	Boot            string
	ProcessInstance string
	ClockDomain     string
}

// PendingObservation is bound to one Service before that Service is published.
// It has no installer, setter or activation authority. AttributionBound in its
// existing snapshot always remains false, including when the references parse.
type PendingObservation struct {
	service     *Service
	key         PendingObservationKey
	identity    domain.PeerIdentity
	unavailable string
}

func (o *PendingObservation) Key() PendingObservationKey {
	if o == nil {
		return PendingObservationKey{}
	}
	return o.key
}

// Identity is derived from the constructed Service's identity, not a caller's
// second claimed identity. It does not verify the other key references.
func (o *PendingObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}

func (o *PendingObservation) Snapshot() domain.PendingQueueObservationSnapshot {
	if o == nil || o.service == nil {
		return domain.PendingQueueObservationSnapshot{UnknownReason: "source_unavailable"}
	}
	if o.unavailable != "" {
		return domain.PendingQueueObservationSnapshot{UnknownReason: o.unavailable}
	}
	return o.service.PendingQueueObservation()
}

type pendingObservationConstruction struct {
	key      PendingObservationKey
	identity domain.PeerIdentity
	clock    func() (time.Duration, error)
	readout  *PendingObservation
}

// NewServiceWithPendingObservation explicitly opts into the existing K12 source.
// Ordinary constructors remain inactive. Clock initialization failure is an
// unavailable observation, never a new Service startup refusal. SinceBoot
// supplies duration reads with errors, unlike Clock.Now's stale-read fallback.
func NewServiceWithPendingObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, key PendingObservationKey) (*Service, *PendingObservation, error) {
	return newServiceWithPendingObservation(cfg, id, bus, key, trustedclock.SinceBoot)
}

// The provider is frozen only at construction; this is not a runtime setter.
func newServiceWithPendingObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, key PendingObservationKey, clock func() (time.Duration, error)) (*Service, *PendingObservation, error) {
	s, readouts, err := newServiceWithObservations(cfg, id, bus, ObservationOptions{Pending: &key}, clock)
	return s, readouts.Pending, err
}

func validPendingObservationKey(key PendingObservationKey) bool {
	for _, ref := range []string{key.Anchor, key.Boot, key.ProcessInstance, key.ClockDomain} {
		if !validObservationReference(ref) {
			return false
		}
	}
	return validObservationArtifact(key.ArtifactSHA256)
}

func (c *pendingObservationConstruction) install(s *Service) {
	// This edge precedes callbacks and publication of the first Service. The
	// actual baseline and source attachment share the mutation owner's lock.
	o := &PendingObservation{service: s, key: c.key, identity: c.identity}
	c.readout = o
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	var count int64
	for _, frames := range s.pending {
		if int64(len(frames)) > maxPendingObservationCount-count {
			o.unavailable = "initial_count_overflow"
			return
		}
		count += int64(len(frames))
	}
	observation, err := newPendingQueueObservation(count, c.clock)
	if err != nil {
		o.unavailable = "initial_source_unavailable"
		return
	}
	s.pendingObservation = observation
}

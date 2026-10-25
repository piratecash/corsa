package node

import (
	"crypto/rand"
	"errors"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// TerminalObservationKey contains bounded caller assertions, not verified
// attribution. Unlike K12, this source has no duration clock or clock reference.
type TerminalObservationKey struct {
	Anchor          string
	ArtifactSHA256  string
	Boot            string
	ProcessInstance string
}

// ObservationOptions is the fixed cross-package construction envelope. Keys
// are copied before the one original Service builder is invoked. Nil stays off.
type ObservationOptions struct {
	Pending  *PendingObservationKey
	Terminal *TerminalObservationKey
}

type ObservationReadouts struct {
	Pending  *PendingObservation
	Terminal *TerminalObservation
}

// TerminalObservationSnapshot copies the original restricted source by value.
// Lifecycle Status and CodeStatus are separate: coarse or pending evidence is
// never promoted to exact remote-code coverage. Instance is a source ID, not
// independently verified process/boot attribution. No UTC window is supplied.
type TerminalObservationSnapshot struct {
	Available, AttributionBound                               bool
	SchemaVersion                                             uint32
	Population, Status, CodeStatus, Reason                    string
	Instance                                                  [16]byte
	Registered, Active, Removed, Classified, Pending, Missing uint64
	RegistrationSerial, TerminalSerial, Version, Coarse       uint64
	Buckets                                                   [2][k10KindCount][len(k10ProtocolCodes)]uint64
	Directions                                                [2]string
	Kinds                                                     [k10KindCount]string
	Codes                                                     [len(k10ProtocolCodes)]string
}

type TerminalObservation struct {
	service     *Service
	key         TerminalObservationKey
	identity    domain.PeerIdentity
	unavailable string
}

func (o *TerminalObservation) Key() TerminalObservationKey {
	if o == nil {
		return TerminalObservationKey{}
	}
	return o.key
}
func (o *TerminalObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}
func (o *TerminalObservation) Snapshot() TerminalObservationSnapshot {
	result := TerminalObservationSnapshot{
		SchemaVersion: 1, Population: "RegisteredTransportInstance", Status: "INACTIVE", CodeStatus: "UNKNOWN", Reason: "source_unavailable",
		Directions: [2]string{"inbound", "outbound"},
		Kinds:      [k10KindCount]string{"missing", "owner-stopped", "owner-error", "owner-EOF", "command-stopped", "loopback-condemned", "local-eviction", "legacy-refusal"},
		Codes:      k10ProtocolCodes,
	}
	if o == nil || o.service == nil {
		return result
	}
	if o.unavailable != "" {
		result.Status, result.Reason = "UNKNOWN", o.unavailable
		return result
	}
	v := o.service.terminalObservation.snapshot()
	result.Available = o.service.terminalObservation != nil
	result.Population, result.Status, result.CodeStatus, result.Reason = v.population, v.status, v.codeStatus, v.reason
	result.Instance = v.instance
	result.Registered, result.Active, result.Removed = v.registered, v.active, v.removed
	result.Classified, result.Pending, result.Missing = v.classified, v.pending, v.missing
	result.RegistrationSerial, result.TerminalSerial, result.Version, result.Coarse = v.registrationSerial, v.terminalSerial, v.version, v.coarse
	result.Buckets = v.buckets
	return result
}

type terminalObservationConstruction struct {
	key         TerminalObservationKey
	identity    domain.PeerIdentity
	instance    [16]byte
	unavailable string
	readout     *TerminalObservation
}

// NewServiceWithObservations is one typed entry for the actual App bridge.
// It does not install collectors, alter policy or create another Service for
// a second observation. Existing one-sided constructors delegate here.
func NewServiceWithObservations(cfg config.Node, id *identity.Identity, bus *ebus.Bus, options ObservationOptions) (*Service, ObservationReadouts, error) {
	return newServiceWithObservations(cfg, id, bus, options, trustedclock.SinceBoot)
}

func NewServiceWithTerminalObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, key TerminalObservationKey) (*Service, *TerminalObservation, error) {
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{Terminal: &key})
	return s, readouts.Terminal, err
}

func newServiceWithObservations(cfg config.Node, id *identity.Identity, bus *ebus.Bus, options ObservationOptions, clock func() (time.Duration, error)) (*Service, ObservationReadouts, error) {
	if options.Pending == nil && options.Terminal == nil {
		return NewService(cfg, id, bus), ObservationReadouts{}, nil
	}
	if id == nil || domain.PeerIdentityFromWire(id.Address).IsZero() {
		return nil, ObservationReadouts{}, errors.New("observation: invalid node identity")
	}
	owner := domain.PeerIdentityFromWire(id.Address)
	var pending *pendingObservationConstruction
	var terminal *terminalObservationConstruction
	if options.Pending != nil {
		key := *options.Pending
		if !validPendingObservationKey(key) {
			return nil, ObservationReadouts{}, errors.New("pending observation: invalid key")
		}
		pending = &pendingObservationConstruction{key: key, identity: owner, clock: clock}
	}
	if options.Terminal != nil {
		key := *options.Terminal
		if !validTerminalObservationKey(key) {
			return nil, ObservationReadouts{}, errors.New("terminal observation: invalid key")
		}
		terminal = &terminalObservationConstruction{key: key, identity: owner}
		if _, err := rand.Read(terminal.instance[:]); err != nil || terminal.instance == ([16]byte{}) {
			terminal.unavailable = "initial_source_unavailable"
		}
	}
	s := newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{pendingObservation: pending, terminalObservation: terminal})
	var readouts ObservationReadouts
	if pending != nil {
		readouts.Pending = pending.readout
	}
	if terminal != nil {
		readouts.Terminal = terminal.readout
	}
	return s, readouts, nil
}

func validTerminalObservationKey(key TerminalObservationKey) bool {
	for _, ref := range []string{key.Anchor, key.Boot, key.ProcessInstance} {
		if !validObservationReference(ref) {
			return false
		}
	}
	return validObservationArtifact(key.ArtifactSHA256)
}
func validObservationReference(ref string) bool {
	if len(ref) == 0 || len(ref) > 128 {
		return false
	}
	for i := range len(ref) {
		if ref[i] < '!' || ref[i] > '~' {
			return false
		}
	}
	return true
}
func validObservationArtifact(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	nonzero := false
	for i := range len(digest) {
		c := digest[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
		nonzero = nonzero || c != '0'
	}
	return nonzero
}

func (c *terminalObservationConstruction) install(s *Service) {
	o := &TerminalObservation{service: s, key: c.key, identity: c.identity, unavailable: c.unavailable}
	c.readout = o
	if o.unavailable != "" {
		return
	}
	// This original owner is still empty, before callbacks/registration or
	// publication. An unexpectedly populated registry is not a zero baseline.
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if len(s.conns) != 0 || len(s.connIDByNetConn) != 0 {
		o.unavailable = "initial_registry_not_empty"
		return
	}
	s.terminalObservation = newK10TerminalSource(c.instance)
}

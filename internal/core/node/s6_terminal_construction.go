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
	AdvertisedConnections *AdvertisedConnectionObservationRequest
	RawPinMembership      *RawPinMembershipObservationRequest
	RegisteredProvenV2    *RegisteredProvenV2ObservationRequest
	Pending               *PendingObservationKey
	Terminal              *TerminalObservationKey
	Guard                 *GuardObservationKey
	Handshake             *HandshakeObservationRequest
	HandshakeFile         *HandshakeFileObservationRequest
	ConnectionTotal       *ConnectionTotalObservationRequest
	ConnectionTotalFile   *ConnectionTotalFileObservationRequest
}

type ObservationReadouts struct {
	AdvertisedConnections *AdvertisedConnectionObservation
	RawPinMembership      *RawPinMembershipObservation
	RegisteredProvenV2    *RegisteredProvenV2Observation
	Pending               *PendingObservation
	Terminal              *TerminalObservation
	Guard                 *GuardObservation
	Handshake             *HandshakeObservation
	HandshakeFile         *HandshakeFileObservation
	ConnectionTotal       *ConnectionTotalObservation
	ConnectionTotalFile   *ConnectionTotalFileObservation
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
	// Freeze the advertised scalar request before ANY observer callback.
	var advertised *k3AdvertisedRequest
	if options.AdvertisedConnections != nil {
		request := *options.AdvertisedConnections
		advertised = &k3AdvertisedRequest{ConnectionCapacity: request.ConnectionCapacity, SlotCapacity: request.SlotCapacity, HistoryCapacity: request.HistoryCapacity, Instance: request.Instance, elapsed: clock}
	}
	// Freeze before ANY other observation clock/RNG/build callback.
	var membership *pinMembershipRequest
	if options.RawPinMembership != nil {
		request := *options.RawPinMembership
		membership = &pinMembershipRequest{BaselineCapacity: request.BaselineCapacity, HistoryCapacity: request.HistoryCapacity, Instance: request.Instance, elapsed: clock}
	}
	// Freeze this request before any other observer clock/RNG/build callback.
	// Public input has only scalars/value tokens; elapsed remains private.
	var registered *k3RegisteredRequest
	if options.RegisteredProvenV2 != nil {
		request := *options.RegisteredProvenV2
		registered = &k3RegisteredRequest{OwnerCapacity: request.OwnerCapacity, HistoryCapacity: request.HistoryCapacity, Instance: request.Instance, elapsed: clock}
	}
	if advertised == nil && membership == nil && registered == nil && options.Pending == nil && options.Terminal == nil && options.Guard == nil && options.Handshake == nil && options.HandshakeFile == nil && options.ConnectionTotal == nil && options.ConnectionTotalFile == nil {
		return NewService(cfg, id, bus), ObservationReadouts{}, nil
	}
	if options.Handshake != nil && options.HandshakeFile != nil {
		return nil, ObservationReadouts{}, errors.New("handshake observation: memory and file modes are mutually exclusive")
	}
	if options.ConnectionTotal != nil && options.ConnectionTotalFile != nil {
		return nil, ObservationReadouts{}, errors.New("connection-total observation: memory and file modes are mutually exclusive")
	}
	if id == nil || domain.PeerIdentityFromWire(id.Address).IsZero() {
		return nil, ObservationReadouts{}, errors.New("observation: invalid node identity")
	}
	owner := domain.PeerIdentityFromWire(id.Address)
	var pending *pendingObservationConstruction
	var terminal *terminalObservationConstruction
	var guard *guardObservationConstruction
	var handshake *handshakeObservationConstruction
	var handshakeFile *handshakeFileObservationConstruction
	var connectionTotal *connectionTotalObservationConstruction
	var connectionTotalFile *connectionTotalFileObservationConstruction
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
	if options.Guard != nil {
		key := *options.Guard
		if !validGuardObservationKey(key) {
			return nil, ObservationReadouts{}, errors.New("guard observation: invalid key")
		}
		guard = prepareGuardObservation(key, owner)
	}
	if options.Handshake != nil {
		request := *options.Handshake
		if !validHandshakeObservationKey(request.Key) {
			return nil, ObservationReadouts{}, errors.New("handshake observation: invalid key references")
		}
		handshake = prepareHandshakeObservation(request, owner, clock)
	}
	if options.HandshakeFile != nil {
		request := *options.HandshakeFile
		if !validHandshakeObservationKey(request.Key) {
			return nil, ObservationReadouts{}, errors.New("file handshake observation: invalid key references")
		}
		handshakeFile = prepareHandshakeFileObservation(request, owner, clock)
	}
	if options.ConnectionTotal != nil {
		request := *options.ConnectionTotal
		if !validConnectionTotalObservationKey(request.Key) {
			return nil, ObservationReadouts{}, errors.New("connection-total observation: invalid key references")
		}
		connectionTotal = prepareConnectionTotalObservation(request, owner, clock)
	}
	if options.ConnectionTotalFile != nil {
		request := *options.ConnectionTotalFile
		if !validConnectionTotalObservationKey(request.Key) {
			return nil, ObservationReadouts{}, errors.New("file connection-total observation: invalid key references")
		}
		connectionTotalFile = prepareConnectionTotalFileObservation(request, owner, clock)
	}
	// Any construction panic after the file factory returned its lease must
	// release it. Normal observation I/O errors remain on the returned handle,
	// owned by the Service, rather than becoming startup/admission policy.
	constructed := false
	defer func() {
		if !constructed && connectionTotalFile != nil {
			_ = connectionTotalFile.readout.Close()
		}
		if !constructed && handshakeFile != nil {
			_ = handshakeFile.readout.Close()
		}
	}()
	s := newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{k3Advertised: advertised, pinMembership: membership, k3Registered: registered, pendingObservation: pending, terminalObservation: terminal, guardObservation: guard, handshakeObservation: handshake, handshakeFileObservation: handshakeFile, connectionTotalObservation: connectionTotal, connectionTotalFileObservation: connectionTotalFile})
	constructed = true
	var readouts ObservationReadouts
	if advertised != nil {
		readouts.AdvertisedConnections = &AdvertisedConnectionObservation{service: s, source: s.k3Advertised, manager: s.connManager, identity: domain.PeerIdentityFromWire(s.Address()), key: AdvertisedConnectionObservationKey{Instance: advertised.Instance}}
	}
	if membership != nil {
		readout := &RawPinMembershipObservation{service: s, identity: domain.PeerIdentityFromWire(s.Address()), key: RawPinMembershipObservationKey{Instance: membership.Instance}}
		if s.secureSessions != nil {
			readout.store = s.secureSessions.store
			if readout.store != nil {
				readout.source = readout.store.pinMembership
			}
		}
		readouts.RawPinMembership = readout
	}
	if registered != nil {
		readouts.RegisteredProvenV2 = &RegisteredProvenV2Observation{service: s, source: s.k3Registered, identity: domain.PeerIdentityFromWire(s.Address()), key: RegisteredProvenV2ObservationKey{Instance: registered.Instance}}
	}
	if pending != nil {
		readouts.Pending = pending.readout
	}
	if terminal != nil {
		readouts.Terminal = terminal.readout
	}
	if guard != nil {
		readouts.Guard = guard.readout
	}
	if handshake != nil {
		readouts.Handshake = handshake.readout
	}
	if handshakeFile != nil {
		readouts.HandshakeFile = handshakeFile.readout
		readouts.Handshake = handshakeFile.legacy
	}
	if connectionTotal != nil {
		readouts.ConnectionTotal = connectionTotal.readout
	}
	if connectionTotalFile != nil {
		readouts.ConnectionTotalFile = connectionTotalFile.readout
		readouts.ConnectionTotal = connectionTotalFile.legacy
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

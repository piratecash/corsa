package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// secure_session_store.go is what keeps a peer that proved v2 from being taken
// back to the relayable v1 proof — across restarts, not only for the life of
// the process (docs/protocol/session_v2.md, "Downgrade protection").
//
//   - An IDENTITY that proved itself over v2 is pinned for good: a v1
//     session naming it is refused in either direction — a v1 hello on
//     accept, a v1 welcome on dial.
//   - An ENDPOINT this node dialled and reached over v2 is bound: a v1 answer
//     from it is a downgrade, not an old node, and the dial fails. Unlike the
//     identity pin, the binding also covers somebody ELSE answering on that
//     address with an unpinned identity. A binding is ordinary and lasts 30
//     days from the last v2 session on it — an address may change hands,
//     so a binding that only records that it once answered over v2 is not
//     kept for good — unless the dial asked to protect it: a protected
//     binding guards a contact or a relation of this node, so it never
//     expires and is never lowered or removed automatically. Only a dial
//     with the intent "to X" that proved X, a contact, asks
//     (dialProtection), and no dial path carries such an intent yet;
//     protected bindings from earlier builds are kept as they are.
//
// A binding is keyed by the endpoint's canonical form (canonicalEndpoint), so
// two spellings of one endpoint share it, and an address with no canonical
// form gets none. Each binding keeps the source group it was created in
// (endpointSourceGroup); a group creates at most
// generalCreationsPerGroupPerDay new ordinary bindings a day. A name dialled
// direct also binds the address the dial reached, as an ordinary derived key.
//
// The store never drops live protection to make room. At a bound a new
// protection is refused with its reason (protected bindings in pools K and
// R, 8 192 of the 32 768 places for K only, at most 4 per identity — see
// maxProtectedBindings), and what is already there stays; the endpoint may
// get an ordinary binding instead. A NEW identity at the pin bound fails its
// session with errPinStoreFull: a v2 session whose mandatory pin is not
// stored is not established, and the dialler does not fall back to v1 in
// that attempt.
//
// A change that could not be written is reported to the caller — a session
// whose protection is not on disk is not established — and stays pending:
// the next note writes it even when it changes nothing itself.
//
// Apart from the expiry of an ordinary binding, only the operator takes a
// binding out of the store or lowers it (endpoint_binding_operator.go): an
// ordinary binding removed by its address, named protected ones demoted to
// ordinary after a dry run. Those changes go the other way round — written
// first, made in memory only once the write landed.
//
// Persistence is one JSON file written through secretfile (unique temp,
// owner-only at creation, fsync, rename), next to the node's peers file.

const (
	secureSessionStoreFormat = 1
	// maxPinnedIdentities bounds the pins. Every v2 handshake can pin, and a
	// fresh identity costs an attacker nothing, so the bound is a refusal of
	// NEW pins, never an eviction of old ones.
	maxPinnedIdentities = 20000
	// maxGeneralBindings bounds the ordinary endpoint bindings, apart from
	// the protected ones: a full ordinary tier never takes a protected place
	// and the other way round.
	maxGeneralBindings = 16384
	// maxProtectedBindings, protectedReserveContact and
	// maxProtectedPerIdentity bound the protected bindings
	// (docs/protocol/session_v2.md, "Bounds of the endpoint bindings").
	// Protected bindings never expire and are never lowered automatically,
	// so without a bound the tier would only grow; the per-identity cap
	// keeps one identity from filling the tier with its own endpoints.
	// The reserve is for the contact pool alone: a relation never takes it,
	// not even for a while — with nothing ever evicted, a reserve place a
	// relation took would never come back.
	maxProtectedBindings    = 32768
	protectedReserveContact = 8192
	maxProtectedPerIdentity = 4
	// endpointBindingTTL is how long an ordinary binding outlives the last
	// v2 session on its endpoint. It expires, unlike a protected one,
	// because an address may change hands and the binding only records
	// that it once answered over v2.
	endpointBindingTTL = domain.EndpointBindingGeneralTerm
	// generalCreationsPerGroupPerDay bounds the NEW ordinary bindings one
	// source group may create in any 24 hours. A mutual v2 handshake costs an
	// attacker with fresh keys nothing, and onion names cost nothing either,
	// so without it one block or one overlay could fill the ordinary tier in
	// an afternoon. Renewals and changes of owner of existing bindings are
	// not creations and are never refused by it.
	generalCreationsPerGroupPerDay = 64
	generalCreationWindow          = 24 * time.Hour
	// renewalFlushEvery batches the writes of renewals for the whole store:
	// a renewal of an ordinary binding is made in memory at once and only
	// marks the store as holding unwritten renewals; the periodic flush
	// (flushRenewals, on the bootstrap tick) writes them at most this often.
	// A crash loses at most this much of a binding's 30 days, plus one tick.
	renewalFlushEvery = 60 * time.Second
)

// errLegacyRefusedPinned is a v1 session naming an identity that proved v2.
var errLegacyRefusedPinned = errors.New("secure session: v1 refused for an identity that proved v2")

// errPinStoreFull is a v2 session of an identity not pinned yet, refused
// because the store is at its bound and the mandatory pin cannot be stored.
// Its text is the diagnostic code operators search for.
var errPinStoreFull = errors.New("pin_store_full")

// endpointProtectionKind is what a dial asks of the binding it earns.
type endpointProtectionKind int

const (
	// protectionNotAsked: an ordinary binding.
	protectionNotAsked endpointProtectionKind = iota + 1
	// protectionWanted: protected if a place is free, ordinary otherwise.
	protectionWanted
	// protectionRequired: the operation needs the endpoint protected; a
	// refusal fails it instead of passing an ordinary binding off as one.
	protectionRequired
)

// endpointProtection is the protection a dial asks for, and the pool it
// would count against.
type endpointProtection struct {
	kind endpointProtectionKind
	pool domain.ProtectedBindingPool
}

func endpointProtectionNone() endpointProtection {
	return endpointProtection{kind: protectionNotAsked}
}

// endpointProtectionWanted is for a dial with the intent "to X" that would
// like X's endpoint protected but stands without it (dialProtection).
func endpointProtectionWanted(pool domain.ProtectedBindingPool) endpointProtection {
	return endpointProtection{kind: protectionWanted, pool: pool}
}

// endpointProtectionRequired is for an operation that is incomplete without
// the protection. No operation needs it today — a dial "to X" only wants it
// (dialProtection) — so only tests use it; the first operation that needs
// X's endpoint protected will.
func endpointProtectionRequired(pool domain.ProtectedBindingPool) endpointProtection {
	return endpointProtection{kind: protectionRequired, pool: pool}
}

func (p endpointProtection) asked() bool {
	return p.kind == protectionWanted || p.kind == protectionRequired
}

// endpointBindingTier is the binding an endpoint has after a note.
type endpointBindingTier int

const (
	bindingTierNone endpointBindingTier = iota + 1
	bindingTierGeneral
	bindingTierProtected
)

func (t endpointBindingTier) String() string {
	switch t {
	case bindingTierGeneral:
		return "general"
	case bindingTierProtected:
		return "protected"
	default:
		return "none"
	}
}

// endpointBindingResult is what a note did to the endpoint: its tier now,
// and the refusal when protection was asked for and not given. The three
// outcomes a caller must tell apart — protected; protection refused and an
// ordinary binding recorded; no binding — are all here, and a refused
// protection is never reported as protected.
type endpointBindingResult struct {
	tier    endpointBindingTier
	refused *domain.ProtectedBindingRefused
	// limitRefusalReported: refused is a bound that this note counted and
	// must log — the first refusal of this binding's protection.
	limitRefusalReported bool
	// given is the protection this note gave the endpoint — created it
	// protected, or raised its ordinary binding — for the caller to journal;
	// nil when the note gave none, a renewal of a protected binding included.
	given *bindingProtection
	// ownerChange is present when the note took a live binding over from
	// another identity: the endpoint changed hands. The caller journals it
	// with the connection it happened on (logOwnerChange).
	ownerChange *endpointOwnerChange
	// quotaRefusal is present when no binding was created because the
	// endpoint's source group used up its new ordinary bindings for the day.
	quotaRefusal *sourceGroupQuotaRefusal
	// expired are the ordinary bindings this note removed because their
	// term had run out: the one it replaced on the same endpoint, or those
	// pruned to make room in a full ordinary tier.
	expired []expiredEndpointBinding
	// derived is the outcome for the address a name dialled direct reached,
	// bound beside the name; nil when the dial derives no key.
	derived *derivedBindingOutcome
}

// endpointOwnerChange is an endpoint whose binding a note took over from
// previous.
type endpointOwnerChange struct {
	endpoint domain.CanonicalEndpoint
	previous domain.PeerIdentity
}

// sourceGroupQuotaRefusal is a new ordinary binding refused because its group
// created generalCreationsPerGroupPerDay of them in the last day. journal: the
// first such refusal in the group since it last had room, the one that is
// logged — every refusal is counted, but a flood of fresh endpoints in one
// group is one journal line, not one per endpoint.
type sourceGroupQuotaRefusal struct {
	endpoint domain.CanonicalEndpoint
	group    domain.SourceGroup
	journal  bool
}

// derivedBindingOutcome is what a note did to the derived key of a name.
type derivedBindingOutcome struct {
	endpoint domain.CanonicalEndpoint
	result   endpointBindingResult
}

// expiredEndpointBinding is an ordinary binding removed at the end of its
// term, as its journal line names it.
type expiredEndpointBinding struct {
	address  domain.PeerAddress
	identity domain.PeerIdentity
	lastV2   time.Time
}

// bindingProtection is what makes a binding protected: the identity whose
// standing gave the protection and the pool it counts against, both fixed
// when the protection was given. A new owner of the endpoint inherits the
// protection but not the place it takes: that stays counted for by.
type bindingProtection struct {
	by   domain.PeerIdentity
	pool domain.ProtectedBindingPool
}

type endpointBinding struct {
	// identity is the last identity proven on the endpoint over v2.
	identity domain.PeerIdentity
	lastV2   time.Time
	// protection is nil for an ordinary binding.
	protection *bindingProtection
	// group is the endpoint's source group, fixed when the binding was
	// created (endpointSourceGroup); a binding read from a file written
	// before groups existed gets the group of its own address.
	group domain.SourceGroup
	// firstAt is when the binding was created; nil for a binding written
	// before creation times were kept, which counts against no group's
	// daily quota.
	firstAt *time.Time
	// derivedFrom is the name whose direct dial reached this address; nil
	// for an endpoint dialled by itself. A derived key stays ordinary for
	// good: the address behind a name is the provider's fact, not part of
	// the identity, and a binding that never expired would refuse v1 there
	// long after the address moved on.
	derivedFrom *domain.CanonicalEndpoint
	// demotedAt is when an operator moved the binding from the protected tier
	// to the ordinary one (demote); nil for a binding never demoted.
	demotedAt *time.Time
	// limitRefusalCounted: a protection of this binding was refused at a
	// bound and counted, so renewals that meet the same bound are not
	// counted or logged again. In memory only: after a restart the first
	// refusal is counted once more.
	limitRefusalCounted bool
}

// live reports whether the binding still requires v2 at now.
func (b endpointBinding) live(now time.Time) bool {
	return b.protection != nil || now.Before(b.termEnd())
}

// termEnd is when an ordinary binding stops requiring v2: endpointBindingTTL
// after the last v2 session on the endpoint or after the operator's demotion,
// whichever is later. A protected binding's last v2 is not written on renewal,
// so a demotion counting from it could end the term at once.
func (b endpointBinding) termEnd() time.Time {
	start := b.lastV2
	if b.demotedAt != nil && b.demotedAt.After(start) {
		start = *b.demotedAt
	}
	return start.Add(endpointBindingTTL)
}

func (b endpointBinding) tier() endpointBindingTier {
	if b.protection != nil {
		return bindingTierProtected
	}
	return bindingTierGeneral
}

// protectedPoolRule is how far a pool may fill the protected tier and how a
// refusal at that point is named.
type protectedPoolRule struct {
	ceiling int
	full    domain.ProtectedBindingRefusalReason
}

func protectedPoolRuleFor(pool domain.ProtectedBindingPool) protectedPoolRule {
	if pool == domain.ProtectedBindingPoolContact {
		return protectedPoolRule{ceiling: maxProtectedBindings, full: domain.ProtectedBindingRefusedStoreFull}
	}
	return protectedPoolRule{ceiling: maxProtectedBindings - protectedReserveContact, full: domain.ProtectedBindingRefusedPoolRFull}
}

// secureSessionStore is the downgrade-protection state. mu guards the maps
// and is a leaf: no I/O and no call out under it. writeMu serialises disk
// writes and is held WITHOUT mu, so a slow disk never blocks a lookup.
type secureSessionStore struct {
	dir  string // "" — memory only (a Service built without a peers path)
	name string
	// clock is the trusted time every term and every time written to the
	// file is read from: a wall clock set forward in this process does not
	// end ordinary bindings early (trustedclock). It synchronises itself and
	// is called under mu as a leaf; the jumps it finds are journalled by the
	// flush, after mu is released.
	clock  *trustedclock.Clock
	logger zerolog.Logger

	mu         sync.Mutex
	identities map[domain.PeerIdentity]time.Time
	// endpoints is keyed by the canonical endpoint (canonicalEndpoint), so
	// two spellings of one endpoint are one binding.
	endpoints map[domain.CanonicalEndpoint]endpointBinding
	// generalCreations holds, per source group, when its ordinary bindings
	// of the last generalCreationWindow were created, oldest first — the
	// daily quota. It counts creations, not live bindings: a binding that
	// expired or changed owner since still used its group's place. Rebuilt
	// at load from the bindings' firstAt; groups with nothing left in the
	// window are dropped by the periodic sweep.
	generalCreations map[domain.SourceGroup]*groupCreations
	// nonCanonical holds the bindings of the file stored under an address
	// with no canonical form (see loadBindingsLocked), by that exact
	// address. They are counted with the others against the bounds.
	nonCanonical map[domain.PeerAddress]endpointBinding
	// generalBindings, protectedByPool and protectedByGiver count endpoints
	// by tier, by pool and by protected_by, so a bound is checked without a
	// scan. Kept by insertBindingLocked, removeBindingLocked and
	// protectBindingLocked — the only writers of endpoints.
	generalBindings  int
	protectedByPool  map[domain.ProtectedBindingPool]int
	protectedByGiver map[domain.PeerIdentity]int
	// unreadable: the file exists but could not be read. Fail-closed:
	// every identity and every endpoint is treated as protected, so v1 is
	// refused while v2 keeps working, until an operator repairs the file.
	unreadable bool
	// locationUnsafe: the location audit at load found the file or a
	// directory on the way to it changeable by someone other than this
	// user or root, or could not tell. Set once while the store is built,
	// before it is shared; read under mu with the other figures.
	locationUnsafe bool
	// changes counts every change, written the count the last successful
	// write covered: a gap is protection that is in memory only.
	changes, written uint64
	// renewalsUnwritten: an ordinary binding was renewed in memory since the
	// last snapshot. lastFlush is when a flush last wrote; nil until the
	// first one, which is therefore due at once. lastExpirySweep is when the
	// periodic flush last looked for ordinary bindings past their term; nil
	// until the first one. Kept apart from lastFlush, so a sweep that found
	// nothing does not hold back the next batch of renewals.
	renewalsUnwritten bool
	lastFlush         *renewalFlush
	lastExpirySweep   *renewalFlush
	// pinRefusals counts sessions refused with errPinStoreFull.
	pinRefusals uint64
	// protectedRefusals counts protected bindings refused at a bound.
	protectedRefusals map[domain.ProtectedBindingRefusalReason]uint64
	// groupQuotaRefusals counts new ordinary bindings refused by their
	// source group's daily quota.
	groupQuotaRefusals uint64
	// skippedNotCanonical counts proven outbound sessions whose dialled
	// address had no canonical form, so their endpoint got no binding.
	skippedNotCanonical uint64
	// v1RefusalsIdentity counts v1 sessions refused because the identity they
	// name requires v2 (errLegacyRefusedPinned), on accept and on dial.
	v1RefusalsIdentity uint64
	// v1RefusalsEndpoint counts v1 answers refused because the endpoint that
	// gave them is bound to v2 (errLegacyRefusedV2Bound).
	v1RefusalsEndpoint uint64
	// v1DialsHeld counts v1 dials withheld by the hold that follows a
	// refused v1 answer from the same address (errLegacyRefusedBackoff).
	v1DialsHeld uint64
	// dialsHeldV2Retry counts dials refused before any socket by an
	// address's v2 retry backoff (errV2RetryHeld).
	dialsHeldV2Retry uint64

	writeMu sync.Mutex
	// writeFile puts the encoded store on disk; writeToDisk outside tests.
	writeFile func(raw []byte) error
}

// secureSessionStorePath is where the store lives: the configured path, or
// secure-sessions-<port>.json next to the peers file.
func secureSessionStorePath(cfg config.Node) string {
	if cfg.SecureSessionStorePath != "" {
		return cfg.SecureSessionStorePath
	}
	if cfg.PeersStatePath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(cfg.PeersStatePath), "secure-sessions-"+config.PortSuffix(cfg.ListenAddress)+".json")
}

// loadSecureSessionStore reads the store before the node accepts or dials.
func loadSecureSessionStore(path string, clock *trustedclock.Clock) *secureSessionStore {
	return loadSecureSessionStoreWith(path, clock, log.Logger)
}

// loadSecureSessionStoreWith is loadSecureSessionStore writing to logger: the
// load itself logs (an unsafe location, an unreadable file), so a test that
// pins those lines needs its logger in place before the first one.
func loadSecureSessionStoreWith(path string, clock *trustedclock.Clock, logger zerolog.Logger) *secureSessionStore {
	store := &secureSessionStore{
		clock:             clock,
		logger:            logger,
		identities:        make(map[domain.PeerIdentity]time.Time),
		endpoints:         make(map[domain.CanonicalEndpoint]endpointBinding),
		generalCreations:  make(map[domain.SourceGroup]*groupCreations),
		nonCanonical:      make(map[domain.PeerAddress]endpointBinding),
		protectedByPool:   make(map[domain.ProtectedBindingPool]int),
		protectedByGiver:  make(map[domain.PeerIdentity]int),
		protectedRefusals: make(map[domain.ProtectedBindingRefusalReason]uint64),
	}
	if path == "" {
		// Memory only: nothing to write, so no snapshot is ever taken.
		store.logClockGuard()
		return store
	}
	store.writeFile = store.writeToDisk
	store.dir, store.name = filepath.Dir(path), filepath.Base(path)
	store.logClockGuard()
	store.auditLocation(path)
	if err := store.load(); err != nil {
		store.unreadable = true
		store.logger.Error().Err(err).Str("store", store.name).Msg("secure_session_store_unreadable_v1_refused_for_every_peer")
	}
	return store
}

// auditLocation checks once, at load, that nobody but this user or root can
// change the store file or any directory on the way to it
// (secretfile.AuditLocation — owner and mode bits on POSIX, the file's DACL
// on Windows; extended ACLs are not seen). A violation, or a location that
// could not be inspected, is a loud warning and the pin_store_unsafe flag —
// and nothing more: the store loads and writes as before. The node does not
// refuse to start over it, so a node whose data directory is a 0777 volume
// keeps its peers.
//
// The warning names the component by its role and the store by its file
// name, never by path: the log line is enough to find the directory, and
// the node's filesystem layout is not something its log has to carry.
func (s *secureSessionStore) auditLocation(path string) {
	err := secretfile.AuditLocation(path)
	if err == nil {
		return
	}
	s.locationUnsafe = true
	var unsafe *secretfile.UnsafeLocationError
	if errors.As(err, &unsafe) {
		s.logger.Warn().Str("store", s.name).
			Str("component", unsafe.Component.String()).
			Str("violation", string(unsafe.Violation)).
			Str("owner", unsafe.Owner).
			Str("mode", unsafe.Mode.String()).
			Str("fix", "make the file 0600 and owned by the node's user, and every directory above it writable by nobody but that user or root").
			Msg("pin_store_unsafe")
		return
	}
	s.logger.Warn().Err(err).Str("store", s.name).Str("violation", "unverifiable").
		Msg("pin_store_unsafe")
}

// now is the store's trusted time in UTC, so the file does not depend on the
// zone the node runs in.
func (s *secureSessionStore) now() time.Time { return s.clock.Now().UTC() }

// logClockGuard says once, at load, that this platform has no clock since
// boot to hold the wall clock against: a wall clock set forward then ends
// ordinary bindings early, in a running process too.
func (s *secureSessionStore) logClockGuard() {
	if s.clock.Stats().Guarded {
		return
	}
	s.logger.Warn().Str("store", s.name).
		Msg("clock_jump_guard_unavailable_wall_clock_steps_end_ordinary_bindings_early")
}

// reportClockJump journals the wall clock jump the trusted time found since
// the last report: the excess was not counted toward any term. Called with
// no lock held.
func (s *secureSessionStore) reportClockJump() {
	jump, found := s.clock.TakeUnreportedJump()
	if !found {
		return
	}
	s.logger.Warn().Str("store", s.name).
		Time("wall", jump.Wall).
		Time("trusted", jump.Trusted).
		Dur("ahead", jump.Ahead).
		Msg("clock_jump_detected")
}

type secureSessionFile struct {
	Format     int                                          `json:"format"`
	Identities map[domain.PeerIdentity]time.Time            `json:"identities"`
	Endpoints  map[domain.PeerAddress]endpointBindingRecord `json:"endpoints"`
}

// endpointBindingRecord is a binding as the file holds it, under the
// endpoint's canonical address. ProtectedBy and Pool are present exactly when
// Protected is true. A protected record without both was written before pools
// existed, when only a contact's binding was protected: it is read as given
// by its identity, in pool K.
//
// Transport, SourceGroup, FirstAt, DerivedFrom and DemotedAt came later and
// are optional, so the file keeps format 1 both ways: a build before them
// ignores the fields, and a record without them — written by such a build,
// under the raw address it dialled — gets its transport and group from that
// address, and no creation time. DemotedAt is present only on an ordinary
// binding an operator moved from the protected tier.
type endpointBindingRecord struct {
	Identity    domain.PeerIdentity          `json:"identity"`
	LastV2      time.Time                    `json:"last_v2"`
	Protected   bool                         `json:"protected,omitempty"`
	ProtectedBy *domain.PeerIdentity         `json:"protected_by,omitempty"`
	Pool        *domain.ProtectedBindingPool `json:"pool,omitempty"`
	Transport   *domain.EndpointTransport    `json:"transport,omitempty"`
	SourceGroup *domain.SourceGroup          `json:"source_group,omitempty"`
	FirstAt     *time.Time                   `json:"first_at,omitempty"`
	DerivedFrom *domain.PeerAddress          `json:"derived_from,omitempty"`
	DemotedAt   *time.Time                   `json:"demoted_at,omitempty"`
}

func recordOf(endpoint domain.CanonicalEndpoint, binding endpointBinding) endpointBindingRecord {
	record := recordWithoutTransport(binding)
	transport := endpoint.Transport()
	record.Transport = &transport
	return record
}

// recordWithoutTransport is binding as the file holds it without a transport —
// as a record of an address with no canonical form, whose transport cannot be
// told, written back as it was read.
func recordWithoutTransport(binding endpointBinding) endpointBindingRecord {
	group := binding.group
	record := endpointBindingRecord{
		Identity:    binding.identity,
		LastV2:      binding.lastV2,
		SourceGroup: &group,
		FirstAt:     binding.firstAt,
		DemotedAt:   binding.demotedAt,
	}
	if binding.protection != nil {
		by, pool := binding.protection.by, binding.protection.pool
		record.Protected, record.ProtectedBy, record.Pool = true, &by, &pool
	}
	if binding.derivedFrom != nil {
		name := binding.derivedFrom.Address()
		record.DerivedFrom = &name
	}
	return record
}

var (
	errIncompleteBindingRecord    = errors.New("decode: an incomplete endpoint binding")
	errContradictoryBindingRecord = errors.New("decode: a protected binding without its giver or pool, or an ordinary one with them")
	errBindingTransportMismatch   = errors.New("decode: an endpoint binding whose transport contradicts its address")
	errProtectedDerivedBinding    = errors.New("decode: a derived endpoint binding marked protected")
	errProtectedDemotedBinding    = errors.New("decode: a protected endpoint binding marked demoted")
	errZeroPinnedIdentity         = errors.New("decode: a zero identity")
)

// bindingFromRecord checks a record of endpoint and turns it into a binding.
func bindingFromRecord(endpoint domain.CanonicalEndpoint, record endpointBindingRecord) (endpointBinding, error) {
	if record.Identity.IsZero() {
		return endpointBinding{}, errIncompleteBindingRecord
	}
	if record.Transport != nil && *record.Transport != endpoint.Transport() {
		return endpointBinding{}, errBindingTransportMismatch
	}
	protection, err := protectionOf(record)
	if err != nil {
		return endpointBinding{}, err
	}
	group, err := groupOf(endpoint, record)
	if err != nil {
		return endpointBinding{}, err
	}
	derivedFrom, err := derivedFromOf(record)
	if err != nil {
		return endpointBinding{}, err
	}
	if err := checkTierFields(protection, derivedFrom, record.DemotedAt); err != nil {
		return endpointBinding{}, err
	}
	return endpointBinding{
		identity:    record.Identity,
		lastV2:      record.LastV2,
		protection:  protection,
		group:       group,
		firstAt:     record.FirstAt,
		derivedFrom: derivedFrom,
		demotedAt:   record.DemotedAt,
	}, nil
}

// checkTierFields refuses a protected record carrying what only an ordinary
// binding has: the name it derives from, or the moment an operator demoted it.
func checkTierFields(protection *bindingProtection, derivedFrom *domain.CanonicalEndpoint, demotedAt *time.Time) error {
	switch {
	case protection == nil:
		return nil
	case derivedFrom != nil:
		return errProtectedDerivedBinding
	case demotedAt != nil:
		return errProtectedDemotedBinding
	default:
		return nil
	}
}

// protectionOf reads the protection of a record; nil for an ordinary one.
func protectionOf(record endpointBindingRecord) (*bindingProtection, error) {
	switch {
	case !record.Protected && record.ProtectedBy == nil && record.Pool == nil:
		return nil, nil
	case record.Protected && record.ProtectedBy == nil && record.Pool == nil:
		return &bindingProtection{by: record.Identity, pool: domain.ProtectedBindingPoolContact}, nil
	case !record.Protected || record.ProtectedBy == nil || record.Pool == nil:
		return nil, errContradictoryBindingRecord
	case record.ProtectedBy.IsZero() || !record.Pool.Valid():
		return nil, errIncompleteBindingRecord
	}
	return &bindingProtection{by: *record.ProtectedBy, pool: *record.Pool}, nil
}

// groupOf reads the source group of a record. A record written before groups
// existed gets the group of its own address: a name's connected address was
// not kept, so a name falls into the shared unresolved-name group.
func groupOf(endpoint domain.CanonicalEndpoint, record endpointBindingRecord) (domain.SourceGroup, error) {
	if record.SourceGroup == nil {
		return endpointSourceGroup(endpoint, nil), nil
	}
	if *record.SourceGroup == "" {
		return "", errIncompleteBindingRecord
	}
	return *record.SourceGroup, nil
}

// derivedFromOf reads the name a derived key was reached by; nil for an
// endpoint dialled by itself.
func derivedFromOf(record endpointBindingRecord) (*domain.CanonicalEndpoint, error) {
	if record.DerivedFrom == nil {
		return nil, nil
	}
	name, err := canonicalEndpoint(*record.DerivedFrom)
	if err != nil {
		return nil, errIncompleteBindingRecord
	}
	return &name, nil
}

func (s *secureSessionStore) load() error {
	dir, err := secretfile.Open(s.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	// A crash between a temp and its rename leaves the temp behind, up to
	// the size of the whole store; only old ones go, since the directory is
	// shared with nodes on other ports that may be writing right now.
	sweepStaleSecretTemps(dir)
	raw, err := dir.ReadFile(s.name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read: %w", secretfile.StripPath(err))
	}
	var file secureSessionFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if file.Format != secureSessionStoreFormat {
		return fmt.Errorf("unknown format %d", file.Format)
	}
	return s.loadFile(file)
}

// loadFile puts what a decoded file holds into the empty store. Nothing else
// holds the store yet; mu is taken for the counters' writers, whose contract
// it is. The journal lines of the load are written after mu is released.
func (s *secureSessionStore) loadFile(file secureSessionFile) error {
	now := s.now()
	s.mu.Lock()
	for id, at := range file.Identities {
		if id.IsZero() {
			s.mu.Unlock()
			return errZeroPinnedIdentity
		}
		s.identities[id] = at
	}
	report, err := s.loadBindingsLocked(file.Endpoints, now)
	if err == nil {
		s.rebuildGeneralCreationsLocked(now)
	}
	s.mu.Unlock()
	s.logLoadReport(report)
	return err
}

// loadedBinding is a binding read from the file with the address it was
// stored under, which decides between two records of one endpoint.
type loadedBinding struct {
	stored  domain.PeerAddress
	binding endpointBinding
}

// bindingLoadReport is what loading the bindings found against the file, for
// the journal.
type bindingLoadReport struct {
	kept   []keptNonCanonicalRecord
	merged []mergedBindingRecords
}

// keptNonCanonicalRecord is a record whose stored address has no canonical
// form. The previous build keyed bindings by the raw dialled address, so its
// file may hold one. It is kept under that exact address (nonCanonical).
type keptNonCanonicalRecord struct {
	stored domain.PeerAddress
	tier   endpointBindingTier
	reason error
}

// mergedBindingRecords is a record of an endpoint folded into the binding
// another record of the same canonical endpoint keeps the owner of.
type mergedBindingRecords struct {
	endpoint domain.CanonicalEndpoint
	owner    loadedBinding
	merged   endpointBinding
	dropped  loadedBinding
}

// loadBindingsLocked reads the bindings of the file under their canonical
// keys. Records that are spellings of one endpoint — written by the previous
// build — become one by mergeLoadedBindings, which never lowers protection;
// the merged state is marked unwritten, so the file converges on the next
// write. A record whose address has no canonical form is kept as it was under
// that exact address: dropping it would remove a protection automatically.
// Caller holds mu.
func (s *secureSessionStore) loadBindingsLocked(records map[domain.PeerAddress]endpointBindingRecord, now time.Time) (bindingLoadReport, error) {
	var report bindingLoadReport
	spellings := make(map[domain.CanonicalEndpoint][]loadedBinding, len(records))
	for stored, record := range records {
		endpoint, keyErr := canonicalEndpoint(stored)
		if keyErr != nil {
			binding, err := nonCanonicalBindingFromRecord(record)
			if err != nil {
				return report, err
			}
			clampLoadedTimes(&binding, now)
			s.nonCanonical[stored] = binding
			s.countBindingLocked(binding)
			report.kept = append(report.kept, keptNonCanonicalRecord{stored: stored, tier: binding.tier(), reason: keyErr})
			continue
		}
		binding, err := bindingFromRecord(endpoint, record)
		if err != nil {
			return report, err
		}
		clampLoadedTimes(&binding, now)
		spellings[endpoint] = append(spellings[endpoint], loadedBinding{stored: stored, binding: binding})
	}
	for endpoint, loaded := range spellings {
		merged := mergeLoadedBindings(loaded)
		for _, dropped := range loaded[1:] {
			report.merged = append(report.merged, mergedBindingRecords{endpoint: endpoint, owner: loaded[0], merged: merged, dropped: dropped})
		}
		s.insertBindingLocked(endpoint, merged)
	}
	if len(report.merged) > 0 {
		s.changes++
	}
	return report, nil
}

// nonCanonicalBindingFromRecord checks a record stored under an address
// without a canonical form like any other record, except that its transport,
// which cannot be compared with the address, need only be a known one; a
// record without a group falls into the shared unresolved-name group.
func nonCanonicalBindingFromRecord(record endpointBindingRecord) (endpointBinding, error) {
	if record.Identity.IsZero() {
		return endpointBinding{}, errIncompleteBindingRecord
	}
	if record.Transport != nil && !record.Transport.Valid() {
		return endpointBinding{}, errBindingTransportMismatch
	}
	protection, err := protectionOf(record)
	if err != nil {
		return endpointBinding{}, err
	}
	derivedFrom, err := derivedFromOf(record)
	if err != nil {
		return endpointBinding{}, err
	}
	if err := checkTierFields(protection, derivedFrom, record.DemotedAt); err != nil {
		return endpointBinding{}, err
	}
	group := domain.SourceGroupUnresolvedName
	if record.SourceGroup != nil {
		if *record.SourceGroup == "" {
			return endpointBinding{}, errIncompleteBindingRecord
		}
		group = *record.SourceGroup
	}
	return endpointBinding{identity: record.Identity, lastV2: record.LastV2, protection: protection, group: group, firstAt: record.FirstAt, derivedFrom: derivedFrom, demotedAt: record.DemotedAt}, nil
}

// clampLoadedTimes brings times in the future (a clock set back since) to
// now: a last_v2 or a demoted_at left there would stretch the binding past its
// 30 days, a first_at would hold a place of its group's quota longer than a
// day.
func clampLoadedTimes(binding *endpointBinding, now time.Time) {
	if binding.lastV2.After(now) {
		binding.lastV2 = now
	}
	binding.firstAt = clampedTime(binding.firstAt, now)
	binding.demotedAt = clampedTime(binding.demotedAt, now)
}

// clampedTime is at, or now when at lies after now; nil stays nil.
func clampedTime(at *time.Time, now time.Time) *time.Time {
	if at == nil || !at.After(now) {
		return at
	}
	clamped := now
	return &clamped
}

// mergeLoadedBindings folds the records of one endpoint into one binding,
// field by field, and leaves loaded ordered with the owner first. Every order
// below is total, so the outcome does not depend on the order the file's map
// is read in:
//
//   - the owner — identity, last v2, group — is the record with the latest
//     last v2, then the smaller stored address: the identity last proven on
//     the endpoint;
//   - the protection is the strongest: any over none, the contact pool over
//     the relation pool — a merge never lowers a binding or moves a
//     contact's place into the relation pool — and its giver comes with it;
//     among equal pools, the owner order decides. A protected binding's last
//     v2 is not written on renewal, so it cannot decide the pool;
//   - the creation time is the earliest, and unknown if any record's is;
//   - a derived key stays derived, and a demotion stays with its latest
//     moment — the longest term — only while the merged binding is ordinary.
func mergeLoadedBindings(loaded []loadedBinding) endpointBinding {
	slices.SortFunc(loaded, func(a, b loadedBinding) int {
		if order := b.binding.lastV2.Compare(a.binding.lastV2); order != 0 {
			return order
		}
		return strings.Compare(string(a.stored), string(b.stored))
	})
	merged := loaded[0].binding
	merged.protection = strongestProtection(loaded)
	merged.firstAt = earliestCreation(loaded)
	merged.demotedAt = latestDemotion(loaded)
	merged.limitRefusalCounted = false
	if merged.protection != nil {
		merged.derivedFrom, merged.demotedAt = nil, nil
	}
	return merged
}

// latestDemotion is the latest moment an operator demoted any of the records;
// nil when none was demoted.
func latestDemotion(loaded []loadedBinding) *time.Time {
	var latest *time.Time
	for _, entry := range loaded {
		if at := entry.binding.demotedAt; at != nil && (latest == nil || at.After(*latest)) {
			latest = at
		}
	}
	return latest
}

// strongestProtection is the protection of the first record, in owner order,
// with the strongest pool; nil when no record is protected.
func strongestProtection(loaded []loadedBinding) *bindingProtection {
	var strongest *bindingProtection
	for _, entry := range loaded {
		protection := entry.binding.protection
		switch {
		case protection == nil:
		case strongest == nil:
			strongest = protection
		case strongest.pool != domain.ProtectedBindingPoolContact && protection.pool == domain.ProtectedBindingPoolContact:
			strongest = protection
		}
	}
	return strongest
}

// earliestCreation is the earliest creation time of the records; nil when any
// record was written before creation times were kept, since it is older than
// every known one.
func earliestCreation(loaded []loadedBinding) *time.Time {
	var earliest *time.Time
	for _, entry := range loaded {
		if entry.binding.firstAt == nil {
			return nil
		}
		if earliest == nil || entry.binding.firstAt.Before(*earliest) {
			earliest = entry.binding.firstAt
		}
	}
	return earliest
}

// logLoadReport journals what the load found against the file. The lines
// pair endpoints with identities, so like logOwnerChange they stay in the
// local log. Called outside mu.
func (s *secureSessionStore) logLoadReport(report bindingLoadReport) {
	for _, kept := range report.kept {
		s.logger.Warn().Err(kept.reason).Str("store", s.name).Str("peer", string(kept.stored)).
			Str("tier", kept.tier.String()).
			Msg("endpoint_binding_kept_not_canonical")
	}
	for _, merged := range report.merged {
		s.logger.Info().Str("store", s.name).
			Str("peer", merged.endpoint.String()).
			Str("owner_from", string(merged.owner.stored)).
			Str("identity", merged.merged.identity.String()).
			Str("tier", merged.merged.tier().String()).
			Str("dropped_from", string(merged.dropped.stored)).
			Str("dropped_identity", merged.dropped.binding.identity.String()).
			Str("dropped_tier", merged.dropped.binding.tier().String()).
			Msg("endpoint_binding_merged")
	}
}

// stats is the store's state for diagnostics. Takes mu, a leaf held for
// map reads only.
func (s *secureSessionStore) stats() domain.SecureSessionStoreStats {
	guard := s.clock.Stats()
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.SecureSessionStoreStats{
		ReadAt:                              s.clock.Wall(),
		PinnedIdentities:                    len(s.identities),
		PinCapacity:                         maxPinnedIdentities,
		Full:                                len(s.identities) >= maxPinnedIdentities,
		PinRefusalsStoreFull:                s.pinRefusals,
		V1RefusalsIdentityPinned:            s.v1RefusalsIdentity,
		V1RefusalsEndpointBound:             s.v1RefusalsEndpoint,
		V1DialsHeldByRefusalBackoff:         s.v1DialsHeld,
		DialsHeldByV2RetryBackoff:           s.dialsHeldV2Retry,
		ProtectedBindingsContact:            s.protectedByPool[domain.ProtectedBindingPoolContact],
		ProtectedBindingsRelation:           s.protectedByPool[domain.ProtectedBindingPoolRelation],
		ProtectedCapacity:                   maxProtectedBindings,
		ProtectedReserveContact:             protectedReserveContact,
		ProtectedPerIdentityCapacity:        maxProtectedPerIdentity,
		GeneralBindings:                     s.generalBindings,
		GeneralCapacity:                     maxGeneralBindings,
		ProtectedRefusalsPerIdentity:        s.protectedRefusals[domain.ProtectedBindingRefusedPerIdentity],
		ProtectedRefusalsPoolRFull:          s.protectedRefusals[domain.ProtectedBindingRefusedPoolRFull],
		ProtectedRefusalsStoreFull:          s.protectedRefusals[domain.ProtectedBindingRefusedStoreFull],
		GeneralCreationsPerGroupPerDay:      generalCreationsPerGroupPerDay,
		GeneralRefusalsSourceGroupQuota:     s.groupQuotaRefusals,
		EndpointBindingsSkippedNotCanonical: s.skippedNotCanonical,
		NonCanonicalBindings:                len(s.nonCanonical),
		Unreadable:                          s.unreadable,
		LocationUnsafe:                      s.locationUnsafe,
		ClockJumpGuarded:                    guard.Guarded,
		ClockJumpsDetected:                  guard.JumpsDetected,
		ClockJumpActive:                     guard.JumpActive,
	}
}

// noteIdentityRefusal counts one v1 session refused because the identity it
// names requires v2.
func (s *secureSessionStore) noteIdentityRefusal() {
	s.mu.Lock()
	s.v1RefusalsIdentity++
	s.mu.Unlock()
}

// noteEndpointRefusal counts one v1 answer refused because the endpoint that
// gave it is bound to v2.
func (s *secureSessionStore) noteEndpointRefusal() {
	s.mu.Lock()
	s.v1RefusalsEndpoint++
	s.mu.Unlock()
}

// noteLegacyDialHeld counts one v1 dial withheld by the hold that follows a
// refused v1 answer from the same address.
func (s *secureSessionStore) noteLegacyDialHeld() {
	s.mu.Lock()
	s.v1DialsHeld++
	s.mu.Unlock()
}

// noteDialHeldByV2Retry counts one dial refused before any socket by its
// address's v2 retry backoff.
func (s *secureSessionStore) noteDialHeldByV2Retry() {
	s.mu.Lock()
	s.dialsHeldV2Retry++
	s.mu.Unlock()
}

// identityPinned reports whether a v1 session naming id must be refused.
func (s *secureSessionStore) identityPinned(id domain.PeerIdentity) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable {
		return true
	}
	_, pinned := s.identities[id]
	return pinned
}

// endpointRequiresV2 reports whether a v1 answer from address is a downgrade.
// An alias of another host (errEndpointHostAlias) always requires v2; any
// other address without a canonical form only where a record of the file
// kept under that exact address (nonCanonical) says so.
func (s *secureSessionStore) endpointRequiresV2(address domain.PeerAddress) bool {
	endpoint, keyErr := canonicalEndpoint(address)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable {
		return true
	}
	var binding endpointBinding
	var bound bool
	switch {
	case keyErr == nil:
		binding, bound = s.endpoints[endpoint]
	case errors.Is(keyErr, errEndpointHostAlias):
		// The host it names may be bound under its canonical key, which the
		// alias cannot be brought to without resolving it; v1 is not
		// followed on the word of an alias.
		return true
	default:
		binding, bound = s.nonCanonical[address]
	}
	return bound && binding.live(s.now())
}

// endpointsOfIdentity are the endpoints whose live binding names one identity
// as the last proven there, as one snapshot of the store.
type endpointsOfIdentity struct {
	canonical    map[domain.CanonicalEndpoint]bool
	nonCanonical map[domain.PeerAddress]bool
}

// covers reports whether address is one of the endpoints: under its
// canonical key, or — for an address without one — exactly as a binding an
// earlier build wrote stores it.
func (e endpointsOfIdentity) covers(address domain.PeerAddress) bool {
	endpoint, err := canonicalEndpoint(address)
	if err != nil {
		return e.nonCanonical[address]
	}
	return e.canonical[endpoint]
}

// endpointsBoundTo is the snapshot of the endpoints whose live binding was
// last proven by id over v2. Takes mu only; a caller that needs it under a
// domain mutex takes the snapshot before entering that mutex and passes it
// in, so the store's mu never nests under a domain mutex and adds no edge to
// the canonical lock order.
func (s *secureSessionStore) endpointsBoundTo(id domain.PeerIdentity) endpointsOfIdentity {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	bound := endpointsOfIdentity{canonical: make(map[domain.CanonicalEndpoint]bool), nonCanonical: make(map[domain.PeerAddress]bool)}
	for endpoint, binding := range s.endpoints {
		if binding.identity == id && binding.live(now) {
			bound.canonical[endpoint] = true
		}
	}
	for address, binding := range s.nonCanonical {
		if binding.identity == id && binding.live(now) {
			bound.nonCanonical[address] = true
		}
	}
	return bound
}

// noteProvenInbound pins a peer proven on an accepted connection. An error
// means the protection is not stored — errPinStoreFull for a new identity at
// the bound, or a write failure — and the caller must not establish the
// session on it.
func (s *secureSessionStore) noteProvenInbound(id domain.PeerIdentity) error {
	return s.pinOnly(id, func() {})
}

// pinOnly pins id and binds nothing; alsoLocked runs under mu once the pin is
// stored, for a counter of the caller's. An error is noteProvenInbound's.
func (s *secureSessionStore) pinOnly(id domain.PeerIdentity, alsoLocked func()) error {
	var refused error
	pinned := 0
	err := s.update(func(now time.Time) bool {
		added, pinErr := s.pinLocked(id, now)
		refused, pinned = pinErr, len(s.identities)
		if pinErr == nil {
			alsoLocked()
		}
		return added
	})
	if refused != nil {
		s.logPinRefusal(id, pinned)
		return refused
	}
	return err
}

// outboundDial is the endpoint a proven outbound session was dialled at, and
// the address its socket reached.
type outboundDial struct {
	address domain.PeerAddress
	// connected is the peer address of the dialled socket; nil when the
	// socket has no IP peer (net.Pipe in tests). On a dial through the SOCKS5
	// proxy it is the proxy, which bindingTargetOf never uses.
	connected *netip.AddrPort
}

// outboundDialOf is the dial of address that conn carries.
func outboundDialOf(address domain.PeerAddress, conn net.Conn) outboundDial {
	tcp, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return outboundDial{address: address}
	}
	connected := tcp.AddrPort()
	return outboundDial{address: address, connected: &connected}
}

// bindingTarget is what a proven outbound dial binds: its canonical endpoint
// in its source group and, for a name dialled direct, the address the dial
// reached as a derived key in the same group.
type bindingTarget struct {
	endpoint domain.CanonicalEndpoint
	group    domain.SourceGroup
	derived  *domain.CanonicalEndpoint
}

// bindingTargetOf works out what dial binds. It fails only when the dialled
// address has no canonical form, and then nothing is bound.
func bindingTargetOf(dial outboundDial) (bindingTarget, error) {
	endpoint, err := canonicalEndpoint(dial.address)
	if err != nil {
		return bindingTarget{}, err
	}
	reached := nameReachedAt(endpoint, dial.connected)
	if reached == nil {
		return bindingTarget{endpoint: endpoint, group: endpointSourceGroup(endpoint, nil)}, nil
	}
	addr := reached.Addr()
	target := bindingTarget{endpoint: endpoint, group: endpointSourceGroup(endpoint, &addr)}
	if derived, err := canonicalEndpoint(domain.PeerAddress(reached.String())); err == nil {
		target.derived = &derived
	}
	return target, nil
}

// nameReachedAt is the address a name dialled direct reached; nil for an IP
// endpoint (it is its own address), an overlay (the socket reached the
// proxy, not the peer) and a socket whose peer is unknown.
func nameReachedAt(endpoint domain.CanonicalEndpoint, connected *netip.AddrPort) *netip.AddrPort {
	if connected == nil || endpoint.Transport() != domain.EndpointTransportTCP {
		return nil
	}
	if _, err := netip.ParseAddr(endpoint.Host()); err == nil {
		return nil
	}
	return connected
}

// noteProvenOutbound pins the peer and binds the endpoint this node dialled,
// with the protection the dial asks for — and, for a name dialled direct, the
// address the dial reached as a derived ordinary key. The result says what
// the endpoint has now. An error means the session must not be established
// on it: its protection is not stored (errPinStoreFull, a write failure), or
// the dial required the endpoint protected and the protection was refused
// (*domain.ProtectedBindingRefused). A session refused with errPinStoreFull
// leaves no endpoint binding: it was not established, so nothing it earned is
// recorded. A required protection that is refused still leaves the ordinary
// binding the v2 session earned on the endpoint — the binding records a
// completed mutual v2 handshake on an endpoint this node dialled, which is
// what creates one, and dropping it would only weaken the endpoint; it is the
// operation that needed the protection that does not complete. An address
// without a canonical form gets no binding, and the session stands on its
// identity pin.
func (s *secureSessionStore) noteProvenOutbound(dial outboundDial, id domain.PeerIdentity, protection endpointProtection) (endpointBindingResult, error) {
	target, keyErr := bindingTargetOf(dial)
	if keyErr != nil {
		return s.noteProvenWithoutBinding(dial.address, id, keyErr)
	}
	if protection.asked() && !protection.pool.Valid() {
		s.logger.Error().Str("peer", target.endpoint.String()).Str("pool", string(protection.pool)).Msg("secure_session_protection_with_unknown_pool_ignored")
		protection = endpointProtectionNone()
	}
	result := endpointBindingResult{tier: bindingTierNone}
	var refused error
	pinned := 0
	err := s.update(func(now time.Time) bool {
		added, pinErr := s.pinLocked(id, now)
		if pinErr != nil {
			refused, pinned = pinErr, len(s.identities)
			return false
		}
		var bound bool
		result, bound = s.bindTargetLocked(target, id, protection, now)
		return added || bound
	})
	if refused != nil {
		s.logPinRefusal(id, pinned)
		return endpointBindingResult{tier: bindingTierNone}, refused
	}
	s.logBindingOutcome(target.endpoint, id, result)
	if err != nil {
		return result, err
	}
	if result.refused != nil && protection.kind == protectionRequired {
		return result, result.refused
	}
	return result, nil
}

// noteProvenWithoutBinding pins id proven on a dial to address, which has no
// canonical form (reason) and so gets no binding: binding it under its raw
// spelling would give one host two keys, the very gap canonicalEndpoint
// closes. The session stands on the pin.
func (s *secureSessionStore) noteProvenWithoutBinding(address domain.PeerAddress, id domain.PeerIdentity, reason error) (endpointBindingResult, error) {
	err := s.pinOnly(id, func() { s.skippedNotCanonical++ })
	if errors.Is(err, errPinStoreFull) {
		return endpointBindingResult{tier: bindingTierNone}, err
	}
	s.logger.Warn().Str("peer", string(address)).Str("reason", reason.Error()).
		Msg("endpoint_binding_skipped_not_canonical")
	return endpointBindingResult{tier: bindingTierNone}, err
}

func (s *secureSessionStore) logPinRefusal(id domain.PeerIdentity, pinned int) {
	s.logger.Warn().
		Str("peer", id.String()).
		Str("reason", errPinStoreFull.Error()).
		Int("pinned", pinned).
		Msg("secure_session_refused_pin_store_full")
}

// logBindingOutcome reports the protection a note gave, and what it could not
// give: a protection refused at a bound, once per binding; an endpoint refused
// by its group's daily quota, once per group until the group has room again;
// an endpoint left without any binding by a full tier. The derived key's
// outcome follows. The protection line pairs an endpoint with an identity, so
// like logOwnerChange it stays in the local log.
func (s *secureSessionStore) logBindingOutcome(endpoint domain.CanonicalEndpoint, id domain.PeerIdentity, result endpointBindingResult) {
	if result.given != nil {
		s.logger.Info().
			Str("peer", endpoint.String()).
			Str("identity", id.String()).
			Str("protected_by", result.given.by.String()).
			Str("pool", string(result.given.pool)).
			Msg("endpoint_binding_protected")
	}
	if result.limitRefusalReported {
		s.logger.Warn().
			Str("peer", endpoint.String()).
			Str("identity", id.String()).
			Str("reason", string(result.refused.Reason)).
			Str("recorded", result.tier.String()).
			Msg("protected_binding_limit_reached")
	}
	switch {
	case result.quotaRefusal != nil:
		s.logQuotaRefusal(*result.quotaRefusal)
	case result.tier == bindingTierNone:
		s.logger.Warn().Str("peer", endpoint.String()).Msg("secure_session_endpoint_binding_refused_store_full")
	}
	s.logExpired(result.expired)
	if result.derived != nil {
		s.logBindingOutcome(result.derived.endpoint, id, result.derived.result)
	}
}

func (s *secureSessionStore) logQuotaRefusal(refusal sourceGroupQuotaRefusal) {
	if !refusal.journal {
		return
	}
	s.logger.Warn().
		Str("peer", refusal.endpoint.String()).
		Str("source_group", string(refusal.group)).
		Int("quota_per_day", generalCreationsPerGroupPerDay).
		Msg("endpoint_binding_refused_source_group_quota")
}

// logOwnerChange journals a note that took a live binding — the endpoint's or
// its derived key's — over from another identity, with the connection the new
// owner proved itself on. The line pairs an endpoint with identities, so it
// stays in the local log: no RPC answer carries it. Called after the note,
// outside mu.
func (s *secureSessionStore) logOwnerChange(id domain.PeerIdentity, result endpointBindingResult, connID domain.ConnID) {
	if result.derived != nil {
		s.logOwnerChange(id, result.derived.result, connID)
	}
	if result.ownerChange == nil {
		return
	}
	s.logger.Info().
		Str("peer", result.ownerChange.endpoint.String()).
		Str("previous_identity", result.ownerChange.previous.String()).
		Str("identity", id.String()).
		Uint64("conn_id", uint64(connID)).
		Str("tier", result.tier.String()).
		Msg("endpoint_owner_changed")
}

// endpointOwnerMismatch is a dial "to expected" at address on which another
// identity proved itself, on the dial's connection connID. proven is nil
// when the handshake did not name it.
type endpointOwnerMismatch struct {
	address  domain.PeerAddress
	expected domain.PeerIdentity
	proven   *domain.PeerIdentity
	connID   domain.ConnID
}

// logOwnerMismatch journals a mismatch: the dial failed and the binding was
// left as it was. Like logOwnerChange, the line stays in the local log.
func (s *secureSessionStore) logOwnerMismatch(mismatch endpointOwnerMismatch) {
	line := s.logger.Warn().
		Str("peer", string(mismatch.address)).
		Str("expected_identity", mismatch.expected.String()).
		Uint64("conn_id", uint64(mismatch.connID))
	if mismatch.proven != nil {
		line = line.Str("identity", mismatch.proven.String())
	}
	line.Msg("endpoint_owner_mismatch")
}

// logExpired journals every ordinary binding removed at the end of its term.
// Like logOwnerChange, the lines stay in the local log. Called outside mu.
func (s *secureSessionStore) logExpired(expired []expiredEndpointBinding) {
	for _, binding := range expired {
		s.logger.Info().
			Str("peer", string(binding.address)).
			Str("identity", binding.identity.String()).
			Time("last_v2", binding.lastV2).
			Msg("endpoint_binding_expired")
	}
}

// flushRenewals removes the ordinary bindings past their term and writes the
// removals and the renewals made since the last snapshot, and retries a
// change whose write failed, each at most once per renewalFlushEvery. Called
// on the bootstrap tick; nothing is written when nothing is pending.
func (s *secureSessionStore) flushRenewals() error {
	return s.flushPending(flushRenewalsWhenDue)
}

// flushRenewalsNow is flushRenewals without the wait: the last flush when the
// node stops.
func (s *secureSessionStore) flushRenewalsNow() error {
	return s.flushPending(flushRenewalsAtStop)
}

// renewalFlush is the moment a periodic pass over the store last ran: a
// flush that wrote, or a sweep of expired bindings.
type renewalFlush struct {
	at time.Time
}

// dueAt reports whether a pass that waits for renewalFlushEvery may run at
// at; one that never ran may.
func (f *renewalFlush) dueAt(at time.Time) bool {
	return f == nil || at.Sub(f.at) >= renewalFlushEvery
}

// flushPending removes the ordinary bindings past their term and writes what
// is unwritten — the removals, the batched renewals, a change whose write
// failed. A flush that waits does each at most once per renewalFlushEvery;
// the flush at stop does both at once.
func (s *secureSessionStore) flushPending(moment renewalFlushMoment) error {
	// A wall clock jump found since the last tick — by this flush or by any
	// lookup — is journalled on the way out, once mu is released: the
	// lookups that find it run under mu.
	defer s.reportClockJump()
	s.mu.Lock()
	if s.unreadable {
		// Fail-closed: every endpoint is treated as bound, and nothing may be
		// written over the file that could not be read.
		s.mu.Unlock()
		return nil
	}
	at := s.now()
	expired := s.sweepExpiredWhenDueLocked(moment, at)
	if moment == flushRenewalsWhenDue && !s.lastFlush.dueAt(at) {
		// The removals are made in memory and wait for the next batch, like
		// renewals: until then they only drop bindings that no longer
		// require v2.
		s.renewalsUnwritten = s.renewalsUnwritten || len(expired) > 0
		s.mu.Unlock()
		s.logExpired(expired)
		return nil
	}
	if s.renewalsUnwritten || len(expired) > 0 {
		s.changes++
	}
	pending := s.changes != s.written
	if pending {
		s.lastFlush = &renewalFlush{at: at}
	}
	s.mu.Unlock()
	s.logExpired(expired)
	if !pending {
		return nil
	}
	return s.persist()
}

// sweepExpiredWhenDueLocked removes the ordinary bindings past their term at
// at, at most once per renewalFlushEvery unless the node is stopping: a scan
// of every binding under mu on each bootstrap tick would cost lookups for
// nothing, since an expired binding already stops requiring v2 the moment
// its term ends (endpointBinding.live). Caller holds mu.
func (s *secureSessionStore) sweepExpiredWhenDueLocked(moment renewalFlushMoment, at time.Time) []expiredEndpointBinding {
	if moment == flushRenewalsWhenDue && !s.lastExpirySweep.dueAt(at) {
		return nil
	}
	s.lastExpirySweep = &renewalFlush{at: at}
	s.forgetOldCreationsLocked(at)
	return s.pruneExpiredLocked(at)
}

// update runs change under mu, then writes the store when the change — or an
// earlier one that never reached the disk — calls for it.
func (s *secureSessionStore) update(change func(now time.Time) bool) error {
	s.mu.Lock()
	if s.unreadable {
		// The file could not be read; writing now would replace whatever
		// protection it held with this process's partial view.
		s.mu.Unlock()
		return nil
	}
	if change(s.now()) {
		s.changes++
	}
	pending := s.changes != s.written
	s.mu.Unlock()
	if !pending {
		return nil
	}
	return s.persist()
}

// pinLocked pins id and reports whether that changed the store. An identity
// already pinned is served as before; a NEW identity at the bound is refused
// with errPinStoreFull — never by evicting an existing pin: existing
// protection is guaranteed, new pins are not promised. Caller holds mu; the
// caller logs a refusal after releasing it.
func (s *secureSessionStore) pinLocked(id domain.PeerIdentity, now time.Time) (bool, error) {
	if _, pinned := s.identities[id]; pinned {
		return false, nil
	}
	if len(s.identities) >= maxPinnedIdentities {
		s.pinRefusals++
		return false, errPinStoreFull
	}
	s.identities[id] = now
	return true, nil
}

// bindingRequest is one endpoint a note binds: the endpoint in its source
// group, the name it was reached by for a derived key, the identity proven on
// it and the protection the dial asks for.
type bindingRequest struct {
	endpoint    domain.CanonicalEndpoint
	group       domain.SourceGroup
	derivedFrom *domain.CanonicalEndpoint
	id          domain.PeerIdentity
	protection  endpointProtection
	// quota is the dial's place in its group's daily quota, shared by the
	// name and its derived key.
	quota *dialQuota
}

// dialQuota is one dial's place in its source group's daily quota: a name and
// the derived key it reached are one creation, so they take one place
// together and a refusal of the pair is counted once.
type dialQuota struct {
	charged bool
	refused bool
}

// bindTargetLocked binds the dialled endpoint and, for a name dialled direct,
// the address it reached as a derived key: always ordinary, whatever the dial
// asked for the name. It reports the endpoint's outcome with the derived
// key's inside, and whether the change must reach the disk now. Caller holds
// mu.
func (s *secureSessionStore) bindTargetLocked(target bindingTarget, id domain.PeerIdentity, protection endpointProtection, now time.Time) (endpointBindingResult, bool) {
	quota := &dialQuota{}
	result, due := s.bindLocked(bindingRequest{endpoint: target.endpoint, group: target.group, id: id, protection: protection, quota: quota}, now)
	if target.derived == nil {
		return result, due
	}
	name := target.endpoint
	derived, derivedDue := s.bindLocked(bindingRequest{endpoint: *target.derived, group: target.group, derivedFrom: &name, id: id, protection: endpointProtectionNone(), quota: quota}, now)
	result.derived = &derivedBindingOutcome{endpoint: *target.derived, result: derived}
	return result, due || derivedDue
}

// bindLocked binds the requested endpoint — renewing a live binding,
// creating a new one otherwise — and reports the outcome and whether the
// change must reach the disk now. The outcome names a previous owner the
// binding was taken from and every expired binding removed on the way, for
// the caller to journal once mu is released. Caller holds mu.
func (s *secureSessionStore) bindLocked(req bindingRequest, now time.Time) (endpointBindingResult, bool) {
	binding, bound := s.endpoints[req.endpoint]
	if bound && binding.live(now) {
		return s.renewBindingLocked(req, binding, now)
	}
	return s.createBindingLocked(req, bound, now)
}

// laterOf is the later of two times. A renewal never moves last_v2 back: a
// wall clock set back is taken as it is, and renewals stamped with it would
// end the binding at once when the clock returns — a return is no jump, so
// the trusted time does not hold it back.
func laterOf(recorded, at time.Time) time.Time {
	if at.After(recorded) {
		return at
	}
	return recorded
}

// renewBindingLocked records a new v2 session on a live binding. A new owner
// takes the binding over with its tier — a protected one stays protected,
// counted for whoever gave the protection — and never raises it: only the
// identity that already owned the endpoint can, so a protection asked for by
// a new owner is refused with owner_changed (not a bound, so not counted as
// one). A derived key is never raised either (derived_key). A protected
// binding is never lowered and its pool never changes; the source group, the
// creation time and the name a key derives from stay as they were. Caller
// holds mu.
func (s *secureSessionStore) renewBindingLocked(req bindingRequest, binding endpointBinding, now time.Time) (endpointBindingResult, bool) {
	changedOwner := binding.identity != req.id
	result := endpointBindingResult{tier: binding.tier()}
	raised := false
	switch {
	case !req.protection.asked() || binding.protection != nil:
	case binding.derivedFrom != nil:
		result.refused = &domain.ProtectedBindingRefused{Reason: domain.ProtectedBindingRefusedDerivedKey}
	case changedOwner:
		result.refused = &domain.ProtectedBindingRefused{Reason: domain.ProtectedBindingRefusedOwnerChanged}
	default:
		result.refused = s.protectedRefusalLocked(req.id, req.protection.pool)
		if result.refused == nil {
			s.protectBindingLocked(&binding, req.id, req.protection.pool)
			raised, result.tier, result.given = true, bindingTierProtected, binding.protection
		} else if !binding.limitRefusalCounted {
			s.countLimitRefusalLocked(result.refused.Reason)
			binding.limitRefusalCounted, result.limitRefusalReported = true, true
		}
	}
	if changedOwner {
		result.ownerChange = &endpointOwnerChange{endpoint: req.endpoint, previous: binding.identity}
	}
	binding.identity, binding.lastV2 = req.id, laterOf(binding.lastV2, now)
	s.endpoints[req.endpoint] = binding
	due := changedOwner || raised
	if !due && binding.protection == nil {
		// A protected binding never expires, so its last_v2 changes
		// nothing that has to survive a crash; an ordinary one waits for
		// the batch.
		s.renewalsUnwritten = true
	}
	return result, due
}

// createBindingLocked binds an endpoint that has no live binding: protected
// when asked and a place is free, ordinary otherwise — or not at all when the
// endpoint's source group created its new ordinary bindings for the day, or
// when the ordinary tier is full too. An expired binding under the same
// endpoint is replaced. A refusal at a bound is counted here; when no binding
// is left to remember that, the next attempt counts again. Caller holds mu.
func (s *secureSessionStore) createBindingLocked(req bindingRequest, expired bool, now time.Time) (endpointBindingResult, bool) {
	result := endpointBindingResult{tier: bindingTierNone}
	if expired {
		result.expired = append(result.expired, s.removeExpiredLocked(req.endpoint))
	}
	created := now
	binding := endpointBinding{identity: req.id, lastV2: now, group: req.group, firstAt: &created, derivedFrom: req.derivedFrom}
	if req.protection.asked() {
		result.refused = s.protectedRefusalLocked(req.id, req.protection.pool)
		if result.refused == nil {
			binding.protection = &bindingProtection{by: req.id, pool: req.protection.pool}
			result.given = binding.protection
		} else {
			s.countLimitRefusalLocked(result.refused.Reason)
			binding.limitRefusalCounted, result.limitRefusalReported = true, true
		}
	}
	if binding.protection == nil {
		if refusal := s.quotaRefusalLocked(req, now); refusal != nil {
			result.quotaRefusal = refusal
			return result, len(result.expired) > 0
		}
		room, pruned := s.generalRoomLocked(now)
		result.expired = append(result.expired, pruned...)
		if !room {
			return result, len(result.expired) > 0
		}
		if !req.quota.charged {
			s.recordGeneralCreationLocked(req.group, now)
			req.quota.charged = true
		}
	}
	s.insertBindingLocked(req.endpoint, binding)
	result.tier = binding.tier()
	return result, true
}

// quotaRefusalLocked decides whether the dial of req may create one more
// ordinary binding in its group: a dial that already took its place may, and
// one already refused is refused again without being counted twice. Caller
// holds mu.
func (s *secureSessionStore) quotaRefusalLocked(req bindingRequest, now time.Time) *sourceGroupQuotaRefusal {
	switch {
	case req.quota.charged:
		return nil
	case req.quota.refused:
		return &sourceGroupQuotaRefusal{endpoint: req.endpoint, group: req.group}
	case s.groupHasCreationRoomLocked(req.group, now):
		return nil
	}
	req.quota.refused = true
	return s.refuseByGroupQuotaLocked(req.endpoint, req.group)
}

// groupCreations is when one source group created its ordinary bindings
// within the last generalCreationWindow, oldest first as the clock gave them.
// refusalJournalled: a refusal by the quota was journalled since the group
// last created one, so the next refusals are only counted.
type groupCreations struct {
	at                []time.Time
	refusalJournalled bool
}

// forgetBefore drops the creations at or before cutoff.
func (c *groupCreations) forgetBefore(cutoff time.Time) {
	kept := 0
	for kept < len(c.at) && !c.at[kept].After(cutoff) {
		kept++
	}
	c.at = c.at[kept:]
}

// groupHasCreationRoomLocked reports whether group may create one more
// ordinary binding at now. Caller holds mu.
func (s *secureSessionStore) groupHasCreationRoomLocked(group domain.SourceGroup, now time.Time) bool {
	creations, known := s.generalCreations[group]
	if !known {
		return true
	}
	creations.forgetBefore(now.Add(-generalCreationWindow))
	return len(creations.at) < generalCreationsPerGroupPerDay
}

// recordGeneralCreationLocked counts one ordinary binding created in group at
// at. Caller holds mu.
func (s *secureSessionStore) recordGeneralCreationLocked(group domain.SourceGroup, at time.Time) {
	creations, known := s.generalCreations[group]
	if !known {
		creations = &groupCreations{}
		s.generalCreations[group] = creations
	}
	creations.at = append(creations.at, at)
	creations.refusalJournalled = false
}

// refuseByGroupQuotaLocked counts a new ordinary binding of endpoint refused
// by its group's quota and says whether this refusal is the one to journal.
// Caller holds mu and has seen groupHasCreationRoomLocked refuse, so the
// group's creations are known.
func (s *secureSessionStore) refuseByGroupQuotaLocked(endpoint domain.CanonicalEndpoint, group domain.SourceGroup) *sourceGroupQuotaRefusal {
	s.groupQuotaRefusals++
	creations := s.generalCreations[group]
	journal := !creations.refusalJournalled
	creations.refusalJournalled = true
	return &sourceGroupQuotaRefusal{endpoint: endpoint, group: group, journal: journal}
}

// rebuildGeneralCreationsLocked counts the bindings of the loaded file
// created within the last day against their groups, so a restart does not
// hand any group a place back. A binding without a creation time was written
// before the quota existed and counts against none. A protected one counts
// too: the file does not tell one created ordinary and raised within the day,
// whose place was taken, from one created protected, whose was not — so after
// a restart the quota errs on the side of fewer new bindings. A derived key
// created by the same note as its name (sharedCreation) took no place of its
// own. Caller holds mu.
func (s *secureSessionStore) rebuildGeneralCreationsLocked(now time.Time) {
	cutoff := now.Add(-generalCreationWindow)
	for _, binding := range s.endpoints {
		if binding.firstAt == nil || !binding.firstAt.After(cutoff) || s.sharedCreationLocked(binding) {
			continue
		}
		s.recordGeneralCreationLocked(binding.group, *binding.firstAt)
	}
	for _, creations := range s.generalCreations {
		slices.SortFunc(creations.at, time.Time.Compare)
	}
}

// sharedCreationLocked reports whether binding is a derived key created by
// the same note as its name: the note stamps both with one creation time, and
// the dial took one place of the quota for the pair. Caller holds mu.
func (s *secureSessionStore) sharedCreationLocked(binding endpointBinding) bool {
	if binding.derivedFrom == nil || binding.firstAt == nil {
		return false
	}
	name, bound := s.endpoints[*binding.derivedFrom]
	return bound && name.firstAt != nil && name.firstAt.Equal(*binding.firstAt)
}

// forgetOldCreationsLocked drops the creations that left the window and the
// groups left with none, so the quota's memory follows the last day's
// creations rather than every group ever seen. Caller holds mu.
func (s *secureSessionStore) forgetOldCreationsLocked(now time.Time) {
	cutoff := now.Add(-generalCreationWindow)
	for group, creations := range s.generalCreations {
		creations.forgetBefore(cutoff)
		if len(creations.at) == 0 {
			delete(s.generalCreations, group)
		}
	}
}

// protectedRefusalLocked checks the three bounds on protected bindings for
// one more given by id in pool: the identity's own bound first, then the
// pool's. The check and the insert that follows it run under the same hold
// of mu, so concurrent creations never pass a bound by even one. Caller holds
// mu.
func (s *secureSessionStore) protectedRefusalLocked(id domain.PeerIdentity, pool domain.ProtectedBindingPool) *domain.ProtectedBindingRefused {
	rule := protectedPoolRuleFor(pool)
	total := s.protectedByPool[domain.ProtectedBindingPoolContact] + s.protectedByPool[domain.ProtectedBindingPoolRelation]
	switch {
	case s.protectedByGiver[id] >= maxProtectedPerIdentity:
		return &domain.ProtectedBindingRefused{Reason: domain.ProtectedBindingRefusedPerIdentity}
	case s.protectedByPool[pool] >= rule.ceiling, total >= maxProtectedBindings:
		return &domain.ProtectedBindingRefused{Reason: rule.full}
	default:
		return nil
	}
}

// countLimitRefusalLocked counts one protection refused at a bound. Caller
// holds mu.
func (s *secureSessionStore) countLimitRefusalLocked(reason domain.ProtectedBindingRefusalReason) {
	s.protectedRefusals[reason]++
}

// generalRoomLocked reports whether one more ordinary binding fits, and the
// expired bindings it removed to make room. Only a full tier is scanned here:
// a scan per new binding would be quadratic work under mu, and the periodic
// flush sweeps the expired ones anyway (sweepExpiredWhenDueLocked). Caller
// holds mu.
func (s *secureSessionStore) generalRoomLocked(now time.Time) (bool, []expiredEndpointBinding) {
	if s.generalBindings < maxGeneralBindings {
		return true, nil
	}
	pruned := s.pruneExpiredLocked(now)
	return s.generalBindings < maxGeneralBindings, pruned
}

// pruneExpiredLocked drops ordinary bindings whose 30 days ran out — the one
// exception to "protection is never dropped", since an ordinary binding only
// records that an address once answered over v2 — and returns them for the
// journal. Protected ones are never pruned. Caller holds mu.
func (s *secureSessionStore) pruneExpiredLocked(now time.Time) []expiredEndpointBinding {
	var pruned []expiredEndpointBinding
	for endpoint, binding := range s.endpoints {
		if !binding.live(now) {
			pruned = append(pruned, s.removeExpiredLocked(endpoint))
		}
	}
	for stored, binding := range s.nonCanonical {
		if !binding.live(now) {
			delete(s.nonCanonical, stored)
			s.uncountBindingLocked(binding)
			pruned = append(pruned, expiredEndpointBinding{address: stored, identity: binding.identity, lastV2: binding.lastV2})
		}
	}
	return pruned
}

// removeExpiredLocked removes the expired binding of endpoint and returns it
// as its journal line names it. Caller holds mu and has checked the binding
// is present and past its term.
func (s *secureSessionStore) removeExpiredLocked(endpoint domain.CanonicalEndpoint) expiredEndpointBinding {
	binding := s.endpoints[endpoint]
	s.removeBindingLocked(endpoint)
	return expiredEndpointBinding{address: endpoint.Address(), identity: binding.identity, lastV2: binding.lastV2}
}

// insertBindingLocked puts binding under endpoint and counts it. Caller holds
// mu.
func (s *secureSessionStore) insertBindingLocked(endpoint domain.CanonicalEndpoint, binding endpointBinding) {
	if _, present := s.endpoints[endpoint]; present {
		s.removeBindingLocked(endpoint)
	}
	s.endpoints[endpoint] = binding
	s.countBindingLocked(binding)
}

// countBindingLocked counts binding against its tier's bounds: ordinary, or
// protected by pool and by giver. Caller holds mu.
func (s *secureSessionStore) countBindingLocked(binding endpointBinding) {
	if binding.protection == nil {
		s.generalBindings++
		return
	}
	s.protectedByPool[binding.protection.pool]++
	s.protectedByGiver[binding.protection.by]++
}

// uncountBindingLocked undoes countBindingLocked. Caller holds mu.
func (s *secureSessionStore) uncountBindingLocked(binding endpointBinding) {
	if binding.protection == nil {
		s.generalBindings--
		return
	}
	s.protectedByPool[binding.protection.pool]--
	if s.protectedByGiver[binding.protection.by]--; s.protectedByGiver[binding.protection.by] == 0 {
		delete(s.protectedByGiver, binding.protection.by)
	}
}

// removeBindingLocked deletes the binding of endpoint and uncounts it. Only
// an expired ordinary binding is ever removed automatically; an operator
// removes an ordinary one by its address (removeGeneralBinding). Caller holds
// mu.
func (s *secureSessionStore) removeBindingLocked(endpoint domain.CanonicalEndpoint) {
	binding, present := s.endpoints[endpoint]
	if !present {
		return
	}
	delete(s.endpoints, endpoint)
	s.uncountBindingLocked(binding)
}

// protectBindingLocked raises an ordinary binding to protected, given by id in
// pool, and moves it between the counts. The caller writes the binding back
// to the map. Caller holds mu.
func (s *secureSessionStore) protectBindingLocked(binding *endpointBinding, id domain.PeerIdentity, pool domain.ProtectedBindingPool) {
	binding.protection = &bindingProtection{by: id, pool: pool}
	// Protected again: the term of an earlier demotion no longer applies.
	binding.demotedAt = nil
	s.generalBindings--
	s.protectedByPool[pool]++
	s.protectedByGiver[id]++
}

// persist writes a snapshot of the store and reports a failure. The snapshot
// is taken under mu, the encoding and the write happen after it is released;
// on success the change count the snapshot covered is recorded as written.
func (s *secureSessionStore) persist() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.written == s.changes {
		// A save that waited for writeMu while another wrote the state it
		// was called for has nothing left to write.
		s.mu.Unlock()
		return nil
	}
	covers := s.changes
	// The snapshot carries every renewal made so far.
	s.renewalsUnwritten = false
	if s.memoryOnlyLocked() {
		s.written = covers
		s.mu.Unlock()
		return nil
	}
	file := s.snapshotLocked()
	s.mu.Unlock()

	if err := s.writeSnapshot(file); err != nil {
		return err
	}
	s.mu.Lock()
	s.markWrittenLocked(covers)
	s.mu.Unlock()
	return nil
}

// memoryOnlyLocked reports whether the store has nowhere to write. Caller
// holds mu.
func (s *secureSessionStore) memoryOnlyLocked() bool {
	return s.dir == "" && s.writeFile == nil
}

// snapshotLocked is the store as the file holds it. Caller holds mu.
func (s *secureSessionStore) snapshotLocked() secureSessionFile {
	file := secureSessionFile{
		Format:     secureSessionStoreFormat,
		Identities: make(map[domain.PeerIdentity]time.Time, len(s.identities)),
		Endpoints:  make(map[domain.PeerAddress]endpointBindingRecord, len(s.endpoints)+len(s.nonCanonical)),
	}
	for id, at := range s.identities {
		file.Identities[id] = at
	}
	for endpoint, binding := range s.endpoints {
		file.Endpoints[endpoint.Address()] = recordOf(endpoint, binding)
	}
	// A canonical address always reads back as itself and these never do,
	// so the two sets of keys cannot collide.
	for stored, binding := range s.nonCanonical {
		file.Endpoints[stored] = recordWithoutTransport(binding)
	}
	return file
}

// writeSnapshot encodes file and puts it on disk, logging a failure. Caller
// holds writeMu and not mu.
func (s *secureSessionStore) writeSnapshot(file secureSessionFile) error {
	raw, err := json.Marshal(file)
	if err == nil {
		err = s.writeFile(raw)
	}
	if err != nil {
		s.logger.Error().Err(err).Str("store", s.name).Msg(writeFailureMessage(err))
		return fmt.Errorf("secure session store: %w", err)
	}
	return nil
}

// markWrittenLocked records that the changes up to covers are on disk.
// Caller holds mu.
func (s *secureSessionStore) markWrittenLocked(covers uint64) {
	if covers > s.written {
		s.written = covers
	}
}

// writeFailureMessage tells a directory this process may not write into —
// a configuration to fix, typically a root-owned data directory — from any
// other failed write.
func writeFailureMessage(err error) string {
	if errors.Is(err, secretfile.ErrDirectoryNotWritable) {
		return "pin_store_directory_not_writable"
	}
	return "secure_session_store_write_failed"
}

// writeToDisk writes raw through secretfile; a memory-only store has nowhere
// to write and nothing to lose.
func (s *secureSessionStore) writeToDisk(raw []byte) error {
	if s.dir == "" {
		return nil
	}
	dir, err := secretfile.Open(s.dir)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Write(s.name, raw)
}

// renewalFlushMoment says whether a flush of the secure-session store's
// renewals waits for renewalFlushEvery.
type renewalFlushMoment int

const (
	flushRenewalsWhenDue renewalFlushMoment = iota + 1
	flushRenewalsAtStop
)

// flushSecureSessionRenewals removes the store's expired ordinary bindings
// and writes them with its batched renewals: on the bootstrap tick once
// renewalFlushEvery has passed, and whatever is unwritten when the loop
// stops. Disk I/O, so the caller holds no domain mutex.
func (s *Service) flushSecureSessionRenewals(moment renewalFlushMoment) {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return
	}
	flush := s.secureSessions.store.flushRenewals
	if moment == flushRenewalsAtStop {
		flush = s.secureSessions.store.flushRenewalsNow
	}
	if err := flush(); err != nil {
		// persist has logged the failure at Error with the store's name;
		// this line only says what follows from it.
		log.Debug().Err(err).Str("store", s.secureSessions.store.name).Str("operation", "renewal_flush").
			Msg("secure_session_renewal_flush_failed_retried_on_a_later_tick")
	}
}

// SecureSessionStoreStats is the v2 downgrade-protection store as diagnostics
// see it; a node without v2 state reports the zero value.
func (s *Service) SecureSessionStoreStats() domain.SecureSessionStoreStats {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return domain.SecureSessionStoreStats{}
	}
	return s.secureSessions.store.stats()
}

package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
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
//     address with an unpinned identity. A binding to one of this node's
//     CONTACTS is protected and never expires — nobody but the user makes an
//     identity a contact, so an attacker cannot buy one; any other binding
//     lasts 30 days from the last v2 session on it (owner's decision С-15).
//     A protected binding is never lowered or removed automatically (С-16).
//
// The store never drops live protection to make room: at its bound a new
// binding is refused and logged, and what is already there stays. A NEW
// identity at the pin bound fails its session with errPinStoreFull: a v2
// session whose mandatory pin is not stored is not established, and the
// dialler does not fall back to v1 in that attempt.
//
// A change that could not be written is reported to the caller — a session
// whose protection is not on disk is not established — and stays pending:
// the next note writes it even when it changes nothing itself.
//
// Persistence is one JSON file written through secretfile (unique temp,
// owner-only at creation, fsync, rename), next to the node's peers file.

const (
	secureSessionStoreFormat = 1
	// maxPinnedIdentities and maxEndpointBindings bound the store. Every v2
	// handshake can pin, and a fresh identity costs an attacker nothing, so
	// the bound is a refusal of NEW pins, never an eviction of old ones.
	maxPinnedIdentities = 20000
	maxEndpointBindings = 16384
	// maxProtectedBindings bounds the bindings that never expire (С-17). A
	// protected binding refused at the bound becomes an ordinary one.
	maxProtectedBindings = 32768
	// endpointBindingTTL is the owner's 30 days from the last v2 session.
	endpointBindingTTL = 30 * 24 * time.Hour
	// endpointRefreshPersistEvery bounds disk writes for a binding that is
	// merely renewed: an in-memory renewal is always made, a written one at
	// most this often. A crash loses at most this much of the 30 days.
	endpointRefreshPersistEvery = time.Hour
)

// errLegacyRefusedPinned is a v1 session naming an identity that proved v2.
var errLegacyRefusedPinned = errors.New("secure session: v1 refused for an identity that proved v2")

// errPinStoreFull is a v2 session of an identity not pinned yet, refused
// because the store is at its bound and the mandatory pin cannot be stored.
// Its text is the diagnostic code operators search for.
var errPinStoreFull = errors.New("pin_store_full")

type endpointBinding struct {
	Identity  domain.PeerIdentity `json:"identity"`
	LastV2    time.Time           `json:"last_v2"`
	Protected bool                `json:"protected,omitempty"`
	persisted time.Time
}

// live reports whether the binding still requires v2 at now.
func (b endpointBinding) live(now time.Time) bool {
	return b.Protected || now.Before(b.LastV2.Add(endpointBindingTTL))
}

// secureSessionStore is the downgrade-protection state. mu guards the maps
// and is a leaf: no I/O and no call out under it. writeMu serialises disk
// writes and is held WITHOUT mu, so a slow disk never blocks a lookup.
type secureSessionStore struct {
	dir   string // "" — memory only (a Service built without a peers path)
	name  string
	clock func() time.Time

	mu         sync.Mutex
	identities map[domain.PeerIdentity]time.Time
	endpoints  map[domain.PeerAddress]endpointBinding
	// unreadable: the file exists but could not be read. Fail-closed:
	// every identity and every endpoint is treated as protected, so v1 is
	// refused while v2 keeps working, until an operator repairs the file.
	unreadable bool
	// changes counts every change, written the count the last successful
	// write covered: a gap is protection that is in memory only.
	changes, written uint64
	// pinRefusals counts sessions refused with errPinStoreFull.
	pinRefusals uint64

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
func loadSecureSessionStore(path string, clock func() time.Time) *secureSessionStore {
	store := &secureSessionStore{
		clock:      clock,
		identities: make(map[domain.PeerIdentity]time.Time),
		endpoints:  make(map[domain.PeerAddress]endpointBinding),
	}
	if path == "" {
		// Memory only: nothing to write, so no snapshot is ever taken.
		return store
	}
	store.writeFile = store.writeToDisk
	store.dir, store.name = filepath.Dir(path), filepath.Base(path)
	if err := store.load(); err != nil {
		store.unreadable = true
		log.Error().Err(err).Str("store", store.name).Msg("secure_session_store_unreadable_v1_refused_for_every_peer")
	}
	return store
}

type secureSessionFile struct {
	Format     int                                    `json:"format"`
	Identities map[domain.PeerIdentity]time.Time      `json:"identities"`
	Endpoints  map[domain.PeerAddress]endpointBinding `json:"endpoints"`
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
	now := s.clock()
	for id, at := range file.Identities {
		if id.IsZero() {
			return errors.New("decode: a zero identity")
		}
		s.identities[id] = at
	}
	for address, binding := range file.Endpoints {
		if binding.Identity.IsZero() || strings.TrimSpace(string(address)) == "" {
			return errors.New("decode: an incomplete endpoint binding")
		}
		// A last_v2 in the future (clock set back since) is clamped to now,
		// or a clock change would stretch the binding past its 30 days.
		if binding.LastV2.After(now) {
			binding.LastV2 = now
		}
		binding.persisted = binding.LastV2
		s.endpoints[address] = binding
	}
	return nil
}

// stats is the store's state for diagnostics. Takes mu, a leaf held for
// map reads only.
func (s *secureSessionStore) stats() domain.SecureSessionStoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.SecureSessionStoreStats{
		ReadAt:               s.clock(),
		PinnedIdentities:     len(s.identities),
		PinCapacity:          maxPinnedIdentities,
		Full:                 len(s.identities) >= maxPinnedIdentities,
		PinRefusalsStoreFull: s.pinRefusals,
		Unreadable:           s.unreadable,
	}
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
func (s *secureSessionStore) endpointRequiresV2(address domain.PeerAddress) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable {
		return true
	}
	binding, bound := s.endpoints[address]
	return bound && binding.live(s.clock())
}

// noteProvenInbound pins a peer proven on an accepted connection. An error
// means the protection is not stored — errPinStoreFull for a new identity at
// the bound, or a write failure — and the caller must not establish the
// session on it.
func (s *secureSessionStore) noteProvenInbound(id domain.PeerIdentity) error {
	var refused error
	err := s.update(func(now time.Time) bool {
		pinned, pinErr := s.pinLocked(id, now)
		refused = pinErr
		return pinned
	})
	if refused != nil {
		return refused
	}
	return err
}

// noteProvenOutbound pins the peer and binds the endpoint this node dialled;
// protected marks a binding to one of this node's contacts. An error means
// the protection is not stored and the session must not be established. A
// session refused with errPinStoreFull leaves no endpoint binding either: it
// was not established, so nothing it earned is recorded.
func (s *secureSessionStore) noteProvenOutbound(address domain.PeerAddress, id domain.PeerIdentity, protected bool) error {
	var refused error
	err := s.update(func(now time.Time) bool {
		pinned, pinErr := s.pinLocked(id, now)
		if pinErr != nil {
			refused = pinErr
			return false
		}
		bound := s.bindLocked(address, id, protected, now)
		return pinned || bound
	})
	if refused != nil {
		return refused
	}
	return err
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
	if change(s.clock()) {
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
// with errPinStoreFull — never by evicting an existing pin (owner's decision
// on С-5: existing protection is guaranteed, new pins are not promised).
// Caller holds mu.
func (s *secureSessionStore) pinLocked(id domain.PeerIdentity, now time.Time) (bool, error) {
	if _, pinned := s.identities[id]; pinned {
		return false, nil
	}
	if len(s.identities) >= maxPinnedIdentities {
		s.pinRefusals++
		log.Warn().
			Str("peer", id.String()).
			Str("reason", errPinStoreFull.Error()).
			Int("pinned", len(s.identities)).
			Msg("secure_session_refused_pin_store_full")
		return false, errPinStoreFull
	}
	s.identities[id] = now
	return true, nil
}

// bindLocked binds address to id, renews a live binding, and reports whether
// the change must reach the disk now. A binding is raised to protected when
// asked and room allows; a protected one is never lowered. Caller holds mu.
func (s *secureSessionStore) bindLocked(address domain.PeerAddress, id domain.PeerIdentity, protected bool, now time.Time) bool {
	binding, bound := s.endpoints[address]
	if bound && binding.live(now) {
		changedOwner := binding.Identity != id
		raised := protected && !binding.Protected && s.protectedRoomLocked(address)
		binding.Identity, binding.LastV2 = id, now
		binding.Protected = binding.Protected || raised
		due := changedOwner || raised || now.Sub(binding.persisted) >= endpointRefreshPersistEvery
		if due {
			binding.persisted = now
		}
		s.endpoints[address] = binding
		return due
	}
	if !bound && len(s.endpoints) >= maxEndpointBindings {
		// Only a full store is scanned: a scan per new binding would be
		// quadratic work under mu.
		s.pruneExpiredLocked(now)
		if len(s.endpoints) >= maxEndpointBindings {
			log.Warn().Str("peer", string(address)).Msg("secure_session_endpoint_binding_refused_store_full")
			return false
		}
	}
	s.endpoints[address] = endpointBinding{
		Identity:  id,
		LastV2:    now,
		Protected: protected && s.protectedRoomLocked(address),
		persisted: now,
	}
	return true
}

// protectedRoomLocked reports whether one more protected binding fits. At
// the bound the binding stays ordinary — refused protection, logged — and
// no protected binding is ever dropped for room. Caller holds mu.
func (s *secureSessionStore) protectedRoomLocked(address domain.PeerAddress) bool {
	protected := 0
	for _, binding := range s.endpoints {
		if binding.Protected {
			protected++
		}
	}
	if protected < maxProtectedBindings {
		return true
	}
	log.Warn().Str("peer", string(address)).Msg("secure_session_protected_binding_refused_limit")
	return false
}

// pruneExpiredLocked drops ordinary bindings whose 30 days ran out — the
// owner's one exception to "protection is never dropped". Protected ones are
// never pruned. Caller holds mu.
func (s *secureSessionStore) pruneExpiredLocked(now time.Time) {
	for address, binding := range s.endpoints {
		if !binding.live(now) {
			delete(s.endpoints, address)
		}
	}
}

// persist writes a snapshot of the store and reports a failure. The snapshot
// is taken under mu, the write happens after it is released; on success the
// change count the snapshot covered is recorded as written.
func (s *secureSessionStore) persist() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	covers := s.changes
	if s.dir == "" && s.writeFile == nil {
		s.written = covers
		s.mu.Unlock()
		return nil
	}
	file := secureSessionFile{
		Format:     secureSessionStoreFormat,
		Identities: make(map[domain.PeerIdentity]time.Time, len(s.identities)),
		Endpoints:  make(map[domain.PeerAddress]endpointBinding, len(s.endpoints)),
	}
	for id, at := range s.identities {
		file.Identities[id] = at
	}
	for address, binding := range s.endpoints {
		file.Endpoints[address] = binding
	}
	s.mu.Unlock()

	raw, err := json.Marshal(file)
	if err == nil {
		err = s.writeFile(raw)
	}
	if err != nil {
		log.Error().Err(err).Str("store", s.name).Msg("secure_session_store_write_failed")
		return fmt.Errorf("secure session store: %w", err)
	}
	s.mu.Lock()
	if covers > s.written {
		s.written = covers
	}
	s.mu.Unlock()
	return nil
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

// SecureSessionStoreStats is the v2 downgrade-protection store as diagnostics
// see it; a node without v2 state reports the zero value.
func (s *Service) SecureSessionStoreStats() domain.SecureSessionStoreStats {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return domain.SecureSessionStoreStats{}
	}
	return s.secureSessions.store.stats()
}

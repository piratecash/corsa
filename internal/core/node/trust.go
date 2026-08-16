package node

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

var errTrustConflict = errors.New("trusted contact conflict")

// trustContactSourceRecord marks contact key material that came from a
// verified signed identity record rather than from the TOFU epidemic.
const trustContactSourceRecord = "identity_record"

// trustFileVersion is the schema version this build writes. Version 2 added
// the signed identity-record rows; a file without the field is the legacy
// contacts-only layout and is migrated in place on the first save.
const trustFileVersion = 2

type trustedContact struct {
	Address      string    `json:"address"`
	PubKey       string    `json:"pub_key"`
	BoxKey       string    `json:"box_key"`
	BoxSignature string    `json:"box_signature"`
	FirstSeenAt  time.Time `json:"first_seen_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	Source       string    `json:"source"`
}

// trustRecordRow is the on-disk form of one verified signed identity record
// (docs/protocol/identity-lookup.md): the {v, body, sig} triple verbatim
// plus the storage key halves. Seq is NOT denormalised — it lives inside
// the signed body, and one source of truth beats a cached copy that can
// drift.
type trustRecordRow struct {
	Network  string    `json:"network"`
	Address  string    `json:"address"`
	Body     string    `json:"body"`
	Sig      string    `json:"sig"`
	StoredAt time.Time `json:"stored_at"`
	V        int       `json:"v"`
}

type trustFile struct {
	Contacts  map[string]trustedContact `json:"contacts"`
	Conflicts map[string]string         `json:"conflicts,omitempty"`
	Records   []trustRecordRow          `json:"records,omitempty"`
	Version   int                       `json:"version"`
}

// trustRecordKey is the composite storage key of a record. The address
// alone would let a record from another network occupy the slot — the
// signature binds a record to one network, so the store key must too.
type trustRecordKey struct {
	network string
	address string
}

// trustedIdentityRecord is one stored record in memory: the signed triple
// plus its parsed body, so read paths never re-parse.
type trustedIdentityRecord struct {
	storedAt time.Time
	record   protocol.SignedIdentityRecord
	body     protocol.IdentityRecordBody
}

type trustStore struct {
	path      string
	mu        sync.RWMutex
	contacts  map[string]trustedContact
	conflicts map[string]string
	records   map[trustRecordKey]trustedIdentityRecord
	// snapshotGen numbers snapshots in mutation order (owned by mu, taken
	// with write intent). Disk writes happen outside mu, so two mutators
	// can reach saveSnapshot in either order — the generation is what
	// keeps an older snapshot from overwriting a newer one on disk.
	snapshotGen uint64

	// saveMu serializes the write-temp-then-rename sequence (they share
	// one .tmp path) and owns savedGen — the generation that last reached
	// disk. Never taken with mu held; holding it during disk I/O is the
	// point.
	saveMu   sync.Mutex
	savedGen uint64
}

func loadTrustStore(path string, self trustedContact) (*trustStore, error) {
	store := &trustStore{
		path:      path,
		contacts:  map[string]trustedContact{},
		conflicts: map[string]string{},
		records:   map[trustRecordKey]trustedIdentityRecord{},
	}

	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			var payload trustFile
			if err := json.Unmarshal(data, &payload); err != nil {
				return nil, fmt.Errorf("decode trust store %s: %w", path, err)
			}
			if payload.Contacts != nil {
				store.contacts = payload.Contacts
			}
			if payload.Conflicts != nil {
				store.conflicts = payload.Conflicts
			}
			// A legacy (pre-version) file simply has no record rows; the
			// first save rewrites it as trustFileVersion. Individual rows
			// that fail to parse are skipped, not fatal: one torn row must
			// not take the whole contact store (and the node) down.
			for _, row := range payload.Records {
				key, restored, err := restoreTrustRecordRow(row)
				if err != nil {
					log.Warn().Err(err).
						Str("record_address", row.Address).
						Str("record_network", row.Network).
						Msg("trust_store_record_row_skipped")
					continue
				}
				store.records[key] = restored
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read trust store %s: %w", path, err)
		}
	}

	if self.Address != "" {
		now := time.Now().UTC()
		if existing, ok := store.contacts[self.Address]; ok {
			// The caller-supplied self contact is canonical for OUR OWN
			// key material — it is derived from the identity file plus the
			// runtime DM-acceptance policy (a relay-only node passes an
			// empty BoxKey/BoxSignature so the box key is not republished
			// via fetch_contacts). Refreshing the row instead of keeping
			// the persisted one prevents a stale box key from surviving a
			// policy flip across restarts. FirstSeenAt is preserved as
			// history.
			self.FirstSeenAt = existing.FirstSeenAt
			self.LastSeenAt = now
			store.contacts[self.Address] = self
		} else {
			self.FirstSeenAt = now
			self.LastSeenAt = now
			store.contacts[self.Address] = self
		}
	}

	if err := store.save(); err != nil {
		return nil, err
	}

	return store, nil
}

// remember adds or refreshes a contact. stored reports whether the
// contact is present in the LIVE store on return — true even when only
// the disk persist failed, because the in-memory write has already been
// applied by then. Callers keying side effects to live trust state (the
// known-set pin in trustContact) must act on stored, not err: err alone
// cannot distinguish "conflict-path save failed" (not stored) from
// "stored but save failed".
func (s *trustStore) remember(contact trustedContact) (stored bool, err error) {
	now := time.Now().UTC()

	s.mu.Lock()
	if existing, ok := s.contacts[contact.Address]; ok {
		if existing.PubKey != contact.PubKey || existing.BoxKey != contact.BoxKey || existing.BoxSignature != contact.BoxSignature {
			s.conflicts[contact.Address] = fmt.Sprintf("pinned contact mismatch from %s at %s", contact.Source, now.Format(time.RFC3339))
			snapshot := s.snapshotLocked()
			s.mu.Unlock()
			if err := s.saveSnapshot(snapshot); err != nil {
				return false, err
			}
			return false, errTrustConflict
		}

		existing.LastSeenAt = now
		existing.Source = contact.Source
		s.contacts[contact.Address] = existing
		snapshot := s.snapshotLocked()
		s.mu.Unlock()
		return true, s.saveSnapshot(snapshot)
	}

	contact.FirstSeenAt = now
	contact.LastSeenAt = now
	s.contacts[contact.Address] = contact
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return true, s.saveSnapshot(snapshot)
}

// forget removes a contact from the trust store and persists the change.
// removed reports whether the contact was deleted from the LIVE store —
// true even when the subsequent disk persist failed, because the
// in-memory delete has already been applied by then. Callers keying side
// effects to live trust state (the known-set unpin in
// deleteTrustedContactFrame) must act on removed even when err is
// non-nil.
func (s *trustStore) forget(identity domain.PeerIdentity) (removed bool, err error) {
	address := identity.String()

	s.mu.Lock()
	if _, ok := s.contacts[address]; !ok {
		s.mu.Unlock()
		return false, nil
	}
	delete(s.contacts, address)
	delete(s.conflicts, address)
	// The stored record follows the contact out: it exists because there was
	// a dialogue, and keeping the keys of a deleted contact on disk would
	// contradict the deletion.
	for key := range s.records {
		if key.address == address {
			delete(s.records, key)
		}
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()

	if err := s.saveSnapshot(snapshot); err != nil {
		return true, fmt.Errorf("persist trust store after forget %s: %w", address, err)
	}
	return true, nil
}

// restoreTrustRecordRow rebuilds one stored record from its disk row:
// decode the triple, re-parse the body (the seq and keys live there), and
// re-derive the storage key from the SIGNED address rather than trusting
// the row's copy.
func restoreTrustRecordRow(row trustRecordRow) (trustRecordKey, trustedIdentityRecord, error) {
	body, err := base64.RawURLEncoding.DecodeString(row.Body)
	if err != nil {
		return trustRecordKey{}, trustedIdentityRecord{}, fmt.Errorf("decode record body: %w", err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(row.Sig)
	if err != nil {
		return trustRecordKey{}, trustedIdentityRecord{}, fmt.Errorf("decode record sig: %w", err)
	}
	record := protocol.SignedIdentityRecord{Version: row.V, Body: body, Sig: sig}
	parsed, err := protocol.ParseIdentityRecordBody(body)
	if err != nil {
		return trustRecordKey{}, trustedIdentityRecord{}, err
	}
	if parsed.Address.String() != row.Address {
		return trustRecordKey{}, trustedIdentityRecord{}, fmt.Errorf(
			"record row address %s does not match signed body address %s", row.Address, parsed.Address)
	}
	// Full cryptographic re-verification, not just structure: a corrupted
	// or tampered signature must not survive a restart — a self-record row
	// is re-published via push_identity, and a store is not a proof.
	if _, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(row.Network), parsed.Address); err != nil {
		return trustRecordKey{}, trustedIdentityRecord{}, fmt.Errorf("record row failed verification: %w", err)
	}
	key := trustRecordKey{network: row.Network, address: row.Address}
	return key, trustedIdentityRecord{record: record, body: parsed, storedAt: row.StoredAt}, nil
}

// recordBodies returns the parsed bodies of every stored record of one
// network — the startup reseed source for the knowledge maps: verified key
// material that survived the restart must be usable without a fresh
// lookup.
func (s *trustStore) recordBodies(network string) []protocol.IdentityRecordBody {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]protocol.IdentityRecordBody, 0, len(s.records))
	for key, stored := range s.records {
		if key.network == network {
			out = append(out, stored.body)
		}
	}
	return out
}

// recordFor returns the stored signed record of one identity on one
// network. The bool is false when the store holds none.
func (s *trustStore) recordFor(network domain.NetworkID, identity domain.PeerIdentity) (protocol.SignedIdentityRecord, protocol.IdentityRecordBody, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stored, ok := s.records[trustRecordKey{network: network.String(), address: identity.String()}]
	if !ok {
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, false
	}
	return stored.record, stored.body, true
}

// rememberRecord merges an ALREADY VERIFIED signed record into the store
// under the seq-merge contract and persists on acceptance.
//
// Acceptance is also the seq-gated replacement path of the trust store: a
// verified record with a higher seq is the owner's own word about their
// keys, so it MAY change a pinned contact's key material — the one thing
// the TOFU remember() path refuses — and it clears the address's conflict
// marker while doing so. A keyless (dm=false) record empties the contact's
// box fields: the owner has withdrawn the key, keeping a copy would keep
// encrypting to a tombstone.
//
// Duplicate and stale are silent no-ops. A conflict (same seq, different
// bytes) keeps the stored record; the caller logs it — the owner is obliged
// to issue a new seq.
func (s *trustStore) rememberRecord(network domain.NetworkID, record protocol.SignedIdentityRecord, body protocol.IdentityRecordBody) (domain.IdentityRecordMergeOutcome, error) {
	now := time.Now().UTC()
	key := trustRecordKey{network: network.String(), address: body.Address.String()}

	s.mu.Lock()
	stored := domain.AbsentIdentityRecord()
	if existing, ok := s.records[key]; ok {
		stored = domain.ExistingIdentityRecord(existing.body.Seq, existing.record.Body)
	}
	outcome := domain.DecideIdentityRecordMerge(stored, body.Seq, record.Body)
	if !outcome.Accepted() {
		s.mu.Unlock()
		return outcome, nil
	}

	s.records[key] = trustedIdentityRecord{record: record, body: body, storedAt: now}
	if contact, ok := s.contacts[key.address]; ok {
		contact.PubKey = string(body.PubKey)
		contact.BoxKey = string(body.BoxKey)
		contact.BoxSignature = string(body.BoxSig)
		contact.Source = trustContactSourceRecord
		contact.LastSeenAt = now
		s.contacts[key.address] = contact
		delete(s.conflicts, key.address)
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()

	if err := s.saveSnapshot(snapshot); err != nil {
		return outcome, fmt.Errorf("persist trust store after record merge %s: %w", key.address, err)
	}
	return outcome, nil
}

func (s *trustStore) trustedContacts() map[string]trustedContact {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]trustedContact, len(s.contacts))
	for address, contact := range s.contacts {
		out[address] = contact
	}
	return out
}

func (s *trustStore) save() error {
	// Write lock: snapshotLocked advances the generation counter.
	s.mu.Lock()
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return s.saveSnapshot(snapshot)
}

// trustSnapshot is one consistent copy of the store state, taken under mu
// and persisted after it is released — disk I/O never runs under mu.
type trustSnapshot struct {
	contacts  map[string]trustedContact
	conflicts map[string]string
	records   []trustRecordRow
	gen       uint64
}

// snapshotLocked stamps and copies the state. Caller holds mu with WRITE
// intent — the generation counter advances here.
func (s *trustStore) snapshotLocked() trustSnapshot {
	s.snapshotGen++
	contacts := make(map[string]trustedContact, len(s.contacts))
	for address, contact := range s.contacts {
		contacts[address] = contact
	}

	conflicts := make(map[string]string, len(s.conflicts))
	for address, conflict := range s.conflicts {
		conflicts[address] = conflict
	}

	records := make([]trustRecordRow, 0, len(s.records))
	for key, stored := range s.records {
		records = append(records, trustRecordRow{
			Network:  key.network,
			Address:  key.address,
			V:        stored.record.Version,
			Body:     base64.RawURLEncoding.EncodeToString(stored.record.Body),
			Sig:      base64.RawURLEncoding.EncodeToString(stored.record.Sig),
			StoredAt: stored.storedAt,
		})
	}

	return trustSnapshot{contacts: contacts, conflicts: conflicts, records: records, gen: s.snapshotGen}
}

// saveSnapshot persists one snapshot, dropping it when a NEWER one already
// reached disk: mutators release mu before writing, so two saves can race
// here in either order, and generation ordering — not scheduling luck — is
// what the on-disk file follows. saveMu also serializes the shared .tmp.
func (s *trustStore) saveSnapshot(snapshot trustSnapshot) error {
	if s.path == "" {
		return nil
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	if snapshot.gen <= s.savedGen {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create trust store directory: %w", err)
	}

	payload, err := json.MarshalIndent(trustFile{
		Version:   trustFileVersion,
		Contacts:  snapshot.contacts,
		Conflicts: snapshot.conflicts,
		Records:   snapshot.records,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal trust store: %w", err)
	}

	// Write-temp-then-rename: the trust store is loaded at startup and a
	// truncated file fails the whole node, so a crash or full disk mid-write
	// must leave the previous generation intact.
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o600); err != nil {
		return fmt.Errorf("write trust store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace trust store: %w", err)
	}

	s.savedGen = snapshot.gen
	return nil
}

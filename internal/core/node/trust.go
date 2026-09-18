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
// the signed identity-record rows; version 3 added the independently tracked
// last-online observation for trusted contacts. A file without either field is
// a legacy layout and is upgraded in place on the first save.
const trustFileVersion = 3

type trustedContact struct {
	Address      string    `json:"address"`
	PubKey       string    `json:"pub_key"`
	BoxKey       string    `json:"box_key"`
	BoxSignature string    `json:"box_signature"`
	FirstSeenAt  time.Time `json:"first_seen_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	// LastOnlineAt is what THIS node observed with its own clock: a final
	// route lost, or a DM the sender handed us over its own session. Chat
	// history is deliberately NOT persisted beside it: what the messages say
	// is recomputed from the chatlog at startup, which keeps one writer and
	// one source of truth instead of a second copy to keep coherent.
	LastOnlineAt time.Time `json:"last_online_at,omitzero"`
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
	// records holds the PERSISTENT records: the node's own and those of
	// trusted contacts (docs/protocol/identity-lookup.md §4, the owner and
	// the interlocutor). A record is persistent exactly when its address is
	// a contact at the time it is accepted or promoted, and it leaves with
	// the contact (forget). Everything else lives in sessionRecords.
	records map[trustRecordKey]trustedIdentityRecord
	// sessionRecords holds the records of everybody else — session peers
	// and lookup targets that are not contacts — in memory only and under
	// a budget. See trust_session_records.go for the contract.
	sessionRecords *sessionRecordCache
	// clock is the store's time source; tests pin it to drive the session
	// record TTL. Wall time otherwise.
	clock func() time.Time
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
		path:           path,
		contacts:       map[string]trustedContact{},
		conflicts:      map[string]string{},
		records:        map[trustRecordKey]trustedIdentityRecord{},
		sessionRecords: newSessionRecordCache(maxSessionIdentityRecords, maxSessionIdentityRecordBytes, sessionIdentityRecordTTL),
		clock:          func() time.Time { return time.Now().UTC() },
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
			//
			// Rows whose address is neither a contact nor this node are
			// the session-peer records an earlier build persisted; they
			// are not restored (a session peer re-pushes its record right
			// after auth, and a lookup recovers any other), and the save
			// at the end of this load rewrites the file without them. The
			// contacts themselves are untouched by this migration.
			migrated := 0
			for _, row := range payload.Records {
				if _, contact := store.contacts[row.Address]; !contact && row.Address != self.Address {
					migrated++
					continue
				}
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
			if migrated > 0 {
				log.Info().Int("records", migrated).Msg("trust_store_session_records_not_restored")
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read trust store %s: %w", path, err)
		}
	}

	if self.Address != "" {
		now := store.clock()
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
			self.LastOnlineAt = existing.LastOnlineAt
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

// recordLastOnlineAt stores a locally observed presence moment for trusted
// identities. It deliberately does not create contacts: route gossip can
// mention identities the user has never trusted, while this JSON is the
// durable trusted-contact store. One batch produces one atomic snapshot
// write, and an older observation can never move the timestamp back.
//
// minAdvance is how much newer the observation has to be than the stored one
// to be worth a write. Zero means any advance counts, which is what the
// online→offline transition wants. The DM path passes a real interval and
// relies on the check happening HERE, under the same lock as the update: a
// caller that compares first and writes after leaves a window in which every
// message of a burst reads the same stale stamp and each buys its own rewrite
// of the whole trust file.
func (s *trustStore) recordLastOnlineAt(identities []domain.PeerIdentity, at time.Time, minAdvance time.Duration) (updated int, err error) {
	if len(identities) == 0 || at.IsZero() {
		return 0, nil
	}
	observedAt := at.UTC()

	s.mu.Lock()
	for _, identity := range identities {
		if identity.IsZero() {
			continue
		}
		address := identity.String()
		contact, ok := s.contacts[address]
		if !ok {
			continue
		}
		if !contact.LastOnlineAt.IsZero() && observedAt.Sub(contact.LastOnlineAt) < minAdvance {
			continue
		}
		if !contact.LastOnlineAt.IsZero() && !observedAt.After(contact.LastOnlineAt) {
			continue
		}
		contact.LastOnlineAt = observedAt
		s.contacts[address] = contact
		updated++
	}
	if updated == 0 {
		s.mu.Unlock()
		return 0, nil
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()

	if err := s.saveSnapshot(snapshot); err != nil {
		return updated, fmt.Errorf("persist trust-store last-online: %w", err)
	}
	return updated, nil
}

// remember adds or refreshes a contact. stored reports whether the
// contact is present in the LIVE store on return — true even when only
// the disk persist failed, because the in-memory write has already been
// applied by then. Callers keying side effects to live trust state (the
// known-set pin in trustContact) must act on stored, not err: err alone
// cannot distinguish "conflict-path save failed" (not stored) from
// "stored but save failed".
func (s *trustStore) remember(contact trustedContact) (stored bool, err error) {
	now := s.clock()

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
	s.promoteSessionRecordsLocked(contact.Address)
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return true, s.saveSnapshot(snapshot)
}

// promoteSessionRecordsLocked moves every cached record of an address that
// just became a contact into the persistent set: the interlocutor's record
// belongs on disk, and the seq gate it carries must not depend on the
// cache budget from now on. Caller holds mu (write).
func (s *trustStore) promoteSessionRecordsLocked(address string) {
	for key := range s.sessionRecords.entries {
		if key.address != address {
			continue
		}
		if stored, ok := s.sessionRecords.takeLocked(key); ok {
			s.records[key] = stored
		}
	}
}

// isPersistentRecordAddressLocked reports whether a record for address
// belongs on disk — the address is a contact (the node's own contact row
// included). Caller holds mu.
func (s *trustStore) isPersistentRecordAddressLocked(address string) bool {
	_, ok := s.contacts[address]
	return ok
}

// sweepSessionRecords drops session records past their TTL. Called on the
// maintenance cadence so the cache shrinks while the node is quiet, not
// only when the next record arrives. Records of identities the node is
// still talking to or asking about are stepped over — see
// setRecordProtection.
func (s *trustStore) sweepSessionRecords(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionRecords.sweepLocked(now)
}

// setRecordProtection installs the live set the session-record cache
// evicts through. Wired once, at construction: the cache does not consult
// it and then delete, it deletes INSIDE it, so a pin taken while an
// eviction is choosing cannot be overtaken by that eviction.
func (s *trustStore) setRecordProtection(protection *recordProtection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionRecords.protection = protection
}

// sessionRecordUsage reports the cache's live count and bytes, how much of
// it the live set is holding, and its eviction history, for the resource
// breakdown.
func (s *trustStore) sessionRecordUsage() sessionRecordStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats := s.sessionRecords.stats
	stats.protected = s.sessionRecords.protectedCountLocked()
	return stats
}

// persistentUsage reports the sizes of the persistent maps.
func (s *trustStore) persistentUsage() (contacts, conflicts, records int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.contacts), len(s.conflicts), len(s.records)
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
		// Not a contact — nothing on disk to forget, but "forget X" still
		// means the cached session record goes: it is the one record the
		// user can name, and the cache would otherwise keep it until the
		// budget or the TTL got there. removed stays false: no contact
		// was deleted, and that is what the caller's unpin keys on.
		s.sessionRecords.deleteAddressLocked(address)
		s.mu.Unlock()
		return false, nil
	}
	delete(s.contacts, address)
	delete(s.conflicts, address)
	// The stored record follows the contact out: it exists because there was
	// a dialogue, and keeping the keys of a deleted contact on disk would
	// contradict the deletion. The cache can hold nothing for a contact,
	// but a record accepted between the two map writes of a racing
	// promotion would; clearing it costs one walk of a bounded map.
	for key := range s.records {
		if key.address == address {
			delete(s.records, key)
		}
	}
	s.sessionRecords.deleteAddressLocked(address)
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
	stored, ok := s.recordLocked(trustRecordKey{network: network.String(), address: identity.String()})
	if !ok {
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, false
	}
	return stored.record, stored.body, true
}

// recordLocked finds a record in whichever set holds it. A key is in at
// most one: acceptance routes it by contact membership, promotion moves it
// from the cache to the persistent set, and forget clears both.
func (s *trustStore) recordLocked(key trustRecordKey) (trustedIdentityRecord, bool) {
	if stored, ok := s.records[key]; ok {
		return stored, true
	}
	return s.sessionRecords.getLocked(key)
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
//
// Where an accepted record lands depends on whose it is: a contact's (or
// the node's own) goes to the persistent set and to disk; anybody else's
// goes to the bounded session cache and never touches the disk — so a
// stream of pushes from ever-new session peers costs bounded memory and no
// I/O, instead of one full rewrite of the trust file per peer.
func (s *trustStore) rememberRecord(network domain.NetworkID, record protocol.SignedIdentityRecord, body protocol.IdentityRecordBody) (domain.IdentityRecordMergeOutcome, error) {
	return s.mergeRecord(network, record, body, false)
}

// rememberOwnRecord merges the node's OWN record. It is persistent by
// construction — the owner is the first of the three holders in
// identity-lookup.md §4 — and does not depend on the self contact row
// being present, which a store loaded without a self contact (tests, a
// relay-only profile) would otherwise lack.
func (s *trustStore) rememberOwnRecord(network domain.NetworkID, record protocol.SignedIdentityRecord, body protocol.IdentityRecordBody) (domain.IdentityRecordMergeOutcome, error) {
	return s.mergeRecord(network, record, body, true)
}

// mergeRecord is the shared merge: the seq gate against whichever set holds
// the current record, then placement. persistent forces the disk set;
// otherwise contact membership at this moment decides.
func (s *trustStore) mergeRecord(network domain.NetworkID, record protocol.SignedIdentityRecord, body protocol.IdentityRecordBody, persistent bool) (domain.IdentityRecordMergeOutcome, error) {
	now := s.clock()
	key := trustRecordKey{network: network.String(), address: body.Address.String()}

	s.mu.Lock()
	stored := domain.AbsentIdentityRecord()
	if existing, ok := s.recordLocked(key); ok {
		stored = domain.ExistingIdentityRecord(existing.body.Seq, existing.record.Body)
	}
	outcome := domain.DecideIdentityRecordMerge(stored, body.Seq, record.Body)
	if !outcome.Accepted() {
		s.mu.Unlock()
		return outcome, nil
	}

	accepted := trustedIdentityRecord{record: record, body: body, storedAt: now}
	if !persistent && !s.isPersistentRecordAddressLocked(key.address) {
		s.sessionRecords.putLocked(key, accepted)
		s.mu.Unlock()
		return outcome, nil
	}

	// A record that was cached before its address became persistent (a
	// forced own record, or a contact added after the record arrived and
	// promoted here rather than in remember) must not exist in both sets.
	s.sessionRecords.deleteLocked(key)
	s.records[key] = accepted
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

// isTrustedContact answers the membership question without copying the whole
// address book, which trustedContacts does and which the per-message paths
// would otherwise pay for on every arrival.
// contactBoxKey returns the X25519 public box key stored for a contact.
//
// The contact's OWN record is the authority for this, not the general
// knowledge cache: the cache holds keys for every identity this node has heard
// of, and the reciprocity gate is a question about contacts specifically. A
// contact with no stored box key answers false — nothing to verify against.
func (s *trustStore) contactBoxKey(identity domain.PeerIdentity) (string, bool) {
	if identity.IsZero() {
		return "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	contact, ok := s.contacts[identity.String()]
	if !ok || contact.BoxKey == "" {
		return "", false
	}
	return contact.BoxKey, true
}

func (s *trustStore) isTrustedContact(identity domain.PeerIdentity) bool {
	if identity.IsZero() {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.contacts[identity.String()]
	return ok
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

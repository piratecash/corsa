package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// identity_intent_store.go is the durable resolution-intent table of the
// identity lookup engine (docs/protocol/identity-lookup.md): the REASONS a
// background lookup keeps running. An open chat is UI state and a keyless
// message cannot be written to the chatlog, so after a restart this table
// is the ONLY seed for background resolutions.
//
// Semantics are a refcount over the reason set: removing one reason (the
// user closed the chat) does not cancel the operation while other reasons
// live. A resolution is cancelled only when its reason set becomes empty.

// identityIntentReasonType names why a resolution must keep running.
type identityIntentReasonType string

const (
	// identityIntentReasonUIChat — the user opened a chat with a keyless
	// identity.
	identityIntentReasonUIChat identityIntentReasonType = "ui_chat"
	// identityIntentReasonRecovery — a decrypt-recovery job needs fresh
	// keys; ReasonID points at the message.
	identityIntentReasonRecovery identityIntentReasonType = "recovery"
	// identityIntentReasonPendingSend — an outbound artifact waits for the
	// keys.
	identityIntentReasonPendingSend identityIntentReasonType = "pending_send"
	// identityIntentReasonPresence — a contact cannot be probed for liveness
	// until their record (and with it their box key) is known. Without the
	// key no sealed reciprocity claim can be built, and a probe without one
	// would be answered on the public path — so presence asks for the record
	// rather than sending a probe that bypasses its own gate.
	identityIntentReasonPresence identityIntentReasonType = "presence"
)

// identityIntentReason is one row's reason half; the pair (type, id) is
// the refcount key within a target.
type identityIntentReason struct {
	Type identityIntentReasonType `json:"reason_type"`
	// ID disambiguates same-typed reasons (a message id for recovery); may
	// be empty for singleton reasons like ui_chat.
	ID string `json:"reason_id"`
}

// identityIntentRow is one durable intent.
type identityIntentRow struct {
	CreatedAt time.Time            `json:"created_at"`
	Target    string               `json:"target"`
	Reason    identityIntentReason `json:"reason"`
	Attempts  int                  `json:"attempts"`
}

// identityIntentFile is the on-disk shape.
type identityIntentFile struct {
	Intents []identityIntentRow `json:"intents"`
	Version int                 `json:"version"`
}

const identityIntentFileVersion = 1

// identityIntentKey identifies one row.
type identityIntentKey struct {
	target domain.PeerIdentity
	reason identityIntentReason
}

// identityIntentStore is the mutex-owned durable table. Its own lock, no
// domain mutex involvement; disk writes happen outside the lock from a
// snapshot, mirroring the trust store.
type identityIntentStore struct {
	path string
	mu   sync.Mutex
	rows map[identityIntentKey]identityIntentRow
	// snapshotGen numbers snapshots in mutation order (owned by mu).
	snapshotGen uint64

	// saveMu serializes the shared-.tmp write-then-rename and owns
	// savedGen — the generation last persisted. Mutators write outside mu,
	// so saves can arrive out of order; the generation check keeps an
	// older snapshot from clobbering a newer file.
	saveMu   sync.Mutex
	savedGen uint64
}

// loadIdentityIntentStore reads the table; a missing file is an empty
// table, a torn file starts empty with a fresh rewrite on the next change
// (losing intents degrades to "the user re-opens the chat", which is
// recoverable — unlike a node refusing to start).
func loadIdentityIntentStore(path string) *identityIntentStore {
	store := &identityIntentStore{path: path, rows: map[identityIntentKey]identityIntentRow{}}
	if path == "" {
		return store
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return store
	}
	var payload identityIntentFile
	if err := json.Unmarshal(data, &payload); err != nil {
		return store
	}
	for _, row := range payload.Intents {
		target, err := domain.ParsePeerIdentity(row.Target)
		if err != nil || target.IsZero() {
			continue
		}
		store.rows[identityIntentKey{target: target, reason: row.Reason}] = row
	}
	return store
}

// add registers a reason for the target. Returns true when the reason is
// new (first registration wins; attempts survive re-adds).
func (s *identityIntentStore) add(target domain.PeerIdentity, reason identityIntentReason, now time.Time) (bool, error) {
	key := identityIntentKey{target: target, reason: reason}
	s.mu.Lock()
	if _, exists := s.rows[key]; exists {
		s.mu.Unlock()
		return false, nil
	}
	s.rows[key] = identityIntentRow{
		Target:    target.String(),
		Reason:    reason,
		CreatedAt: now.UTC(),
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return true, s.save(snapshot)
}

// remove drops one reason. remaining reports how many reasons the target
// still has — zero means the resolution lost its last reason and must be
// cancelled.
func (s *identityIntentStore) remove(target domain.PeerIdentity, reason identityIntentReason) (remaining int, err error) {
	key := identityIntentKey{target: target, reason: reason}
	s.mu.Lock()
	if _, exists := s.rows[key]; !exists {
		remaining = s.countLocked(target)
		s.mu.Unlock()
		return remaining, nil
	}
	delete(s.rows, key)
	remaining = s.countLocked(target)
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return remaining, s.save(snapshot)
}

// removeReasonType drops every reason of one type for the target (a
// recovery job closed or was evicted — its per-message reasons go
// together). remaining counts the target's surviving reasons of ANY type.
func (s *identityIntentStore) removeReasonType(target domain.PeerIdentity, reasonType identityIntentReasonType) (remaining int, err error) {
	s.mu.Lock()
	removed := false
	for key := range s.rows {
		if key.target == target && key.reason.Type == reasonType {
			delete(s.rows, key)
			removed = true
		}
	}
	remaining = s.countLocked(target)
	if !removed {
		s.mu.Unlock()
		return remaining, nil
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return remaining, s.save(snapshot)
}

// removeTarget drops every reason of a target (resolution terminal).
func (s *identityIntentStore) removeTarget(target domain.PeerIdentity) error {
	s.mu.Lock()
	removed := false
	for key := range s.rows {
		if key.target == target {
			delete(s.rows, key)
			removed = true
		}
	}
	if !removed {
		s.mu.Unlock()
		return nil
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return s.save(snapshot)
}

// recordAttempt bumps the attempt counter on every row of the target, so a
// restart resumes the background phase instead of restarting it.
func (s *identityIntentStore) recordAttempt(target domain.PeerIdentity) error {
	s.mu.Lock()
	touched := false
	for key, row := range s.rows {
		if key.target == target {
			row.Attempts++
			s.rows[key] = row
			touched = true
		}
	}
	if !touched {
		s.mu.Unlock()
		return nil
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return s.save(snapshot)
}

// identityIntentSeed is one target's reseed state after a restart.
type identityIntentSeed struct {
	Target    domain.PeerIdentity
	CreatedAt time.Time
	Attempts  int
	Reasons   int
}

// seeds returns per-target aggregates for reseeding the background phase:
// the oldest CreatedAt (the 7-day lifetime counts from the first reason)
// and the highest attempt count.
func (s *identityIntentStore) seeds() []identityIntentSeed {
	s.mu.Lock()
	defer s.mu.Unlock()
	byTarget := map[domain.PeerIdentity]*identityIntentSeed{}
	for key, row := range s.rows {
		seed, ok := byTarget[key.target]
		if !ok {
			seed = &identityIntentSeed{Target: key.target, CreatedAt: row.CreatedAt}
			byTarget[key.target] = seed
		}
		if row.CreatedAt.Before(seed.CreatedAt) {
			seed.CreatedAt = row.CreatedAt
		}
		if row.Attempts > seed.Attempts {
			seed.Attempts = row.Attempts
		}
		seed.Reasons++
	}
	out := make([]identityIntentSeed, 0, len(byTarget))
	for _, seed := range byTarget {
		out = append(out, *seed)
	}
	return out
}

func (s *identityIntentStore) countLocked(target domain.PeerIdentity) int {
	count := 0
	for key := range s.rows {
		if key.target == target {
			count++
		}
	}
	return count
}

// identityIntentSnapshot is one consistent, generation-stamped copy of the
// table, taken under mu and persisted after it is released.
type identityIntentSnapshot struct {
	rows []identityIntentRow
	gen  uint64
}

// snapshotLocked stamps and copies the rows. Caller holds mu.
func (s *identityIntentStore) snapshotLocked() identityIntentSnapshot {
	s.snapshotGen++
	rows := make([]identityIntentRow, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, row)
	}
	return identityIntentSnapshot{rows: rows, gen: s.snapshotGen}
}

// save persists one snapshot, dropping it when a newer generation already
// reached disk — mutators write outside mu, so saves can race here in
// either order (see the trust store's saveSnapshot for the same contract).
func (s *identityIntentStore) save(snapshot identityIntentSnapshot) error {
	if s.path == "" {
		return nil
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	if snapshot.gen <= s.savedGen {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create identity intents directory: %w", err)
	}
	payload, err := json.MarshalIndent(identityIntentFile{Version: identityIntentFileVersion, Intents: snapshot.rows}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal identity intents: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o600); err != nil {
		return fmt.Errorf("write identity intents: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace identity intents: %w", err)
	}
	s.savedGen = snapshot.gen
	return nil
}

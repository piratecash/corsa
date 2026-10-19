package node

import (
	"encoding/json"
	"fmt"
	"math"
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
// Semantics are a refcount over the reason set: explicitly removing one reason does not cancel the operation while other reasons
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
	action    identityLookupActionStamp
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

// mutate snapshots memory only. Resolver callers already hold resolver.mu;
// the only lock added here is intent.mu. Save the returned value after both.
func (s *identityIntentStore) mutate(change func() bool) (*identityIntentSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshotGen == math.MaxUint64 {
		return nil, fmt.Errorf("identity intents: generation exhausted")
	}
	if !change() {
		return nil, nil
	}
	snapshot := s.snapshotLocked()
	return &snapshot, nil
}

func (s *identityIntentStore) addMemory(target domain.PeerIdentity, reason identityIntentReason, now time.Time, action identityLookupActionStamp) (*identityIntentSnapshot, error) {
	return s.mutate(func() bool {
		key := identityIntentKey{target: target, reason: reason}
		row, exists := s.rows[key]
		if exists {
			if action.origin == identityLookupOriginNone || row.action == action {
				return false
			}
			row.action = action
			s.rows[key] = row
			// Runtime-only promotion is never serialized, but returns a snapshot
			// to preserve one mutation/snapshot ordering for this transition.
			return true
		}
		s.rows[key] = identityIntentRow{Target: target.String(), Reason: reason, CreatedAt: now.UTC(), action: action}
		return true
	})
}

func (s *identityIntentStore) removeMemory(target domain.PeerIdentity, matches func(identityIntentKey, identityIntentRow) bool) (int, *identityIntentSnapshot, error) {
	remaining := 0
	snapshot, err := s.mutate(func() bool {
		changed := false
		for key, row := range s.rows {
			if key.target == target && matches(key, row) {
				delete(s.rows, key)
				changed = true
			}
		}
		remaining = s.countLocked(target)
		return changed
	})
	return remaining, snapshot, err
}
func (s *identityIntentStore) recordAttemptMemory(target domain.PeerIdentity) (*identityIntentSnapshot, error) {
	return s.mutate(func() bool {
		changed := false
		for key, row := range s.rows {
			if key.target == target {
				row.Attempts++
				s.rows[key] = row
				changed = true
			}
		}
		return changed
	})
}
func (s *identityIntentStore) persist(snapshot *identityIntentSnapshot) error {
	if snapshot == nil {
		return nil
	}
	return s.save(*snapshot)
}

// Compatibility store operations are standalone: all I/O follows intent.mu.
func (s *identityIntentStore) add(target domain.PeerIdentity, reason identityIntentReason, now time.Time) (bool, error) {
	snapshot, err := s.addMemory(target, reason, now, identityLookupActionStamp{})
	if err != nil {
		return false, err
	}
	return snapshot != nil, s.persist(snapshot)
}
func (s *identityIntentStore) remove(target domain.PeerIdentity, reason identityIntentReason) (int, error) {
	remaining, snapshot, err := s.removeMemory(target, func(key identityIntentKey, _ identityIntentRow) bool { return key.reason == reason })
	if err != nil {
		return remaining, err
	}
	return remaining, s.persist(snapshot)
}
func (s *identityIntentStore) recordAttempt(target domain.PeerIdentity) error {
	snapshot, err := s.recordAttemptMemory(target)
	if err != nil {
		return err
	}
	return s.persist(snapshot)
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
		row.action = identityLookupActionStamp{}
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

// seedForTarget returns the current aggregate under intent.mu; resolver callers
// also hold resolver.mu so later mutation cannot target a replacement reason set.
func (s *identityIntentStore) seedForTarget(target domain.PeerIdentity) (identityIntentSeed, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seed := identityIntentSeed{Target: target}
	for key, row := range s.rows {
		if key.target != target {
			continue
		}
		if seed.Reasons == 0 || row.CreatedAt.Before(seed.CreatedAt) {
			seed.CreatedAt = row.CreatedAt
		}
		if row.Attempts > seed.Attempts {
			seed.Attempts = row.Attempts
		}
		seed.Reasons++
	}
	return seed, seed.Reasons > 0
}

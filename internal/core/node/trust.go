package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"corsa/internal/core/domain"
)

var errTrustConflict = errors.New("trusted contact conflict")

type trustedContact struct {
	Address      string    `json:"address"`
	PubKey       string    `json:"pub_key"`
	BoxKey       string    `json:"box_key"`
	BoxSignature string    `json:"box_signature"`
	FirstSeenAt  time.Time `json:"first_seen_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	Source       string    `json:"source"`
}

type trustFile struct {
	Contacts  map[string]trustedContact `json:"contacts"`
	Conflicts map[string]string         `json:"conflicts,omitempty"`
}

type trustStore struct {
	path      string
	mu        sync.RWMutex
	contacts  map[string]trustedContact
	conflicts map[string]string
}

func loadTrustStore(path string, self trustedContact) (*trustStore, error) {
	store := &trustStore{
		path:      path,
		contacts:  map[string]trustedContact{},
		conflicts: map[string]string{},
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
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read trust store %s: %w", path, err)
		}
	}

	if self.Address != "" {
		now := time.Now().UTC()
		if existing, ok := store.contacts[self.Address]; ok {
			existing.LastSeenAt = now
			store.contacts[self.Address] = existing
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

func (s *trustStore) remember(contact trustedContact) error {
	now := time.Now().UTC()

	s.mu.Lock()
	if existing, ok := s.contacts[contact.Address]; ok {
		if existing.PubKey != contact.PubKey || existing.BoxKey != contact.BoxKey || existing.BoxSignature != contact.BoxSignature {
			s.conflicts[contact.Address] = fmt.Sprintf("pinned contact mismatch from %s at %s", contact.Source, now.Format(time.RFC3339))
			contacts, conflicts := s.snapshotLocked()
			s.mu.Unlock()
			if err := s.saveSnapshot(contacts, conflicts); err != nil {
				return err
			}
			return errTrustConflict
		}

		existing.LastSeenAt = now
		existing.Source = contact.Source
		s.contacts[contact.Address] = existing
		contacts, conflicts := s.snapshotLocked()
		s.mu.Unlock()
		return s.saveSnapshot(contacts, conflicts)
	}

	contact.FirstSeenAt = now
	contact.LastSeenAt = now
	s.contacts[contact.Address] = contact
	contacts, conflicts := s.snapshotLocked()
	s.mu.Unlock()
	return s.saveSnapshot(contacts, conflicts)
}

// forget removes a contact from the trust store and persists the change.
// Returns true if the contact existed and was removed.
func (s *trustStore) forget(identity domain.PeerIdentity) (bool, error) {
	address := string(identity)

	s.mu.Lock()
	if _, ok := s.contacts[address]; !ok {
		s.mu.Unlock()
		return false, nil
	}
	delete(s.contacts, address)
	delete(s.conflicts, address)
	contacts, conflicts := s.snapshotLocked()
	s.mu.Unlock()

	if err := s.saveSnapshot(contacts, conflicts); err != nil {
		return false, fmt.Errorf("persist trust store after forget %s: %w", address, err)
	}
	return true, nil
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
	s.mu.RLock()
	contacts, conflicts := s.snapshotLocked()
	s.mu.RUnlock()
	return s.saveSnapshot(contacts, conflicts)
}

func (s *trustStore) snapshotLocked() (map[string]trustedContact, map[string]string) {
	contacts := make(map[string]trustedContact, len(s.contacts))
	for address, contact := range s.contacts {
		contacts[address] = contact
	}

	conflicts := make(map[string]string, len(s.conflicts))
	for address, conflict := range s.conflicts {
		conflicts[address] = conflict
	}

	return contacts, conflicts
}

func (s *trustStore) saveSnapshot(contacts map[string]trustedContact, conflicts map[string]string) error {
	if s.path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create trust store directory: %w", err)
	}

	payload, err := json.MarshalIndent(trustFile{
		Contacts:  contacts,
		Conflicts: conflicts,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal trust store: %w", err)
	}

	if err := os.WriteFile(s.path, payload, 0o600); err != nil {
		return fmt.Errorf("write trust store: %w", err)
	}

	return nil
}

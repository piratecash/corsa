package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
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

	if existing, ok := s.contacts[contact.Address]; ok {
		if existing.PubKey != contact.PubKey || existing.BoxKey != contact.BoxKey || existing.BoxSignature != contact.BoxSignature {
			s.conflicts[contact.Address] = fmt.Sprintf("pinned contact mismatch from %s at %s", contact.Source, now.Format(time.RFC3339))
			if err := s.save(); err != nil {
				return err
			}
			return errTrustConflict
		}

		existing.LastSeenAt = now
		existing.Source = contact.Source
		s.contacts[contact.Address] = existing
		return s.save()
	}

	contact.FirstSeenAt = now
	contact.LastSeenAt = now
	s.contacts[contact.Address] = contact
	return s.save()
}

func (s *trustStore) trustedContacts() map[string]trustedContact {
	out := make(map[string]trustedContact, len(s.contacts))
	for address, contact := range s.contacts {
		out[address] = contact
	}
	return out
}

func (s *trustStore) save() error {
	if s.path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create trust store directory: %w", err)
	}

	payload, err := json.MarshalIndent(trustFile{
		Contacts:  s.contacts,
		Conflicts: s.conflicts,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal trust store: %w", err)
	}

	if err := os.WriteFile(s.path, payload, 0o600); err != nil {
		return fmt.Errorf("write trust store: %w", err)
	}

	return nil
}

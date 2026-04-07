package desktop

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/domain"
)

type Preferences struct {
	path     string
	Language string            `json:"language"`
	Aliases  map[string]string `json:"aliases,omitempty"`
}

func LoadPreferences(path string) (*Preferences, error) {
	prefs := &Preferences{path: path}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return prefs, nil
		}
		return nil, fmt.Errorf("read preferences %s: %w", path, err)
	}

	if err := json.Unmarshal(data, prefs); err != nil {
		return nil, fmt.Errorf("decode preferences %s: %w", path, err)
	}

	prefs.path = path
	prefs.Language = normalizeLanguage(prefs.Language)
	if prefs.Aliases == nil {
		prefs.Aliases = make(map[string]string)
	}
	return prefs, nil
}

func (p *Preferences) Save() error {
	if p == nil || p.path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return fmt.Errorf("create preferences directory: %w", err)
	}

	aliases := p.Aliases
	if len(aliases) == 0 {
		aliases = nil
	}

	payload, err := json.MarshalIndent(struct {
		Language string            `json:"language"`
		Aliases  map[string]string `json:"aliases,omitempty"`
	}{
		Language: normalizeLanguage(p.Language),
		Aliases:  aliases,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal preferences: %w", err)
	}

	if err := os.WriteFile(p.path, payload, 0o600); err != nil {
		return fmt.Errorf("write preferences: %w", err)
	}

	return nil
}

// Alias returns the user-assigned alias for the given identity.
// Returns empty string if no alias is set.
func (p *Preferences) Alias(identity domain.PeerIdentity) string {
	if p == nil || p.Aliases == nil {
		return ""
	}
	return p.Aliases[string(identity)]
}

// SetAlias assigns a display name for the given identity.
// Empty alias removes the mapping.
func (p *Preferences) SetAlias(identity domain.PeerIdentity, alias string) {
	if p == nil {
		return
	}
	if p.Aliases == nil {
		p.Aliases = make(map[string]string)
	}
	key := string(identity)
	if alias == "" {
		delete(p.Aliases, key)
	} else {
		p.Aliases[key] = alias
	}
}

func preferencePathForIdentity(identityPath string) string {
	return identityPath + ".desktop.json"
}

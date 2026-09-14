package desktop

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Preferences is what the user has chosen, persisted next to the identity it
// belongs to. Exported fields are the persisted shape; path is unexported and
// therefore invisible to encoding/json, which is what lets Save marshal the
// value itself instead of restating the field list.
type Preferences struct {
	path         string
	Language     string            `json:"language"`
	Aliases      map[string]string `json:"aliases,omitempty"`
	RecentEmojis []string          `json:"recent_emojis,omitempty"`
	// CheckGitHubReleases is consent to ask the project's repository which
	// release is the newest. OFF by default and never implied: the request
	// goes straight to a third party, outside the p2p transport, and shows
	// that party this node's IP address. See internal/core/updatecheck.
	CheckGitHubReleases bool `json:"check_github_releases,omitempty"`
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
	prefs.RecentEmojis = normalizeRecentEmojis(prefs.RecentEmojis)
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

	// A normalized COPY of the value, not a hand-written mirror struct. The
	// mirror repeated every field a second time, and the compiler could not
	// tell that a field added to Preferences alone would load and then never
	// be written back. path is unexported, so json leaves it out.
	persisted := *p
	persisted.Language = normalizeLanguage(p.Language)
	persisted.RecentEmojis = normalizeRecentEmojis(p.RecentEmojis)
	if len(persisted.Aliases) == 0 {
		persisted.Aliases = nil
	}

	payload, err := json.MarshalIndent(persisted, "", "  ")
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
	return p.Aliases[identity.String()]
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
	key := identity.String()
	if alias == "" {
		delete(p.Aliases, key)
	} else {
		p.Aliases[key] = alias
	}
}

func preferencePathForIdentity(identityPath string) string {
	return identityPath + ".desktop.json"
}

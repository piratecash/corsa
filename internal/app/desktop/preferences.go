package desktop

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Preferences struct {
	path     string
	Language string `json:"language"`
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
	return prefs, nil
}

func (p *Preferences) Save() error {
	if p == nil || p.path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return fmt.Errorf("create preferences directory: %w", err)
	}

	payload, err := json.MarshalIndent(struct {
		Language string `json:"language"`
	}{
		Language: normalizeLanguage(p.Language),
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal preferences: %w", err)
	}

	if err := os.WriteFile(p.path, payload, 0o600); err != nil {
		return fmt.Errorf("write preferences: %w", err)
	}

	return nil
}

func preferencePathForIdentity(identityPath string) string {
	return identityPath + ".desktop.json"
}

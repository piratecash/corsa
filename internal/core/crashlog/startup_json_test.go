package crashlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// These startup events have scalar fields. Reject repeated top-level keys
// before decoding values: json.Unmarshal alone silently keeps the last one.
func decodeStartupJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("startup event is not a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("startup event key is not a string")
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, fmt.Errorf("duplicate startup JSON key %q", key)
		}
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("startup event has no closing object")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, errors.New("startup event has trailing JSON")
	}
	return fields, nil
}

func TestSetupJSONStartupKeepsSeveritySeparateFromConfiguredLevel(t *testing.T) {
	for _, tc := range []struct {
		name, configured, effective string
	}{
		{"default_warn", "", "warn"},
		{"explicit_info", "info", "info"},
		{"explicit_error", "error", "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			preserveCrashlogProcessState(t)
			dir := t.TempDir()
			t.Setenv("CORSA_CHATLOG_DIR", dir)
			t.Setenv("CORSA_LOG_LEVEL", tc.configured)
			t.Setenv(envLogFormat, "json")
			cleanup := Setup()
			defer cleanup()

			raw, err := os.ReadFile(filepath.Join(dir, logFileName))
			if err != nil {
				t.Fatal(err)
			}
			var startup map[string]json.RawMessage
			found := 0
			for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
				fields, err := decodeStartupJSONObject(line)
				if err != nil {
					t.Fatalf("startup log must have unique JSON fields: %v", err)
				}
				if string(fields["message"]) == `"logging initialised"` {
					startup = fields
					found++
				}
			}
			if found != 1 {
				t.Fatalf("logging initialised events = %d, want 1", found)
			}
			if string(startup["level"]) != `"info"` {
				t.Fatalf("startup severity = %s, want info", startup["level"])
			}
			if string(startup["configured_level"]) != fmt.Sprintf("%q", tc.effective) {
				t.Fatalf("configured level = %s, want %q", startup["configured_level"], tc.effective)
			}
			if got := zerolog.GlobalLevel().String(); got != tc.effective {
				t.Fatalf("steady-state gate = %q, want %q", got, tc.effective)
			}
		})
	}
}

func TestSetupLogcatStartupKeepsInfoSeverity(t *testing.T) {
	preserveCrashlogProcessState(t)
	dir := t.TempDir()
	t.Setenv("CORSA_CHATLOG_DIR", dir)
	t.Setenv("CORSA_LOG_LEVEL", "error")
	file, err := os.OpenFile(filepath.Join(dir, "captured-logcat.log"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	os.Stderr = file
	cleanup := setupLogcat()
	defer cleanup()

	raw, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, " INF ") || !strings.Contains(text, "logging to logcat (android)") {
		t.Fatalf("logcat startup must retain info severity: %q", text)
	}
	if !strings.Contains(text, "configured_level=error") {
		t.Fatalf("logcat startup must expose the separate configured gate: %q", text)
	}
	if zerolog.GlobalLevel() != zerolog.ErrorLevel {
		t.Fatal("logcat steady-state error gate was not applied")
	}
}

func TestStartupJSONDecoderRejectsDuplicateFields(t *testing.T) {
	if _, err := decodeStartupJSONObject([]byte(`{"level":"info","level":"warn","message":"logging initialised"}`)); err == nil {
		t.Fatal("duplicate level must not be silently overwritten")
	}
	if _, err := decodeStartupJSONObject([]byte(`{"level":"info","configured_level":"warn","message":"logging initialised"}`)); err != nil {
		t.Fatal(err)
	}
}

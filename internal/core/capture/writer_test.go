package capture

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestFileWriterRefusesWritesBeyondBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	w := &fileWriter{file: f, remaining: 4}
	if n, err := w.Write([]byte("1234")); n != 4 || err != nil {
		t.Fatalf("write at the exact limit: %d, %v", n, err)
	}
	if n, err := w.Write([]byte("5")); n != 0 || !errors.Is(err, errCaptureFileLimit) {
		t.Fatalf("write over the limit: %d, %v", n, err)
	}
	if info, err := f.Stat(); err != nil || info.Size() != 4 {
		t.Fatalf("unexpected file size after refusal: %v, %v", info, err)
	}
}

func TestFileWriterRefusesWholePrettyEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	w := &fileWriter{file: f, remaining: maxCaptureFileBytes}
	event := Event{Ts: time.Now(), WireDir: domain.WireDirectionIn, Kind: domain.PayloadKindJSON, Raw: `{"type":"ping"}`}
	if err := renderPrettyLine(w, event, false); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w.remaining = 2 // Enough for the separator but not for the whole event.
	if err := renderPrettyLine(w, event, true); !errors.Is(err, errCaptureFileLimit) {
		t.Fatalf("expected budget refusal, got %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || w.remaining != 2 {
		t.Fatal("budget refusal wrote part of an event or charged bytes")
	}
}

func TestRenderCompactLine(t *testing.T) {
	ts := time.Date(2026, 4, 14, 12, 44, 3, 412_000_000, time.UTC)

	tests := []struct {
		name string
		ev   Event
		want string
	}{
		{
			name: "outbound json success",
			ev: Event{
				Ts:      ts,
				WireDir: domain.WireDirectionOut,
				Outcome: domain.SendOutcomeSent,
				Kind:    domain.PayloadKindJSON,
				Raw:     `{"type":"ping"}`,
			},
			want: "2026-04-14T12:44:03.412Z out: {\"type\":\"ping\"}\n",
		},
		{
			name: "inbound json",
			ev: Event{
				Ts:      ts,
				WireDir: domain.WireDirectionIn,
				Kind:    domain.PayloadKindJSON,
				Raw:     `{"type":"pong"}`,
			},
			want: "2026-04-14T12:44:03.412Z in: {\"type\":\"pong\"}\n",
		},
		{
			name: "outbound write failed",
			ev: Event{
				Ts:      ts,
				WireDir: domain.WireDirectionOut,
				Outcome: domain.SendOutcomeWriteFailed,
				Kind:    domain.PayloadKindJSON,
				Raw:     `{"type":"ping"}`,
			},
			want: "2026-04-14T12:44:03.412Z out [write_failed]: {\"type\":\"ping\"}\n",
		},
		{
			name: "non-json payload",
			ev: Event{
				Ts:      ts,
				WireDir: domain.WireDirectionIn,
				Kind:    domain.PayloadKindNonJSON,
				Raw:     "HELLO",
			},
			want: "2026-04-14T12:44:03.412Z in: HELLO\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := renderCompactLine(&buf, tt.ev); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := buf.String()
			if got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

func TestRenderPrettyLine(t *testing.T) {
	ts := time.Date(2026, 4, 14, 12, 44, 3, 412_000_000, time.UTC)

	ev := Event{
		Ts:      ts,
		WireDir: domain.WireDirectionOut,
		Outcome: domain.SendOutcomeSent,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"ping"}`,
	}

	// First event — no separator.
	var buf bytes.Buffer
	if err := renderPrettyLine(&buf, ev, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := buf.String()
	if !strings.HasPrefix(got, "2026-04-14T12:44:03.412Z out:\n") {
		t.Errorf("header mismatch: %q", got)
	}
	if !strings.Contains(got, "\"type\": \"ping\"") {
		t.Errorf("expected pretty-printed JSON, got: %q", got)
	}

	// Second event — should have separator (blank line).
	buf.Reset()
	if err := renderPrettyLine(&buf, ev, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got = buf.String()
	if !strings.HasPrefix(got, "\n") {
		t.Error("expected leading blank line for needSeparator=true")
	}
}

func TestRenderCompactLine_CompactsJSON(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ev := Event{
		Ts:      ts,
		WireDir: domain.WireDirectionIn,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{  "type" :  "ping"  }`,
	}
	var buf bytes.Buffer
	if err := renderCompactLine(&buf, ev); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, `{"type":"ping"}`) {
		t.Errorf("expected compacted JSON, got: %q", got)
	}
}

func TestRenderPrettyLine_WriteFailed(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ev := Event{
		Ts:      ts,
		WireDir: domain.WireDirectionOut,
		Outcome: domain.SendOutcomeWriteFailed,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"ping"}`,
	}
	var buf bytes.Buffer
	if err := renderPrettyLine(&buf, ev, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "[write_failed]") {
		t.Errorf("expected [write_failed] marker, got: %q", got)
	}
}

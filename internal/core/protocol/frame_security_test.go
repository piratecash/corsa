package protocol

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// checkJSONDepth tests
// ---------------------------------------------------------------------------

func TestCheckJSONDepthNormal(t *testing.T) {
	t.Parallel()
	// Normal frame — depth 1 (top-level object).
	if err := checkJSONDepth(`{"type":"ping"}`, maxJSONDepth); err != nil {
		t.Fatalf("normal frame rejected: %v", err)
	}
}

func TestCheckJSONDepthNestedArray(t *testing.T) {
	t.Parallel()
	// Frame with nested array of objects — depth 3.
	json := `{"type":"peers","messages":[{"id":"abc","body":"hello"}]}`
	if err := checkJSONDepth(json, maxJSONDepth); err != nil {
		t.Fatalf("nested array frame rejected: %v", err)
	}
}

func TestCheckJSONDepthExceedsLimit(t *testing.T) {
	t.Parallel()
	// Build deeply nested JSON: {{{ ... }}}
	var b strings.Builder
	for i := 0; i < maxJSONDepth+5; i++ {
		b.WriteString(`{"a":`)
	}
	b.WriteString(`1`)
	for i := 0; i < maxJSONDepth+5; i++ {
		b.WriteString(`}`)
	}
	deep := b.String()

	if err := checkJSONDepth(deep, maxJSONDepth); err == nil {
		t.Fatal("deeply nested JSON should be rejected")
	}
}

func TestCheckJSONDepthExactlyAtLimit(t *testing.T) {
	t.Parallel()
	// Build JSON at exactly the limit.
	var b strings.Builder
	for i := 0; i < maxJSONDepth; i++ {
		b.WriteString(`{"a":`)
	}
	b.WriteString(`1`)
	for i := 0; i < maxJSONDepth; i++ {
		b.WriteString(`}`)
	}
	exact := b.String()

	if err := checkJSONDepth(exact, maxJSONDepth); err != nil {
		t.Fatalf("JSON at exact limit should be allowed: %v", err)
	}
}

func TestCheckJSONDepthIgnoresStrings(t *testing.T) {
	t.Parallel()
	// Braces inside strings should not count.
	json := `{"body":"{{{{{{{{{{{{{{{{{{{{"}`
	if err := checkJSONDepth(json, maxJSONDepth); err != nil {
		t.Fatalf("braces in strings should be ignored: %v", err)
	}
}

func TestCheckJSONDepthIgnoresEscapedQuotes(t *testing.T) {
	t.Parallel()
	// Escaped quotes should not toggle string mode.
	json := `{"body":"hello \" {{{{{{{{{{{{"}`
	if err := checkJSONDepth(json, maxJSONDepth); err != nil {
		t.Fatalf("escaped quotes should not confuse parser: %v", err)
	}
}

func TestCheckJSONDepthArrays(t *testing.T) {
	t.Parallel()
	// Arrays also contribute to depth.
	var b strings.Builder
	for i := 0; i < maxJSONDepth+5; i++ {
		b.WriteString(`[`)
	}
	b.WriteString(`1`)
	for i := 0; i < maxJSONDepth+5; i++ {
		b.WriteString(`]`)
	}
	deep := b.String()

	if err := checkJSONDepth(deep, maxJSONDepth); err == nil {
		t.Fatal("deeply nested arrays should be rejected")
	}
}

// ---------------------------------------------------------------------------
// ParseFrameLine rejects deep nesting
// ---------------------------------------------------------------------------

func TestParseFrameLineRejectsDeepNesting(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	for i := 0; i < maxJSONDepth+5; i++ {
		b.WriteString(`{"a":`)
	}
	b.WriteString(`1`)
	for i := 0; i < maxJSONDepth+5; i++ {
		b.WriteString(`}`)
	}

	_, err := ParseFrameLine(b.String())
	if err == nil {
		t.Fatal("ParseFrameLine should reject deeply nested JSON")
	}
}

func TestParseFrameLineAcceptsNormal(t *testing.T) {
	t.Parallel()
	frame, err := ParseFrameLine(`{"type":"ping"}`)
	if err != nil {
		t.Fatalf("normal frame should be accepted: %v", err)
	}
	if frame.Type != "ping" {
		t.Fatalf("expected type ping, got %s", frame.Type)
	}
}

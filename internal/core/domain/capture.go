package domain

import (
	"net/netip"
	"strings"
)

// ---------------------------------------------------------------------------
// Wire direction — per-frame send/recv inside a capture file
// ---------------------------------------------------------------------------

// WireDirection indicates whether a captured frame was sent to or received
// from the remote peer. This is orthogonal to PeerDirection (which describes
// how the TCP session was established).
type WireDirection string

const (
	WireDirectionIn  WireDirection = "in"
	WireDirectionOut WireDirection = "out"
)

// String returns the canonical short label for file headers.
func (d WireDirection) String() string { return string(d) }

// IsValid reports whether the direction is one of the known values.
func (d WireDirection) IsValid() bool {
	return d == WireDirectionIn || d == WireDirectionOut
}

// ---------------------------------------------------------------------------
// Send outcome — what happened when we tried to write a frame
// ---------------------------------------------------------------------------

// SendOutcome describes the result of a send-path write attempt.
// Capture events carry this so the operator can distinguish successful
// sends from failed write attempts directly in the capture file.
type SendOutcome string

const (
	SendOutcomeSent        SendOutcome = "sent"
	SendOutcomeWriteFailed SendOutcome = "write_failed"
)

// String returns the canonical label for capture file headers.
func (o SendOutcome) String() string { return string(o) }

// IsValid reports whether the outcome is one of the known values.
func (o SendOutcome) IsValid() bool {
	return o == SendOutcomeSent || o == SendOutcomeWriteFailed
}

// IsFailed reports whether the outcome represents a failed write attempt.
func (o SendOutcome) IsFailed() bool {
	return o == SendOutcomeWriteFailed
}

// ---------------------------------------------------------------------------
// Capture format — how capture events are rendered to disk
// ---------------------------------------------------------------------------

// CaptureFormat selects the file rendering style for captured traffic.
type CaptureFormat string

const (
	CaptureFormatCompact CaptureFormat = "compact"
	CaptureFormatPretty  CaptureFormat = "pretty"
)

// String returns the canonical format label.
func (f CaptureFormat) String() string { return string(f) }

// IsValid reports whether the format is one of the known values.
func (f CaptureFormat) IsValid() bool {
	return f == CaptureFormatCompact || f == CaptureFormatPretty
}

// ParseCaptureFormat parses a user-supplied string into a CaptureFormat.
// Returns CaptureFormatCompact for empty input (default).
// Returns false if the string is not a recognised format.
func ParseCaptureFormat(s string) (CaptureFormat, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "compact":
		return CaptureFormatCompact, true
	case "pretty":
		return CaptureFormatPretty, true
	default:
		return "", false
	}
}

// ---------------------------------------------------------------------------
// Capture scope — granularity of a capture rule
// ---------------------------------------------------------------------------

// CaptureScope describes the match granularity of a traffic capture rule.
type CaptureScope string

const (
	CaptureScopeConnID CaptureScope = "conn_id"
	CaptureScopeIP     CaptureScope = "ip"
	CaptureScopeAll    CaptureScope = "all"
)

// String returns the canonical scope label.
func (s CaptureScope) String() string { return string(s) }

// IsValid reports whether the scope is one of the known values.
func (s CaptureScope) IsValid() bool {
	return s == CaptureScopeConnID || s == CaptureScopeIP || s == CaptureScopeAll
}

// ---------------------------------------------------------------------------
// Payload kind — classification of a captured wire payload
// ---------------------------------------------------------------------------

// PayloadKind classifies the content of a captured wire payload so that
// the operator (and downstream tooling) can distinguish normal JSON traffic
// from anomalies without re-parsing every line.
type PayloadKind string

const (
	// PayloadKindJSON — payload is valid JSON.
	PayloadKindJSON PayloadKind = "json"
	// PayloadKindInvalidJSON — payload looks like JSON but failed to parse.
	PayloadKindInvalidJSON PayloadKind = "invalid_json"
	// PayloadKindNonJSON — payload is clearly not a JSON line.
	PayloadKindNonJSON PayloadKind = "non_json"
	// PayloadKindFrameTooLarge — input was discarded or truncated by a
	// size guard before reaching capture.
	PayloadKindFrameTooLarge PayloadKind = "frame_too_large"
	// PayloadKindMalformed — general fallback for cases that do not fit
	// the categories above.
	PayloadKindMalformed PayloadKind = "malformed"
)

// String returns the canonical kind label.
func (k PayloadKind) String() string { return string(k) }

// IsValid reports whether the kind is one of the known values.
func (k PayloadKind) IsValid() bool {
	switch k {
	case PayloadKindJSON, PayloadKindInvalidJSON, PayloadKindNonJSON,
		PayloadKindFrameTooLarge, PayloadKindMalformed:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Filename sanitization for capture paths
// ---------------------------------------------------------------------------

// SanitizeCapturePathComponent replaces characters that are unsafe in
// filesystem path components: colons (IPv6, timestamps), path separators,
// control characters, and trailing whitespace/dots. The result is safe to
// use as a single directory or filename segment on all major OSes.
//
// This is the single helper for all capture filename construction —
// raw IP/string values must never be concatenated into paths directly.
func SanitizeCapturePathComponent(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == ':':
			b.WriteByte('-')
		case r == '/' || r == '\\' || r == '\x00':
			// drop path separators and null bytes
		case r < 0x20 || r == 0x7F:
			// drop control characters
		case r == '%':
			b.WriteByte('-')
		default:
			b.WriteRune(r)
		}
	}
	result := strings.TrimSpace(b.String())
	result = strings.Trim(result, ".")
	if result == "" {
		return "unknown"
	}
	return result
}

// FormatIPForFilename returns a filesystem-safe string representation of
// an IP address. IPv6 colons are replaced with hyphens; zone suffixes are
// stripped.
func FormatIPForFilename(addr netip.Addr) string {
	// WithZone("") strips any zone suffix.
	clean := addr.WithZone("")
	return SanitizeCapturePathComponent(clean.String())
}

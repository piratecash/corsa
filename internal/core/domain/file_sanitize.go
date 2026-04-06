package domain

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// SanitizeFileName strips path traversal components and dangerous characters
// from an untrusted file name received over the network.
//
// The function guarantees:
//   - Percent-encoded sequences decoded before validation (prevents %2F bypass).
//   - No directory separators (/, \) in the result — backslashes are
//     normalised to forward slashes before basename extraction so that
//     Windows-style paths (C:\Users\victim\file.txt) are handled correctly
//     even on non-Windows platforms.
//   - No path traversal sequences (.., .).
//   - No null bytes (including percent-encoded %00).
//   - No Unicode bidirectional overrides (prevents RLO extension spoofing).
//   - No control characters (prevents log injection, ANSI escape codes).
//   - Windows reserved device names prefixed with underscore (prevents CON/NUL/PRN hangs).
//   - No leading/trailing whitespace or dots.
//   - Non-empty result (falls back to "unnamed" if nothing remains).
//   - Valid UTF-8 (invalid bytes are removed).
//   - Maximum length of 255 bytes (filesystem-safe).
//
// This MUST be called on every file name arriving from the network before
// it is used to construct any filesystem path.
func SanitizeFileName(name string) string {
	// 1. Decode percent-encoded sequences (%2F → /, %2E → ., %00 → \x00).
	//    An attacker can bypass filepath.Base by encoding slashes and dots
	//    as %2F, %2E, %5C etc. We decode iteratively to handle double-
	//    encoding (%252F → %2F → /) — up to 3 rounds to catch triple
	//    encoding without infinite loops.
	for i := 0; i < 3; i++ {
		decoded, err := url.PathUnescape(name)
		if err != nil || decoded == name {
			break
		}
		name = decoded
	}

	// 2. Strip null bytes — they can truncate C-level path strings.
	name = strings.ReplaceAll(name, "\x00", "")

	// 3. Strip Unicode bidirectional override characters. An attacker can
	//    use U+202E (RLO) to visually reverse the file extension:
	//    "innocent\u202Eexe.txt" displays as "innocenttxt.exe" in many UIs.
	//    Also strip other bidi controls that can confuse rendering.
	name = stripBidiControls(name)

	// 4. Strip control characters (ASCII 0x01-0x1F, 0x7F, and C1 0x80-0x9F
	//    except for valid multi-byte UTF-8). These can inject fake log lines,
	//    ANSI escape sequences, or terminal control codes.
	name = stripControlChars(name)

	// 5. Normalise path separators and extract only the base name.
	//    On non-Windows platforms filepath.Base does not treat backslash as
	//    a path separator, so "C:\Users\victim\file.txt" would survive as a
	//    single component. Normalise backslashes to forward slashes first so
	//    filepath.Base correctly extracts the basename on every OS.
	name = strings.ReplaceAll(name, "\\", "/")
	name = filepath.Base(name)

	// 6. Reject the degenerate cases that filepath.Base can return.
	if name == "." || name == ".." || name == string(filepath.Separator) {
		return "unnamed"
	}

	// 7. Strip leading/trailing whitespace and dots (prevents hidden files
	//    on Unix and reserved names on Windows like "." or "..").
	name = strings.TrimSpace(name)
	name = strings.Trim(name, ".")
	name = strings.TrimSpace(name)

	// 8. Remove invalid UTF-8 sequences.
	if !utf8.ValidString(name) {
		var b strings.Builder
		for i := 0; i < len(name); {
			r, size := utf8.DecodeRuneInString(name[i:])
			if r != utf8.RuneError || size > 1 {
				b.WriteRune(r)
			}
			i += size
		}
		name = b.String()
	}

	// 9. Reject Windows reserved device names. On Windows, files named CON,
	//     NUL, PRN, COM1..COM9, LPT1..LPT9 (with or without extension) can
	//     cause hangs, crashes, or access to hardware devices.
	if isWindowsReservedName(name) {
		name = "_" + name
	}

	// 10. Truncate to 255 bytes (most filesystems limit individual names).
	if len(name) > 255 {
		name = truncateUTF8(name, 255)
	}

	// 11. Final fallback.
	if name == "" {
		return "unnamed"
	}

	return name
}

// stripBidiControls removes Unicode bidirectional override and embedding
// characters that can trick users into misreading file extensions.
//
// Targeted codepoints:
//   - U+200E  LEFT-TO-RIGHT MARK
//   - U+200F  RIGHT-TO-LEFT MARK
//   - U+200B  ZERO WIDTH SPACE (invisible separator)
//   - U+200C  ZERO WIDTH NON-JOINER
//   - U+200D  ZERO WIDTH JOINER
//   - U+202A  LEFT-TO-RIGHT EMBEDDING
//   - U+202B  RIGHT-TO-LEFT EMBEDDING
//   - U+202C  POP DIRECTIONAL FORMATTING
//   - U+202D  LEFT-TO-RIGHT OVERRIDE
//   - U+202E  RIGHT-TO-LEFT OVERRIDE
//   - U+2066  LEFT-TO-RIGHT ISOLATE
//   - U+2067  RIGHT-TO-LEFT ISOLATE
//   - U+2068  FIRST STRONG ISOLATE
//   - U+2069  POP DIRECTIONAL ISOLATE
//   - U+FEFF  BYTE ORDER MARK (zero-width no-break space)
func stripBidiControls(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '\u200E', '\u200F', '\u200B', '\u200C', '\u200D',
			'\u202A', '\u202B', '\u202C', '\u202D', '\u202E',
			'\u2066', '\u2067', '\u2068', '\u2069',
			'\uFEFF':
			return -1 // drop
		}
		return r
	}, s)
}

// stripControlChars removes ASCII control characters (0x01-0x1F except \t,
// plus 0x7F DEL) and Unicode C1 control characters (0x80-0x9F). These can
// inject ANSI escape sequences into terminal-based log viewers or create
// fake log lines via embedded newlines.
func stripControlChars(s string) string {
	return strings.Map(func(r rune) rune {
		// Allow tab (0x09) — some file names legitimately contain tabs
		// (rare but harmless for filesystem). Block everything else.
		if r == '\t' {
			return r
		}
		if r < 0x20 || r == 0x7F { // ASCII C0 controls + DEL
			return -1
		}
		if r >= 0x80 && r <= 0x9F { // Unicode C1 controls
			return -1
		}
		return r
	}, s)
}

// isWindowsReservedName returns true if the file name (without extension)
// matches a Windows reserved device name. These names cause special behavior
// on Windows regardless of extension: "CON.txt" still opens the console.
func isWindowsReservedName(name string) bool {
	// Strip extension for comparison.
	base := strings.TrimSuffix(name, filepath.Ext(name))
	base = strings.ToUpper(strings.TrimSpace(base))

	switch base {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}

	// COM1..COM9 and LPT1..LPT9.
	if len(base) == 4 &&
		(strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) {
		digit := base[3]
		if digit >= '1' && digit <= '9' {
			return true
		}
	}

	return false
}

// ValidateFileHash checks that a file hash string is a valid hex-encoded
// SHA-256 digest. This prevents glob/path injection through crafted hash
// values that could contain wildcards or path separators.
func ValidateFileHash(hash string) error {
	if len(hash) != 64 {
		return fmt.Errorf("invalid file hash length: expected 64 hex chars, got %d", len(hash))
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return fmt.Errorf("invalid file hash encoding: %w", err)
	}
	return nil
}

// truncateUTF8 truncates s to at most maxBytes without breaking a multi-byte
// UTF-8 character.
func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	// Walk backward from maxBytes to find a valid rune boundary.
	for maxBytes > 0 && !utf8.RuneStart(s[maxBytes]) {
		maxBytes--
	}
	return s[:maxBytes]
}

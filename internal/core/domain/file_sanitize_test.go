package domain

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// SanitizeFileName tests
// ---------------------------------------------------------------------------

func TestSanitizeFileNameSimple(t *testing.T) {
	t.Parallel()
	got := SanitizeFileName("document.pdf")
	if got != "document.pdf" {
		t.Fatalf("expected document.pdf, got %q", got)
	}
}

func TestSanitizeFileNamePathTraversalUnix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"../../../etc/crontab", "crontab"},
		{"../../../../etc/cron.d/evil", "evil"},
		{"../secret.txt", "secret.txt"},
		{"./hidden.txt", "hidden.txt"},
		{"foo/../bar.txt", "bar.txt"},
		{"/etc/passwd", "passwd"},
		{"///etc///shadow", "shadow"},
	}
	for _, tc := range cases {
		got := SanitizeFileName(tc.input)
		if got != tc.want {
			t.Errorf("SanitizeFileName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSanitizeFileNamePathTraversalWindows(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{`..\..\Windows\System32\config`, "config"},
		{`C:\Users\victim\file.txt`, "file.txt"},
		{`\\server\share\file.doc`, "file.doc"},
	}
	for _, tc := range cases {
		got := SanitizeFileName(tc.input)
		if got != tc.want {
			t.Errorf("SanitizeFileName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSanitizeFileNameNullBytes(t *testing.T) {
	t.Parallel()
	got := SanitizeFileName("file\x00.txt")
	if strings.Contains(got, "\x00") {
		t.Fatalf("result should not contain null bytes: %q", got)
	}
	if got != "file.txt" {
		t.Fatalf("expected file.txt, got %q", got)
	}
}

func TestSanitizeFileNameEmpty(t *testing.T) {
	t.Parallel()
	got := SanitizeFileName("")
	if got != "unnamed" {
		t.Fatalf("expected unnamed for empty input, got %q", got)
	}
}

func TestSanitizeFileNameDotsOnly(t *testing.T) {
	t.Parallel()
	cases := []string{".", "..", "...", "...."}
	for _, input := range cases {
		got := SanitizeFileName(input)
		if got != "unnamed" {
			t.Errorf("SanitizeFileName(%q) = %q, want unnamed", input, got)
		}
	}
}

func TestSanitizeFileNameWhitespace(t *testing.T) {
	t.Parallel()
	got := SanitizeFileName("  file.txt  ")
	if got != "file.txt" {
		t.Fatalf("expected file.txt, got %q", got)
	}
}

func TestSanitizeFileNameLongName(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("a", 500) + ".txt"
	got := SanitizeFileName(long)
	if len(got) > 255 {
		t.Fatalf("result should be <= 255 bytes, got %d", len(got))
	}
}

func TestSanitizeFileNamePreservesUnicode(t *testing.T) {
	t.Parallel()
	got := SanitizeFileName("документ.pdf")
	if got != "документ.pdf" {
		t.Fatalf("expected документ.pdf, got %q", got)
	}
}

func TestSanitizeFileNameNoDirectorySeparators(t *testing.T) {
	t.Parallel()
	inputs := []string{
		"../../../etc/cron.d/evil-rule",
		"a/b/c/d.txt",
		"a\\b\\c\\d.txt",
		"/absolute/path.txt",
	}
	for _, input := range inputs {
		got := SanitizeFileName(input)
		if strings.ContainsAny(got, "/") {
			t.Errorf("SanitizeFileName(%q) = %q, contains directory separator", input, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Percent-encoding bypass tests
// ---------------------------------------------------------------------------

func TestSanitizeFileNamePercentEncodedSlash(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		// %2F = /
		{"..%2F..%2F..%2Fetc%2Fcron.d%2Fevil", "evil"},
		// %2f lowercase
		{"..%2f..%2f..%2fetc%2fpasswd", "passwd"},
		// %5C = backslash
		{"..%5C..%5C..%5CWindows%5Cevil.dll", "evil.dll"},
		// mixed literal and encoded
		{"../..%2F../../etc/shadow", "shadow"},
	}
	for _, tc := range cases {
		got := SanitizeFileName(tc.input)
		if got != tc.want {
			t.Errorf("SanitizeFileName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSanitizeFileNameDoubleEncoding(t *testing.T) {
	t.Parallel()
	// %252F → first decode → %2F → second decode → /
	got := SanitizeFileName("..%252F..%252Fetc%252Fpasswd")
	if strings.ContainsAny(got, "/\\") {
		t.Fatalf("double-encoded slashes not caught: %q", got)
	}
	if got != "passwd" {
		t.Fatalf("expected passwd, got %q", got)
	}
}

func TestSanitizeFileNameTripleEncoding(t *testing.T) {
	t.Parallel()
	// %25252F → %252F → %2F → /
	got := SanitizeFileName("..%25252F..%25252Fetc%25252Fpasswd")
	if strings.ContainsAny(got, "/\\") {
		t.Fatalf("triple-encoded slashes not caught: %q", got)
	}
	if got != "passwd" {
		t.Fatalf("expected passwd, got %q", got)
	}
}

func TestSanitizeFileNamePercentEncodedDots(t *testing.T) {
	t.Parallel()
	// %2E = .
	cases := []struct {
		input string
		want  string
	}{
		{"%2E%2E/%2E%2E/etc/passwd", "passwd"},
		{"%2e%2e%2f%2e%2e%2fetc%2fpasswd", "passwd"},
	}
	for _, tc := range cases {
		got := SanitizeFileName(tc.input)
		if got != tc.want {
			t.Errorf("SanitizeFileName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSanitizeFileNamePercentEncodedNull(t *testing.T) {
	t.Parallel()
	// %00 = null byte
	got := SanitizeFileName("file%00.txt")
	if strings.Contains(got, "\x00") {
		t.Fatalf("percent-encoded null byte not stripped: %q", got)
	}
	if got != "file.txt" {
		t.Fatalf("expected file.txt, got %q", got)
	}
}

func TestSanitizeFileNameSafePercentInName(t *testing.T) {
	t.Parallel()
	// A file literally named "report 50%.pdf" should survive
	// (% not followed by valid hex pair stays as-is after unescape).
	got := SanitizeFileName("report 50%.pdf")
	if got != "report 50%.pdf" {
		t.Fatalf("expected 'report 50%%.pdf', got %q", got)
	}
}

func TestSanitizeFileNameHiddenFiles(t *testing.T) {
	t.Parallel()
	// Leading dots are stripped to prevent hidden files.
	got := SanitizeFileName(".bashrc")
	if got != "bashrc" {
		t.Fatalf("expected bashrc, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// ValidateFileHash tests
// ---------------------------------------------------------------------------

func TestValidateFileHashValid(t *testing.T) {
	t.Parallel()
	hash := "3f432de348e9c402784b1732ce6e8ddbc3698516958c5b5bd707315e75800d59"
	if err := ValidateFileHash(hash); err != nil {
		t.Fatalf("valid hash rejected: %v", err)
	}
}

func TestValidateFileHashTooShort(t *testing.T) {
	t.Parallel()
	if err := ValidateFileHash("abc123"); err == nil {
		t.Fatal("short hash should be rejected")
	}
}

func TestValidateFileHashTooLong(t *testing.T) {
	t.Parallel()
	hash := strings.Repeat("a", 65)
	if err := ValidateFileHash(hash); err == nil {
		t.Fatal("long hash should be rejected")
	}
}

func TestValidateFileHashNonHex(t *testing.T) {
	t.Parallel()
	// 64 chars but contains non-hex characters.
	hash := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	if err := ValidateFileHash(hash); err == nil {
		t.Fatal("non-hex hash should be rejected")
	}
}

func TestValidateFileHashWithGlobChars(t *testing.T) {
	t.Parallel()
	// An attacker trying to inject glob wildcards.
	hash := "3f432de348e9c402784b1732ce6e8ddb*3698516958c5b5bd707315e75800d5"
	if err := ValidateFileHash(hash); err == nil {
		t.Fatal("hash with glob chars should be rejected")
	}
}

func TestValidateFileHashWithPathTraversal(t *testing.T) {
	t.Parallel()
	hash := "../../../etc/passwd/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := ValidateFileHash(hash); err == nil {
		t.Fatal("hash with path traversal should be rejected")
	}
}

// ---------------------------------------------------------------------------
// Unicode Bidi Override tests
// ---------------------------------------------------------------------------

func TestSanitizeFileNameBidiRLO(t *testing.T) {
	t.Parallel()
	// U+202E RIGHT-TO-LEFT OVERRIDE: "innocent\u202Eexe.txt" renders
	// as "innocenttxt.exe" in many UIs — classic extension spoofing.
	got := SanitizeFileName("innocent\u202Eexe.txt")
	if strings.ContainsRune(got, '\u202E') {
		t.Fatalf("RLO character not stripped: %q", got)
	}
	// After stripping RLO, the name should be "innocentexe.txt".
	if got != "innocentexe.txt" {
		t.Fatalf("expected innocentexe.txt, got %q", got)
	}
}

func TestSanitizeFileNameBidiVariants(t *testing.T) {
	t.Parallel()
	bidiChars := []rune{
		'\u200E', '\u200F', '\u200B', '\u200C', '\u200D',
		'\u202A', '\u202B', '\u202C', '\u202D', '\u202E',
		'\u2066', '\u2067', '\u2068', '\u2069', '\uFEFF',
	}
	for _, ch := range bidiChars {
		input := "file" + string(ch) + ".txt"
		got := SanitizeFileName(input)
		if strings.ContainsRune(got, ch) {
			t.Errorf("bidi char U+%04X not stripped from %q → %q", ch, input, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Control character / log injection tests
// ---------------------------------------------------------------------------

func TestSanitizeFileNameNewlineInjection(t *testing.T) {
	t.Parallel()
	// An attacker injects newlines to create fake log entries.
	got := SanitizeFileName("file.txt\n2026-04-05 CRITICAL: system compromised")
	if strings.Contains(got, "\n") {
		t.Fatalf("newline not stripped: %q", got)
	}
}

func TestSanitizeFileNameANSIEscape(t *testing.T) {
	t.Parallel()
	// ANSI escape sequence: ESC[31m makes text red in terminals.
	got := SanitizeFileName("file\x1b[31mEVIL\x1b[0m.txt")
	if strings.Contains(got, "\x1b") {
		t.Fatalf("ANSI escape not stripped: %q", got)
	}
}

func TestSanitizeFileNameCarriageReturn(t *testing.T) {
	t.Parallel()
	got := SanitizeFileName("file.txt\rOverwritten")
	if strings.Contains(got, "\r") {
		t.Fatalf("carriage return not stripped: %q", got)
	}
}

func TestSanitizeFileNameTabPreserved(t *testing.T) {
	t.Parallel()
	// Tabs are rare but not dangerous — preserve them.
	got := SanitizeFileName("file\twith\ttabs.txt")
	if got != "file\twith\ttabs.txt" {
		t.Fatalf("tabs should be preserved: %q", got)
	}
}

// ---------------------------------------------------------------------------
// Windows reserved device name tests
// ---------------------------------------------------------------------------

func TestSanitizeFileNameWindowsReserved(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input    string
		mustHave string // the result must contain this prefix
	}{
		{"CON", "_"},
		{"CON.txt", "_"},
		{"con.txt", "_"},
		{"NUL", "_"},
		{"NUL.txt", "_"}, // NUL with extension
		{"PRN", "_"},
		{"AUX", "_"},
		{"COM1", "_"},
		{"COM9.log", "_"},
		{"LPT1", "_"},
		{"LPT9.dat", "_"},
	}
	for _, tc := range cases {
		got := SanitizeFileName(tc.input)
		if !strings.HasPrefix(got, tc.mustHave) {
			t.Errorf("SanitizeFileName(%q) = %q, should start with %q", tc.input, got, tc.mustHave)
		}
	}
}

func TestSanitizeFileNameNonReservedPassThrough(t *testing.T) {
	t.Parallel()
	// These look similar but are NOT reserved.
	safe := []string{"COM0.txt", "LPT0.txt", "CONX.txt", "NULLify.txt", "console.log"}
	for _, name := range safe {
		got := SanitizeFileName(name)
		if strings.HasPrefix(got, "_") {
			t.Errorf("SanitizeFileName(%q) = %q, should not be prefixed (not reserved)", name, got)
		}
	}
}

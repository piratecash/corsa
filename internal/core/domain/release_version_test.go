package domain

import (
	"errors"
	"testing"
)

func TestParseReleaseVersionAcceptsBothSpellings(t *testing.T) {
	// The tag carries the "v"; config.CorsaVersion does not. Both name the
	// same release, which is the whole reason this type exists.
	for _, text := range []string{"v2.3.69", "2.3.69", "  v2.3.69  "} {
		version, err := ParseReleaseVersion(text)
		if err != nil {
			t.Fatalf("ParseReleaseVersion(%q): unexpected error: %v", text, err)
		}
		want := ReleaseVersion{Major: 2, Minor: 3, Build: 69}
		if version != want {
			t.Fatalf("ParseReleaseVersion(%q) = %+v, want %+v", text, version, want)
		}
		if got := version.String(); got != "2.3.69" {
			t.Fatalf("String() = %q, want %q", got, "2.3.69")
		}
	}
}

func TestParseReleaseVersionRejectsMalformed(t *testing.T) {
	// Signed components are the interesting half: strconv.Atoi would accept
	// "-1", and a negative component sorts BELOW a real release, so a tag
	// named "2.-1.0" would quietly read as older than everything instead of
	// being refused.
	cases := []string{"", "v", "2.3", "2.3.69.1", "2.3.x", "-1.0.0", "2.-1.0", "2.3.+1", "two.three.four"}
	for _, text := range cases {
		_, err := ParseReleaseVersion(text)
		if err == nil {
			t.Fatalf("ParseReleaseVersion(%q): expected an error", text)
		}
		if !errors.Is(err, ErrReleaseVersionMalformed) {
			t.Fatalf("ParseReleaseVersion(%q): error %v is not ErrReleaseVersionMalformed", text, err)
		}
	}
}

func TestReleaseVersionNewerComparesComponentsInOrder(t *testing.T) {
	cases := []struct {
		name  string
		left  ReleaseVersion
		right ReleaseVersion
		newer bool
	}{
		// The case a string comparison gets wrong: "2.3.9" > "2.3.70"
		// lexicographically, and the build component is where releases
		// actually differ.
		{"build nine against seventy", ReleaseVersion{2, 3, 9}, ReleaseVersion{2, 3, 70}, false},
		{"build seventy against nine", ReleaseVersion{2, 3, 70}, ReleaseVersion{2, 3, 9}, true},
		{"minor outranks build", ReleaseVersion{2, 4, 0}, ReleaseVersion{2, 3, 99}, true},
		{"major outranks minor", ReleaseVersion{3, 0, 0}, ReleaseVersion{2, 9, 9}, true},
		{"equal is not newer", ReleaseVersion{2, 3, 69}, ReleaseVersion{2, 3, 69}, false},
		{"older is not newer", ReleaseVersion{2, 3, 68}, ReleaseVersion{2, 3, 69}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.left.Newer(tc.right); got != tc.newer {
				t.Fatalf("%s.Newer(%s) = %v, want %v", tc.left, tc.right, got, tc.newer)
			}
		})
	}
}

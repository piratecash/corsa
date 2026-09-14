// Package domain — released version as an ordered value.
package domain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrReleaseVersionMalformed is returned by ParseReleaseVersion for anything
// that is not "MAJOR.MINOR.BUILD" with an optional leading "v". Callers that
// walk a list of names (git tags, for one) use errors.Is to tell "this entry
// is not a release" from a real failure.
var ErrReleaseVersionMalformed = errors.New("malformed release version")

// ReleaseVersion is a published Corsa release as its three ordered numeric
// components.
//
// It exists because ClientVersion is a DISPLAY string and two display strings
// cannot be compared: "2.3.9" sorts above "2.3.70" lexicographically, and the
// build component is exactly where releases differ. Anything that has to
// decide which of two versions is newer takes this type, so the comparison is
// a method rather than a string trick repeated at each call site.
//
// The zero value is not "unknown" — it is version 0.0.0, which is older than
// every real release. Absence is modelled by the caller (a pointer, an ok
// flag), never by the zero value.
type ReleaseVersion struct {
	Major int
	Minor int
	Build int
}

// ParseReleaseVersion reads "2.3.69" or "v2.3.69".
//
// The leading "v" is accepted because that is how the release tags are
// written, while config.CorsaVersion has no prefix — both spellings name the
// same release and the type is what makes them comparable.
func ParseReleaseVersion(text string) (ReleaseVersion, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(text), "v")
	parts := strings.Split(trimmed, ".")
	if len(parts) != 3 {
		return ReleaseVersion{}, fmt.Errorf("%w: %q", ErrReleaseVersionMalformed, text)
	}

	components := make([]int, 0, len(parts))
	for _, part := range parts {
		// ParseUint, not Atoi: Atoi accepts "-1" and "+2", which would make
		// "2.-1.0" a version that sorts below "2.0.0" instead of being refused.
		value, err := strconv.ParseUint(part, 10, 31)
		if err != nil {
			return ReleaseVersion{}, fmt.Errorf("%w: %q", ErrReleaseVersionMalformed, text)
		}
		components = append(components, int(value))
	}

	return ReleaseVersion{Major: components[0], Minor: components[1], Build: components[2]}, nil
}

// String renders the version without the tag prefix.
func (v ReleaseVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Build)
}

// Newer reports whether v is a later release than other.
func (v ReleaseVersion) Newer(other ReleaseVersion) bool {
	if v.Major != other.Major {
		return v.Major > other.Major
	}
	if v.Minor != other.Minor {
		return v.Minor > other.Minor
	}
	return v.Build > other.Build
}

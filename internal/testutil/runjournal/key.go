package runjournal

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Measurement names the driver or scenario family a configuration belongs to.
type Measurement string

// Label is the configuration in words, for a human reading the directory. It
// is part of the identifier: two configurations with identical parameters that
// exist for different reasons (a control and a grid point) are different runs.
type Label string

// ParamName names one input of a configuration.
type ParamName string

// ParamValue is one input's value, already rendered by the driver. The journal
// compares values as written and never interprets them.
type ParamValue string

// Param is one named input of a configuration.
type Param struct {
	Name  ParamName  `json:"name"`
	Value ParamValue `json:"value"`
}

// ConfigKey is what a run IS: which measurement, which configuration of it,
// under which parameters.
//
// Every input that changes the result belongs in Params. A parameter left out
// is a parameter a resumed sweep cannot notice has changed: it would skip a run
// made under other conditions and call it the same one.
//
// The sources a run was measured with are deliberately NOT part of the key: the
// same configuration measured on other code is the same configuration measured
// again, and is caught on resume as a mismatch rather than hidden as a new run.
type ConfigKey struct {
	Measurement Measurement
	Label       Label
	Params      []Param
}

// ConfigID is a configuration's identifier: sixteen lowercase hex characters
// over the canonical form of the key.
type ConfigID string

// configIDDomain separates this hash from every other sha256 in the project, so
// the same bytes hashed for another purpose can never produce an identifier.
const configIDDomain = "corsa/testutil/runjournal/config/v1\x00"

// configIDHexLen is short enough to type into a command line and long enough
// that the configurations of one stand do not collide.
const configIDHexLen = 16

// Validate refuses a key that cannot identify a run.
func (k ConfigKey) Validate() error {
	if k.Measurement == "" {
		return fmt.Errorf("%w: no measurement", ErrInvalidKey)
	}
	seen := make(map[ParamName]struct{}, len(k.Params))
	for _, param := range k.Params {
		if param.Name == "" {
			return fmt.Errorf("%w: %s has a parameter without a name", ErrInvalidKey, k.Measurement)
		}
		if _, duplicate := seen[param.Name]; duplicate {
			// Two values under one name would make the canonical form depend on
			// which of them a reader happens to look at.
			return fmt.Errorf("%w: %s declares parameter %q twice", ErrInvalidKey, k.Measurement, param.Name)
		}
		seen[param.Name] = struct{}{}
	}
	return nil
}

// canonicalParams returns the parameters sorted by name in byte order. Sorting
// is what makes the identifier a function of the parameter SET: a driver that
// declares the same parameters in another order describes the same run.
//
// Ties are broken by value, so even a key that fails Validate (two values under
// one name) has exactly one canonical form instead of one per sort run.
func (k ConfigKey) canonicalParams() []Param {
	sorted := append([]Param(nil), k.Params...)
	slices.SortFunc(sorted, func(a, b Param) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Value, b.Value))
	})
	return sorted
}

// canonical renders the key with every field length-prefixed. Length prefixes,
// rather than separators, are what keep {a: "b=c"} and {"a=b": c} apart: no
// value can contain a byte that shifts a field boundary.
func (k ConfigKey) canonical() []byte {
	var out strings.Builder
	writeField := func(tag string, value string) {
		out.WriteString(tag)
		out.WriteString(strconv.Itoa(len(value)))
		out.WriteByte(':')
		out.WriteString(value)
		out.WriteByte('\n')
	}
	writeField("m", string(k.Measurement))
	writeField("l", string(k.Label))
	params := k.canonicalParams()
	writeField("n", strconv.Itoa(len(params)))
	for _, param := range params {
		writeField("k", string(param.Name))
		writeField("v", string(param.Value))
	}
	return []byte(out.String())
}

// ID derives the configuration's identifier from every field of the key.
func (k ConfigKey) ID() ConfigID {
	sum := sha256.Sum256(append([]byte(configIDDomain), k.canonical()...))
	return ConfigID(hex.EncodeToString(sum[:])[:configIDHexLen])
}

// Param reads one parameter back by name.
func (k ConfigKey) Param(name ParamName) (ParamValue, bool) {
	for _, param := range k.Params {
		if param.Name == name {
			return param.Value, true
		}
	}
	return "", false
}

func (k ConfigKey) String() string {
	return fmt.Sprintf("%s %s [%s]", k.Measurement, k.Label, k.ID())
}

// RequireDistinct is the one assertion every enumeration owes: two
// configurations under one identifier would share a file, and the second would
// be skipped as already done.
func RequireDistinct(keys []ConfigKey) error {
	seen := make(map[ConfigID]ConfigKey, len(keys))
	for _, key := range keys {
		if err := key.Validate(); err != nil {
			return err
		}
		id := key.ID()
		if previous, clash := seen[id]; clash {
			return fmt.Errorf("%w: %q and %q are both %s", ErrDuplicateConfig, previous.Label, key.Label, id)
		}
		seen[id] = key
	}
	return nil
}

// fileStem is the common prefix of every file of one configuration. The slugs
// are there for a human; the identifier is what makes the name unique — which
// is why they can be cut to a length that keeps every name of the journal
// within what a filesystem accepts, whatever the driver calls things. A name
// the filesystem refuses would surface only when the result is written, after
// the measurement it was supposed to keep.
func (k ConfigKey) fileStem() string {
	return fmt.Sprintf("%s-%s-%s", slug(string(k.Measurement)), slug(string(k.Label)), k.ID())
}

// maxSlugBytes bounds each slug so that the longest name of the journal — an
// attempt, "<slug>-<slug>-<id>.attempt-<token>.failed" — fits in
// maxFileNameBytes. Derived rather than written down, so a longer suffix or
// token shrinks the slugs instead of producing names the filesystem refuses.
const maxSlugBytes = (maxFileNameBytes - longestSuffixBytes - 2*len("-") - configIDHexLen) / 2

// longestSuffixBytes is what follows the stem in the longest file name.
const longestSuffixBytes = len(attemptInfix) + attemptTokenHexLen + len(attemptSuffix)

// resultName is where the completed result lives; it is created once.
func (k ConfigKey) resultName() string { return k.fileStem() + resultSuffix }

// attemptName is where ONE failed attempt lives. A fresh token per attempt is
// what keeps a failure from ever sharing a name with a success or with another
// failure, so no path of the journal opens an existing name for writing.
func (k ConfigKey) attemptName(token string) string {
	return k.fileStem() + attemptInfix + token + attemptSuffix
}

// attemptTokenHexLen is the length of a failed attempt's random token.
const attemptTokenHexLen = 16

// quarantineName marks the result as recorded while the sources changed;
// releaseName records the operator's decision to keep it anyway. Each is
// created once, like the result, so neither the mark nor the decision can be
// rewritten after the fact.
func (k ConfigKey) quarantineName() string { return k.fileStem() + quarantineSuffix }

func (k ConfigKey) releaseName() string { return k.fileStem() + releaseSuffix }

// verifiedName is where run vouches for the result it recorded, once its
// final source check passed. One name per run: a mark of another run is
// another file and never lifts this run's doubt.
func (k ConfigKey) verifiedName(run sweepRun) string {
	return k.fileStem() + verifiedInfix + string(run)
}

const (
	resultSuffix     = ".result"
	attemptInfix     = ".attempt-"
	attemptSuffix    = ".failed"
	quarantineSuffix = ".quarantined"
	releaseSuffix    = ".released"
	verifiedInfix    = ".verified-"
)

// slug renders text as a file-name component: everything outside [A-Za-z0-9]
// collapses into one dash, so a label may carry any character without deciding
// what a file is called.
func slug(text string) string {
	var out strings.Builder
	previousDash := false
	for _, symbol := range text {
		if isSlugSafe(symbol) {
			out.WriteRune(symbol)
			previousDash = false
			continue
		}
		if !previousDash {
			out.WriteByte('-')
			previousDash = true
		}
	}
	trimmed := strings.Trim(out.String(), "-")
	// The slug is ASCII, so cutting at a byte offset cannot split a character.
	return strings.TrimRight(trimmed[:min(len(trimmed), maxSlugBytes)], "-")
}

func isSlugSafe(symbol rune) bool {
	return (symbol >= 'a' && symbol <= 'z') || (symbol >= 'A' && symbol <= 'Z') || (symbol >= '0' && symbol <= '9')
}

// maxFileNameBytes is the longest file name the common filesystems accept
// (ext4, APFS, NTFS all stop at 255).
const maxFileNameBytes = 255

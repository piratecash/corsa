package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// pin_store_marker.go is the marker that tells a node's first start from the
// loss of the pin store it kept (docs/protocol/session_v2.md, "The pin store
// marker").
//
// Without it the two look the same — no store file — and the node would
// start on an empty store either way: after a deleted file, a restore that
// left the store behind, a changed port or CORSA_SECURE_SESSION_STORE_PATH,
// every peer that proved v2 could again be taken back to v1, and nothing
// would say so. The marker lives next to the identity, because the identity
// is what the pins belong to and what survives such changes: a new node is a
// new identity and starts without one.
//
// The marker is written only once a store file exists: after the store's
// first write, or at a start that finds a store the marker does not vouch
// for. Any crash therefore leaves one of three states, none of them a false
// alarm and none of them an empty start over a lost store:
//
//   - neither file: the first write never landed, nothing was promised (the
//     note that wrote it failed) — a first start;
//   - a store and no marker: the store landed, the marker did not — the next
//     start keeps the pins and writes the marker;
//   - both.
//
// The marker holds no secret: the format and the absolute path of the store
// it vouches for, which the refusal quotes when the store is looked for
// elsewhere. It is written through secretfile all the same — owner-only,
// never through a link — and audited like the store: a marker someone else
// can delete is a marker someone else can turn a loss into a first start
// with.

// pinStoreMarkerSuffix follows the identity file's name, as the desktop's
// preferences do: the marker shares the identity's port suffix and its
// CORSA_IDENTITY_PATH override.
const pinStoreMarkerSuffix = ".pin-store-marker.json"

const pinStoreMarkerFormat = 1

// pinStoreMarkerFile is the marker as it lies on disk.
type pinStoreMarkerFile struct {
	Format int `json:"format"`
	// Store is the absolute path of the store file the marker vouches for.
	Store string `json:"store"`
}

// pinStoreMarkerPath is where a node keeps its marker: next to its identity
// file. A node configured without one (in-process tests) keeps none.
func pinStoreMarkerPath(cfg config.Node) string {
	if cfg.IdentityPath == "" {
		return ""
	}
	return cfg.IdentityPath + pinStoreMarkerSuffix
}

// pinStoreMarkerState is what a start found in the marker's place.
type pinStoreMarkerState int

const (
	// markerNotKept: the node keeps no marker (no identity file path).
	markerNotKept pinStoreMarkerState = iota + 1
	// markerAbsent: nothing at the marker's name.
	markerAbsent
	// markerIntact: a marker this build reads.
	markerIntact
	// markerDamaged: something is at the marker's name, or could be, and
	// it cannot be read as a marker. It still says a store was kept.
	markerDamaged
)

// pinStoreMarker is the marker as a start found it.
type pinStoreMarker struct {
	state pinStoreMarkerState
	// store is the path the marker vouches for; set when intact only.
	store *string
}

// vouchesFor reports whether the marker records the store at storeAbs.
func (m pinStoreMarker) vouchesFor(storeAbs string) bool {
	return m.state == markerIntact && *m.store == storeAbs
}

// readPinStoreMarker reads the marker at path through a pinned handle of
// its directory. A marker that cannot be read is damaged, not absent: only
// a name that is not there is.
func readPinStoreMarker(path string) pinStoreMarker {
	if path == "" {
		return pinStoreMarker{state: markerNotKept}
	}
	dir, err := secretfile.Open(filepath.Dir(path))
	if errors.Is(err, fs.ErrNotExist) {
		return pinStoreMarker{state: markerAbsent}
	}
	if err != nil {
		return pinStoreMarker{state: markerDamaged}
	}
	defer func() { _ = dir.Close() }()
	raw, err := dir.ReadFile(filepath.Base(path))
	if errors.Is(err, fs.ErrNotExist) {
		return pinStoreMarker{state: markerAbsent}
	}
	if err != nil {
		return pinStoreMarker{state: markerDamaged}
	}
	var file pinStoreMarkerFile
	if err := json.Unmarshal(raw, &file); err != nil || file.Format != pinStoreMarkerFormat || file.Store == "" {
		return pinStoreMarker{state: markerDamaged}
	}
	return pinStoreMarker{state: markerIntact, store: &file.Store}
}

// pinStoreFileOnDisk reports the first of paths ("" skipped) that names an
// entry on disk. An entry that cannot be inspected counts as present: the
// question is asked to refuse, and a refusal is the safe answer.
func pinStoreFileOnDisk(paths ...string) (string, bool) {
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
			return path, true
		}
	}
	return "", false
}

// anotherStoreRemains reports whether recorded — the path the marker vouched
// for — still holds a store other than the one at current: a copy rather
// than a move. One file under two spellings of its path is not another
// store; an entry that cannot be compared counts as another one.
func anotherStoreRemains(recorded, current string) bool {
	old, err := os.Stat(recorded)
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil {
		return true
	}
	now, err := os.Stat(current)
	return err != nil || !os.SameFile(old, now)
}

// otherNodeMarkers lists, by file name, the markers of other nodes beside the
// marker at path: the nodes that share this directory under other identity
// files — with the default paths, the same data directory on other ports.
// It only reads the directory. A directory that cannot be listed yields
// none: the list feeds a warning, not a decision.
func otherNodeMarkers(path string) []string {
	if path == "" {
		return nil
	}
	dir, err := secretfile.Open(filepath.Dir(path))
	if err != nil {
		return nil
	}
	defer func() { _ = dir.Close() }()
	entries, err := dir.Entries()
	if err != nil {
		return nil
	}
	own := filepath.Base(path)
	var others []string
	for _, entry := range entries {
		name := entry.Name()
		if name != own && strings.HasSuffix(name, pinStoreMarkerSuffix) {
			others = append(others, name)
		}
	}
	return others
}

// encodePinStoreMarker is the marker vouching for the store at storeAbs.
func encodePinStoreMarker(storeAbs string) ([]byte, error) {
	raw, err := json.Marshal(pinStoreMarkerFile{Format: pinStoreMarkerFormat, Store: storeAbs})
	if err != nil {
		return nil, fmt.Errorf("encode the pin store marker: %w", err)
	}
	return raw, nil
}

// writePinStoreMarker lands raw at path through secretfile, after judging
// the location once more with audit: a directory created only now, or one
// that became unsafe since the start, is never written into unjudged.
func writePinStoreMarker(path string, raw []byte, audit func(string) error) error {
	if err := ensurePrivateDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	if err := audit(path); err != nil {
		return fmt.Errorf("the marker's location: %w", err)
	}
	return secretfile.WriteFile(path, raw)
}

// ensurePrivateDirectory creates the marker's directory when it does not
// exist — the identity's directory, normally there already; an SDK node
// given its key inline may have none — and makes the directory it created
// owner-only (mode 0700, on Windows a DACL for the user alone) instead of
// leaving it the rights it would inherit. The directory is empty in the
// moment between, and the audit that follows judges it and everything above.
func ensurePrivateDirectory(dir string) error {
	if _, err := os.Lstat(dir); !errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create the marker's directory: %w", secretfile.StripPath(err))
	}
	created, err := secretfile.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = created.Close() }()
	return created.Restrict()
}

// pinStoreFileState is what a start found of the store file.
type pinStoreFileState int

const (
	storeFileAbsent pinStoreFileState = iota + 1
	storeFileLoaded
	storeFileUnreadable
)

// pinStoreStartup is what a start does with what it found.
type pinStoreStartup int

const (
	// startupFirst: no store was kept — the marker comes with the first
	// write of the store.
	startupFirst pinStoreStartup = iota + 1
	// startupKept: the store loaded and the marker, if the node keeps one,
	// vouches for it.
	startupKept
	// startupMarkKept: the store loaded and the marker does not vouch for
	// it — absent (an earlier build, or a crash between the two writes),
	// damaged, or recording a path the operator has since moved the store
	// from. The store wins: the marker is written now.
	startupMarkKept
	// startupLost: the marker says a store was kept, and there is none.
	startupLost
	// startupUnreadable: a store that cannot be read.
	startupUnreadable
)

func decidePinStoreStartup(marker pinStoreMarker, file pinStoreFileState, storeAbs string) pinStoreStartup {
	switch {
	case file == storeFileUnreadable:
		return startupUnreadable
	case file == storeFileAbsent && (marker.state == markerAbsent || marker.state == markerNotKept):
		return startupFirst
	case file == storeFileAbsent:
		return startupLost
	case marker.state == markerNotKept || marker.vouchesFor(storeAbs):
		return startupKept
	default:
		return startupMarkKept
	}
}

// markerRewriteReason names, for the journal, why a start wrote the marker.
func markerRewriteReason(marker pinStoreMarker) string {
	switch marker.state {
	case markerAbsent:
		return "no_marker_for_existing_store"
	case markerDamaged:
		return "marker_damaged"
	default:
		return "store_moved"
	}
}

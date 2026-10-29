//go:build !windows

package node

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/secretfile"
)

// Ordinary proof protection needs namespace confirmation as well as atomic
// visibility. Global Dir.Write and the C3 transaction writer are unchanged.
func writeOrdinaryPinFile(path string, raw []byte) (err error) {
	parent := filepath.Dir(path)
	directory, err := secretfile.Open(parent)
	if err != nil {
		return &pinFileWriteError{commit: secretfile.DurableNotCommitted, cause: err}
	}
	commit := secretfile.DurableNotCommitted
	defer func() {
		failure := errors.Join(err, secretfile.StripPath(directory.Close()))
		if failure != nil {
			err = &pinFileWriteError{commit: commit, cause: failure}
		}
	}()
	// Confirm existing/new ancestor entries and filesystem support before mutation.
	if err := directory.SyncAncestors(parent); err != nil {
		return err
	}
	outcome, err := directory.DurableWrite(filepath.Base(path), raw)
	commit = outcome
	if err != nil {
		return err
	}
	if outcome != secretfile.DurableCommitted {
		return fmt.Errorf("ordinary pin replacement not confirmed: outcome %d", outcome)
	}
	// The leaf entry is confirmed, but the entire namespace remains unconfirmed
	// until the post-write route barrier also completes.
	commit = secretfile.DurableIndeterminate
	if err := directory.SyncAncestors(parent); err != nil {
		return err
	}
	commit = secretfile.DurableCommitted
	return nil
}

// Re-reading visible bytes after a process restart does not prove that a
// previous rename committed. Confirm the exact validated input, not a new
// projection that could alter historical metadata or recovery provenance.
func (s *secureSessionStore) confirmLoadedPinStore(raw []byte) error {
	// An existing visible marker must also cross this process's confirmation
	// barrier. A node explicitly configured without a marker keeps that policy.
	s.markerWritten = s.markerPath == ""
	return s.writeFile(raw)
}

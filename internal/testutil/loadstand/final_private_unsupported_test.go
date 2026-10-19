//go:build !linux && (!darwin || !cgo)

package loadstand

import (
	"errors"
	"os"
	"testing"
)

func TestFinalPrivateAuthorityIsExplicitlyUnsupported(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "slot")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := auditPrivateFinalFD(f, false); !errors.Is(err, ErrUnsupportedArtifact) {
		t.Fatal("unknown ACL authority accepted", err)
	}
}

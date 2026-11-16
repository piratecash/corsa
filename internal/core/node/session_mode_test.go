package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// TestTheConfiguredSessionModeIsWired ties the announced floor to the TLS-only
// listener and dialler, including the protocol-31 probe hint.
func TestTheConfiguredSessionModeIsWired(t *testing.T) {
	mode, err := configuredSessionMode()
	if err != nil {
		t.Fatalf("version constants: %v", err)
	}
	if mode != sessionv2.ModeV2Only {
		t.Fatalf("version constants select %s, want %s", mode, sessionv2.ModeV2Only)
	}
}

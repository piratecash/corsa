package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// TestTheConfiguredSessionModeIsWired is the guard that keeps the v1 → v2
// transition from being forgotten or half-done: a version bump that selects
// a session mode the node's accept and dial paths do not implement fails
// here, before it can ship. Wiring a mode means adding it to
// sessionModeWired together with the paths and their tests.
func TestTheConfiguredSessionModeIsWired(t *testing.T) {
	mode, err := configuredSessionMode()
	if err != nil {
		t.Fatalf("version constants: %v", err)
	}
	if !sessionModeWired[mode] {
		t.Fatalf("the version constants select session mode %s, which the node's accept and dial paths do not implement: wire it (docs/protocol/session_v2.md) before bumping the version", mode)
	}
}

// Every mode is either wired or waiting for a named step; a mode nobody
// accounts for would let a bump through with nothing behind it.
func TestEverySessionModeIsAccountedFor(t *testing.T) {
	pending := map[sessionv2.Mode]string{
		sessionv2.ModeV2Only: "the legacy path removed, then the S7 bump of MinimumProtocolVersion",
	}
	for _, mode := range []sessionv2.Mode{sessionv2.ModeLegacyOnly, sessionv2.ModeTransition, sessionv2.ModeV2Only} {
		_, isPending := pending[mode]
		if sessionModeWired[mode] == isPending {
			t.Errorf("mode %s: wired=%v, pending=%v — exactly one must hold", mode, sessionModeWired[mode], isPending)
		}
	}
}

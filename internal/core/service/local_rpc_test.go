package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// firstCheckPassesContext reports itself live for exactly one Err() call and
// cancelled from then on. The first call is the check before dispatch; any
// later one is a check AFTER the node has already acted.
type firstCheckPassesContext struct {
	context.Context
	checks int
}

func (f *firstCheckPassesContext) Err() error {
	f.checks++
	if f.checks <= 1 {
		return nil
	}
	return context.Canceled
}

func TestDispatchReturnsWhatHappenedEvenWhenTheContextEnds(t *testing.T) {
	// HandleLocalFrame is synchronous and cannot be interrupted, so a context
	// that ends during the call prevents nothing. Checking it again afterwards
	// threw the reply away: a send_message had already stored the message and
	// queued it, but the caller got context.Canceled and no message ID — and a
	// retry produced a SECOND message with a new one.
	dir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := node.NewService(config.Node{
		ListenAddress:  ":0",
		TrustStorePath: filepath.Join(dir, "trust.json"),
		PeersStatePath: filepath.Join(dir, "peers.json"),
	}, id, ebus.New())
	t.Cleanup(func() { svc.WaitBackground() })

	client := NewLocalRPCClient(NewAppInfo(config.App{Version: "test"}, config.Node{}, id), svc)
	ctx := &firstCheckPassesContext{Context: context.Background()}

	reply, err := client.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "ping"})
	if err != nil {
		t.Fatalf("LocalRequestFrameCtx() error = %v — the completed dispatch was discarded", err)
	}
	if reply.Type == "" {
		t.Fatal("LocalRequestFrameCtx() returned an empty frame")
	}
	// One check, and only one: the pre-dispatch one. A second consultation
	// would be a check after the node has already acted, which is what this
	// context is built to expose.
	if ctx.checks != 1 {
		t.Fatalf("the context was checked %d times, want exactly 1 (before dispatch)", ctx.checks)
	}
}

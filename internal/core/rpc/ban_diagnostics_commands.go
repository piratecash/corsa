package rpc

import (
	"context"
	"encoding/json"
)

// FreshBanDiagnosticsProvider supplies a snapshot read barrier for process
// measurements. Older adapters still serve their original, incomplete shape.
type FreshBanDiagnosticsProvider interface {
	FreshBanDiagnostics(context.Context) (json.RawMessage, error)
}

func readBanDiagnostics(ctx context.Context, provider ConnectionDiagnosticProvider) (json.RawMessage, error) {
	if fresh, ok := provider.(FreshBanDiagnosticsProvider); ok {
		return fresh.FreshBanDiagnostics(ctx)
	}
	return provider.ListBannedJSON()
}

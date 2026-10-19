package rpc

import "github.com/piratecash/corsa/internal/core/domain"

// RuntimeMetricsProvider is optional so older node adapters remain usable.
type RuntimeMetricsProvider interface {
	RuntimeMetrics() domain.RuntimeMetrics
}

// RegisterRuntimeMetricsCommand keeps the runtime-only diagnostic separate
// from the public memory dashboard command and its broader transport policy.
func RegisterRuntimeMetricsCommand(t *CommandTable, provider RuntimeMetricsProvider) {
	info := CommandInfo{
		Name:        "getRuntimeMetrics",
		Description: "Get Go process heap/GC/goroutines/GOMAXPROCS and cumulative CPU-second estimates; unsupported metrics are explicit",
		Category:    "system",
		Transport:   TransportLoopbackOnly,
	}
	if provider == nil {
		t.RegisterUnavailable(info)
		return
	}
	t.Register(info, func(req CommandRequest) CommandResponse {
		if response, done := ctxDone(req); done {
			return response
		}
		return jsonResponse(provider.RuntimeMetrics())
	})
}

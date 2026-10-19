package rpc

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
)

type DatagramRuntimeStateProvider interface {
	DatagramRuntimeState(context.Context) (domain.DatagramRuntimeState, error)
}

// The provider exists independently of whether this node enabled the plane.
func RegisterDatagramRuntimeStateCommand(t *CommandTable, provider DatagramRuntimeStateProvider) {
	info := CommandInfo{Name: "getDatagramRuntimeState", Description: "Get this Service's datagram constructor facts; not serving health or general configuration", Category: "datagram", Transport: TransportLoopbackOnly}
	if provider == nil {
		t.RegisterUnavailable(info)
		return
	}
	t.Register(info, func(req CommandRequest) CommandResponse {
		if response, done := ctxDone(req); done {
			return response
		}
		state, err := provider.DatagramRuntimeState(commandContext(req))
		if err != nil {
			return unavailableError(domain.ErrDatagramStateUnavailable)
		}
		if err := state.Validate(); err != nil {
			return unavailableError(err)
		}
		return jsonResponse(state)
	})
}

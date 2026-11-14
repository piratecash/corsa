package fullwitness

import (
	"encoding/json"
	"errors"

	"github.com/piratecash/corsa/internal/core/rpc"
)

// RegisterRouteWireCensus adds a read-only command to the fixture's original
// authenticated loopback table. Its window is fixed at startup.
func RegisterRouteWireCensus(table *rpc.CommandTable, name string, read func() (json.RawMessage, error)) error {
	if table == nil || name == "" || read == nil || table.Has(name) {
		return errors.New("invalid fixture registry witness command")
	}
	table.Register(rpc.CommandInfo{Name: name, Description: "Read the fixture registry census commitment", Category: "loadstand", Transport: rpc.TransportLoopbackOnly}, func(req rpc.CommandRequest) rpc.CommandResponse {
		if len(req.Args) != 0 {
			return rpc.CommandResponse{Error: errors.New("fixture registry witness takes no arguments"), ErrorKind: rpc.ErrValidation}
		}
		if req.Ctx == nil || req.Ctx.Err() == nil {
			raw, err := read()
			if err == nil && len(raw) != 0 && (req.Ctx == nil || req.Ctx.Err() == nil) {
				return rpc.CommandResponse{Data: raw}
			}
		}
		return rpc.CommandResponse{Error: errors.New("fixture registry witness unavailable"), ErrorKind: rpc.ErrUnavailable}
	})
	return nil
}

//go:build linux

package fullwitness

import (
	"encoding/json"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// PublicIdentity is a fixed public-only wire tuple. Keep field names/order equal
// to the consuming stand DTO; this package must not import its parent.
type PublicIdentity struct {
	Identity     domain.PeerIdentity
	PublicKey    string
	BoxPublicKey string
	BoxSignature string
}

// RegisterPlannedFullPublicIdentity registers only this measured Full fixture.
// The handler captures an
// immutable public encoding, never identity/private keys, and cannot sign or
// read files on request. Existing HTTP dispatch enforces LoopbackOnly plus auth.
func RegisterPlannedFullPublicIdentity(table *rpc.CommandTable, public PublicIdentity) error {
	// Startup-only: the caller owns this table before the server is started.
	// Has resolves aliases/case. This is not a concurrent register-if-absent API.
	if table.Has("loadstandFullPublicIdentity") {
		return errors.New("fixture public witness command already registered")
	}
	raw, err := json.Marshal(public)
	if err != nil {
		return err
	}
	encoded := string(raw)
	table.Register(rpc.CommandInfo{Name: "loadstandFullPublicIdentity", Description: "Read this fixture Full's signed public identity", Category: "loadstand", Transport: rpc.TransportLoopbackOnly}, func(req rpc.CommandRequest) rpc.CommandResponse {
		if len(req.Args) != 0 {
			return rpc.CommandResponse{Error: errors.New("fixture public witness takes no arguments"), ErrorKind: rpc.ErrValidation}
		}
		return rpc.CommandResponse{Data: json.RawMessage(encoded)}
	})
	return nil
}

// Package datagram implements the unguaranteed datagram transport layer
// specified in docs/refactoring/datagram-transport.md.
//
// Layering contract: this package MUST NOT import internal/core/node. Everything
// the layer needs from the node (route candidates, session capabilities, frame
// dispatch) arrives through interfaces supplied by the caller in a Config struct,
// mirroring how internal/core/service/filerouter is wired.
//
// Clock convention follows the rest of the project (routing.Table,
// node.rotatingHashDedup): an injectable `func() time.Time` field on the Config
// struct, defaulting to time.Now when nil. There is no Clock interface.
package datagram

package sdk

import "github.com/piratecash/corsa/internal/core/domain"

// ErrPinStoreRefused is the error New returns, wrapped, when the node must
// not start over its v2 pin store: the store, its marker next to the
// identity path or a directory above them can be changed by someone other
// than the process's user or root, or a store the marker says was kept is
// missing or unreadable. Match it with errors.Is; the message names the path
// and the fix. The SDK changes no permissions and deletes nothing for it.
var ErrPinStoreRefused = domain.ErrPinStoreRefused

//go:build windows

package node

import "github.com/piratecash/corsa/internal/core/secretfile"

// Preserve ordinary Windows operation while its namespace-durability backend
// remains UNVERIFIED. This retains atomic protected legacy writes; it does NOT
// claim the POSIX power-loss-confirmation repair. C3 durability stays unsupported.
func writeOrdinaryPinFile(path string, raw []byte) error {
	return secretfile.WriteFile(path, raw)
}

// Do not turn the intentionally unsupported strict Windows backend into a
// blanket ordinary-startup refusal. Native durability remains an open item.
func (s *secureSessionStore) confirmLoadedPinStore([]byte) error { return nil }

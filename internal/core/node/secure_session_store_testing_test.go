package node

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// loadSecureSessionStore is a store at path as a node without a marker opens
// it — the form the store's own tests use, where the marker is not the
// subject. path "" is a memory-only store.
func loadSecureSessionStore(path string, clock *trustedclock.Clock) *secureSessionStore {
	return loadSecureSessionStoreWith(path, clock, log.Logger)
}

// loadSecureSessionStoreWith is loadSecureSessionStore writing to logger: the
// load itself logs, so a test that pins those lines needs its logger in
// place before the first one.
func loadSecureSessionStoreWith(path string, clock *trustedclock.Clock, logger zerolog.Logger) *secureSessionStore {
	return openSecureSessionStore(secureSessionStoreOptions{
		path:   path,
		clock:  clock,
		logger: logger,
		audit:  secretfile.AuditLocation,
	})
}

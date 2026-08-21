//go:build !android && !sqlite_cgo

package storage

// SQLite driver selection: the pure-Go driver, used everywhere except Android.
//
// modernc.org/sqlite is a pure-Go transpile of SQLite: no cgo, painless
// cross-compilation for every node/SDK/headless target. Android is not in its
// supported-platform matrix, so that one target uses the cgo driver instead —
// see driver_cgo.go.
//
// The sqlite_cgo build tag selects the cgo driver instead, on any platform, so
// that the driver contract suite can run against both — see driver_cgo.go.
//
// The two files must stay semantically in sync: same journal mode, busy
// timeout and foreign-key enforcement, only the DSN syntax differs. The
// migration catalog and every repository statement are shared, so any
// behavioural divergence between the drivers is a bug, and the driver contract
// suite is what proves it.

import (
	_ "modernc.org/sqlite"
)

const (
	// sqliteDriverName is the database/sql driver name registered by
	// modernc.org/sqlite.
	sqliteDriverName = "sqlite"

	// sqliteDSNOptions configures a 5s busy timeout, foreign-key
	// enforcement and secure deletion on every pooled connection
	// (modernc.org/sqlite `_pragma` DSN syntax).
	//
	// secure_delete overwrites the content of a freed page instead of
	// merely unlinking it. This database holds chat history, and a
	// message the user deleted must not stay legible in slack space of
	// the file — the whole point of deleting it. The cost is one extra
	// write per freed page on DELETE, which is invisible next to the
	// per-message work around it. See docs/storage.md for the residual
	// -wal window and what closes it.
	//
	// journal_mode is deliberately NOT here. It is a property of the FILE,
	// not of a connection, and switching it needs an exclusive lock that
	// SQLite refuses immediately — without consulting busy_timeout — while
	// another connection holds the database. Setting it from the DSN made
	// every pooled connection retry that switch and made two processes
	// starting against the same fresh file race, with the loser failing to
	// start at all. ensureWALMode does it once, with a retry.
	sqliteDSNOptions = "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=secure_delete(1)"
)

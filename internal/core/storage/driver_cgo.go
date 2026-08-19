//go:build android || sqlite_cgo

package storage

// SQLite driver selection: the cgo driver, used on Android and wherever the
// sqlite_cgo tag is set.
//
// modernc.org/sqlite (used everywhere else — see driver_purego.go) compiles
// for GOOS=android only because Android also carries the linux build tag; the
// platform is absent from its support matrix and cross-build suite, so a
// successful compile says nothing about the Android ABI, WAL locking or crash
// recovery. mattn/go-sqlite3 links real SQLite through cgo, which the gogio
// build already runs through the Android NDK, so the cgo cost is paid on this
// target anyway.
//
// Must stay semantically in sync with driver_purego.go: same journal mode,
// busy timeout and foreign-key enforcement, only the DSN syntax differs.
//
// The file is NOT named driver_android.go any more, and that is the point: a
// _android.go suffix is itself a GOOS constraint, so no build tag could select
// this driver anywhere else. Android is the only target that ships it and no CI
// runner runs Android, which left the driver contract suite testing the pure-Go
// driver only — a divergence in DSN handling, WAL or transaction semantics
// would have been found in an Android build. With the sqlite_cgo tag the
// contract runs against this driver on an ordinary machine: `make test-cgo`.

import (
	_ "github.com/mattn/go-sqlite3"
)

const (
	// sqliteDriverName is the database/sql driver name registered by
	// mattn/go-sqlite3.
	sqliteDriverName = "sqlite3"

	// sqliteDSNOptions configures a 5s busy timeout and foreign-key
	// enforcement on every pooled connection (mattn/go-sqlite3
	// underscore-parameter DSN syntax). journal_mode is set once by
	// ensureWALMode instead — see driver_purego.go for why.
	sqliteDSNOptions = "?_busy_timeout=5000&_foreign_keys=1"
)

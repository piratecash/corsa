//go:build android

package chatlog

// SQLite driver selection, Android builds.
//
// modernc.org/sqlite (used everywhere else — see driver_default.go) does
// not compile for GOOS=android: its libc layer (modernc.org/libc) has no
// android port. mattn/go-sqlite3 links the real SQLite via cgo, which
// gogio compiles with the Android NDK toolchain anyway, so the cgo cost
// is already paid on this target. Must stay semantically in sync with
// driver_default.go: same WAL journal mode and 5s busy timeout, only the
// DSN syntax differs between the drivers.

import (
	_ "github.com/mattn/go-sqlite3"
)

const (
	// sqliteDriverName is the database/sql driver name registered by
	// mattn/go-sqlite3.
	sqliteDriverName = "sqlite3"
	// sqliteDSNOptions enables WAL journaling and a 5s busy timeout
	// (mattn/go-sqlite3 underscore-parameter DSN syntax).
	sqliteDSNOptions = "?_journal_mode=WAL&_busy_timeout=5000"
)

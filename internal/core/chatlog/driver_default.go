//go:build !android

package chatlog

// SQLite driver selection, non-Android builds.
//
// modernc.org/sqlite is a pure-Go transpile of SQLite: no cgo, painless
// cross-compilation. Its libc layer (modernc.org/libc) has no
// GOOS=android port, so Android uses the cgo driver instead — see
// driver_android.go. The two files must stay semantically in sync: same
// WAL journal mode and 5s busy timeout, only the DSN syntax differs
// between the drivers.

import (
	_ "modernc.org/sqlite"
)

const (
	// sqliteDriverName is the database/sql driver name registered by
	// modernc.org/sqlite.
	sqliteDriverName = "sqlite"
	// sqliteDSNOptions enables WAL journaling and a 5s busy timeout
	// (modernc.org/sqlite `_pragma` DSN syntax).
	sqliteDSNOptions = "?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)"
)

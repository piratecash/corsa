//go:build !windows && (!darwin || !cgo)

package main

// Platforms without a startup dialog report the error through the log.
func showStartupNotice(string) error { return nil }

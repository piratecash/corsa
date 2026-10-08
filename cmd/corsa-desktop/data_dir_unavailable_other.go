//go:build !windows

package main

// showDataDirUnavailable shows nothing outside Windows: a launch from a
// terminal already has the log line, and a console-less GUI start is the
// -H windowsgui case only.
func showDataDirUnavailable(string) error { return nil }

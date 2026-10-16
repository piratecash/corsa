//go:build !windows

package main

// showStartupNotice shows nothing outside Windows: a launch from a terminal
// already has the log line, and a console-less GUI start is the
// -H windowsgui case only.
func showStartupNotice(string) error { return nil }

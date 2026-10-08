//go:build windows

package main

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// showDataDirUnavailable shows a modal error box: a -H windowsgui binary
// has no console, and without it the app would just never appear.
func showDataDirUnavailable(text string) error {
	title, err := windows.UTF16PtrFromString(dataDirUnavailableTitle)
	if err != nil {
		return fmt.Errorf("encode notice title: %w", err)
	}
	body, err := windows.UTF16PtrFromString(text)
	if err != nil {
		return fmt.Errorf("encode notice text: %w", err)
	}
	if _, err := windows.MessageBox(0, body, title, windows.MB_OK|windows.MB_ICONERROR); err != nil {
		return fmt.Errorf("show notice: %w", err)
	}
	return nil
}

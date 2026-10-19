//go:build !unix

package main

import "errors"

func disableCoreDumps() error { return errors.New("private SDK child requires core dump suppression") }

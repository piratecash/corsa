// Package overlaysim is the measurement harness for M1 of
// docs/refactoring/dht/21-anonymity-transport.md §4.3.4″.6: is the structural
// half of the overlay connected, and at which Q-neighbour quota.
//
// The model it implements — how links are formed, which constraints of steps
// 16a/19 it honours, and what it simplifies in which direction — is
// docs/refactoring/dht/21-m1-connectivity-model.md. Read that before the code:
// a simulator whose assumptions are not written down produces illustrations,
// not measurements.
//
// # Why this file has no code
//
// Everything lives in _test.go files. The harness has no production consumer
// and must not acquire one: the mechanism it measures is direction 2 of blocker
// O5, taken for verification only, and O5 is open. A package that cannot be
// linked into a binary cannot become the speculative infrastructure this tree
// has already had to cut once.
//
// This file exists so that `go build ./...` does not trip over a directory with
// no non-test Go files. It deliberately declares nothing.
package overlaysim

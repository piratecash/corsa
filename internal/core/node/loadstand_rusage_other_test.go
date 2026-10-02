//go:build !unix

package node

import "errors"

// errLoadStandRusageUnavailable: the platform has no getrusage. The process
// sample keeps this as the reason its rusage reading is absent.
var errLoadStandRusageUnavailable = errors.New("getrusage is not available on this platform")

func readLoadStandRusage() (loadStandRusage, error) {
	return loadStandRusage{}, errLoadStandRusageUnavailable
}

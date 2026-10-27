//go:build !darwin && !linux

package secretfile

import "os"

func tryDirectoryFlock(*os.File) error { return ErrDirectoryLeaseUnsupported }

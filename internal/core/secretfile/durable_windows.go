//go:build windows

package secretfile

import "os"

// The existing protected handle-relative writer is retained. Strict durable
// replacement/removal needs a separately verified Windows namespace backend.
func (d *Dir) durablePlatformPreflight() error { return ErrDurabilityUnsupported }
func (d *Dir) syncDurableDirectory() error     { return ErrDurabilityUnsupported }

func (d *Dir) openRegularForRead(string) (*os.File, error) { return nil, ErrDurabilityUnsupported }

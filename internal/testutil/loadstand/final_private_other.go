//go:build !darwin && !linux

package loadstand

import "os"

func auditPrivateFinalFD(*os.File, bool) error { return ErrUnsupportedArtifact }

//go:build darwin && !cgo

package loadstand

import "os"

func auditFinalACL(*os.File) error { return ErrUnsupportedArtifact }

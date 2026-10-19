//go:build !darwin && !linux

package loadstand

import "os"

func duplicateFinalFD(*os.File) (*os.File, error) { return nil, ErrUnsupportedArtifact }

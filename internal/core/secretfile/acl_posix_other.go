//go:build !darwin && !windows

package secretfile

import (
	"io/fs"
	"os"
)

func auditPathACL(string, fs.FileInfo, LocationComponent) error         { return nil }
func auditDescriptorACL(*os.File, fs.FileInfo, LocationComponent) error { return nil }
func auditCreationDirectoryACL(*Dir) error                              { return nil }
func auditNewFileACL(*os.File) error                                    { return nil }

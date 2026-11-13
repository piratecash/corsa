package capture

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func captureFreeBytes(dir string) (uint64, error) {
	path, err := filepath.Abs(dir)
	if err != nil {
		return 0, err
	}
	if !strings.HasSuffix(path, `\`) {
		path += `\`
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var free uint64
	err = windows.GetDiskFreeSpaceEx(name, &free, nil, nil)
	return free, err
}

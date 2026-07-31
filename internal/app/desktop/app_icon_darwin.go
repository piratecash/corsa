//go:build darwin

package desktop

/*
#cgo LDFLAGS: -framework AppKit
#include <stddef.h>

void corsaSetApplicationIcon(const unsigned char *data, size_t length);
*/
import "C"

import (
	"sync"
	"unsafe"

	appicons "github.com/piratecash/corsa/assets/icons"
)

var applicationIconOnce sync.Once

// platformSetAppIcon supplies the Dock icon when Corsa is started as a raw
// executable (notably through `go run`), where no macOS application bundle is
// available to provide CFBundleIconFile.
func platformSetAppIcon() {
	applicationIconOnce.Do(func() {
		if len(appicons.AppIconPNG) == 0 {
			return
		}
		C.corsaSetApplicationIcon(
			(*C.uchar)(unsafe.Pointer(&appicons.AppIconPNG[0])),
			C.size_t(len(appicons.AppIconPNG)),
		)
	})
}

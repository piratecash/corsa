//go:build darwin && cgo

package main

/*
#cgo LDFLAGS: -framework CoreFoundation
#include <CoreFoundation/CoreFoundation.h>

static int showStartupAlert(const UInt8 *text, CFIndex length) {
    CFStringRef body = CFStringCreateWithBytes(
        kCFAllocatorDefault, text, length, kCFStringEncodingUTF8, false);
    if (body == NULL) {
        return -1;
    }
    SInt32 result = CFUserNotificationDisplayAlert(
        30, kCFUserNotificationStopAlertLevel, NULL, NULL, NULL,
        CFSTR("Corsa cannot start"), body, CFSTR("OK"), NULL, NULL, NULL);
    CFRelease(body);
    return result;
}
*/
import "C"

import (
	"fmt"
	"strings"
	"unsafe"
)

// showStartupNotice can run before the Gio event loop is started. A finite
// wait lets an unattended start finish after logging the original error.
func showStartupNotice(text string) error {
	text = strings.ToValidUTF8(text, "\uFFFD")
	if result := C.showStartupAlert((*C.UInt8)(unsafe.Pointer(unsafe.StringData(text))), C.CFIndex(len(text))); result != 0 {
		return fmt.Errorf("show macOS startup notice: status %d", int(result))
	}
	return nil
}

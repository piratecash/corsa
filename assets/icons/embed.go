// Package icons exposes application icon assets that must also be available
// at runtime, outside an installed application bundle.
package icons

import _ "embed"

// AppIconPNG is a compact application icon suitable for runtime platform APIs.
//
//go:embed png/app-icon-256.png
var AppIconPNG []byte

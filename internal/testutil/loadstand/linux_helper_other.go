//go:build !linux

package loadstand

import "context"

func RunLinuxHelper(context.Context) error { return ErrLinuxOwnerUnsupported }

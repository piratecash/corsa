//go:build !linux

package rpc

func registerPlatformBaselineClock(t *CommandTable, info CommandInfo) {
	t.RegisterUnavailable(info)
}

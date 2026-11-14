package rpc

func registerBaselineClock(t *CommandTable) {
	info := CommandInfo{
		Name: "getBaselineClock", Category: "system", Transport: TransportLoopbackOnly,
		Description: "Read local clock brackets and chrony tracking for the S6 baseline",
	}
	registerPlatformBaselineClock(t, info)
}

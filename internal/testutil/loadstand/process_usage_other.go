//go:build !linux && !darwin

package loadstand

import "os"

func ownedProcessUsage(*os.ProcessState) ProcessMetric[ProcessFinalUsage] {
	return unavailableProcessMetric[ProcessFinalUsage](ProcessPlatformUnsupported)
}

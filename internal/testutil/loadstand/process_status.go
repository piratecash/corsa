package loadstand

import "os"

func captureOwnedExit(state *os.ProcessState, waitFailed bool) ProcessExitResult {
	out := ProcessExitResult{WaitFailed: waitFailed, Usage: FinalUsageObservation{Scope: OwnedCommandUsage, State: ProcessMetricUnavailable, Reason: ProcessMissing}}
	if state == nil {
		return out
	}
	out.Code = ProcessExitCode(state.ExitCode())
	out.Signal = ownedWaitSignal(state)
	out.Kind = WaitNormal
	if out.Signal != 0 {
		out.Kind = WaitSignaled
	}
	usage := ownedProcessUsage(state)
	out.Usage.State = usage.State
	out.Usage.Reason = usage.Reason
	if usage.Value != nil {
		out.Usage.Value = *usage.Value
	}
	return out
}

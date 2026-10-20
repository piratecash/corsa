package loadstand

import (
	"os/exec"
	"runtime"
	"testing"
	"time"
)

// These are common startup-owner controls using a real owned dummy command,
// not Linux namespace/helper/ACL or parent-death evidence.
func linuxTestClaimProcess(t *testing.T) *Process {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("owned dummy command control requires the supported POSIX test hosts")
	}
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	rpc, err := NewRPCClient(RPCOptions{Address: "127.0.0.1:1", Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sleep", "30")
	if err := command.Start(); err != nil {
		rpc.Close()
		t.Fatal(err)
	}
	process := startOwnedProcess(t.Context(), command, rpc, nil, Provenance{}, nil, false)
	t.Cleanup(func() {
		process.terminal.submit(StopLifetime, true, time.Now().Add(time.Second), false)
		select {
		case <-process.Done():
		case <-time.After(time.Second):
			t.Error("owned dummy command did not join")
		}
	})
	return process
}
func TestLinuxStartObservedStopBeforeClaimDoesNotStart(t *testing.T) {
	for name, id := range map[string]LinuxRequestID{"eof": 0, "stop": 2} {
		t.Run(name, func(t *testing.T) {
			startup := newLinuxStartClaim()
			preflight, resume := make(chan struct{}), make(chan struct{})
			outcome := make(chan bool, 1)
			go func() {
				close(preflight)
				<-resume
				outcome <- startup.claim()
			}()
			<-preflight
			startup.observeStop(id, time.Now().Add(time.Second), false)
			close(resume)
			if <-outcome {
				process := linuxTestClaimProcess(t)
				if err := startup.publish(process); err != nil {
					t.Fatal(err)
				}
				t.Fatal("observed Stop before launch claim still started a real command")
			}
			if state := startup.snapshot(); state.phase != linuxStartDenied || !state.stopped || state.stopID != id {
				t.Fatal("no-target startup decision lost its exact Stop observation", state)
			}
		})
	}
}
func TestLinuxStartClaimWinsStopBeforePublicationStopsActualProcess(t *testing.T) {
	startup := newLinuxStartClaim()
	if !startup.claim() {
		t.Fatal("fresh startup claim refused")
	}
	process := linuxTestClaimProcess(t)
	// The actual Start outcome exists but has not been published to the latch.
	startup.observeStop(2, time.Now().Add(time.Second), false)
	if err := startup.publish(process); err != nil {
		t.Fatal(err)
	}
	select {
	case <-process.Done():
	case <-time.After(300 * time.Millisecond):
		t.Fatal("latched Stop was lost at actual Process publication")
	}
	first, err := process.ExitResult()
	if err != nil || !first.Signals.TermAttempted || first.Kind == WaitUnknown {
		t.Fatal("publication did not retain actual terminal/wait facts", err, first)
	}
	second, err := process.ExitResult()
	if err != nil || first != second || startup.claim() {
		t.Fatal("replayed startup altered actual terminal owner", err)
	}
}
func TestLinuxStartStopCoalescingKeepsEarliestDeadlineAndDefect(t *testing.T) {
	startup := newLinuxStartClaim()
	deadline := time.Now().Add(time.Second)
	startup.observeStop(2, deadline, false)
	startup.observeStop(3, deadline.Add(time.Second), false)
	startup.observeStop(0, deadline.Add(-time.Millisecond), false)
	state := startup.snapshot()
	if state.stopID != 2 || !state.defect || !state.deadline.Equal(deadline.Add(-time.Millisecond)) {
		t.Fatal("coalesced Stop erased correlation, defect or deadline", state)
	}
}

func TestLinuxStartExpiredStopRetainsActualSignalFacts(t *testing.T) {
	startup := newLinuxStartClaim()
	if !startup.claim() {
		t.Fatal("fresh startup claim refused")
	}
	process := linuxTestClaimProcess(t)
	deadline := time.Now().Add(-time.Second)
	startup.observeStop(2, deadline, true)
	if err := startup.publish(process); err != nil {
		t.Fatal(err)
	}
	select {
	case <-process.Done():
	case <-time.After(time.Second):
		t.Fatal("expired Stop was not applied to the owned command")
	}
	result, err := process.ExitResult()
	if err != nil || !result.Signals.ForceRequested || result.Kind == WaitUnknown {
		t.Fatal("expired deadline lost actual wait facts", err, result)
	}
	if result.Signals.ForceAttempted == (result.Signals.ForceResult == SignalNotAttempted) {
		t.Fatal("requested force was mislabeled as an actual syscall result", result.Signals)
	}
	if state := startup.snapshot(); !state.defect || !state.deadline.Equal(deadline) {
		t.Fatal("publication cleared prior defect or deadline", state)
	}
}
func TestLinuxStartFailedClaimCannotBeReused(t *testing.T) {
	startup := newLinuxStartClaim()
	if !startup.claim() {
		t.Fatal("fresh startup claim refused")
	}
	startup.failed()
	if startup.claim() || startup.publish(nil) == nil || startup.snapshot().phase != linuxStartFailed {
		t.Fatal("failed start minted another launch or Process")
	}
}

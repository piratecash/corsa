//go:build linux

package loadstand

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

// This is the ORIGINAL managed control stream's existing seven cases,
// including its runtime.Stack IO-wait witness and cancel/Close join.
// No native authorization/env, namespace, helper executable or target exists.
func TestLinuxAdversarialManagedControlStream(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root SO_PEERCRED fixture required; no capabilities or namespace admission needed")
	}
	linuxAdversarialStreamChecks(t)
}

// Qualify the actual original helper→sampler mutex frames on the current Go.
// The sampler remains uninitialised: after release it refuses BEFORE opening
// any process/proc source, never supplies a synthetic process observation.
func TestLinuxAdversarialGo1273OriginalBlockedSampleStack(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	caller, work := linuxTestPacketPair(t)
	sampler := &LinuxProcessSampler{}
	sampler.mu.Lock()
	held := true
	release := func() {
		if held {
			held = false
			sampler.mu.Unlock()
		}
	}
	workContext, cancelWork := context.WithCancel(ctx)
	original := &linuxHelperRuntime{
		work: work, init: linuxInit{Nonce: LaunchNonce{1}},
		sampler: sampler, workCtx: workContext, cancelWork: cancelWork,
		startup: newLinuxStartClaim(), workDone: make(chan struct{}),
	}
	// This join is after sampleWork's own deferred original workDone close.
	joined := make(chan struct{})
	t.Cleanup(func() {
		release()
		cancelWork()
		if err := errors.Join(caller.close(), work.close()); err != nil {
			t.Error("original packet close failed", err)
		}
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("original helper sampleWork did not join")
		}
	})
	go func() {
		original.sampleWork()
		close(joined)
	}()
	request, err := encodeLinuxPacket(linuxSample, 2, 1,
		linuxSampleRequest{Nonce: original.init.Nonce, BudgetNanos: int64(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	if err = caller.write(request, deadline); err != nil {
		t.Fatal("original sample packet refused", err)
	}
	// Same function and required frames as the future real native blocked
	// sample control. It inspects real runtime.Stack, never a supplied stack.
	if err = linuxAdversarialBlockedStack(ctx); err != nil {
		t.Fatal("original helper/sampler mutex wait not actually witnessed", err)
	}
	if workContext.Err() != nil {
		t.Fatal("work expired before actual blocked-stack witness")
	}
	release()
	packet, err := caller.read(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("original sample worker reply did not arrive", err)
	}
	reply, err := decodeLinuxBody[linuxSampleReply](packet)
	if err != nil || packet.kind != linuxSampleResult || packet.id != 2 || packet.ordinal != 1 ||
		reply.Nonce != original.init.Nonce || reply.Code != linuxResultUnavailable ||
		!reflect.DeepEqual(reply.Sample, LinuxProcessSample{}) {
		t.Fatal("uninitialised original sampler invented a reading or changed correlation", err)
	}
	if original.startup.snapshot().stopped {
		t.Fatal("ordinary unavailable sample was relabelled protocol/stop failure")
	}
	// Cleanup cancels and closes the exact original endpoints, then joins the
	// actual worker. No target/helper Wait, NS restoration or kernel claim.
}

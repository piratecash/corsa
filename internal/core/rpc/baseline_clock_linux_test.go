package rpc

import (
	"bytes"
	"io"
	"testing"
)

func TestBaselineClockOutputIsBoundedThroughCopy(t *testing.T) {
	var output baselineClockOutput
	if _, err := io.Copy(&output, bytes.NewReader(make([]byte, 4097))); err == nil {
		t.Fatal("accepted oversized chrony output")
	}
	if output.buffer.Len() > 4096 {
		t.Fatal("retained oversized chrony output")
	}
}

func TestBaselineClockRegistration(t *testing.T) {
	table := NewCommandTable()
	registerBaselineClock(table)
	policy, ok := table.TransportPolicyFor("getBaselineClock")
	if !ok || policy != TransportLoopbackOnly {
		t.Fatalf("transport = %v, present = %v", policy, ok)
	}
	reply := table.Execute(CommandRequest{Name: "getBaselineClock", Args: map[string]interface{}{"path": "/tmp/input"}})
	if reply.Error == nil || reply.ErrorKind != ErrValidation {
		t.Fatal("accepted caller-supplied arguments")
	}
}

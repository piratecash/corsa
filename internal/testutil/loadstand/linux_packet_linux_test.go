//go:build linux

package loadstand

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func linuxTestClose(t *testing.T, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, os.ErrClosed) {
		t.Error("owned test close failed", err)
	}
}
func linuxTestPacketPair(t *testing.T) (*linuxPacketConn, *linuxPacketConn) {
	t.Helper()
	first, original, err := newLinuxPacketPair()
	if err != nil {
		t.Fatal(err)
	}
	second, err := checkedLinuxPacketFile(original)
	if err != nil {
		_ = first.close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := errors.Join(first.close(), second.close()); err != nil {
			t.Error(err)
		}
	})
	return first, second
}
func linuxTestFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
func TestLinuxPacketManagedCloseInterruptsParkedRead(t *testing.T) {
	first, second := linuxTestPacketPair(t)
	read := make(chan error, 1)
	go func() { _, err := first.read(time.Time{}); read <- err }()
	time.Sleep(10 * time.Millisecond)
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("closed read succeeded")
		}
	case <-time.After(250 * time.Millisecond):
		_ = second.close()
		<-read
		t.Fatal("managed Close failed to evict parked packet read")
	}
}
func TestLinuxPacketBootstrapAliasesDoNotMaskEOF(t *testing.T) {
	first, second := linuxTestPacketPair(t)
	if err := second.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.read(time.Now().Add(time.Second)); !errors.Is(err, io.EOF) {
		t.Fatal("bootstrap alias masked EOF", err)
	}
}
func TestLinuxPacketSCMRightsDisposedForEveryVerdict(t *testing.T) {
	for _, scenario := range []string{"valid", "malformed", "payload-truncated", "rights-truncated"} {
		t.Run(scenario, func(t *testing.T) {
			first, second := linuxTestPacketPair(t)
			owned, err := os.Open("/dev/null")
			if err != nil {
				t.Fatal(err)
			}
			defer linuxTestClose(t, owned)
			packet, err := encodeLinuxPacket(linuxSample, 2, 1, linuxSampleRequest{Nonce: LaunchNonce{1}, BudgetNanos: 1})
			if err != nil {
				t.Fatal(err)
			}
			count := 3
			switch scenario {
			case "malformed":
				packet = []byte("invalid")
			case "payload-truncated":
				packet = make([]byte, linuxPacketLimit+1)
			case "rights-truncated":
				count = 32
			}
			fds := make([]int, count)
			for index := range fds {
				fds[index] = int(owned.Fd())
			}
			before := linuxTestFDCount(t)
			if _, _, err := second.conn.WriteMsgUnix(packet, unix.UnixRights(fds...), nil); err != nil {
				t.Fatal(err)
			}
			if _, err := first.read(time.Now().Add(time.Second)); !errors.Is(err, ErrLinuxOwnerProtocol) {
				t.Fatal("rights packet accepted", err)
			}
			if after := linuxTestFDCount(t); after != before {
				t.Fatalf("received rights leaked: before=%d after=%d", before, after)
			}
		})
	}
}
func TestLinuxPacketReceivedRightsAreCloexec(t *testing.T) {
	first, second := linuxTestPacketPair(t)
	owned, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	defer linuxTestClose(t, owned)
	if _, _, err := second.conn.WriteMsgUnix([]byte{1}, unix.UnixRights(int(owned.Fd())), nil); err != nil {
		t.Fatal(err)
	}
	var payload [1]byte
	oob := make([]byte, unix.CmsgSpace(4))
	_, n, _, _, err := first.conn.ReadMsgUnix(payload[:], oob)
	if err != nil {
		t.Fatal(err)
	}
	if err := disposeLinuxAncillary(oob[:n]); err != nil {
		t.Fatal("received FD was not CLOEXEC or cleanup failed", err)
	}
}
func TestLinuxPacketBackpressureStopAndManagedClose(t *testing.T) {
	first, second := linuxTestPacketPair(t)
	packet, err := encodeLinuxPacket(linuxStop, 2, 1, linuxStopRequest{Nonce: LaunchNonce{1}, BudgetNanos: 1})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if first.conn.SetWriteDeadline(time.Now().Add(5*time.Millisecond)) != nil {
			t.Fatal("deadline failed")
		}
		if _, _, err := first.conn.WriteMsgUnix(packet, nil, nil); err != nil {
			break
		}
	}
	delivery, err := first.sendStop(packet)
	if err != nil || delivery != LinuxStopNoPacket {
		t.Fatal("backpressured Stop was not one-shot NoPacket", delivery, err)
	}
	// Drain accepted packets and prove EOF after the one-way shutdown.
	for {
		if _, err := second.read(time.Now().Add(time.Second)); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
func TestLinuxPacketManagedCloseInterruptsBackpressuredWrite(t *testing.T) {
	first, second := linuxTestPacketPair(t)
	packet, err := encodeLinuxPacket(linuxSample, 2, 1, linuxSampleRequest{Nonce: LaunchNonce{1}, BudgetNanos: 1})
	if err != nil {
		t.Fatal(err)
	}
	for first.write(packet, time.Now().Add(3*time.Millisecond)) == nil {
	}
	blocked := make(chan error, 1)
	go func() { blocked <- first.write(packet, time.Now().Add(time.Minute)) }()
	time.Sleep(10 * time.Millisecond)
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("backpressured closed write succeeded")
		}
	case <-time.After(250 * time.Millisecond):
		_ = second.close()
		<-blocked
		t.Fatal("managed Close did not evict backpressured writer")
	}
}

// Synthetic wire values qualify the closed Sample reply schema, not OS custody.
func TestLinuxSampleReadElapsedProtocol(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ns    int64
		known bool
	}{{"known-zero", 0, true}, {"positive", 17, true}, {"unavailable", 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			original := linuxSampleReply{Nonce: LaunchNonce{1}, Code: linuxResultReady, Sample: LinuxProcessSample{ReadElapsedNS: tc.ns, ReadElapsedKnown: tc.known, ExecutableReadBrackets: ProcessExecutableReadBrackets{First: ProcessExecutableReadBracket{ElapsedNS: tc.ns, ElapsedKnown: tc.known, VerifiedBytes: 19}, Last: ProcessExecutableReadBracket{ElapsedNS: tc.ns, ElapsedKnown: tc.known, VerifiedBytes: 23}}}}
			encoded, err := encodeLinuxPacket(linuxSampleResult, 1, 1, original)
			if err != nil || len(encoded) > linuxPacketLimit {
				t.Fatal("original packet bound", err, len(encoded))
			}
			packet, err := decodeLinuxPacket(encoded)
			if err != nil {
				t.Fatal(err)
			}
			reply, err := decodeLinuxBody[linuxSampleReply](packet)
			if err != nil || reply.Sample.ReadElapsedNS != tc.ns || reply.Sample.ReadElapsedKnown != tc.known || reply.Sample.ExecutableReadBrackets != original.Sample.ExecutableReadBrackets {
				t.Fatal("elapsed wire scalar", err, reply)
			}
		})
	}
	t.Run("malformed", func(t *testing.T) {
		oversize := linuxSampleReply{Nonce: LaunchNonce{1}, Code: linuxResultReady, Sample: LinuxProcessSample{ReadElapsedKnown: true}}
		oversize.Sample.Descriptors.Reason = ProcessUnavailableReason(string(bytes.Repeat([]byte{'x'}, linuxPacketLimit)))
		if encoded, err := encodeLinuxPacket(linuxSampleResult, 1, 1, oversize); !errors.Is(err, ErrLinuxOwnerProtocol) || encoded != nil {
			t.Fatal("oversize typed elapsed Sample reply accepted", len(encoded), err)
		}
		raw, err := encodeLinuxPacket(linuxSampleResult, 1, 1, linuxSampleReply{Nonce: LaunchNonce{1}, Code: linuxResultReady, Sample: LinuxProcessSample{ReadElapsedKnown: true, ExecutableReadBrackets: ProcessExecutableReadBrackets{First: ProcessExecutableReadBracket{ElapsedNS: 3, ElapsedKnown: true, VerifiedBytes: 111}, Last: ProcessExecutableReadBracket{ElapsedNS: 7, ElapsedKnown: true, VerifiedBytes: 222}}}})
		if err != nil {
			t.Fatal(err)
		}
		p, err := decodeLinuxPacket(raw)
		if err != nil {
			t.Fatal(err)
		}

		// Exact pre-stream DTO shape: the new pair is the sole schema difference.
		type oldProcessSample struct {
			Provenance       LinuxProcessProvenance           `json:"provenance"`
			ReadStartedAt    time.Time                        `json:"read_started_at"`
			ReadCompletedAt  time.Time                        `json:"read_completed_at"`
			ReadElapsedNS    int64                            `json:"read_elapsed_ns"`
			ReadElapsedKnown bool                             `json:"read_elapsed_known"`
			CPU              ProcessCPU                       `json:"cpu"`
			Memory           ProcessMemory                    `json:"memory"`
			Descriptors      ProcessMetric[ProcessFDCount]    `json:"descriptors"`
			Scheduler        ProcessScheduler                 `json:"scheduler"`
			FinalWait4       ProcessMetric[ProcessFinalUsage] `json:"final_wait4"`
		}
		type oldSampleReply struct {
			Nonce  LaunchNonce
			Code   linuxResultCode
			Sample oldProcessSample
		}
		if _, err := decodeLinuxBody[oldSampleReply](p); !errors.Is(err, ErrLinuxOwnerProtocol) {
			t.Fatal("new reply accepted by old decoder", err)
		}
		tooLarge := make([]byte, linuxPacketLimit+1)
		copy(tooLarge, raw)
		binary.BigEndian.PutUint32(tooLarge[8:12], uint32(len(tooLarge)-linuxHeaderSize))
		if _, err := decodeLinuxPacket(tooLarge); !errors.Is(err, ErrLinuxOwnerProtocol) {
			t.Fatal("original16KiB decode bound weakened", err)
		}
		negative := p
		negative.body = bytes.Replace(p.body, []byte(`"elapsed_ns":3`), []byte(`"elapsed_ns":-1`), 1)
		if reply, err := decodeLinuxBody[linuxSampleReply](negative); err != nil || reply.Sample.ExecutableReadBrackets.First.ElapsedNS != -1 {
			t.Fatal("typed negative diagnostic became protocol authority", err)
		}
		for _, change := range []struct{ from, to string }{
			{`"read_elapsed_ns":0,`, ``},
			{`"read_elapsed_known":true,`, ``},
			{`"read_elapsed_ns":0`, `"read_elapsed_ns":0,"read_elapsed_ns":0`},
			{`"read_elapsed_ns":0`, `"Read_Elapsed_NS":0`},
			{`"read_elapsed_ns":0`, `"read_elapsed_ns":null`},
			{`"read_elapsed_known":true`, `"read_elapsed_known":null`},
			{`"read_elapsed_ns":0`, `"read_elapsed_ns":9223372036854775808`},
			{`"executable_read_brackets":{"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111},"last":{"elapsed_ns":7,"elapsed_known":true,"verified_bytes":222}},`, ``},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111},`, ``},
			{`,"last":{"elapsed_ns":7,"elapsed_known":true,"verified_bytes":222}`, ``},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":null`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"First":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_known":true,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_known":true}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":null}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_known":null,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":null,"elapsed_known":true,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":9223372036854775808,"elapsed_known":true,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":9223372036854775808}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":[]`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3.5,"elapsed_known":true,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_known":1,"verified_bytes":111}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111,"cpu":0}`},
			{`"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`, `"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111},"first":{"elapsed_ns":3,"elapsed_known":true,"verified_bytes":111}`},
		} {
			if bytes.Count(p.body, []byte(change.from)) != 1 {
				t.Fatal("closed wire control not applicable", change.from)
			}
			bad := p
			bad.body = bytes.Replace(p.body, []byte(change.from), []byte(change.to), 1)
			if _, err := decodeLinuxBody[linuxSampleReply](bad); !errors.Is(err, ErrLinuxOwnerProtocol) {
				t.Fatal("malformed elapsed schema accepted", change, err)
			}
		}
	})
}

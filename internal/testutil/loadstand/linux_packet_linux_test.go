//go:build linux

package loadstand

import (
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

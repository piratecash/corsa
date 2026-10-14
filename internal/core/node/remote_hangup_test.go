package node

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// promptWaitBound bounds a test's wait for something the code under test does
// at once — a refusal, a close. It only decides how a missing reaction fails,
// never whether a working one passes.
const promptWaitBound = 2 * time.Second

// expectHangUp fails t unless the remote end of a pipe sees its peer close
// within promptWaitBound, as opposed to the read timing out on a live
// connection.
func expectHangUp(t *testing.T, remote net.Conn) {
	t.Helper()
	if err := remote.SetReadDeadline(time.Now().Add(promptWaitBound)); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("set read deadline: %v", err)
	}
	_, err := remote.Read(make([]byte, 1))
	if err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the remote end saw no hang-up (read = %v): the socket is still open", err)
	}
}

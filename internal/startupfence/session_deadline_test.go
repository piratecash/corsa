package startupfence

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

func TestPrivateSessionRequestCancellationCannotLeaveCleanupBlocked(t *testing.T) {
	for _, operation := range []string{"begin", "receipt"} {
		t.Run(operation, func(t *testing.T) {
			directory, err := os.MkdirTemp("/tmp", "fence-io-")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(directory) }()
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: directory + "/channel", Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			accepted := make(chan *net.UnixConn, 1)
			go func() {
				c, e := listener.AcceptUnix()
				if e == nil {
					accepted <- c
				}
			}()
			connection, err := net.DialUnix("unix", nil, listener.Addr().(*net.UnixAddr))
			if err != nil {
				t.Fatal(err)
			}
			remote := <-accepted
			defer func() { _ = remote.Close() }()
			file, err := connection.File()
			if err != nil {
				t.Fatal(err)
			}
			_ = connection.Close()
			defer func() { _ = file.Close() }()
			// A protocol fixture tests bounded I/O only. This private field does not
			// manufacture a production startup capability outside this package.
			session := &SupervisedSession{file: file, reader: bufio.NewReader(file), encoder: json.NewEncoder(file), verified: true}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if operation == "begin" {
					_, err := session.BeginRemoval(ctx, Binding{})
					done <- err
				} else {
					done <- session.RecordReceipt(ctx, Receipt{})
				}
			}()
			if _, err := bufio.NewReader(remote).ReadString('\n'); err != nil {
				t.Fatal(err)
			}
			cancel() // The request is on the wire, but no response ever arrives.
			select {
			case err := <-done:
				if !errors.Is(err, ErrUnavailable) && !errors.Is(err, context.Canceled) {
					t.Fatalf("unexpected cancellation outcome: %v", err)
				}
			case <-time.After(250 * time.Millisecond):
				_ = file.Close()
				<-done
				t.Fatal("cancelled private request left local cleanup blocked")
			}
		})
	}
}

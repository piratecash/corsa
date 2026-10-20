package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestLinuxRPCActualTransportCarriesFiniteDialBound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]bool{"ready": true}); err != nil {
			t.Error("owned response failed", err)
		}
	}))
	defer server.Close()
	observed := make(chan time.Duration, 1)
	dial := boundedLinuxRPCDial(func(ctx context.Context, network, address string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, ErrLinuxOwnerBound
		}
		observed <- time.Until(deadline)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	})
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress(server.Listener.Addr().String()), Credentials: credentials, DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var reply struct {
		Ready bool `json:"ready"`
	}
	if err := client.Call(ctx, "probe", nil, &reply); err != nil {
		t.Fatal("actual HTTP Transport rejected an otherwise valid bounded RPC", err)
	}
	if remaining := <-observed; !reply.Ready || remaining <= 0 || remaining > time.Second {
		t.Fatal("actual dial did not reuse the existing finite RPC bound", remaining)
	}
}

func TestLinuxRPCDirectParentBoundsAndCancellation(t *testing.T) {
	short, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	deadline, _ := short.Deadline()
	_, err := boundedLinuxRPCDial(func(ctx context.Context, _, _ string) (net.Conn, error) {
		actual, ok := ctx.Deadline()
		if !ok || !actual.Equal(deadline) {
			t.Error("shorter parent deadline was extended")
		}
		return nil, ErrLinuxOwnerProtocol
	})(short, "tcp4", "127.0.0.1:1")
	if !errors.Is(err, ErrLinuxOwnerProtocol) {
		t.Fatal(err)
	}
	cancel()
	called := false
	_, err = boundedLinuxRPCDial(func(context.Context, string, string) (net.Conn, error) {
		called = true
		return nil, nil
	})(short, "tcp4", "127.0.0.1:1")
	if !errors.Is(err, context.Canceled) || called {
		t.Fatal("cancelled parent admitted a dial", err)
	}
}

func TestLinuxRPCTransportParentCancellationJoinsDial(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, done := make(chan struct{}), make(chan error, 1)
	dial := boundedLinuxRPCDial(func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	go func() { _, err := dial(parent, "tcp4", "127.0.0.1:1"); done <- err }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("transport parent cancellation lost")
	}
}

func TestLinuxRPCRequestReturnDoesNotInventHeldDialJoin(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "request_cancel", true: "client_close"}[closeClient], func(t *testing.T) {
			started, release, dialDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer func() { releaseOnce.Do(func() { close(release) }); <-dialDone }()
			dial := boundedLinuxRPCDial(func(ctx context.Context, _, _ string) (net.Conn, error) {
				defer close(dialDone)
				close(started)
				<-release // deliberately held owned I/O; caller return is not join
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return nil, ErrLinuxOwnerProtocol
			})
			credentials, err := NewCredentials()
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewRPCClient(RPCOptions{Address: "127.0.0.1:1", Credentials: credentials, DialContext: dial})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			callDone := make(chan error, 1)
			go func() { var reply any; callDone <- client.Call(ctx, "probe", nil, &reply) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("actual transport did not start dial")
			}
			if closeClient {
				client.Close()
			} else {
				cancel()
			}
			select {
			case err := <-callDone:
				if closeClient && !errors.Is(err, ErrRPCClosed) || !closeClient && !errors.Is(err, context.Canceled) {
					t.Fatal("caller cancellation was lost", err)
				}
			case <-time.After(time.Second):
				t.Fatal("caller waited for held dial")
			}
			select {
			case <-dialDone:
				t.Fatal("caller return fabricated held dial join")
			default:
			}
			releaseOnce.Do(func() { close(release) })
		})
	}
}

func TestLinuxRPCAdapterCancelDoesNotCloseTransferredConnection(t *testing.T) {
	left, right := net.Pipe()
	defer func() {
		if err := left.Close(); err != nil {
			t.Error(err)
		}
		if err := right.Close(); err != nil {
			t.Error(err)
		}
	}()
	var observed context.Context
	conn, err := boundedLinuxRPCDial(func(ctx context.Context, _, _ string) (net.Conn, error) { observed = ctx; return left, nil })(t.Context(), "tcp4", "127.0.0.1:1")
	if err != nil || conn != left || observed.Err() != context.Canceled {
		t.Fatal("adapter did not close its context at transfer", err)
	}
	if err := right.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal("adapter revoked an already transferred connection", err)
	}
	if err := left.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal("adapter revoked an already transferred connection", err)
	}
	written := make(chan error, 1)
	go func() { _, err := right.Write([]byte{1}); written <- err }()
	var value [1]byte
	_, readErr := io.ReadFull(conn, value[:])
	writeErr := <-written
	if readErr != nil || writeErr != nil || value[0] != 1 {
		t.Fatal("adapter revoked an already transferred connection", readErr, writeErr)
	}
}

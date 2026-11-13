//go:build s6_observation_probe && !d1_resource_probe && (darwin || linux)

package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestTerminalLifecycleInitIsTerminalOnly(t *testing.T) {
	v := sampleInit()
	v.Version, v.Mode = 2, "K10"
	if validateInit(v, testNonce) == nil {
		t.Fatal("accepted unrelated sources")
	}
	key := v.Request.Terminal
	v.Request = initFrame{}.Request
	v.Request.Terminal = key
	if err := validateInit(v, testNonce); err != nil {
		t.Fatal(err)
	}
	v.Version = 1
	if validateInit(v, testNonce) == nil {
		t.Fatal("accepted K10 on the existing protocol")
	}
}

func TestTerminalLifecycleExchange(t *testing.T) {
	for _, scenario := range []string{"complete", "wrong-start", "no-STOP", "early-return", "extra-after-STOP", "run-error", "new-instance", "pending", "external-cancel"} {
		t.Run(scenario, func(t *testing.T) {
			f := fixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done, joined := make(chan error, 1), make(chan struct{})
			entered := make(chan struct{})
			initial := terminalBoundary{Identity: "1111111111111111111111111111111111111111", Key: sampleInit().Request.Terminal}
			initial.Snapshot.Available = true
			initial.Snapshot.SchemaVersion = 1
			initial.Snapshot.Status = "COMPLETE"
			initial.Snapshot.Population = "RegisteredTransportInstance"
			initial.Snapshot.Instance[0] = 1
			initial.Snapshot.CodeStatus = "COMPLETE_RESTRICTED_OWNER_CLASSIFICATION"
			final := initial
			final.Snapshot.Registered, final.Snapshot.Removed, final.Snapshot.Classified = 1, 1, 1
			final.Snapshot.RegistrationSerial, final.Snapshot.TerminalSerial = 1, 1
			final.Snapshot.Coarse, final.Snapshot.CodeStatus = 1, "UNKNOWN"
			if scenario == "new-instance" {
				final.Snapshot.Instance[0]++
			}
			if scenario == "pending" {
				final.Snapshot.Pending = 1
			}
			drained := false
			go func() {
				defer close(joined)
				defer f.pipes.close()
				done <- newChannel(f.pipes, testNonce).serveTerminalLifecycle(ctx, func() terminalBoundary {
					if drained {
						return final
					}
					return initial
				}, func(runCtx context.Context) error {
					close(entered)
					if scenario == "early-return" {
						return nil
					}
					<-runCtx.Done()
					drained = true
					if scenario == "run-error" {
						return errors.New("fixture drain error")
					}
					return nil
				})
			}()
			t.Cleanup(func() {
				cancel()
				f.pipes.close()
				select {
				case <-joined:
				case <-time.After(time.Second):
					t.Error("owned lifecycle test did not join")
				}
			})
			if err := f.response.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			r := bufio.NewReader(f.response)
			first, err := r.ReadBytes('\n')
			want := terminalBoundaryFrame{2, testNonce, -1, "TERMINAL_INITIAL", true, initial}
			var frame terminalBoundaryFrame
			if err != nil || canonical(first, &frame) != nil || frame != want {
				t.Fatal("initial frame mismatch", err)
			}
			select {
			case <-entered:
				t.Fatal("Run started before START")
			default:
			}
			start := readFrame{2, testNonce, 0, "START"}
			if scenario == "wrong-start" {
				start.Nonce += "wrong"
			}
			send(t, f, marshal(t, start))
			if scenario != "wrong-start" {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("Run did not start")
				}
			}
			switch scenario {
			case "wrong-start", "no-STOP", "early-return":
			case "external-cancel":
				cancel()
			default:
				send(t, f, marshal(t, readFrame{2, testNonce, 1, "STOP"}))
				if scenario == "extra-after-STOP" {
					send(t, f, marshal(t, readFrame{2, testNonce, 2, "READ"}))
				}
			}
			_ = f.request.Close()
			err = wait(t, done)
			if scenario == "complete" {
				last, readErr := r.ReadBytes('\n')
				want = terminalBoundaryFrame{2, testNonce, 1, "TERMINAL_FINAL", true, final}
				if err != nil || readErr != nil || canonical(last, &frame) != nil || frame != want {
					t.Fatal("final frame mismatch", err, readErr)
				}
			} else if err == nil {
				t.Fatal("incomplete lifecycle succeeded")
			}
			if extra, err := r.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected response after lifecycle: byte=%d err=%v", extra, err)
			}
		})
	}
}

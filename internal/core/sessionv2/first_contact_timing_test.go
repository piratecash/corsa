//go:build s6_first_contact_probe

package sessionv2

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

func TestFirstContactPhaseTiming(t *testing.T) {
	t.Run("tls timeout records only tls", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			dialer := newNode(t)
			wire, peer := net.Pipe()
			t.Cleanup(func() { _ = wire.Close(); _ = peer.Close() })
			timeout := 5 * time.Second
			var elapsed time.Duration
			var phaseErr error
			calls := 0
			_, err := DialWithTiming(context.Background(), wire, dialer.local, testIntro(), AnyPeer(),
				Timeouts{TLS: timeout, Proof: time.Second}, func(phase string, duration time.Duration, err error) {
					calls++
					if phase != "tls" {
						t.Errorf("phase = %q, want tls", phase)
					}
					elapsed, phaseErr = duration, err
				})
			if !errors.Is(err, ErrNotV2) || !errors.Is(phaseErr, os.ErrDeadlineExceeded) || calls != 1 || elapsed != timeout {
				t.Fatalf("dial=%v phase=%v calls=%d elapsed=%v", err, phaseErr, calls, elapsed)
			}
		})
	})
	t.Run("successful handshake records tls then proof", func(t *testing.T) {
		dialer, listener := newNode(t), newNode(t)
		wire, peer := net.Pipe()
		t.Cleanup(func() { _ = wire.Close(); _ = peer.Close() })
		accepted := acceptAsync(context.Background(), peer, listener)
		var phases []string
		_, err := DialWithTiming(context.Background(), wire, dialer.local, testIntro(), AnyPeer(), testTimeouts,
			func(phase string, duration time.Duration, err error) {
				phases = append(phases, phase)
				if duration < 0 || err != nil {
					t.Errorf("phase %s: elapsed=%v error=%v", phase, duration, err)
				}
			})
		if other := <-accepted; err != nil || other.err != nil {
			t.Fatalf("dial: %v, accept: %v", err, other.err)
		}
		if len(phases) != 2 || phases[0] != "tls" || phases[1] != "proof" {
			t.Fatalf("phases = %v, want [tls proof]", phases)
		}
	})
}

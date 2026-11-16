package node

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func TestBelowFloorHelloReceivesNoWelcome(t *testing.T) {
	t.Parallel()
	for _, version := range []int{28, 29, 30} {
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			quiet, address, stopQuiet := listenerNode(t, nil)
			t.Cleanup(stopQuiet)
			requireBelowFloorLineEOF(t, address, version, nil)
			assertNoBelowFloorSession(t, quiet)

			for _, tc := range []struct {
				name  string
				alert []byte
			}{
				{"busy listener with alert", sessionv2.RefusalAlert()},
				{"busy listener without alert", nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					alertSenders := 0
					if len(tc.alert) > 0 {
						alertSenders = productionAlertPoolBounds().total
					}
					busy := refusingNode(t, config.Node{}, func(s *Service) {
						s.listenerRefusals = newListenerRefusals(alertPoolBounds{total: alertSenders, perSource: max(alertSenders, 1)}, s.startedAt)
						s.loopbackInbound = newLoopbackInboundClass(loopbackInboundBounds{maxActive: 0, maxNewPerWindow: 1, window: time.Hour}, productionIdentityPenaltyPolicy(), nil)
					})
					requireBelowFloorLineEOF(t, busy.cfg.ListenAddress, version, tc.alert)
					stats := busy.ListenerRefusalStats()
					if stats.LoopbackConnections != 1 || (stats.ClosedWithoutAlert != 0) != (len(tc.alert) == 0) {
						t.Fatalf("unexpected listener refusal accounting: %+v", stats)
					}
					assertNoBelowFloorSession(t, busy)
				})
			}
		})
	}
}

// A line-based dialler reads EOF (or a reset when the listener closed with
// unread input). Neither the TLS refusal alert nor silence produces a frame.
func requireBelowFloorLineEOF(t *testing.T, address string, version int, expected []byte) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "{\"type\":\"hello\",\"version\":%d,\"client\":\"node\",\"listener\":\"0\"}\n", version); err != nil &&
		!errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE) {
		t.Fatal(err)
	}
	reply, err := bufio.NewReader(conn).ReadBytes('\n')
	if (!errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET)) || !bytes.Equal(reply, expected) {
		t.Fatalf("v%d read %x after a below-floor hello, err=%v, want %x then close", version, reply, err, expected)
	}
}

func assertNoBelowFloorSession(t *testing.T, s *Service) {
	t.Helper()
	if stats := s.SessionKindStats(); stats.InboundV2 != 0 || stats.InboundLegacy != 0 {
		t.Fatalf("below-floor hello created a session: %+v", stats)
	}
	s.ipStateMu.RLock()
	ban := s.bans["127.0.0.1"]
	s.ipStateMu.RUnlock()
	if ban.Score > 0 || !ban.Blacklisted.IsZero() {
		t.Fatalf("listener charged the below-floor peer: %+v", ban)
	}
}

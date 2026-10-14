package node

import (
	"errors"
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
)

// TestRegisterInboundConnNamesWhyItRefused pins that each refusal of
// registerInboundConn carries its own sentinel. handleConn logs the error it
// gets, once; a bare "refused" made it guess the cause, and the guess it
// logged was wrong for the override case.
func TestRegisterInboundConnNamesWhyItRefused(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		service func(t *testing.T) *Service
		want    error
	}{
		{
			name:    "network_override",
			service: newOverrideService,
			want:    ErrNetworkOverrideOwnsNoConnections,
		},
		{
			name: "shutdown_drain_has_run",
			service: func(t *testing.T) *Service {
				svc := newRefusalTestService(t, 0)
				svc.closeAllInboundConns()
				return svc
			},
			want: errInboundRegistrationClosed,
		},
		{
			name: "inbound_limit_reached",
			service: func(t *testing.T) *Service {
				svc := newRefusalTestService(t, 1)
				occupant, remote := net.Pipe()
				t.Cleanup(func() { _ = remote.Close() })
				if err := svc.registerInboundConn(occupant, nil); err != nil {
					t.Fatalf("fixture: the first conn under a limit of one was refused: %v", err)
				}
				return svc
			},
			want: errInboundLimitReached,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := tc.service(t)
			server, client := net.Pipe()
			t.Cleanup(func() { _ = client.Close() })
			t.Cleanup(func() { _ = server.Close() })

			err := svc.registerInboundConn(server, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("registerInboundConn = %v, want %v", err, tc.want)
			}
			if _, registered := svc.connIDFor(server); registered {
				t.Fatal("the refused conn is in the registry")
			}
		})
	}
}

// newRefusalTestService builds a plain Service with the given inbound limit
// (zero: none). Every conn it registers is closed when the test ends by the
// same drain Run's shutdown uses.
func newRefusalTestService(t *testing.T, maxIncoming int) *Service {
	t.Helper()
	cfg := configtest.WithTempDataDirs(t, config.Node{
		ListenAddress:     "127.0.0.1:0",
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
		MaxIncomingPeers:  maxIncoming,
	})
	svc := NewService(cfg, testIdentityForNetworkConsumerTest(t), nil)
	t.Cleanup(svc.WaitBackground)
	t.Cleanup(svc.closeAllInboundConns)
	return svc
}

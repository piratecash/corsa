package node

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// inbound_subnet_diversity_test.go pins who occupies a subnet group for the
// outbound-diversity filter: only this node's own outbound choices — its
// connection-manager slots and its outbound sessions. An inbound connection
// is chosen by the remote side, so a remote that authenticates inbound
// sessions from addresses in many groups must not be able to stop this node
// from dialling the honest peers in those groups.
//
// The addresses are public (1.2.x.x, 2001:db8::): subnetGroupKey gives no
// group to loopback and private addresses, so they would not exercise the
// filter at all.

// registerInboundFrom registers an inbound connection whose transport
// remote address is remote; authenticated mirrors what completeInboundAuth
// leaves behind (the tracked flag). Its NetCore is closed when the test ends.
func registerInboundFrom(t *testing.T, svc *Service, remote string, authenticated bool) {
	t.Helper()
	host, portText, err := net.SplitHostPort(remote)
	if err != nil {
		t.Fatalf("split %q: %v", remote, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("port of %q: %v", remote, err)
	}
	conn := newNoIOConn(t, &net.TCPAddr{IP: net.ParseIP(host), Port: port})
	core := newTestNetCore(t, 0, conn, netcore.Inbound, netcore.Options{LastActivity: time.Now().UTC()})
	svc.peerMu.Lock()
	svc.registerTestInboundLocked(conn, core, authenticated)
	svc.peerMu.Unlock()
}

// registerOutboundSession records an outbound session the way a published
// session is recorded: under peerMu → deliveryMu.
func registerOutboundSession(svc *Service, address domain.PeerAddress) {
	svc.peerMu.Lock()
	svc.deliveryMu.Lock()
	svc.upstream[address] = struct{}{}
	svc.deliveryMu.Unlock()
	svc.peerMu.Unlock()
}

func knownPeerExcludeReasons(svc *Service, address domain.PeerAddress) []domain.ExcludeReason {
	for _, info := range svc.peerProvider.KnownPeers() {
		if info.Address == address {
			return info.ExcludeReasons
		}
	}
	return nil
}

func hasExcludeReason(reasons []domain.ExcludeReason, want domain.ExcludeReason) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}

func TestOnlyOutboundChoicesOccupyASubnetGroup(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		candidate string
		occupy    func(t *testing.T, svc *Service)
		wantKept  bool
	}{
		"no connection": {
			candidate: "1.2.3.4:64646",
			occupy:    func(*testing.T, *Service) {},
			wantKept:  true,
		},
		// An outbound session spends one of this node's outbound slots in the
		// group: skipping the rest of the group is what the filter is for.
		"outbound session in the same /24": {
			candidate: "1.2.3.4:64646",
			occupy: func(_ *testing.T, svc *Service) {
				registerOutboundSession(svc, "1.2.3.77:64646")
			},
			wantKept: false,
		},
		"unauthenticated inbound from the same /24": {
			candidate: "1.2.3.4:64646",
			occupy: func(t *testing.T, svc *Service) {
				registerInboundFrom(t, svc, "1.2.3.77:51000", false)
			},
			wantKept: true,
		},
		"authenticated inbound from another host in the same /24": {
			candidate: "1.2.3.4:64646",
			occupy: func(t *testing.T, svc *Service) {
				registerInboundFrom(t, svc, "1.2.3.77:51000", true)
			},
			wantKept: true,
		},
		"authenticated inbound from another host in the same /64": {
			candidate: "[2001:db8:1:1::5]:64646",
			occupy: func(t *testing.T, svc *Service) {
				registerInboundFrom(t, svc, "[2001:db8:1:1::77]:51000", true)
			},
			wantKept: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly, tc.candidate)
			candidate := domain.PeerAddress(tc.candidate)
			if !candidatesContain(svc, candidate) {
				t.Fatalf("precondition: %s is not a candidate with no connection", candidate)
			}

			tc.occupy(t, svc)

			if got := candidatesContain(svc, candidate); got != tc.wantKept {
				t.Fatalf("candidate %s kept = %v, want %v; candidates=%v",
					candidate, got, tc.wantKept, svc.peerProvider.Candidates())
			}
			reasons := knownPeerExcludeReasons(svc, candidate)
			if got := hasExcludeReason(reasons, domain.ExcludeReasonSameSubnet); got == tc.wantKept {
				t.Fatalf("list_peers reports same_subnet = %v for %s, want %v; reasons=%v",
					got, candidate, !tc.wantKept, reasons)
			}
		})
	}
}

// One authenticated inbound per group used to be enough: K inbound sessions
// from K distinct /24s left the connection manager nothing to dial in any of
// those K groups, and its slots went to whatever groups the remote left free.
func TestAuthenticatedInboundSessionsDoNotEmptyTheFill(t *testing.T) {
	t.Parallel()

	const groups = 8
	for name, withInbound := range map[string]bool{
		"no inbound":                        false,
		"one authenticated inbound per /24": true,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bootstrap := make([]string, 0, groups)
			for g := 0; g < groups; g++ {
				bootstrap = append(bootstrap, "1.2."+strconv.Itoa(g)+".4:64646")
			}
			svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly, bootstrap...)
			if got := len(svc.peerProvider.Candidates()); got != groups {
				t.Fatalf("precondition: %d candidates, want %d", got, groups)
			}
			if withInbound {
				for g := 0; g < groups; g++ {
					registerInboundFrom(t, svc, "1.2."+strconv.Itoa(g)+".200:51000", true)
				}
			}

			dialFn, _ := fakeDialFn()
			cm := NewConnectionManager(ConnectionManagerConfig{
				MaxSlotsFn: func() int { return groups },
				Provider:   svc.peerProvider,
				DialFn:     dialFn,
			})
			// The provider's QueuedFn reads svc.connManager, so the manager
			// under test replaces the one NewService built.
			svc.connManager = cm
			stopCM := runCMUntilStopped(t, cm)
			defer stopCM()
			cm.NotifyBootstrapReady()

			waitForConditionMsg(t, 5*time.Second, "the bootstrap fill did not take one honest peer per /24", func() bool {
				return cm.SlotCount() == groups
			})
		})
	}
}

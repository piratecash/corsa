package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// test_helper_netcore_cleanup_test.go pins that every test helper which builds
// a NetCore over a net.Pipe releases that NetCore's writer goroutine when the
// test that called it ends. Closing the pipe ends is not enough: an idle
// writerLoop only returns on Close, so a helper without it leaked one
// goroutine and its buffers per call, and a stress test that calls such a
// helper a thousand times per run kept a thousand writers alive per run.
//
// The observable is WriterDone: on a core that never wrote, it closes only
// when writerLoop returns, so "closed after the subtest's cleanup" is exactly
// "the writer goroutine is gone". (failedNetCore is not covered here: its
// writer has already failed a write by design, which closes WriterDone while
// the loop is still alive, and the loop's own exit signal is not exported.)

func TestNetCoreTestHelpersReleaseTheirWriterOnCleanup(t *testing.T) {
	t.Parallel()

	cases := map[string]func(t *testing.T) *netcore.NetCore{
		"attachPushObserver": func(t *testing.T) *netcore.NetCore {
			svc := newInitMapsTestService(t, nil)
			_ = attachPushObserver(t, svc, "cleanup-recipient", netcore.ConnID(9601))
			return svc.netCoreForID(domain.ConnID(9601))
		},
		"legacyInboundConn": func(t *testing.T) *netcore.NetCore {
			svc := newInitMapsTestService(t, nil)
			connID := legacyInboundConn(t, svc, netcore.ConnID(9602),
				&net.TCPAddr{IP: net.IPv4(203, 0, 113, 9), Port: 64646}, domain.PeerIdentity{})
			return svc.netCoreForID(connID)
		},
		"newQueueTestSession": func(t *testing.T) *netcore.NetCore {
			session, _ := newQueueTestSession(t, domain.PeerAddress("10.0.0.96:64646"), 4)
			return session.netCore
		},
		"attachTestNetCore": func(t *testing.T) *netcore.NetCore {
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = local.Close() })
			t.Cleanup(func() { _ = remote.Close() })
			session := &peerSession{address: domain.PeerAddress("10.0.0.97:64646"), conn: local}
			attachTestNetCore(t, session)
			return session.netCore
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			var core *netcore.NetCore
			t.Run("helper", func(t *testing.T) {
				core = build(t)
				if core == nil {
					t.Fatal("fixture: the helper built no NetCore")
				}
			})
			// The inner subtest has returned, so its cleanups have run.
			select {
			case <-core.WriterDone():
			default:
				t.Fatalf("%s left its NetCore writer goroutine running after the test's cleanup", name)
			}
		})
	}
}

package node

import (
	"sync"
	"testing"
	"time"
)

// This is the ordinary listener with production loopback/handshake limits.
// The default shared ceiling is off, but its reservations still count usage;
// positive ownership before cancellation makes the final zero non-vacuous.
func TestCancellingRunReleasesAStalledLoopbackTLSHandshake(t *testing.T) {
	listener, stopNode := startQuietNode(t, withProductionLimits)
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(stopNode) }
	t.Cleanup(stop)

	if listener.disableRateLimiting || listener.inboundHandshakes == nil || listener.loopbackInbound == nil || listener.connBudget == nil {
		t.Fatal("ordinary production admission owners are not installed")
	}
	if listener.inboundHandshakes.bounds != productionHandshakeBounds() || listener.loopbackInbound.bounds != productionLoopbackInboundBounds(0) {
		t.Fatal("fixture replaced the production handshake or loopback bounds")
	}
	before := listener.connBudget.Snapshot()
	if before.Enabled || before.Total != 0 || before.MaxInbound != 0 || before.Used != 0 || before.Inbound != 0 || before.Outbound != 0 || before.Auxiliary != 0 {
		t.Fatalf("default unbounded shared ceiling must start with zero owned usage: %+v", before)
	}

	// The existing helper sends a complete record from a real TLS 1.3 client
	// offering corsa/2, then sends nothing further. It registers client.Close
	// only for test cleanup, so no peer close releases ownership before stop.
	stalledHandshake(t, listener.cfg.ListenAddress, clientHelloRecord(t))
	waitForConditionMsg(t, 5*time.Second, "the genuine loopback TLS handshake never held all admission owners", func() bool {
		budget := listener.connBudget.Snapshot()
		return listener.inboundHandshakes.running() == 1 &&
			listener.inboundHandshakes.loopbackRunning() == 1 &&
			loopbackActive(listener) == 1 && budget.Used == 1 && budget.Inbound == 1 &&
			budget.Outbound == 0 && budget.Auxiliary == 0
	})

	// This cancels the original Run and joins it through the existing helper.
	// The client remains open: no synthetic release or close supplies the zero.
	stop()
	budget := listener.connBudget.Snapshot()
	nodeHandshakes := listener.inboundHandshakes.running()
	loopbackHandshakes := listener.inboundHandshakes.loopbackRunning()
	active := loopbackActive(listener)
	if nodeHandshakes != 0 || loopbackHandshakes != 0 || active != 0 || budget.Used != 0 || budget.Inbound != 0 || budget.Outbound != 0 || budget.Auxiliary != 0 {
		t.Fatalf("cancelled Run retained admission ownership: node_handshakes=%d loopback_handshakes=%d loopback_active=%d shared=%+v", nodeHandshakes, loopbackHandshakes, active, budget)
	}
}

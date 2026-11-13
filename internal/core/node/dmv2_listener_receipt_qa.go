//go:build s6_direct_dmv2_qa

package node

import (
	"context"
	"net"
)

// ServeDirectDMV2ListenerTestPipe exposes the normal accepted handler to
// an external package test. This entry exists only in the overlay.
func (s *Service) ServeDirectDMV2ListenerTestPipe(conn net.Conn) {
	s.directDMV2ListenerTest = true
	s.handleConn(conn, nil)
	s.runCancel()
	s.closeBackgroundAdmission()
	s.WaitBackground()
}

// EnableDirectDMV2InboundWakeTest enables the listener before creating pipes.
func (s *Service) EnableDirectDMV2InboundWakeTest() {
	s.directDMV2ListenerTest = true
}

// HandleDirectDMV2InboundWakeTest uses the actual per-connection handler.
// The SDK fixture owns service shutdown after all connections have joined.
func (s *Service) HandleDirectDMV2InboundWakeTest(conn net.Conn) {
	s.handleConn(conn, nil)
}

// RunDirectDMV2BootstrapWakeTest runs the normal sweep with its real ticker.
// The test owns this loop. It does not start the listener or ConnectionManager.
func (s *Service) RunDirectDMV2BootstrapWakeTest(ctx context.Context) {
	s.bootstrapLoop(ctx)
}

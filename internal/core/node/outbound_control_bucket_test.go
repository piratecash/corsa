package node

import (
	"io"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// outbound_control_bucket_test.go pins who drops an outbound session's
// control-frame bucket (cmdLimiter key "outbound:<connID>"): the goroutine
// that dispatched the session's frames, on every way the session ends. Each
// test charges the bucket the way production does and asserts nothing of it
// is left once the session's goroutine is done.

func TestFailCMSessionSetup_DropsTheControlFrameBucket(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const addr = domain.PeerAddress("10.0.0.8:64646")
	session := &peerSession{address: addr, connID: 4343}
	bucketKey := outboundControlFrameLimitKey(session.connID)

	svc.outboundControlFrameAllowed(session)
	if !commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("precondition: a setup-time control frame charges the bucket")
	}
	svc.failCMSessionSetup(session, addr, 1, io.ErrUnexpectedEOF)

	if commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("the control-frame bucket of a session whose setup failed outlived it")
	}
}

// The same for a session the ConnectionManager evicted while its setup ran.
func TestAbortLocallyEvictedCMSession_DropsTheControlFrameBucket(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const addr = domain.PeerAddress("10.0.0.9:64646")
	session := &peerSession{address: addr, connID: 4444}
	bucketKey := outboundControlFrameLimitKey(session.connID)

	svc.outboundControlFrameAllowed(session)
	if !commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("precondition: a setup-time control frame charges the bucket")
	}
	svc.abortLocallyEvictedCMSession(session, addr, nil)

	if commandLimiterHasBucket(svc.cmdLimiter, bucketKey) {
		t.Fatal("the control-frame bucket of a session evicted during setup outlived it")
	}
}

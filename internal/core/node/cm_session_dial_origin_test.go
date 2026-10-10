package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// cm_session_dial_origin_test.go pins who may delete a fallback-port
// dialOrigin entry. The entry is keyed by the dial address alone, so every
// path that ends a session which was never published — a stale dial success,
// an aborted or failed setup — must leave it alone while a different session
// is registered under that address: it is that session's entry.

const (
	dialOriginFallback = domain.PeerAddress("10.0.0.1:64646")
	dialOriginPrimary  = domain.PeerAddress("10.0.0.1:7777")
)

func TestOnCMStaleSession_LeavesANewerSessionsDialOrigin(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	newer := &peerSession{address: dialOriginFallback}
	svc.peerMu.Lock()
	svc.sessions[dialOriginFallback] = newer
	svc.dialOrigin[dialOriginFallback] = dialOriginPrimary
	svc.peerMu.Unlock()

	svc.onCMStaleSession(&peerSession{address: dialOriginFallback})

	svc.peerMu.RLock()
	got, ok := svc.dialOrigin[dialOriginFallback]
	svc.peerMu.RUnlock()
	if !ok || got != dialOriginPrimary {
		t.Errorf("dialOrigin[%s] after a stale success = %q (%v), want the registered session's %s kept",
			dialOriginFallback, got, ok, dialOriginPrimary)
	}
}

func TestOnCMStaleSession_ForgetsItsOwnDialOrigin(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	svc.peerMu.Lock()
	svc.dialOrigin[dialOriginFallback] = dialOriginPrimary
	svc.peerMu.Unlock()

	svc.onCMStaleSession(&peerSession{address: dialOriginFallback})

	svc.peerMu.RLock()
	_, ok := svc.dialOrigin[dialOriginFallback]
	svc.peerMu.RUnlock()
	if ok {
		t.Errorf("dialOrigin[%s] survived the stale success that registered it", dialOriginFallback)
	}
}

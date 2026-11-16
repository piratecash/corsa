package node

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// requireNothingProvenAt fails when anything at address reads as v2 proven
// there: a pin, a binding, v2_strong. A held fall back to v1 forbids v1; it
// shows nothing about v2.
func requireNothingProvenAt(t *testing.T, svc *Service, address domain.PeerAddress) {
	t.Helper()
	if pins := svc.SecureSessionStoreStats().PinnedIdentities; pins != 0 {
		t.Fatalf("%d pinned identities, want none", pins)
	}
	if svc.secureSessions.store.endpointRequiresV2(address) || svc.secureSessions.marks.v2Strong(address) {
		t.Fatal("the held address reads as proven v2: a binding or v2_strong")
	}
}

func TestTheAggregateStatusReportsTLSOnly(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	frame := svc.aggregateStatusFrame()
	if frame.AggregateStatus == nil || !frame.AggregateStatus.V2TLSFirst {
		t.Fatalf("aggregate status %+v, want v2_tls_first=true", frame.AggregateStatus)
	}
	encoded, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(encoded, `"v2_tls_first":true`) {
		t.Fatalf("the frame omits the enabled v2_tls_first field: %s", encoded)
	}
	stats := svc.SecureSessionStoreStats()
	if stats.TLSFirstDisabled || stats.DialsOpenedByProbe != 0 {
		t.Fatalf("unexpected probe-first stats: %+v", stats)
	}
}

func TestBoundAndUnboundEndpointsAreReachedOverTLS(t *testing.T) {
	t.Parallel()
	t.Run("unbound", func(t *testing.T) {
		t.Parallel()
		_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
		defer stopListener()
		dialer, stopDialer := startQuietNode(t, nil)
		defer stopDialer()
		target, connections := startCountingForwarder(t, address)

		session, err := dialEstablishedPastTransport(t, dialer, target)
		if err != nil {
			t.Fatalf("the dial was not established: %v", err)
		}
		if _, proven := session.provenIdentity(); !proven {
			t.Fatal("the session was not authenticated over TLS")
		}
		if got := connections(); got != 1 {
			t.Fatalf("%d connections, want one TLS connection", got)
		}
	})
	t.Run("bound", func(t *testing.T) {
		t.Parallel()
		listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
		defer stopListener()
		dialer, stopDialer := startQuietNode(t, nil)
		defer stopDialer()
		target, connections := startCountingForwarder(t, address)
		if _, err := dialer.secureSessions.store.noteProvenOutbound(dialledAt(target), peerIdentityOf(t, listener), endpointProtectionNone()); err != nil {
			t.Fatalf("bind: %v", err)
		}

		session, err := dialEstablishedPastTransport(t, dialer, target)
		if err != nil {
			t.Fatalf("the dial was not established: %v", err)
		}
		if _, proven := session.provenIdentity(); !proven {
			t.Fatal("the session to a bound endpoint is v1, want v2")
		}
		if got := connections(); got != 1 {
			t.Fatalf("%d connections, want the TLS one alone", got)
		}
		if got := dialer.SecureSessionStoreStats().DialsOpenedByProbe; got != 0 {
			t.Fatalf("%d dials opened by the probe to a bound endpoint", got)
		}
	})
}

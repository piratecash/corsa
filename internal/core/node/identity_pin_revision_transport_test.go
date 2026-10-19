package node

import (
	"bufio"
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestIdentityPinRevisionExhaustionAfterOutboundProofDoesNotChargeTheAddress(t *testing.T) {
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	target := domain.PeerAddress(normalizeAddress(address))
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new_pin", true: "existing_pin"}[existing], func(t *testing.T) {
			dialer, stop := startQuietNode(t, nil)
			defer stop()
			store := dialer.secureSessions.store
			peer := peerIdentityOf(t, listener)
			if existing {
				if _, err := store.noteProvenOutbound(dialledAt(target), peer, endpointProtectionNone()); err != nil {
					t.Fatal(err)
				}
			}
			dialer.secureSessions.marks.noteOutboundV2(target)
			marks := dialer.secureSessions.marks
			marks.mu.Lock()
			beforeMark := marks.marks[target]
			marks.mu.Unlock()
			store.mu.Lock()
			beforeStore := store.snapshotLocked()
			store.evidenceRevision = identityPinEvidenceRevision(^uint64(0))
			changes, written := store.changes, store.written
			store.mu.Unlock()
			dialer.recordAddedPeer(target, addPeerModeOperator)
			oldAttempt := time.Now().Add(-time.Hour)
			dialer.peerMu.Lock()
			dialer.persistedMeta[target].AllowImmediateDial = false
			dialer.persistedMeta[target].DialAttempts = 5
			dialer.persistedMeta[target].LastDialAttemptAt = &oldAttempt
			beforeAging := *dialer.persistedMeta[target]
			dialer.health[target] = &peerHealth{Address: target, Score: 17, ConsecutiveFailures: 2}
			dialer.peerMu.Unlock()
			result, err := dialer.dialForCM(context.Background(), []domain.PeerAddress{target})
			if result.Session != nil {
				_ = result.Session.Close()
				t.Fatal("a session was established without publishing its required pin")
			}
			if !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
				t.Fatalf("real outbound proof did not hit forced revision limit: %v", err)
			}
			dialer.onCMDialFailed(target, err, false)
			dialer.peerMu.RLock()
			health := *dialer.health[target]
			afterAging := *dialer.persistedMeta[target]
			dialer.peerMu.RUnlock()
			if health.Score != 17 || health.ConsecutiveFailures != 2 {
				t.Errorf("local proof revision refusal charged address: score=%d failures=%d", health.Score, health.ConsecutiveFailures)
			}
			if !reflect.DeepEqual(beforeAging, afterAging) {
				t.Errorf("local proof revision refusal changed aging: before=%+v after=%+v", beforeAging, afterAging)
			}
			store.mu.Lock()
			afterStore := store.snapshotLocked()
			if changes != store.changes || written != store.written {
				t.Error("refused proof changed persistence counters")
			}
			store.mu.Unlock()
			if !reflect.DeepEqual(beforeStore, afterStore) {
				t.Error("refused proof changed pin/binding timestamps or TTL")
			}
			marks.mu.Lock()
			afterMark := marks.marks[target]
			marks.mu.Unlock()
			if !reflect.DeepEqual(beforeMark, afterMark) {
				t.Error("refused proof changed existing v2 marks")
			}
		})
	}
}

func TestIdentityPinRevisionExhaustionAfterInboundProofPreservesProtection(t *testing.T) {
	t.Parallel()
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new_pin", true: "existing_pin"}[existing], func(t *testing.T) {
			listener, dialer := newDialChoiceService(t), newDialChoiceService(t)
			store := listener.secureSessions.store
			id := peerIdentityOf(t, dialer)
			endpoint := domain.PeerAddress("192.0.2.18:64646")
			if existing {
				if _, err := store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone()); err != nil {
					t.Fatal(err)
				}
			}
			store.mu.Lock()
			before := store.snapshotLocked()
			changes, written := store.changes, store.written
			store.evidenceRevision = identityPinEvidenceRevision(^uint64(0))
			store.mu.Unlock()
			server, client := tcpPair(t)
			var totals netcore.TransportTotals
			metered, err := netcore.NewMeteredConn(server, &totals)
			if err != nil {
				t.Fatal(err)
			}
			accepted := make(chan error, 1)
			go func() {
				conn, _, err := listener.acceptSecureInbound(metered, bufio.NewReader(metered))
				if conn != nil {
					_ = conn.Close()
				}
				accepted <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			session, err := sessionv2.Dial(ctx, client, dialer.secureSessions.local, dialer.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
			if err != nil {
				t.Fatalf("inbound fixture did not complete a real mutual proof: %v", err)
			}
			wire, err := session.Conn()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = wire.Close() }()
			select {
			case err := <-accepted:
				if !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
					t.Fatalf("inbound did not refuse publication at revision limit: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("inbound proof refusal did not finish")
			}
			store.mu.Lock()
			after := store.snapshotLocked()
			if changes != store.changes || written != store.written {
				t.Error("refused inbound proof changed persistence counters")
			}
			store.mu.Unlock()
			if !reflect.DeepEqual(before, after) {
				t.Error("refused inbound proof changed pin/binding timestamps or TTL")
			}
			if store.identityPinned(id) != existing || store.endpointRequiresV2(endpoint) != existing {
				t.Fatal("inbound refusal weakened existing protection or installed a new pin")
			}
		})
	}
}

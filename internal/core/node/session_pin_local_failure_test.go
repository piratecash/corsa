package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// This exercises real mutual TLS/proof and the ordinary CM dial ladder. The
// existing store seams inject local persistence errors; they do not model
// filesystem durability or a physical crash. No Service.Run/listener is used.
func TestLocalPinPersistenceFailureAfterProofDoesNotChargeTheAddress(t *testing.T) {
	for _, failure := range []string{"store-write", "marker-write"} {
		t.Run(failure, func(t *testing.T) {
			dialer, listener := newDialChoiceService(t), newDialChoiceService(t)
			store, _ := openPinStore(t, newPinStorePaths(t), newStoreClock())
			requireStarts(t, store)
			dialer.secureSessions.store = store
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			proofReady := make(chan struct{})
			cause := errors.New("local persistence refused")
			writes, markers := 0, 0
			realWrite := store.writeFile
			store.writeFile = func(raw []byte) error {
				writes++
				// The dialer's last proof write may finish before Accept settles
				// its listener deadline. Close only after both proof APIs return.
				select {
				case <-proofReady:
				case <-ctx.Done():
					return ctx.Err()
				}
				if failure == "store-write" {
					return cause
				}
				return realWrite(raw)
			}
			store.writeMarker = func([]byte) error { markers++; return cause }

			address := dialChoiceAddress
			dialer.recordAddedPeer(address, addPeerModeOperator)
			oldAttempt := time.Now().Add(-time.Hour)
			dialer.peerMu.Lock()
			dialer.persistedMeta[address].AllowImmediateDial = false
			dialer.persistedMeta[address].DialAttempts = 2
			dialer.persistedMeta[address].LastDialAttemptAt = &oldAttempt
			beforeAging := *dialer.persistedMeta[address]
			dialer.health[address] = &peerHealth{Address: address, Score: 17, ConsecutiveFailures: 2}
			beforeHealth := *dialer.health[address]
			dialer.peerMu.Unlock()

			type proofEnd struct {
				proved bool
				err    error
			}
			ended := make(chan proofEnd, 1)
			peer := newScriptedPeer(func(conn net.Conn, _ *bufio.Reader) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				proof, err := sessionv2.Accept(ctx, conn, listener.secureSessions.local,
					listener.welcomeFrame("", ""), listener.secureSessions.certificates, sessionv2.DefaultTimeouts)
				if err != nil {
					ended <- proofEnd{err: err}
					return
				}
				wire, err := proof.Conn()
				if err != nil {
					ended <- proofEnd{err: err}
					return
				}
				if err := wire.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					ended <- proofEnd{proved: true, err: err}
					return
				}
				close(proofReady)
				var b [1]byte
				n, err := wire.Read(b[:])
				if n != 0 || err == nil {
					ended <- proofEnd{proved: true, err: errors.New("local refusal left an application byte or an open stream")}
					return
				}
				if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
					ended <- proofEnd{proved: true, err: err}
					return
				}
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
					ended <- proofEnd{proved: true, err: fmt.Errorf("unexpected terminal read: %w", err)}
					return
				}
				ended <- proofEnd{proved: true}
			})
			rawDial := peer.dialer()
			dialer.peerTCPDial = func(ctx context.Context, _ string, _ time.Duration) (net.Conn, error) { return rawDial(ctx) }
			result, err := dialer.dialForCM(ctx, []domain.PeerAddress{address})
			if result.Session != nil {
				_ = result.Session.Close()
				t.Fatal("pin failure admitted a session")
			}
			if !errors.Is(err, errPinProtectionLocalFailure) || !errors.Is(err, cause) {
				t.Fatalf("local refusal lost its classification or original cause: %v", err)
			}
			if !isUnchargedDialRefusal(err) || !localAddressDialOutcome(err) {
				t.Fatalf("local persistence failure was classified as remote: %v", err)
			}
			sockets := peer.sockets(t)
			if len(sockets) != 1 || !isClientHello(sockets[0]) {
				t.Fatalf("local pin refusal fell back or skipped TLS: %q", sockets)
			}
			select {
			case end := <-ended:
				if !end.proved || end.err != nil {
					t.Fatalf("mutual proof/closed transport witness: %+v", end)
				}
			case <-ctx.Done():
				t.Fatal("proof peer did not finish")
			}
			if writes != 1 || markers != map[string]int{"store-write": 0, "marker-write": 1}[failure] {
				t.Fatalf("store/marker ordering changed: writes=%d markers=%d", writes, markers)
			}
			if !store.identityPinned(peerIdentityOf(t, listener)) {
				t.Fatal("failed persistence discarded the in-memory protection")
			}
			store.mu.Lock()
			pending, markerWritten := store.changes != store.written, store.markerWritten
			store.mu.Unlock()
			if !pending || markerWritten {
				t.Fatal("failed persistence was acknowledged as written")
			}
			if dialer.secureSessions.marks.v2Strong(address) || dialer.secureSessions.marks.awaitsPinRoom(address) {
				t.Fatal("ordinary local write failure published v2 success or full-store pacing")
			}
			dialer.onCMDialFailed(address, err, false)
			dialer.peerMu.RLock()
			afterHealth, afterAging := *dialer.health[address], *dialer.persistedMeta[address]
			dialer.peerMu.RUnlock()
			if !reflect.DeepEqual(beforeHealth, afterHealth) || !reflect.DeepEqual(beforeAging, afterAging) {
				t.Fatalf("local persistence failure charged health or aging: health=%+v aging=%+v", afterHealth, afterAging)
			}
			if nodeAuthenticatedIdentity(dialer, peerIdentityOf(t, listener)) {
				t.Fatal("failed local protection authenticated an application peer")
			}
		})
	}
}

// The injected network refusal enters through the actual CM TCP-dial seam,
// unlike a local pin error after proof. This is not a kernel reachability test.
func TestLocalPinFailureClassificationDoesNotExemptTCPDialFailure(t *testing.T) {
	svc := newDialChoiceService(t)
	address := dialChoiceAddress
	svc.recordAddedPeer(address, addPeerModeOperator)
	svc.peerMu.Lock()
	svc.health[address] = &peerHealth{Address: address, Score: 17, ConsecutiveFailures: 2}
	beforeAttempts := svc.persistedMeta[address].DialAttempts
	svc.peerMu.Unlock()
	cause := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("injected connection refused")}
	calls := 0
	svc.peerTCPDial = func(context.Context, string, time.Duration) (net.Conn, error) { calls++; return nil, cause }
	result, err := svc.dialForCM(context.Background(), []domain.PeerAddress{address})
	if result.Session != nil {
		_ = result.Session.Close()
		t.Fatal("refused TCP dial admitted a session")
	}
	if calls != 1 || !errors.Is(err, cause) || !errors.Is(err, errPeerDialTransport) || errors.Is(err, errPinProtectionLocalFailure) {
		t.Fatalf("TCP failure was reclassified as local pin protection: calls=%d err=%v", calls, err)
	}
	if isUnchargedDialRefusal(err) || localAddressDialOutcome(err) {
		t.Fatal("ordinary network failure was exempted")
	}
	svc.onCMDialFailed(address, err, false)
	svc.peerMu.RLock()
	health, attempts := *svc.health[address], svc.persistedMeta[address].DialAttempts
	svc.peerMu.RUnlock()
	if health.Score != 17+peerScoreFailure || health.ConsecutiveFailures != 3 || attempts != beforeAttempts+1 {
		t.Fatalf("TCP failure was not charged exactly once: score=%d failures=%d attempts=%d", health.Score, health.ConsecutiveFailures, attempts)
	}
	requireNothingProvenAt(t, svc, address)
}

func TestLocalPinFailurePreservesFullStoreAndCancellationCauses(t *testing.T) {
	svc := newDialChoiceService(t)
	if err := svc.notePinNotStored(dialChoiceAddress, nil); err != nil || svc.secureSessions.marks.awaitsPinRoom(dialChoiceAddress) {
		t.Fatal("nil persistence outcome became a refusal")
	}
	for _, cause := range []error{context.Canceled, domain.ErrIdentityPinRevisionExhausted} {
		err := svc.notePinNotStored(dialChoiceAddress, cause)
		if !errors.Is(err, cause) || !errors.Is(err, errPinProtectionLocalFailure) || !isUnchargedDialRefusal(err) || !localAddressDialOutcome(err) {
			t.Fatalf("local revision/cancellation identity lost: %v", err)
		}
		if svc.secureSessions.marks.awaitsPinRoom(dialChoiceAddress) {
			t.Fatal("non-capacity failure started full-store pacing")
		}
	}
	full := fmt.Errorf("fixture: %w", errPinStoreFull)
	if got := svc.notePinNotStored(dialChoiceAddress, full); got != full || errors.Is(got, errPinProtectionLocalFailure) {
		t.Fatalf("full store lost its original separate outcome: %v", got)
	}
	if !svc.secureSessions.marks.awaitsPinRoom(dialChoiceAddress) {
		t.Fatal("full store lost its existing wait for room")
	}
}

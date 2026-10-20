package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/mock"
)

type n1ServiceFixture struct {
	service *Service
	dialer  *Service
	clock   *ninFixtureClock
}

func n1NewServiceFixture(t *testing.T) n1ServiceFixture {
	t.Helper()
	c := new(ninFixtureClock)
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", ListenerSet: true, ListenerEnabled: true, EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull, SecureSessionStorePath: filepath.Join(t.TempDir(), "pins.json")})
	seed := ninOpenFixture(t, cfg.SecureSessionStorePath, c)
	files := ninFilesFixture(t, seed)
	ninInstallVirginFixture(t, seed, files)
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := newInboundPinCandidateService(cfg, id, nil, inboundPinClock{elapsed: c.read})
	if err != nil {
		t.Fatal(err)
	}
	// N1 tests own original accepted transport lifetimes, not route-grace
	// timers. Run withdrawal inline BEFORE any acceptance/Run worker exists.
	svc.routeWithdrawalGracePeriodTest = -1
	if svc.inboundPins == nil {
		t.Fatal("private candidate unavailable")
	}
	dialerID, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dc := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, Type: domain.NodeTypeFull})
	dialer := NewService(dc, dialerID, nil)
	if dialer.inboundPins != nil {
		t.Fatal("ordinary constructor activated N1")
	}
	// Complete current loaded-journal strict obligation; no TLS/maturity created.
	if err := svc.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		svc.runCancel()
		dialer.runCancel()
		svc.closeBackgroundAdmission()
		svc.WaitBackground()
		dialer.closeBackgroundAdmission()
		dialer.WaitBackground()
	})
	return n1ServiceFixture{svc, dialer, c}
}

type n1ConnectionFixture struct {
	n1ServiceFixture
	wire      *peekedConn
	reader    *bufio.Reader
	owner     *proofTransportOwner
	client    net.Conn
	rawClient net.Conn
	rawReads  *atomic.Int64
	rawErrors *atomic.Int64
	rawWrite  *n1HeldWrite
}

// Real accepted TCP/TLS proof -> original mandatory pin -> real registry ->
// real auth publication. Only the final shared publisher is deliberately held
// by the test; no test assigns Published or manufactures binding/proof.
func n1AcceptedFixture(t *testing.T) n1ConnectionFixture {
	t.Helper()
	return n1AcceptedOnService(t, n1NewServiceFixture(t))
}

func n1AcceptedOnService(t *testing.T, f n1ServiceFixture) n1ConnectionFixture {
	t.Helper()
	return n1AcceptedWithWrite(t, f, nil)
}
func n1AcceptedWithWrite(t *testing.T, f n1ServiceFixture, gate *n1HeldWrite) n1ConnectionFixture {
	t.Helper()
	server, client := tcpPair(t)
	reads := new(atomic.Int64)
	failures := new(atomic.Int64)
	tap := &n1ReadTap{Conn: server, successful: reads, failed: failures, writeGate: gate}
	metered, err := netcore.NewMeteredConn(tap, &f.service.transportTotals)
	if err != nil {
		t.Fatal(err)
	}
	type accepted struct {
		wire   net.Conn
		reader *bufio.Reader
		peer   *sessionv2.Peer
		err    error
	}
	result := make(chan accepted, 1)
	opened := make(chan struct{})
	go func() {
		defer close(opened)
		wire, reader, peer, err := f.service.openInboundTransport(metered, metered)
		result <- accepted{wire, reader, peer, err}
	}()
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
		select {
		case <-opened:
		case <-time.After(2 * time.Second):
			t.Error("real accepted transport worker not joined")
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dialed, err := sessionv2.Dial(ctx, client, f.dialer.secureSessions.local, f.dialer.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	var got accepted
	select {
	case got = <-result:
	case <-ctx.Done():
		t.Fatal("actual accept did not return", ctx.Err())
	}
	if got.err != nil || got.peer == nil {
		t.Fatal("actual proof failed", got.err)
	}
	wire, ok := got.wire.(*peekedConn)
	if !ok || wire.common.Load() == nil || wire.common.Load().nin == nil {
		t.Fatal("real accepted proof did not acquire original candidate wire")
	}
	if err := f.service.registerInboundConn(wire, nil); err != nil {
		t.Fatal(err)
	}
	id, owner := f.service.inboundTerminalForWire(wire)
	if id == 0 || owner == nil {
		t.Fatal("original registration did not mint owner")
	}
	protected, err := dialed.Conn()
	if err != nil {
		t.Fatal(err)
	}
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _, _ = io.Copy(io.Discard, protected) }()
	t.Cleanup(func() {
		_ = protected.Close()
		_ = f.service.Network().Close(context.Background(), id)
		f.service.unregisterInboundConn(wire)
		select {
		case <-drainDone:
		case <-time.After(2 * time.Second):
			t.Error("peer TLS drain not joined")
		}
	})
	if !f.service.acceptProvenInboundOwned(id, *got.peer, wire.RemoteAddr().String(), owner) {
		t.Fatal("real accepted auth metadata not published")
	}
	return n1ConnectionFixture{n1ServiceFixture: f, wire: wire, reader: got.reader, owner: owner, client: protected, rawClient: client, rawReads: reads, rawErrors: failures, rawWrite: gate}
}

func n1AssertNoEarned(t *testing.T, f n1ConnectionFixture) {
	t.Helper()
	s := f.service.localContactPinStore()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inboundAllocations != 0 || s.ninJournal.row(0).count != 0 {
		t.Fatal("terminal/immature session earned class/debit")
	}
}

func n1AssertTerminal(t *testing.T, owner *proofTransportOwner) {
	t.Helper()
	w := owner.life.nin
	w.transport.mu.Lock()
	defer w.transport.mu.Unlock()
	if w.phase != inboundTerminal || !w.transport.ended || w.producer != nil || w.transport.entry != nil || w.transport.evidence != nil || w.transport.peer != nil || w.binding.owner != nil {
		t.Fatal("terminal retained authority chain")
	}
	if _, err := w.session.Peer(); !errors.Is(err, sessionv2.ErrZeroSession) {
		t.Fatal("terminal retained original TLS proof")
	}
}

func TestInboundN1OriginalPublicationAgeAndDefaultOff(t *testing.T) {
	f := n1AcceptedFixture(t)
	f.clock.advance(10 * time.Minute)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("original accepted owner did not publish")
	}
	f.owner.life.mu.Lock()
	ready := f.owner.life.nin.ready
	f.owner.life.mu.Unlock()
	if ready != 10*time.Minute {
		t.Fatal("pre-publication handshake/auth time was credited", ready)
	}
	if err := f.service.inboundPins.pass(t.Context()); !errors.Is(err, errNinWitness) {
		t.Fatal("immediate publication earned maturity", err)
	}
	f.clock.advance(10*time.Minute - time.Nanosecond)
	if err := f.service.inboundPins.pass(t.Context()); !errors.Is(err, errNinWitness) {
		t.Fatal("ten-minute boundary expired early", err)
	}
	n1AssertNoEarned(t, f)
	f.clock.advance(time.Nanosecond)
	if err := f.service.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.service.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	s := f.service.localContactPinStore()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inboundAllocations != 1 || s.inboundConfirmed != 1 || s.ninJournal.row(0).count != 1 {
		t.Fatal("actual original lifetime did not earn+confirm exactly once")
	}
}

func n1WaitPublisherMutex(t *testing.T, gid uint64) {
	t.Helper()
	n1WaitMutexFrame(t, gid, "publishInboundLifetime(")
}

func n1WaitMutexFrame(t *testing.T, gid uint64, frame string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for size := 128 << 10; size <= 8<<20; size *= 2 {
			buf := make([]byte, size)
			n := runtime.Stack(buf, true)
			if n == len(buf) {
				continue
			}
			prefix := "goroutine " + strconv.FormatUint(gid, 10) + " ["
			for _, block := range strings.Split(string(buf[:n]), "\n\n") {
				if strings.HasPrefix(block, prefix) && strings.Contains(block, frame) && strings.Contains(block, "sync.") {
					return
				}
			}
			break
		}
		runtime.Gosched()
	}
	t.Fatal("actual original owner not observed waiting on held mutex", frame)
}

func TestInboundN1PublicationReadingFollowsBothMutexWaits(t *testing.T) {
	for _, kind := range []string{"peer", "leaf"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			var release func()
			if kind == "peer" {
				f.service.peerMu.Lock()
				release = f.service.peerMu.Unlock
			} else {
				f.owner.life.mu.Lock()
				release = f.owner.life.mu.Unlock
			}
			held := true
			done := make(chan bool, 1)
			closed := make(chan struct{})
			entry := make(chan uint64, 1)
			t.Cleanup(func() {
				if held {
					release()
					held = false
				}
				select {
				case <-closed:
				case <-time.After(2 * time.Second):
					t.Error("original publisher not joined")
				}
			})
			reads := f.clock.reads.Load()
			go func() {
				defer close(closed)
				entry <- relationGateCurrentGID()
				done <- f.service.publishInboundLifetime(f.owner)
			}()
			var gid uint64
			select {
			case gid = <-entry:
			case <-time.After(2 * time.Second):
				t.Fatal("publisher entry missing")
			}
			n1WaitPublisherMutex(t, gid)
			if f.clock.reads.Load() != reads {
				t.Fatal("publication sampled elapsed before owner mutex wait completed")
			}
			f.clock.advance(10 * time.Minute)
			release()
			held = false
			select {
			case ok := <-done:
				if !ok {
					t.Fatal("actual publisher refused after lock release")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("publisher did not complete")
			}
			f.owner.life.mu.Lock()
			ready := f.owner.life.nin.ready
			f.owner.life.mu.Unlock()
			if ready != 10*time.Minute {
				t.Fatal("wait time became prepublication age", ready)
			}
			if err := f.service.inboundPins.pass(t.Context()); !errors.Is(err, errNinWitness) {
				t.Fatal("held publication time earned maturity", err)
			}
		})
	}
}

func TestInboundN1EndBeforePublicationOrStageNeverReactivates(t *testing.T) {
	for _, kind := range []string{"provisional", "published"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			if kind == "published" && !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			f.clock.advance(10 * time.Minute)
			f.owner.end()
			f.owner.end()
			n1AssertTerminal(t, f.owner)
			if f.service.publishInboundLifetime(f.owner) {
				t.Fatal("terminal owner republished")
			}
			if err := f.service.inboundPins.pass(t.Context()); err != nil {
				t.Fatal(err)
			}
			n1AssertNoEarned(t, f)
		})
	}
}

func TestInboundN1ActualTLSCloseNotifyEndsBeforeReadCaller(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	f.clock.advance(10 * time.Minute)
	before := f.rawReads.Load()
	rawFailures := f.rawErrors.Load()
	writer, ok := f.client.(interface{ CloseWrite() error })
	if !ok {
		t.Fatal("actual proven TLS did not expose CloseWrite")
	}
	if err := writer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// TLS decoded EOF follows an authenticated raw record, not raw TCP EOF.
	if _, err := f.reader.ReadString('\n'); !errors.Is(err, io.EOF) {
		t.Fatal("actual TLS close_notify did not yield EOF", err)
	}
	if f.rawReads.Load() <= before {
		t.Fatal("TLS EOF lacked successful encrypted raw read")
	}
	if f.rawErrors.Load() != rawFailures {
		t.Fatal("close_notify witness observed raw error instead of decoded-only EOF")
	}
	n1AssertTerminal(t, f.owner)
	if err := f.service.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	n1AssertNoEarned(t, f)
}

func TestInboundN1StageEndLinearizationAndStrictObligation(t *testing.T) {
	for _, kind := range []string{"end-preflight", "stage-first", "cancel-lock"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			if !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			f.clock.advance(10 * time.Minute)
			s := f.service.localContactPinStore()
			real := ninFilesFixture(t, s)
			if kind == "end-preflight" {
				files := NewMockidentityPinDurableFiles(t)
				files.EXPECT().Preflight(mock.Anything).RunAndReturn(func(ctx context.Context) error { f.owner.end(); return real.Preflight(ctx) }).Once()
				if _, err := s.stageInboundPin(t.Context(), f.owner.life.nin, files); !errors.Is(err, errNinWitness) {
					t.Fatal("End during preflight earned", err)
				}
				n1AssertNoEarned(t, f)
				return
			}
			if kind == "cancel-lock" {
				s.pinMutationGate.Lock()
				held := true
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				joined := make(chan struct{})
				entry := make(chan uint64, 1)
				t.Cleanup(func() {
					cancel()
					if held {
						s.pinMutationGate.Unlock()
					}
					select {
					case <-joined:
					case <-time.After(2 * time.Second):
						t.Error("stage not joined")
					}
				})
				go func() {
					defer close(joined)
					entry <- relationGateCurrentGID()
					_, err := s.stageInboundPin(ctx, f.owner.life.nin, real)
					done <- err
				}()
				var gid uint64
				select {
				case gid = <-entry:
				case <-time.After(2 * time.Second):
					t.Fatal("stage entry missing")
				}
				n1WaitMutexFrame(t, gid, "stageInboundPin(")
				cancel()
				s.pinMutationGate.Unlock()
				held = false
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("stage not joined")
				}
				n1AssertNoEarned(t, f)
				return
			}

			stage, err := s.stageInboundPin(t.Context(), f.owner.life.nin, real)
			if err != nil || stage.state != inboundPinCountedUnconfirmed {
				t.Fatal("stage failed", err)
			}
			f.owner.end()
			n1AssertTerminal(t, f.owner)
			unknown := NewMockidentityPinDurableFiles(t)
			unknown.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			unknown.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, errors.New("unknown")).Once()
			if _, err := s.confirmPinMetadata(t.Context(), unknown); err == nil {
				t.Fatal("unknown strict write ACKed")
			}
			if err := f.service.inboundPins.pass(t.Context()); err != nil {
				t.Fatal("global strict owed state lost ended witness", err)
			}
			s.mu.Lock()
			count, confirmed, events := s.inboundAllocations, s.inboundConfirmed, s.ninJournal.row(0).count
			s.mu.Unlock()
			if count != 1 || confirmed != 1 || events != 1 {
				t.Fatal("End refunded/redebit or lost strict obligation", count, confirmed, events)
			}
		})
	}
}

func TestInboundN1LogicalTerminalOwnedBodiesAndNilWrappers(t *testing.T) {
	for _, kind := range []string{"non-json", "parse", "unknown", "ack-invalid", "close", "unregister", "shutdown", "condemn"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			if !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			switch kind {
			case "non-json":
				f.service.handleCommandOwned(f.owner.id, "not-json", f.owner)
			case "parse":
				f.service.dispatchNetworkFrameOwned(f.owner.id, "{broken}", f.owner)
			case "unknown":
				f.service.dispatchNetworkFrameOwned(f.owner.id, `{"type":"unknown-n1-command"}`, f.owner)
			case "ack-invalid":
				f.service.handleAckDeleteFrameOwned(f.owner.id, protocol.Frame{}, f.owner)
			case "close":
				_ = f.service.Network().Close(t.Context(), f.owner.id)
			case "unregister":
				f.service.unregisterInboundConn(f.wire)
			case "shutdown":
				f.service.closeAllInboundConns()
			case "condemn":
				f.service.loopbackInbound.track(f.owner.id)
				if !f.service.condemnInboundSelected(f.owner.id) {
					t.Fatal("condemn failed")
				}
			}
			n1AssertTerminal(t, f.owner)
			n1AssertNoEarned(t, f)
		})
	}
}

// A genuine incoming owner stays local to the synchronous delivery stack.
// Generic datagram ingress has no capsule and cannot mint one from a ConnID.
func TestInboundN1ArrivalContextDoesNotRebindOriginalOwner(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	ctx := acceptedTransportArrivalContext(t.Context(), f.service, f.owner.id, f.owner)
	if ctx.Value(proofTransportArrivalKey{}) != f.owner {
		t.Fatal("original capsule not retained")
	}
	if acceptedTransportArrivalContext(t.Context(), f.service, f.owner.id+1, f.owner).Value(proofTransportArrivalKey{}) != nil {
		t.Fatal("wrong channel accepted original capsule")
	}
	f.owner.end()
	if acceptedTransportArrivalContext(t.Context(), f.service, f.owner.id, f.owner).Value(proofTransportArrivalKey{}) != nil {
		t.Fatal("terminal capsule became live")
	}
	n1AssertTerminal(t, f.owner)
}

// Observations only: this tap cannot create a proof, owner, group or clock.
type n1ReadTap struct {
	net.Conn
	successful *atomic.Int64
	failed     *atomic.Int64
	writeGate  *n1HeldWrite
}

func (c *n1ReadTap) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		c.failed.Add(1)
	}
	if n > 0 && err == nil {
		c.successful.Add(1)
	}
	return n, err
}

func TestInboundN1TLSFatalRecordEndsDecodedOwner(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	f.clock.advance(10 * time.Minute)
	before := f.rawReads.Load()
	rawFailures := f.rawErrors.Load()
	// Own raw wire, after genuine mutual proof: a bounded TLS application record
	// with an invalid encrypted body. Peer drain from the fixture consumes the
	// fatal alert, so the test observes T.Read returning rather than TLS blocked
	// internally sending its alert.
	if _, err := f.rawClient.Write([]byte{23, 3, 3, 0, 1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := f.wire.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err := f.reader.ReadString('\n')
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatal("invalid record did not return actual TLS fatal error", err)
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("TLS fatal witness timed out", err)
	}
	if f.rawReads.Load() <= before {
		t.Fatal("TLS fatal error lacked successful encrypted raw read")
	}
	if f.rawErrors.Load() != rawFailures {
		t.Fatal("fatal record witness observed raw error instead of decoded-only TLS error")
	}
	n1AssertTerminal(t, f.owner)
	if err := f.service.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	n1AssertNoEarned(t, f)
}

// This separate generic owned-reader branch is deliberately NOT labelled a
// real TLS n>0+EOF guarantee; TLS may split application bytes and close_notify.
type n1PartialErrorReader struct{}

func (n1PartialErrorReader) Read(p []byte) (int, error) {
	return copy(p, "partial"), io.ErrUnexpectedEOF
}
func TestInboundN1OwnedReadEndsOnPartialError(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	wrapped := &peekedConn{Conn: f.wire.Conn, reader: n1PartialErrorReader{}}
	wrapped.bindProofTransport(f.owner.life)
	n, err := wrapped.Read(make([]byte, 32))
	if n != 7 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("partial fixture did not reach actual wrapper outcome", n, err)
	}
	n1AssertTerminal(t, f.owner)
	n1AssertNoEarned(t, f)
}

func TestInboundN1C3OriginalEntryCannotReattachAfterRemoval(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	f.clock.advance(10 * time.Minute)
	original := f.owner.life.nin.binding
	removeContactPinForTest(t, f.service, original.identity) // real durable C3, not map surgery
	// Reproof may legitimately repin the same identity, but it cannot renew the
	// OLD connection's original entry incarnation or count a new debit for it.
	if _, err := f.service.localContactPinStore().noteProvenInboundBinding(original.identity); err != nil {
		t.Fatal(err)
	}
	s := f.service.localContactPinStore()
	s.mu.Lock()
	entry := s.identities[original.identity]
	same := original.matchesLocked(s)
	s.mu.Unlock()
	if same || entry.incarnation == original.incarnation {
		t.Fatal("C3 reproof reused old entry incarnation")
	}
	if err := f.service.inboundPins.pass(t.Context()); !errors.Is(err, errNinWitness) {
		t.Fatal("old live connection reattached new pin owner", err)
	}
	n1AssertNoEarned(t, f)
}

func TestInboundN1TwoRealSameIdentitySessionsKeepSeparateLifetimes(t *testing.T) {
	first := n1AcceptedFixture(t)
	if !first.service.publishInboundLifetime(first.owner) {
		t.Fatal("publish A failed")
	}
	first.clock.advance(5 * time.Minute)
	second := n1AcceptedOnService(t, first.n1ServiceFixture)
	if !second.service.publishInboundLifetime(second.owner) {
		t.Fatal("publish B failed")
	}
	if first.owner.life.nin == second.owner.life.nin || first.owner.id == second.owner.id {
		t.Fatal("real independent sessions shared lifetime owner")
	}
	first.owner.end()
	first.owner.end()
	n1AssertTerminal(t, first.owner)
	second.owner.life.mu.Lock()
	live := second.owner.life.nin.phase == inboundPublished
	second.owner.life.mu.Unlock()
	if !live {
		t.Fatal("A callback ended B")
	}
	first.clock.advance(5 * time.Minute)
	if err := first.service.inboundPins.pass(t.Context()); !errors.Is(err, errNinWitness) {
		t.Fatal("B inherited A's ten-minute age", err)
	}
	n1AssertNoEarned(t, first)
	first.clock.advance(5 * time.Minute)
	if err := first.service.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	s := first.service.localContactPinStore()
	s.mu.Lock()
	count, events := s.inboundAllocations, s.ninJournal.row(0).count
	s.mu.Unlock()
	if count != 1 || events != 1 {
		t.Fatal("original B did not mature once", count, events)
	}
}

// This fixture runs the UNMODIFIED accepted handler entry, including transport,
// registry, accepted-auth bookkeeping, publication and real reader teardown.
// The earlier fixture deliberately parks only the final publisher for H1.
func TestInboundN1ActualHandlerTLSOwnerEndsBeforeRegistryTeardown(t *testing.T) {
	f := n1NewServiceFixture(t)
	server, client := tcpPair(t)
	handlerDone := make(chan struct{})
	go func() { defer close(handlerDone); f.service.handleConn(server, nil) }()
	var protected net.Conn
	t.Cleanup(func() {
		if protected != nil {
			_ = protected.Close()
		}
		_ = client.Close()
		_ = server.Close()
		select {
		case <-handlerDone:
		case <-time.After(2 * time.Second):
			t.Error("actual accepted handler not joined")
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	proven, err := sessionv2.Dial(ctx, client, f.dialer.secureSessions.local, f.dialer.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	protected, err = proven.Conn()
	if err != nil {
		t.Fatal(err)
	}
	drain := make(chan struct{})
	go func() { defer close(drain); _, _ = io.Copy(io.Discard, protected) }()
	t.Cleanup(func() {
		_ = protected.Close()
		select {
		case <-drain:
		case <-time.After(2 * time.Second):
			t.Error("actual handler peer reader not joined")
		}
	})
	var owner *proofTransportOwner
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.service.peerMu.RLock()
		for id, entry := range f.service.conns {
			if entry.common != nil && entry.common.nin != nil {
				candidate := f.service.transportTerminalLocked(id)
				if candidate != nil {
					candidate.life.mu.Lock()
					published := candidate.life.nin.phase == inboundPublished
					candidate.life.mu.Unlock()
					if published {
						owner = candidate
						break
					}
				}
			}
		}
		f.service.peerMu.RUnlock()
		if owner != nil {
			break
		}
		runtime.Gosched()
	}
	if owner == nil {
		t.Fatal("normal handler did not publish genuine N1 owner")
	}
	f.clock.advance(10 * time.Minute)
	// Deliberately block all later registry/diagnostic/teardown access. The
	// raw/decoded observation and same-leaf End require no peerMu lookup.
	f.service.peerMu.Lock()
	held := true
	t.Cleanup(func() {
		if held {
			f.service.peerMu.Unlock()
		}
	})
	closeWriter, ok := protected.(interface{ CloseWrite() error })
	if !ok {
		t.Fatal("missing real TLS CloseWrite")
	}
	if err := closeWriter.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	terminal := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		owner.life.mu.Lock()
		terminal = owner.life.nin.phase == inboundTerminal
		owner.life.mu.Unlock()
		if terminal {
			break
		}
		runtime.Gosched()
	}
	if !terminal {
		t.Fatal("decoded EOF did not End before blocked handler registry work")
	}
	n1AssertTerminal(t, owner)
	// Use the actually selected original leaf directly: pass intentionally
	// requires peerMu and cannot be used as a diagnostic while it is held.
	store := f.service.localContactPinStore()
	files := ninFilesFixture(t, store)
	if _, err := store.stageInboundPin(t.Context(), owner.life.nin, files); !errors.Is(err, errNinWitness) {
		t.Fatal("selected mature owner earned after actual decoded EOF", err)
	}
	f.service.peerMu.Unlock()
	held = false
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("actual handler did not finish after registry release")
	}
}

func TestInboundN1PendingMetadataAloneDoesNotBecomeGlobalOwed(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	f.service.WaitBackground()
	store := f.service.localContactPinStore()
	identity := f.owner.life.nin.binding.identity
	store.mu.Lock()
	entry := store.identities[identity]
	metadata := entry.classMetadata()
	store.mu.Unlock()
	// Pending reserves one existing raw class place. It is NOT earned Eligible
	// metadata or a journal debit, and cannot make the worker skip maturity for
	// strict confirmation. This is a labelled existing/loaded-metadata fixture,
	// not a new N-in admission; generic CAS is not used to invent eligibility.
	metadata.Inbound = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginInboundSession, SourceGroup: domain.SourceGroupLoopback, At: time.Now()}
	store.pinMutationGate.RLock()
	store.mu.Lock()
	if err := entry.setUnpublishedMetadata(metadata); err != nil {
		store.mu.Unlock()
		store.pinMutationGate.RUnlock()
		t.Fatal(err)
	}
	store.putPinEntryLocked(identity, entry)
	store.changes++
	// Copy values, not the shared metadata pointer: even an in-place mutation
	// must not make both sides of this structural before/after oracle change.
	beforeMetadata := entry.classMetadata()
	beforeRevision, beforeConfirmedRevision := entry.protectionRevision, entry.metadata.confirmedRevision
	beforeCount, beforeConfirmed, beforeEvents := store.inboundAllocations, store.inboundConfirmed, store.ninJournal.row(0).count
	beforeChanges := store.changes
	beforeJournal, beforeJournalConfirmed := store.ninJournal.stamp(), store.ninJournal.confirmed
	store.mu.Unlock()
	store.pinMutationGate.RUnlock()
	if beforeMetadata.Inbound.State != domain.IdentityPinAllocationPending || beforeCount != 1 || beforeConfirmed != 0 || beforeEvents != 0 {
		t.Fatal("Pending fixture must start with exactly one reserved raw place, no earned eligibility/debit/confirmation", beforeCount, beforeConfirmed, beforeEvents)
	}
	if err := f.service.inboundPins.pass(t.Context()); !errors.Is(err, errNinWitness) {
		t.Fatal("Pending alone was treated as global strict owed", err)
	}
	store.mu.Lock()
	after := store.identities[identity]
	afterMetadata := after.classMetadata()
	count, confirmed, events := store.inboundAllocations, store.inboundConfirmed, store.ninJournal.row(0).count
	revision, changes := after.protectionRevision, store.changes
	var confirmedRevision identityPinProtectionRevision
	metadataPresent := after.metadata != nil
	if metadataPresent {
		confirmedRevision = after.metadata.confirmedRevision
	}
	journal, journalConfirmed := store.ninJournal.stamp(), store.ninJournal.confirmed
	store.mu.Unlock()
	// Structural equality intentionally retains the exact immutable timestamp
	// representation and every metadata field; Time.Equal would weaken this.
	if !metadataPresent || !reflect.DeepEqual(afterMetadata, beforeMetadata) || revision != beforeRevision || confirmedRevision != beforeConfirmedRevision || changes != beforeChanges || count != beforeCount || confirmed != beforeConfirmed || events != beforeEvents || journal != beforeJournal || journalConfirmed != beforeJournalConfirmed {
		t.Fatal("immature Pending changed existing reserved place, metadata, eligibility, debit or strict confirmation", count, confirmed, events)
	}
}

func TestInboundN1ClockFaultAndCancellationCannotPublish(t *testing.T) {
	for _, kind := range []string{"clock-error", "negative", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			switch kind {
			case "clock-error":
				f.clock.failed.Store(true)
			case "negative":
				f.clock.tick.Store(-1)
			case "cancelled":
				f.service.runCancel()
			}
			if f.service.publishInboundLifetime(f.owner) {
				t.Fatal("unavailable/cancelled elapsed authority published")
			}
			f.owner.life.mu.Lock()
			phase := f.owner.life.nin.phase
			f.owner.life.mu.Unlock()
			if phase != inboundProvisional {
				t.Fatal("failed publication changed lifetime phase", phase)
			}
			n1AssertNoEarned(t, f)
		})
	}
}

func n1PushWire(t *testing.T, f n1ConnectionFixture) string {
	t.Helper()
	record, _ := f.dialer.SelfIdentityRecord()
	if record.Version == 0 {
		t.Fatal("real dialer own record missing")
	}
	payload, err := protocol.BuildPushIdentityPayload(protocol.PushIdentityPayload{V: domain.IdentityLookupSchemaVersion, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	frame := protocol.DatagramFrame{Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRouted, Class: domain.DatagramClassControl, Src: domain.PeerIdentityFromWire(f.dialer.identity.Address), Dst: domain.PeerIdentityFromWire(f.service.identity.Address), TTL: 1, RoutePolicy: domain.RoutePolicyBest, DType: domain.DTypePushIdentity, Payload: payload, Auth: &protocol.DatagramAuth{AuthVersion: domain.AuthVersionBase, PubKey: append([]byte(nil), f.dialer.identity.PublicKey...), Salt: salt, MaxTTL: 1, Time: time.Now().UTC().Unix()}}
	signed, err := protocol.SignDatagram(frame, f.service.datagramLayer().network, f.dialer.identity.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := protocol.MarshalDatagramFrameLine(signed)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestInboundN1ActualIncomingPipelinePreservesCapsuleAndDropDistinction(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	// Genuine accepted auth -> owned dispatcher -> wire -> line -> actual
	// Pipeline.HandleInbound -> registered push handler. Unique real signatures
	// avoid replay-cache refusal before the application limiter.
	for n := 0; n < 3; n++ {
		if !f.service.dispatchNetworkFrameOwned(f.owner.id, n1PushWire(t, f), f.owner) {
			t.Fatal("push dispatch changed keepalive contract", n)
		}
		f.owner.life.mu.Lock()
		phase := f.owner.life.nin.phase
		f.owner.life.mu.Unlock()
		if n < 2 && phase != inboundPublished {
			t.Fatal("first push or nonterminal first rate drop retired owner", n)
		}
		if n == 2 && phase != inboundTerminal {
			t.Fatal("repeated own-session push refusal lost original capsule")
		}
	}
	n1AssertTerminal(t, f.owner)
	n1AssertNoEarned(t, f)
}

func TestInboundN1AmbiguousDropAndCancelledSendKeepOwnerLive(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	if !f.service.dispatchNetworkFrameOwned(f.owner.id, `{"type":"ping","type":"datagram"}`, f.owner) {
		t.Fatal("ambiguous frame changed keepalive drop contract")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	selected, err := f.service.Network().(*networkBridge).sendFrameBound(ctx, f.owner.id, []byte("ignored\n"), true)
	if !errors.Is(err, context.Canceled) || selected != nil {
		t.Fatal("cancelled operation selected an owner or attempted send", err)
	}
	f.owner.life.mu.Lock()
	live := f.owner.life.nin.phase == inboundPublished
	f.owner.life.mu.Unlock()
	if !live {
		t.Fatal("nonterminal drop/cancel Ended original session")
	}
	f.clock.advance(10 * time.Minute)
	if err := f.service.inboundPins.pass(t.Context()); err != nil {
		t.Fatal("nonterminal live session could not mature", err)
	}
}

func TestInboundN1StaleCollectorEndsActualOriginalBeforeWarning(t *testing.T) {
	for _, kind := range []string{"zero", "fresh", "stale"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			if !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			now := time.Now()
			threshold := f.service.sessionLiveness().inboundSessionIdle
			f.service.peerMu.RLock()
			core := f.service.coreForIDLocked(f.owner.id)
			f.service.peerMu.RUnlock()
			switch kind {
			case "zero":
				core.SetLastActivity(time.Time{})
			case "fresh":
				core.SetLastActivity(now)
			case "stale":
				core.SetLastActivity(now.Add(-threshold))
			}
			f.service.peerMu.Lock()
			selected := f.service.staleInboundTerminalsLocked(now, threshold)
			f.service.peerMu.Unlock()
			if kind != "stale" {
				if len(selected) != 0 {
					t.Fatal("zero/fresh activity selected for stale close")
				}
				f.owner.life.mu.Lock()
				live := f.owner.life.nin.phase == inboundPublished
				f.owner.life.mu.Unlock()
				if !live {
					t.Fatal("fresh owner retired")
				}
				return
			}
			if len(selected) != 1 || selected[0].owner.life.nin != f.owner.life.nin || selected[0].id != f.owner.id {
				t.Fatal("stale collector lost actual selected owner")
			}
			// The facade has returned; no caller warning/Close has run yet.
			n1AssertTerminal(t, f.owner)
			n1AssertNoEarned(t, f)
		})
	}
}

func TestInboundN1CapturedSendOwnerAndActualCloseStayDistinct(t *testing.T) {
	a := n1AcceptedFixture(t)
	if !a.service.publishInboundLifetime(a.owner) {
		t.Fatal("publish A failed")
	}
	b := n1AcceptedOnService(t, a.n1ServiceFixture)
	if !a.service.publishInboundLifetime(b.owner) {
		t.Fatal("publish B failed")
	}
	// Actual bound send selects A with its real core. A delayed eviction can
	// only end that captured owner. A separately selected Close(B) owns B.
	sendOwner, err := a.service.Network().(*networkBridge).sendFrameBound(t.Context(), a.owner.id, []byte("{\"type\":\"ping\"}\n"), true)
	if err != nil || sendOwner == nil || sendOwner.life.nin != a.owner.life.nin {
		t.Fatal("bound send did not retain actual A", err)
	}
	a.service.unregisterInboundConn(a.wire)
	sendOwner.end()
	b.owner.life.mu.Lock()
	liveB := b.owner.life.nin.phase == inboundPublished
	b.owner.life.mu.Unlock()
	if !liveB {
		t.Fatal("delayed A outcome ended B")
	}
	if err := a.service.Network().Close(t.Context(), b.owner.id); err != nil {
		t.Fatal(err)
	}
	n1AssertTerminal(t, b.owner)
	c := n1AcceptedOnService(t, a.n1ServiceFixture)
	if !a.service.publishInboundLifetime(c.owner) {
		t.Fatal("publish C failed")
	}
	sendOwner.end()
	b.owner.end()
	c.owner.life.mu.Lock()
	liveC := c.owner.life.nin.phase == inboundPublished
	c.owner.life.mu.Unlock()
	if !liveC {
		t.Fatal("stale send/Close owner ended later C")
	}
}

func TestInboundN1JoinedHeartbeatRetiresOnlyConnectedNoPong(t *testing.T) {
	for _, kind := range []string{"stopped", "not-connected", "connected-no-pong"} {
		t.Run(kind, func(t *testing.T) {
			base := n1NewServiceFixture(t)
			schedule := productionLiveness()
			schedule.listenerPingInterval = time.Nanosecond
			schedule.maxPingJitter = 0
			schedule.pongStall = time.Nanosecond
			base.service.liveness = &schedule // existing immutable-before-owner fixture port
			f := n1AcceptedOnService(t, base)
			if !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			addr := domain.PeerAddress("192.0.2.41:7654")
			f.service.peerMu.Lock()
			f.service.health[addr] = &peerHealth{Address: addr, Connected: kind == "connected-no-pong"}
			f.service.peerMu.Unlock()
			stop := make(chan struct{})
			stopped := false
			done := make(chan struct{})
			if kind == "stopped" {
				close(stop)
				stopped = true
			}
			t.Cleanup(func() {
				if !stopped {
					close(stop)
					stopped = true
				}
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("actual existing heartbeat not joined")
				}
			})
			go func() { defer close(done); f.service.inboundHeartbeatOwned(f.owner.id, addr, stop, f.owner) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("actual existing heartbeat did not return")
			}
			f.owner.life.mu.Lock()
			terminal := f.owner.life.nin.phase == inboundTerminal
			f.owner.life.mu.Unlock()
			if terminal != (kind == "connected-no-pong") {
				t.Fatal("heartbeat termination verdict changed", kind, terminal)
			}
			n1AssertNoEarned(t, f)
		})
	}
}

func TestInboundN1DefaultConstructorsDoNotInstallOrMigrateJournal(t *testing.T) {
	for _, kind := range []string{"fresh-file", "memory"} {
		t.Run(kind, func(t *testing.T) {
			cfg := configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull})
			if kind == "fresh-file" {
				cfg.SecureSessionStorePath = filepath.Join(t.TempDir(), "pin.json")
			} else {
				cfg.PeersStatePath = ""
			}
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			svc := NewService(cfg, id, nil)
			t.Cleanup(func() { svc.runCancel(); svc.closeBackgroundAdmission(); svc.WaitBackground() })
			if svc.inboundPins != nil {
				t.Fatal("ordinary constructor enabled N1")
			}
			store := svc.localContactPinStore()
			if store == nil {
				t.Fatal("ordinary v2 protection unavailable")
			}
			store.mu.Lock()
			installed := store.ninFormat3 || store.ninJournal != nil
			store.mu.Unlock()
			if installed {
				t.Fatal("ordinary constructor installed/migrated journal")
			}
			// Ordinary mandatory protection still succeeds independently of the absent
			// optional N-in producer. No test bool converts it into N1 eligibility.
			if _, err := store.noteProvenInboundBinding(domain.PeerIdentity{23}); err != nil {
				t.Fatal("ordinary mandatory pinning regressed", err)
			}
			candidate, err := newInboundPinCandidateService(cfg, id, nil, inboundPinClock{})
			t.Cleanup(func() { candidate.runCancel(); candidate.closeBackgroundAdmission(); candidate.WaitBackground() })
			if !errors.Is(err, domain.InboundQuotaNotInstalled) || candidate.inboundPins != nil {
				t.Fatal("private factory migrated absent/legacy journal", err)
			}
			candidateStore := candidate.localContactPinStore()
			candidateStore.mu.Lock()
			attached := candidateStore.ninFormat3 || candidateStore.ninJournal != nil
			candidateStore.mu.Unlock()
			if attached {
				t.Fatal("refused candidate installed journal")
			}

		})
	}
}

func TestInboundN1InitialPushActualQueueFaultEndsBeforeCallback(t *testing.T) {
	for _, kind := range []string{"callback", "no-callback", "no-target"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			if !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			f.service.WaitBackground() // join the actual pre-existing auth jobs first
			layer := f.service.datagramLayer()
			full := false
			for n := 0; n <= layer.limits.Queue.ControlFrames; n++ {
				if !layer.queue.Enqueue(datagram.QueuedFrame{Class: domain.DatagramClassControl, Frame: protocol.DatagramFrame{Class: domain.DatagramClassControl}, Bytes: 1}) {
					full = true
					break
				}
			}
			if !full {
				t.Fatal("bounded actual class queue did not become full")
			}
			peer := domain.PeerIdentityFromWire(f.dialer.identity.Address)
			if kind == "no-target" {
				peer = domain.PeerIdentity{}
			}
			called := false
			var closeSession func()
			if kind != "no-callback" {
				closeSession = func() { called = true; n1AssertTerminal(t, f.owner) }
			}
			f.service.sendInitialIdentityPushOwned(t.Context(), peer, closeSession, f.owner)
			f.owner.life.mu.Lock()
			terminal := f.owner.life.nin.phase == inboundTerminal
			f.owner.life.mu.Unlock()
			want := kind == "callback"
			if called != want || terminal != want {
				t.Fatal("initial push fault/skip/nil callback outcome changed", kind, called, terminal)
			}
			n1AssertNoEarned(t, f)
		})
	}
}

func TestInboundN1OriginalRawEOFEndsWithoutDecodedReader(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	raw := f.owner.life.evidence.raw
	if err := f.rawClient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := raw.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Read(make([]byte, 1)); err == nil {
		t.Fatal("own raw TCP close returned no terminal error")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("raw peer closure witness timed out", err)
	}
	// No W/T.Read has run. The ORIGINAL R hook must observe this itself.
	n1AssertTerminal(t, f.owner)
	n1AssertNoEarned(t, f)
}

func TestInboundN1OversizeBodyKeepsDatagramDropDistinct(t *testing.T) {
	for _, kind := range []string{"datagram", "ping"} {
		t.Run(kind, func(t *testing.T) {
			f := n1AcceptedFixture(t)
			if !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			// Labelled framing-body fixture after actual proof/publication. It does not
			// pretend these string bytes passed through TLS or manufacture a lifetime.
			line := `{"type":"` + kind + `","padding":"` + strings.Repeat("x", maxCommandLineBytes+64) + `"}` + "\n"
			_, err := f.service.readInboundCommandLineOwned(bufio.NewReader(strings.NewReader(line)), f.owner.id, f.owner)
			f.owner.life.mu.Lock()
			terminal := f.owner.life.nin.phase == inboundTerminal
			f.owner.life.mu.Unlock()
			if kind == "datagram" {
				if !errors.Is(err, errInboundLineDropped) || terminal {
					t.Fatal("silent oversize datagram drop became terminal", err, terminal)
				}
			} else {
				if !errors.Is(err, errFrameTooLarge) || !terminal {
					t.Fatal("oversize command lost terminal decision", err, terminal)
				}
			}
			n1AssertNoEarned(t, f)
		})
	}
}

// Owned fixture flow barrier only; it carries no pin/proof/phase authority.
// Arm happens after genuine TLS setup and joins all earlier auth jobs first.
type n1HeldWrite struct {
	arm         chan struct{}
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func n1NewHeldWrite() *n1HeldWrite {
	return &n1HeldWrite{arm: make(chan struct{}), entered: make(chan struct{}), release: make(chan struct{})}
}
func (g *n1HeldWrite) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }
func (c *n1ReadTap) Write(p []byte) (int, error) {
	if c.writeGate != nil {
		select {
		case <-c.writeGate.arm:
			c.writeGate.enterOnce.Do(func() { close(c.writeGate.entered) })
			<-c.writeGate.release
		default:
		}
	}
	return c.Conn.Write(p)
}
func (c *n1ReadTap) Close() error {
	if c.writeGate != nil {
		c.writeGate.unblock()
	}
	return c.Conn.Close()
}

// Read a COMPLETE bounded snapshot; a prefix never proves owner absence.
func n1CompleteStacks(t *testing.T) map[uint64]string {
	t.Helper()
	for size := 128 << 10; size <= 8<<20; size *= 2 {
		raw := make([]byte, size)
		n := runtime.Stack(raw, true)
		if n == len(raw) {
			continue
		}
		result := map[uint64]string{}
		for _, block := range strings.Split(string(raw[:n]), "\n\n") {
			header := strings.SplitN(block, "\n", 2)[0]
			parts := strings.Fields(header)
			if len(parts) > 1 && parts[0] == "goroutine" {
				id, err := strconv.ParseUint(parts[1], 10, 64)
				if err == nil {
					result[id] = block
				}
			}
		}
		return result
	}
	t.Fatal("complete original-owner stack exceeds explicit8MiB bound")
	return nil
}

func TestInboundN1ServiceRunOwnsActualBlockedProducerAndJoin(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	f.clock.advance(10 * time.Minute)
	store := f.service.localContactPinStore()
	files := ninFilesFixture(t, store)
	if _, err := store.stageInboundPin(t.Context(), f.owner.life.nin, files); err != nil {
		t.Fatal(err)
	}
	f.owner.end() // earned strict obligation survives the original transport
	f.service.WaitBackground()
	// The existing bootstrap fixture channel prevents a periodic flush from
	// obscuring WHICH Run-owned worker is held on the strict writer.
	f.service.bootstrapTickOverride = make(chan time.Time)
	store.writeMu.Lock()
	held := true
	parent, cancel := context.WithCancel(t.Context())
	runGID := make(chan uint64, 1)
	var worker uint64
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() { defer close(joined); runGID <- relationGateCurrentGID(); done <- f.service.Run(parent) }()
	t.Cleanup(func() {
		cancel()
		if held {
			store.writeMu.Unlock()
			held = false
		}
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("actual candidate Run not joined")
		}
		// Also reap a deliberately detached-worker causal mutant. A complete
		// snapshot proves the observed actual GID left after releasing its lock.
		if worker != 0 {
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if _, alive := n1CompleteStacks(t)[worker]; !alive {
					return
				}
				runtime.Gosched()
			}
			t.Error("observed actual N1 worker survived cleanup")
		}
	})
	var run uint64
	select {
	case run = <-runGID:
	case <-time.After(2 * time.Second):
		t.Fatal("Run owner entry missing")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for id, stack := range n1CompleteStacks(t) {
			if strings.Contains(stack, "(*inboundPinProducer).runWithFiles(") && strings.Contains(stack, "confirmPinMetadata(") && strings.Contains(stack, "sync.") {
				if worker != 0 && worker != id {
					t.Fatal("more than one actual candidate producer entered strict operation")
				}
				worker = id
				// Actual goRunLoop frame + original creator GID, not just a cancellation
				// request or another shutdown flush that also happens to wait on writeMu.
				if !strings.Contains(stack, "(*Service).goRunLoop.func1(") || !strings.Contains(stack, "in goroutine "+strconv.FormatUint(run, 10)) {
					t.Fatal("N1 producer is detached from the actual Run lifecycle owner")
				}
			}
		}
		if worker != 0 {
			break
		}
		runtime.Gosched()
	}
	if worker == 0 {
		t.Fatal("actual Run never started its N1 strict worker")
	}
	cancel()
	waiting := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stacks := n1CompleteStacks(t)
		target := stacks[run]
		workerStack := stacks[worker]
		if workerStack == "" {
			t.Fatal("held N1 dependency returned before release")
		}
		if strings.Contains(target, "stopRunLifecycle(") && strings.Contains(target, "sync.") {
			waiting = true
			break
		}
		select {
		case <-joined:
			t.Fatal("Run returned before its held N1 worker")
		default:
		}
		runtime.Gosched()
	}
	if !waiting {
		t.Fatal("Run was not observed joining N1 at its actual lifecycle wait (a shutdown flush is not that join)")
	}
	select {
	case <-joined:
		t.Fatal("Run falsely completed while N1 strict dependency held")
	default:
	}
	store.writeMu.Unlock()
	held = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish after its N1 worker was released")
	}
	<-joined
	if _, exists := n1CompleteStacks(t)[worker]; exists {
		t.Fatal("actual N1 worker survived Run join")
	}
	for n := 0; n < 1000; n++ {
		f.service.inboundPins.notify()
	}
	runtime.Gosched()
	store.mu.Lock()
	count, events, confirmed := store.inboundAllocations, store.ninJournal.row(0).count, store.inboundConfirmed
	store.mu.Unlock()
	if count != 1 || events != 1 || confirmed != 0 {
		t.Fatal("cancelled/finished producer revived, lost owed debt or falsely ACKed", count, events, confirmed)
	}
}

type n1StrictReply struct {
	commit secretfile.DurableCommit
	err    error
}
type n1StrictCall struct {
	at    time.Time
	raw   []byte
	reply chan n1StrictReply
}

// Existing generated durable port, BORROWED by the common scheduler. Returning
// a mock outcome tests ACK/ordering contracts, never actual kernel durability.
func n1BorrowedStrict(t *testing.T) (identityPinDurableFiles, chan n1StrictCall) {
	t.Helper()
	files := NewMockidentityPinDurableFiles(t)
	calls := make(chan n1StrictCall, 4)
	files.EXPECT().Preflight(mock.Anything).Return(nil)
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, raw []byte) (secretfile.DurableCommit, error) {
		call := n1StrictCall{at: time.Now(), raw: append([]byte(nil), raw...), reply: make(chan n1StrictReply, 1)}
		calls <- call
		answer := <-call.reply
		return answer.commit, answer.err
	})
	files.EXPECT().Close().Return(nil).Once()
	t.Cleanup(func() { _ = files.Close() })
	return files, calls
}
func n1Reply(call n1StrictCall, commit secretfile.DurableCommit, err error) {
	select {
	case call.reply <- n1StrictReply{commit, err}:
	default:
	}
}

func TestInboundN1ActualSchedulerCoalescesWakeAndPacesAfterCompletion(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	f.clock.advance(10 * time.Minute)
	f.service.WaitBackground()
	synctest.Test(t, func(t *testing.T) {
		files, calls := n1BorrowedStrict(t)
		wake := f.service.inboundPins.wake
		f.service.inboundPins.wake = make(chan struct{}, 1)
		defer func() { f.service.inboundPins.wake = wake }()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var held []n1StrictCall
		defer func() { n1JoinBorrowedScheduler(t, cancel, done, calls, held) }()
		go func() { defer close(done); f.service.inboundPins.runWithFiles(ctx, files) }()
		synctest.Wait()
		var first n1StrictCall
		select {
		case first = <-calls:
			held = append(held, first)
		default:
			t.Fatal("actual scheduler did not enter first paired strict operation")
		}
		if cap(f.service.inboundPins.wake) != 1 {
			t.Fatal("wake ceased to be cap1")
		}
		for n := 0; n < 1000; n++ {
			f.service.inboundPins.notify()
		}
		if len(f.service.inboundPins.wake) != 1 {
			t.Fatal("notifications did not coalesce while work was held")
		}
		// Model an already-issued noninterruptible strict syscall: the existing
		// attempt context expires after2s, but completion is observed at3s.
		time.Sleep(3 * time.Second)
		synctest.Wait()
		select {
		case unexpected := <-calls:
			held = append(held, unexpected)
			t.Fatal("more than one synchronous attempt ran while strict work held")
		default:
		}
		completed := time.Now()
		n1Reply(first, secretfile.DurableIndeterminate, errors.New("unknown strict outcome"))
		synctest.Wait()
		for n := 0; n < 1000; n++ {
			f.service.inboundPins.notify()
		}
		synctest.Wait()
		select {
		case unexpected := <-calls:
			held = append(held, unexpected)
			t.Fatal("wake or pre-completion timer accelerated the retry")
		default:
		}
		time.Sleep(peerSweepInterval - time.Nanosecond)
		synctest.Wait()
		select {
		case unexpected := <-calls:
			held = append(held, unexpected)
			t.Fatal("retry deadline was measured before actual completion")
		default:
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		var second n1StrictCall
		select {
		case second = <-calls:
			held = append(held, second)
		default:
			t.Fatal("actual relative retry did not enter strict confirmation")
		}
		if second.at.Sub(completed) != peerSweepInterval {
			t.Fatal("retry was not relative to actual completion", second.at.Sub(completed))
		}
		n1Reply(second, secretfile.DurableCommitted, nil)
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("actual common worker ignored cancellation")
		}
		for n := 0; n < 1000; n++ {
			f.service.inboundPins.notify()
		}
		time.Sleep(2 * peerSweepInterval)
		synctest.Wait()
		select {
		case unexpected := <-calls:
			held = append(held, unexpected)
			t.Fatal("post-join notifications revived strict work")
		default:
		}
	})
}

func TestInboundN1ActualWorkerConfirmsOwedBeforeSecondMatureOwner(t *testing.T) {
	first := n1AcceptedFixture(t)
	if !first.service.publishInboundLifetime(first.owner) {
		t.Fatal("publish first failed")
	}
	first.clock.advance(10 * time.Minute)
	store := first.service.localContactPinStore()
	if _, err := store.stageInboundPin(t.Context(), first.owner.life.nin, ninFilesFixture(t, store)); err != nil {
		t.Fatal(err)
	}
	first.owner.end()
	otherID, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	other := NewService(configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull, EnableDatagramV1: true, ListenAddress: "127.0.0.1:0"}), otherID, nil)
	t.Cleanup(func() { other.runCancel(); other.closeBackgroundAdmission(); other.WaitBackground() })
	second := n1AcceptedOnService(t, n1ServiceFixture{service: first.service, dialer: other, clock: first.clock})
	if !first.service.publishInboundLifetime(second.owner) {
		t.Fatal("publish second failed")
	}
	first.clock.advance(10 * time.Minute)
	second.owner.life.mu.Lock()
	secondIdentity := second.owner.life.nin.binding.identity
	second.owner.life.mu.Unlock()
	first.service.WaitBackground()
	synctest.Test(t, func(t *testing.T) {
		files, calls := n1BorrowedStrict(t)
		wake := first.service.inboundPins.wake
		first.service.inboundPins.wake = make(chan struct{}, 1)
		defer func() { first.service.inboundPins.wake = wake }()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var held []n1StrictCall
		defer func() { n1JoinBorrowedScheduler(t, cancel, done, calls, held) }()
		go func() { defer close(done); first.service.inboundPins.runWithFiles(ctx, files) }()
		for n := 0; n < 3; n++ {
			if n > 0 {
				time.Sleep(peerSweepInterval)
			}
			synctest.Wait()
			var call n1StrictCall
			select {
			case call = <-calls:
				held = append(held, call)
			default:
				t.Fatal("global strict owed was not confirmed before selecting another owner", n)
			}
			image, err := decodeSecureSessionFile(call.raw)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if n == 2 {
				want = 2
			}
			if image.InboundQuota.rows[0].count != uint8(want) {
				t.Fatal("worker debited a second owner before strict owed ACK", n, image.InboundQuota.rows[0].count)
			}
			store.mu.Lock()
			count := store.inboundAllocations
			secondState := store.identities[secondIdentity].classMetadata().Inbound.State
			store.mu.Unlock()
			if count != want || (n < 2 && secondState != domain.IdentityPinAllocationAbsent) {
				t.Fatal("strict-first ordering changed", n, count, secondState)
			}
			if n == 0 {
				n1Reply(call, secretfile.DurableIndeterminate, errors.New("unknown"))
			} else {
				n1Reply(call, secretfile.DurableCommitted, nil)
			}
			synctest.Wait()
		}
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("strict-first worker not joined")
		}
	})
}

// At most two queued/issued calls can exist in these single-worker fixtures;
// four finite drains also cover a causal mutant's immediately queued attempt.
// Cancellation precedes releasing the current syscall; the worker cannot
// issue a fresh successful operation after that cancellation.
func n1JoinBorrowedScheduler(t *testing.T, cancel context.CancelFunc, done <-chan struct{}, calls <-chan n1StrictCall, held []n1StrictCall) {
	t.Helper()
	cancel()
	for _, call := range held {
		n1Reply(call, secretfile.DurableIndeterminate, errors.New("fixture cleanup"))
	}
	for pass := 0; pass < 4; pass++ {
		for draining := true; draining; {
			select {
			case call := <-calls:
				n1Reply(call, secretfile.DurableIndeterminate, errors.New("fixture cleanup"))
			default:
				draining = false
			}
		}
		synctest.Wait()
		select {
		case <-done:
			return
		default:
		}
	}
	t.Error("actual borrowed scheduler not joined after bounded cancellation/drain")
}

// A test-only observer at the ACTUAL diagnostic writer. It creates no
// transport/proof/phase authority and never calls an N1 leaf itself.
type n1DiagnosticBarrier struct {
	address     []byte
	needle      []byte
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (b *n1DiagnosticBarrier) Write(p []byte) (int, error) {
	if bytes.Contains(p, b.needle) && bytes.Contains(p, b.address) {
		b.enterOnce.Do(func() { close(b.entered) })
		<-b.release
	}
	return len(p), nil
}
func (b *n1DiagnosticBarrier) unblock() { b.releaseOnce.Do(func() { close(b.release) }) }

func TestInboundN1ActualEvictionEndsBeforeHeldDiagnosticAndClose(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		name := "async_full"
		if synchronous {
			name = "sync_timeout"
		}
		t.Run(name, func(t *testing.T) {
			gate := n1NewHeldWrite()
			t.Cleanup(gate.unblock)
			f := n1AcceptedWithWrite(t, n1NewServiceFixture(t), gate)
			if !f.service.publishInboundLifetime(f.owner) {
				t.Fatal("publish failed")
			}
			f.clock.advance(10 * time.Minute)
			f.service.WaitBackground()
			f.service.peerMu.RLock()
			core := f.service.coreForIDLocked(f.owner.id)
			f.service.peerMu.RUnlock()
			if core == nil {
				t.Fatal("original actual core missing")
			}
			payload := []byte("{\"type\":\"ping\"}\n")
			// A real sync flush drains prior auth/background writes before arming the
			// write barrier; queue capacity below therefore belongs only to this test.
			if core.SendRawSyncCtx(t.Context(), payload) != netcore.SendOK {
				t.Fatal("prior actual auth writes did not flush")
			}
			close(gate.arm)
			if core.SendRaw(payload) != netcore.SendOK {
				t.Fatal("cannot prime original writer")
			}
			select {
			case <-gate.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("actual TLS raw writer did not enter held write")
			}
			if !synchronous {
				_, capacity := core.WriterQueue()
				for n := 0; n < capacity; n++ {
					if core.SendRaw(payload) != netcore.SendOK {
						t.Fatal("bounded original queue could not be filled", n)
					}
				}
				if queued, capacity := core.WriterQueue(); queued != capacity {
					t.Fatal("original queue not full")
				}
			}
			needle := "protocol_trace"
			if synchronous {
				needle = "sync flush timeout, disconnecting peer"
			}
			diagnostic := &n1DiagnosticBarrier{needle: []byte(needle), address: []byte(`"addr":` + strconv.Quote(core.RemoteAddr())), entered: make(chan struct{}), release: make(chan struct{})}
			previous, level := log.Logger, zerolog.GlobalLevel()
			log.Logger = zerolog.New(diagnostic)
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
			done := make(chan error, 1)
			joined := make(chan struct{})
			// Registered BEFORE starting the actual consumer; every Fatal releases both
			// dependencies and joins the same owned caller before restoring logging.
			t.Cleanup(func() {
				gate.unblock()
				diagnostic.unblock()
				select {
				case <-joined:
				case <-time.After(2 * time.Second):
					t.Error("actual eviction caller not joined")
				}
				log.Logger = previous
				zerolog.SetGlobalLevel(level)
			})
			go func() {
				defer close(joined)
				if synchronous {
					done <- f.service.sendFrameBytesViaNetworkSync(t.Context(), f.owner.id, payload)
				} else {
					done <- f.service.sendFrameViaNetwork(t.Context(), f.owner.id, protocol.Frame{Type: "ping"})
				}
			}()
			limit := 2 * time.Second
			if synchronous {
				limit = 7 * time.Second
			} // existing real5s flush + phase margin
			select {
			case <-diagnostic.entered:
			case <-time.After(limit):
				t.Fatal("actual full/timeout diagnostic was not entered")
			}
			n1AssertTerminal(t, f.owner)
			if writerReturned(core) {
				t.Fatal("Close already completed before held diagnostic")
			}
			store := f.service.localContactPinStore()
			if _, err := store.stageInboundPin(t.Context(), f.owner.life.nin, ninFilesFixture(t, store)); !errors.Is(err, errNinWitness) {
				t.Fatal("eviction did not retire original proof before diagnostic", err)
			}
			n1AssertNoEarned(t, f)
			// Release write BEFORE allowing the diagnostic to reach actual Close.
			gate.unblock()
			diagnostic.unblock()
			select {
			case err := <-done:
				want := netcore.ErrSendBufferFull
				if synchronous {
					want = netcore.ErrSendTimeout
				}
				if !errors.Is(err, want) {
					t.Fatal("actual eviction sentinel changed", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("actual eviction did not complete after release")
			}
			<-joined
			if !writerReturned(core) {
				t.Fatal("actual eviction Close did not join the original writer")
			}
		})
	}
}

// CONTROLLED CONSISTENCY fixture, not production ConnID reuse, a TLS proof, or
// a Published lifetime. Its zero-proof Provisional leaves can ONLY be ended.
// The real successful proof/registry/maturity tests above use distinct IDs.
func TestInboundN1ControlledSameKeyCapturedSendAndCloseNeverRelookup(t *testing.T) {
	f := n1NewServiceFixture(t)
	bridge := &networkBridge{svc: f.service}
	key := domain.ConnID(718001)
	type controlled struct {
		entry *connEntry
		core  *netcore.NetCore
		gate  *n1HeldWrite
		peer  net.Conn
	}
	makeOwner := func() controlled {
		local, peer := net.Pipe()
		gate := n1NewHeldWrite()
		close(gate.arm)
		tap := &n1ReadTap{Conn: local, writeGate: gate}
		t.Cleanup(func() { gate.unblock(); _ = peer.Close() })
		core := newTestNetCore(t, key, tap, netcore.Inbound, netcore.Options{})
		entry := &connEntry{core: core}
		entry.common = &proofTransportLifetime{entry: entry, id: key, token: f.service.proofToken}
		entry.common.nin = &matureInboundWitness{transport: entry.common, phase: inboundProvisional}
		return controlled{entry, core, gate, peer}
	}
	a, b, c := makeOwner(), makeOwner(), makeOwner()
	put := func(value controlled) {
		f.service.peerMu.Lock()
		f.service.conns[key] = value.entry
		f.service.peerMu.Unlock()
	}
	payload := []byte("{\"type\":\"ping\"}\n")
	if a.core.SendRaw(payload) != netcore.SendOK {
		t.Fatal("cannot prime controlled A")
	}
	select {
	case <-a.gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("controlled A writer not held")
	}
	_, capacity := a.core.WriterQueue()
	for n := 0; n < capacity; n++ {
		if a.core.SendRaw(payload) != netcore.SendOK {
			t.Fatal("controlled A queue failed to fill", n)
		}
	}
	put(a)
	selectedA := bridge.selectInboundSend(key)
	if selectedA.core != a.core || selectedA.owner == nil || selectedA.owner.life.nin != a.entry.common.nin {
		t.Fatal("actual send selection did not capture A")
	}
	put(b) // SAME registry key, AFTER actual capture and BEFORE actual send
	sendOwner, sendErr := selectedA.sendCapturedInbound(t.Context(), payload, false)
	if !errors.Is(sendErr, netcore.ErrSendBufferFull) {
		t.Fatal("actual captured A did not fail full", sendErr)
	}
	sendOwner.end() // the consumer's captured result, never a current-key lookup
	if sendOwner == nil || sendOwner.life.nin != a.entry.common.nin {
		t.Fatal("delayed SEND result retired a replacement instead of original A")
	}
	n1AssertTerminal(t, &proofTransportOwner{life: a.entry.common, id: key})
	b.entry.common.mu.Lock()
	phaseB := b.entry.common.nin.phase
	b.entry.common.mu.Unlock()
	if phaseB != inboundProvisional {
		t.Fatal("delayed SEND result ended current B")
	}
	selectedB := bridge.selectInboundClose(key) // End B occurs INSIDE actual selection
	b.entry.common.mu.Lock()
	endedB := b.entry.common.nin.phase == inboundTerminal
	b.entry.common.mu.Unlock()
	if selectedB.core != b.core || !endedB || writerReturned(b.core) {
		t.Fatal("Close did not end selected B before actual close/join")
	}
	put(c) // replacement AFTER Close selection, before its actual core.Close
	if err := selectedB.closeCapturedInbound(); err != nil {
		t.Fatal(err)
	}
	if !writerReturned(b.core) || writerReturned(c.core) {
		t.Fatal("Close relooked up same key and closed C instead of captured B")
	}
	c.entry.common.mu.Lock()
	phaseC := c.entry.common.nin.phase
	c.entry.common.mu.Unlock()
	if phaseC != inboundProvisional {
		t.Fatal("late Close ended C replacement")
	}
	sendOwner.end()
	if writerReturned(c.core) {
		t.Fatal("old result replay touched new C")
	}
	store := f.service.localContactPinStore()
	store.mu.Lock()
	count, events := store.inboundAllocations, store.ninJournal.row(0).count
	store.mu.Unlock()
	if count != 0 || events != 0 {
		t.Fatal("controlled zero-proof fixture acquired class/debit", count, events)
	}
}

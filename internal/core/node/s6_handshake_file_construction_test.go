package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func fileConstructionRequest(dir *secretfile.Dir) HandshakeFileObservationRequest {
	return HandshakeFileObservationRequest{Key: handshakeConstructionKey(), Capacity: 2, ChunkCapacity: 1, Session: [16]byte{141}, Directory: dir, Limits: HandshakeFileObservationLimits{RecordBytes: 2048, StoreBytes: 131072, OrphanBytes: 65536, ScanBytes: 1048576, StoreRecords: 128, OrphanRecords: 64, ScanBatch: 4, ScanSteps: 4096}}
}
func fileConstructionDir(t *testing.T) *secretfile.Dir {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("file observation lease unsupported on this platform")
	}
	dir, err := secretfile.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	return dir
}
func constructFileService(t *testing.T, request HandshakeFileObservationRequest, clock *pendingConstructionClock) (*Service, ObservationReadouts) {
	t.Helper()
	cfg, id := pendingConstructionInputs(t)
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{HandshakeFile: &request}, clock.read)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		if err := s.CloseHandshakeFileObservation(); err != nil {
			t.Error(err)
		}
	})
	if r.HandshakeFile == nil || r.HandshakeFile.service != s || r.HandshakeFile.gate != s.inboundHandshakes || r.HandshakeFile != s.handshakeFileObservation || r.Handshake == nil || r.Handshake.gate != s.inboundHandshakes {
		t.Fatal("file/legacy handle is not the one original Service gate")
	}
	return s, r
}
func captureFile(t *testing.T, o *HandshakeFileObservation) HandshakeFileObservationResult {
	t.Helper()
	v, err := o.Capture()
	if err != nil || v.Status != "COMMITTED_PAST" || v.CommitOutcome != secretfile.DurableCommitted || v.AckStatus != "RETIRED" || v.AttributionBound || !v.PastOnly || v.SchemaVersion != 2 {
		t.Fatal("actual service file capture", v, err)
	}
	return v
}
func fileConstructionRead(t *testing.T, dir *secretfile.Dir, name string) []byte {
	t.Helper()
	f, err := dir.OpenFileForRead(name)
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, 2049))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || len(raw) > 2048 {
		t.Fatal(readErr, closeErr)
	}
	return raw
}

func TestS6HandshakeFileConstructionActualClientHello(t *testing.T) {
	end := time.Now().Add(5 * time.Second)
	dir := fileConstructionDir(t)
	request := fileConstructionRequest(dir)
	clock := &pendingConstructionClock{}
	s, r := constructFileService(t, request, clock)
	o := r.HandshakeFile
	if o.Identity() != domain.PeerIdentityFromWire(s.Address()) || s.StartupRefusal() != nil || s.inboundHandshakes.bounds != productionHandshakeBounds() {
		t.Fatal("owner/bounds/startup changed")
	}
	_ = handshakeFill(t, s.inboundHandshakes, 15) //48 distinct external+15loopback actual owners
	record := handshakeConstructionHello(t, end)
	client, server := net.Pipe()
	notice := &handshakeConstructionWriteNotice{Conn: sourcedConn{Conn: server, remote: addrOf("127.0.0.1")}, writing: make(chan struct{})}
	var totals netcore.TransportTotals
	metered, err := netcore.NewMeteredConn(notice, &totals)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var acceptErr error
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		timer := time.NewTimer(time.Until(end))
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Error("actual file-backed TLS worker failed owned join")
		}
	})
	clock.set(2*time.Second, nil)
	go func() {
		defer close(done)
		conn, _, err := s.acceptSecureInbound(metered, bufio.NewReader(metered))
		acceptErr = err
		if conn != nil {
			_ = conn.Close()
		}
	}()
	if err := client.SetWriteDeadline(end); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(record); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(end))
	defer timer.Stop()
	select {
	case <-notice.writing:
	case <-done:
		t.Fatal("TLS returned before owning full gate")
	case <-timer.C:
		t.Fatal("TLS did not reach original full gate")
	}
	full := captureFile(t, o)
	if full.Checkpoint.Rows != 1 || !full.Checkpoint.ThroughFull || full.Checkpoint.ThroughSequence != 1 || full.Checkpoint.ThroughEntry.Count != 64 || full.Checkpoint.Bounds != (HandshakeObservationBounds{64, 16, 4}) {
		t.Fatal("TLS did not use captured Service gate", full)
	}
	old := fileConstructionRead(t, dir, handshakeFinalName(full.Checkpoint.ID))
	if v := r.Handshake.Snapshot(); v.Status != "UNKNOWN" || v.Cut.Available || len(v.Rows) != 0 || !v.PastOnly {
		t.Fatal("legacy getter certified V2 suffix", v)
	}
	clock.set(3*time.Second, nil)
	_ = client.Close()
	select {
	case <-done:
	case <-timer.C:
		t.Fatal("actual TLS cleanup did not release slot64")
	}
	if acceptErr == nil {
		t.Fatal("incomplete closed TLS unexpectedly succeeded")
	}
	exit := captureFile(t, o)
	if exit.Checkpoint.ThroughFull || !exit.Checkpoint.FromFull || exit.Checkpoint.FromEntry != full.Checkpoint.ThroughEntry || exit.Checkpoint.ThroughSequence != 2 || s.inboundHandshakes.running() != 63 {
		t.Fatal("TLS release/carry lost", exit)
	}
	clock.set(4*time.Second, nil)
	release := handshakeEnterFrom(t, s.inboundHandshakes, "198.18.2.1")
	again := captureFile(t, o)
	clock.set(5*time.Second, nil)
	release()
	release()
	last := captureFile(t, o)
	if again.Checkpoint.ThroughSequence != 3 || last.Checkpoint.ThroughSequence != 4 || s.inboundHandshakes.fullObservation.used != 0 || len(s.inboundHandshakes.fullObservation.rows) != 2 {
		t.Fatal("bounded original gate rollover failed")
	}
	if !bytes.Equal(old, fileConstructionRead(t, dir, handshakeFinalName(full.Checkpoint.ID))) {
		t.Fatal("retirement/reuse removed committed records")
	}
	if v, err := o.Capture(); err != nil || v.Status != "NO_PROGRESS" || v.CommitOutcome != 0 || v.Checkpoint.ID != 0 {
		t.Fatal("same cut fabricated checkpoint", v, err)
	}
}

func TestS6HandshakeFileConstructionKeyCoexistence(t *testing.T) {
	dir := fileConstructionDir(t)
	request := fileConstructionRequest(dir)
	cfg, id := pendingConstructionInputs(t)
	clock := &pendingConstructionClock{}
	pk, tk, gk := pendingConstructionKey(), terminalConstructionKey(), guardConstructionKey()
	total := ConnectionTotalObservationRequest{Key: connectionTotalConstructionKey(), Capacity: 4}
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &pk, Terminal: &tk, Guard: &gk, HandshakeFile: &request, ConnectionTotal: &total}, clock.read)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		if err := r.HandshakeFile.Close(); err != nil {
			t.Error(err)
		}
	})
	if r.Pending == nil || r.Terminal == nil || r.Guard == nil || r.Handshake == nil || r.HandshakeFile == nil || r.ConnectionTotal == nil || r.HandshakeFile.gate != s.inboundHandshakes {
		t.Fatal("one constructor lost a source")
	}
	owner := domain.PeerIdentityFromWire(s.Address())
	if r.HandshakeFile.Identity() != owner || r.Handshake.Identity() != owner || r.Pending.Identity() != owner || r.Terminal.Identity() != owner || r.Guard.Identity() != owner || r.ConnectionTotal.Identity() != owner {
		t.Fatal("multiple Service owners")
	}
	request.Key.Epoch[0] = 0
	request.Key.Anchor = "changed"
	request.Capacity = 0
	request.Session[0] = 0
	request.Directory = nil
	request.Limits.RecordBytes = 0
	if r.HandshakeFile.Key() != handshakeConstructionKey() || r.HandshakeFile.session != ([16]byte{141}) || len(s.inboundHandshakes.fullObservation.rows) != 2 {
		t.Fatal("caller mutated frozen request")
	}
	copyKey := r.HandshakeFile.Key()
	copyKey.Configuration[0] = 0
	if r.HandshakeFile.Key() != handshakeConstructionKey() {
		t.Fatal("key aliases caller")
	}
	initial := r.HandshakeFile.Status()
	if initial.Status != "READY" || initial.SchemaVersion != 2 || initial.AttributionBound || !initial.PastOnly {
		t.Fatal("baseline is not explicit no-checkpoint", initial)
	}
	clock.set(time.Second, nil)
	releases := handshakeFill(t, s.inboundHandshakes, 16)
	full := captureFile(t, r.HandshakeFile)
	full.Checkpoint.Source.Epoch[0] = 0
	full.Checkpoint.FromEntry.Sequence = 999
	releases[0]()
	if v := r.HandshakeFile.Status(); v.Checkpoint.Source.Epoch != handshakeConstructionKey().Epoch || v.Checkpoint.FromEntry.Sequence == 999 {
		t.Fatal("result aliases stored checkpoint")
	}
}

func TestS6HandshakeFileConstructionUnavailableDefaults(t *testing.T) {
	cfg, id := pendingConstructionInputs(t)
	called := 0
	s, readouts, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{}, func() (time.Duration, error) { called++; return 0, errors.New("ordinary provider must stay off") })
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	if called != 0 || readouts.HandshakeFile != nil || s.handshakeFileObservation != nil || s.inboundHandshakes.fullObservation != nil {
		t.Fatal("ordinary path activated file observation")
	}
	if err := s.CloseHandshakeFileObservation(); err != nil || called != 0 {
		t.Fatal("ordinary close activated work", err)
	}
	var absent *HandshakeFileObservation
	if v, err := absent.Capture(); err != nil || v.Status != "INACTIVE" || absent.Key() != (HandshakeObservationKey{}) || absent.Err() != nil || absent.Close() != nil {
		t.Fatal("nil handle fabricated work")
	}
	for _, kind := range []string{"nil-directory", "nonfresh", "held-lease", "bad-capacity", "bad-file-caps"} {
		t.Run(kind, func(t *testing.T) {
			dir := fileConstructionDir(t)
			request := fileConstructionRequest(dir)
			switch kind {
			case "nil-directory":
				request.Directory = nil
			case "nonfresh":
				if out, err := dir.DurableWrite("foreign", []byte("occupied")); err != nil || out != secretfile.DurableCommitted {
					t.Fatal(out, err)
				}
			case "held-lease":
				lease, err := dir.TryDirectoryLease()
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := lease.Close(); err != nil {
						t.Error(err)
					}
				}()
			case "bad-capacity":
				request.Capacity = 0
			case "bad-file-caps":
				request.Limits.RecordBytes = 0
			}
			clock := &pendingConstructionClock{}
			s, r := constructFileService(t, request, clock)
			if s.StartupRefusal() != nil || r.HandshakeFile.Status().Status != "UNKNOWN" || r.HandshakeFile.Err() == nil || s.inboundHandshakes.bounds != productionHandshakeBounds() {
				t.Fatal("unavailable observer became gate/startup policy", r.HandshakeFile.Status())
			}
			var releases []func()
			for n := 0; n < 2; n++ {
				releases = append(releases, handshakeEnterFrom(t, s.inboundHandshakes, "198.18.3."+string(rune('1'+n))))
			}
			for _, release := range releases {
				release()
				release()
			}
			if s.inboundHandshakes.running() != 0 {
				t.Fatal("observation error skipped original release")
			}
			if v, err := r.HandshakeFile.Capture(); v.Status != "UNKNOWN" || err == nil || s.StartupRefusal() != nil {
				t.Fatal("I/O unavailable fabricated file coverage", v, err)
			}
			if v := r.Handshake.Snapshot(); v.Status != "UNKNOWN" || v.Cut.Available {
				t.Fatal("unavailable file mode fell back to V1", v)
			}
		})
	}
}

func TestS6HandshakeFileConstructionValidation(t *testing.T) {
	dir := fileConstructionDir(t)
	request := fileConstructionRequest(dir)
	cfg, id := pendingConstructionInputs(t)
	memory := HandshakeObservationRequest{Key: request.Key, Capacity: 2}
	called := 0
	provider := func() (time.Duration, error) { called++; return 0, nil }
	if s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{Handshake: &memory, HandshakeFile: &request}, provider); err == nil || s != nil || r.HandshakeFile != nil || called != 0 {
		t.Fatal("dual mode was selected or reached I/O/provider")
	}
	bad := request
	bad.Key.ArtifactSHA256 = strings.Repeat("0", 64)
	if s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{HandshakeFile: &bad}, provider); err == nil || s != nil || r.HandshakeFile != nil || called != 0 {
		t.Fatal("invalid refs constructed file source")
	}
	lease, err := dir.TryDirectoryLease()
	if err != nil {
		t.Fatal("failed constructor retained lease", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestS6HandshakeFileConstructionDeferredResult(t *testing.T) {
	dir := fileConstructionDir(t)
	request := fileConstructionRequest(dir)
	cfg, id := pendingConstructionInputs(t)
	clock := &pendingConstructionClock{}
	started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(block) })
	var blockNext atomic.Bool
	provider := func() (time.Duration, error) {
		if blockNext.CompareAndSwap(true, false) {
			close(started)
			<-block
		}
		return clock.read()
	}
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{HandshakeFile: &request}, provider)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		if err := r.HandshakeFile.Close(); err != nil {
			t.Error(err)
		}
	})
	clock.set(time.Second, nil)
	releases := handshakeFill(t, s.inboundHandshakes, 16)
	q, status := s.inboundHandshakes.sealHandshakeSegment()
	if status != "OFFERED" {
		t.Fatal(status)
	}
	track := k5OwnedCleanup(t, unblock, func() {
		for _, release := range releases {
			release()
		}
	})
	track(done)
	clock.set(2*time.Second, nil)
	blockNext.Store(true)
	go func() {
		defer close(done)
		release, err := s.inboundHandshakes.enter(handshakeSource{})
		if release != nil || k5RefusedBy(t, err) != refusalHandshakesNode {
			t.Error("original full refusal changed", err)
		}
	}()
	k5Wait(t, started)
	v, err := r.HandshakeFile.Capture()
	if err != nil || v.Status != "DEFERRED" || v.CommitOutcome != secretfile.DurableCommitted || v.AckStatus != "DEFERRED" || v.AckReason != "read-or-completion" || v.Checkpoint.ID != q.checkpointID || s.inboundHandshakes.fullObservation.used != 1 {
		t.Fatal("committed receipt confused with retirement", v, err)
	}
	unblock()
	k5Wait(t, done)
	after := captureFile(t, r.HandshakeFile)
	if after.Checkpoint.ID != v.Checkpoint.ID || after.Digest != v.Digest || s.inboundHandshakes.fullObservation.used != 0 {
		t.Fatal("same Q retry did not retire exact committed data", after)
	}
}

func TestS6HandshakeFileConstructionCloseFence(t *testing.T) {
	dir := fileConstructionDir(t)
	request := fileConstructionRequest(dir)
	clock := &pendingConstructionClock{}
	s, r := constructFileService(t, request, clock)
	o := r.HandshakeFile
	clock.set(time.Second, nil)
	releases := handshakeFill(t, s.inboundHandshakes, 16)
	started, block, captureDone, closeDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(block) })
	track := k5OwnedCleanup(t, unblock, func() {
		for _, release := range releases {
			release()
		}
	})
	track(captureDone)
	track(closeDone)
	real := o.issuer.writeRecord
	o.issuer.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
		if strings.HasSuffix(name, ".final") {
			close(started)
			<-block
		}
		return real(name, raw)
	}
	var captured HandshakeFileObservationResult
	var captureErr, closeErr error
	go func() { defer close(captureDone); captured, captureErr = o.Capture() }()
	k5Wait(t, started)
	go func() { defer close(closeDone); closeErr = s.CloseHandshakeFileObservation() }()
	waitForConditionMsg(t, 3*time.Second, "Close did not fence new Capture", func() bool { return o.closing.Load() })
	if !s.peerMu.TryLock() {
		t.Fatal("Close held Service peer lock")
	}
	s.peerMu.Unlock()
	if !s.inboundHandshakes.mu.TryLock() {
		t.Fatal("Close/Capture I/O held gate lock")
	}
	s.inboundHandshakes.mu.Unlock()
	if v, err := o.Capture(); err == nil || v.Status != "UNKNOWN" || v.Reason != "observation_closed" {
		t.Fatal("late Capture passed fence", v, err)
	}
	select {
	case <-closeDone:
		t.Fatal("Close returned before active file operation joined")
	default:
	}
	if lease, err := dir.TryDirectoryLease(); lease != nil || !errors.Is(err, secretfile.ErrDirectoryLeaseBusy) {
		if lease != nil {
			_ = lease.Close()
		}
		t.Fatal("active Capture released actual store lease", err)
	}
	unblock()
	k5Wait(t, captureDone)
	k5Wait(t, closeDone)
	if captureErr != nil || captured.Status != "COMMITTED_PAST" || closeErr != nil || !o.Status().Closed || o.Status().Status != "CLOSED" {
		t.Fatal("capture/close ownership", captured, captureErr, closeErr, o.Status())
	}
	if err := o.Close(); err != nil {
		t.Fatal("idempotent close", err)
	}
	if err := dir.DurablePreflight(); err != nil {
		t.Fatal("observer closed borrowed Dir", err)
	}
	lease, err := dir.TryDirectoryLease()
	if err != nil {
		t.Fatal("never-Run explicit close did not release lease", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestS6HandshakeFileConstructionRunExit(t *testing.T) {
	dir := fileConstructionDir(t)
	request := fileConstructionRequest(dir)
	cfg, id := pendingConstructionInputs(t)
	cfg.MaxTotalConnections = -1
	clock := &pendingConstructionClock{}
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{HandshakeFile: &request}, clock.read)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		if err := r.HandshakeFile.Close(); err != nil {
			t.Error(err)
		}
	})
	if !errors.Is(s.StartupRefusal(), connbudget.ErrInvalidConfig) {
		t.Fatal("original startup refusal lost")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	var runErr error
	track := k5OwnedCleanup(t, cancel)
	track(done)
	go func() { defer close(done); runErr = s.Run(ctx) }()
	k5Wait(t, done)
	if !errors.Is(runErr, connbudget.ErrInvalidConfig) || !r.HandshakeFile.Status().Closed {
		t.Fatal("early Run return changed policy or leaked observer", runErr, r.HandshakeFile.Status())
	}
	lease, err := dir.TryDirectoryLease()
	if err != nil {
		t.Fatal("Run early-exit lease not released", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestS6HandshakeFileConstructionUnconfirmedNoProgress(t *testing.T) {
	dir := fileConstructionDir(t)
	request := fileConstructionRequest(dir)
	clock := &pendingConstructionClock{}
	_, r := constructFileService(t, request, clock)
	o := r.HandshakeFile
	// Negative readiness-state fixture only, not a claim to inject a kernel
	// sync failure: a still-live issuer has a valid lease/exact identity bytes
	// but has not established runtime initialization confirmation.
	o.issuer.mu.Lock()
	o.issuer.identityConfirmed = false
	boundErr := o.issuer.boundLocked()
	o.issuer.mu.Unlock()
	if boundErr != nil {
		t.Fatal("readiness case lost valid lease", boundErr)
	}
	if v, err := o.Capture(); err == nil || v.Status != "UNKNOWN" || v.Reason != "initial_file_confirmation_unknown" || v.CommitOutcome != 0 {
		t.Fatal("unconfirmed same-clock init looked clean", v, err)
	}
	// A genuinely later offered EMPTY Q may strictly confirm the identity;
	// this path is not permanently disabled by the unavailability result.
	clock.set(time.Second, nil)
	v := captureFile(t, o)
	if v.Checkpoint.Rows != 0 || !o.issuer.identityConfirmed {
		t.Fatal("offered Q failed real same-live strict recovery", v)
	}
	if v, err := o.Capture(); err != nil || v.Status != "NO_PROGRESS" {
		t.Fatal("confirmed same cut", v, err)
	}
}

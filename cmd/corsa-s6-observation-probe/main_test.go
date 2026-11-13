//go:build s6_observation_probe && !d1_resource_probe && (darwin || linux)

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appnode "github.com/piratecash/corsa/internal/app/node"
	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"golang.org/x/sys/unix"
)

const testNonce = "0123456789abcdef0123456789abcdef"

type pipeFixture struct {
	pipes             *controlPipes
	request, response *os.File
}

func ownFD(t *testing.T, file *os.File) int {
	t.Helper()
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		_ = unix.Close(fd)
		t.Fatal(err)
	}
	return fd
}
func fixture(t *testing.T) pipeFixture {
	t.Helper()
	rr, rw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rr.Close(); _ = rw.Close() })
	sr, sw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sr.Close(); _ = sw.Close() })
	in, out := ownFD(t, rr), ownFD(t, sw)
	a, err := inspectPipe(in, true)
	if err != nil {
		t.Fatal(err)
	}
	b, err := inspectPipe(out, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, expected := range []pipeIdentity{a, b} {
			if current, err := inspectPipe(expected.fd, expected.read); err == nil && current == expected {
				_ = unix.Close(expected.fd)
			}
		}
	})
	for _, fd := range []int{in, out} {
		if _, err = unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
			t.Fatal(err)
		}
	}
	p, err := adoptControlPipes(in, out)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	return pipeFixture{p, rw, sr}
}
func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}
func send(t *testing.T, f pipeFixture, b []byte) {
	t.Helper()
	if err := f.request.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err := f.request.Write(b)
	if err != nil || n != len(b) {
		t.Fatal(n, err)
	}
}
func read(t *testing.T, f pipeFixture, r *bufio.Reader) valueFrame {
	t.Helper()
	if err := f.response.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	b, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > responseLimit {
		t.Fatal("output bound")
	}
	var v valueFrame
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func session(t *testing.T, f pipeFixture, r captureReads, mode string, returned *atomic.Bool) (<-chan error, *bufio.Reader) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done, joined := make(chan error, 1), make(chan struct{})
	go func() { defer close(joined); done <- newChannel(f.pipes, testNonce).serveReads(ctx, r, mode, returned) }()
	t.Cleanup(func() {
		cancel()
		f.pipes.close()
		_ = f.request.Close()
		_ = f.response.Close()
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("owned channel worker did not join")
		}
	})
	reader := bufio.NewReader(f.response)
	if v := read(t, f, reader); v.Kind != "CONSTRUCTED" || v.InvokedSources != 0 || v.Sources != nil || v.AttributionVerified || v.JointAtomicCut {
		t.Fatal("construction became readiness", v)
	}
	return done, reader
}
func wait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("channel join")
		return nil
	}
}
func fakeReads(order *[]int) captureReads {
	step := func(i int) { *order = append(*order, i) }
	return captureReads{
		registered: func() observed[corenode.RegisteredProvenV2ObservationSnapshot, corenode.RegisteredProvenV2ObservationKey] {
			step(0)
			return observed[corenode.RegisteredProvenV2ObservationSnapshot, corenode.RegisteredProvenV2ObservationKey]{Snapshot: corenode.RegisteredProvenV2ObservationSnapshot{Status: "UNKNOWN", Qualification: "UNKNOWN", PastOnly: true}}
		},
		advertised: func() observed[corenode.AdvertisedConnectionObservationSnapshot, corenode.AdvertisedConnectionObservationKey] {
			step(1)
			return observed[corenode.AdvertisedConnectionObservationSnapshot, corenode.AdvertisedConnectionObservationKey]{Snapshot: corenode.AdvertisedConnectionObservationSnapshot{Status: "UNKNOWN", Qualification: "UNKNOWN", PastOnly: true}}
		},
		pins: func() observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey] {
			step(2)
			return observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey]{Snapshot: corenode.RawPinMembershipObservationSnapshot{Status: "UNKNOWN", Qualification: "UNKNOWN", PastOnly: true}}
		},
		guard: func() observed[corenode.GuardObservationSnapshot, corenode.GuardObservationKey] {
			step(3)
			v := corenode.GuardObservationSnapshot{}
			v.Status = "IN_FLIGHT"
			return observed[corenode.GuardObservationSnapshot, corenode.GuardObservationKey]{Snapshot: v}
		},
		terminal: func() observed[corenode.TerminalObservationSnapshot, corenode.TerminalObservationKey] {
			step(4)
			return observed[corenode.TerminalObservationSnapshot, corenode.TerminalObservationKey]{Snapshot: corenode.TerminalObservationSnapshot{Status: "COMPLETE", CodeStatus: "UNKNOWN", Coarse: 1}}
		},
		pending: func() observed[domain.PendingQueueObservationSnapshot, corenode.PendingObservationKey] {
			step(5)
			return observed[domain.PendingQueueObservationSnapshot, corenode.PendingObservationKey]{Snapshot: domain.PendingQueueObservationSnapshot{UnknownReason: "source_unavailable"}}
		},
		handshake: func() observed[corenode.HandshakeObservationSnapshot, corenode.HandshakeObservationKey] {
			step(6)
			return observed[corenode.HandshakeObservationSnapshot, corenode.HandshakeObservationKey]{Snapshot: corenode.HandshakeObservationSnapshot{Status: "UNKNOWN", PastOnly: true}}
		},
		total: func() observed[corenode.ConnectionTotalObservationSnapshot, corenode.ConnectionTotalObservationKey] {
			step(7)
			v := corenode.ConnectionTotalObservationSnapshot{}
			v.Status = "DISABLED"
			return observed[corenode.ConnectionTotalObservationSnapshot, corenode.ConnectionTotalObservationKey]{Snapshot: v}
		},
	}
}
func sampleInit() initFrame {
	tok := [16]byte{1}
	hash := strings.Repeat("a", 64)
	h := corenode.HandshakeObservationKey{Anchor: "anchor", ArtifactSHA256: hash, Boot: "boot", ProcessInstance: "process", Epoch: tok, ClockDomain: tok, Configuration: tok}
	b := corenode.ConnectionTotalObservationKey{Anchor: h.Anchor, ArtifactSHA256: hash, Boot: h.Boot, ProcessInstance: h.ProcessInstance, Epoch: tok, ClockDomain: tok, Configuration: tok}
	return initFrame{Version: 1, Nonce: testNonce, Mode: "ON", Request: appnode.S6ObservationProbeRequest{
		RegisteredProvenV2:    corenode.RegisteredProvenV2ObservationRequest{OwnerCapacity: 2, HistoryCapacity: 8, Instance: tok},
		AdvertisedConnections: corenode.AdvertisedConnectionObservationRequest{ConnectionCapacity: 2, SlotCapacity: 2, HistoryCapacity: 8, Instance: tok},
		RawPinMembership:      corenode.RawPinMembershipObservationRequest{BaselineCapacity: 4, HistoryCapacity: 8, Instance: tok},
		Guard:                 corenode.GuardObservationKey{Anchor: h.Anchor, ArtifactSHA256: hash, Boot: h.Boot, ProcessInstance: h.ProcessInstance, ClockDomain: "clock", ClockInstance: tok},
		Terminal:              corenode.TerminalObservationKey{Anchor: h.Anchor, ArtifactSHA256: hash, Boot: h.Boot, ProcessInstance: h.ProcessInstance},
		Pending:               corenode.PendingObservationKey{Anchor: h.Anchor, ArtifactSHA256: hash, Boot: h.Boot, ProcessInstance: h.ProcessInstance, ClockDomain: "clock"},
		Handshake:             corenode.HandshakeObservationRequest{Key: h, Capacity: 8}, ConnectionTotal: corenode.ConnectionTotalObservationRequest{Key: b, Capacity: 8},
	}}
}

func TestS6ProbeEntryArgumentsAndEnvironment(t *testing.T) {
	for _, args := range [][]string{nil, {"--nonce=" + testNonce}, {"--nonce=" + testNonce, "--request-fd=3", "--response-fd=4"}, {"--nonce=" + testNonce, "--request-fd=10", "--response-fd=10"}, {"--nonce=" + testNonce, "--request-fd=010", "--response-fd=11"}, {"--version", "extra"}} {
		if _, err := entryArgs(args); err == nil {
			t.Fatal(args)
		}
	}
	if o, err := entryArgs([]string{"--version"}); err != nil || !o.version {
		t.Fatal(o, err)
	}
	if o, err := entryArgs([]string{"--nonce=" + testNonce, "--request-fd=10", "--response-fd=11"}); err != nil || o.input != 10 || o.output != 11 {
		t.Fatal(o, err)
	}
	env := map[string]string{appdata.DataDirEnv: t.TempDir()}
	get := func(k string) string { return env[k] }
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	if err := entryEnvironment(get, lookup); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"CORSA_STARTUP_FENCE_CANDIDATE", "CORSA_STARTUP_FENCE_FD", "CORSA_STARTUP_FENCE_LAUNCHER", "CORSA_STARTUP_FENCE_STORE"} {
		env[k] = ""
		if err := entryEnvironment(get, lookup); err == nil {
			t.Fatal(k)
		}
		delete(env, k)
	}
	for _, k := range []string{"CORSA_PPROF_ADDR", "CORSA_MEM_LIMIT_BYTES"} {
		env[k] = "x"
		if err := entryEnvironment(get, lookup); err == nil {
			t.Fatal(k)
		}
		delete(env, k)
	}
	now := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	if err := entryRunAllowed(ctx, now.Add(time.Second), now); err != nil {
		t.Fatal(err)
	}
	cancel()
	if entryRunAllowed(ctx, now.Add(time.Second), now) == nil || entryRunAllowed(context.Background(), now, now) == nil {
		t.Fatal("expired construction may call App.Run")
	}
}
func TestS6ProbePipeCustodyAndDeadlines(t *testing.T) {
	f := fixture(t)
	if f.pipes.input.fd < 10 || f.pipes.output.fd < 10 || f.pipes.check() != nil {
		t.Fatal("pipe custody")
	}
	if _, err := inspectPipe(f.pipes.input.fd, false); err == nil {
		t.Fatal("wrong direction")
	}
	if _, err := adoptControlPipes(f.pipes.input.fd, f.pipes.input.fd); err == nil {
		t.Fatal("alias")
	}
	file, err := os.CreateTemp(t.TempDir(), "not-pipe")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if _, err = inspectPipe(int(file.Fd()), true); err == nil {
		t.Fatal("regular file")
	}
	start := time.Now()
	if _, err = newChannel(f.pipes, testNonce).line(requestLimit, start.Add(20*time.Millisecond)); err == nil {
		t.Fatal("missing input passed")
	}
	if time.Since(start) > time.Second {
		t.Fatal("pollable deadline absent")
	}
}
func TestS6ProbeCanonicalSessionAndEightIndependentReads(t *testing.T) {
	init := sampleInit()
	raw := marshal(t, init)
	if len(raw) > initLimit {
		t.Fatal("full8request INIT exceeds4096", len(raw))
	}
	t.Logf("full INIT bytes=%d", len(raw))
	f := fixture(t)
	send(t, f, raw)
	got, err := newChannel(f.pipes, testNonce).readInit(time.Now().Add(time.Second))
	if err != nil || !reflect.DeepEqual(got, init) {
		t.Fatal(err, got)
	}
	if err = validateInit(got, testNonce); err != nil {
		t.Fatal(err)
	}
	got.Request.RegisteredProvenV2.OwnerCapacity = 1025
	if validateInit(got, testNonce) == nil {
		t.Fatal("unbounded capacity")
	}
	var order []int
	var returned atomic.Bool
	done, reader := session(t, f, fakeReads(&order), "ON", &returned)
	for seq := 0; seq < maxReads; seq++ {
		send(t, f, marshal(t, readFrame{1, testNonce, seq, "READ"}))
		v := read(t, f, reader)
		if v.Kind != "CUT" || v.InvokedSources != 8 || v.Sequence != seq || v.Sources == nil || !v.IndependentReads || v.JointAtomicCut || v.AttributionVerified || !v.Provisional {
			t.Fatal(v)
		}
	}
	send(t, f, marshal(t, readFrame{1, testNonce, maxReads, "STOP"}))
	_ = f.request.Close()
	if v := read(t, f, reader); v.Kind != "STOP_REQUESTED" {
		t.Fatal(v)
	}
	if err = wait(t, done); err != nil {
		t.Fatal(err)
	}
	want := []int{}
	for range maxReads {
		want = append(want, 0, 1, 2, 3, 4, 5, 6, 7)
	}
	if !reflect.DeepEqual(order, want) {
		t.Fatal(order)
	}
}
func TestS6ProbePreservesUnknownDisabledAndUnverifiedLabels(t *testing.T) {
	var order []int
	var returned atomic.Bool
	r := fakeReads(&order)
	v, n, err := r.capture(context.Background(), &returned, "ON")
	if err != nil || n != 8 {
		t.Fatal(n, err)
	}
	if v.Registered.Snapshot.Status != "UNKNOWN" || v.Registered.Snapshot.Qualification != "UNKNOWN" || v.Pins.Snapshot.Qualification != "UNKNOWN" || v.Terminal.Snapshot.CodeStatus != "UNKNOWN" || v.Terminal.Snapshot.Coarse != 1 || v.Total.Snapshot.Status != "DISABLED" || v.Pending.Snapshot.CoverageValid || v.Guard.Snapshot.AttributionBound {
		t.Fatal("source state normalized", v)
	}
	off, err := readsForApp(&appnode.App{}, "OFF")
	if err != nil {
		t.Fatal(err)
	}
	if value, n, err := off.capture(context.Background(), &returned, "OFF"); err != nil || value != nil || n != 0 {
		t.Fatal(value, n, err)
	}
	if _, _, err = r.capture(context.Background(), &returned, "OFF"); err == nil {
		t.Fatal("OFF fakecallbacks")
	}
	if _, err = readsForApp(&appnode.App{}, "ON"); err == nil {
		t.Fatal("ON missing handles")
	}
}
func TestS6ProbeRefusesSequenceEOFFramingAndRunReturn(t *testing.T) {
	for _, name := range []string{"duplicate-key", "unknown-key", "wrong-nonce", "wrong-seq", "unknown-kind", "partial-EOF", "extra-after-STOP", "fifth-READ"} {
		t.Run(name, func(t *testing.T) {
			f := fixture(t)
			var order []int
			var returned atomic.Bool
			done, reader := session(t, f, fakeReads(&order), "ON", &returned)
			raw := marshal(t, readFrame{1, testNonce, 0, "READ"})
			switch name {
			case "duplicate-key":
				raw = bytes.Replace(raw, []byte("\"v\":1"), []byte("\"v\":1,\"v\":1"), 1)
			case "unknown-key":
				raw = bytes.Replace(raw, []byte("\"v\":1"), []byte("\"v\":1,\"extra\":0"), 1)
			case "wrong-nonce":
				raw = marshal(t, readFrame{1, strings.Repeat("a", 32), 0, "READ"})
			case "wrong-seq":
				raw = marshal(t, readFrame{1, testNonce, 1, "READ"})
			case "unknown-kind":
				raw = marshal(t, readFrame{1, testNonce, 0, "ACTIVATE"})
			case "partial-EOF":
				raw = raw[:len(raw)-1]
			case "extra-after-STOP":
				raw = append(marshal(t, readFrame{1, testNonce, 0, "STOP"}), marshal(t, readFrame{1, testNonce, 1, "READ"})...)
			case "fifth-READ":
				for seq := 0; seq < maxReads; seq++ {
					send(t, f, marshal(t, readFrame{1, testNonce, seq, "READ"}))
					_ = read(t, f, reader)
				}
				raw = marshal(t, readFrame{1, testNonce, maxReads, "READ"})
			}
			send(t, f, raw)
			if name == "partial-EOF" || name == "extra-after-STOP" {
				_ = f.request.Close()
			}
			if wait(t, done) == nil {
				t.Fatal("invalid session passed")
			}
			if name != "fifth-READ" && len(order) != 0 {
				t.Fatal("invalid request read owners", order)
			}
		})
	}
	var returned atomic.Bool
	var order []int
	r := fakeReads(&order)
	original := r.pins
	r.pins = func() observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey] {
		v := original()
		returned.Store(true)
		return v
	}
	if _, _, err := r.capture(context.Background(), &returned, "ON"); err == nil || !reflect.DeepEqual(order, []int{0, 1, 2}) {
		t.Fatal("observed Run return did not fence", order, err)
	}
	f := fixture(t)
	returned.Store(false)
	done, _ := session(t, f, captureReads{}, "OFF", &returned)
	_ = f.request.Close()
	if err := wait(t, done); err != nil {
		t.Fatal("owned emptyEOF", err)
	}
}

type cancelOnMarshal struct{ cancel context.CancelFunc }

func (v cancelOnMarshal) MarshalJSON() ([]byte, error) {
	v.cancel()
	return []byte(`{"x":1}`), nil
}

func TestS6ProbeEncodingBoundsAndBackpressure(t *testing.T) {
	t.Run("encoding-cancel-before-publication", func(t *testing.T) {
		f := fixture(t)
		c := newChannel(f.pipes, testNonce)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if c.sendBound(ctx, &atomic.Bool{}, cancelOnMarshal{cancel}) == nil || c.written != 0 {
			t.Fatal("encoding crossed cancellation and published")
		}
	})
	t.Run("write-crossing-is-provisional", func(t *testing.T) {
		f := fixture(t)
		c := newChannel(f.pipes, testNonce)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done, joined := make(chan error, 1), make(chan struct{})
		t.Cleanup(func() {
			cancel()
			f.pipes.close()
			_ = f.response.Close()
			select {
			case <-joined:
			case <-time.After(3 * time.Second):
				t.Error("owned write worker did not join")
			}
		})
		go func() {
			defer close(joined)
			done <- c.sendBound(ctx, &atomic.Bool{}, strings.Repeat("z", 128*1024))
		}()
		if err := f.response.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		first := make([]byte, 1)
		if n, err := f.response.Read(first); n != 1 || err != nil {
			t.Fatal("writer never entered publication", n, err)
		}
		cancel()
		rest, err := bufio.NewReader(f.response).ReadBytes('\n')
		if err != nil || len(rest) > responseLimit {
			t.Fatal("bounded write drain", err)
		}
		if err = wait(t, done); err == nil || c.written == 0 {
			t.Fatal("written bytes silently accepted across cancellation", err)
		}
	})
	f := fixture(t)
	c := newChannel(f.pipes, testNonce)
	if c.sendBound(context.Background(), &atomic.Bool{}, strings.Repeat("x", responseLimit)) == nil || c.written != 0 {
		t.Fatal("oversize emitted partial success")
	}
	c.written = channelLimit - 1
	if c.sendBound(context.Background(), &atomic.Bool{}, valueFrame{}) == nil || c.written != channelLimit-1 {
		t.Fatal("session total overflow emitted")
	}
	c.written = 0
	if err := f.response.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if n, err := f.response.Read(b); n != 0 || err == nil {
		t.Fatal("overflow published bytes")
	}
	start := time.Now()
	if err := c.sendBound(context.Background(), &atomic.Bool{}, strings.Repeat("y", 128*1024)); err == nil {
		t.Fatal("backpressure absent")
	}
	if time.Since(start) > 4*time.Second {
		t.Fatal("write deadline not pollable")
	}
}
func TestS6ProbePagedPinCut(t *testing.T) {
	var order []int
	reads := fakeReads(&order)
	readPins := reads.pins
	pins := readPins()
	if err := json.Unmarshal([]byte(`{"Status":"PAST","DurableWrites":{"Status":"PAST","Events":[{"Kind":"begin"}]}}`), &pins.Snapshot); err != nil {
		t.Fatal(err)
	}
	pins.Snapshot.BaselineIdentities = make([]domain.PeerIdentity, 20_000)
	for i := range pins.Snapshot.BaselineIdentities {
		sum := sha256.Sum256([]byte(fmt.Sprint(i)))
		copy(pins.Snapshot.BaselineIdentities[i][:], sum[:])
	}
	pins.Snapshot.DurableWrites.Events[0].Identities = pins.Snapshot.BaselineIdentities[:1000]
	want := bytes.TrimSuffix(marshal(t, pins), []byte{'\n'})
	if len(want) <= responseLimit {
		t.Fatal("fixture does not require paging")
	}
	order = nil
	reads.pins = func() observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey] {
		_ = readPins()
		return pins
	}
	f := fixture(t)
	done, reader := session(t, f, reads, "ON", &atomic.Bool{})
	send(t, f, marshal(t, readFrame{1, testNonce, 0, "READ"}))
	var payload bytes.Buffer
	var cut valueFrame
	count := 0
	for {
		raw, err := reader.ReadBytes('\n')
		if err != nil || len(raw) > responseLimit {
			t.Fatal("pin page exceeded frame bound or was truncated", len(raw), err)
		}
		var header struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &header); err != nil {
			t.Fatal(err)
		}
		if header.Kind == "CUT" {
			if err := canonical(raw, &cut); err != nil {
				t.Fatal(err)
			}
			break
		}
		var page pinPageFrame
		if err := canonical(raw, &page); err != nil || page.Kind != "PIN_PAGE" ||
			page.Version != 1 || page.Nonce != testNonce || page.Sequence != 0 ||
			page.Index != count || len(page.Data) == 0 || len(page.Data) > pinPagePayloadLimit {
			t.Fatal("invalid pin page binding", err, page.Index)
		}
		count++
		payload.Write(page.Data)
	}
	receipt := cut.PinPages
	if count < 2 || receipt == nil || receipt.Count != count || receipt.CompressedBytes != payload.Len() ||
		receipt.UncompressedBytes != len(want) || receipt.SHA256 != fmt.Sprintf("%x", sha256.Sum256(want)) ||
		cut.Sources == nil || cut.Sources.Pins.Identity != pins.Identity || cut.Sources.Pins.Key != pins.Key ||
		cut.Sources.Pins.Snapshot.Status != "PAGED" || !cut.Provisional || cut.InvokedSources != 8 {
		t.Fatal("missing bound receipt or pin locator", receipt, count)
	}
	gzipReader, err := gzip.NewReader(&payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(io.LimitReader(gzipReader, int64(len(want)+1)))
	if closeErr := gzipReader.Close(); err != nil || closeErr != nil || !bytes.Equal(got, want) {
		t.Fatal("pin prefix did not round-trip exactly", err, closeErr)
	}
	send(t, f, marshal(t, readFrame{1, testNonce, 1, "STOP"}))
	_ = f.request.Close()
	if stop := read(t, f, reader); stop.Kind != "STOP_REQUESTED" {
		t.Fatal(stop)
	}
	if err := wait(t, done); err != nil || !reflect.DeepEqual(order, []int{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Fatal("paged read was not a single eight-source capture", err, order)
	}

	f = fixture(t)
	c := newChannel(f.pipes, testNonce)
	c.written = channelLimit - responseLimit
	cut.Sources = &sourceValues{Pins: pins}
	if err := c.sendCut(context.Background(), &atomic.Bool{}, cut); err == nil || c.written != channelLimit-responseLimit {
		t.Fatal("aggregate overflow wrote part of a paged CUT", err, c.written)
	}
}

func TestS6ProbeRepeatedPinCutsShareExactPrefixes(t *testing.T) {
	var order []int
	reads := fakeReads(&order)
	pins := reads.pins()
	pins.Snapshot.BaselineIdentities = make([]domain.PeerIdentity, 20_000)
	for i := range pins.Snapshot.BaselineIdentities {
		sum := sha256.Sum256([]byte(fmt.Sprint(i)))
		copy(pins.Snapshot.BaselineIdentities[i][:], sum[:])
	}
	pinSnapshotAt := func(seq int) observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey] {
		value := pins
		raw := fmt.Sprintf(`{"Status":"PAST","Cut":{"Before":%d,"After":%d},"DurableWrites":{"Status":"PAST","Events":[{"Kind":"begin"}]}}`, seq+1, seq+1)
		if err := json.Unmarshal([]byte(raw), &value.Snapshot); err != nil {
			panic(err)
		}
		value.Snapshot.DurableWrites.Events[0].Identities = pins.Snapshot.BaselineIdentities[:1000]
		if seq > 0 {
			event := value.Snapshot.DurableWrites.Events[0]
			event.Kind = "confirmed"
			event.Identities = nil
			value.Snapshot.DurableWrites.Events = append(value.Snapshot.DurableWrites.Events, event)
		}
		return value
	}
	next := 0
	reads.pins = func() observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey] {
		value := pinSnapshotAt(next)
		next++
		return value
	}
	f := fixture(t)
	done, reader := session(t, f, reads, "ON", &atomic.Bool{})
	var base []byte
	wireBytes := 0
	for seq := range maxReads {
		send(t, f, marshal(t, readFrame{1, testNonce, seq, "READ"}))
		var compressed bytes.Buffer
		var cut valueFrame
		count := 0
		for {
			raw, err := reader.ReadBytes('\n')
			if err != nil || len(raw) > responseLimit {
				t.Fatal("truncated or oversized frame", seq, err)
			}
			wireBytes += len(raw)
			var header struct {
				Kind string `json:"kind"`
			}
			if err := json.Unmarshal(raw, &header); err != nil {
				t.Fatal(err)
			}
			if header.Kind == "CUT" {
				if err := canonical(raw, &cut); err != nil {
					t.Fatal(err)
				}
				break
			}
			var page pinPageFrame
			if err := canonical(raw, &page); err != nil || page.Kind != "PIN_PAGE" ||
				page.Sequence != seq || page.Index != count {
				t.Fatal("invalid pin page", seq, count, err)
			}
			count++
			compressed.Write(page.Data)
		}
		receipt := cut.PinPages
		if receipt == nil || receipt.Count != count || receipt.CompressedBytes != compressed.Len() {
			t.Fatal("invalid pin receipt", seq, receipt)
		}
		zipped, err := gzip.NewReader(&compressed)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(zipped, int64(max(receipt.UncompressedBytes, receipt.DeltaBytes))+1))
		if closeErr := zipped.Close(); err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		if seq == 0 {
			if receipt.BaseSHA256 != "" || receipt.DeltaBytes != 0 {
				t.Fatal("first read depends on a base")
			}
		} else {
			if receipt.BaseSHA256 != pinDigest(base) || len(body) != receipt.DeltaBytes || count != 1 {
				t.Fatal("read did not use its prior snapshot", seq, receipt)
			}
			var delta pinDelta
			if err := json.Unmarshal(body, &delta); err != nil {
				t.Fatal(err)
			}
			var rebuilt []byte
			inserted := delta.Insert
			for _, span := range delta.Copy {
				start, from, size := span[0], span[1], span[2]
				gap := start - len(rebuilt)
				rebuilt = append(rebuilt, inserted[:gap]...)
				inserted = inserted[gap:]
				rebuilt = append(rebuilt, base[from:from+size]...)
			}
			body = append(rebuilt, inserted...)
		}
		want := bytes.TrimSuffix(marshal(t, pinSnapshotAt(seq)), []byte{'\n'})
		if len(body) != receipt.UncompressedBytes || pinDigest(body) != receipt.SHA256 || !bytes.Equal(body, want) {
			t.Fatal("reconstructed pin snapshot changed", seq)
		}
		base = body
	}
	if wireBytes >= channelLimit {
		t.Fatal("repeated reads exceeded aggregate limit", wireBytes)
	}
	t.Logf("four paged CUTs: %d bytes", wireBytes)
	send(t, f, marshal(t, readFrame{1, testNonce, maxReads, "STOP"}))
	_ = f.request.Close()
	if stop := read(t, f, reader); stop.Kind != "STOP_REQUESTED" {
		t.Fatal(stop)
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestS6ProbeDefaultAndMixedTagExclusion(t *testing.T) {
	for _, name := range []string{"main.go", "channel.go", "pin_pages.go", "main_test.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		first := strings.SplitN(string(raw), "\n", 2)[0]
		expr, err := constraint.Parse(first)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			s6, d1, darwin, linux bool
			want                  bool
		}{{false, false, true, false, false}, {true, false, true, false, true}, {true, false, false, true, true}, {true, true, true, false, false}, {true, false, false, false, false}} {
			got := expr.Eval(func(tag string) bool {
				switch tag {
				case "s6_observation_probe":
					return tc.s6
				case "d1_resource_probe":
					return tc.d1
				case "darwin":
					return tc.darwin
				case "linux":
					return tc.linux
				}
				return false
			})
			if got != tc.want {
				t.Fatal(name, tc)
			}
		}
		if name == "main_test.go" {
			continue
		}
		tree, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(tree, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && (strings.Contains(sel.Sel.Name, "D1") || sel.Sel.Name == "SetMemoryLimit") {
				t.Error("unwanted activation", sel.Sel.Name)
			}
			return true
		})
	}
}

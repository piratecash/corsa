//go:build s6_observation_probe && !d1_resource_probe && (darwin || linux)

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	appnode "github.com/piratecash/corsa/internal/app/node"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"golang.org/x/sys/unix"
)

const initLimit = 4096
const requestLimit = 256
const responseLimit = 256 * 1024
const channelLimit = 1024 * 1024
const maxReads = 4
const readIdle = 30 * time.Second
const writeBound = 2 * time.Second

type pipeIdentity struct {
	fd             int
	dev, ino       uint64
	mode, uid, gid uint32
	read           bool
}

// These are exact native anonymous-pipe stat conventions, NOT a regular
// secret-file permission policy or proof that an inherited FIFO is anonymous.
// Parent creation/attachment provenance remains independently required.
func expectedPipePermissions(goos string) (uint32, error) {
	switch goos {
	case "darwin":
		return 0660, nil
	case "linux":
		return 0600, nil
	default:
		return 0, entryError("pipe-platform-unsupported")
	}
}

func inspectPipe(fd int, read bool) (pipeIdentity, error) {
	permissions, err := expectedPipePermissions(runtime.GOOS)
	if err != nil {
		return pipeIdentity{}, err
	}
	var s unix.Stat_t
	if fd <= 2 || unix.Fstat(fd, &s) != nil {
		return pipeIdentity{}, entryError("pipe-stat")
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	want := unix.O_WRONLY
	if read {
		want = unix.O_RDONLY
	}
	if err != nil || flags&unix.O_ACCMODE != want || s.Mode&unix.S_IFMT != unix.S_IFIFO || s.Uid != uint32(os.Geteuid()) || s.Gid != uint32(os.Getegid()) || uint32(s.Mode)&0777 != permissions {
		return pipeIdentity{}, entryError("pipe-custody-direction")
	}
	return pipeIdentity{fd: fd, dev: uint64(s.Dev), ino: uint64(s.Ino), mode: uint32(s.Mode), uid: s.Uid, gid: s.Gid, read: read}, nil
}
func (p pipeIdentity) check() error {
	current, err := inspectPipe(p.fd, p.read)
	if err != nil || current != p {
		return entryError("pipe-identity-changed")
	}
	flags, err := unix.FcntlInt(uintptr(p.fd), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_NONBLOCK == 0 {
		return entryError("pipe-no-nonblock")
	}
	fdFlags, err := unix.FcntlInt(uintptr(p.fd), unix.F_GETFD, 0)
	if err != nil || fdFlags&unix.FD_CLOEXEC == 0 {
		return entryError("pipe-no-CLOEXEC")
	}
	return nil
}

type controlPipes struct {
	in, out       *os.File
	input, output pipeIdentity
	once          sync.Once
}

func (p *controlPipes) close() { p.once.Do(func() { _ = p.in.Close(); _ = p.out.Close() }) }
func (p *controlPipes) check() error {
	if err := p.input.check(); err != nil {
		return err
	}
	return p.output.check()
}
func adoptControlPipes(inFD, outFD int) (*controlPipes, error) {
	input, err := inspectPipe(inFD, true)
	if err != nil {
		return nil, err
	}
	output, err := inspectPipe(outFD, false)
	if err != nil {
		return nil, err
	}
	// Reject duplicate endpoint stamps; Darwin does not expose peer pairing
	// through these inode facts. Two distinct channel objects/fixed attachment
	// require the future source-bound parent controller creation provenance.
	if input.fd == output.fd || input.dev == output.dev && input.ino == output.ino {
		return nil, entryError("pipe-alias")
	}
	// Ownership starts after both fixed roles validate. All later execs must
	// lose these private endpoints; NewFile does not set CLOEXEC for us.
	for _, fd := range []int{inFD, outFD} {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil {
			_ = unix.Close(inFD)
			_ = unix.Close(outFD)
			return nil, entryError("pipe-CLOEXEC-read")
		}
		if _, err = unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
			_ = unix.Close(inFD)
			_ = unix.Close(outFD)
			return nil, entryError("pipe-CLOEXEC-set")
		}
	}
	// Nonblock MUST precede
	// NewFile so Go1.27.3 registers inherited pipes with its runtime poller.
	if unix.SetNonblock(inFD, true) != nil || unix.SetNonblock(outFD, true) != nil {
		_ = unix.Close(inFD)
		_ = unix.Close(outFD)
		return nil, entryError("pipe-nonblock")
	}
	in, out := os.NewFile(uintptr(inFD), "s6-value-request"), os.NewFile(uintptr(outFD), "s6-value-response")
	p := &controlPipes{in: in, out: out, input: input, output: output}
	if in == nil || out == nil {
		p.close()
		return nil, entryError("pipe-wrapper")
	}
	if p.check() != nil || in.SetReadDeadline(time.Now().Add(readIdle)) != nil || out.SetWriteDeadline(time.Now().Add(writeBound)) != nil {
		p.close()
		return nil, entryError("pipe-poller-deadline")
	}
	return p, nil
}

type initFrame struct {
	Version int                               `json:"v"`
	Nonce   string                            `json:"nonce"`
	Mode    string                            `json:"mode"`
	Request appnode.S6ObservationProbeRequest `json:"request"`
}
type readFrame struct {
	Version  int    `json:"v"`
	Nonce    string `json:"nonce"`
	Sequence int    `json:"seq"`
	Kind     string `json:"kind"`
}
type observed[T any, K any] struct {
	Identity string `json:"identity"`
	Key      K      `json:"key"`
	Snapshot T      `json:"snapshot"`
}
type sourceValues struct {
	Registered          observed[corenode.RegisteredProvenV2ObservationSnapshot, corenode.RegisteredProvenV2ObservationKey]     `json:"registered"`
	Advertised          observed[corenode.AdvertisedConnectionObservationSnapshot, corenode.AdvertisedConnectionObservationKey] `json:"advertised"`
	Pins                observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey]         `json:"pins"`
	Guard               observed[corenode.GuardObservationSnapshot, corenode.GuardObservationKey]                               `json:"guard"`
	Terminal            observed[corenode.TerminalObservationSnapshot, corenode.TerminalObservationKey]                         `json:"terminal"`
	Pending             observed[domain.PendingQueueObservationSnapshot, corenode.PendingObservationKey]                        `json:"pending"`
	DatagramQueueGrowth *observed[datagram.QueueGrowthSnapshot, corenode.PendingObservationKey]                                 `json:"datagram_queue_growth,omitempty"`
	Handshake           observed[corenode.HandshakeObservationSnapshot, corenode.HandshakeObservationKey]                       `json:"handshake"`
	Total               observed[corenode.ConnectionTotalObservationSnapshot, corenode.ConnectionTotalObservationKey]           `json:"total"`
}

// The fixed eight functions are private constructor-bound readers, not a
// callback source API or collector. OFF leaves all nil and never invokes them.
type captureReads struct {
	registered          func() observed[corenode.RegisteredProvenV2ObservationSnapshot, corenode.RegisteredProvenV2ObservationKey]
	advertised          func() observed[corenode.AdvertisedConnectionObservationSnapshot, corenode.AdvertisedConnectionObservationKey]
	pins                func() observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey]
	guard               func() observed[corenode.GuardObservationSnapshot, corenode.GuardObservationKey]
	terminal            func() observed[corenode.TerminalObservationSnapshot, corenode.TerminalObservationKey]
	pending             func() observed[domain.PendingQueueObservationSnapshot, corenode.PendingObservationKey]
	datagramQueueGrowth func() observed[datagram.QueueGrowthSnapshot, corenode.PendingObservationKey]
	handshake           func() observed[corenode.HandshakeObservationSnapshot, corenode.HandshakeObservationKey]
	total               func() observed[corenode.ConnectionTotalObservationSnapshot, corenode.ConnectionTotalObservationKey]
}

func readsForApp(a *appnode.App, mode string) (captureReads, error) {
	if a == nil {
		return captureReads{}, entryError("capture-App-unavailable")
	}
	r, ad, p, g, t, q, h, b := a.RegisteredProvenV2Observation(), a.AdvertisedConnectionObservation(), a.RawPinMembershipObservation(), a.GuardObservation(), a.TerminalObservation(), a.PendingObservation(), a.HandshakeObservation(), a.ConnectionTotalObservation()
	if mode == "OFF" {
		if r != nil || ad != nil || p != nil || g != nil || t != nil || q != nil || h != nil || b != nil {
			return captureReads{}, entryError("OFF-source-installed")
		}
		return captureReads{}, nil
	}
	if mode != "ON" || r == nil || ad == nil || p == nil || g == nil || t == nil || q == nil || h == nil || b == nil {
		return captureReads{}, entryError("ON-source-unavailable")
	}
	reads := captureReads{
		registered: func() observed[corenode.RegisteredProvenV2ObservationSnapshot, corenode.RegisteredProvenV2ObservationKey] {
			return observed[corenode.RegisteredProvenV2ObservationSnapshot, corenode.RegisteredProvenV2ObservationKey]{r.Identity().String(), r.Key(), r.Snapshot()}
		},
		advertised: func() observed[corenode.AdvertisedConnectionObservationSnapshot, corenode.AdvertisedConnectionObservationKey] {
			return observed[corenode.AdvertisedConnectionObservationSnapshot, corenode.AdvertisedConnectionObservationKey]{ad.Identity().String(), ad.Key(), ad.Snapshot()}
		},
		pins: func() observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey] {
			return observed[corenode.RawPinMembershipObservationSnapshot, corenode.RawPinMembershipObservationKey]{p.Identity().String(), p.Key(), p.Snapshot()}
		},
		guard: func() observed[corenode.GuardObservationSnapshot, corenode.GuardObservationKey] {
			return observed[corenode.GuardObservationSnapshot, corenode.GuardObservationKey]{g.Identity().String(), g.Key(), g.Snapshot()}
		},
		terminal: func() observed[corenode.TerminalObservationSnapshot, corenode.TerminalObservationKey] {
			return observed[corenode.TerminalObservationSnapshot, corenode.TerminalObservationKey]{t.Identity().String(), t.Key(), t.Snapshot()}
		},
		pending: func() observed[domain.PendingQueueObservationSnapshot, corenode.PendingObservationKey] {
			return observed[domain.PendingQueueObservationSnapshot, corenode.PendingObservationKey]{q.Identity().String(), q.Key(), q.Snapshot()}
		},
		handshake: func() observed[corenode.HandshakeObservationSnapshot, corenode.HandshakeObservationKey] {
			return observed[corenode.HandshakeObservationSnapshot, corenode.HandshakeObservationKey]{h.Identity().String(), h.Key(), h.Snapshot()}
		},
		total: func() observed[corenode.ConnectionTotalObservationSnapshot, corenode.ConnectionTotalObservationKey] {
			return observed[corenode.ConnectionTotalObservationSnapshot, corenode.ConnectionTotalObservationKey]{b.Identity().String(), b.Key(), b.Snapshot()}
		},
	}
	if q.HasDatagramQueueGrowth() {
		reads.datagramQueueGrowth = func() observed[datagram.QueueGrowthSnapshot, corenode.PendingObservationKey] {
			return observed[datagram.QueueGrowthSnapshot, corenode.PendingObservationKey]{q.Identity().String(), q.Key(), q.DatagramQueueGrowth()}
		}
	}
	return reads, nil
}
func alive(ctx context.Context, returned *atomic.Bool) error {
	if ctx == nil || returned == nil || returned.Load() || ctx.Err() != nil {
		return entryError("App-run-return-or-cancel")
	}
	return nil
}
func take[T any](ctx context.Context, returned *atomic.Bool, read func() T) (T, error) {
	var zero T
	if read == nil {
		return zero, entryError("capture-reader-unavailable")
	}
	if err := alive(ctx, returned); err != nil {
		return zero, err
	}
	value := read()
	if err := alive(ctx, returned); err != nil {
		return zero, err
	}
	return value, nil
}
func (r captureReads) capture(ctx context.Context, returned *atomic.Bool, mode string) (*sourceValues, int, error) {
	if err := alive(ctx, returned); err != nil {
		return nil, 0, err
	}
	if mode == "OFF" {
		if r.registered != nil || r.advertised != nil || r.pins != nil || r.guard != nil || r.terminal != nil || r.pending != nil || r.datagramQueueGrowth != nil || r.handshake != nil || r.total != nil {
			return nil, 0, entryError("OFF-reader-installed")
		}
		return nil, 0, nil
	}
	if mode != "ON" {
		return nil, 0, entryError("capture-mode")
	}
	var v sourceValues
	var err error
	if v.Registered, err = take(ctx, returned, r.registered); err != nil {
		return nil, 0, err
	}
	if v.Advertised, err = take(ctx, returned, r.advertised); err != nil {
		return nil, 0, err
	}
	if v.Pins, err = take(ctx, returned, r.pins); err != nil {
		return nil, 0, err
	}
	if v.Guard, err = take(ctx, returned, r.guard); err != nil {
		return nil, 0, err
	}
	if v.Terminal, err = take(ctx, returned, r.terminal); err != nil {
		return nil, 0, err
	}
	if v.Pending, err = take(ctx, returned, r.pending); err != nil {
		return nil, 0, err
	}
	if v.Handshake, err = take(ctx, returned, r.handshake); err != nil {
		return nil, 0, err
	}
	if v.Total, err = take(ctx, returned, r.total); err != nil {
		return nil, 0, err
	}
	count := 8
	if r.datagramQueueGrowth != nil {
		growth, err := take(ctx, returned, r.datagramQueueGrowth)
		if err != nil {
			return nil, 0, err
		}
		v.DatagramQueueGrowth = &growth
		count++
	}
	return &v, count, nil
}
func canonical(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return entryError("channel-JSON")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return entryError("channel-extra-JSON")
	}
	encoded, err := json.Marshal(value)
	if err != nil || !bytes.Equal(append(encoded, '\n'), raw) {
		return entryError("channel-noncanonical")
	}
	return nil
}
func validateInit(v initFrame, nonce string) error {
	if v.Version != 1 || v.Nonce != nonce || (v.Mode != "ON" && v.Mode != "OFF") {
		return entryError("INIT-binding")
	}
	q := v.Request
	labels := []string{
		q.Guard.Anchor, q.Guard.ArtifactSHA256, q.Guard.Boot, q.Guard.ProcessInstance, q.Guard.ClockDomain,
		q.Terminal.Anchor, q.Terminal.ArtifactSHA256, q.Terminal.Boot, q.Terminal.ProcessInstance,
		q.Pending.Anchor, q.Pending.ArtifactSHA256, q.Pending.Boot, q.Pending.ProcessInstance, q.Pending.ClockDomain,
		q.Handshake.Key.Anchor, q.Handshake.Key.ArtifactSHA256, q.Handshake.Key.Boot, q.Handshake.Key.ProcessInstance,
		q.ConnectionTotal.Key.Anchor, q.ConnectionTotal.Key.ArtifactSHA256, q.ConnectionTotal.Key.Boot, q.ConnectionTotal.Key.ProcessInstance,
	}
	for _, label := range labels {
		if len(label) > 128 {
			return entryError("INIT-label-length")
		}
	}
	caps := []struct{ value, maximum int }{
		{q.RegisteredProvenV2.OwnerCapacity, 1024}, {q.RegisteredProvenV2.HistoryCapacity, 4096},
		{q.AdvertisedConnections.ConnectionCapacity, 1024}, {q.AdvertisedConnections.SlotCapacity, 1024}, {q.AdvertisedConnections.HistoryCapacity, 4096},
		{q.RawPinMembership.BaselineCapacity, 20000}, {q.RawPinMembership.HistoryCapacity, 4096}, {q.Handshake.Capacity, 4096}, {q.ConnectionTotal.Capacity, 4096},
	}
	for _, cap := range caps {
		if cap.value < 0 || cap.value > cap.maximum {
			return entryError("INIT-capacity-ceiling")
		}
	}
	// These are allocation/refusal ceilings, not deployment selections. Zero
	// reaches the original source; it is never replaced by a successful default.
	return nil
}

type channel struct {
	pipes   *controlPipes
	reader  *bufio.Reader
	nonce   string
	written int
	pinBase []byte
}

func newChannel(p *controlPipes, nonce string) *channel {
	return &channel{pipes: p, reader: bufio.NewReaderSize(p.in, initLimit+1), nonce: nonce}
}
func (c *channel) line(limit int, deadline time.Time) ([]byte, error) {
	if err := c.pipes.check(); err != nil {
		return nil, err
	}
	if err := c.pipes.in.SetReadDeadline(deadline); err != nil {
		return nil, entryError("channel-read-deadline")
	}
	raw, err := c.reader.ReadSlice('\n')
	if errors.Is(err, io.EOF) && len(raw) == 0 {
		return nil, io.EOF
	}
	if err != nil || len(raw) == 0 || len(raw) > limit {
		return nil, entryError("channel-framing-or-limit")
	}
	if err = c.pipes.check(); err != nil {
		return nil, err
	}
	return raw, nil
}
func (c *channel) readInit(deadline time.Time) (initFrame, error) {
	var v initFrame
	raw, err := c.line(initLimit, deadline)
	if err != nil {
		return v, err
	}
	if err = canonical(raw, &v); err != nil {
		return v, err
	}
	return v, validateInit(v, c.nonce)
}
func (c *channel) sendBound(ctx context.Context, returned *atomic.Bool, frame any) error {
	if err := alive(ctx, returned); err != nil {
		return err
	}
	// Standard JSON may allocate a complete temporary DTO buffer. Source request
	// capacities and INIT size are finite, but output bounds are NOT a heap cap.
	// The transient encoding allocation belongs to the future shared cgroup peak.
	raw, err := json.Marshal(frame)
	if err != nil {
		return entryError("channel-encode")
	}
	return c.sendLine(ctx, returned, append(raw, '\n'))
}

func (c *channel) sendLine(ctx context.Context, returned *atomic.Bool, raw []byte) error {
	if err := alive(ctx, returned); err != nil {
		return err
	}
	if len(raw) > responseLimit || len(raw) > channelLimit-c.written {
		return entryError("channel-encoded-limit")
	}
	if err := c.pipes.check(); err != nil {
		return err
	}
	deadline := time.Now().Add(writeBound)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err := c.pipes.out.SetWriteDeadline(deadline); err != nil {
		return entryError("channel-write-deadline")
	}
	if err := alive(ctx, returned); err != nil {
		return err
	}
	n, err := c.pipes.out.Write(raw)
	c.written += n
	if err != nil || n != len(raw) {
		return entryError("channel-partial-write")
	}
	if err := c.pipes.check(); err != nil {
		return err
	}
	// Bytes cannot be retracted. CUT remains provisional until the complete
	// control session and owned process finish successfully. This is an
	// observed software fence, never continuous physical Run liveness.
	return alive(ctx, returned)
}

type valueFrame struct {
	Version             int             `json:"v"`
	Nonce               string          `json:"nonce"`
	Sequence            int             `json:"seq"`
	Kind                string          `json:"kind"`
	Mode                string          `json:"mode"`
	IndependentReads    bool            `json:"independent_reads"`
	JointAtomicCut      bool            `json:"joint_atomic_cut"`
	AttributionVerified bool            `json:"attribution_verified"`
	Provisional         bool            `json:"provisional"`
	InvokedSources      int             `json:"invoked_sources"`
	Sources             *sourceValues   `json:"sources"`
	PinPages            *pinPageReceipt `json:"pin_pages,omitempty"`
}

func (c *channel) serve(ctx context.Context, a *appnode.App, mode string, returned *atomic.Bool) error {
	reads, err := readsForApp(a, mode)
	if err != nil {
		return err
	}
	return c.serveReads(ctx, reads, mode, returned)
}
func (c *channel) serveReads(ctx context.Context, reads captureReads, mode string, returned *atomic.Bool) error {
	if err := alive(ctx, returned); err != nil {
		return err
	}
	if err := c.sendBound(ctx, returned, valueFrame{Version: 1, Nonce: c.nonce, Sequence: -1, Kind: "CONSTRUCTED", Mode: mode, IndependentReads: true}); err != nil {
		return err
	}
	for seq := 0; seq <= maxReads; seq++ {
		if err := alive(ctx, returned); err != nil {
			return err
		}
		deadline := time.Now().Add(readIdle)
		if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
			deadline = end
		}
		raw, err := c.line(requestLimit, deadline)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var request readFrame
		if err = canonical(raw, &request); err != nil {
			return err
		}
		if request.Version != 1 || request.Nonce != c.nonce || request.Sequence != seq {
			return entryError("channel-request-binding")
		}
		if request.Kind == "STOP" {
			// The parent must half-close after STOP; reject queued extra or
			// repeated commands rather than silently accepting a prefix.
			stopEnd := time.Now().Add(2 * time.Second)
			if end, ok := ctx.Deadline(); ok && end.Before(stopEnd) {
				stopEnd = end
			}
			if _, err = c.line(requestLimit, stopEnd); !errors.Is(err, io.EOF) {
				return entryError("channel-extra-or-unclosed-STOP")
			}
			return c.sendBound(ctx, returned, valueFrame{Version: 1, Nonce: c.nonce, Sequence: seq, Kind: "STOP_REQUESTED", Mode: mode, IndependentReads: true})
		}
		if request.Kind != "READ" || seq == maxReads {
			return entryError("channel-operation-or-count")
		}
		sources, count, err := reads.capture(ctx, returned, mode)
		if err != nil {
			return err
		}
		if err = alive(ctx, returned); err != nil {
			return err
		}
		frame := valueFrame{Version: 1, Nonce: c.nonce, Sequence: seq, Kind: "CUT", Mode: mode, IndependentReads: true, Provisional: true, InvokedSources: count, Sources: sources}
		if err = c.sendCut(ctx, returned, frame); err != nil {
			return err
		}
	}
	return entryError("channel-unclosed-session")
}

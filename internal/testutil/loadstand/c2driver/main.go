// c2driver is a fixed private experiment, not a node or SDK implementation.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

const (
	networkName = "gazeta-devnet" // Stock Service constant, not a test factory selection.
	endpoint    = "127.0.0.1:64646"
	rpcURL      = "http://127.0.0.1:46464/rpc/v1/exec"
	cliSHA      = "9ba93241a3f717fb50a6221957e4e3146f982ef5fc7b68a37f90bfe7a49e55bf"
	maxLine     = 64 * 1024
	maxFrames   = 2048
	maxBytes    = 8 * 1024 * 1024
	operation   = 2 * time.Second
)

var errRefused = errors.New("c2a: fixed contract unavailable or refused")
var lower40 = regexp.MustCompile(`^[0-9a-f]{40}$`)
var lower64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
var lower32 = regexp.MustCompile(`^[0-9a-f]{32}$`)

type activation struct {
	Ready       bool   `json:"ready"`
	Profile     string `json:"profile"`
	Nonce       string `json:"nonce"`
	ApprovalSHA string `json:"approval_sha256"`
	CLISHA      string `json:"cli_sha256"`
	Server      string `json:"server_identity"`
	StartedAt   string `json:"server_started_at"`
	WorkMillis  int64  `json:"work_millis"`
}
type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// checkJSON rejects duplicate keys and deep/ambiguous JSON before typed decoding.
func checkJSON(raw []byte, maximum int) error {
	if len(raw) == 0 || len(raw) > maximum {
		return errRefused
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 24 {
			return errRefused
		}
		token, err := d.Token()
		if err != nil {
			return errRefused
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				s, ok := k.(string)
				if e != nil || !ok || keys[s] {
					return errRefused
				}
				keys[s] = true
				if e = value(depth + 1); e != nil {
					return e
				}
			}
			end, e := d.Token()
			if e != nil || end != json.Delim('}') {
				return errRefused
			}
		case '[':
			for d.More() {
				if e := value(depth + 1); e != nil {
					return e
				}
			}
			end, e := d.Token()
			if e != nil || end != json.Delim(']') {
				return errRefused
			}
		default:
			return errRefused
		}
		return nil
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errRefused
	}
	return nil
}
func decode(raw []byte, maximum int, dst any, closed bool) error {
	if err := checkJSON(raw, maximum); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if closed {
		d.DisallowUnknownFields()
	}
	if d.Decode(dst) != nil {
		return errRefused
	}
	return nil
}
func parseActivation(raw []byte) (activation, error) {
	var a activation
	if decode(raw, 4096, &a, true) != nil || !a.Ready || a.Profile != "C2A8-v1" || !lower32.MatchString(a.Nonce) || !lower64.MatchString(a.ApprovalSHA) || a.CLISHA != cliSHA || !lower40.MatchString(a.Server) || a.Server == "0000000000000000000000000000000000000000" || a.WorkMillis <= 0 || a.WorkMillis > 80000 {
		return a, errRefused
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 8 {
		return a, errRefused
	}
	for _, key := range []string{"ready", "profile", "nonce", "approval_sha256", "cli_sha256", "server_identity", "server_started_at", "work_millis"} {
		if _, ok := fields[key]; !ok {
			return a, errRefused
		}
	}
	t, e := time.Parse(time.RFC3339Nano, a.StartedAt)
	if e != nil || t.IsZero() || t.Location() != time.UTC {
		return a, errRefused
	}
	return a, nil
}
func parseCredentials(raw []byte) (credentials, error) {
	var c credentials
	if decode(raw, 1024, &c, true) != nil {
		return c, errRefused
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 {
		return credentials{}, errRefused
	}
	if _, ok := fields["username"]; !ok {
		return credentials{}, errRefused
	}
	if _, ok := fields["password"]; !ok {
		return credentials{}, errRefused
	}
	for _, v := range []string{c.Username, c.Password} {
		if len(v) < 1 || len(v) > 128 {
			return credentials{}, errRefused
		}
		for _, r := range v {
			if r < 33 || r > 126 {
				return credentials{}, errRefused
			}
		}
	}
	for _, r := range c.Username {
		if r == ':' {
			return credentials{}, errRefused
		}
	}
	return c, nil
}

type allowance struct {
	mu            sync.Mutex
	frames, bytes int
}

func (b *allowance) account(n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n <= 0 || n > maxLine || b.frames >= maxFrames || n > maxBytes-b.bytes {
		return errRefused
	}
	b.frames++
	b.bytes += n
	return nil
}
func readLine(r *bufio.Reader, b *allowance) ([]byte, error) {
	line, e := r.ReadSlice('\n')
	if len(line) > 0 && b.account(len(line)) != nil {
		return nil, errRefused
	}
	if e != nil {
		return nil, errRefused
	}
	return line, nil
}
func parseFrame(raw []byte) (protocol.Frame, error) {
	var f protocol.Frame
	if len(raw) == 0 || raw[len(raw)-1] != '\n' || decode(raw, maxLine, &f, false) != nil {
		return f, errRefused
	}
	var authority map[string]json.RawMessage
	if json.Unmarshal(raw, &authority) != nil {
		return f, errRefused
	}
	if _, ok := authority["type"]; !ok {
		return f, errRefused
	}
	for key := range authority {
		for _, canonical := range []string{"type", "network", "count", "peers"} {
			if strings.EqualFold(key, canonical) && key != canonical {
				return f, errRefused
			}
		}
	}
	if f.Type == "peers" {
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return f, errRefused
		}
		for _, key := range []string{"count", "peers"} {
			if bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
				return f, errRefused
			}
		}
		if f.Count < 0 || f.Count > 64 || f.Count != len(f.Peers) {
			return f, errRefused
		}
		for _, p := range f.Peers {
			h, port, e := net.SplitHostPort(p)
			n, e2 := strconv.ParseUint(port, 10, 16)
			if e != nil || e2 != nil || n == 0 || len(h) < 1 || len(h) > 253 || len(p) > 300 {
				return f, errRefused
			}
		}
	}
	return f, nil
}

type population struct {
	ctx        context.Context
	budget     allowance
	failures   chan error
	peers      []*peer
	identities map[domain.PeerIdentity]bool
}

func (g *population) fail() {
	select {
	case g.failures <- errRefused:
	default:
	}
}
func (g *population) check() error {
	select {
	case <-g.failures:
		return errRefused
	default:
	}
	if g.ctx.Err() != nil {
		return errRefused
	}
	return nil
}

type pending struct {
	kind  string
	reply chan protocol.Frame
}
type proofReceipt struct {
	Ordinal   int    `json:"ordinal"`
	Identity  string `json:"identity"`
	Server    string `json:"server"`
	Local     string `json:"local"`
	Remote    string `json:"remote"`
	Exchanges int    `json:"useful_exchanges"`
}
type peer struct {
	conn               net.Conn
	ctx                context.Context
	cancel             context.CancelFunc
	callbackDone, done chan struct{}
	writeMu, mu        sync.Mutex
	exchange           sync.Mutex
	waiting            *pending
	closing            bool
	g                  *population
	identity           domain.PeerIdentity
	public             proofReceipt
}

func (p *peer) write(f protocol.Frame) error {
	line, e := protocol.MarshalFrameLineBytes(f)
	if e != nil {
		return errRefused
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if p.g.budget.account(len(line)) != nil {
		return errRefused
	}
	ctx, cancel := context.WithTimeout(p.ctx, operation)
	defer cancel()
	d, _ := ctx.Deadline()
	if p.conn.SetWriteDeadline(d) != nil {
		return errRefused
	}
	n, e := p.conn.Write(line)
	if e != nil || n != len(line) {
		return errRefused
	}
	return nil
}
func (p *peer) read() {
	defer close(p.done)
	r := bufio.NewReaderSize(p.conn, maxLine)
	for {
		raw, e := readLine(r, &p.g.budget)
		if e != nil {
			p.mu.Lock()
			intent := p.closing
			p.mu.Unlock()
			if !intent {
				p.g.fail()
			}
			return
		}
		f, e := parseFrame(raw)
		if e != nil {
			p.g.fail()
			return
		}
		switch f.Type {
		case "ping":
			if p.write(protocol.Frame{Type: "pong", Network: networkName}) != nil {
				p.g.fail()
				return
			}
		case "get_peers":
			if p.write(protocol.Frame{Type: "peers"}) != nil {
				p.g.fail()
				return
			} // This wire-only nonlistener advertises no peers.
		case "pong", "peers":
			p.mu.Lock()
			w := p.waiting
			if w == nil || w.kind != f.Type || (f.Type == "pong" && f.Network != networkName) {
				p.mu.Unlock()
				p.g.fail()
				return
			}
			p.waiting = nil
			p.mu.Unlock()
			select {
			case w.reply <- f:
			default:
				p.g.fail()
				return
			}
		case "contacts", "announce_peer", "announce_routes", "routes_update", "request_resync", "push_identity":
			// Bounded production control frames are parsed/accounted, never installed.
		default:
			p.g.fail()
			return
		}
	}
}
func (p *peer) request(ctx context.Context, send, expect string) error {
	w := &pending{kind: expect, reply: make(chan protocol.Frame, 1)}
	p.mu.Lock()
	if p.closing || p.waiting != nil {
		p.mu.Unlock()
		return errRefused
	}
	p.waiting = w
	p.mu.Unlock()
	if p.write(protocol.Frame{Type: send}) != nil {
		return errRefused
	}
	select {
	case <-w.reply:
		return p.g.check()
	case <-p.g.failures:
		return errRefused
	case <-ctx.Done():
		return errRefused
	case <-p.done:
		return errRefused
	}
}
func (p *peer) useful() error {
	p.exchange.Lock()
	defer p.exchange.Unlock()
	ctx, cancel := context.WithTimeout(p.ctx, operation)
	defer cancel()
	// One outstanding exchange. Pre-pong separates setup traffic; post-pong closes
	// the server's original sequential read lane. No request ID is invented.
	if p.request(ctx, "ping", "pong") != nil {
		return errRefused
	}
	if p.request(ctx, "get_peers", "peers") != nil {
		return errRefused
	}
	if p.request(ctx, "ping", "pong") != nil {
		return errRefused
	}
	p.public.Exchanges++
	return nil
}
func (p *peer) close(ctx context.Context) error {
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.cancel()
	select {
	case <-p.callbackDone:
	case <-ctx.Done():
		return errRefused
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return errRefused
	}
}
func (g *population) dial(server domain.PeerIdentity) error {
	if g.check() != nil || len(g.peers) >= 12 {
		return errRefused
	}
	id, e := identity.Generate()
	if e != nil {
		return errRefused
	}
	own := domain.PeerIdentityFromWire(id.Address)
	if g.identities[own] {
		return errRefused
	}
	g.identities[own] = true
	ctx, cancel := context.WithTimeout(g.ctx, operation)
	defer cancel()
	d := net.Dialer{}
	d.SetMultipathTCP(false)
	raw, e := d.DialContext(ctx, "tcp4", endpoint)
	if e != nil {
		return errRefused
	}
	transferred := false
	defer func() {
		if !transferred {
			_ = raw.Close()
		}
	}()
	local, ok := raw.LocalAddr().(*net.TCPAddr)
	remote, ok2 := raw.RemoteAddr().(*net.TCPAddr)
	if !ok || !ok2 || !local.IP.Equal(net.IPv4(127, 0, 0, 1)) || local.Port == 0 || !remote.IP.Equal(net.IPv4(127, 0, 0, 1)) || remote.Port != 64646 {
		return errRefused
	}
	binding, e := sessionv2.NewLocal(id, domain.NetworkID(networkName))
	if e != nil {
		return errRefused
	}
	hello := protocol.Frame{Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion, Client: "node", ClientVersion: config.CorsaVersion, Listener: "0", NodeType: string(domain.NodeTypeFull)}
	proof, e := sessionv2.Dial(ctx, raw, binding, hello, sessionv2.ExpectPeer(server), sessionv2.DefaultTimeouts)
	if e != nil {
		return errRefused
	}
	proved, e := proof.Peer()
	if e != nil || proved.Identity != server || proved.Intro.Network != networkName {
		return errRefused
	}
	witness, ok := proved.Proof()
	got, ok2 := witness.Identity()
	if !ok || !ok2 || got != server {
		return errRefused
	}
	for _, capability := range proved.Intro.Capabilities {
		if capability == string(domain.CapOverlayFindNodeV1) {
			return errRefused
		}
	}
	conn, e := proof.Conn()
	if e != nil || conn.SetDeadline(time.Time{}) != nil {
		return errRefused
	}
	pc, stop := context.WithCancel(g.ctx)
	p := &peer{conn: conn, ctx: pc, cancel: stop, callbackDone: make(chan struct{}), done: make(chan struct{}), g: g, identity: own, public: proofReceipt{len(g.peers) + 1, own.String(), server.String(), raw.LocalAddr().String(), raw.RemoteAddr().String(), 0}}
	g.peers = append(g.peers, p) // Cleanup owns it BEFORE reader/callback/barrier starts.
	context.AfterFunc(pc, func() { _ = conn.Close(); close(p.callbackDone) })
	transferred = true
	go p.read()
	return p.useful()
}

type census struct {
	Ready     bool    `json:"ready"`
	UpdatedAt *string `json:"updated_at"`
	V2        *struct {
		Sessions   *uint64 `json:"sessions"`
		Identities *uint64 `json:"identities"`
	} `json:"inbound_v2"`
}
type routeReply struct {
	Loopback *struct {
		StartedAt *string `json:"started_at"`
		ReadAt    *string `json:"read_at"`
		Enforced  *bool   `json:"enforced"`
		Active    *int    `json:"active"`
		MaxActive *int    `json:"max_active"`
		MaxNew    *int    `json:"max_new_per_window"`
		Window    *int    `json:"window_seconds"`
	} `json:"loopback_inbound"`
	Kinds struct {
		Live census `json:"live"`
	} `json:"session_kinds"`
}
type control struct {
	a             activation
	secret        credentials
	http          *http.Client
	rpcCount      int
	rpcBytes      int
	loopbackLabel string
}

func (c *control) rpc(ctx context.Context, method string, out any) error {
	if c.rpcCount >= 128 {
		return errRefused
	}
	c.rpcCount++
	body, _ := json.Marshal(struct {
		Command string         `json:"command"`
		Args    map[string]any `json:"args"`
	}{method, map[string]any{}})
	op, stop := context.WithTimeout(ctx, operation)
	defer stop()
	req, e := http.NewRequestWithContext(op, http.MethodPost, rpcURL, bytes.NewReader(body))
	if e != nil {
		return errRefused
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.secret.Username, c.secret.Password)
	reply, e := c.http.Do(req)
	if e != nil {
		return errRefused
	}
	defer func() { _ = reply.Body.Close() }()
	if reply.StatusCode != 200 || reply.Header.Get("Content-Encoding") != "" && reply.Header.Get("Content-Encoding") != "identity" {
		return errRefused
	}
	raw, e := io.ReadAll(io.LimitReader(reply.Body, 1024*1024+1))
	if e != nil || len(raw) > 8*1024*1024-c.rpcBytes {
		return errRefused
	}
	c.rpcBytes += len(raw)
	return decode(raw, 1024*1024, out, false)
}
func (c *control) status(ctx context.Context) error {
	var s struct {
		Identity  string `json:"identity"`
		Address   string `json:"address"`
		StartedAt string `json:"started_at"`
	}
	if c.rpc(ctx, "getNodeStatus", &s) != nil || s.Identity != c.a.Server || s.Address != s.Identity || s.StartedAt != c.a.StartedAt {
		return errRefused
	}
	return nil
}
func (c *control) acceptRoute(last routeReply, want int) (bool, error) {
	b := last.Loopback
	if b == nil || b.StartedAt == nil || b.ReadAt == nil || b.Enforced == nil || !*b.Enforced || b.Active == nil || *b.Active < 0 || b.MaxActive == nil || *b.MaxActive != 64 || b.MaxNew == nil || *b.MaxNew != 100 || b.Window == nil || *b.Window != 10 {
		return false, errRefused
	}
	for _, v := range []string{*b.StartedAt, *b.ReadAt} {
		t, e := time.Parse(time.RFC3339Nano, v)
		if e != nil || t.IsZero() {
			return false, errRefused
		}
	}
	if c.loopbackLabel == "" {
		c.loopbackLabel = *b.StartedAt
	}
	if *b.StartedAt != c.loopbackLabel {
		return false, errRefused
	}
	return *b.Active == want, nil // Instantaneous class gauge; cached census is NOT read here.
}
func (c *control) active(ctx context.Context, want int) (routeReply, error) {
	var last routeReply
	op, stop := context.WithTimeout(ctx, operation)
	defer stop()
	for {
		if c.status(op) != nil || c.rpc(op, "fetchRouteSummary", &last) != nil {
			return last, errRefused
		}
		matched, e := c.acceptRoute(last, want)
		if e != nil {
			return last, e
		}
		if matched {
			return last, nil
		}
		select {
		case <-op.Done():
			return last, errRefused
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func emit(nonce, phase string, active int, diag census, g *population) error {
	// Cached live census is diagnostic only. It never authorizes a transition.
	var count *int
	if active >= 0 {
		count = &active
	}
	var peers []proofReceipt
	var charge *struct {
		Frames int `json:"frames"`
		Bytes  int `json:"bytes"`
	}
	if g != nil {
		g.budget.mu.Lock()
		charge = &struct {
			Frames int `json:"frames"`
			Bytes  int `json:"bytes"`
		}{g.budget.frames, g.budget.bytes}
		g.budget.mu.Unlock()
		for _, p := range g.peers {
			p.mu.Lock()
			closed := p.closing
			p.mu.Unlock()
			if !closed {
				peers = append(peers, p.public)
			}
		}
		if active >= 0 && len(peers) != active {
			return errRefused
		}
	}
	row := struct {
		Kind                 string         `json:"kind"`
		Nonce                string         `json:"nonce"`
		Phase                string         `json:"phase"`
		VerifiedChannels     *int           `json:"verified_channels"`
		Peers                []proofReceipt `json:"peers"`
		CachedLive           census         `json:"cached_live"`
		CachedLiveAcceptance bool           `json:"cached_live_acceptance"`
		ApplicationCharge    any            `json:"application_charge"`
	}{"c2a", nonce, phase, count, peers, diag, false, charge}
	raw, e := json.Marshal(row)
	if e != nil || len(raw) > 16383 {
		return errRefused
	}
	raw = append(raw, '\n')
	n, e := os.Stdout.Write(raw)
	if e != nil || n != len(raw) {
		return errRefused
	}
	return nil
}
func work(ctx context.Context, a activation, s credentials) (err error) {
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, DialContext: (&net.Dialer{Timeout: operation}).DialContext}
	defer transport.CloseIdleConnections()
	c := &control{a: a, secret: s, http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return errRefused }}}
	g := &population{ctx: ctx, failures: make(chan error, 1), identities: map[domain.PeerIdentity]bool{}}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, p := range g.peers {
			if p.close(cleanup) != nil {
				err = errRefused
			}
		}
		if g.check() != nil {
			err = errRefused
		}
	}()
	server, e := domain.ParsePeerIdentity(a.Server)
	if e != nil {
		return errRefused
	}
	if _, e = c.active(ctx, 0); e != nil {
		return e
	}
	for i := 0; i < 8; i++ {
		if g.dial(server) != nil {
			return errRefused
		}
	}
	snap, e := c.active(ctx, 8)
	if e != nil {
		return e
	}
	if emit(a.Nonce, "eight-proven", 8, snap.Kinds.Live, g) != nil {
		return errRefused
	}
	for tick := 0; tick < 4; tick++ {
		select {
		case <-ctx.Done():
			return errRefused
		case <-time.After(5 * time.Second):
		}
		for _, p := range g.peers {
			if p.useful() != nil {
				return errRefused
			}
		}
	}
	cleanup, stop := context.WithTimeout(ctx, operation)
	for _, p := range g.peers[:4] {
		if p.close(cleanup) != nil {
			stop()
			return errRefused
		}
	}
	stop()
	snap, e = c.active(ctx, 4)
	if e != nil {
		return e
	}
	for _, p := range g.peers[4:8] {
		if p.useful() != nil {
			return errRefused
		}
	}
	if emit(a.Nonce, "four-survivors", 4, snap.Kinds.Live, g) != nil {
		return errRefused
	}
	for i := 0; i < 4; i++ {
		if g.dial(server) != nil {
			return errRefused
		}
	}
	snap, e = c.active(ctx, 8)
	if e != nil {
		return e
	}
	for _, p := range g.peers[4:] {
		if p.useful() != nil {
			return errRefused
		}
	}
	if emit(a.Nonce, "eight-recovered", 8, snap.Kinds.Live, g) != nil {
		return errRefused
	}
	cleanup, stop = context.WithTimeout(ctx, operation)
	for _, p := range g.peers {
		if p.close(cleanup) != nil {
			stop()
			return errRefused
		}
	}
	stop()
	if g.check() != nil {
		return errRefused
	}
	snap, e = c.active(ctx, 0)
	if e != nil {
		return e
	}
	return emit(a.Nonce, "zero-drained", 0, snap.Kinds.Live, g)
}
func main() {
	// No source execution/activation by default. Future host must bind owner input.
	if len(os.Args) != 1 || os.Getenv("CORSA_C2A_RUN") != "1" {
		os.Exit(2)
	}
	raw, e := io.ReadAll(io.LimitReader(os.Stdin, 4097))
	if e != nil {
		os.Exit(2)
	}
	a, e := parseActivation(raw)
	if e != nil {
		os.Exit(2)
	}
	if os.Getuid() != 501 || os.Getgid() != 20 || syscall.Setrlimit(syscall.RLIMIT_CORE, &syscall.Rlimit{Cur: 0, Max: 0}) != nil {
		os.Exit(2)
	}
	s, e := readCredentials()
	if e != nil {
		os.Exit(2)
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, time.Duration(a.WorkMillis)*time.Millisecond)
	defer cancel()
	if work(ctx, a, s) != nil {
		_ = emit(a.Nonce, "STOP", -1, census{}, nil)
		os.Exit(2)
	}
}

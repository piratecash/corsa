package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func TestActivationAndSecretSchema(t *testing.T) {
	a := activation{true, "C2A8-v1", strings.Repeat("1", 32), strings.Repeat("2", 64), cliSHA, strings.Repeat("3", 40), "2026-10-25T00:00:00Z", 80000}
	raw, _ := json.Marshal(a)
	if _, e := parseActivation(raw); e != nil {
		t.Fatal("valid fixed activation", e)
	}
	for _, name := range []string{"inert", "wrong-profile", "wrong-CLI", "wrong-identity", "zero-identity", "deadline", "extra", "duplicate", "case-alias"} {
		t.Run(name, func(t *testing.T) {
			b := a
			switch name {
			case "inert":
				b.Ready = false
			case "wrong-profile":
				b.Profile = "other"
			case "wrong-CLI":
				b.CLISHA = strings.Repeat("0", 64)
			case "wrong-identity":
				b.Server = strings.Repeat("A", 40)
			case "zero-identity":
				b.Server = strings.Repeat("0", 40)
			case "deadline":
				b.WorkMillis = 80001
			}
			encoded, _ := json.Marshal(b)
			if name == "extra" {
				encoded = append(encoded[:len(encoded)-1], []byte(`,"extra":true}`)...)
			}
			if name == "duplicate" {
				encoded = append(encoded[:len(encoded)-1], []byte(`,"ready":true}`)...)
			}
			if name == "case-alias" {
				encoded = bytes.Replace(encoded, []byte(`"ready"`), []byte(`"READY"`), 1)
			}
			if _, e := parseActivation(encoded); e == nil {
				t.Fatal("unadmitted activation")
			}
		})
	}
	for _, raw := range []string{`{"username":"u","password":"p","password":"q"}`, `{"username":"u","password":"p","argv":"bad"}`, `{"username":"u:x","password":"p"}`, `{"username":"u","password":""}`, `{"USERNAME":"u","password":"p"}`} {
		if _, e := parseCredentials([]byte(raw)); e == nil {
			t.Fatal("bad secret schema accepted")
		}
	}
	if _, e := parseCredentials([]byte(`{"username":"u","password":"p"}`)); e != nil {
		t.Fatal(e)
	}
}
func TestBoundedFramesAndPartialAccounting(t *testing.T) {
	var b allowance
	if b.account(0) == nil || b.account(maxLine+1) == nil {
		t.Fatal("bad size accepted")
	}
	b.frames = maxFrames
	if b.account(1) == nil {
		t.Fatal("frame bound")
	}
	b.frames = 0
	b.bytes = maxBytes - 1
	if b.account(2) == nil {
		t.Fatal("byte bound")
	}
	b = allowance{}
	if _, e := readLine(bufio.NewReaderSize(strings.NewReader("unfinished"), maxLine), &b); e == nil || b.bytes != 10 {
		t.Fatal("partial read escaped accounting")
	}
	for _, raw := range []string{"{\"type\":\"peers\"}\n", "{\"type\":\"peers\",\"count\":0,\"peers\":[]}\n", "{\"type\":\"peers\",\"count\":1,\"peers\":[\"127.0.0.1:1\"]}\n"} {
		if _, e := parseFrame([]byte(raw)); e != nil {
			t.Fatal("valid empty/bounded response", e)
		}
	}
	for _, name := range []string{"duplicate", "null-count", "mismatch", "huge-count", "bad-peer", "truncated", "deep", "case-alias"} {
		t.Run(name, func(t *testing.T) {
			raw := "{\"type\":\"peers\"}\n"
			switch name {
			case "case-alias":
				raw = "{\"Type\":\"peers\"}\n"
			case "duplicate":
				raw = "{\"type\":\"peers\",\"type\":\"pong\"}\n"
			case "null-count":
				raw = "{\"type\":\"peers\",\"count\":null}\n"
			case "mismatch":
				raw = "{\"type\":\"peers\",\"count\":1}\n"
			case "huge-count":
				raw = "{\"type\":\"peers\",\"count\":65}\n"
			case "bad-peer":
				raw = "{\"type\":\"peers\",\"count\":1,\"peers\":[\"127.0.0.1:0\"]}\n"
			case "truncated":
				raw = "{\"type\":\"peers\"}"
			case "deep":
				raw = "{\"type\":\"peers\",\"x\":" + strings.Repeat("[", 25) + "0" + strings.Repeat("]", 25) + "}\n"
			}
			if _, e := parseFrame([]byte(raw)); e == nil {
				t.Fatal("malformed frame accepted")
			}
		})
	}
}
func pipePeer(t *testing.T, g *population) (*peer, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	if g == nil {
		g = &population{ctx: t.Context(), failures: make(chan error, 1)}
	}
	ctx, cancel := context.WithCancel(g.ctx)
	p := &peer{conn: a, ctx: ctx, cancel: cancel, callbackDone: make(chan struct{}), done: make(chan struct{}), g: g}
	g.peers = append(g.peers, p)
	context.AfterFunc(ctx, func() { _ = a.Close(); close(p.callbackDone) })
	go p.read()
	t.Cleanup(func() {
		_ = b.Close()
		c, stop := context.WithTimeout(context.Background(), operation)
		defer stop()
		_ = p.close(c)
	})
	return p, b
}
func TestIntentionalCloseAndUnexpectedEOF(t *testing.T) {
	t.Run("intentional-one-does-not-cancel-sibling", func(t *testing.T) {
		p, _ := pipePeer(t, nil)
		q, remote := pipePeer(t, p.g)
		ctx, stop := context.WithTimeout(t.Context(), operation)
		defer stop()
		if p.close(ctx) != nil || p.g.check() != nil {
			t.Fatal("normal retirement failed population")
		}
		server := make(chan error, 1)
		go func() {
			r := bufio.NewReader(remote)
			for _, want := range []string{"ping", "get_peers", "ping"} {
				line, e := r.ReadBytes('\n')
				if e != nil {
					server <- e
					return
				}
				f, e := parseFrame(line)
				if e != nil || f.Type != want {
					server <- errRefused
					return
				}
				reply := "{\"type\":\"pong\",\"network\":\"gazeta-devnet\"}\n"
				if want == "get_peers" {
					reply = "{\"type\":\"peers\"}\n"
				}
				if _, e = io.WriteString(remote, reply); e != nil {
					server <- e
					return
				}
			}
			server <- nil
		}()
		if q.useful() != nil {
			t.Fatal("intentional sibling retirement broke genuine remaining read/write")
		}
		if e := <-server; e != nil {
			t.Fatal(e)
		}
		select {
		case <-p.done:
		default:
			t.Fatal("reader not joined")
		}
		select {
		case <-p.callbackDone:
		default:
			t.Fatal("callback not joined")
		}
	})
	t.Run("unexpected-EOF-fails", func(t *testing.T) {
		p, remote := pipePeer(t, nil)
		_ = remote.Close()
		select {
		case <-p.g.failures:
		case <-time.After(operation):
			t.Fatal("unexpected EOF hidden")
		}
	})
}
func TestSingleOutstandingUsefulExchange(t *testing.T) {
	p, remote := pipePeer(t, nil)
	served := make(chan error, 1)
	go func() {
		r := bufio.NewReader(remote)
		for _, want := range []string{"ping", "get_peers", "ping"} {
			line, e := r.ReadBytes('\n')
			if e != nil {
				served <- e
				return
			}
			f, e := parseFrame(line)
			if e != nil || f.Type != want {
				served <- errRefused
				return
			}
			reply := "{\"type\":\"pong\",\"network\":\"gazeta-devnet\"}\n"
			if want == "get_peers" {
				reply = "{\"type\":\"peers\"}\n"
			}
			if _, e = io.WriteString(remote, reply); e != nil {
				served <- e
				return
			}
		}
		served <- nil
	}()
	if p.useful() != nil {
		t.Fatal("empty peers + FIFO barriers refused")
	}
	if e := <-served; e != nil {
		t.Fatal(e)
	}
	p.mu.Lock()
	p.waiting = &pending{kind: "pong", reply: make(chan protocol.Frame, 1)}
	p.mu.Unlock()
	ctx, stop := context.WithTimeout(t.Context(), operation)
	defer stop()
	if p.request(ctx, "ping", "pong") == nil {
		t.Fatal("second outstanding request accepted")
	}
}
func TestProductionProofRejectsWrongIdentity(t *testing.T) {
	listener, e := identity.Generate()
	if e != nil {
		t.Fatal(e)
	}
	dialer, e := identity.Generate()
	if e != nil {
		t.Fatal(e)
	}
	wrong, e := identity.Generate()
	if e != nil {
		t.Fatal(e)
	}
	localServer, e := sessionv2.NewLocal(listener, domain.NetworkID(networkName))
	if e != nil {
		t.Fatal(e)
	}
	localClient, e := sessionv2.NewLocal(dialer, domain.NetworkID(networkName))
	if e != nil {
		t.Fatal(e)
	}
	cert, e := sessionv2.NewCertificateSource(time.Now)
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"right", "wrong"} {
		t.Run(name, func(t *testing.T) {
			left, right := net.Pipe()
			defer func() { _ = left.Close() }()
			defer func() { _ = right.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), operation)
			defer cancel()
			serverDone := make(chan error, 1)
			go func() {
				s, e := sessionv2.Accept(ctx, left, localServer, protocol.Frame{Version: config.ProtocolVersion, Listener: "1"}, cert, sessionv2.DefaultTimeouts)
				if e == nil {
					_, e = s.Peer()
				}
				serverDone <- e
			}()
			expected := domain.PeerIdentityFromWire(listener.Address)
			if name == "wrong" {
				expected = domain.PeerIdentityFromWire(wrong.Address)
			}
			proof, e := sessionv2.Dial(ctx, right, localClient, protocol.Frame{Version: config.ProtocolVersion, Listener: "0"}, sessionv2.ExpectPeer(expected), sessionv2.DefaultTimeouts)
			if name == "wrong" {
				if !errors.Is(e, sessionv2.ErrPeerMismatch) {
					t.Fatal("wrong identity was not genuine proof refusal")
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				peer, e := proof.Peer()
				w, ok := peer.Proof()
				id, proven := w.Identity()
				if e != nil || !ok || !proven || id != expected {
					t.Fatal("no real original proof")
				}
			}
			_ = left.Close()
			_ = right.Close()
			select {
			case <-serverDone:
			case <-time.After(operation):
				t.Fatal("original Accept worker not joined")
			}
		})
	}
}

// Upstream source semantics, not an additional runtime/API permission mechanism.
func TestStockSourceClosure(t *testing.T) {
	pins := map[string]string{"internal/core/node/service.go": "607e644c46c2aed0688d2d4025afdbe030eca5cd9a431138ef979856a434bb4c", "internal/core/node/datagram_integration.go": "adbf2d8050d0f35882f72598c069c5f8a7100faaa85f8f3fe6e74b9c065ed192", "internal/core/node/network_consumer.go": "06d718a902edc2a379b63f05f277beefa019e2c5a65a2e2d3070911627918a40", "internal/core/node/loopback_inbound.go": "776c4a0237cdecba7974ead826a413c0935c7f3aef6660790b06d59ef1504be6", "internal/core/rpc/routing_commands.go": "0732ea237185dceaf4e9ef2fb7a104502251b98dbef7afa6ee58c362e307345a", "internal/app/node/app.go": "5f72326b0900c4bed5f7d64e9486945309c9230ca3910dde8946f3532f5db1ee", "internal/core/node/peer_management.go": "d3636f241d395d13b438a4b29733be958b56a62596ef2090cc37e3ec552224a5", "internal/core/netcore/net_core.go": "1521705a66677e6d633bff7778b2c91da9152438e266f07adb7fbfd09cf12a57"} // Frozen stock source; no runtime capability enabled.
	if len(pins) != 8 {
		t.Fatal("inert unbound source closure")
	}
	for name, want := range pins {
		raw, e := os.ReadFile(filepath.Join("..", "..", "..", "..", name))
		if e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != want {
			t.Fatal("upstream source changed", name)
		}
	}
	raw, e := os.ReadFile(filepath.Join("..", "..", "..", "..", "internal", "core", "node", "service.go"))
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(raw, []byte(`networkName = "gazeta-devnet"`)) {
		t.Fatal("stock network changed")
	}
}

func TestInstantActiveAndCachedCensusSeparation(t *testing.T) {
	raw := []byte(`{"loopback_inbound":{"started_at":"2026-10-25T00:00:01Z","read_at":"2026-10-25T00:00:03Z","enforced":true,"active":8,"max_active":64,"max_new_per_window":100,"window_seconds":10},"session_kinds":{"inbound_v2":999,"live":{"ready":false,"updated_at":null,"inbound_v2":{"sessions":0,"identities":0}}}}`)
	var r routeReply
	if decode(raw, 1024*1024, &r, false) != nil {
		t.Fatal("fixed snapshot decode")
	}
	c := &control{a: activation{StartedAt: "2026-10-25T00:00:00Z"}}
	if yes, e := c.acceptRoute(r, 8); !yes || e != nil {
		t.Fatal("fresh instantaneous gauge incorrectly depends on cached/cumulative census or different constructor label")
	}
	n := 4
	r.Loopback.Active = &n
	if yes, e := c.acceptRoute(r, 8); yes || e != nil {
		t.Fatal("wrong instantaneous active accepted")
	}
	r.Loopback.Active = nil
	if _, e := c.acceptRoute(r, 8); e == nil {
		t.Fatal("missing active treated as zero")
	}
	n = 0
	r.Loopback.Active = &n
	if yes, e := c.acceptRoute(r, 0); !yes || e != nil {
		t.Fatal("known zero not preserved")
	}
	changed := "2026-10-25T00:00:02Z"
	r.Loopback.StartedAt = &changed
	if _, e := c.acceptRoute(r, 0); e == nil {
		t.Fatal("class incarnation label changed silently")
	}
}
func TestUnexpectedReplyRefuses(t *testing.T) {
	for _, name := range []string{"unsolicited-peers", "wrong-network-pong", "unexpected-hello"} {
		t.Run(name, func(t *testing.T) {
			p, remote := pipePeer(t, nil)
			line := "{\"type\":\"peers\"}\n"
			if name == "wrong-network-pong" {
				p.mu.Lock()
				p.waiting = &pending{kind: "pong", reply: make(chan protocol.Frame, 1)}
				p.mu.Unlock()
				line = "{\"type\":\"pong\",\"network\":\"wrong\"}\n"
			}
			if name == "unexpected-hello" {
				line = "{\"type\":\"hello\"}\n"
			}
			sent := make(chan error, 1)
			go func() { _, e := io.WriteString(remote, line); sent <- e }()
			select {
			case <-p.g.failures:
			case <-time.After(operation):
				t.Fatal("unexpected frame silently dropped")
			}
			select {
			case <-sent:
			case <-time.After(operation):
				t.Fatal("sender not joined")
			}
		})
	}
}

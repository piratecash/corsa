package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
)

// okCommand is a registered command that does nothing but succeed, so a test
// can tell "the gate refused me" from "the handler failed".
func okCommand(name string, policy TransportPolicy) (CommandInfo, CommandHandler) {
	return CommandInfo{Name: name, Description: "test", Category: "test", Transport: policy},
		func(CommandRequest) CommandResponse {
			return CommandResponse{Data: json.RawMessage(`{"ok":true}`)}
		}
}

// TestCheckTransportPolicy pins the decision itself, away from HTTP: the
// loopback policy needs BOTH a loopback socket and a listener that demands
// credentials, and an unknown policy denies rather than defaults to open.
func TestCheckTransportPolicy(t *testing.T) {
	t.Parallel()
	loopback4 := net.ParseIP("127.0.0.1")
	loopback6 := net.ParseIP("::1")
	remote := net.ParseIP("203.0.113.9")

	cases := []struct {
		name    string
		policy  TransportPolicy
		ip      net.IP
		auth    bool
		allowed bool
	}{
		{"any: remote host with auth", TransportAnyAuthenticated, remote, true, true},
		{"any: remote host without auth", TransportAnyAuthenticated, remote, false, true},
		{"loopback: IPv4 loopback with auth", TransportLoopbackOnly, loopback4, true, true},
		{"loopback: IPv6 loopback with auth", TransportLoopbackOnly, loopback6, true, true},
		{"loopback: loopback without auth", TransportLoopbackOnly, loopback4, false, false},
		{"loopback: remote host with auth", TransportLoopbackOnly, remote, true, false},
		{"loopback: unknown socket address", TransportLoopbackOnly, nil, true, false},
		{"in-process: loopback with auth", TransportInProcessOnly, loopback4, true, false},
		{"unknown policy denies", TransportPolicy(42), loopback4, true, false},
	}

	for _, tc := range cases {
		err := checkTransportPolicy(tc.policy, tc.ip, tc.auth)
		if tc.allowed && err != nil {
			t.Fatalf("%s: refused with %v", tc.name, err)
		}
		if !tc.allowed {
			if err == nil {
				t.Fatalf("%s: allowed", tc.name)
			}
			if !errors.Is(err, ErrTransportForbidden) {
				t.Fatalf("%s: error = %v, want ErrTransportForbidden", tc.name, err)
			}
		}
	}
}

// TestTransportPolicyForResolvesAliasesAndCase: the policy must survive every
// spelling Execute accepts. A gate that can be dodged by typing the snake_case
// alias — or shouting the name — is not a gate.
func TestTransportPolicyForResolvesAliasesAndCase(t *testing.T) {
	t.Parallel()
	table := NewCommandTable()
	table.Register(okCommand("secretThing", TransportLoopbackOnly))
	table.RegisterAlias("secret_thing", "secretThing")

	for _, spelling := range []string{"secretThing", "secret_thing", "SECRETTHING", "Secret_Thing"} {
		policy, known := table.TransportPolicyFor(spelling)
		if !known {
			t.Fatalf("%q: command not found", spelling)
		}
		if policy != TransportLoopbackOnly {
			t.Fatalf("%q: policy = %s, want loopback_only", spelling, policy)
		}
	}

	if _, known := table.TransportPolicyFor("noSuchCommand"); known {
		t.Fatal("an unregistered name reported a policy")
	}
}

// TestTransportPolicyJSONRoundTrip: a custom encoder without a matching
// decoder is a broken type. Adding Transport to CommandInfo with only
// MarshalJSON made the whole help response undecodable for every client that
// reads it back into []CommandInfo — the encoder writes a string, the default
// decoder for an integer type wants a number.
func TestTransportPolicyJSONRoundTrip(t *testing.T) {
	t.Parallel()
	for _, policy := range []TransportPolicy{TransportAnyAuthenticated, TransportLoopbackOnly, TransportInProcessOnly} {
		encoded, err := json.Marshal(policy)
		if err != nil {
			t.Fatalf("%s: marshal: %v", policy, err)
		}
		var decoded TransportPolicy
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("%s: unmarshal %s: %v", policy, encoded, err)
		}
		if decoded != policy {
			t.Fatalf("round trip of %s produced %s", policy, decoded)
		}
	}

	// A CommandInfo carrying a restricted policy must survive the same trip,
	// because that — not the bare enum — is what clients actually decode.
	original := CommandInfo{Name: "identityBackup", Category: "identity", Transport: TransportLoopbackOnly}
	encoded, err := json.Marshal([]CommandInfo{original})
	if err != nil {
		t.Fatalf("marshal command list: %v", err)
	}
	var list []CommandInfo
	if err := json.Unmarshal(encoded, &list); err != nil {
		t.Fatalf("unmarshal command list %s: %v", encoded, err)
	}
	if len(list) != 1 || list[0].Transport != TransportLoopbackOnly {
		t.Fatalf("command list round trip = %+v", list)
	}

	// Tolerated inputs: an absent field is the default reach, and a numeric
	// value from an older payload still decodes.
	var absent CommandInfo
	if err := json.Unmarshal([]byte(`{"name":"ping"}`), &absent); err != nil {
		t.Fatalf("unmarshal without the field: %v", err)
	}
	if absent.Transport != TransportAnyAuthenticated {
		t.Fatalf("absent transport decoded as %s", absent.Transport)
	}
	var numeric TransportPolicy
	if err := json.Unmarshal([]byte(`1`), &numeric); err != nil {
		t.Fatalf("unmarshal numeric: %v", err)
	}
	if numeric != TransportLoopbackOnly {
		t.Fatalf("numeric 1 decoded as %s", numeric)
	}

	// And a value this build does not know is a rejection, not a silent
	// downgrade to "reachable from anywhere".
	for _, bad := range []string{`"wide_open"`, `9`} {
		var policy TransportPolicy
		if err := json.Unmarshal([]byte(bad), &policy); err == nil {
			t.Fatalf("unknown policy %s decoded as %s", bad, policy)
		}
	}
}

// TestHTTPRefusesLoopbackOnlyFromNonLoopbackSocket drives the gate through
// the real Fiber stack. fiber's test connection reports 0.0.0.0 as its peer,
// which is exactly the shape of the case that must be refused, and it proves
// the refusal reads the SOCKET rather than a header.
func TestHTTPRefusesLoopbackOnlyFromNonLoopbackSocket(t *testing.T) {
	t.Parallel()
	table := NewCommandTable()
	table.Register(okCommand("secretThing", TransportLoopbackOnly))
	table.Register(okCommand("openThing", TransportAnyAuthenticated))

	server, err := NewServer(config.RPC{Host: "127.0.0.1", Port: "0", Username: "u", Password: "p"}, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	// A forged forwarding header must not buy access: the gate reads the
	// socket, and the socket here is 0.0.0.0.
	status, body := execOverTest(t, server, "secretThing", map[string]string{
		"X-Forwarded-For": "127.0.0.1",
		"X-Real-IP":       "127.0.0.1",
	})
	if status != http.StatusForbidden {
		t.Fatalf("loopback-only command over a non-loopback socket returned %d: %s", status, body)
	}

	if status, body := execOverTest(t, server, "openThing", nil); status != http.StatusOK {
		t.Fatalf("an unrestricted command was refused: %d %s", status, body)
	}
}

// TestHTTPAllowsLoopbackOnlyOverRealLoopbackSocket is the other half: the
// policy must not be a blanket ban, or corsa-cli loses identity backup on a
// headless node. This binds a real listener and dials it over 127.0.0.1, so
// the socket address is genuine rather than simulated.
func TestHTTPAllowsLoopbackOnlyOverRealLoopbackSocket(t *testing.T) {
	table := NewCommandTable()
	table.Register(okCommand("secretThing", TransportLoopbackOnly))

	port := freeLoopbackPort(t)
	server, err := NewServer(config.RPC{Host: "127.0.0.1", Port: port, Username: "u", Password: "p"}, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := server.StartAsync(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.ShutdownWithTimeout(5 * time.Second) })

	req, err := http.NewRequest(http.MethodPost,
		"http://"+net.JoinHostPort("127.0.0.1", port)+"/rpc/v1/exec",
		strings.NewReader(`{"command":"secretThing"}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("u", "p")

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("loopback-only command over a real loopback socket returned %d: %s", resp.StatusCode, body)
	}
}

// TestHTTPRefusesLoopbackOnlyWithoutAuth: an address check answers "is the
// caller on this machine?", not "is the caller this user". Every other local
// process is also on this machine.
func TestHTTPRefusesLoopbackOnlyWithoutAuth(t *testing.T) {
	table := NewCommandTable()
	table.Register(okCommand("secretThing", TransportLoopbackOnly))

	port := freeLoopbackPort(t)
	server, err := NewServer(config.RPC{Host: "127.0.0.1", Port: port}, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := server.StartAsync(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.ShutdownWithTimeout(5 * time.Second) })

	resp, err := http.Post("http://"+net.JoinHostPort("127.0.0.1", port)+"/rpc/v1/exec",
		"application/json", strings.NewReader(`{"command":"secretThing"}`))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("an unauthenticated listener served a loopback-only command: %d %s", resp.StatusCode, body)
	}
}

// TestUnknownCommandStaysNotFound: the gate must not turn a typo into a 403,
// or the status code becomes a probe for which commands exist.
func TestUnknownCommandStaysNotFound(t *testing.T) {
	t.Parallel()
	table := NewCommandTable()
	table.Register(okCommand("openThing", TransportAnyAuthenticated))
	server, err := NewServer(config.RPC{Host: "127.0.0.1", Port: "0"}, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if status, body := execOverTest(t, server, "noSuchCommand", nil); status != http.StatusNotFound {
		t.Fatalf("unknown command returned %d: %s", status, body)
	}
}

// execOverTest posts a command through the universal /exec dispatcher using
// fiber's in-memory connection (peer 0.0.0.0).
func execOverTest(t *testing.T, server *Server, command string, headers map[string]string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/rpc/v1/exec",
		strings.NewReader(fmt.Sprintf(`{"command":%q}`, command)))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("u", "p")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := server.Test(req)
	if err != nil {
		t.Fatalf("test request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// freeLoopbackPort reserves and releases a loopback port so the server can
// bind a known one. The RPC config takes a port string and StartAsync does
// not report the bound port back, so "port 0" would leave the test with no
// address to dial.
func freeLoopbackPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split port: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return port
}

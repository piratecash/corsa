package rpc

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
)

// Compile-time proof that the server answers for every verb. Stringer alone
// would leave %d and %x resolving through fmt's reflective walk, which is the
// path that reaches an unexported field's contents.
var (
	_ fmt.Formatter  = &Server{}
	_ fmt.Stringer   = &Server{}
	_ fmt.GoStringer = &Server{}
)

// TestServerNeverRendersPassword: Server keeps a config.RPC — password
// included — in an UNEXPORTED field, and fmt calls no method on the way down
// through one of those. The redaction on config.RPC is therefore worth
// nothing here; the container has to answer for itself.
func TestServerNeverRendersPassword(t *testing.T) {
	t.Parallel()
	const password = "s3cr3t-rpc-password"
	table := NewCommandTable()
	table.Register(okCommand("openThing", TransportAnyAuthenticated))

	server, err := NewServer(config.RPC{Host: "127.0.0.1", Port: "46464", Username: "corsa", Password: password}, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
		rendered := fmt.Sprintf(verb, server)
		if strings.Contains(rendered, password) {
			t.Fatalf("fmt %s of the server leaked the password: %s", verb, rendered)
		}
		if !strings.Contains(rendered, "rpc.Server{") {
			t.Fatalf("fmt %s of the server did not go through Format: %s", verb, rendered)
		}
		// The diagnostics must survive the redaction, or every log line about
		// the server becomes useless and someone reaches for %+v on its guts.
		if !strings.Contains(rendered, "127.0.0.1:46464") {
			t.Fatalf("fmt %s of the server dropped the listen address: %s", verb, rendered)
		}
	}

	// A struct merely HOLDING the server is the realistic accident.
	holder := struct {
		Server *Server
		Note   string
	}{Server: server, Note: "support bundle"}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if rendered := fmt.Sprintf(verb, holder); strings.Contains(rendered, password) {
			t.Fatalf("fmt %s of a struct holding the server leaked the password: %s", verb, rendered)
		}
	}

	// The VALUE form is the case a pointer-receiver Format cannot answer:
	// fmt walks the copy by reflection and calls nothing. It is only safe
	// because the field holds no password — which is the point of storing it
	// nowhere rather than relying on the method.
	//
	// (Copying a Server is itself a bug go vet reports, so this is not a
	// supported way to print one — but "vet would have caught it" is not a
	// reason for the bytes to be there.)
	if server.cfg.Password != "" {
		t.Fatalf("the server retained the password after construction: %q", server.cfg.Password)
	}

	// And authentication must still work — the middleware captured the
	// credentials before the field was cleared.
	if !server.authEnabled {
		t.Fatal("clearing the stored password disarmed authentication")
	}
	if status, body := execWithCredentials(t, server, "corsa", "wrong-password"); status != http.StatusUnauthorized {
		t.Fatalf("wrong credentials returned %d (%s), want 401", status, body)
	}
	if status, body := execWithCredentials(t, server, "corsa", password); status != http.StatusOK {
		t.Fatalf("correct credentials returned %d (%s), want 200", status, body)
	}
}

// execWithCredentials runs one authenticated command through the server's
// in-memory connection and returns the status and body.
func execWithCredentials(t *testing.T, server *Server, username, password string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/rpc/v1/exec", strings.NewReader(`{"command":"openThing"}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)
	resp, err := server.Test(req)
	if err != nil {
		t.Fatalf("test request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// TestNilServerRendersWithoutPanic: a redaction helper that panics on nil
// turns a diagnostic print into a crash, in exactly the situation — something
// went wrong — where the print was added.
func TestNilServerRendersWithoutPanic(t *testing.T) {
	t.Parallel()
	var server *Server
	if rendered := fmt.Sprintf("%+v", server); !strings.Contains(rendered, "rpc.Server(nil)") {
		t.Fatalf("nil server rendered as %s", rendered)
	}
}

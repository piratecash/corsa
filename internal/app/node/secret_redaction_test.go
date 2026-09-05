package node

import (
	"fmt"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
)

// The VALUE assertions are the load-bearing ones: a method set that covers
// only the pointer leaves fmt.Sprintf("%+v", *app) printing the password.
var (
	_ fmt.Formatter  = App{}
	_ fmt.Stringer   = App{}
	_ fmt.GoStringer = App{}
	_ fmt.Formatter  = &App{}
)

// TestAppNeverRendersRPCPassword: App holds a whole config.Config BY VALUE in
// an unexported field, and fmt reaches no method through one of those — it
// walks the field by reflection, so config.RPC's own redaction never runs.
//
// The struct is built directly rather than through New(), which would load an
// identity, open a database and start a node; the field under test is the
// config, and giving it one is the whole setup this needs.
func TestAppNeverRendersRPCPassword(t *testing.T) {
	t.Parallel()
	const password = "s3cr3t-rpc-password"
	cfg := config.Default()
	cfg.RPC.Username = "corsa"
	cfg.RPC.Password = password
	app := &App{cfg: cfg}

	for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
		// Both forms: one character at the call site used to decide whether
		// the credential reached the log.
		for label, subject := range map[string]any{"pointer": app, "value": *app} {
			rendered := fmt.Sprintf(verb, subject)
			if strings.Contains(rendered, password) {
				t.Fatalf("fmt %s of the %s app leaked the RPC password: %s", verb, label, rendered)
			}
			if !strings.Contains(rendered, "node.App{") {
				t.Fatalf("fmt %s of the %s app did not go through Format: %s", verb, label, rendered)
			}
			// The diagnostics must survive, or the next person reaches past
			// the redaction for the struct's guts.
			if !strings.Contains(rendered, cfg.Node.ListenAddress) {
				t.Fatalf("fmt %s of the %s app dropped the listen address: %s", verb, label, rendered)
			}
		}
	}

	// A struct merely HOLDING the app is the realistic accident, by value as
	// much as by pointer.
	holder := struct {
		App      *App
		AppValue App
		Note     string
	}{App: app, AppValue: *app, Note: "support bundle"}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if rendered := fmt.Sprintf(verb, holder); strings.Contains(rendered, password) {
			t.Fatalf("fmt %s of a struct holding the app leaked the password: %s", verb, rendered)
		}
	}

	// The config itself still carries the credential — the redaction changes
	// what is printed, not what the RPC server is given.
	if app.cfg.RPC.Password != password {
		t.Fatal("redaction changed what the app holds")
	}
}

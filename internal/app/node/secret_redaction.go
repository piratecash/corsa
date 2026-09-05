package node

import (
	"fmt"
	"io"
)

// secret_redaction.go keeps the RPC password out of every formatted rendering
// of the headless app.
//
// App holds a whole config.Config BY VALUE in an unexported field, and fmt
// reaches no method through one of those: it walks the field by reflection,
// calling nothing, so the redaction config.RPC does for itself never runs and
// the password is printed verbatim. The container has to answer.
//
// Formatter rather than Stringer: %v, %s, %q and %x consult Stringer and %#v
// consults GoStringer, but %d consults neither.
//
// Unlike rpc.Server, this struct does not drop the secret after use — App
// keeps the config as the process's own configuration record, and clearing a
// field of it would be surprising to every other reader. So the redaction is
// the whole answer here, which is exactly why it must cover every verb.

// Format renders the app for EVERY verb, redacted.
//
// VALUE receiver: a method on *App is in the pointer's method set only, so
// fmt.Sprintf("%+v", app) would be redacted while fmt.Sprintf("%+v", *app)
// walked the copy by reflection and printed the password. A value receiver is
// in both method sets. It is available here because App holds no locks.
func (a App) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, a.String())
}

// String is the single redacted rendering Format hands to every verb.
func (a App) String() string {
	return fmt.Sprintf("node.App{Network: %s, Listen: %s, RPC: %s:%s, Secrets: redacted}",
		a.cfg.App.Network, a.cfg.Node.ListenAddress, a.cfg.RPC.Host, a.cfg.RPC.Port)
}

// GoString covers %#v for callers that reach it without going through Format.
func (a App) GoString() string {
	return a.String()
}

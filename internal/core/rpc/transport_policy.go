package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
)

// transport_policy.go answers, in the type system, the question "where may
// this command be called from?".
//
// CommandTable is one table shared by two very different callers: the desktop
// console, which runs INSIDE the node process and is the user sitting at the
// machine, and the HTTP server, which is a socket. Before this policy existed
// the distinction lived only in a comment ("LOCAL-ONLY RPC") at the top of a
// handler file — and a guarantee written only in a comment is not a
// guarantee. identityBackup was the case that made this concrete: it is
// documented as local-only, and it was reachable from any host the operator's
// RPC listener happened to be bound to.

// TransportPolicy declares which transports may reach a command.
type TransportPolicy int

const (
	// TransportAnyAuthenticated is the zero value and the default: the
	// command is reachable from anywhere the RPC listener accepts, subject
	// to the listener's own authentication. Every command that existed
	// before this policy keeps exactly its previous reach.
	TransportAnyAuthenticated TransportPolicy = iota

	// TransportLoopbackOnly restricts the command to a caller on the same
	// machine — an in-process console call, or an HTTP request whose real
	// socket peer is a loopback address AND whose listener has
	// authentication configured. Both conditions are required: the address
	// check keeps out remote hosts, the auth requirement keeps out any
	// other process on the same machine.
	TransportLoopbackOnly

	// TransportInProcessOnly restricts the command to callers inside the
	// node process (the desktop console). No HTTP request qualifies,
	// loopback or not.
	TransportInProcessOnly
)

func (p TransportPolicy) String() string {
	switch p {
	case TransportAnyAuthenticated:
		return "any_authenticated"
	case TransportLoopbackOnly:
		return "loopback_only"
	case TransportInProcessOnly:
		return "in_process_only"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
	}
}

// MarshalJSON renders the policy by name. The numeric value is an
// implementation detail whose meaning would silently change if a constant
// were ever inserted in the middle of the block; the name is what a client
// can act on.
func (p TransportPolicy) MarshalJSON() ([]byte, error) {
	return []byte(`"` + p.String() + `"`), nil
}

// transportPolicyByName is the inverse of String, and the two are kept in one
// file precisely so a renamed constant breaks both halves at once.
var transportPolicyByName = map[string]TransportPolicy{
	"any_authenticated": TransportAnyAuthenticated,
	"loopback_only":     TransportLoopbackOnly,
	"in_process_only":   TransportInProcessOnly,
}

// UnmarshalJSON is the decoder MarshalJSON obliges this type to have. Without
// it, adding the field to CommandInfo silently broke every client that reads
// the command list back into []CommandInfo — rpc.Client.FetchCommands among
// them: the encoder emits a string, the default decoder for an integer type
// wants a number, and the whole help response fails to parse.
//
// A number is accepted too. Not for symmetry — nothing this build writes
// emits one — but because an older peer's stored payload may carry the
// pre-string encoding, and refusing to read it would turn a cosmetic format
// change into a hard failure.
func (p *TransportPolicy) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "null" {
		// Absent means "unspecified", which is the default reach — the same
		// thing the field's omitempty produces on the way out.
		*p = TransportAnyAuthenticated
		return nil
	}
	if strings.HasPrefix(trimmed, `"`) {
		var name string
		if err := json.Unmarshal(data, &name); err != nil {
			return fmt.Errorf("transport policy: %w", err)
		}
		policy, ok := transportPolicyByName[name]
		if !ok {
			return fmt.Errorf("transport policy: unknown value %q", name)
		}
		*p = policy
		return nil
	}
	var numeric int
	if err := json.Unmarshal(data, &numeric); err != nil {
		return fmt.Errorf("transport policy: %w", err)
	}
	policy := TransportPolicy(numeric)
	if _, ok := transportPolicyByName[policy.String()]; !ok {
		return fmt.Errorf("transport policy: unknown value %d", numeric)
	}
	*p = policy
	return nil
}

// ErrTransportForbidden is returned when a command is refused because of
// where the call came from, not because of what it asked for. Distinguishable
// via errors.Is so the HTTP layer maps it to 403 without reading text.
var ErrTransportForbidden = errors.New("command not available on this transport")

// checkTransportPolicy decides whether an HTTP request may run a command.
//
// remote is the address of the REAL socket peer — never a header. X-Forwarded-For
// and its relatives are written by the client, so a policy that trusted them
// would be satisfied by anyone willing to type one.
//
// authEnabled reports whether the listener requires credentials. A loopback
// check alone answers "is the caller on this machine?", not "is the caller
// this user" — every other local process, including a browser rendering a
// hostile page, is also on this machine.
func checkTransportPolicy(policy TransportPolicy, remote net.IP, authEnabled bool) error {
	switch policy {
	case TransportAnyAuthenticated:
		return nil

	case TransportLoopbackOnly:
		if !authEnabled {
			return fmt.Errorf("%w: loopback-only commands require RPC authentication (set CORSA_RPC_USERNAME and CORSA_RPC_PASSWORD)", ErrTransportForbidden)
		}
		if remote == nil {
			return fmt.Errorf("%w: the caller's socket address is unknown, so loopback cannot be proven", ErrTransportForbidden)
		}
		if !remote.IsLoopback() {
			return fmt.Errorf("%w: loopback-only command called from %s", ErrTransportForbidden, remote)
		}
		return nil

	case TransportInProcessOnly:
		return fmt.Errorf("%w: this command is available only inside the node process", ErrTransportForbidden)

	default:
		// An unknown policy is a programming error, and the safe reading of
		// "I do not know what this permits" is "nothing".
		return fmt.Errorf("%w: unknown transport policy %d", ErrTransportForbidden, int(policy))
	}
}

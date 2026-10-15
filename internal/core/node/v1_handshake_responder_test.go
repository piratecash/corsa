package node

import (
	"bufio"
	"io"
	"strings"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// v1HandshakeResponder plays the v1 handshake of a responder at this build's
// protocol version with the same calls the real hello and auth_session
// handlers make: connauth.PrepareAuth mints the challenge (and refuses a hello
// whose identity fields do not bind), connauth.VerifyAuthSession decides
// between auth_ok and the error a bad signature gets. Nothing past the
// handshake is answered here — each mock serves its own commands.
type v1HandshakeResponder struct {
	identity *identity.Identity
	auth     *connauth.State
}

func newV1HandshakeResponder() (*v1HandshakeResponder, error) {
	id, err := identity.Generate()
	if err != nil {
		return nil, err
	}
	return &v1HandshakeResponder{identity: id}, nil
}

// welcome answers hello: this build's protocol range, the responder's
// identity and a fresh challenge.
func (r *v1HandshakeResponder) welcome(hello protocol.Frame) (protocol.Frame, error) {
	state, err := connauth.PrepareAuth(hello)
	if err != nil {
		return protocol.Frame{}, err
	}
	r.auth = state
	return protocol.Frame{
		Type:                   "welcome",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Address:                r.identity.Address,
		Challenge:              state.Challenge,
	}, nil
}

// authenticated reports whether an auth_session has verified.
func (r *v1HandshakeResponder) authenticated() bool {
	return r.auth != nil && r.auth.Verified
}

// authReply answers auth_session: auth_ok for a valid signature over the
// challenge, the responder's error otherwise.
func (r *v1HandshakeResponder) authReply(authSession protocol.Frame) protocol.Frame {
	verified, reply, _ := connauth.VerifyAuthSession(r.auth, authSession)
	r.auth = verified
	return reply
}

// answerHello reads the dialler's hello from reader and writes the welcome to
// conn. Returns the hello.
func (r *v1HandshakeResponder) answerHello(conn io.Writer, reader *bufio.Reader) (protocol.Frame, error) {
	hello, err := readTestFrameLine(reader)
	if err != nil {
		return protocol.Frame{}, err
	}
	welcome, err := r.welcome(hello)
	if err != nil {
		return hello, err
	}
	return hello, writeTestFrameLine(conn, welcome)
}

// answerAuth reads the dialler's auth_session from reader and writes the
// verdict to conn. Returns the verdict.
func (r *v1HandshakeResponder) answerAuth(conn io.Writer, reader *bufio.Reader) (protocol.Frame, error) {
	authSession, err := readTestFrameLine(reader)
	if err != nil {
		return protocol.Frame{}, err
	}
	reply := r.authReply(authSession)
	return reply, writeTestFrameLine(conn, reply)
}

func readTestFrameLine(reader *bufio.Reader) (protocol.Frame, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return protocol.Frame{}, err
	}
	return protocol.ParseFrameLine(strings.TrimSpace(line))
}

func writeTestFrameLine(conn io.Writer, frame protocol.Frame) error {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return err
	}
	_, err = io.WriteString(conn, line)
	return err
}

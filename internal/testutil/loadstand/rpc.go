package loadstand

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

var ErrFamilyUnavailable = errors.New("process metric family unavailable")
var ErrInvalidSample = errors.New("invalid process metric response")
var ErrRPCClosed = errors.New("process RPC handle is closed")
var ErrRPCAccess = errors.New("process RPC access refused")

type RPCOptions struct {
	Address     domain.ListenAddress
	Credentials *Credentials
	DialContext func(context.Context, string, string) (net.Conn, error)
}

// RPCClient owns its HTTP transport; Close ends idle sockets before a node is
// restarted. Its transport never uses an ambient HTTP proxy or follows redirects.
type RPCClient struct {
	address     domain.ListenAddress
	credentials *Credentials
	http        *http.Client
	transport   *http.Transport
	closed      context.Context
	cancel      context.CancelFunc
}

func NewRPCClient(options RPCOptions) (*RPCClient, error) {
	if !options.Credentials.valid() {
		return nil, ErrInvalidConfiguration
	}
	if _, _, err := net.SplitHostPort(string(options.Address)); err != nil {
		return nil, ErrInvalidConfiguration
	}
	dial := options.DialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: time.Second}).DialContext
	}
	transport := &http.Transport{DialContext: dial, MaxIdleConnsPerHost: 1}
	closed, cancel := context.WithCancel(context.Background())
	return &RPCClient{address: options.Address, credentials: options.Credentials, transport: transport, closed: closed, cancel: cancel, http: &http.Client{Transport: transport, Timeout: 6 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *RPCClient) Close() { c.cancel(); c.transport.CloseIdleConnections() }
func (c *RPCClient) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprintf(state, "RPCClient(%s)", c.address)
}
func (c *RPCClient) Call(ctx context.Context, command string, args any, target any) error {
	if c.closed.Err() != nil {
		return ErrRPCClosed
	}
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopClosing := context.AfterFunc(c.closed, cancel)
	defer stopClosing()
	payload, err := json.Marshal(struct {
		Command string `json:"command"`
		Args    any    `json:"args,omitempty"`
	}{command, args})
	if err != nil {
		return fmt.Errorf("encode RPC request: %w", err)
	}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, "http://"+string(c.address)+"/rpc/v1/exec", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build RPC request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.credentials.username, c.credentials.passwordText())
	response, err := c.http.Do(req)
	if err != nil {
		if c.closed.Err() != nil {
			return ErrRPCClosed
		}
		return fmt.Errorf("RPC transport: %w", err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	closeErr := response.Body.Close()
	if c.closed.Err() != nil {
		return ErrRPCClosed
	}
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("read RPC response: %w", err)
	}
	switch response.StatusCode {
	case http.StatusNotFound, http.StatusServiceUnavailable:
		return ErrFamilyUnavailable
	case http.StatusForbidden, http.StatusUnauthorized:
		return ErrRPCAccess
	case http.StatusOK:
	default:
		return fmt.Errorf("RPC status %d", response.StatusCode)
	}
	if len(raw) > 1<<20 {
		return ErrInvalidSample
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("%w: JSON decode", ErrInvalidSample)
	}
	return nil
}

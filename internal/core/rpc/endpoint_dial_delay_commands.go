package rpc

import (
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// endpoint_dial_delay_commands.go registers the operator's view of the
// per-endpoint dial delays (docs/rpc/secure_session.md):
// the list — reason, claimed version, next attempt — and the retry of one
// endpoint by hand after its peer was upgraded.
//
// Both are TransportLoopbackOnly: the list names endpoints, and a retry lets
// the node dial an endpoint it was holding off. A retry ends the current
// old-version wait of the endpoint it names, keeping its count, and nothing
// else — no pin, no endpoint binding, no v2 retry backoff, no dial cooldown —
// so it never permits a downgrade; it dials nothing itself. The handlers
// parse and validate; the node decides.

// EndpointDialDelayDTO is one endpoint of listEndpointDialDelays.
type EndpointDialDelayDTO struct {
	Endpoint       domain.PeerAddress             `json:"endpoint"`
	Reason         domain.EndpointDialDelayReason `json:"reason"`
	ClaimedVersion domain.ProtocolVersion         `json:"claimed_version"`
	Steps          int                            `json:"steps"`
	NextAttemptAt  time.Time                      `json:"next_attempt_at"`
}

// RegisterEndpointDialDelayCommands registers the dial-delay commands. A nil
// provider registers them as unavailable — with their policy, so a remote
// caller is refused by the gate before a 503 tells it anything.
func RegisterEndpointDialDelayCommands(t *CommandTable, dd EndpointDialDelayProvider) {
	listInfo := CommandInfo{
		Name:        "listEndpointDialDelays",
		Description: "List the endpoints whose dials are delayed: reason, claimed version, next attempt",
		Category:    "secure_session",
		Usage:       "",
		Transport:   TransportLoopbackOnly,
	}
	retryInfo := CommandInfo{
		Name:        "retryEndpointDial",
		Description: "End the current old-version wait of one endpoint (its count is kept): the next dial the node makes may go at once; dials nothing, lifts no pin, binding, v2 retry backoff or dial cooldown",
		Category:    "secure_session",
		Usage:       "<host:port>",
		Transport:   TransportLoopbackOnly,
	}
	if dd == nil {
		t.RegisterUnavailable(listInfo)
		t.RegisterUnavailable(retryInfo)
		return
	}
	t.Register(listInfo, listEndpointDialDelaysHandler(dd))
	t.Register(retryInfo, retryEndpointDialHandler(dd))
}

func listEndpointDialDelaysHandler(dd EndpointDialDelayProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		delays, err := dd.ListEndpointDialDelays(requestContext(req))
		if err != nil {
			return internalError(fmt.Errorf("list endpoint dial delays: %w", err))
		}
		dtos := make([]EndpointDialDelayDTO, 0, len(delays))
		for _, delay := range delays {
			dtos = append(dtos, EndpointDialDelayDTO{
				Endpoint:       delay.Endpoint,
				Reason:         delay.Reason,
				ClaimedVersion: delay.ClaimedVersion,
				Steps:          delay.Steps,
				NextAttemptAt:  delay.NextAttemptAt,
			})
		}
		return jsonResponse(dtos)
	}
}

// endpointDialRetryResult is the answer of retryEndpointDial.
type endpointDialRetryResult struct {
	Endpoint domain.PeerAddress `json:"endpoint"`
	Retried  bool               `json:"retried"`
}

func retryEndpointDialHandler(dd EndpointDialDelayProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		endpoint, err := requiredEndpointArg(req.Args, "endpoint")
		if err != nil {
			return validationError(err)
		}
		if err := dd.RetryEndpointDial(requestContext(req), endpoint); err != nil {
			wrapped := fmt.Errorf("retry endpoint dial: %w", err)
			if errors.Is(err, domain.ErrEndpointDialDelayNotFound) {
				return validationError(wrapped)
			}
			return internalError(wrapped)
		}
		return jsonResponse(endpointDialRetryResult{Endpoint: endpoint, Retried: true})
	}
}

// --- client ---

// ListEndpointDialDelays runs listEndpointDialDelays. The RPC listener must
// be reached over loopback with credentials.
func (c *Client) ListEndpointDialDelays() ([]EndpointDialDelayDTO, error) {
	var delays []EndpointDialDelayDTO
	return delays, c.execInto("listEndpointDialDelays", map[string]interface{}{}, &delays)
}

// RetryEndpointDial runs retryEndpointDial for one endpoint.
func (c *Client) RetryEndpointDial(endpoint string) error {
	var result endpointDialRetryResult
	return c.execInto("retryEndpointDial", map[string]interface{}{"endpoint": endpoint}, &result)
}

package startupfence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type request struct {
	Action  string              `json:"action"`
	Node    domain.PeerIdentity `json:"node"`
	Nonce   string              `json:"nonce,omitempty"`
	Grant   *StartupGrant       `json:"grant,omitempty"`
	Lease   *Lease              `json:"lease,omitempty"`
	Binding *Binding            `json:"binding,omitempty"`
	Receipt *Receipt            `json:"receipt,omitempty"`
}
type response struct {
	Error  string         `json:"error,omitempty"`
	Status *Status        `json:"status,omitempty"`
	Grant  *StartupGrant  `json:"grant,omitempty"`
	Lease  *Lease         `json:"lease,omitempty"`
	Permit *RemovalPermit `json:"permit,omitempty"`
}

type BrokerHandler struct {
	Ledger              *Ledger
	OperatorFingerprint string
}

func Fingerprint(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}

func (h BrokerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/fence" {
		http.NotFound(w, r)
		return
	}
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "authentication required", http.StatusForbidden)
		return
	}
	var req request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.respond(w, response{Error: ErrInvalid.Error()}, http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		h.respond(w, response{Error: ErrInvalid.Error()}, http.StatusBadRequest)
		return
	}
	status, err := h.Ledger.Status(r.Context(), req.Node)
	if err != nil {
		h.respond(w, response{Error: ErrUnauthorized.Error()}, http.StatusForbidden)
		return
	}
	caller := Fingerprint(r.TLS.PeerCertificates[0])
	operator := h.OperatorFingerprint != "" && caller == h.OperatorFingerprint
	launcher := caller == status.Enrollment.LauncherFingerprint
	if !h.authorized(req.Action, operator, launcher) {
		h.respond(w, response{Error: ErrUnauthorized.Error()}, http.StatusForbidden)
		return
	}
	result, err := h.execute(r.Context(), req)
	if err != nil {
		h.respond(w, response{Error: protocolError(err).Error()}, http.StatusConflict)
		return
	}
	h.respond(w, result, http.StatusOK)
}

func (BrokerHandler) authorized(action string, operator, launcher bool) bool {
	if operator && launcher {
		return false
	}
	switch action {
	case "status":
		return operator || launcher
	case "finalize":
		return operator
	case "prepare", "consume", "begin", "receipt", "prepare_recovery", "consume_recovery", "complete_recovery", "seal_writer", "recovery_receipt":
		return launcher
	default:
		return false
	}
}
func (h BrokerHandler) execute(ctx context.Context, r request) (response, error) {
	switch r.Action {
	case "status":
		s, err := h.Ledger.Status(ctx, r.Node)
		return response{Status: &s}, err
	case "prepare":
		g, err := h.Ledger.PrepareStartup(ctx, r.Node, r.Nonce)
		return response{Grant: &g}, err
	case "prepare_recovery":
		g, err := h.Ledger.PrepareRecovery(ctx, r.Node, r.Nonce)
		return response{Grant: &g}, err
	case "consume_recovery":
		if r.Grant == nil || r.Grant.Node != r.Node {
			return response{}, ErrInvalid
		}
		permit, err := h.Ledger.ConsumeRecovery(ctx, *r.Grant)
		return response{Permit: &permit}, err
	case "recovery_receipt":
		if r.Grant == nil || r.Receipt == nil || r.Grant.Node != r.Node || r.Receipt.Permit.Binding.Node != r.Node {
			return response{}, ErrInvalid
		}
		return response{}, h.Ledger.RecordRecoveryReceipt(ctx, *r.Grant, *r.Receipt)
	case "complete_recovery":
		if r.Grant == nil || r.Receipt == nil || r.Grant.Node != r.Node || r.Receipt.Permit.Binding.Node != r.Node {
			return response{}, ErrInvalid
		}
		return response{}, h.Ledger.CompleteRecovery(ctx, *r.Grant, *r.Receipt)
	case "consume":
		if r.Grant == nil || r.Grant.Node != r.Node {
			return response{}, ErrInvalid
		}
		lease, err := h.Ledger.ConsumeStartup(ctx, *r.Grant)
		return response{Lease: &lease}, err
	case "begin":
		if r.Lease == nil || r.Binding == nil || r.Binding.Node != r.Node || r.Lease.Node != r.Node {
			return response{}, ErrInvalid
		}
		p, err := h.Ledger.BeginRemoval(ctx, *r.Lease, *r.Binding)
		return response{Permit: &p}, err
	case "receipt":
		if r.Receipt == nil || r.Receipt.Permit.Binding.Node != r.Node {
			return response{}, ErrInvalid
		}
		return response{}, h.Ledger.RecordReceipt(ctx, *r.Receipt)
	case "seal_writer":
		if r.Receipt == nil || r.Receipt.Permit.Binding.Node != r.Node {
			return response{}, ErrInvalid
		}
		return response{}, h.Ledger.SealWriter(ctx, *r.Receipt)
	case "finalize":
		if r.Receipt == nil || r.Receipt.Permit.Binding.Node != r.Node {
			return response{}, ErrInvalid
		}
		s, err := h.Ledger.Finalize(ctx, *r.Receipt)
		return response{Status: &s}, err
	default:
		return response{}, ErrInvalid
	}
}
func (BrokerHandler) respond(w http.ResponseWriter, result response, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// A failed reply does not undo the durable CAS. The client treats a lost
	// reply as unknown and cannot start its next destructive step.
	_ = json.NewEncoder(w).Encode(result)
}

type BrokerClient struct {
	endpoint string
	http     *http.Client
}

func NewBrokerClient(endpoint string, roots *x509.CertPool, cert tls.Certificate) (*BrokerClient, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
		return nil, ErrInvalid
	}
	if roots == nil || len(cert.Certificate) == 0 {
		return nil, ErrInvalid
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}}}
	return &BrokerClient{endpoint: endpoint + "/v1/fence", http: &http.Client{Transport: transport, Timeout: 10 * time.Second}}, nil
}

func (c *BrokerClient) call(ctx context.Context, req request) (response, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return response{}, err
	}
	wire, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return response{}, err
	}
	wire.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(wire)
	if err != nil {
		return response{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var result response
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 32<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return response{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if result.Error != "" {
		for _, known := range []error{ErrFenced, ErrChanged, ErrReceiptRequired, ErrUnauthorized, ErrInvalid, ErrUnsupported, ErrUnavailable} {
			if result.Error == known.Error() {
				return response{}, known
			}
		}
		return response{}, ErrUnavailable
	}
	if resp.StatusCode != http.StatusOK {
		return response{}, ErrUnavailable
	}
	return result, nil
}
func (c *BrokerClient) Status(ctx context.Context, node domain.PeerIdentity) (Status, error) {
	r, err := c.call(ctx, request{Action: "status", Node: node})
	if err != nil {
		return Status{}, err
	}
	if r.Status == nil {
		return Status{}, ErrUnavailable
	}
	return *r.Status, nil
}
func (c *BrokerClient) PrepareStartup(ctx context.Context, node domain.PeerIdentity, nonce string) (StartupGrant, error) {
	r, err := c.call(ctx, request{Action: "prepare", Node: node, Nonce: nonce})
	if err != nil {
		return StartupGrant{}, err
	}
	if r.Grant == nil {
		return StartupGrant{}, ErrUnavailable
	}
	return *r.Grant, nil
}
func (c *BrokerClient) ConsumeStartup(ctx context.Context, g StartupGrant) (Lease, error) {
	r, err := c.call(ctx, request{Action: "consume", Node: g.Node, Grant: &g})
	if err != nil {
		return Lease{}, err
	}
	if r.Lease == nil {
		return Lease{}, ErrUnavailable
	}
	return *r.Lease, nil
}
func (c *BrokerClient) begin(ctx context.Context, lease Lease, b Binding) (RemovalPermit, error) {
	r, err := c.call(ctx, request{Action: "begin", Node: b.Node, Lease: &lease, Binding: &b})
	if err != nil {
		return RemovalPermit{}, err
	}
	if r.Permit == nil {
		return RemovalPermit{}, ErrUnavailable
	}
	return *r.Permit, nil
}
func (c *BrokerClient) RecordReceipt(ctx context.Context, receipt Receipt) error {
	_, err := c.call(ctx, request{Action: "receipt", Node: receipt.Permit.Binding.Node, Receipt: &receipt})
	return err
}
func (c *BrokerClient) RecordRecoveryReceipt(ctx context.Context, grant StartupGrant, receipt Receipt) error {
	_, err := c.call(ctx, request{Action: "recovery_receipt", Node: grant.Node, Grant: &grant, Receipt: &receipt})
	return err
}

func (c *BrokerClient) Finalize(ctx context.Context, receipt Receipt) (Status, error) {
	r, err := c.call(ctx, request{Action: "finalize", Node: receipt.Permit.Binding.Node, Receipt: &receipt})
	if err != nil {
		return Status{}, err
	}
	if r.Status == nil {
		return Status{}, ErrUnavailable
	}
	return *r.Status, nil
}
func (c *BrokerClient) PrepareRecovery(ctx context.Context, node domain.PeerIdentity, nonce string) (StartupGrant, error) {
	r, err := c.call(ctx, request{Action: "prepare_recovery", Node: node, Nonce: nonce})
	if err != nil {
		return StartupGrant{}, err
	}
	if r.Grant == nil {
		return StartupGrant{}, ErrUnavailable
	}
	return *r.Grant, nil
}
func (c *BrokerClient) ConsumeRecovery(ctx context.Context, g StartupGrant) (RemovalPermit, error) {
	r, err := c.call(ctx, request{Action: "consume_recovery", Node: g.Node, Grant: &g})
	if err != nil {
		return RemovalPermit{}, err
	}
	if r.Permit == nil {
		return RemovalPermit{}, ErrUnavailable
	}
	return *r.Permit, nil
}
func (c *BrokerClient) CompleteRecovery(ctx context.Context, g StartupGrant, receipt Receipt) error {
	_, err := c.call(ctx, request{Action: "complete_recovery", Node: g.Node, Grant: &g, Receipt: &receipt})
	return err
}
func (c *BrokerClient) SealWriter(ctx context.Context, receipt Receipt) error {
	_, err := c.call(ctx, request{Action: "seal_writer", Node: receipt.Permit.Binding.Node, Receipt: &receipt})
	return err
}

// TLSConfig requires a chain-verified client certificate on every connection.
func TLSConfig(cert tls.Certificate, clients *x509.CertPool) *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: clients, ClientAuth: tls.RequireAndVerifyClientCert}
}

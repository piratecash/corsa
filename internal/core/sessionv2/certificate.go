package sessionv2

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/identity"
)

// certificate.go owns the TLS server certificate of the v2 listener.
//
// The certificate proves NOTHING: identity is proven by session_proof over
// the exporter, and the traffic keys come from the ephemeral (EC)DHE, so a
// leaked certificate key exposes neither a session nor an identity. It is
// still a secret of this process (docs/protocol/session_v2.md, "Secrets"),
// and it is cached rather than minted per connection because minting costs
// CPU on input nobody has authenticated yet — a lever for denial of service.

// CertificateRotation is how long one certificate is served.
const CertificateRotation = time.Hour

// Clock is the time source the certificate rotation reads.
type Clock func() time.Time

// CertificateSource is the single owner of the listener certificate and its
// key. Only a pointer to the certificate leaves it, through GetCertificate:
// the certificate never sits in a tls.Config field, where printing the
// config would dump the key as decimal bytes and json.Marshal as base64.
//
// The value holds only a pointer, so its value-receiver methods copy no lock.
type CertificateSource struct {
	state *certificateState
}

type certificateState struct {
	clock Clock

	mu      sync.Mutex
	current *tls.Certificate
	expires time.Time
}

// ErrNoClock is a certificate source built without a time source.
var ErrNoClock = errors.New("sessionv2: certificate source needs a clock")

// NewCertificateSource returns a source that mints its first certificate on
// first use.
func NewCertificateSource(clock Clock) (CertificateSource, error) {
	if clock == nil {
		return CertificateSource{}, ErrNoClock
	}
	return CertificateSource{state: &certificateState{clock: clock}}, nil
}

// GetCertificate is the tls.Config.GetCertificate callback.
func (c CertificateSource) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if c.state == nil {
		return nil, ErrNoClock
	}
	return c.state.certificate()
}

func (s *certificateState) certificate() (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if s.current != nil && now.Before(s.expires) {
		return s.current, nil
	}
	minted, err := mintCertificate(now)
	if err != nil {
		return nil, err
	}
	s.current, s.expires = minted, now.Add(CertificateRotation)
	return minted, nil
}

// mintCertificate issues a fresh self-signed Ed25519 certificate. Its
// validity window is wide on purpose: the dialer does not verify it, and a
// clock skew between the two ends must not fail a handshake over a field
// that proves nothing.
func mintCertificate(now time.Time) (*tls.Certificate, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("sessionv2: certificate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, fmt.Errorf("sessionv2: certificate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "corsa"},
		NotBefore:    now.Add(-24 * time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		return nil, fmt.Errorf("sessionv2: certificate: %w", err)
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, nil
}

// MarshalJSON refuses: the source owns a private key. Value receiver, so
// both CertificateSource and *CertificateSource refuse.
func (CertificateSource) MarshalJSON() ([]byte, error) {
	return nil, identity.ErrSecretSerialization
}

// UnmarshalJSON refuses for symmetry: a half-decoded key source is worse
// than none.
func (*CertificateSource) UnmarshalJSON([]byte) error {
	return identity.ErrSecretSerialization
}

// Format redacts the source for every verb, %d included, which skips
// Stringer and would otherwise walk into the key bytes.
func (CertificateSource) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprint(state, "sessionv2.CertificateSource{[redacted]}")
}

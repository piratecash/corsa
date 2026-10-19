package startupfence

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/secretfile"
)

func readProtected(path string) ([]byte, error) {
	if err := secretfile.AuditLocation(path); err != nil {
		return nil, err
	}
	dir, err := secretfile.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	return dir.ReadFile(filepath.Base(path))
}

// LoadTLSMaterial refuses a key readable or writable by other users before
// reading it. Certificate and CA files follow the same conservative policy.
func LoadTLSMaterial(certPath, keyPath, caPath string) (tls.Certificate, *x509.CertPool, error) {
	certRaw, err := readProtected(certPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("certificate location: %w", err)
	}
	keyRaw, err := readProtected(keyPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("private-key location: %w", err)
	}
	defer clear(keyRaw)
	cert, err := tls.X509KeyPair(certRaw, keyRaw)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("TLS key pair: %w", err)
	}
	caRaw, err := readProtected(caPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("CA location: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caRaw) {
		return tls.Certificate{}, nil, fmt.Errorf("CA encoding: %w", ErrInvalid)
	}
	return cert, roots, nil
}

func CertificateFingerprint(path string) (string, error) {
	raw, err := readProtected(path)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", ErrInvalid
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", err
	}
	return Fingerprint(cert), nil
}

func ReadConfiguration(path string) ([]byte, error) { return readProtected(path) }

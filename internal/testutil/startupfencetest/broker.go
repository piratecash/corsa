// Package startupfencetest supplies real mTLS broker fixtures for integration tests.
package startupfencetest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
	"math/big"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func NewBroker(t *testing.T, nodeID domain.PeerIdentity) (*startupfence.Ledger, *startupfence.BrokerClient, startupfence.Enrollment, func()) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "adapter CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caRaw, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caRaw)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(serial int64, server bool) tls.Certificate {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			leaf.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		raw, err := x509.CreateCertificate(rand.Reader, leaf, ca, &k.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err = x509.ParseCertificate(raw)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{raw, caRaw}, PrivateKey: k, Leaf: leaf}
	}
	serverCert, launcherCert := issue(2, true), issue(3, false)
	brokerDir := t.TempDir()
	if err := os.Chmod(brokerDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := startupfence.OpenLedger(context.Background(), filepath.Join(brokerDir, "broker.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	enrollment := startupfence.Enrollment{Node: nodeID, Store: "candidate-store", LauncherFingerprint: startupfence.Fingerprint(launcherCert.Leaf), FaultDomain: "independent-test-broker", LauncherProfile: "linux-systemd-v1"}
	if err := ledger.Enroll(context.Background(), enrollment); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(startupfence.BrokerHandler{Ledger: ledger})
	server.TLS = startupfence.TLSConfig(serverCert, roots)
	server.StartTLS()
	client, err := startupfence.NewBrokerClient(server.URL, roots, launcherCert)
	if err != nil {
		t.Fatal(err)
	}
	return ledger, client, enrollment, func() {
		server.Close()
		if err := ledger.Close(); err != nil {
			t.Error(err)
		}
	}
}

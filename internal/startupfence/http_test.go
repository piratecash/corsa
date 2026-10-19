package startupfence

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/piratecash/corsa/internal/core/identity"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The closure keeps certificate keys out of reflection-based formatting of
// test owners, just like the production callback boundary.
type testPKI func() (*x509.CertPool, tls.Certificate, tls.Certificate, tls.Certificate, tls.Certificate)

func (testPKI) MarshalJSON() ([]byte, error)   { return nil, identity.ErrSecretSerialization }
func (*testPKI) UnmarshalJSON([]byte) error    { return identity.ErrSecretSerialization }
func (testPKI) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("[redacted PKI fixture]")) }

func (p testPKI) roots() *x509.CertPool     { v, _, _, _, _ := p(); return v }
func (p testPKI) server() tls.Certificate   { _, v, _, _, _ := p(); return v }
func (p testPKI) launcher() tls.Certificate { _, _, v, _, _ := p(); return v }
func (p testPKI) operator() tls.Certificate { _, _, _, v, _ := p(); return v }
func (p testPKI) other() tls.Certificate    { _, _, _, _, v := p(); return v }

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(serial int64, name string, server bool) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key, Leaf: leaf}
	}
	server, launcher, operator, other := issue(2, "broker", true), issue(3, "launcher", false), issue(4, "operator", false), issue(5, "stranger", false)
	return func() (*x509.CertPool, tls.Certificate, tls.Certificate, tls.Certificate, tls.Certificate) {
		return roots, server, launcher, operator, other
	}
}

func testBroker(t *testing.T) (*Ledger, *httptest.Server, testPKI, Enrollment) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenLedger(ctx, filepath.Join(dir, "ledger.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ledger.Close(); err != nil {
			t.Error(err)
		}
	})
	pki := newTestPKI(t)
	e := testEnrollment()
	e.LauncherFingerprint = Fingerprint(pki.launcher().Leaf)
	if err := ledger.Enroll(ctx, e); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(BrokerHandler{Ledger: ledger, OperatorFingerprint: Fingerprint(pki.operator().Leaf)})
	server.TLS = TLSConfig(pki.server(), pki.roots())
	server.StartTLS()
	t.Cleanup(server.Close)
	return ledger, server, pki, e
}
func testClient(t *testing.T, server *httptest.Server, pki testPKI, cert tls.Certificate) *BrokerClient {
	t.Helper()
	client, err := NewBrokerClient(server.URL, pki.roots(), cert)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestRealMTLSRolesAndOperatorCAS(t *testing.T) {
	_, server, pki, e := testBroker(t)
	ctx := context.Background()
	launcher := testClient(t, server, pki, pki.launcher())
	operator := testClient(t, server, pki, pki.operator())
	stranger := testClient(t, server, pki, pki.other())
	if _, err := stranger.Status(ctx, e.Node); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("stranger status: %v", err)
	}
	if _, err := operator.PrepareStartup(ctx, e.Node, "boot"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("operator acted as launcher: %v", err)
	}
	grant, err := launcher.PrepareStartup(ctx, e.Node, "boot")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := launcher.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	b := pinBindingBackup(t, Binding{Node: e.Node, Store: e.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: e.Node, PinRevision: 1, PreviousPinnedAt: time.Now()})
	permit, err := launcher.begin(ctx, lease, b)
	if err != nil {
		t.Fatal(err)
	}
	receipt := Receipt{Permit: permit, Kind: ReceiptCleanCommitted, SnapshotDigest: strings.Repeat("a", 64)}
	if err := launcher.RecordReceipt(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Finalize(ctx, receipt); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("launcher unfenced: %v", err)
	}
	if _, err := launcher.PrepareStartup(ctx, e.Node, "restart"); !errors.Is(err, ErrFenced) {
		t.Fatalf("restart: %v", err)
	}
	if _, err := operator.Finalize(ctx, receipt); !errors.Is(err, ErrFenced) {
		t.Fatalf("operator finalized live writer: %v", err)
	}
	if err := launcher.SealWriter(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := operator.Finalize(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.PrepareStartup(ctx, e.Node, "allowed"); err != nil {
		t.Fatal(err)
	}
}

type discardReply struct {
	transport http.RoundTripper
	action    string
}

func (d discardReply) RoundTrip(req *http.Request) (*http.Response, error) {
	var decoded request
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		_ = json.NewDecoder(body).Decode(&decoded)
		_ = body.Close()
	}
	response, err := d.transport.RoundTrip(req)
	if err != nil || decoded.Action != d.action {
		return response, err
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return nil, errors.New("injected lost acknowledgement after server commit")
}

func TestLostBeginAcknowledgementLeavesDurableFenceAndNoPermit(t *testing.T) {
	ledger, server, pki, e := testBroker(t)
	ctx := context.Background()
	client := testClient(t, server, pki, pki.launcher())
	grant, err := client.PrepareStartup(ctx, e.Node, "boot")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = discardReply{transport: client.http.Transport, action: "begin"}
	permit, err := client.begin(ctx, lease, pinBindingBackup(t, Binding{Node: e.Node, Store: e.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: e.Node, PinRevision: 1, PreviousPinnedAt: time.Unix(1, 0).UTC()}))
	if !errors.Is(err, ErrUnavailable) || permit.Operation != "" {
		t.Fatalf("lost ack returned permit %v: %v", permit.Operation, err)
	}
	status, err := ledger.Status(ctx, e.Node)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateFenced {
		t.Fatalf("state = %s", status.State)
	}
	if _, err := ledger.PrepareStartup(ctx, e.Node, "restart"); !errors.Is(err, ErrFenced) {
		t.Fatalf("restart = %v", err)
	}
}

func TestCredentialTokensNeverAppearInFormattedDiagnostics(t *testing.T) {
	secret := CredentialToken(strings.Repeat("c", 64))
	value := StartupGrant{Token: secret}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%d"} {
		rendered := fmt.Sprintf(verb, value)
		if strings.Contains(rendered, string(secret)) {
			t.Fatalf("format %s leaked credential", verb)
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), string(secret)) {
		t.Fatal("authenticated protocol lost credential")
	}
}

func TestCertificateCannotServeBothLauncherAndOperator(t *testing.T) {
	handler := BrokerHandler{}
	for _, action := range []string{"status", "prepare", "consume", "begin", "receipt", "finalize", "seal_writer", "complete_recovery"} {
		if handler.authorized(action, true, true) {
			t.Fatalf("dual-role certificate admitted %s", action)
		}
	}
}

func TestPrivateTestPKICannotSerializeCapturedKeys(t *testing.T) {
	pki := newTestPKI(t)
	if _, err := json.Marshal(pki); !errors.Is(err, identity.ErrSecretSerialization) {
		t.Fatalf("fixture secret serialization: %v", err)
	}
}

func TestRecoveryReceiptRequiresLauncherRoleAndExactActiveGrant(t *testing.T) {
	_, server, pki, e := testBroker(t)
	launcher := testClient(t, server, pki, pki.launcher())
	operator := testClient(t, server, pki, pki.operator())
	stranger := testClient(t, server, pki, pki.other())
	ctx := context.Background()
	grant, err := launcher.PrepareStartup(ctx, e.Node, "boot")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := launcher.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := launcher.begin(ctx, lease, pinBindingBackup(t, Binding{Node: e.Node, Store: e.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: e.Node, PinRevision: 1, PreviousPinnedAt: time.Unix(1, 0).UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := launcher.PrepareRecovery(ctx, e.Node, "recover")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.ConsumeRecovery(ctx, recovery); err != nil {
		t.Fatal(err)
	}
	receipt := Receipt{Permit: permit, Kind: ReceiptProtectionRestored, SnapshotDigest: strings.Repeat("a", 64)}
	for _, client := range []*BrokerClient{operator, stranger} {
		if err := client.RecordRecoveryReceipt(ctx, recovery, receipt); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("non-launcher recorded recovery: %v", err)
		}
	}
	wrong := recovery
	wrong.Token = "stale-grant"
	if err := launcher.RecordRecoveryReceipt(ctx, wrong, receipt); !errors.Is(err, ErrChanged) {
		t.Fatalf("wrong active grant accepted: %v", err)
	}
	if err := launcher.RecordRecoveryReceipt(ctx, recovery, receipt); err != nil {
		t.Fatal(err)
	}
}

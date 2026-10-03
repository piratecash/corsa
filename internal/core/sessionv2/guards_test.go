package sessionv2

import (
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/identity"
)

// guards_test.go pins the secrets and TLS parameters of the session (#20).

func TestTheTLSParametersAreThoseOfTheContract(t *testing.T) {
	certs, err := NewCertificateSource(time.Now)
	if err != nil {
		t.Fatalf("certificates: %v", err)
	}
	for name, config := range map[string]*tls.Config{"listener": listenerConfig(certs), "dialer": dialerConfig()} {
		switch {
		case config.MinVersion != tls.VersionTLS13 || config.MaxVersion != tls.VersionTLS13:
			t.Errorf("%s: versions %x–%x, want TLS 1.3 only", name, config.MinVersion, config.MaxVersion)
		case len(config.NextProtos) != 1 || config.NextProtos[0] != ALPN:
			t.Errorf("%s: ALPN %v", name, config.NextProtos)
		case !config.SessionTicketsDisabled || config.ClientSessionCache != nil:
			t.Errorf("%s: resumption is possible", name)
		case !config.DynamicRecordSizingDisabled:
			t.Errorf("%s: dynamic record sizing undercounts records for rotation", name)
		case config.KeyLogWriter != nil:
			t.Errorf("%s: traffic secrets are logged", name)
		case len(config.Certificates) != 0:
			t.Errorf("%s: a certificate sits in the config, one print away from its key", name)
		}
	}
	if listenerConfig(certs).GetCertificate == nil {
		t.Error("listener: no certificate source")
	}
}

func TestTheCertificateIsCachedAndRotatedHourly(t *testing.T) {
	now := time.Unix(1780000000, 0)
	certs, err := NewCertificateSource(func() time.Time { return now })
	if err != nil {
		t.Fatalf("certificates: %v", err)
	}
	first, _ := certs.GetCertificate(nil)
	again, _ := certs.GetCertificate(nil)
	if first == nil || first != again {
		t.Fatal("the certificate is minted per connection instead of cached")
	}
	now = now.Add(CertificateRotation)
	if rotated, _ := certs.GetCertificate(nil); rotated == first {
		t.Fatal("the certificate was not rotated after an hour")
	}
	if _, err := NewCertificateSource(nil); !errors.Is(err, ErrNoClock) {
		t.Fatalf("source without a clock = %v", err)
	}
}

// The certificate key must not leave the source by JSON or by any verb, %d
// included; the scan covers every encoding a byte slice prints in.
func TestTheCertificateKeyDoesNotLeakThroughSerialisationOrFormatting(t *testing.T) {
	certs, err := NewCertificateSource(time.Now)
	if err != nil {
		t.Fatalf("certificates: %v", err)
	}
	certificate, err := certs.GetCertificate(nil)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	key := []byte(certificate.PrivateKey.(ed25519.PrivateKey))

	if _, err := json.Marshal(certs); !errors.Is(err, identity.ErrSecretSerialization) {
		t.Errorf("json.Marshal(source) = %v", err)
	}
	if _, err := json.Marshal(&certs); !errors.Is(err, identity.ErrSecretSerialization) {
		t.Errorf("json.Marshal(&source) = %v", err)
	}
	var decoded CertificateSource
	if err := json.Unmarshal([]byte(`{}`), &decoded); !errors.Is(err, identity.ErrSecretSerialization) {
		t.Errorf("json.Unmarshal = %v", err)
	}

	type holder struct{ Source CertificateSource }
	encodings := map[string]string{
		"raw":     string(key),
		"base64":  base64.StdEncoding.EncodeToString(key),
		"base64u": base64.RawURLEncoding.EncodeToString(key),
		"hex":     hex.EncodeToString(key),
		"decimal": strings.Trim(fmt.Sprintf("%d", key), "[]"),
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%d", "%x", "%q"} {
		for _, value := range []any{certs, &certs, holder{certs}, &holder{certs}} {
			out := fmt.Sprintf(verb, value)
			for name, encoded := range encodings {
				if strings.Contains(out, encoded) {
					t.Errorf("%s of %T prints the key (%s)", verb, value, name)
				}
			}
		}
	}
}

// Only the owner keeps a certificate or a TLS config in a field, and only
// this package skips certificate verification.
func TestNoOtherTypeHoldsTLSSecretsAndOnlyThisPackageSkipsVerification(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		inThisPackage := filepath.Base(filepath.Dir(path)) == "sessionv2"
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.StructType:
				for _, field := range node.Fields.List {
					owner := inThisPackage && fieldNamed(field, "current")
					if holdsTLSSecret(field.Type) && !owner {
						t.Errorf("%s: a struct field of type %s", fset.Position(field.Pos()), render(field.Type))
					}
				}
			case *ast.KeyValueExpr:
				if ident, ok := node.Key.(*ast.Ident); ok && ident.Name == "InsecureSkipVerify" && !inThisPackage {
					t.Errorf("%s: InsecureSkipVerify outside sessionv2", fset.Position(node.Pos()))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

func holdsTLSSecret(expr ast.Expr) bool {
	rendered := render(expr)
	return strings.Contains(rendered, "tls.Certificate") || strings.Contains(rendered, "tls.Config")
}

func fieldNamed(field *ast.Field, name string) bool {
	for _, ident := range field.Names {
		if ident.Name == name {
			return true
		}
	}
	return false
}

func render(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return "*" + render(e.X)
	case *ast.SelectorExpr:
		return render(e.X) + "." + e.Sel.Name
	case *ast.Ident:
		return e.Name
	case *ast.ArrayType:
		return "[]" + render(e.Elt)
	case *ast.MapType:
		return "map[" + render(e.Key) + "]" + render(e.Value)
	default:
		return ""
	}
}

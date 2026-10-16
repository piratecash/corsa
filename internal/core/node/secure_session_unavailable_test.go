package node

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// secure_session_unavailable_test.go pins that a node never falls back to v1
// silently over pins it cannot enforce: a build with the secure session whose
// v2 state cannot be built refuses to start, and a v1-only build refuses to
// start over a pin store or marker left on disk.

var errInjectedCertificates = errors.New("injected: certificate source unusable")

func sessionsOptionsIn(dir string) secureSessionsOptions {
	return secureSessionsOptions{
		storePath:  filepath.Join(dir, "secure-sessions-64646.json"),
		markerPath: filepath.Join(dir, "identity-64646.json"+pinStoreMarkerSuffix),
		clock:      time.Now,
	}
}

func generatedIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id
}

func modeAlways(mode sessionv2.Mode) func() (sessionv2.Mode, error) {
	return func() (sessionv2.Mode, error) { return mode, nil }
}

// refusalOfSessions is the reason the Service built on sessions refuses to
// start with, which must be a pin store refusal.
func refusalOfSessions(t *testing.T, sessions *secureSessions) domain.PinStoreRefusalReason {
	t.Helper()
	err := (&Service{secureSessions: sessions}).StartupRefusal()
	if !errors.Is(err, domain.ErrPinStoreRefused) {
		t.Fatalf("StartupRefusal = %v, want ErrPinStoreRefused", err)
	}
	var refusal *PinStoreRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("StartupRefusal = %T, want *PinStoreRefusalError", err)
	}
	return refusal.Reason
}

func TestAnUnusableCertificateSourceIsARefusalNotV1(t *testing.T) {
	t.Parallel()
	builders := productionSecureSessionBuilders()
	builders.certificates = func(sessionv2.Clock) (sessionv2.CertificateSource, error) {
		return sessionv2.CertificateSource{}, errInjectedCertificates
	}
	sessions := buildSecureSessions(generatedIdentity(t), sessionsOptionsIn(t.TempDir()), builders)
	if reason := refusalOfSessions(t, sessions); reason != domain.PinStoreRefusalSecureSessionUnavailable {
		t.Fatalf("reason = %s, want %s", reason, domain.PinStoreRefusalSecureSessionUnavailable)
	}
	if err := (&Service{secureSessions: sessions}).StartupRefusal(); !errors.Is(err, errInjectedCertificates) {
		t.Fatalf("the refusal does not carry its cause: %v", err)
	}
}

func TestAnUnreadableSessionModeIsARefusalNotV1(t *testing.T) {
	t.Parallel()
	builders := productionSecureSessionBuilders()
	builders.mode = func() (sessionv2.Mode, error) { return 0, errors.New("injected: contradictory version constants") }
	sessions := buildSecureSessions(generatedIdentity(t), sessionsOptionsIn(t.TempDir()), builders)
	if reason := refusalOfSessions(t, sessions); reason != domain.PinStoreRefusalSecureSessionUnavailable {
		t.Fatalf("reason = %s, want %s", reason, domain.PinStoreRefusalSecureSessionUnavailable)
	}
}

func TestAnIdentityThatCannotProveIsARefusalNotV1(t *testing.T) {
	t.Parallel()
	sessions := buildSecureSessions(nil, sessionsOptionsIn(t.TempDir()), productionSecureSessionBuilders())
	if reason := refusalOfSessions(t, sessions); reason != domain.PinStoreRefusalSecureSessionUnavailable {
		t.Fatalf("reason = %s, want %s", reason, domain.PinStoreRefusalSecureSessionUnavailable)
	}
}

// A v1-only build (a rollback, or version constants below the activation)
// cannot enforce pins; over a store or a marker it refuses to start.
func TestAV1OnlyBuildOverAPinStoreIsARefusal(t *testing.T) {
	t.Parallel()
	for name, file := range map[string]func(secureSessionsOptions) string{
		"store":  func(opts secureSessionsOptions) string { return opts.storePath },
		"marker": func(opts secureSessionsOptions) string { return opts.markerPath },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := sessionsOptionsIn(t.TempDir())
			if err := os.WriteFile(file(opts), []byte(`{}`), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			builders := productionSecureSessionBuilders()
			builders.mode = modeAlways(sessionv2.ModeLegacyOnly)
			sessions := buildSecureSessions(generatedIdentity(t), opts, builders)
			if reason := refusalOfSessions(t, sessions); reason != domain.PinStoreRefusalLegacyOnlyBuild {
				t.Fatalf("reason = %s, want %s", reason, domain.PinStoreRefusalLegacyOnlyBuild)
			}
			if _, err := os.Stat(file(opts)); err != nil {
				t.Fatalf("the refusal touched the %s: %v", name, err)
			}
		})
	}
}

func TestAV1OnlyBuildWithNoPinStoreStarts(t *testing.T) {
	t.Parallel()
	builders := productionSecureSessionBuilders()
	builders.mode = modeAlways(sessionv2.ModeLegacyOnly)
	sessions := buildSecureSessions(generatedIdentity(t), sessionsOptionsIn(t.TempDir()), builders)
	if err := (&Service{secureSessions: sessions}).StartupRefusal(); err != nil {
		t.Fatalf("StartupRefusal = %v, want a v1-only node with nothing on disk to start", err)
	}
	if sessions.mode != sessionv2.ModeLegacyOnly {
		t.Fatalf("mode = %s, want legacy only", sessions.mode)
	}
}

func TestABuildWithTheSecureSessionStartsOnAFreshDirectory(t *testing.T) {
	t.Parallel()
	sessions := buildSecureSessions(generatedIdentity(t), sessionsOptionsIn(t.TempDir()), productionSecureSessionBuilders())
	if err := (&Service{secureSessions: sessions}).StartupRefusal(); err != nil {
		t.Fatalf("StartupRefusal = %v, want a first start", err)
	}
}

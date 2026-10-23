//go:build linux

package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// Private native owner composition only. A separate capless creator UID501
// owns the fixture. This consumer really runs as UID502; no owner or audit
// substitution is used. The ordinary test default has no fixture effects.
func TestServiceRunRefusesRealForeignPinOwnersBeforeListen(t *testing.T) {
	if os.Getenv("CORSA_S6_REAL_OWNER_SERVICE") != "1" {
		t.Skip("requires the separately admitted real UID501/502 owned fixture")
	}
	if os.Geteuid() != 502 || os.Getegid() != 20 || os.Getenv("TMPDIR") != "/tmp" {
		t.Fatal("actual consumer UID502:GID20 and original /tmp required")
	}
	foreignBefore := s6ServiceForeignSnapshot(t)
	// This opt-in executor selects this parent alone. Preserve the original
	// logger's level/context and redirect only its writer before NewService
	// captures it. Keep every bounded raw record for the test's stdout proof.
	originalLogger := log.Logger
	capturedLogs := &s6ServiceOwnerLogCapture{}
	log.Logger = originalLogger.Output(capturedLogs)
	t.Cleanup(func() {
		// Every child has returned from Run and WaitBackground before the
		// parent's cleanup runs. No production logger or level is changed.
		log.Logger = originalLogger
	})
	for _, tc := range []struct {
		name           string
		store          string
		markerIdentity string
		component      secretfile.LocationComponent
		mode           fs.FileMode
	}{
		{"foreign-pin-store-file", "/fixture/foreign/secure-sessions-64646.json", "", secretfile.LocationComponent{Kind: secretfile.ComponentFile}, 0o600},
		{"foreign-marker-file", "", "/fixture/foreign/identity-64646.json", secretfile.LocationComponent{Kind: secretfile.ComponentFile}, 0o600},
		{"foreign-pin-directory", "/fixture/foreign/missing-store.json", "", secretfile.LocationComponent{Kind: secretfile.ComponentDirectory, LevelsAbove: 1}, fs.ModeDir | 0o755},
		{"own-first-start-reaches-listen", "", "", secretfile.LocationComponent{}, 0},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			// MkdirTemp itself requests 0700. Go's t.TempDir numbered child
			// is not used as a substitute for this actual owner-only mode.
			dir, err := os.MkdirTemp("/tmp", "corsa-s6-service-owner-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error("remove own consumer scratch", err)
				}
			})
			own := s6ServiceOwnerStamp(t, dir)
			if own.uid != 502 || own.gid != 20 || own.mode != fs.ModeDir|0o700 {
				t.Fatal("consumer scratch is not actual own0700 directory")
			}
			// Hold a real, test-owned loopback listener through Run. If Run
			// reached its own bind, the original network returns EADDRINUSE.
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal("create own occupied-listener control", err)
			}
			t.Cleanup(func() {
				if err := occupied.Close(); err != nil {
					t.Error("close own occupied listener", err)
				}
			})
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			cfg := deriveTestAdvertisePort(config.Node{
				ListenAddress:          occupied.Addr().String(),
				Type:                   domain.NodeTypeFull,
				IdentityPath:           filepath.Join(dir, "identity.json"),
				TrustStorePath:         filepath.Join(dir, "trust.json"),
				IdentityIntentsPath:    filepath.Join(dir, "identity-intents.json"),
				PeersStatePath:         filepath.Join(dir, "peers.json"),
				SecureSessionStorePath: filepath.Join(dir, "pins.json"),
				ChatLogDir:             dir,
				AllowPrivatePeers:      true,
			})
			if tc.store != "" {
				cfg.SecureSessionStorePath = tc.store
			}
			if tc.markerIdentity != "" {
				cfg.IdentityPath = tc.markerIdentity
			}
			logStart := capturedLogs.position()
			service := NewService(cfg, id, nil)
			startup := service.StartupRefusal()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			runErr := service.Run(ctx)
			cancel()
			// Join before any post-Run fatal assertion or logger restoration.
			admitted := service.goBackground(func() {})
			service.WaitBackground()
			select {
			case <-service.runCtx.Done():
			default:
				t.Fatal("original Run lifecycle context survived return")
			}
			rawLogs, overflow := capturedLogs.since(logStart)
			t.Logf("original_service_logs_json=%s", rawLogs)
			if overflow {
				t.Fatal("original service logs exceeded the test-owned 64KiB bound; retained prefix is not a complete log")
			}
			if admitted {
				t.Fatal("original Run left background admission open")
			}
			if tc.name == "own-first-start-reaches-listen" {
				var bind *net.OpError
				if startup != nil || errors.Is(runErr, domain.ErrPinStoreRefused) || !errors.Is(runErr, syscall.EADDRINUSE) || !errors.As(runErr, &bind) || bind.Op != "listen" {
					t.Fatalf("own positive did not reach original occupied-listener refusal: startup=%v run=%v", startup, runErr)
				}
				s6ServiceQualifyOriginalLogs(t, rawLogs, nil, cfg.SecureSessionStorePath)
			} else {
				wantFile := cfg.SecureSessionStorePath
				if tc.markerIdentity != "" {
					wantFile = cfg.IdentityPath + pinStoreMarkerSuffix
				}
				first := s6ServiceOwnerRefusal(t, startup, wantFile, tc.component, tc.mode)
				returned := s6ServiceOwnerRefusal(t, runErr, wantFile, tc.component, tc.mode)
				if first != returned || runErr != startup || errors.Is(runErr, syscall.EADDRINUSE) {
					t.Fatal("original Run did not return the same startup refusal before listen")
				}
				s6ServiceQualifyOriginalLogs(t, rawLogs, returned, cfg.SecureSessionStorePath)
				// No missing pin-store or marker may be created as a fallback.
				for _, path := range []string{filepath.Join(dir, "pins.json"), filepath.Join(dir, "identity.json") + pinStoreMarkerSuffix, "/fixture/foreign/missing-store.json"} {
					if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("refused startup created or cannot inspect fallback %s: %v", path, err)
					}
				}
				t.Logf("actual_euid=502 StartupRefusal+Run typed=pin_store_refused reason=location_unsafe foreign_owner=501 component=%d level=%d before_listen=true", tc.component.Kind, tc.component.LevelsAbove)
			}
			if s6ServiceForeignSnapshot(t) != foreignBefore {
				t.Fatal("original service changed foreign fixture security metadata")
			}
		}) {
			// Fail including cleanup before another service case is started.
			return
		}
	}
}

func s6ServiceOwnerRefusal(t *testing.T, err error, file string, component secretfile.LocationComponent, mode fs.FileMode) *PinStoreRefusalError {
	t.Helper()
	var refusal *PinStoreRefusalError
	var unsafe *secretfile.UnsafeLocationError
	if !errors.Is(err, domain.ErrPinStoreRefused) || !errors.As(err, &refusal) || refusal.Reason != domain.PinStoreRefusalLocationUnsafe || refusal.File != file || !errors.Is(err, secretfile.ErrUnsafeLocation) || !errors.As(err, &unsafe) {
		t.Fatalf("original startup path did not preserve typed unsafe-location refusal: %v", err)
	}
	if unsafe.Violation != secretfile.ViolationForeignOwner || unsafe.Owner != "uid 501" || unsafe.Component != component || unsafe.Mode != mode {
		t.Fatalf("wrong original foreign-owner attribution: %v", unsafe)
	}
	return refusal
}

type s6ServiceOwnerMetadata struct {
	dev, ino                         uint64
	uid, gid                         uint32
	mode                             fs.FileMode
	nlink                            uint64
	size, mtime, ctimeSec, ctimeNsec int64
}

func s6ServiceOwnerStamp(t *testing.T, path string) s6ServiceOwnerMetadata {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal("actual fixture metadata unavailable", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&fs.ModeSymlink != 0 {
		t.Fatal("fixture metadata is not original nonsymlink POSIX metadata")
	}
	return s6ServiceOwnerMetadata{dev: uint64(st.Dev), ino: st.Ino, uid: st.Uid, gid: st.Gid, mode: info.Mode(), nlink: uint64(st.Nlink), size: info.Size(), mtime: info.ModTime().UnixNano(), ctimeSec: st.Ctim.Sec, ctimeNsec: st.Ctim.Nsec}
}

func s6ServiceForeignSnapshot(t *testing.T) [3]s6ServiceOwnerMetadata {
	t.Helper()
	result := [3]s6ServiceOwnerMetadata{
		s6ServiceOwnerStamp(t, "/fixture/foreign"),
		s6ServiceOwnerStamp(t, "/fixture/foreign/secure-sessions-64646.json"),
		s6ServiceOwnerStamp(t, "/fixture/foreign/identity-64646.json.pin-store-marker.json"),
	}
	for i, st := range result {
		if st.uid != 501 || st.gid != 20 || (i == 0 && st.mode != fs.ModeDir|0o755) || (i != 0 && (st.mode != 0o600 || st.nlink != 1)) {
			t.Fatal("foreign fixture does not have actual producer501 directory0755/files0600")
		}
	}
	if _, err := os.Lstat("/fixture/foreign/missing-store.json"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("missing leaf control already exists or cannot be inspected")
	}
	return result
}

// This writer owns at most64KiB for the whole isolated parent. Once the bound
// is crossed it remembers failure and retains the exact prefix; successful
// Write acknowledgement avoids zerolog writing an alternate error to stderr.
// The caller must fail on overflow and never qualifies a truncated record set.
type s6ServiceOwnerLogCapture struct {
	mu       sync.Mutex
	raw      []byte
	overflow bool
}

func (w *s6ServiceOwnerLogCapture) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	const limit = 64 * 1024
	room := limit - len(w.raw)
	if len(p) > room {
		w.raw = append(w.raw, p[:room]...)
		w.overflow = true
	} else {
		w.raw = append(w.raw, p...)
	}
	return len(p), nil
}

func (w *s6ServiceOwnerLogCapture) position() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.raw)
}

func (w *s6ServiceOwnerLogCapture) since(start int) ([]byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.raw[start:]...), w.overflow
}

func s6ServiceQualifyOriginalLogs(t *testing.T, raw []byte, refused *PinStoreRefusalError, store string) {
	t.Helper()
	var unsafe *secretfile.UnsafeLocationError
	if refused != nil && !errors.As(refused, &unsafe) {
		t.Fatal("log expectation lacks the original typed unsafe-location cause")
	}
	refusals, starts := 0, 0
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var record struct {
			Level     string `json:"level"`
			Message   string `json:"message"`
			Store     string `json:"store"`
			Reason    string `json:"reason"`
			Component string `json:"component"`
			Violation string `json:"violation"`
			Owner     string `json:"owner"`
			Mode      string `json:"mode"`
			Error     string `json:"error"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("original logger did not publish complete JSON records: %v", err)
		}
		switch record.Level {
		case "trace", "debug", "info", "warn", "error":
		default:
			t.Fatalf("unexpected original service log severity %q", record.Level)
		}
		if record.Message == "pin_store_refused" {
			refusals++
			if refused == nil || record.Level != "error" || record.Store != filepath.Base(store) || record.Reason != string(refused.Reason) || record.Component != unsafe.Component.String() || record.Violation != string(unsafe.Violation) || record.Owner != unsafe.Owner || record.Mode != unsafe.Mode.String() || record.Error != unsafe.Error() {
				t.Fatalf("original refusal log did not preserve the actual typed owner verdict: %s", line)
			}
		} else if record.Level == "error" {
			t.Fatalf("unexpected original service error record: %s", line)
		}
		if record.Message == "node_service_starting" {
			starts++
			if record.Level != "info" {
				t.Fatal("original startup record did not retain Info severity")
			}
		}
	}
	if (refused != nil && (refusals != 1 || starts != 0)) || (refused == nil && (refusals != 0 || starts != 1)) {
		t.Fatalf("wrong original log frontier: refusals=%d starts=%d", refusals, starts)
	}
}

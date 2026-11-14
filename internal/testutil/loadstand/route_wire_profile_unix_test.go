//go:build (darwin && cgo) || linux

package loadstand

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRouteWireProfileAnchoredReadAndSeal(t *testing.T) {
	for _, mode := range []string{"original", "same-size mutation", "file symlink", "FIFO", "public permissions", "hard link", "profile replacement", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			envelope, binding, expected := routeWireFixture(t)
			raw, err := envelope.Encode()
			if err != nil {
				t.Fatal(err)
			}
			commitment, err := commitRouteWireWindow(expected, binding.RuntimeBinding().Identity)
			if err != nil {
				t.Fatal(err)
			}
			seal := RouteWireFinalSeal{Census: commitment, Bytes: len(raw), SHA256: sha256.Sum256(raw)}
			base := t.TempDir()
			profileDir := filepath.Join(base, "member-1")
			if err := os.Mkdir(profileDir, 0700); err != nil {
				t.Fatal(err)
			}
			parent, err := os.Open(base)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = parent.Close() })
			var st unix.Stat_t
			if err := unix.Fstatat(int(parent.Fd()), "member-1", &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				t.Fatal(err)
			}
			profile := routeWireProfileIdentity{device: uint64(st.Dev), inode: st.Ino, uid: st.Uid, gid: st.Gid}
			name := filepath.Join(profileDir, "final-"+binding.launch.nonce.String()+".json")
			if err := os.WriteFile(name, raw, 0600); err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			switch mode {
			case "same-size mutation":
				envelope.Endpoints[0].Sent.FullV3.Bytes++
				updated, err := envelope.Encode()
				if err != nil || len(updated) != len(raw) {
					t.Fatal("fixture mutation changed size", err)
				}
				if err := os.WriteFile(name, updated, 0600); err != nil {
					t.Fatal(err)
				}
			case "file symlink":
				target := filepath.Join(base, "copy.json")
				if err := os.Rename(name, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, name); err != nil {
					t.Fatal(err)
				}
			case "FIFO":
				if err := os.Remove(name); err != nil {
					t.Fatal(err)
				}
				if err := unix.Mkfifo(name, 0600); err != nil {
					t.Fatal(err)
				}
			case "public permissions":
				if err := os.Chmod(name, 0644); err != nil {
					t.Fatal(err)
				}
			case "hard link":
				if err := os.Link(name, filepath.Join(base, "alias")); err != nil {
					t.Fatal(err)
				}
			case "profile replacement":
				if err := os.Rename(profileDir, filepath.Join(base, "original")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(profileDir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			got, err := readRouteWireProfile(ctx, parent, profile, binding.launch.nonce, seal)
			if mode != "original" {
				want := ErrUnsupportedArtifact
				if mode == "canceled" {
					want = context.Canceled
				}
				if mode == "same-size mutation" {
					want = ErrFinalReportInvalid
				}
				if !errors.Is(err, want) || got != nil {
					t.Fatalf("invalid report escaped: bytes=%d err=%v", len(got), err)
				}
				return
			}
			prior := NewTransportHighWater(binding.RuntimeBinding())
			result, decodeErr := DecodeSealedHeadlessRouteWireReport(got, binding, prior, expected, seal)
			if err != nil || decodeErr != nil || len(result.Endpoints) != 2 {
				t.Fatalf("original report rejected: read=%v decode=%v", err, decodeErr)
			}
		})
	}
}

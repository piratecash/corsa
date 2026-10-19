//go:build unix

package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

func TestLoadStandReceiptArtifactBoundary(t *testing.T) {
	for _, kind := range []string{"valid", "symlink", "fifo", "oversize", "unreadable", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "receipts.jsonl")
			recipient := domain.PeerIdentityFromWire(strings.Repeat("1", 40))
			raw, err := json.Marshal(loadstand.Receipt{ID: "11111111-1111-4111-8111-111111111111", Recipient: recipient})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			switch kind {
			case "symlink":
				if err := os.Rename(path, path+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("receipts.jsonl.original", path); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := os.WriteFile(path, []byte(strings.Repeat("x", 1025)), 0600); err != nil {
					t.Fatal(err)
				}
			case "unreadable":
				if os.Geteuid() == 0 {
					t.Skip("root bypasses mode-based read protection")
				}
				if err := os.Chmod(path, 0); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				cancel()
			}
			member := &loadStandSDKMember{options: loadstand.ProcessOptions{Config: loadstand.ChildConfig{DataDir: dir}}, receipt: loadstand.StartReceipt{Public: loadstand.PublicIdentity{Identity: recipient}}}
			result := make(chan error, 1)
			go func() {
				rows, err := member.Receipts(ctx, 1)
				if err == nil && len(rows) != 1 {
					err = loadstand.ErrInvalidSample
				}
				result <- err
			}()
			select {
			case err := <-result:
				if kind == "valid" && err != nil {
					t.Fatal(err)
				}
				if kind != "valid" && err == nil {
					t.Fatal("unsupported artifact accepted")
				}
				if kind == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatal("cancelled read did not retain context outcome")
				}
			case <-time.After(200 * time.Millisecond):
				// Keep a failing old implementation from leaking a goroutine on FIFO.
				if kind == "fifo" {
					fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NONBLOCK, 0)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := syscall.Write(fd, []byte(strings.Repeat("x", 2048))); err != nil {
						t.Error(err)
					}
					select {
					case <-result:
					case <-time.After(time.Second):
						t.Error("FIFO did not release after explicit writer")
					}
					if err := syscall.Close(fd); err != nil {
						t.Error(err)
					}
				}
				t.Fatal("unsupported receipt blocked bounded dispatcher")
			}
		})
	}
}

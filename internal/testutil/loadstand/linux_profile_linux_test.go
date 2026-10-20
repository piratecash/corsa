//go:build linux

package loadstand

import (
	"context"
	"os"
	"testing"
)

func TestLinuxProfileZeroAndUnownedValuesRefuse(t *testing.T) {
	var parent AuditedProfileParent
	if _, err := parent.acquire(); err == nil {
		t.Fatal("zero parent imported authority")
	}
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateAuditedProfileParent(context.Background(), "relative", LaunchNonce{1}); err == nil {
		t.Fatal("relative base accepted")
	}
	if os.Geteuid() != 0 {
		if _, err := CreateAuditedProfileParent(context.Background(), "/tmp", LaunchNonce{1}); err == nil {
			t.Fatal("nonroot minted profile parent")
		}
	}
}

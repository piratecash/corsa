//go:build !linux

package startupfence

import (
	"context"
	"errors"
	"testing"
)

func TestUnsupportedPlatformCannotContactBrokerOrMintCapability(t *testing.T) {
	controller := &Controller{}
	if err := controller.Start(context.Background(), ChildCommand{}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported startup: %v", err)
	}
	if _, err := ConnectSupervisor(nil, "", testEnrollment().Node, testEnrollment().Store); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported capability: %v", err)
	}
}

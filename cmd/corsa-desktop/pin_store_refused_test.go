package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestPinStoreRefusalNoticeIncludesTheOperatorInstructions(t *testing.T) {
	refusal := fmt.Errorf("node refuses to start: %w: /fixture/pins.json: remove the extended ACL grant",
		domain.ErrPinStoreRefused)
	var notices []string
	show := func(text string) error {
		notices = append(notices, text)
		return nil
	}
	reportPinStoreRefused(errors.New("other startup error"), show)
	reportPinStoreRefused(refusal, show)
	if len(notices) != 1 || !strings.HasSuffix(notices[0], "\n\n"+refusal.Error()) {
		t.Fatalf("notices = %q, want the complete pin-store refusal once", notices)
	}
}

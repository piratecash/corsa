package main

import (
	"strings"
	"testing"
)

func TestProductionSupervisorIsUnavailableByDefault(t *testing.T) {
	err := run([]string{"--config", "/configuration-must-not-be-read"})
	if err == nil || !strings.Contains(err.Error(), "production enablement unavailable") {
		t.Fatalf("default launch = %v", err)
	}
}

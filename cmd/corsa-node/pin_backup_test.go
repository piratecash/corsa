//go:build linux

package main

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
	"testing"
)

func pinBindingBackup(t testing.TB, b startupfence.Binding) startupfence.Binding {
	t.Helper()
	backup, err := domain.NewIdentityPinBackup(b.Identity, b.PreviousPinnedAt, domain.IdentityPinMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	b.Backup = backup
	return b
}

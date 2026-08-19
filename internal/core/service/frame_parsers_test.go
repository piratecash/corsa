package service

import (
	"context"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func TestSanitizeReplyReferencesReportsAFailedLookup(t *testing.T) {
	// A lookup that FAILED says nothing about the reference. The bool-only
	// form returned false for a cancelled context exactly as it did for a
	// genuine miss, so a valid quote was stripped from the history and the
	// caller was handed the edited messages as a successful read.
	owner := domaintest.ID("owner")
	store := newTestChatlogStore(t, owner)

	peer := domaintest.ID("peer")
	messages := []DirectMessage{{
		ID:        "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
		Sender:    peer,
		Recipient: owner,
		ReplyTo:   domain.MessageID("b2c3d4e5-f6a7-4b8c-9d0e-f1a2b3c4d5e6"),
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sanitizeReplyReferences(ctx, messages, store, owner.String())
	if err == nil {
		t.Fatal("a failed lookup was reported as a successful sanitisation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if messages[0].ReplyTo == "" {
		t.Fatal("the reply reference was cleared on a lookup that never ran")
	}
}

func TestSanitizeReplyReferencesClearsAnAbsentReference(t *testing.T) {
	// The other half: an established absence still clears the reference, which
	// is what stops a peer from injecting a cross-thread quote.
	owner := domaintest.ID("owner")
	store := newTestChatlogStore(t, owner)

	peer := domaintest.ID("peer")
	messages := []DirectMessage{{
		ID:        "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
		Sender:    peer,
		Recipient: owner,
		ReplyTo:   domain.MessageID("b2c3d4e5-f6a7-4b8c-9d0e-f1a2b3c4d5e6"),
	}}

	if err := sanitizeReplyReferences(context.Background(), messages, store, owner.String()); err != nil {
		t.Fatalf("sanitizeReplyReferences: %v", err)
	}
	if messages[0].ReplyTo != "" {
		t.Fatalf("ReplyTo = %q, want it cleared: the quoted message is not in this conversation", messages[0].ReplyTo)
	}
}

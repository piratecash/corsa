package domain

import (
	"encoding/json"
	"testing"
)

func TestDMCommandValid(t *testing.T) {
	t.Parallel()

	// The empty command is valid — it represents a regular DM with no
	// command attached. All three named commands are valid.
	validCommands := []DMCommand{
		"",
		DMCommandFileAnnounce,
		DMCommandMessageDelete,
		DMCommandMessageDeleteAck,
	}
	for _, c := range validCommands {
		if !c.Valid() {
			t.Errorf("DMCommand(%q).Valid() = false, want true", c)
		}
	}

	invalidCommands := []DMCommand{
		"unknown",
		"FILE_ANNOUNCE",
		"chunk_request", // a FileAction string must not pass as a DMCommand
		"file_downloaded",
		"delete",
	}
	for _, c := range invalidCommands {
		if c.Valid() {
			t.Errorf("DMCommand(%q).Valid() = true, want false", c)
		}
	}
}

func TestDMCommandIsControl(t *testing.T) {
	t.Parallel()

	controlCommands := []DMCommand{
		DMCommandMessageDelete,
		DMCommandMessageDeleteAck,
	}
	for _, c := range controlCommands {
		if !c.IsControl() {
			t.Errorf("DMCommand(%q).IsControl() = false, want true", c)
		}
	}

	// Empty command and DMCommandFileAnnounce are data DMs, not control.
	dataCommands := []DMCommand{"", DMCommandFileAnnounce}
	for _, c := range dataCommands {
		if c.IsControl() {
			t.Errorf("DMCommand(%q).IsControl() = true, want false", c)
		}
	}

	// Unknown strings are not control DMs.
	if DMCommand("unknown").IsControl() {
		t.Error("DMCommand(\"unknown\").IsControl() = true, want false")
	}
}

func TestMessageDeleteStatusValid(t *testing.T) {
	t.Parallel()

	validStatuses := []MessageDeleteStatus{
		MessageDeleteStatusDeleted,
		MessageDeleteStatusNotFound,
		MessageDeleteStatusDenied,
		MessageDeleteStatusImmutable,
	}
	for _, s := range validStatuses {
		if !s.Valid() {
			t.Errorf("MessageDeleteStatus(%q).Valid() = false, want true", s)
		}
	}

	invalidStatuses := []MessageDeleteStatus{"", "ok", "DELETED", "missing"}
	for _, s := range invalidStatuses {
		if s.Valid() {
			t.Errorf("MessageDeleteStatus(%q).Valid() = true, want false", s)
		}
	}
}

func TestMessageDeleteStatusIsTerminalSuccess(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status MessageDeleteStatus
		want   bool
	}{
		{MessageDeleteStatusDeleted, true},
		{MessageDeleteStatusNotFound, true},
		{MessageDeleteStatusDenied, false},
		{MessageDeleteStatusImmutable, false},
		{MessageDeleteStatus(""), false},
		{MessageDeleteStatus("unknown"), false},
	}
	for _, tc := range cases {
		got := tc.status.IsTerminalSuccess()
		if got != tc.want {
			t.Errorf("MessageDeleteStatus(%q).IsTerminalSuccess() = %v, want %v",
				tc.status, got, tc.want)
		}
	}
}

// validUUID is a syntactically correct MessageID used by the payload
// tests below. It satisfies the v4 layout constraints checked by
// MessageID.IsValid().
const validUUID = "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"

func TestMessageDeletePayloadValid(t *testing.T) {
	t.Parallel()

	good := MessageDeletePayload{TargetID: validUUID}
	if !good.Valid() {
		t.Errorf("MessageDeletePayload(%q).Valid() = false, want true", good.TargetID)
	}

	bad := []MessageDeletePayload{
		{TargetID: ""},
		{TargetID: "not-a-uuid"},
		{TargetID: "a1b2c3d4-e5f6-1a7b-8c9d-e0f1a2b3c4d5"}, // not v4 (version nibble is 1)
	}
	for _, p := range bad {
		if p.Valid() {
			t.Errorf("MessageDeletePayload(%q).Valid() = true, want false", p.TargetID)
		}
	}
}

func TestMessageDeletePayloadJSONRoundTrip(t *testing.T) {
	t.Parallel()

	payload := MessageDeletePayload{TargetID: validUUID}
	encoded, err := MarshalMessageDeletePayload(payload)
	if err != nil {
		t.Fatalf("MarshalMessageDeletePayload: %v", err)
	}

	var decoded MessageDeletePayload
	if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.TargetID != payload.TargetID {
		t.Errorf("TargetID = %q, want %q", decoded.TargetID, payload.TargetID)
	}
	if !decoded.Valid() {
		t.Errorf("decoded payload not valid: %+v", decoded)
	}
}

func TestMessageDeleteAckPayloadValid(t *testing.T) {
	t.Parallel()

	good := MessageDeleteAckPayload{
		TargetID: validUUID,
		Status:   MessageDeleteStatusDeleted,
	}
	if !good.Valid() {
		t.Errorf("MessageDeleteAckPayload.Valid() = false for %+v", good)
	}

	bad := []MessageDeleteAckPayload{
		{TargetID: "", Status: MessageDeleteStatusDeleted},
		{TargetID: validUUID, Status: ""},
		{TargetID: validUUID, Status: MessageDeleteStatus("unknown")},
		{TargetID: "not-a-uuid", Status: MessageDeleteStatusNotFound},
	}
	for _, p := range bad {
		if p.Valid() {
			t.Errorf("MessageDeleteAckPayload.Valid() = true for %+v, want false", p)
		}
	}
}

func TestMessageDeleteAckPayloadJSONRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []MessageDeleteAckPayload{
		{TargetID: validUUID, Status: MessageDeleteStatusDeleted},
		{TargetID: validUUID, Status: MessageDeleteStatusNotFound},
		{TargetID: validUUID, Status: MessageDeleteStatusDenied},
		{TargetID: validUUID, Status: MessageDeleteStatusImmutable},
	}
	for _, payload := range cases {
		encoded, err := MarshalMessageDeleteAckPayload(payload)
		if err != nil {
			t.Fatalf("MarshalMessageDeleteAckPayload(%+v): %v", payload, err)
		}

		var decoded MessageDeleteAckPayload
		if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
			t.Fatalf("Unmarshal(%+v): %v", payload, err)
		}
		if decoded.TargetID != payload.TargetID {
			t.Errorf("TargetID = %q, want %q", decoded.TargetID, payload.TargetID)
		}
		if decoded.Status != payload.Status {
			t.Errorf("Status = %q, want %q", decoded.Status, payload.Status)
		}
		if !decoded.Valid() {
			t.Errorf("decoded payload not valid: %+v", decoded)
		}
	}
}

// TestOutgoingDMCommandTypeIsDMCommand pins the OutgoingDM.Command field
// type to DMCommand. If a future refactor weakens this back to a plain
// string or to FileAction, this test fails at compile time — the
// assignments below would not type-check.
func TestOutgoingDMCommandTypeIsDMCommand(t *testing.T) {
	t.Parallel()

	dm := OutgoingDM{
		Body:        "hi",
		Command:     DMCommandFileAnnounce,
		CommandData: "{}",
	}
	if dm.Command != DMCommandFileAnnounce {
		t.Errorf("Command = %q, want DMCommandFileAnnounce", dm.Command)
	}

	control := OutgoingDM{
		Command:     DMCommandMessageDelete,
		CommandData: `{"target_id":"` + validUUID + `"}`,
	}
	if !control.Command.IsControl() {
		t.Errorf("control DM Command.IsControl() = false")
	}
}

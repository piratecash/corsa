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

// TestDMCommandValidIncludesConversationDelete pins that the bulk-wipe
// commands (conversation_delete / conversation_delete_ack) join the
// recognised DMCommand set. A regression that drops them from
// DMCommand.Valid would cause the inbound dispatcher to silently drop
// authenticated wipe requests.
func TestDMCommandValidIncludesConversationDelete(t *testing.T) {
	t.Parallel()

	commands := []DMCommand{
		DMCommandConversationDelete,
		DMCommandConversationDeleteAck,
	}
	for _, c := range commands {
		if !c.Valid() {
			t.Errorf("DMCommand(%q).Valid() = false, want true", c)
		}
	}
}

// TestDMCommandIsControlIncludesConversationDelete pins that the bulk-
// wipe commands count as control DMs (and therefore travel on the
// dedicated control wire topic, are not persisted to chatlog, etc.).
func TestDMCommandIsControlIncludesConversationDelete(t *testing.T) {
	t.Parallel()

	commands := []DMCommand{
		DMCommandConversationDelete,
		DMCommandConversationDeleteAck,
	}
	for _, c := range commands {
		if !c.IsControl() {
			t.Errorf("DMCommand(%q).IsControl() = false, want true", c)
		}
	}
}

// TestConversationDeleteStatusValid pins the recognised set of terminal
// statuses for conversation_delete_ack. Empty and unknown strings must
// be rejected so a malformed ack is dropped at the boundary instead of
// being treated as success.
func TestConversationDeleteStatusValid(t *testing.T) {
	t.Parallel()

	valid := []ConversationDeleteStatus{
		ConversationDeleteStatusApplied,
		ConversationDeleteStatusError,
	}
	for _, s := range valid {
		if !s.Valid() {
			t.Errorf("ConversationDeleteStatus(%q).Valid() = false, want true", s)
		}
	}

	invalid := []ConversationDeleteStatus{"", "ok", "APPLIED", "deleted", "denied"}
	for _, s := range invalid {
		if s.Valid() {
			t.Errorf("ConversationDeleteStatus(%q).Valid() = true, want false", s)
		}
	}
}

// TestConversationDeleteStatusIsTerminalSuccess pins that only "applied"
// stops the sender's retry loop. "error" must be transient — treating it
// as success would silently abandon a wipe the recipient could not yet
// run.
func TestConversationDeleteStatusIsTerminalSuccess(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status ConversationDeleteStatus
		want   bool
	}{
		{ConversationDeleteStatusApplied, true},
		{ConversationDeleteStatusError, false},
		{ConversationDeleteStatus(""), false},
		{ConversationDeleteStatus("unknown"), false},
	}
	for _, tc := range cases {
		got := tc.status.IsTerminalSuccess()
		if got != tc.want {
			t.Errorf("ConversationDeleteStatus(%q).IsTerminalSuccess() = %v, want %v",
				tc.status, got, tc.want)
		}
	}
}

// TestConversationDeleteRequestIDValid pins the UUID v4 shape rule
// for the new typed request id: the same predicate as
// MessageID.IsValid() applies. Empty is invalid — every
// conversation_delete must carry a non-empty id so the matching
// predicate in the ack handler always has something to compare
// against.
func TestConversationDeleteRequestIDValid(t *testing.T) {
	t.Parallel()

	good := []ConversationDeleteRequestID{
		ConversationDeleteRequestID(validUUID),
		"a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
	}
	for _, id := range good {
		if !id.IsValid() {
			t.Errorf("ConversationDeleteRequestID.IsValid() = false for %q, want true", id)
		}
	}

	bad := []ConversationDeleteRequestID{
		"",
		"not-a-uuid",
		"a1b2c3d4-e5f6-1a7b-8c9d-e0f1a2b3c4d5", // version 1, not 4
	}
	for _, id := range bad {
		if id.IsValid() {
			t.Errorf("ConversationDeleteRequestID.IsValid() = true for %q, want false", id)
		}
	}
}

// TestConversationDeletePayloadValid pins the well-formedness rule
// for the wipe payload: a syntactically valid request id is required.
// The wire payload is intent-only — no row list to validate.
func TestConversationDeletePayloadValid(t *testing.T) {
	t.Parallel()

	good := ConversationDeletePayload{RequestID: ConversationDeleteRequestID(validUUID)}
	if !good.Valid() {
		t.Errorf("ConversationDeletePayload.Valid() = false for %+v, want true", good)
	}

	bad := []ConversationDeletePayload{
		{RequestID: ""},           // missing id
		{RequestID: "not-a-uuid"}, // malformed id
	}
	for _, p := range bad {
		if p.Valid() {
			t.Errorf("ConversationDeletePayload.Valid() = true for %+v, want false", p)
		}
	}
}

// TestConversationDeleteAckPayloadValid pins the well-formedness
// rules for the ack payload: request id present + valid UUID v4,
// status one of the recognised values, Deleted non-negative (a
// negative count cannot represent rows that were actually removed
// and would corrupt downstream UI state).
func TestConversationDeleteAckPayloadValid(t *testing.T) {
	t.Parallel()

	reqID := ConversationDeleteRequestID(validUUID)
	good := []ConversationDeleteAckPayload{
		{RequestID: reqID, Status: ConversationDeleteStatusApplied, Deleted: 0},
		{RequestID: reqID, Status: ConversationDeleteStatusApplied, Deleted: 7},
		{RequestID: reqID, Status: ConversationDeleteStatusError, Deleted: 0},
	}
	for _, p := range good {
		if !p.Valid() {
			t.Errorf("ConversationDeleteAckPayload.Valid() = false for %+v, want true", p)
		}
	}

	bad := []ConversationDeleteAckPayload{
		{RequestID: "", Status: ConversationDeleteStatusApplied, Deleted: 0},      // missing id
		{RequestID: "bogus", Status: ConversationDeleteStatusApplied, Deleted: 0}, // malformed id
		{RequestID: reqID, Status: "", Deleted: 0},
		{RequestID: reqID, Status: ConversationDeleteStatus("unknown"), Deleted: 0},
		{RequestID: reqID, Status: ConversationDeleteStatusApplied, Deleted: -1},
		{RequestID: reqID, Status: ConversationDeleteStatusError, Deleted: -42},
	}
	for _, p := range bad {
		if p.Valid() {
			t.Errorf("ConversationDeleteAckPayload.Valid() = true for %+v, want false", p)
		}
	}
}

// TestMarshalConversationDeletePayloadRoundTrip pins the JSON shape:
// a payload with a valid RequestID survives Marshal+Unmarshal
// exactly. A regression in the json tag would silently zero
// RequestID on decode and break the late-ack guard.
func TestMarshalConversationDeletePayloadRoundTrip(t *testing.T) {
	t.Parallel()

	payload := ConversationDeletePayload{
		RequestID: ConversationDeleteRequestID(validUUID),
	}
	encoded, err := MarshalConversationDeletePayload(payload)
	if err != nil {
		t.Fatalf("MarshalConversationDeletePayload: %v", err)
	}

	var decoded ConversationDeletePayload
	if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.RequestID != payload.RequestID {
		t.Errorf("RequestID = %q, want %q", decoded.RequestID, payload.RequestID)
	}
	if !decoded.Valid() {
		t.Errorf("decoded payload not valid: %+v", decoded)
	}
}

// TestMarshalConversationDeleteAckPayloadRoundTrip pins the wire
// shape of the ack payload: RequestID, Status and Deleted all
// survive a Marshal+Unmarshal round-trip exactly. A regression in
// the JSON tags (mistyped field name, missing tag) would silently
// zero one of the values on decode.
func TestMarshalConversationDeleteAckPayloadRoundTrip(t *testing.T) {
	t.Parallel()

	reqID := ConversationDeleteRequestID(validUUID)
	cases := []ConversationDeleteAckPayload{
		{RequestID: reqID, Status: ConversationDeleteStatusApplied, Deleted: 0},
		{RequestID: reqID, Status: ConversationDeleteStatusApplied, Deleted: 1},
		{RequestID: reqID, Status: ConversationDeleteStatusApplied, Deleted: 42},
		{RequestID: reqID, Status: ConversationDeleteStatusError, Deleted: 0},
	}
	for _, payload := range cases {
		encoded, err := MarshalConversationDeleteAckPayload(payload)
		if err != nil {
			t.Fatalf("MarshalConversationDeleteAckPayload(%+v): %v", payload, err)
		}

		var decoded ConversationDeleteAckPayload
		if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
			t.Fatalf("Unmarshal(%+v): %v", payload, err)
		}
		if decoded.RequestID != payload.RequestID {
			t.Errorf("RequestID = %q, want %q", decoded.RequestID, payload.RequestID)
		}
		if decoded.Status != payload.Status {
			t.Errorf("Status = %q, want %q", decoded.Status, payload.Status)
		}
		if decoded.Deleted != payload.Deleted {
			t.Errorf("Deleted = %d, want %d", decoded.Deleted, payload.Deleted)
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

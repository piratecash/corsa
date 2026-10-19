package loadstand

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestGlobalReplicaActualSchemaAndMutants(t *testing.T) {
	id := domain.MessageID("11111111-1111-4111-8111-111111111111")
	sender := strings.Repeat("1", 40)
	for _, kind := range []string{"valid", "missing", "foreign-id", "foreign-topic", "foreign-recipient", "null-item", "case-alias", "body-alias", "wrong-policy"} {
		t.Run(kind, func(t *testing.T) {
			item := map[string]interface{}{"id": id, "sender": sender, "recipient": "*", "body": "global body", "flag": "immutable", "created_at": time.Now().UTC().Format(time.RFC3339)}
			wire := map[string]interface{}{"type": "message", "topic": "global", "id": id, "item": item}
			switch kind {
			case "missing":
				wire = map[string]interface{}{"type": "error", "code": "unknown-message-id"}
			case "foreign-id":
				item["id"] = "22222222-2222-4222-8222-222222222222"
			case "foreign-topic":
				wire["topic"] = "dm"
			case "foreign-recipient":
				item["recipient"] = sender
			case "null-item":
				wire["item"] = nil
			case "case-alias":
				wire["Item"] = nil
			case "body-alias":
				item["Body"] = nil
			case "wrong-policy":
				item["ttl_seconds"] = 1
			}
			raw, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := DecodeGlobalReplica(raw, id)
			if kind == "valid" {
				if err != nil || actual.ID != id || actual.Body != "global body" {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("invalid replica accepted")
			}
			if kind == "missing" && !errors.Is(err, ErrGlobalReplicaAbsent) {
				t.Fatal(err)
			}
		})
	}
}
func TestProcessGlobalPublisherAndReplicaRealRPCSchema(t *testing.T) {
	id := domain.MessageID("11111111-1111-4111-8111-111111111111")
	sender := domain.PeerIdentityFromWire(strings.Repeat("1", 40))
	raw, err := json.Marshal(domain.GlobalPublication{Outcome: domain.GlobalStoredLocally, Message: domain.GlobalMessage{ID: id, Topic: domain.GlobalMessageTopic, ClaimedSender: sender, Body: "body", CreatedAt: time.Now().UTC()}})
	if err != nil {
		t.Fatal(err)
	}
	source := NewProcessSource(protocolClient(t, 200, string(raw)))
	if _, err := source.PublishGlobal(t.Context(), "body"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGlobal(t.Context(), "different body"); err == nil {
		t.Fatal("RPC returned foreign body")
	}
}

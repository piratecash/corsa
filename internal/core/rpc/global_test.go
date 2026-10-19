package rpc

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

type globalPublisherProbe struct{ calls int }

func (p *globalPublisherProbe) PublishGlobalMessage(_ context.Context, command domain.OutgoingGlobalMessage) (domain.GlobalPublication, error) {
	p.calls++
	return domain.GlobalPublication{Outcome: domain.GlobalStoredLocally, Message: domain.GlobalMessage{Body: command.Body}}, nil
}
func TestGlobalPublisherBusinessCommandBoundary(t *testing.T) {
	table := NewCommandTable()
	RegisterGlobalPublisherCommand(table, nil)
	if response := table.Execute(CommandRequest{Name: "publishGlobal", Args: map[string]interface{}{"body": "hello"}}); response.ErrorKind != ErrUnavailable {
		t.Fatal("absent provider fell back")
	}
	probe := &globalPublisherProbe{}
	RegisterGlobalPublisherCommand(table, probe)
	for _, args := range []map[string]interface{}{nil, {"body": 1}, {"body": " "}, {"Body": "x"}, {"body": "x", "sender": "foreign"}, {"body": "x", "id": "chosen"}, {"body": "x", "hops": 100}, {"body": "x", "private_key": "secret"}} {
		if response := table.Execute(CommandRequest{Name: "publishGlobal", Args: args}); response.ErrorKind != ErrValidation || response.Error == nil {
			t.Fatal("raw fields accepted")
		}
	}
	if probe.calls != 0 {
		t.Fatal("invalid command reached publisher")
	}
	if response := table.Execute(CommandRequest{Name: "publishGlobal", Args: map[string]interface{}{"body": " hello "}}); response.Error != nil {
		t.Fatal(response.Error)
	}
	if probe.calls != 1 {
		t.Fatal("business publisher not called once")
	}
}

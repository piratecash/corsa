package desktop

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// These older UI fixtures deliberately omit endpoints. Keep that fixture
// identity explicit while production rows and collision tests use their full
// original DirectMessage endpoints.
func fixtureMessageRef(id string) domain.MessageRef {
	return messageRef(service.DirectMessage{ID: id})
}

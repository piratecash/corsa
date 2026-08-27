package deeplink

import (
	"context"
	"fmt"
)

// Handler acts on one member of the family. Implementations live in the
// layer that owns the effect — importing a contact is a UI-visible
// operation, so its handler is wired by the application, not here.
type Handler interface {
	HandleDeepLink(ctx context.Context, link Link) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, link Link) error

func (f HandlerFunc) HandleDeepLink(ctx context.Context, link Link) error { return f(ctx, link) }

// Router classifies a URI and hands it to the handler of its kind. The
// handler table is fixed at construction: a member that can appear in a
// link but has no handler is a build that would accept the URI and then
// do nothing with it, which is worse than saying "unsupported".
type Router struct {
	handlers map[Kind]Handler
}

// NewRouter takes the whole table at once — a Register method would let
// a caller forget a member and only find out when a user opens that
// link.
func NewRouter(handlers map[Kind]Handler) (*Router, error) {
	if len(handlers) == 0 {
		return nil, fmt.Errorf("deep link router: no handlers")
	}
	table := make(map[Kind]Handler, len(handlers))
	for kind, handler := range handlers {
		if !isKindName(kind) {
			return nil, fmt.Errorf("deep link router: %q is not a kind name", kind)
		}
		if handler == nil {
			return nil, fmt.Errorf("deep link router: handler for %q is nil", kind)
		}
		table[kind] = handler
	}
	return &Router{handlers: table}, nil
}

// Handle classifies raw and runs its handler. The classified link is
// returned even when the handler fails, so the caller can name the kind
// in a log line or a status message.
func (r *Router) Handle(ctx context.Context, raw string) (Link, error) {
	link, err := Classify(raw)
	if err != nil {
		return Link{}, err
	}
	handler, ok := r.handlers[link.Kind]
	if !ok {
		return link, fmt.Errorf("%w: %s", ErrUnsupportedKind, link.Kind)
	}
	if err := handler.HandleDeepLink(ctx, link); err != nil {
		return link, err
	}
	return link, nil
}

// Kinds reports the members this build handles. For logs and the
// "unsupported link" path, which is otherwise silent about what the
// build DOES know.
func (r *Router) Kinds() []Kind {
	kinds := make([]Kind, 0, len(r.handlers))
	for kind := range r.handlers {
		kinds = append(kinds, kind)
	}
	return kinds
}

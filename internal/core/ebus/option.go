package ebus

type subOptions struct {
	sync bool
}

// SubOption configures a subscription.
type SubOption func(*subOptions)

// WithSync makes the handler run synchronously in the publisher's goroutine
// instead of launching a new goroutine. Use for lightweight, non-blocking
// handlers where ordering matters.
func WithSync() SubOption {
	return func(o *subOptions) {
		o.sync = true
	}
}

//go:build !s6_first_contact_probe

package node

import (
	"context"
	"net"

	"github.com/piratecash/corsa/internal/core/domain"
)

type firstContactAttempt struct{}
type firstContactSocketTiming struct{}

func beginFirstContactAttempt(ctx context.Context) context.Context             { return ctx }
func firstContactFromContext(context.Context) firstContactAttempt              { return firstContactAttempt{} }
func recordFirstContactSetup(context.Context, domain.ConnID, error)            {}
func (firstContactAttempt) recordPublication(domain.ConnID, string, int, bool) {}
func (firstContactSocketTiming) record(domain.ConnID, string)                  {}

func openFirstContactSocket(ctx context.Context, dial rawDialer) (net.Conn, firstContactSocketTiming, error) {
	conn, err := dial(ctx)
	return conn, firstContactSocketTiming{}, err
}

//go:build !s6_first_contact_probe

package node

import "context"

func probeLegacyOnTransport(ctx context.Context, transport *dialledTransport, opts legacyProbeOptions) (legacyProbeAnswer, error) {
	return probeLegacy(ctx, transport.metered, opts)
}

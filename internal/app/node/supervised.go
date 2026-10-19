package node

import (
	"context"
	"os"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
)

// PrepareSupervisedStartup precedes App.Run, including pprof and RPC. A recovery
// child returns offline=true and exits; recording protection grants no network
// startup. The caller owns the final Close after every App/Service teardown.
func (a *App) PrepareSupervisedStartup(ctx context.Context, file *os.File, launcher string, store startupfence.StoreID) (*startupfence.SupervisedSession, bool, error) {
	if err := ctx.Err(); err != nil {
		if file != nil {
			_ = file.Close()
		}
		return nil, false, err
	}
	session, err := startupfence.ConnectSupervisor(file, launcher, domain.PeerIdentityFromWire(a.service.Address()), store)
	if err != nil {
		if file != nil {
			_ = file.Close()
		}
		return nil, false, err
	}
	if _, offline := session.RecoveryPermit(); offline {
		return session, true, a.service.RecoverSupervisedIdentityPinProtection(ctx, session)
	}
	return session, false, a.service.AdmitSupervisedIdentityPinStartup(session)
}

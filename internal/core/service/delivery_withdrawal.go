package service

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// delivery_withdrawal.go retries the withdrawals a deletion could not
// complete.
//
// Deleting a message ends with the node giving up whatever it still held
// for it: the backlog envelope, the retry entry, the queued frames. Until
// that happens the PAYLOAD of a deleted message is still in this process,
// held back only by the freeze the deletion took — and the row is gone, so
// no later deletion can name it again. A single failed RPC would otherwise
// leave it there for the life of the process.
//
// The withdrawal is idempotent (a pass that finds nothing removes
// nothing), so retrying costs nothing but the call. It is registered here
// rather than retried by a goroutine per deletion because the sweep that
// drives it already exists, and because a bounded chain of attempts that
// simply gives up is the failure this exists to remove.

// withdrawalBacklog is the set of withdrawals still owed, keyed by peer.
type withdrawalBacklog struct {
	mu    sync.Mutex
	owed  map[domain.PeerIdentity]map[domain.MessageID]struct{}
	noted time.Time
}

func newWithdrawalBacklog() *withdrawalBacklog {
	return &withdrawalBacklog{owed: make(map[domain.PeerIdentity]map[domain.MessageID]struct{})}
}

func (b *withdrawalBacklog) note(peer domain.PeerIdentity, ids []domain.MessageID, now time.Time) {
	if len(ids) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	owed, ok := b.owed[peer]
	if !ok {
		owed = make(map[domain.MessageID]struct{}, len(ids))
		b.owed[peer] = owed
	}
	for _, id := range ids {
		owed[id] = struct{}{}
	}
	b.noted = now
}

// take removes and returns everything owed, so a pass that fails can
// re-note exactly what it did not manage.
func (b *withdrawalBacklog) take() map[domain.PeerIdentity][]domain.MessageID {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.owed) == 0 {
		return nil
	}
	taken := make(map[domain.PeerIdentity][]domain.MessageID, len(b.owed))
	for peer, ids := range b.owed {
		list := make([]domain.MessageID, 0, len(ids))
		for id := range ids {
			list = append(list, id)
		}
		taken[peer] = list
	}
	clear(b.owed)
	return taken
}

func (b *withdrawalBacklog) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for _, ids := range b.owed {
		total += len(ids)
	}
	return total
}

// withdrawDeletedDeliveries takes back what the node still holds for
// messages that no longer exist here, and remembers what it could not.
//
// A failure does NOT thaw: the rows are gone, so these messages are not
// the user's any more, and releasing the freeze would hand the peer a
// conversation that was just deleted. Frozen-and-owed is the safe state,
// and the sweep below is what ends it.
func (r *DMRouter) withdrawDeletedDeliveries(ctx context.Context, peer domain.PeerIdentity, ids []domain.MessageID) error {
	if len(ids) == 0 {
		return nil
	}
	callCtx, cancel := context.WithTimeout(r.detachedCtx(ctx), conversationCompensationBudget)
	defer cancel()

	if _, err := r.client.CancelConversationDelivery(callCtx, peer, ids); err != nil {
		r.withdrawals.note(peer, ids, time.Now().UTC())
		log.Warn().Err(err).
			Str("peer", peer.String()).
			Int("messages", len(ids)).
			Msg("dm_router: withdrawing the deliveries of deleted messages failed; owed until the next sweep")
		return err
	}
	return nil
}

// retryOwedWithdrawals is one sweep over what earlier deletions could not
// withdraw. Driven by the delete-retry tick, which already runs for the
// same reason: something the peer or the node owes us is outstanding.
func (r *DMRouter) retryOwedWithdrawals(ctx context.Context) {
	owed := r.withdrawals.take()
	for peer, ids := range owed {
		// note() on failure puts them straight back, so a peer whose node
		// path stays broken is retried at the sweep's cadence rather than
		// forgotten.
		if err := r.withdrawDeletedDeliveries(ctx, peer, ids); err == nil {
			log.Info().
				Str("peer", peer.String()).
				Int("messages", len(ids)).
				Msg("dm_router: the deliveries of deleted messages were withdrawn on a retry")
		}
	}
}

// detachedCtx keeps a context's values but drops BOTH its deadline and its
// cancellation (context.WithoutCancel: the result's Done() is nil and
// Err() is always nil).
//
// Only compensating work may use it, and only because that work exists to
// run precisely when the operation it compensates for was cancelled or
// timed out. Everything on the ordinary path takes the caller's context as
// its parent so cancellation is not lost between layers.
func (r *DMRouter) detachedCtx(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// first_hop_guard_store.go is the durable half of the guard set.
//
// The set MUST survive a restart, and that is not a convenience: a node that
// re-sampled its first hops on every start would rotate them once per launch,
// which is the failure first_hop_guards.go exists to avoid. Tor persists its
// guards for the same reason and treats the state file as part of the security
// property rather than as a cache.
//
// # Why this is a row in the peer state and not a file of its own
//
// The first cut gave it `first-hop-guards-<port>.json`, and that was wrong for
// a reason bigger than tidiness. Every node-local JSON store in this codebase
// already has a document describing its retirement into SQLite —
// trust, peers, identity intents, file transfers, identity — so a NEW file is a
// new entry in a list somebody is actively trying to empty, plus a sixth
// migration to write.
//
// Putting the rows in the peer state avoids all of it. They belong there on
// merits, not only for convenience: this is durable per-NEIGHBOUR state, which
// is exactly what that file holds, and it already carries two other such
// arrays (banned IPs, remotely banned IPs) beside the peers themselves.
// Sharing the file also settles ownership questions the separate store had to
// answer badly — one writer, one version number, one fsync path, and the
// guard set inherits whatever binding to the node the peer state gets, rather
// than inventing its own.
//
// # One writer
//
// Nothing here writes to disk. The set marks the peer state dirty and
// flushPeerState — the single writer of that file — collects the rows on its
// next pass. A second writer to one path is how two halves of a file start
// clobbering each other.

// firstHopGuardRow is the on-disk shape of one entry. Identities are stored as
// their wire strings so the file stays readable by a person debugging it.
type firstHopGuardRow struct {
	SampledAt    time.Time `json:"sampled_at"`
	ConfirmedAt  time.Time `json:"confirmed_at,omitempty"`
	RetryAt      time.Time `json:"retry_at,omitempty"`
	Identity     string    `json:"identity"`
	ConfirmedSeq uint64    `json:"confirmed_seq,omitempty"`
	Failures     int       `json:"failures,omitempty"`
	Inbound      bool      `json:"inbound,omitempty"`
}

// firstHopGuardRows converts the live set into its stored form.
func firstHopGuardRows(entries []guardEntry) []firstHopGuardRow {
	if len(entries) == 0 {
		return nil
	}
	rows := make([]firstHopGuardRow, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, firstHopGuardRow{
			Identity:     entry.Identity.String(),
			SampledAt:    entry.SampledAt,
			ConfirmedAt:  entry.ConfirmedAt,
			ConfirmedSeq: entry.ConfirmedSeq,
			RetryAt:      entry.RetryAt,
			Failures:     entry.Failures,
			Inbound:      entry.Inbound,
		})
	}
	return rows
}

// firstHopGuardsFromRows restores the set. Order is preserved because it IS
// state: the sampled order is the admission preference (inbound first), and a
// stable sort over it is what keeps that preference alive in the hot set.
//
// An unparsable identity is skipped rather than failing the load: losing one
// guard costs one re-sampling, while refusing to start costs the node.
func firstHopGuardsFromRows(rows []firstHopGuardRow, storedOwner string, self domain.PeerIdentity) []guardEntry {
	if len(rows) == 0 {
		return nil
	}
	// The set belongs to an identity, not to a port. `identity_restore` swaps
	// the key and restarts on the same listen address, so a set restored
	// without this check would put the NEW identity in front of exactly the
	// neighbours the old one used — which is the correlation the guard model
	// exists to bound, handed over for free.
	//
	// An empty stored owner is a file written before the field existed. It is
	// treated as a mismatch rather than as consent: the whole point is that we
	// cannot tell whose set it is, and re-sampling costs one round of probes.
	if storedOwner == "" || self.IsZero() || storedOwner != self.String() {
		log.Warn().
			Str("stored_owner", storedOwner).
			Int("rows", len(rows)).
			Msg("first_hop_guards_discarded_owner_mismatch")
		return nil
	}
	entries := make([]guardEntry, 0, len(rows))
	for _, row := range rows {
		identity, err := domain.ParsePeerIdentity(row.Identity)
		if err != nil || identity.IsZero() {
			continue
		}
		entries = append(entries, guardEntry{
			Identity:     identity,
			SampledAt:    row.SampledAt,
			ConfirmedAt:  row.ConfirmedAt,
			ConfirmedSeq: row.ConfirmedSeq,
			RetryAt:      row.RetryAt,
			Failures:     row.Failures,
			Inbound:      row.Inbound,
		})
	}
	return entries
}

// peerStateGuardPersister is the production persister: it does not write, it
// asks the peer state's single writer to. See the file header.
type peerStateGuardPersister struct{ svc *Service }

// Persist marks the peer state dirty. The rows themselves are read back out of
// the set by flushPeerState, so nothing is copied here and the two cannot
// disagree about what the current set is.
func (p peerStateGuardPersister) Persist([]guardEntry) error {
	if p.svc == nil {
		return nil
	}
	p.svc.markPeerStateDirty()
	return nil
}

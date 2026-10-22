package findnode

import "github.com/piratecash/corsa/internal/core/domain"

type ownedLocalEntryInstall struct{ original, staged *targetEntry }
type ownedLocalAdmissionStage struct {
	cache   *TargetCache
	rows    [MaxTargetEntries]ownedLocalEntryInstall
	count   int
	entries map[domain.FindNodeChallenge]*targetEntry
	result  TargetResult
	changes targetChanges
}

// Called with the original owner/cache/calendar/source locks, BEFORE R.
// Neither the staged rows nor any mutable map/coordinated record alias live
// state. Original Session values and immutable alias slot pointers are data.
func stageOwnedLocalAdmissionLocked(o *TargetCoordinator, id ownedReceivedIdentity, priorities PrioritySnapshot, deadline Instant) (*ownedLocalAdmissionStage, error) {
	live := o.cache
	if len(live.entries) > MaxTargetEntries {
		return nil, ErrCapacity
	}
	staged := &TargetCache{self: live.self, last: live.last, counters: live.counters, entries: make(map[domain.FindNodeChallenge]*targetEntry, len(live.entries)+1), sessions: make(map[Session]int, len(live.sessions)+1)}
	plan := &ownedLocalAdmissionStage{cache: staged}
	originals := make(map[*targetEntry]*targetEntry, len(live.entries))
	for challenge, entry := range live.entries {
		copied := *entry
		copied.recipients = make(map[Session]domain.FindNodeRequestID, len(entry.recipients)+1)
		for source, id := range entry.recipients {
			copied.recipients[source] = id
		}
		if entry.coordinated != nil {
			state := *entry.coordinated
			copied.coordinated = &state
		}
		staged.entries[challenge] = &copied
		plan.rows[plan.count] = ownedLocalEntryInstall{original: entry, staged: &copied}
		plan.count++
		originals[&copied] = entry
	}
	for source, count := range live.sessions {
		staged.sessions[source] = count
	}
	result, err := staged.admitLocked(id.source, id.request, priorities.at, deadline, priorities, &plan.changes)
	if err != nil {
		return nil, err
	}
	// A protected duplicate joins the SAME job promise. Remember the exact live
	// recipient on this bounded original row even after a later class redraw.
	if result.Kind == TargetWaiting && priorities.priorities[id.source] == PriorityProtected {
		entry := staged.entries[id.request.Challenge]
		if entry == nil || entry.coordinated == nil {
			return nil, ErrStaleWork
		}
		found := false
		for _, original := range entry.coordinated.protected {
			found = found || original == id.source
		}
		if !found {
			for index, original := range entry.coordinated.protected {
				if original == (Session{}) {
					entry.coordinated.protected[index] = id.source
					found = true
					break
				}
			}
		}
		if !found {
			return nil, ErrCapacity
		}
	}
	// Build the final map before R, retaining every original generation pointer.
	plan.entries = make(map[domain.FindNodeChallenge]*targetEntry, len(staged.entries))
	for challenge, entry := range staged.entries {
		original := originals[entry]
		if original == nil {
			original = entry
		}
		plan.entries[challenge] = original
	}
	if result.Kind == TargetNeedsSignature {
		result.Work.entry.received = Instant(id.received.Elapsed())
		result.Work.cache = live
	}
	plan.result = result
	return plan, nil
}

// Requires all original locks continuously from staging through this commit.
// It performs no lock acquisition, callback, allocation, cleanup or fresh sample.
func (plan *ownedLocalAdmissionStage) install(live *TargetCache, f Instant) {
	for index := 0; index < plan.count; index++ {
		row := plan.rows[index]
		// Unchanged coordinated records keep their exact existing address too.
		if row.original.coordinated != nil && row.staged.coordinated != nil && *row.original.coordinated == *row.staged.coordinated {
			row.staged.coordinated = row.original.coordinated
		}
		// challenge is constructor-immutable: original work validates it before
		// taking the owner mutex. Never rewrite that key, even with equal bytes.
		// Every mutable field still commits under the same original locks/R.
		row.original.ownerGroup = row.staged.ownerGroup
		row.original.history = row.staged.history
		row.original.historyCount = row.staged.historyCount
		row.original.recipients = row.staged.recipients
		row.original.received = row.staged.received
		row.original.expires = row.staged.expires
		row.original.phase = row.staged.phase
		row.original.body = row.staged.body
		row.original.coordinated = row.staged.coordinated
	}
	live.entries = plan.entries
	live.sessions = plan.cache.sessions
	live.counters = plan.cache.counters
	live.last = f
}

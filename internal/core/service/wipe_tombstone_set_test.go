package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// stubDeleteTaskList counts loads and can fail them, so a test can watch what
// the inbound path pays when the database is wedged rather than late.
type stubDeleteTaskList struct {
	mu    sync.Mutex
	loads int
	fail  bool
	owed  []domain.MessageID
}

func (j *stubDeleteTaskList) OwedDeleteIntentMessageIDs(context.Context) ([]domain.MessageID, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.loads++
	if j.fail {
		return nil, errors.New("database is locked")
	}
	return j.owed, nil
}

func (j *stubDeleteTaskList) loadCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.loads
}

func (j *stubDeleteTaskList) recover(owed []domain.MessageID) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.fail = false
	j.owed = owed
}

// TestFallbackLoadIsThrottledPerMessage: Has runs on the inbound path, and
// a wedged database answers each attempt only after busy_timeout. One load
// per arriving message would put the disk's health on the critical path of
// receiving.
func TestFallbackLoadIsThrottledPerMessage(t *testing.T) {
	t.Parallel()

	tasks := &stubDeleteTaskList{fail: true}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })

	now := time.Now().UTC()
	set.Hydrate(context.Background(), now)
	if tasks.loadCount() != 1 {
		t.Fatalf("startup loads = %d, want 1", tasks.loadCount())
	}

	// A burst of arrivals inside one throttle window costs one load.
	for range 20 {
		set.Refuses(domain.MessageID("11111111-1111-4111-8111-111111111111"), now)
	}
	if got := tasks.loadCount(); got != 2 {
		t.Errorf("loads during a burst = %d, want 2 (startup + one retry)", got)
	}

	// Past the floor, one more attempt — and only one.
	later := now.Add(wipeTombstoneReloadFloor + time.Second)
	for range 20 {
		set.Refuses(domain.MessageID("11111111-1111-4111-8111-111111111111"), later)
	}
	if got := tasks.loadCount(); got != 3 {
		t.Errorf("loads after the floor = %d, want 3", got)
	}
}

// TestFallbackStopsOnceTheLoadSucceeds: the throttle must not delay the
// recovery beyond its own window — once a retry loads the set, Has is a
// pure memory lookup again and the refusals are back.
func TestFallbackStopsOnceTheLoadSucceeds(t *testing.T) {
	t.Parallel()

	const refused = domain.MessageID("22222222-2222-4222-8222-222222222222")
	tasks := &stubDeleteTaskList{fail: true}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })

	now := time.Now().UTC()
	set.Hydrate(context.Background(), now)
	if got, known := set.Refuses(refused, now); got || known {
		t.Fatalf("a failed load answered refused=%v known=%v; it can know neither", got, known)
	}

	tasks.recover([]domain.MessageID{refused})
	later := now.Add(wipeTombstoneReloadFloor + time.Second)
	if got, known := set.Refuses(refused, later); !got || !known {
		t.Fatalf("the retry did not pick the refusals back up: refused=%v known=%v", got, known)
	}

	before := tasks.loadCount()
	for range 20 {
		set.Refuses(refused, later.Add(2*wipeTombstoneReloadFloor))
	}
	if got := tasks.loadCount(); got != before {
		t.Errorf("the fallback kept loading after a successful one: %d → %d", before, got)
	}
}

// TestUnknownRefusalDefersTheMessage: the throttle bounds what the inbound
// path pays for a broken database — it must not become permission to
// re-create a row the user deleted. A store that cannot know refuses to
// decide, and the sender keeps the message and re-sends it.
func TestUnknownRefusalDefersTheMessage(t *testing.T) {
	t.Parallel()

	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	owner := domain.PeerIdentityFromWire(self.Address)
	tasks := &stubDeleteTaskList{fail: true}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })
	set.Hydrate(context.Background(), time.Now().UTC())

	adapter := NewMessageStoreAdapter(
		NewChatlogGateway(newTestChatlogStore(t, owner), owner), self, set, nil)

	envelope := protocol.Envelope{
		ID:        "33333333-3333-4333-8333-333333333333",
		Topic:     "dm",
		Sender:    domain.PeerIdentityFromWire("4444444444444444444444444444444444444444").String(),
		Recipient: self.Address,
		Payload:   []byte("sealed"),
		CreatedAt: time.Now().UTC(),
	}
	if got := adapter.StoreMessage(envelope, false); got != node.StoreDeferred {
		t.Fatalf("StoreMessage = %v, want %v: an undecidable refusal must not be read as permission", got, node.StoreDeferred)
	}
	if _, found, err := adapter.chatlog.Store().EntryByID(context.Background(), domain.MessageID(envelope.ID)); err != nil || found {
		t.Fatalf("the message was stored anyway (found=%v err=%v)", found, err)
	}

	// Once the refusals load, the same message is decidable again.
	tasks.recover(nil)
	later := time.Now().UTC().Add(wipeTombstoneReloadFloor + time.Second)
	if refused, known := set.Refuses(domain.MessageID(envelope.ID), later); refused || !known {
		t.Fatalf("after recovery: refused=%v known=%v, want false/true", refused, known)
	}
	if got := adapter.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("StoreMessage after recovery = %v, want %v", got, node.StoreInserted)
	}
}

// TestThrottledWindowIsUndecidableNotAllowed pins the exact case the
// throttle could otherwise create: inside the window nothing is read from
// the database, so a memory miss there is not evidence of anything. If it
// answered "not refused", a re-delivery arriving in that window would be
// written back into the chatlog and no later reload would remove it again.
func TestThrottledWindowIsUndecidableNotAllowed(t *testing.T) {
	t.Parallel()

	tasks := &stubDeleteTaskList{fail: true}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })
	now := time.Now().UTC()
	set.Hydrate(context.Background(), now)

	const id = domain.MessageID("55555555-5555-4555-8555-555555555555")
	// The first miss spends the one allowed reload.
	set.Refuses(id, now)
	before := tasks.loadCount()

	refused, known := set.Refuses(id, now)
	if tasks.loadCount() != before {
		t.Fatal("the second call inside the window read the database after all")
	}
	if refused || known {
		t.Errorf("inside the throttle window: refused=%v known=%v, want false/false", refused, known)
	}
}

// TestNonDMTopicIsNotGatedByRefusals: every refusal names a chat row, and
// chat rows are DM. For anything else the gate can only say "not refused"
// or "cannot tell", and the second answer would defer traffic that has no
// sender-side retry to fall back on — a loss, which is what the deferral
// exists to prevent. The topic comes from the wire, so this is also what
// stops a peer making our reception depend on a table their messages have
// nothing to do with.
func TestNonDMTopicIsNotGatedByRefusals(t *testing.T) {
	t.Parallel()

	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	owner := domain.PeerIdentityFromWire(self.Address)
	tasks := &stubDeleteTaskList{fail: true}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })
	set.Hydrate(context.Background(), time.Now().UTC())

	adapter := NewMessageStoreAdapter(
		NewChatlogGateway(newTestChatlogStore(t, owner), owner), self, set, nil)

	envelope := protocol.Envelope{
		ID:        "66666666-6666-4666-8666-666666666666",
		Topic:     "global",
		Sender:    domain.PeerIdentityFromWire("4444444444444444444444444444444444444444").String(),
		Recipient: "*",
		Payload:   []byte("sealed"),
		CreatedAt: time.Now().UTC(),
	}
	if got := adapter.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v, want %v: an unreadable DM refusal set must not hold up other topics", got, node.StoreInserted)
	}

	// The same unreadable set still defers a DM.
	envelope.Topic = "dm"
	envelope.Recipient = self.Address
	envelope.ID = "77777777-7777-4777-8777-777777777777"
	if got := adapter.StoreMessage(envelope, false); got != node.StoreDeferred {
		t.Fatalf("StoreMessage(dm) = %v, want %v", got, node.StoreDeferred)
	}
}

// TestSendRejectionKeepsTheNodesReason: every refusal of a send used to
// collapse into "unexpected send reply: error", which threw away the one
// bit that decides what to tell the user — whether it is transient. The
// store-deferred case is exactly that: the node declined to decide, and
// "try again in a moment" is a different message from "this failed".
func TestSendRejectionKeepsTheNodesReason(t *testing.T) {
	t.Parallel()

	deferred := sendRejection(protocol.Frame{
		Type:  "error",
		Code:  protocol.ErrCodeStoreDeferred,
		Error: "the deletions still owed cannot be read",
	})
	if !errors.Is(deferred, protocol.ErrStoreDeferred) {
		t.Fatalf("errors.Is lost the reason: %v", deferred)
	}
	if !strings.Contains(deferred.Error(), "cannot be read") {
		t.Errorf("the node's detail was dropped: %v", deferred)
	}

	// An unrelated refusal keeps its own sentinel, not a generic one.
	other := sendRejection(protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownSenderKey})
	if !errors.Is(other, protocol.ErrUnknownSenderKey) {
		t.Errorf("a non-deferred refusal lost its code: %v", other)
	}
	if errors.Is(other, protocol.ErrStoreDeferred) {
		t.Error("an unrelated refusal reads as transient")
	}

	// A reply that is not an error frame at all keeps the old wording.
	unexpected := sendRejection(protocol.Frame{Type: "message_unknown"})
	if !strings.Contains(unexpected.Error(), "unexpected send reply") {
		t.Errorf("unexpected reply wording changed: %v", unexpected)
	}
}

// refusalSizes reports what the set holds: entries, the positions still ahead
// of the queue's head, and the slots the queue slice has ever needed.
//
// The last one is the CAPACITY on purpose. Reclaiming reuses the backing array,
// so its capacity is the high-water mark of the queue and the only way a test
// can see a peak that a call has already trimmed away by the time it returns —
// which is exactly the failure mode being pinned: a resting size that looks
// perfect while the process spiked to the size of the wipe.
func refusalSizes(set *wipeTombstoneSet) (held, live, slots int) {
	set.mu.Lock()
	defer set.mu.Unlock()
	return len(set.entries), len(set.order) - set.head, cap(set.order)
}

// TestTheRefusalSetIsBounded pins the cap.
//
// The refusals live as long as the process and expire on a horizon measured in
// days, so their number is chosen by the user rather than by this code: a few
// large threads cleared in one sitting is a few hundred thousand ids that
// nothing would shrink until they aged out. A TTL is not a bound.
func TestTheRefusalSetIsBounded(t *testing.T) {
	t.Parallel()

	set := newWipeTombstoneSet(func() deleteTaskList { return nil })
	now := time.Now().UTC()

	// Older refusals first, so the eviction has something to prefer.
	old := make([]domain.MessageID, 0, 1024)
	for i := range 1024 {
		old = append(old, domain.MessageID(fmt.Sprintf("old-%06d", i)))
	}
	set.Note(old, now.Add(-wipeTombstoneTTL/2))

	fresh := make([]domain.MessageID, 0, maxWipeTombstones)
	for i := range maxWipeTombstones {
		fresh = append(fresh, domain.MessageID(fmt.Sprintf("fresh-%06d", i)))
	}
	set.Note(fresh, now)

	held, live, slots := refusalSizes(set)
	if held > maxWipeTombstones {
		t.Fatalf("the set holds %d entries, past the cap of %d", held, maxWipeTombstones)
	}
	// The queue is consumed with the map rather than growing beside it — a queue
	// that only ever appended would be the leak the cap exists to prevent.
	if live > held {
		t.Errorf("the eviction queue holds %d live positions for %d entries", live, held)
	}
	// And the slice never had to hold much more than the set itself.
	if slots > 2*held {
		t.Errorf("the queue slice grew to %d slots for %d entries", slots, held)
	}

	// What survived is the recent half: the ids whose senders may still be
	// re-seeding them.
	if refused, _ := set.Refuses(fresh[len(fresh)-1], now); !refused {
		t.Error("the newest refusal was evicted while older ones stayed")
	}
	if refused, _ := set.Refuses(old[0], now); refused {
		t.Error("an older refusal survived while newer ones were evicted: the order is backwards")
	}
}

// TestARenewedRefusalMovesToTheBackOfTheQueue pins the invariant the queue
// rests on: its head is the next refusal to expire.
//
// Renewing an id used to leave it where it was. The queue then had an entry at
// the head that expires LATER than the ones behind it, and the reaper — which
// stops at the first refusal still refusing — stopped there and never reached
// them. Every expired refusal behind a renewed one stayed in the map for as
// long as the renewal lived: a leak whose size is chosen by how often ids are
// renewed.
func TestARenewedRefusalMovesToTheBackOfTheQueue(t *testing.T) {
	t.Parallel()

	set := newWipeTombstoneSet(func() deleteTaskList { return nil })
	start := time.Now().UTC()

	set.Note([]domain.MessageID{"renewed"}, start)
	set.Note([]domain.MessageID{"behind-it"}, start.Add(time.Hour))
	// The same id refused again — the deletion paths do this whenever a message
	// is re-refused, and Hydrate does it on every load.
	set.Note([]domain.MessageID{"renewed"}, start.Add(2*time.Hour))

	// Past "behind-it" but not past the renewal.
	set.reap(start.Add(wipeTombstoneTTL + 90*time.Minute))

	if refused, _ := set.Refuses("behind-it", start.Add(wipeTombstoneTTL+90*time.Minute)); refused {
		t.Error("an expired refusal is still held: the reaper stopped at the renewed one ahead of it")
	}
	held, live, _ := refusalSizes(set)
	if held != 1 {
		t.Errorf("the set holds %d entries, want only the renewed one", held)
	}
	if live != 1 {
		t.Errorf("the queue holds %d live positions for %d entries", live, held)
	}
	if refused, _ := set.Refuses("renewed", start.Add(wipeTombstoneTTL+90*time.Minute)); !refused {
		t.Error("the renewal itself was taken")
	}
}

// TestForgettingAndRefusingAnIdAgainDoesNotHauntTheQueue pins the bound on the
// queue itself.
//
// A rolled-back deletion forgets the ids it pre-refused, and the next attempt
// refuses them again. Each round used to leave the old position behind, and
// nothing removed it: the queue is drained from the head, so a position in the
// middle waits for everything before it to expire — days. Retrying one deletion
// in a loop therefore grew the queue without bound while the map stayed nearly
// empty, and the cap, which counts the map, never saw it.
func TestForgettingAndRefusingAnIdAgainDoesNotHauntTheQueue(t *testing.T) {
	t.Parallel()

	set := newWipeTombstoneSet(func() deleteTaskList { return nil })
	start := time.Now().UTC()

	// One refusal that outlives the whole test sits at the head of the queue.
	// Without it the superseded positions are all in front and the drain walks
	// straight over them; with it — which is every real node, where something
	// was deleted before the retrying one — the drain stops at the head and the
	// positions behind it are only ever reclaimed by the compaction.
	set.Note([]domain.MessageID{"anchor"}, start)

	const rounds = 8 * wipeTombstoneQueueSlack
	for i := range rounds {
		at := start.Add(time.Duration(i) * time.Second)
		set.Note([]domain.MessageID{"retried"}, at)
		set.Forget([]domain.MessageID{"retried"})
	}
	set.Note([]domain.MessageID{"retried"}, start.Add(rounds*time.Second))

	held, live, _ := refusalSizes(set)
	if held != 2 {
		t.Fatalf("the set holds %d entries, want the anchor and the one that stuck", held)
	}
	if live > 2*held+wipeTombstoneQueueSlack {
		t.Errorf("the queue holds %d live positions for %d entries after %d retries", live, held, rounds)
	}
	if refused, _ := set.Refuses("retried", start.Add(rounds*time.Second)); !refused {
		t.Error("the refusal that stuck is not held")
	}
}

// TestLoadingTheOwedDeletionsPrunesWhatExpired pins that a load does the same
// housekeeping as a deletion.
//
// Hydrate runs at startup and again after every failed load, and it used to
// only ever add. On a node that keeps failing to read its outstanding
// deletions, that is a set that grows on every retry and is pruned by nothing
// until a message happens to arrive.
func TestLoadingTheOwedDeletionsPrunesWhatExpired(t *testing.T) {
	t.Parallel()

	tasks := &stubDeleteTaskList{owed: []domain.MessageID{"still-owed"}}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })
	start := time.Now().UTC()

	set.Note([]domain.MessageID{"long-gone"}, start)
	set.Hydrate(context.Background(), start.Add(wipeTombstoneTTL+time.Minute))

	set.mu.Lock()
	_, stillHeld := set.entries["long-gone"]
	held := len(set.entries)
	set.mu.Unlock()
	if stillHeld {
		t.Error("a refusal that ran out days ago survived the load")
	}
	if held != 0 {
		t.Errorf("the capped set holds %d entries after the load, want none left", held)
	}
	if refused, _ := set.Refuses("still-owed", start.Add(wipeTombstoneTTL+time.Minute)); !refused {
		t.Error("the deletion still owed to a peer is not refused")
	}
}

// TestADeletionThePeerConfirmedStopsBeingExempt pins the half of the owed set
// that used to be missing: it is a MIRROR of the work queue on disk, so it
// shrinks when the work does.
//
// The exemption from the cap rests on that. An id that never leaves is not a
// deletion in flight but a permanent hole in the bound — and the claim that
// "their number falls as the peers confirm" has to be true of the code, not
// only of the documentation.
//
// Nothing is told about the confirmation: the set is read again and made to say
// what the disk says. Reacting to an ack directly is the mistake this file has
// already made once — an ack proves the peer's database, not the absence of
// copies in a relay's buffer.
func TestADeletionThePeerConfirmedStopsBeingExempt(t *testing.T) {
	t.Parallel()

	tasks := &stubDeleteTaskList{owed: []domain.MessageID{"settling", "still-owed"}}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })
	now := time.Now().UTC()

	set.Hydrate(context.Background(), now)
	if refused, _ := set.Refuses("settling", now); !refused {
		t.Fatal("a deletion still owed is not refused")
	}

	// The peer confirms: the request is dropped, and the next read says so.
	tasks.mu.Lock()
	tasks.owed = []domain.MessageID{"still-owed"}
	tasks.mu.Unlock()
	set.Hydrate(context.Background(), now.Add(wipeTombstoneReapPeriod))

	if refused, _ := set.Refuses("settling", now.Add(wipeTombstoneReapPeriod)); refused {
		t.Error("a deletion the peer confirmed is still exempt from the cap")
	}
	if refused, _ := set.Refuses("still-owed", now.Add(wipeTombstoneReapPeriod)); !refused {
		t.Error("the deletion still owed was retired with the settled one")
	}
}

// TestAFailedRefreshKeepsTheRefusalsItAlreadyHas pins the other side of reading
// the list on a timer.
//
// A load that fails does not mean the list changed — it means we could not read
// it. Dropping the refusals would open the replay window on a busy database,
// and answering "cannot tell" for every arrival would stall the receive path
// for as long as the disk is slow. Only a set that has NEVER been read has
// nothing to stand on, and that one falls back per message.
func TestAFailedRefreshKeepsTheRefusalsItAlreadyHas(t *testing.T) {
	t.Parallel()

	tasks := &stubDeleteTaskList{owed: []domain.MessageID{"owed"}}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })
	now := time.Now().UTC()

	set.Hydrate(context.Background(), now)
	set.Note([]domain.MessageID{"noted"}, now)

	tasks.mu.Lock()
	tasks.fail = true
	tasks.mu.Unlock()
	set.Hydrate(context.Background(), now.Add(wipeTombstoneReapPeriod))

	at := now.Add(wipeTombstoneReapPeriod)
	if refused, known := set.Refuses("owed", at); !refused || !known {
		t.Errorf("after a failed refresh the owed refusal reads refused=%v known=%v", refused, known)
	}
	if refused, known := set.Refuses("noted", at); !refused || !known {
		t.Errorf("after a failed refresh this process's own refusal reads refused=%v known=%v", refused, known)
	}
	if refused, known := set.Refuses("never-seen", at); refused || !known {
		t.Errorf("after a failed refresh an unrelated id reads refused=%v known=%v, want the set to still answer", refused, known)
	}
}

// TestTheDeletionsStillOwedSurviveTheCap pins the one exemption from the cap,
// which is a deliberate hole in an otherwise absolute bound.
//
// Those ids are not this process's accumulation: they name deletions this node
// still owes a peer, they are read from the work queue on disk, and they are
// exactly the deletions whose messages may still be re-sent. Evicting them as
// "oldest" — they are hydrated first, so they ARE the oldest — would drop the
// protection of the only refusals the design can justify keeping.
//
// The bound is therefore the cap PLUS the outstanding deletions, and it is
// stated that way rather than claimed to be the cap alone.
func TestTheDeletionsStillOwedSurviveTheCap(t *testing.T) {
	t.Parallel()

	tasks := &stubDeleteTaskList{owed: []domain.MessageID{"owed-a", "owed-b"}}
	set := newWipeTombstoneSet(func() deleteTaskList { return tasks })
	now := time.Now().UTC()

	set.Hydrate(context.Background(), now)

	flood := make([]domain.MessageID, 0, maxWipeTombstones)
	for i := range maxWipeTombstones {
		flood = append(flood, domain.MessageID(fmt.Sprintf("flood-%06d", i)))
	}
	set.Note(flood, now.Add(time.Minute))

	for _, id := range tasks.owed {
		if refused, _ := set.Refuses(id, now.Add(time.Minute)); !refused {
			t.Errorf("%s: a deletion still owed to a peer was evicted by the cap", id)
		}
	}
	held, live, _ := refusalSizes(set)
	if held > maxWipeTombstones {
		t.Errorf("the capped set holds %d entries, past the cap of %d", held, maxWipeTombstones)
	}
	if live > held {
		t.Errorf("the eviction queue holds %d live positions for %d entries", live, held)
	}
}

// TestOneEnormousWipeIsTrimmedAsItGoes pins the PEAK, not the resting size.
//
// A wipe names a whole conversation and arrives as one call. Applying the cap
// after the batch bounds what is kept and nothing else: the allocation the cap
// exists to prevent has already happened by then, and on a big enough thread it
// is the allocation that matters — the resting size looks perfect in a test
// while the process spikes to the size of the wipe.
func TestOneEnormousWipeIsTrimmedAsItGoes(t *testing.T) {
	t.Parallel()

	set := newWipeTombstoneSet(func() deleteTaskList { return nil })
	now := time.Now().UTC()

	const batch = 3 * maxWipeTombstones
	huge := make([]domain.MessageID, 0, batch)
	for i := range batch {
		huge = append(huge, domain.MessageID(fmt.Sprintf("wiped-%07d", i)))
	}
	set.Note(huge, now)

	held, live, slots := refusalSizes(set)
	if held > maxWipeTombstones {
		t.Errorf("the set holds %d entries after a wipe of %d, past the cap of %d", held, batch, maxWipeTombstones)
	}
	if live > held {
		t.Errorf("the queue holds %d live positions for %d entries", live, held)
	}
	// The one that would have been missed: the queue slice grew to the size of
	// the batch and stayed there, because the head kept moving and nothing
	// released what it had walked past until the call returned.
	if slots > 2*(maxWipeTombstones+wipeTombstoneQueueSlack) {
		t.Errorf("the queue slice grew to %d slots on a batch of %d: it was not trimmed as it went", slots, batch)
	}

	// The recent half is what survived, as everywhere else.
	if refused, _ := set.Refuses(huge[len(huge)-1], now); !refused {
		t.Error("the newest refusal of the wipe was evicted")
	}
	if refused, _ := set.Refuses(huge[0], now); refused {
		t.Error("the oldest refusal of the wipe survived a set that is over the cap")
	}
}

// TestAnExpiredRefusalStopsBeingHeldAnywhere pins what "it does not survive the
// process" is worth if the process keeps it anyway.
//
// A queue position holds a PLAIN message id. Moving the head past it does not
// release the string, and shortening the slice does not release what the slots
// beyond it point at: both keep the id readable in this process's memory long
// after its refusal ran out — a quiet node reclaims the prefix never, because
// there is never enough of it to be worth copying. The in-memory window is
// allowed to hold ids only while it is USING them.
func TestAnExpiredRefusalStopsBeingHeldAnywhere(t *testing.T) {
	t.Parallel()

	set := newWipeTombstoneSet(func() deleteTaskList { return nil })
	start := time.Now().UTC()

	set.Note([]domain.MessageID{"gone-one", "gone-two"}, start)
	set.Note([]domain.MessageID{"still-here"}, start.Add(time.Hour))
	set.reap(start.Add(wipeTombstoneTTL + time.Minute))

	set.mu.Lock()
	defer set.mu.Unlock()
	// The whole backing array, not just the part the slice admits to.
	whole := set.order[:cap(set.order)]
	for i, queued := range whole {
		inUse := i >= set.head && i < len(set.order)
		if inUse {
			continue
		}
		if queued.id != "" {
			t.Errorf("slot %d still holds the id %q after its refusal expired", i, queued.id)
		}
	}
	if _, held := set.entries["gone-one"]; held {
		t.Error("an expired refusal is still in the map")
	}
}

// TestExpiredRefusalsLeaveTheQueueToo pins the other half of the same
// structure: the reaper walks the queue from the head and stops at the first
// entry still refusing, rather than scanning the map.
func TestExpiredRefusalsLeaveTheQueueToo(t *testing.T) {
	t.Parallel()

	set := newWipeTombstoneSet(func() deleteTaskList { return nil })
	start := time.Now().UTC()

	set.Note([]domain.MessageID{"first", "second"}, start)
	set.Note([]domain.MessageID{"third"}, start.Add(time.Hour))

	// Past the first two refusals but not the third.
	set.reap(start.Add(wipeTombstoneTTL + time.Minute))

	held, live, _ := refusalSizes(set)
	if held != 1 || live != 1 {
		t.Fatalf("after the reap: %d entries and %d queued, want one of each", held, live)
	}
	if refused, _ := set.Refuses("third", start.Add(wipeTombstoneTTL+time.Minute)); !refused {
		t.Error("the reap took a refusal that had not expired")
	}
}

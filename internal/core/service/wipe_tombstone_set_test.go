package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// stubTombstoneJournal counts loads and can fail them, so a test can watch
// what the inbound path pays when the database is wedged rather than late.
type stubTombstoneJournal struct {
	mu    sync.Mutex
	loads int
	fail  bool
	live  map[domain.MessageID]time.Time
}

func (j *stubTombstoneJournal) NoteWipeTombstones(context.Context, []domain.MessageID, time.Time) error {
	return nil
}

func (j *stubTombstoneJournal) DropWipeTombstones(context.Context, []domain.MessageID) error {
	return nil
}

func (j *stubTombstoneJournal) ReapWipeTombstones(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func (j *stubTombstoneJournal) LiveWipeTombstones(context.Context, time.Time) (map[domain.MessageID]time.Time, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.loads++
	if j.fail {
		return nil, errors.New("database is locked")
	}
	return j.live, nil
}

func (j *stubTombstoneJournal) loadCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.loads
}

func (j *stubTombstoneJournal) recover(live map[domain.MessageID]time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.fail = false
	j.live = live
}

// TestFallbackLoadIsThrottledPerMessage: Has runs on the inbound path, and
// a wedged database answers each attempt only after busy_timeout. One load
// per arriving message would put the disk's health on the critical path of
// receiving.
func TestFallbackLoadIsThrottledPerMessage(t *testing.T) {
	t.Parallel()

	journal := &stubTombstoneJournal{fail: true}
	set := newWipeTombstoneSet(func() wipeTombstoneJournal { return journal })

	now := time.Now().UTC()
	set.Hydrate(context.Background(), now)
	if journal.loadCount() != 1 {
		t.Fatalf("startup loads = %d, want 1", journal.loadCount())
	}

	// A burst of arrivals inside one throttle window costs one load.
	for range 20 {
		set.Refuses(domain.MessageID("11111111-1111-4111-8111-111111111111"), now)
	}
	if got := journal.loadCount(); got != 2 {
		t.Errorf("loads during a burst = %d, want 2 (startup + one retry)", got)
	}

	// Past the floor, one more attempt — and only one.
	later := now.Add(wipeTombstoneReloadFloor + time.Second)
	for range 20 {
		set.Refuses(domain.MessageID("11111111-1111-4111-8111-111111111111"), later)
	}
	if got := journal.loadCount(); got != 3 {
		t.Errorf("loads after the floor = %d, want 3", got)
	}
}

// TestFallbackStopsOnceTheLoadSucceeds: the throttle must not delay the
// recovery beyond its own window — once a retry loads the set, Has is a
// pure memory lookup again and the refusals are back.
func TestFallbackStopsOnceTheLoadSucceeds(t *testing.T) {
	t.Parallel()

	const refused = domain.MessageID("22222222-2222-4222-8222-222222222222")
	journal := &stubTombstoneJournal{fail: true}
	set := newWipeTombstoneSet(func() wipeTombstoneJournal { return journal })

	now := time.Now().UTC()
	set.Hydrate(context.Background(), now)
	if got, known := set.Refuses(refused, now); got || known {
		t.Fatalf("a failed load answered refused=%v known=%v; it can know neither", got, known)
	}

	journal.recover(map[domain.MessageID]time.Time{refused: now.Add(time.Hour)})
	later := now.Add(wipeTombstoneReloadFloor + time.Second)
	if got, known := set.Refuses(refused, later); !got || !known {
		t.Fatalf("the retry did not pick the refusals back up: refused=%v known=%v", got, known)
	}

	before := journal.loadCount()
	for range 20 {
		set.Refuses(refused, later.Add(2*wipeTombstoneReloadFloor))
	}
	if got := journal.loadCount(); got != before {
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
	journal := &stubTombstoneJournal{fail: true}
	set := newWipeTombstoneSet(func() wipeTombstoneJournal { return journal })
	set.Hydrate(context.Background(), time.Now().UTC())

	adapter := NewMessageStoreAdapter(
		NewChatlogGateway(newTestChatlogStore(t, owner), owner), self, set)

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
	journal.recover(nil)
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

	journal := &stubTombstoneJournal{fail: true}
	set := newWipeTombstoneSet(func() wipeTombstoneJournal { return journal })
	now := time.Now().UTC()
	set.Hydrate(context.Background(), now)

	const id = domain.MessageID("55555555-5555-4555-8555-555555555555")
	// The first miss spends the one allowed reload.
	set.Refuses(id, now)
	before := journal.loadCount()

	refused, known := set.Refuses(id, now)
	if journal.loadCount() != before {
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
	journal := &stubTombstoneJournal{fail: true}
	set := newWipeTombstoneSet(func() wipeTombstoneJournal { return journal })
	set.Hydrate(context.Background(), time.Now().UTC())

	adapter := NewMessageStoreAdapter(
		NewChatlogGateway(newTestChatlogStore(t, owner), owner), self, set)

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
		Error: "the refusals of deleted ids are unreadable",
	})
	if !errors.Is(deferred, protocol.ErrStoreDeferred) {
		t.Fatalf("errors.Is lost the reason: %v", deferred)
	}
	if !strings.Contains(deferred.Error(), "unreadable") {
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

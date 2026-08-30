package service

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// dm_beep_test.go pins what the notification sound is FOR: a message this
// process has not seen before. The badge is keyed by message id and adding the
// same id twice changes nothing, so a sound emitted per EVENT rather than per
// MESSAGE is a sound with nothing to show for it — the user hears an arrival
// and finds the sidebar exactly as it was.

func countBeeps(t *testing.T, r *DMRouter, settle time.Duration) int {
	t.Helper()
	deadline := time.After(settle)
	beeps := 0
	for {
		select {
		case ev := <-r.uiEvents:
			if ev.Type == UIEventBeep {
				beeps++
			}
		case <-deadline:
			return beeps
		}
	}
}

// TestOneSoundPerMessageNotPerEvent covers the two ways one message reaches
// this function twice. Both end the same way for the user — the sidebar looks
// exactly as it did — so both have to end the same way for the speaker.
func TestOneSoundPerMessageNotPerEvent(t *testing.T) {
	// A real client, because the point is a message that WAS handled: the
	// first delivery has to reach the badge for the second one to be the
	// repeat this is about.
	client, id := newTestDesktopClientWithNode(t)
	peer := domaintest.ID("beep-peer")
	me := domain.PeerIdentityFromWire(id.Address)

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "beep-1",
		Sender:    peer.String(),
		Recipient: me.String(),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	newRouter := func(t *testing.T) *DMRouter {
		t.Helper()
		r := newTestRouter()
		r.client = client
		r.onNewMessage(event)
		if first := countBeeps(t, r, 150*time.Millisecond); first != 1 {
			t.Fatalf("first delivery produced %d sounds, want exactly 1", first)
		}
		r.mu.RLock()
		badged := len(r.unreadIDs[peer])
		r.mu.RUnlock()
		if badged != 1 {
			t.Fatalf("the first delivery badged %d messages; the fixture cannot say anything about a repeat", badged)
		}
		return r
	}

	t.Run("the same event again", func(t *testing.T) {
		r := newRouter(t)

		r.onNewMessage(event)

		if again := countBeeps(t, r, 150*time.Millisecond); again != 0 {
			t.Fatalf("the same message announced itself %d more times", again)
		}
	})

	t.Run("into the conversation on screen", func(t *testing.T) {
		// The open conversation is the case where NO badge is raised — the
		// user is reading it — so "already counted" cannot answer for it and
		// "already handled" has to. The cache is deliberately not loaded, so
		// the message takes the mid-switch path rather than the one that
		// returns early on a cache hit.
		r := newTestRouter()
		r.client = client
		r.mu.Lock()
		r.activePeer = peer
		r.mu.Unlock()

		r.onNewMessage(event)
		if first := countBeeps(t, r, 150*time.Millisecond); first != 1 {
			t.Fatalf("first delivery into the open chat produced %d sounds, want exactly 1", first)
		}
		r.mu.RLock()
		badged := len(r.unreadIDs[peer])
		r.mu.RUnlock()
		if badged != 0 {
			t.Fatalf("the open conversation raised a badge (%d); this subtest is no longer about the case it was written for", badged)
		}

		r.onNewMessage(event)

		if again := countBeeps(t, r, 150*time.Millisecond); again != 0 {
			t.Fatalf("a message already delivered into the open chat announced itself %d more times", again)
		}
	})

	t.Run("after its id was put back through the dedup gate", func(t *testing.T) {
		r := newRouter(t)

		// What a failed read does when it cannot finish with the message:
		// reopens the gate so the repair path rediscovers it. The BADGE
		// stays — it was recorded before the failure — so the rediscovery
		// has nothing to add to the sidebar.
		r.evictSeenMessages(event.MessageID)

		r.onNewMessage(event)

		if again := countBeeps(t, r, 150*time.Millisecond); again != 0 {
			t.Fatalf("a message already counted on the sidebar announced itself %d more times; the badge cannot move for it, so the sound has nothing to show", again)
		}
	})
}

// TestNoSecondSoundAfterAFailedReload is the combination the two tests above
// leave open, and the one the user actually hears: the conversation is ON
// SCREEN, so no badge is raised, AND the reload that follows the message fails,
// so its id is put back through the dedup gate. Both memories that used to
// answer "have we announced this" are then blank, and the next delivery of the
// same message rings again — for a message that is still nowhere to be seen.
func TestNoSecondSoundAfterAFailedReload(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	peer := domaintest.ID("beep-after-failed-reload")
	me := domain.PeerIdentityFromWire(id.Address)

	r := newTestRouter()
	r.client = client
	r.mu.Lock()
	r.activePeer = peer // on screen: nothing will badge this message
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "beep-reload-1",
		Sender:    peer.String(),
		Recipient: me.String(),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	r.onNewMessage(event)
	if first := countBeeps(t, r, 200*time.Millisecond); first != 1 {
		t.Fatalf("first delivery produced %d sounds, want exactly 1", first)
	}

	r.mu.RLock()
	badged := len(r.unreadIDs[peer])
	r.mu.RUnlock()
	if badged != 0 {
		t.Fatalf("the conversation on screen raised %d badges; this test is about the case where nothing does", badged)
	}

	// The reload that follows a message into a conversation still loading can
	// fail — a database busy, a peer switch under it — and every such path
	// puts the id back through the dedup gate. Reproduced directly, because
	// what matters here is the STATE it leaves: nothing handled, nothing
	// badged, and a message the user has already been told about.
	r.evictSeenMessages(event.MessageID)

	r.onNewMessage(event)

	if again := countBeeps(t, r, 200*time.Millisecond); again != 0 {
		t.Fatalf("the same message announced itself %d more times, with no badge and nothing on screen to account for it", again)
	}
}

// TestStartupReplayDoesNotLeaveTheSoundOwed covers the message that is handled
// WITHOUT ringing. Startup replay re-delivers old messages and deliberately
// makes no sound for them — but "no sound was made" must not read as "a sound
// is still owed": the same event arriving after startup would then ring for a
// message the badge is already counting, which is the whole defect.
func TestStartupReplayDoesNotLeaveTheSoundOwed(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	peer := domaintest.ID("replay-beep-peer")
	me := domain.PeerIdentityFromWire(id.Address)

	r := newTestRouter()
	r.client = client
	r.mu.Lock()
	r.replayingStartup = true
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "replay-beep-1",
		Sender:    peer.String(),
		Recipient: me.String(),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	r.onNewMessage(event)
	if during := countBeeps(t, r, 150*time.Millisecond); during != 0 {
		t.Fatalf("startup replay made %d sounds; it re-delivers old messages", during)
	}

	// Startup ends, and the same message is delivered again — a re-publish,
	// or a rediscovery after a failed apply reopened its id.
	r.mu.Lock()
	r.replayingStartup = false
	r.mu.Unlock()
	r.evictSeenMessages(event.MessageID)

	r.onNewMessage(event)

	if after := countBeeps(t, r, 150*time.Millisecond); after != 0 {
		t.Fatalf("a message carried through startup announced itself %d times afterwards; it is not new and the badge does not move for it", after)
	}
}

// TestHeaderRepairDoesNotReannounce is the same rule on the other path that
// settles messages. The header pass claims ids the live path already dealt
// with, and it must not ring for one it has been told about.
func TestHeaderRepairDoesNotReannounce(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	peer := domaintest.ID("header-beep-peer")
	me := domain.PeerIdentityFromWire(id.Address)

	r := newTestRouter()
	r.client = client

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "header-beep-1",
		Sender:    peer.String(),
		Recipient: me.String(),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	r.onNewMessage(event)
	if first := countBeeps(t, r, 150*time.Millisecond); first != 1 {
		t.Fatalf("first delivery produced %d sounds, want exactly 1", first)
	}

	// A failed apply reopens the id, so the header pass finds it again.
	r.evictSeenMessages(event.MessageID)
	r.mu.Lock()
	r.initialSynced = true // not the first sync, which suppresses the sound anyway
	r.mu.Unlock()

	r.repairUnreadFromHeaders(NodeStatus{DMHeaders: []DMHeader{{
		ID: event.MessageID, Sender: peer, Recipient: me,
	}}})

	if again := countBeeps(t, r, 150*time.Millisecond); again != 0 {
		t.Fatalf("the header pass announced a message the live path had already announced (%d times)", again)
	}
}

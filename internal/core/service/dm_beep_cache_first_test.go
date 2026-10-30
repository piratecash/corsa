package service

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestFirstLiveEventStillBeepsAfterConversationReload covers a pre-existing
// source gap, not an established cause of the user's intermittent DEV silence.
// A real chatlog row can be loaded into the open conversation before its first
// live event is handled. Loading it must not spend that event's one sound.
// Only UIEventBeep is counted: no desktop window, audio device or network runs.
func TestFirstLiveEventStillBeepsAfterConversationReload(t *testing.T) {
	for _, tc := range []struct {
		name        string
		reloadFirst bool
	}{
		{name: "reload-before-first-live-event", reloadFirst: true},
		{name: "live-event-before-reload-control", reloadFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newStoredConversation(t)
			// Registered after the fixture's database cleanup, so all owned
			// receipt work is joined before the database and TempDirs close,
			// including when an assertion fails below.
			t.Cleanup(func() {
				if !c.r.ShutdownDrain(2 * time.Second) {
					t.Error("router background work did not finish")
				}
			})
			c.store(t, "sound-history", 0, chatlog.StatusSeen)
			c.open(t)
			if got := countBeeps(t, c.r, 150*time.Millisecond); got != 0 {
				t.Fatalf("opening existing history emitted %d beeps, want 0", got)
			}

			const incomingID = "sound-first-live-event"
			// Append through the real migrated SQLite chatlog, with a real
			// encrypted envelope. Deliberately hold its live event until the
			// chosen step below; no event-bus overflow is involved.
			c.store(t, incomingID, 1, chatlog.StatusDelivered)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			rows, err := c.client.chatLog.Read(ctx, "dm", c.peer)
			cancel()
			if err != nil {
				t.Fatalf("read the stored incoming envelope: %v", err)
			}
			var event protocol.LocalChangeEvent
			for _, row := range rows {
				if row.ID == incomingID {
					event = protocol.LocalChangeEvent{
						Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: row.ID,
						Sender: row.Sender, Recipient: row.Recipient,
						Body: row.Body, CreatedAt: row.CreatedAt,
					}
				}
			}
			if event.MessageID != incomingID || event.Body == "" {
				t.Fatal("the real chatlog did not return the original incoming envelope")
			}

			load := func() {
				t.Helper()
				if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
					t.Fatal("the actual conversation reload failed")
				}
				var found bool
				for _, message := range c.r.cache.Messages() {
					if message.ID == incomingID {
						found = message.Body == incomingID && message.Sender == c.peer && message.Recipient.String() == c.me.Address
					}
				}
				if !found || !c.r.cache.MatchesPeer(c.peer) {
					t.Fatal("reload did not place and decrypt the incoming message in the original conversation")
				}
				if got := countBeeps(t, c.r, 150*time.Millisecond); got != 0 {
					t.Errorf("a conversation reload emitted %d beeps, want 0", got)
				}
			}
			if tc.reloadFirst {
				load()
			}
			c.r.mu.RLock()
			gate := c.r.seenMessageIDs[localMessageRef(event).Key()]
			replaying, startupComplete := c.r.replayingStartup, c.r.startupComplete
			c.r.mu.RUnlock()
			if gate.announced || gate.handled || replaying || !startupComplete {
				t.Fatalf("fixture lost the first-live-event boundary: gate=%+v replaying=%v startupComplete=%v", gate, replaying, startupComplete)
			}

			c.r.onNewMessage(event)
			if got := countBeeps(t, c.r, 150*time.Millisecond); got != 1 {
				t.Errorf("first live incoming event emitted %d beeps, want exactly 1 (reloadFirst=%v)", got, tc.reloadFirst)
			}
			if !tc.reloadFirst {
				load()
			}
			// The fix must not trade a missed first cue for duplicate cues.
			c.r.onNewMessage(event)
			if got := countBeeps(t, c.r, 150*time.Millisecond); got != 0 {
				t.Errorf("duplicate live event emitted %d beeps, want 0", got)
			}
		})
	}
}

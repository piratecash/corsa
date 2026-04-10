package desktop

import "testing"

// newTestWindow returns a Window struct with just enough state to drive
// applyPendingAttach and the pendingAttach channel dance. It deliberately
// avoids any Gio / router dependencies — the attachment generation logic
// operates purely on the fields assigned here.
func newTestWindow() *Window {
	return &Window{
		pendingAttach: make(chan pendingAttachMsg, 1),
	}
}

// TestApplyPendingAttachUserPickWinsOverEmpty verifies that a fresh user
// pick replaces an empty composer slot and bumps the generation counter.
func TestApplyPendingAttachUserPickWinsOverEmpty(t *testing.T) {
	w := newTestWindow()

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: false})

	if w.attachedFile != "/tmp/a.bin" {
		t.Errorf("attachedFile = %q, want /tmp/a.bin", w.attachedFile)
	}
	if w.attachGeneration != 1 {
		t.Errorf("attachGeneration = %d, want 1 after first pick", w.attachGeneration)
	}
}

// TestApplyPendingAttachUserPickOverwritesExisting verifies that a new
// user pick replaces a prior attachment and bumps the generation — this
// is the path that later invalidates any in-flight restore from the
// previous send.
func TestApplyPendingAttachUserPickOverwritesExisting(t *testing.T) {
	w := newTestWindow()
	w.attachedFile = "/tmp/a.bin"
	w.attachGeneration = 1

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/b.bin", restore: false})

	if w.attachedFile != "/tmp/b.bin" {
		t.Errorf("attachedFile = %q, want /tmp/b.bin", w.attachedFile)
	}
	if w.attachGeneration != 2 {
		t.Errorf("attachGeneration = %d, want 2", w.attachGeneration)
	}
}

// TestApplyPendingAttachRestoreHappyPath verifies that a restore delivered
// while the composer is empty and the generation still matches replays the
// attachment for the user to retry.
func TestApplyPendingAttachRestoreHappyPath(t *testing.T) {
	w := newTestWindow()
	// User picked, triggerFileSend captured gen=1 and cleared attachedFile.
	w.attachedFile = ""
	w.attachGeneration = 1

	w.applyPendingAttach(pendingAttachMsg{
		path:       "/tmp/a.bin",
		restore:    true,
		generation: 1,
	})

	if w.attachedFile != "/tmp/a.bin" {
		t.Errorf("attachedFile = %q, want /tmp/a.bin (restore should succeed)", w.attachedFile)
	}
	// Restore must not bump generation — it reverts to the captured state,
	// not a new user action.
	if w.attachGeneration != 1 {
		t.Errorf("attachGeneration = %d, want 1 (restore must not bump)", w.attachGeneration)
	}
}

// TestApplyPendingAttachRestoreDroppedWhenGenerationBumped verifies the
// core bug fix: a late restore from a failed old send MUST NOT overwrite
// a newer user-selected attachment. The user picked a different file
// while the first send was in flight; attachGeneration advanced past
// the old send's captured generation.
func TestApplyPendingAttachRestoreDroppedWhenGenerationBumped(t *testing.T) {
	w := newTestWindow()
	// Timeline:
	//   gen=1: user picked /tmp/a.bin
	//   triggerFileSend captured sendGen=1 and cleared attachedFile
	//   gen=2: user picked /tmp/b.bin (still present in composer)
	w.attachedFile = "/tmp/b.bin"
	w.attachGeneration = 2

	// Old send's failure arrives with the stale generation.
	w.applyPendingAttach(pendingAttachMsg{
		path:       "/tmp/a.bin",
		restore:    true,
		generation: 1,
	})

	if w.attachedFile != "/tmp/b.bin" {
		t.Errorf("attachedFile = %q, want /tmp/b.bin (newer pick must not be overwritten)", w.attachedFile)
	}
	if w.attachGeneration != 2 {
		t.Errorf("attachGeneration = %d, want 2 (restore must not bump)", w.attachGeneration)
	}
}

// TestApplyPendingAttachRestoreDroppedWhenCancelled verifies that an
// explicit attachment cancel (which bumps the generation) invalidates
// any in-flight restore — the user dismissed the attachment and must
// not see it reappear from a late async failure.
func TestApplyPendingAttachRestoreDroppedWhenCancelled(t *testing.T) {
	w := newTestWindow()
	// Timeline:
	//   gen=1: user picked /tmp/a.bin
	//   triggerFileSend captured sendGen=1, cleared attachedFile
	//   user clicked attachCancelBtn: attachedFile="", generation bumped to 2
	w.attachedFile = ""
	w.attachGeneration = 2

	w.applyPendingAttach(pendingAttachMsg{
		path:       "/tmp/a.bin",
		restore:    true,
		generation: 1,
	})

	if w.attachedFile != "" {
		t.Errorf("attachedFile = %q, want empty (cancel must invalidate stale restore)", w.attachedFile)
	}
}

// TestApplyPendingAttachRestoreDroppedWhenSlotNonEmpty is a defense-in-depth
// check: if somehow attachedFile is non-empty at the moment a restore
// arrives, the restore must not overwrite it even if generations match.
// This guards against any future code path that could set attachedFile
// without bumping generation.
func TestApplyPendingAttachRestoreDroppedWhenSlotNonEmpty(t *testing.T) {
	w := newTestWindow()
	w.attachedFile = "/tmp/newer.bin"
	w.attachGeneration = 1

	w.applyPendingAttach(pendingAttachMsg{
		path:       "/tmp/a.bin",
		restore:    true,
		generation: 1,
	})

	if w.attachedFile != "/tmp/newer.bin" {
		t.Errorf("attachedFile = %q, want /tmp/newer.bin (slot must not be overwritten)", w.attachedFile)
	}
}

// TestRestoreShouldYieldUserPickBeatsRestore verifies that a user pick
// already queued in the channel takes priority over an incoming restore.
func TestRestoreShouldYieldUserPickBeatsRestore(t *testing.T) {
	existing := pendingAttachMsg{path: "/tmp/newer.bin", restore: false}
	incoming := pendingAttachMsg{path: "/tmp/old.bin", restore: true, generation: 5}

	if !restoreShouldYield(existing, incoming) {
		t.Error("restore must yield to a queued user pick")
	}
}

// TestRestoreShouldYieldNewerRestoreWinsOverOlder verifies the
// cross-send race resolution: between two restores competing for the
// single-slot channel, the higher-generation one wins.
func TestRestoreShouldYieldNewerRestoreWinsOverOlder(t *testing.T) {
	// Send #2 already queued its restore with generation=7.
	existing := pendingAttachMsg{path: "/tmp/send2.bin", restore: true, generation: 7}
	// Send #1's older restore arrives with generation=5.
	incoming := pendingAttachMsg{path: "/tmp/send1.bin", restore: true, generation: 5}

	if !restoreShouldYield(existing, incoming) {
		t.Error("older restore must yield to a newer queued restore")
	}
}

// TestRestoreShouldYieldOlderRestoreIsReplaced verifies that an incoming
// restore with a higher generation than the queued one takes the slot.
func TestRestoreShouldYieldOlderRestoreIsReplaced(t *testing.T) {
	existing := pendingAttachMsg{path: "/tmp/send1.bin", restore: true, generation: 5}
	incoming := pendingAttachMsg{path: "/tmp/send2.bin", restore: true, generation: 7}

	if restoreShouldYield(existing, incoming) {
		t.Error("newer restore must replace an older queued restore")
	}
}

// TestRestoreShouldYieldEqualGenerationPrefersExisting verifies that when
// two restores share a generation (should not happen in practice since
// each send captures its own sendGen, but handled deterministically),
// the already-queued one wins to avoid spurious writes.
func TestRestoreShouldYieldEqualGenerationPrefersExisting(t *testing.T) {
	existing := pendingAttachMsg{path: "/tmp/first.bin", restore: true, generation: 5}
	incoming := pendingAttachMsg{path: "/tmp/second.bin", restore: true, generation: 5}

	if !restoreShouldYield(existing, incoming) {
		t.Error("at equal generation the existing queued restore must be kept")
	}
}

// TestRestoreShouldYieldAdoptedUserPickNeverYields verifies that once the
// convergent drain loop in restoreAttach adopts a user pick (msg = existing
// where existing.restore == false), subsequent calls to restoreShouldYield
// with that adopted message as incoming always return false — the user pick
// dominates all further comparisons.
func TestRestoreShouldYieldAdoptedUserPickNeverYields(t *testing.T) {
	// Scenario: loop adopted a user pick, next drain pulls another restore.
	adopted := pendingAttachMsg{path: "/tmp/user-pick.bin", restore: false}
	laterRestore := pendingAttachMsg{path: "/tmp/restore.bin", restore: true, generation: 99}

	if restoreShouldYield(laterRestore, adopted) {
		t.Error("adopted user pick must never yield, even to a high-generation restore")
	}
}

// TestRestoreAttachConvergesUnderContention exercises the convergent loop
// in restoreAttach. Multiple concurrent restore goroutines compete for
// the single-slot pendingAttach channel while a user pick is also in
// flight. The invariant: after all goroutines finish and the UI drains,
// the surviving message must be the user pick (restore == false) — it
// must never be lost to a stale restore.
func TestRestoreAttachConvergesUnderContention(t *testing.T) {
	t.Parallel()

	const iterations = 200
	for i := 0; i < iterations; i++ {
		w := newTestWindow()

		// Pre-fill channel with a user pick (the high-priority message).
		userPick := pendingAttachMsg{path: "/tmp/user.bin", restore: false}
		w.pendingAttach <- userPick

		// Launch several concurrent restore goroutines, each trying to
		// send a stale restore into the channel. The bug was: one of
		// these could drain the user pick, fail to requeue it, and
		// leave a restore in the channel instead.
		const restores = 5
		done := make(chan struct{})
		for r := 0; r < restores; r++ {
			gen := uint64(r + 1)
			go func() {
				defer func() { done <- struct{}{} }()
				msg := pendingAttachMsg{
					path:       "/tmp/restore.bin",
					restore:    true,
					generation: gen,
				}
				// Reproduce the convergent loop from restoreAttach.
				for {
					select {
					case w.pendingAttach <- msg:
						return
					default:
					}
					select {
					case existing := <-w.pendingAttach:
						if restoreShouldYield(existing, msg) {
							msg = existing
						}
					default:
					}
				}
			}()
		}

		// Wait for all restores to finish.
		for r := 0; r < restores; r++ {
			<-done
		}

		// Drain the channel — exactly one message must survive.
		select {
		case survivor := <-w.pendingAttach:
			if survivor.restore {
				t.Fatalf("iteration %d: user pick was lost — channel contains "+
					"restore{gen=%d} instead of user pick",
					i, survivor.generation)
			}
			if survivor.path != "/tmp/user.bin" {
				t.Fatalf("iteration %d: unexpected path %q", i, survivor.path)
			}
		default:
			t.Fatalf("iteration %d: channel is empty — all messages dropped", i)
		}
	}
}

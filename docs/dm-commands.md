# CORSA DM commands

## English

### Overview

A DM command is typed metadata attached to an encrypted direct message.
Transit nodes see only the outer wire topic (`dm` or `dm-control`); command
dispatch happens after the recipient decrypts the envelope.

Two classes of DM are defined:

- **Data DMs** are recorded in chatlog and surface in the chat thread.
  - `file_announce` — pre-existing. Announces an outbound file transfer;
    the message is recorded in chatlog as a regular DM and additionally
    registers a file-transfer mapping on the receiver via
    `FileTransferBridge`.
- **Control DMs** are not recorded in chatlog and never surface in the
  chat thread. They travel on a dedicated wire topic so neither the
  sender's nor the recipient's node persists them, and neither side
  emits a `LocalChangeNewMessage` for them.
  - `message_delete` — new. Asks the recipient to remove a previously
    delivered data DM from their chatlog (and any state attached to it,
    such as receiver-side file-transfer mappings and downloaded blobs).
    The request is honoured **only if the target message's `MessageFlag`
    permits the deletion** for the requesting peer (see the
    Authorization section below for the full matrix); otherwise the
    receiver rejects the request with `message_delete_ack` (`denied` or
    `immutable`) and leaves the chatlog entry and all attached state
    intact.
  - `message_delete_ack` — new. The recipient's reply confirming how the
    `message_delete` was resolved. Also a control DM and not stored.

#### Reliability guarantees

The pair `message_delete` + `message_delete_ack` is designed so the
sender can know with certainty whether the deletion was received and
processed. The full mechanics are specified later in this document; the
guarantees the rest of the system can rely on are:

1. **The recipient must reply.** Every inbound `message_delete` produces
   a `message_delete_ack`, unless the payload itself is malformed or the
   signature fails — those are protocol-level errors and the sender
   retries. The recipient never silently drops a well-formed
   authenticated `message_delete`.
2. **Five explicit ack statuses.** The status is always one of
   `deleted`, `not_found`, `denied`, `immutable`, `error`. The first
   four are terminal. In particular, `not_found` is the documented
   response when the recipient does not have the target message at all
   (already deleted, never received, wrong ID) — this is reported back
   so the sender can stop retrying and surface the outcome to the UI.
   `error` is the one non-terminal answer: the recipient could not
   decide the request because its own chatlog was unavailable or the
   lookup/delete failed. It says nothing about whether the row is there,
   so the sender keeps the intent and asks again. Reporting `not_found`
   for such a fault would retire the intent over a transient database
   error and leave the message alive on one side with nobody left to
   ask.
3. **The sender re-issues the request until it gets an ack**, driven by
   a durable delete intent rather than an in-memory queue: an
   unreachable peer costs nothing and the schedule survives a restart.
   Only a spent attempt budget retires an intent — 720 unanswered
   dispatches, roughly a month of a peer that is reachable and keeps not
   answering. Time a peer spends unreachable costs nothing.

   **Local deletion is not gated on the ack.** `chatlog.DeleteByID`,
   the file-transfer cleanup hook and the UI eviction all run inside
   `SendMessageDelete`, before anything is sent — a message the user
   asked to destroy does not wait on somebody else's connectivity. The
   ack decides only the fate of the intent, and a `denied` /
   `immutable` answer is surfaced so the user knows their copy is gone
   while the peer's is not.

   JSON persistence rendezvous-ed alongside `transfers-*.json` is a
   tracked follow-up; until it lands the UI surfaces in-process
   budget exhaustion via `TopicMessageDeleteCompleted` with
   `Abandoned=true` but cannot signal an abandonment caused by
   restart. After a restart-driven abandonment the local row stays
   intact (because we never deleted it pre-ack) and the user can
   re-issue the delete from the UI.
4. **Idempotent on the recipient.** A duplicate `message_delete` after
   the row is already gone produces the same `not_found` ack as the
   first one. A duplicate request after a successful delete also
   produces `not_found`. The sender treats both `deleted` and
   `not_found` as success and clears the pending entry.

### Type model

`domain.DMCommand` is a typed string enumerating commands that may appear
inside `domain.OutgoingDM.Command`. It is separate from
`domain.FileAction`, which now narrowly identifies file-transfer protocol
frames (`chunk_request`, `chunk_response`, `file_downloaded`,
`file_downloaded_ack`) carried inside the `FileCommandFrame` wire format
— **not** DMs.

```
package domain

type DMCommand string

const (
    DMCommandFileAnnounce      DMCommand = "file_announce"
    DMCommandMessageDelete     DMCommand = "message_delete"
    DMCommandMessageDeleteAck  DMCommand = "message_delete_ack"
)

// Valid accepts the empty command (the regular text-DM case) plus the
// three named values. Empty must remain valid because callers that
// build a plain text OutgoingDM leave Command unset; rejecting it here
// would force every caller to special-case the empty string.
func (c DMCommand) Valid() bool {
    switch c {
    case "", DMCommandFileAnnounce, DMCommandMessageDelete, DMCommandMessageDeleteAck:
        return true
    default:
        return false
    }
}

// IsControl reports whether the command identifies a control DM
// (message_delete, message_delete_ack). The empty command and
// DMCommandFileAnnounce are data DMs, not control.
func (c DMCommand) IsControl() bool {
    switch c {
    case DMCommandMessageDelete, DMCommandMessageDeleteAck:
        return true
    default:
        return false
    }
}
```

`OutgoingDM.Command` switches type from `FileAction` to `DMCommand`.
`service.DirectMessage.Command` follows. The wire shape of the encrypted
plaintext (`directmsg.PlainMessage.Command string`) is unchanged — strings
on the wire, typed at the domain boundary.

Empty command remains the regular text-DM case; callers validate only
non-empty commands with `DMCommand.Valid()`.

`DMCommand.IsControl()` is the canonical predicate that splits data DMs
from control DMs. Send and receive code paths branch on this predicate,
not on string comparisons.

`domain.FileActionAnnounce` is removed: the announce action belongs to the
DM channel, not to the file-command channel. Existing call sites are
migrated to `DMCommandFileAnnounce`.

### Payloads

```
package domain

type MessageDeletePayload struct {
    TargetID MessageID `json:"target_id"`
}

type MessageDeleteStatus string

const (
    MessageDeleteStatusDeleted   MessageDeleteStatus = "deleted"   // row was present and is now gone
    MessageDeleteStatusNotFound  MessageDeleteStatus = "not_found" // no row for target_id; idempotent success
    MessageDeleteStatusDenied    MessageDeleteStatus = "denied"    // flag did not authorize this peer
    MessageDeleteStatusImmutable MessageDeleteStatus = "immutable" // flag forbids deletion outright
    MessageDeleteStatusError     MessageDeleteStatus = "error"     // recipient could not decide; NOT terminal
)

type MessageDeleteAckPayload struct {
    TargetID MessageID           `json:"target_id"`
    Status   MessageDeleteStatus `json:"status"`
}
```

Both payloads are encoded as JSON in the encrypted plaintext's
`command_data`. `Body` is empty for control DMs — control DMs travel on a
dedicated wire topic (see Send / Receive paths) and the body validation
that `DMCrypto.SendDirectMessage` performs for data DMs is bypassed by the
control path. The receiver's control handler discards `Body` regardless.

`MessageDeleteStatusDeleted` and `MessageDeleteStatusNotFound` are both
success outcomes from the protocol's perspective: the two sides are now
consistent, so `handleInboundMessageDeleteAck` retires the intent. It
also re-runs `chatlog.DeleteByID` + `OnMessageDeleted` +
`evictDeletedMessageFromUI` defensively — the local row went at request
time, but a late relay echo can re-insert it, and the ack is the last
moment we are guaranteed to be looking at this id.

`Denied` and `Immutable` are terminal failures: asking again would get
the same answer, so the intent is retired too and the status is
surfaced to the UI. There is nothing to roll back — the user's own copy
is gone either way. What the user learns is that the peer kept theirs.

### Authorization

Each chat message carries a `protocol.MessageFlag` recorded in
`chatlog.Entry.Flag`:

| Flag                | Who may delete on the wire                                    |
|---------------------|---------------------------------------------------------------|
| `immutable`         | Nobody. `message_delete` is rejected.                         |
| `sender-delete`     | Only the original sender of the target message.               |
| `any-delete`        | The original sender or recipient.                             |
| `auto-delete-ttl`   | Same as `sender-delete` until the TTL elapses, then expires.  |
| empty / unknown     | Treated as `sender-delete` (current default policy).          |

The flag is stamped by the AUTHOR at send time and travels with the row,
so it is the author's own answer to "may my counterpart erase this from
their side too". `DMCrypto.SendDirectMessage` stamps `any-delete` on outgoing DMs
(`defaultOutgoingMessageFlag`): a conversation is shared, and a user
deleting a message from it expects it gone, not gone from their own
screen while the other copy stands. `immutable` is the absolute end of that scale — the
"this row is part of the permanent record" promise (legal evidence,
tamper-evident logs) that not even the author can revoke.

The flag gates neither the local copy nor the request. Removing a row
from your own database is always yours to do, at any delivery status, and
asking the peer is always worth doing: the flag is what THEY consult when
the request arrives, and their answer comes back as an ack the user can
see. Deciding on their behalf that they would refuse only hides the
request — the message leaves the user's screen with nothing queued and
nothing said about the copy that remains.

The receiver enforces this when an inbound `message_delete` arrives:

1. Resolve `M = chatlog.Get(target_id)`. If absent — log idempotent skip
   and reply with `message_delete_ack { status: "not_found" }`; no
   chatlog or file-transfer cleanup runs.
2. Read `M.Flag`.
3. If `immutable` — reply ack `immutable` with a warn log and skip.
4. If `sender-delete` (or empty) — require `from == M.Sender`. Otherwise
   reply ack `denied`.
5. If `any-delete` — require `from == M.Sender || from == M.Recipient`.
   Otherwise reply ack `denied`.
6. If `auto-delete-ttl` — same as `sender-delete`; the TTL itself is enforced
   independently by the chatlog's expiry sweeper.

`from` is the verified DM envelope sender (after signature check), not a
self-reported field inside the plaintext payload.

The local sender (UI) re-runs the same check before submitting the
`message_delete`: an `immutable` target is refused before any network
traffic, and a target the flag does not let us reach is deleted locally
without one.

### Send path (control DM, sender side)

A control DM must not appear in the sender's chatlog or chat thread, and
must not produce a UI echo. The existing `DMCrypto.SendDirectMessage` is
the wrong tool: it routes through `node` Frame `send_message`, which
writes the outbound row to chatlog and emits `LocalChangeNewMessage` so
the sender's UI renders the message. Reusing it would surface
`message_delete` as a visible `[delete]` line in the sender's own chat.

The control path is a separate function and a separate node Frame:

```
package service

// DMCrypto.SendControlMessage encrypts a control DM and submits it on
// the dedicated control wire topic. It does not write to chatlog, does
// not return a DirectMessage echo, and does not invalidate the UI
// thread. Body is empty; Command must satisfy DMCommand.IsControl().
func (d *DMCrypto) SendControlMessage(
    ctx context.Context,
    to domain.PeerIdentity,
    cmd domain.DMCommand,
    payload string,
) (domain.MessageID, error)
```

Internally it calls `LocalRequestFrame` with the new Frame type
`send_control_message` and `Topic = "dm-control"`. The node's
`send_control_message` handler:

1. Verifies the topic is `dm-control` and the body is well-formed
   ciphertext.
2. Submits the encrypted envelope to mesh routing using the same path
   as `send_message`.
3. **Skips** the chatlog write that `send_message` performs.
4. **Skips** publishing `LocalChangeNewMessage`.
5. Returns `message_stored`-equivalent ack so the caller knows the wire
   handoff succeeded (this is **not** the recipient ack — see
   "Acknowledgement and retry" below).

The dedicated topic `dm-control` is visible to relays, the same way
`dm` and `gazeta` are visible. This is an intentional metadata leak:
relays can tell that a unit of traffic is a control DM (vs. a data DM).
The mitigation is volume — control DMs are rare, so the side-channel
signal is small. Hiding the distinction would require putting control
DMs on the same `dm` topic, which forces every node to write every
inbound DM to chatlog before it can be classified — that contradicts
the no-storage invariant.

### Receive path (control DM, recipient side)

Control DMs reuse the existing `storeIncomingMessage` entry point and
branch internally on `msg.Topic == TopicControlDM`. There is no
separate `storeIncomingControlMessage` function — the divergence is
expressed as gates inside the shared path so that the locking
discipline (`knowledgeMu` → `gossipMu` sequential, see
`docs/locking.md`) remains identical to the data-DM path and no new
cross-domain edge is introduced. The control-specific behaviour is:

1. `dispatchNetworkFrame` lands the inbound frame in
   `handleInboundPushMessage` / `handleRelayMessage` exactly like a
   data DM. The non-DM verifier gate exempts both `"dm"` and
   `TopicControlDM` (`protocol.IsDMTopic`), so the per-message
   signature verification in `storeIncomingMessage` (`VerifyEnvelope`)
   is the only authenticity gate.
2. Inside `storeIncomingMessage` the topic branch fires three
   divergences from the data-DM path:
   - the chatlog `messageStore.StoreMessage` call is skipped;
   - the `s.topics[msg.Topic] = append(...)` is skipped (control
     envelopes never enter `s.topics["dm-control"]`, which keeps
     `retryableRelayMessages` from accumulating dead state);
   - the `LocalChangeNewMessage` / `emitLocalChange` block is
     replaced with a `LocalChangeNewControlMessage` publication on
     `ebus.TopicMessageControl`, and the publication only fires when
     `msg.Recipient == s.identity.Address` so the sender side does
     not receive its own outbound control DM as if it were inbound.
3. `DMCrypto.DecryptIncomingControlMessage` (a parallel of
   `DecryptIncomingMessage`) decrypts the envelope and returns
   `(domain.DMCommand, commandData string, sender, ok)`. If the inner
   command is not `IsControl()` the event is dropped — that closes the
   hole where a peer could try to inject a data command through the
   control wire.
4. `DMRouter` subscribes to `ebus.TopicMessageControl` and dispatches
   per `DMCommand`:
   - `message_delete` → `handleInboundMessageDelete`
   - `message_delete_ack` → `handleInboundMessageDeleteAck`
5. Unknown commands are logged at debug and dropped.

The chatlog is **never** touched on the inbound control path until the
authorization-passing branch of `handleInboundMessageDelete` calls
`Store.DeleteByID(target_id)`. There is no transient row, no transient
event, no UI flash.

Transit-only relays (control DM passes through this node, but the
recipient is somebody else and there is no direct peer / table route /
gossip-capable target) take the no-store fallback in
`handleRelayMessage` and return the empty status `""` upstream — the
data-DM "stored" fallback is **not** taken for control DMs because
their no-op store would still ack as if the relay succeeded. The
sender's delete intent then treats the attempt as a miss and re-issues
it on its next due tick.

### Acknowledgement and retry

A control DM is an unreliable wire send: relays may drop it, the peer
may be offline, the peer's process may have died between receiving and
processing. The sender therefore keeps a durable **delete intent** per
outstanding request and re-issues it until the recipient's
`message_delete_ack` (any terminal status) arrives.

```
type DeleteIntent struct {
    MessageID     domain.MessageID
    Peer          domain.PeerIdentity
    CreatedAt     time.Time // the user's original request; the TTL clock
    NextAttemptAt time.Time // only moved by attempts that actually went out
    Attempts      int
}
```

The full policy — when a request is dispatched, what an unreachable peer
costs (nothing), the 30 s→1 h backoff and the attempt budget — is described
under "Scheduled deletion" below. Two properties are worth stating here,
because they are what a reader of the older, in-memory design would
expect to be different:

- The intent is a row in the shared state database, so a process restart
  resumes the retry exactly where it left off. There is no in-memory
  queue to lose.
- The local row is already gone before the first dispatch, so an
  unacknowledged request never leaves the user's own thread diverging
  from their intent. What an expired intent means is only that the
  *peer's* copy may still exist; `TopicMessageDeleteCompleted` with
  `Abandoned=true` is how the user is told.

The recipient is fully idempotent: a duplicate `message_delete` after
the row has already been deleted produces the same `not_found` ack as
the first one. The sender treats `not_found` as success and retires the
intent. Stale acks (for a `target_id` with no intent) are dropped
silently, and an ack whose envelope sender is not the peer the intent
was addressed to decides nothing.

### Idempotency

`chatlog.Store.DeleteByID` returns `(false, nil)` when the row is absent.
The control handler maps this to `MessageDeleteStatusNotFound` and replies
with the corresponding ack. A peer who retries `message_delete` because
it never saw the previous ack will receive the ack again; no error is
ever raised back into the wire.

### Cleanup hooks

After `chatlog.Store.DeleteByID(M)` succeeds, the control handler invokes a
generic cleanup chain. For now there is one hook:

- `filetransfer.Manager.CleanupTransferByMessageID(domain.FileID(M.ID))` —
  if `M` was a `file_announce`, this drops the matching sender or receiver
  mapping, releases the transmit-blob ref count (sender side), and deletes
  any partial or completed blob in the download directory (receiver side).
  Idempotent: a no-op when there is no mapping for that ID.

  Dropping the mapping and erasing the bytes are two different writes, and
  only the first one used to be durable — so an unlink that failed left the
  content of a deleted message on disk with nothing left that knew to look
  for it. What must still be erased is now recorded as a CLEANUP INTENT in
  the same persisted state that drops the mapping, cleared only when the
  files it names are actually gone, and retried by the transfer
  maintenance tick — including after a restart, which is the case an
  in-memory retry cannot cover. Retries go through
  `FileStore.PurgeUnreferenced`, which erases without touching a ref count
  and is therefore safe to repeat; the count itself drops exactly once, in
  `Release`. A blob another message still references is left alone and the
  intent is satisfied: the content is owed elsewhere.

Future DM types can register additional cleanup callbacks against this
chain; the chain is order-independent because each callback is scoped to
its own domain.

### Delete routes (UI scope)

A delete always removes the local copy immediately — reachable peer or
not. Keeping a message the user asked to destroy, because somebody else
happens to be offline, is the exposure the feature exists to end. What
varies is what has to happen on the way there.

`domain.MessageDeleteContext` classifies the request from three facts —
who authored the row, whether the flag lets us ask the peer at all, and
whether the recipient confirmed it (a `delivered` or `seen` receipt) —
and `DMRouter.SendMessageDelete` returns the route it took. Peer
reachability is deliberately **not** an input: it decides *when* the peer
is told, which the scheduler owns, never *whether* the local copy goes.

| Direction | Confirmed by peer | Envelope proven unsent | Route       | Local row | Peer side |
|-----------|-------------------|------------------------|-------------|-----------|-----------|
| incoming  | —                 | —                      | `scheduled` | removed   | scheduled |
| outgoing  | no                | yes                    | `recalled`  | removed   | nothing asked |
| outgoing  | no                | no                     | `withdraw`  | removed   | delivery cancelled, then scheduled |
| outgoing  | yes               | —                      | `scheduled` | removed   | scheduled |

- **`scheduled`** (incoming row): the local copy goes and the author is
  asked to drop theirs. Nothing of ours was ever in flight, so there is
  no delivery to cancel first.

  The row's flag does NOT decide whether we ask — it is the author's
  answer, delivered by their ack (`deleted` when they honoured it,
  `denied` when they had reserved the message to themselves). Reading the
  stored flag as "do not even ask" was a bug the user hit: every message
  received before the default changed carries `sender-delete`, and so
  does every message from a peer on an older build, so deleting one
  vanished from the screen with nothing queued, no indicator, and nothing
  said about the copy that remains.
- **`recalled`** (outgoing, never confirmed, and the cancellation proved
  the envelope never reached the wire): nobody has ever seen the message,
  so nothing is scheduled and the terminal outcome is published at once.
  Asking a peer to delete a message they never received would tell them
  one existed.
- **`withdraw`** (outgoing, never confirmed, emission not ruled out): the
  delivery may still be sitting in this node's own queues, so it is
  cancelled **first** (see below); only then is the row removed and the
  peer-side deletion scheduled. The schedule is kept because a copy can
  escape between the send and the cancellation, and `not_found` is a
  cheap answer for the case where it did not.
- **`scheduled`** (outgoing, confirmed): nothing left to cancel. The row
  goes and the peer-side deletion is scheduled.
- **`!found`** (the row is already gone — deleted earlier, expired by
  TTL): treated as an outgoing row with the caller-supplied peer, so a
  re-issued delete still converges the other side.

An `immutable` row is refused up front, with no state mutation at all.

### Scheduled deletion

The peer-side half of a deletion is a durable intent, not an in-memory
retry: one row in `message_delete_intents` (migration 0005) carrying the
peer identity, the message id and the scheduler's bookkeeping — no body,
no sender, no original timestamp. A blanked-out message row was the
alternative and was rejected: it leaves a tombstone in the conversation
for anyone reading the database, which is precisely what the deletion
was meant to prevent.

Lifecycle:

1. `SendMessageDelete` removes the local row, records the intent, and
   plants the refusal of the id — **all in one transaction**
   (`chatlog.DeleteWithIntent`; the local-only routes take
   `DeleteMessageWithTombstone`, which is the same commit minus the
   intent). They are one invariant seen from three sides: the user's
   copy is gone, somebody still owes us the peer's, and a replay of the
   same envelope will be refused rather than re-inserted. Separate
   commits leave crash windows in which the copy is destroyed and nobody
   will ever ask the peer, or in which the row is gone but its refusal
   is not — and the next relay retry hands the message back. When the
   peer is reachable the request also goes out immediately, which is an
   optimisation over waiting for the next sweep and nothing more.
2. `deleteRetryLoop` sweeps due intents every 5 s (`deleteRetryTickPeriod`).
   For each one:
   - `deleteIntentGiveUpAttempts` (720) dispatches made and unanswered
     — written off, with a warn log and a
     `TopicMessageDeleteCompleted` publication carrying `Abandoned=true`.
     The budget is spent in ATTEMPTS, not in days: a calendar deadline
     runs while the peer is unreachable, which is exactly the stretch
     the durable intent exists to survive, so it gives up on the case
     the feature was built for and reports "abandoned" about a peer
     nobody managed to ask. Attempts only accrue when the peer was there
     to be asked, and the backoff caps at an hour, so the budget is
     about a month of being ignored. The price is that a request to a
     contact who never returns is kept indefinitely: one row per
     deletion addressed to an absent identity, dropped with the rest of
     their history when the identity is removed. A re-issue does not
     refill the budget — attempts and `created_at` both survive it —
     because a budget a click can reset is one a user can make immortal
     without meaning to;
   - peer unreachable — **parked for `deleteIntentHoldInterval` (30 s),
     charging nothing**. No attempt, no backoff, no expiry pressure
     beyond the TTL clock. The park is a fairness device rather than a
     delay: due intents are read oldest-first under a limit, so a pile
     of them addressed to one absent contact left at the head of that
     queue would starve every other peer's deletions. The
     `peer.connected` subscription un-parks a peer's intents the moment
     they hand back, so the interval is only the ceiling for a peer that
     becomes routable without connecting to us directly. Parked rows are
     marked (`held`) so that the kick moves ONLY them: an
     intent waiting out the backoff of an attempt that actually went out
     is not parked, and resetting it would hand a peer whose application
     is dead — but whose transport reconnects every few seconds — one
     request per handshake instead of the exponential schedule;
   - peer already served `deleteIntentPerPeerPerSweep` (4) requests this
     sweep — parked to the next tick, also uncharged. A bulk deletion
     can leave hundreds of intents due at once, and firing them at one
     peer as fast as the sweep reads them is what its control-DM rate
     limiter would answer;
   - otherwise — dispatched, then the attempt is charged and the next
     due time set by `deleteIntentBackoff` (30 s doubling to a 1 h cap).
     A dispatch failure still charges the attempt: an attempt is one
     dispatch this node made, and a send that failed is exactly what the
     backoff exists for.
3. `handleInboundMessageDeleteAck` settles the intent. **Every** terminal
   status retires it — `deleted` / `not_found` because the peer is now
   consistent with us, `denied` / `immutable` because asking again will
   not change their answer. The status is published so the user learns
   their copy is gone while the peer's is not. `error` is the exception:
   the intent is kept and the schedule is left exactly as it is —
   nothing is published, because nothing finished. The ack is NOT
   charged as an attempt: the dispatch that provoked it already charged
   one and set the next due time, so charging again would count one
   exchange twice, stepping the backoff up per round-trip and burning
   the give-up budget at double rate. An ack is the answer to an
   attempt, not another attempt.

Because the intent lives in the shared state database, a restart resumes
exactly where it left off: the sweep reads the same rows, and the
scheduler has no in-memory state to lose.

Bounds: one sweep handles at most `deleteIntentSweepLimit` (64) intents,
so a large backlog cannot monopolise the scheduler goroutine or the
control path behind it.

Nothing caps how long a row can sit for a contact who never comes back,
and that is deliberate: the budget is spent in attempts, so a peer who is
simply absent never spends it. The cost is one row per deletion addressed
to an absent identity, and it goes with the rest of their history when
the identity is removed (`DeleteByPeer`, which takes every table naming
that peer). What that costs at rest is a park write per sweep, which is
why parking is batched into one statement and its interval is minutes
rather than seconds: an unbounded parked set turns a per-row park into a
permanent write floor on a device that has to sleep.

A removed identity abandons the peer-side deletions that were scheduled
and not yet acknowledged — the ids are the last rows naming an erased
conversation, and re-sending requests about it would carry exactly the
metadata the removal was for. Asking both sides to forget a thread while
keeping the contact is what the bulk wipe is for.

**Visibility.** The row is gone the moment the user clicks, so there is
no bubble left to hang a per-message indicator on. The conversation
header carries the count instead ("N waiting for the peer to delete",
from `DeleteIntentCountsByPeer`), which is the only lasting feedback a
request handed to an offline peer has; the status line is transient by
design and cannot serve that purpose.

The immediate outcomes carry their route (`MessageDeleteOutcome.Route`)
so the status line can tell them apart: a `recalled` message says the
message had not gone out and the peer never received it, while `local`
and the later peer acks read as a plain deletion. Without that, two
chats answer the same click with the same caption for different
outcomes and the user has no way to tell which they got.

### Cancelling a delivery that is still ours

A DM the recipient has never confirmed may still be in this node's own
delivery state rather than in the network: relays are forwarding-only
and nothing in the mesh stores a user message for an offline recipient
(`docs/protocol/relay.md` INV-3). Deleting such a message therefore has
to stop the delivery, or the peer would be handed a message the user has
already destroyed.

`node.Service.CancelOutgoingDelivery` empties every place that could
still put the envelope on the wire, in one cross-domain section
(`docs/locking.md`): the store-and-forward backlog (`s.topics`), the
queued per-peer frames, the sender-owned end-to-end retry
(`awaitingDelivered`), the relay-retry shadow and the outbound status
entry. It refuses a message this node did not author, so a local caller
cannot purge a transit envelope by guessing an id.

Ordering is load-bearing: the cancellation runs **before** the local row
is removed, so a delivery cannot hand the peer a message the user has
already destroyed. A cancellation that FAILS, however, does not stop the
deletion — "the node cannot be reached to cancel" is the same class of
outage the user is deleting around, and holding their copy hostage to it
is the behaviour this feature exists to end. The peer-side request stays
scheduled, so a message that does escape is still recalled.

What the cancellation guarantees is *no further delivery attempt*, not
"the peer never saw it" — unless it says so. It reports `never_emitted`
only when the sender-owned retry entry was still present and no frame
carrying the envelope had ever left this node
(`deliveryRetryEntry.Emitted`). That claim is what promotes the route
from `withdraw` to `recalled` and skips the peer-side request entirely.

`Emitted` is monotone, but not by virtue of the retry engine alone —
that was the hole this contract was written around. Emission has ONE
accounting point, `noteOwnEnvelopeEmitted`, and every path that can put
an origin envelope in front of a peer calls it:

- the live push at store time and at every retry tick;
- **the auth-time backlog replay** (`pushBacklogToSubscriber`), which
  serves `s.topics["dm"]` to a recipient that dials US. The backlog
  append is NOT gated on the reachability hold, so a message the
  scheduler is still holding is handed over in full the moment the
  recipient connects — with no attempt, no receipt and nothing the retry
  engine would notice. Missing this path meant a delete could take the
  `recalled` route for a message the peer had already collected: no
  intent recorded, their copy kept forever, nothing left to re-issue.

Gossip and relay emissions need no call of their own; they only run from
the origin send and the retry tick, which record the attempt themselves.
The mark is set BEFORE the write, so the answer errs towards "the peer
might have it": an over-cautious yes costs one control DM the peer
answers `not_found`, while a wrong no is silent and unrecoverable.

Outside that claim, a dispatch already in flight is on the wire, and a
peer that received the message earlier — with a receipt lost or never
sent — keeps its copy, which is why `withdraw` still schedules.

Every deleted id is refused for the next hour — including on the recovery
path, where the row is already absent — so a late echo of the envelope
from some relay's in-flight buffer is re-deleted instead of re-creating
the row the delete was meant to leave gone. The refusal lives in the same
`message_delete_intents` row as the request the peer owes us
(`refuse_until`), and outlives it: the ack means the peer will not keep
the message, not that a stale copy cannot still arrive.

The refusals are read once at startup into memory, and the inbound store
consults that memory rather than the database on every message — and only
for DM traffic. Every refusal names a chat row, and chat rows are `dm`, so
for any other topic the gate could only answer "not refused" or "cannot
tell". The second answer would be actively harmful there: an unreadable
refusal set would hold up everything this node stores, and a broadcast
topic has no sender-owned retry to fall back on, so a deferral on it is
not "the sender tries again" but a plain loss. The topic arrives from the
wire, so the check is also what stops a peer making our reception depend
on a table their messages have nothing to do with.

The gate covers OUTGOING DMs too, deliberately: an id can come from
outside (a file announce, a re-send), so a send could otherwise re-create
a message that was just deleted. What that costs is a refused send while
the refusals are unreadable, and the refusal has to say so — the reply
code travels out as a wrapped `protocol.ErrStoreDeferred`, and the UI
reads it as "not now" rather than the "unexpected send reply" every
refusal used to collapse into. Transience is the only thing about this
answer a caller needs, and it is the one thing the old wording lost.

The WORDING is the UI's, not the service's: the error is published on
`TopicMessageSendFailed`, the desktop subscriber recognises the sentinel
through the wrapping and writes `status.send_deferred` from the
catalogue, in the user's language like every other status of this
feature. The service keeps a generic English fallback line for runtimes
with no UI attached, and invents no sentence of its own. When the
startup read FAILS the set is marked unloaded, not empty: a memory miss
then proves nothing, and the store answers `StoreDeferred` instead of
storing on a guess. That is NOT a write error: the envelope stays out of
the runtime backlog and out of the dedup mark, no delivery receipt is
sent and the frame is not acked, so the message stays with the SENDER and
is re-sent. Answering "failed" instead would keep it in this node's memory
and still acknowledge it, which stops the sender retrying a message that
is on no disk anywhere — and a restart then loses it. Guessing "not
refused" is worse still: a replay would re-create a row the user deleted,
and no later reload of the refusals re-deletes it. Reload attempts are
throttled (`wipeTombstoneReloadFloor`), because a wedged database answers
each attempt only after its busy timeout and the reaper retries on its own
tick regardless; the throttle bounds what the inbound path PAYS, never
what it CONCLUDES.

**On disk.** A deletion that only removes the row from SQLite's logical
view protects against reading the database through SQL and nothing else.
The state database therefore runs with `secure_delete` on (freed pages
are overwritten, see [storage.md](storage.md)), and every deletion
follows the commit with a `wal_checkpoint(TRUNCATE)`: in WAL mode the
zeroing is itself a log frame, and the original bytes live in the `-wal`
file until a checkpoint retires them. The checkpoint is best-effort — a
busy one is not a failed deletion, and the automatic checkpoint still
comes.

**Every** deletion means both sides, deliberately: our own removals
(`removeLocalMessage`, the local conversation wipe), the deletions we
perform because a peer asked (`applyInboundDelete`,
`sweepInboundDeleteScope`) and the TTL sweep (`DeleteExpired`). The
peer-side one is the deletion this whole protocol exists to deliver;
retiring our own pages promptly and theirs whenever the log happened to
fill would put the weaker guarantee exactly where the stronger one was
promised.

**Residue.** `DeleteByID` removes the per-message rows the other
repositories keep under the same id — `seen_ack`, `delivery_failed`
(migration 0003) and the resend intents of migration 0004 — in the same
statement batch. Each of those is a durable record that a message with
this id existed and how its delivery went; leaving them behind keeps
precisely the metadata the deletion was for, and re-seeds retry
schedulers with ids that no longer resolve. Per-PEER state
(`decrypt_recovery_jobs`, `peer_established`, `decrypt_recovery_cycles`)
describes the conversation rather than the message and is untouched.

### Bulk wipe ("Delete chat and ask the peer")

A conversation wipe is N message deletions and nothing else. There is no
bulk command on the wire, no request of its own, no scheduler of its own
and no acknowledgement of its own: "delete this thread" and "delete this
message" differ only in how many ids are involved, and saying so in the
data is what removed the entire parallel apparatus the bulk form used to
need — a second intent table, a row-set table, an answers table, a
receiver-side frozen candidate set, a survivor set, a cache of committed
answers, and a timestamp boundary to describe the rows a single request
could not name.

What follows is not only less code. The request is exact (ids, never a
clock, so a peer whose clock lags cannot lose a message written after the
wipe); it is idempotent (a re-issued wipe re-notes the same ids); it needs
no size cap (each id travels on its own control DM); and a partly
delivered wipe is simply the intents that have not settled yet — visible
in the same "N waiting for the peer to delete" count as any other pending
deletion.

`CompleteConversationDelete`, under the outgoing barrier and after the
in-flight send drain:

1. reads the ids the wipe will take (`ConversationCandidateIDs` — every
   non-immutable row of the thread) and marks them against a late echo
   BEFORE they disappear: inside the transaction there is no moment at
   which anything could act on them;
2. FREEZES the node's deliveries for exactly those ids
   (`FreezeOutgoingDeliveriesTo`), which stops every path that could put
   them on the wire and reports what the node knows about them. A freeze
   rather than the cancellation because it is reversible — see below;
3. deletes exactly those rows, writes ONE DELETE INTENT PER MESSAGE for
   the messages the peer may hold, and records the refusal of every id —
   **in one transaction** (`chatlog.DeleteConversationWithIntents`).
   Either the conversation is gone AND somebody is bound to ask the peer
   for each message, or nothing happened and the user can click again; a
   half-applied wipe is the one outcome they cannot see. File-transfer
   cleanup and UI eviction run after the commit, on the ids the
   transaction actually removed;
4. ends the freeze. On commit that is the real withdrawal
   (`CancelOutgoingDeliveriesTo`, one pass, scoped to the ids it took),
   so a queued message cannot be handed over after the thread is gone —
   while an immutable row, which survives the wipe, keeps its delivery
   instead of being stranded in "sending" forever. On a transaction that
   FAILED it is a thaw: the rows are still here, the messages are still
   the user's, and cancelling them would have left pending messages that
   never arrive with a thread still on screen. That reversibility is the
   whole reason the freeze exists; a cancellation cannot be taken back;
5. lets the barrier down — it exists to stop a send racing the wipe, and
   holding it until the peer answers would leave the user unable to write
   to that conversation for as long as the peer stays away.

Nothing is dispatched from there. Every message of the thread is now an
ordinary delete intent, and the delete scheduler owns it: it paces
requests per peer, parks them while the peer is away, wakes them the
moment the peer connects, and settles each on its own ack. The pacing
matters more here than anywhere else — a wipe of a long thread is a lot
of requests — and it is the pacing that already exists rather than a
second policy that has to agree with it.

Authorship is not consulted for the LOCAL removal: the user is erasing
their own view of a conversation, which is theirs to do for either side's
messages. What each peer-side request may do is the peer's own answer,
carried per message by their ack, exactly as for a single deletion.
Immutable rows survive on both sides.

The barrier itself is the only thing this path still schedules.
`BeginConversationDelete` latches it synchronously so a send cannot slip
in between the click and the wipe; `CompleteConversationDelete` releases
it; and a reservation whose owner never came back — a panic between the
two, a scheduling stall past `convDeleteReservationTTL` — is released by
the delete sweep, which publishes a failed outcome so the user is not
left looking at a conversation they cannot write to.

**Messages that never went out.** A message nobody has ever seen is not
asked about, because the request naming it would be how the peer learns
it existed — the rule the single-message `recalled` route keeps, applied
per row. Whether a message reached the wire is the node's answer, and the
only moment it is authoritative is the cancellation.

The wipe is TWO-PHASE against the delivery engine, because the question
cannot be answered atomically otherwise. Classify then delete, and a
message can go out in between — a row read as "never emitted" is already
at the peer by the time it is destroyed, and no request is ever written
for it. Delete then classify, and the only witness left is the node's
memory, which does not survive a restart and is emptied when a retry runs
out of attempts.

So the wipe FREEZES first (`freeze_conversation_delivery`). A freeze stops
every path that could put the named messages on the wire — the retry tick
skips them without charging an attempt, the backlog replay withholds them
— and returns what the node knows about them at that instant. Nothing can
move while it holds, so the transaction then reads chatlog's
`never_emitted` mark (docs/chatlog.md) from each row it deletes, unions it
with the node's answer, and writes a request only for the messages the
peer may actually hold. Deletion and classification are one fact, like the
deletion and its refusal.

A freeze is used rather than the cancellation because the cancellation
cannot be undone: if the transaction then failed, the user would be left
with messages still on screen that nothing will ever send. The freeze ends
one of two ways — the cancellation withdraws the deliveries for good on
commit, or a thaw puts them back on abort. A cancellation that FAILS after
a commit leaves them frozen — the correct state, since the rows are gone
and nothing may send them — and OWED: the withdrawal is idempotent, so the
delete sweep retries it until it succeeds. Until then this process still
holds the payload of a deleted conversation, and no second wipe can help,
chatlog having no rows left to name.

The single-message delete takes the same freeze for its one id, and for
the same reason: it classifies from the row's mark, which means nothing
while the message can still go out. The freeze has no TTL, so EVERY exit
ends it — including the early ones, an immutable message or a row that
could not be read — because a freeze that outlives its decision stops
that message being sent for the life of the process with nothing able to
release it.

The freeze also fixes the order. The local deletion now commits FIRST and
the withdrawal follows: withdrawing is irreversible, and running it first
means a transaction that then fails leaves the row on screen in "sending"
with every delivery hook already destroyed. The message cannot go out
while the transaction runs, so deleting first costs nothing and keeps the
failure recoverable.

A withdrawal that fails after the row is gone is NOT thawed — that
message is not the user's any more — and not dropped either. It is OWED:
the delete sweep retries it, because until it succeeds this process is
still holding the payload of a deleted message and no later deletion can
name it, the row being gone.

A freeze that cannot be taken at all is the one case where no
classification is made. The rows' marks are only meaningful while nothing
can emit behind the transaction's back, so every message in scope becomes
a request instead — the peer is asked about ids they may not resolve,
rather than a message being deleted here while a copy escapes to them with
nothing left to recall it.

This replaced an earlier parked-then-released design, in which the
requests were written parked and the node's cancellation released them
afterwards. Parking put the privacy rule behind a timeout: a crash, a
stall or a cancellation that errored left the requests un-released, and
after the grace they went out as written. There is now no such state. A
request either exists and is due, or was never written.

`recalled` is a claim that requires PROOF, and the proof now survives a
restart. A send that is WITHHELD because the recipient is unreachable
writes the chatlog's `never_emitted` mark (docs/chatlog.md), and the
mark is withdrawn — durably, before the frame goes out — the moment the
message is emitted, so the reseeded entry inherits the claim instead of
guessing at it.

The mark is the only proof; its absence is not the opposite claim.
Everything the outbox does not know reads as emitted: rows from builds
before the mark existed, a mark lost to a crash, a message emitted by a
path that outlived its scheduler entry. That direction is the deliberate
one — announcing an id costs the peer a request they answer `not_found`,
while an unprovable "never emitted" would leave a delivered message with
them and nothing left to ask.

**What a wipe promises.** The thread is gone here, and each message
becomes a request the peer answers for itself. Under the current default
flag (`any-delete`) they honour it; a message an OLDER build stamped
`sender-delete` and its author wrote is theirs to keep, so they answer
`denied` and their copy stays. The confirmation says so rather than
promising both sides unconditionally, and the outcome is visible per
message instead of being hidden behind a single bulk answer.

**Late delivery limitation (unchanged).** A message the peer SENT before
receiving the deletions but that was still in flight lands afterwards and
is outside the wipe. The tombstone set cancels the neighbouring class —
the SAME envelope re-delivered after being wiped — but a brand-new
in-flight message stays visible until the user deletes it.

### Wire flow

```mermaid
sequenceDiagram
    autonumber
    participant A as Alice desktop
    participant NA as Alice node
    participant NB as Bob node
    participant B as Bob desktop
    A->>A: validate M.Flag locally (refuse early if forbidden)
    A->>A: cancel_message_delivery (unconfirmed rows only)
    A->>A: chatlog.DeleteByID(M) + cleanup + UI eviction
    A->>A: record durable DeleteIntent{target=M, peer=B}
    A->>NA: send_control_message{topic="dm-control", Command=message_delete}
    Note over A: dispatched now only if B is reachable; otherwise the sweep sends it when B returns
    NA-->>NB: encrypted envelope on topic dm-control
    Note over NB,B: NO chatlog write, NO LocalChangeNewMessage
    NB-->>B: LocalChangeNewControlMessage
    B->>B: DMCrypto.DecryptIncomingControlMessage
    B->>B: lookup M, check envelope.From vs M.Sender and M.Flag
    alt authorized
        B->>B: chatlog.DeleteByID(M)
        B->>B: filetransfer.CleanupByMessageID(M)
        B->>NB: send_control_message{Command=message_delete_ack, status=deleted|not_found}
    else denied / immutable
        B->>NB: send_control_message{Command=message_delete_ack, status=denied|immutable}
    end
    NB-->>NA: encrypted ack on topic dm-control
    NA-->>A: LocalChangeNewControlMessage
    alt status deleted or not_found
        A->>A: defensive chatlog.DeleteByID(M) + cleanup + UI eviction
    else status denied or immutable
        Note over A: UI surfaces the rejection — A's own copy is already gone
    end
    A->>A: DMRouter drops DeleteIntent{target=M}
    Note over A: asked 720 times and never answered: log warn, drop the intent, publish Abandoned
```

*Diagram 1 — message_delete propagation with control topic and ack*

### Storage rules for control DMs

Control DMs are kept out of chatlog on **both** sides by routing them on
the dedicated `dm-control` topic. The two diversions are:

| Side    | Path that data DMs follow            | Diversion for control DMs                                     |
|---------|--------------------------------------|---------------------------------------------------------------|
| Sender  | `send_message` → write outbound row + `LocalChangeNewMessage` | `send_control_message` funnels through the same `storeMessageFrame` / `storeIncomingMessage`, which on `TopicControlDM` skips the row write, skips the `s.topics` append, and replaces the LocalChange branch with a recipient-only `LocalChangeNewControlMessage` (no event on the sender's own node) |
| Receiver| `dispatchNetworkFrame` → `storeIncomingMessage` → row + `LocalChangeNewMessage` | Same `storeIncomingMessage`, but on `TopicControlDM` it skips chatlog INSERT, skips `s.topics` append, and emits `LocalChangeNewControlMessage` on `ebus.TopicMessageControl` only when `msg.Recipient == s.identity.Address` |

Consequences:

1. A control DM never appears in any chat thread on either side.
2. There is no `LocalChangeNewMessage` for control DMs, so the regular
   UI message list is not invalidated by their arrival. The bubble for
   the deleted target row `M` is removed from the live conversation
   cache (`ConversationCache.RemoveMessage`) by the delete path itself
   — both `SendMessageDelete` (sender side) and `applyInboundDelete`
   (recipient side) call `evictDeletedMessageFromUI`, which drops the
   cache entry, refreshes the sidebar preview, and emits
   `UIEventMessagesUpdated` + `UIEventSidebarUpdated`. Terminal
   delivery outcomes (`deleted`, `not_found`, `denied`, `immutable`,
   `Abandoned`) are signalled separately via
   `ebus.TopicMessageDeleteCompleted` so callers / RPC clients can
   distinguish a real peer-side deletion from a peer rejection.
3. Receipts (`delivered`/`seen`) are not generated for control DMs.
   Reliability is provided by the application-level
   `message_delete_ack` instead, which carries semantic status the
   delivery receipt cannot express.
4. UI code paths that filter messages by `Command` see only data DMs:
   the file-tab list and the chat thread both query chatlog, and
   chatlog never contained a control DM.
5. Control envelopes also stay out of `node.Service.s.topics[...]`.
   `retryableRelayMessages` (the node-level retry loop) reads only
   `s.topics["dm"]`; storing control envelopes in
   `s.topics["dm-control"]` would create unread state that grows
   without bound and offers no real retry — the only path that
   actually retries control DMs is the application-level delete
   intent on the sender side, which terminates on an ack from the
   peer or on its TTL. The intent is a row in the shared state
   database, so a restart resumes it (see §"Scheduled deletion").
   Routing/push fan-out is unaffected because `executeGossipTargets`
   and `sendTableDirectedRelay` send wire frames on the fly,
   independent of `s.topics`.
6. The node-level `relayRetry` tracker likewise rejects control DMs
   at its entry gate (`trackRelayMessage`). Same reasoning as #5:
   the retry loop only consults `s.topics["dm"]`, so a control entry
   in `relayRetry` would be a dead state burning the
   `maxRelayRetryEntries` quota until tombstone TTL.

### Failure modes

| Situation                                 | Receiver behaviour                                        | Sender behaviour                                       |
|-------------------------------------------|-----------------------------------------------------------|--------------------------------------------------------|
| Target ID not in chatlog                  | Reply ack `not_found`.                                    | Treats `not_found` as success; retires the intent.     |
| Envelope sender ≠ M.Sender (sender-delete)| Reply ack `denied`. Warn log with envelope sender.        | Surfaces the rejection to the UI and retires the intent — asking again cannot change the answer. The user's own copy is already gone. |
| `M.Flag == immutable`                     | Reply ack `immutable`. Warn log.                          | Same as `denied`: surfaced and retired.                |
| Inbound control payload malformed JSON    | Drop. Debug log. No ack.                                  | The intent stays due and is re-issued on the next sweep. |
| Inbound control DM signature invalid      | Drop in `DMCrypto.DecryptIncomingControlMessage`. No ack. | The intent stays due and is re-issued on the next sweep. |
| File-transfer cleanup partially fails     | Errors logged; ack reports `deleted` (chatlog row is gone). | Treats `deleted` as success.                         |
| Receiver's chatlog unavailable / lookup or DELETE fails | Reply ack `error`. Warn log. The row, if any, stays. | Keeps the intent, charges the attempt, re-issues on the backoff. Nothing is published — the deletion is still outstanding. |
| Peer offline                              | No ack arrives.                                           | The sweep skips the intent entirely — no attempt charged, no backoff — and dispatches within one tick of the peer becoming reachable. |
| Application crashes during in-flight retry | n/a                                                      | The intent is durable; the next start resumes the same schedule. The local row was already removed at request time, so nothing on this side is left half-done. |
| Peer never answers any of the 720 dispatches | n/a                                                    | The intent is written off: warn log with `target_id` + peer, `TopicMessageDeleteCompleted` with `Abandoned=true`. The user's copy is gone; the peer's may not be. |
| Peer has not been reachable once since the click | n/a                                                | The intent is kept past the TTL: nothing has been asked yet, so there is nothing to give up on. It goes out on the first tick after they return. |

### Migration notes

`chatlog.Entry.Flag` already exists and is populated from the envelope on
arrival, so no schema change is needed. Existing rows whose `Flag` is
empty fall under the "treated as sender-delete" default and remain
deletable by the original sender. Operators who want a stricter policy
must wait for the planned per-thread default-flag setting (out of scope
for this iteration).

`message_delete` is **not** wire-compatible with peers that only
understand data DMs. Control DMs use `Topic == "dm-control"` and the
`send_control_message` frame, so old peers will not decode them as regular
`directmsg.PlainMessage` rows; they will reject or drop the unknown topic /
frame. Rollout must therefore gate outgoing control DMs on an explicit
peer capability or minimum protocol version. Until that capability exists,
the UI must fall back to local-only deletion for peers that do not advertise
support.

### Test plan

- Unit
  - `DMCommand.Valid()` and `IsControl()` partition known and unknown
    strings correctly.
  - `MessageDeletePayload` and `MessageDeleteAckPayload` JSON round-trip;
    `target_id` validation rejects malformed UUID v4.
  - Authorization matrix: every (flag × envelope sender × M.Sender ×
    M.Recipient) combination resolves to one of `deleted`, `not_found`,
    `denied`, `immutable` as documented.
- Send path
  - `DMCrypto.SendControlMessage` does **not** write a row to chatlog
    on the sender side and does **not** emit `LocalChangeNewMessage`.
  - The submitted Frame carries `Type == "send_control_message"` and
    `Topic == "dm-control"`.
- Receive path
  - `storeIncomingMessage` for `Topic == TopicControlDM` skips the
    chatlog INSERT, skips the `s.topics["dm-control"]` append, and
    publishes `LocalChangeNewControlMessage` on
    `ebus.TopicMessageControl` only when
    `msg.Recipient == s.identity.Address` (sender side stays silent).
  - `handleRelayMessage` no-next-hop fallback returns `""` for
    `TopicControlDM` instead of the data-DM `"stored"` status, so
    upstream does not believe a transit relay succeeded when the
    envelope was in fact dropped.
  - `DMRouter` dispatches by `DMCommand`; unknown commands are dropped
    at debug.
- DM router (control handlers)
  - Inbound `message_delete` from `M.Sender` under `sender-delete`
    deletes `M`, triggers cleanup, replies with ack `deleted`.
  - Local `DeleteDM` and authorized inbound `message_delete` invoke
    `evictDeletedMessageFromUI` which drops the bubble from
    `ConversationCache`, refreshes the sidebar preview from chatlog,
    and emits `UIEventMessagesUpdated` + `UIEventSidebarUpdated` so
    the active conversation re-renders without a manual reload.
  - Terminal outcomes (`deleted`, `not_found`, `denied`, `immutable`,
    `Abandoned=true`) are published exactly once via
    `ebus.TopicMessageDeleteCompleted`. **Only incoming local-only
    deletes** (the user removes a message they received from the peer)
    publish `Status=deleted` immediately and skip the wire send;
    outgoing deletes and absent local targets (`!found`) record an
    intent and publish when it settles.
  - Inbound `message_delete` from `M.Recipient` under `sender-delete`
    is denied; `M` remains; ack is `denied`.
  - Inbound `message_delete` for unknown `target_id` produces ack
    `not_found`.
  - Inbound `message_delete` for `immutable` `M` produces ack
    `immutable`.
  - Inbound `message_delete_ack` for an unknown intent is dropped
    silently (no panic, no log noise); one whose envelope sender is
    not the intent's peer leaves the intent scheduled.
- Delete scheduler (durable)
  - The intent is written before the first dispatch and survives a
    reopen of the database; the sweep resumes it.
  - An unreachable peer is skipped with nothing charged: attempts and
    the due time are unchanged, and the next sweep after the peer
    becomes reachable dispatches.
  - A reachable peer is dispatched to, the attempt is charged, and the
    next due time follows the 30 s→1 h backoff.
  - An intent older than the TTL is dropped with the documented warn
    log and a `TopicMessageDeleteCompleted` publication carrying
    `Abandoned=true`.
- Filetransfer
  - `CleanupTransferByMessageID` drops the sender mapping, releases
    the ref, removes the orphaned blob in `transmit/`.
  - Same for the receiver mapping: removes the mapping and the
    partial/completed files in the download dir.
  - Idempotent: a second call returns no error and no panic.
- Integration (style of `internal/core/node/file_integration.go`)
  - `A` sends a file announce to `B`. `A` invokes `DeleteDM(B, fileID)`.
    `A`'s chatlog row and transmit blob are gone before the call
    returns, and a delete intent for `B` is recorded. After the control
    round-trip and `message_delete_ack` (`deleted`), the intent is
    retired; `B` has no record of `M` and no receiver mapping; `B`'s
    partial download is gone.
  - Denied path: `A` invokes `DeleteDM(B, fileID)` for a row whose
    `MessageFlag` does not authorize `A` for the peer (artificially —
    e.g. row Sender forged in test fixture). After ack `denied`,
    `A`'s chatlog row is **still present** and the
    `TopicMessageDeleteCompleted` outcome carries
    `Status=denied`, `Abandoned=false`.
  - Concurrent: `A` deletes while `B` is downloading. `B`'s download
    is cancelled cleanly; no orphan partial file remains; ack is
    `deleted`; `A`'s row is removed only after the ack lands.
  - Offline-then-online: `B` is unreachable when `A` deletes. `A`'s
    row and its transmit blob are gone at once and the intent is
    parked; nothing is dispatched. Once `B` reconnects the intent is
    re-armed, the control DM lands and the ack retires it.
  - Abandoned: the peer never answers for the whole TTL. The intent
    expires with `Abandoned=true`; `A`'s row was never waiting on
    it.

---

## Русский

### Обзор

DM-команда — это типизированная метаинформация внутри зашифрованного
прямого сообщения. Транзитные узлы видят только внешний wire-топик (`dm`
или `dm-control`); диспетчеризация команд происходит после расшифровки у
получателя.

Определены два класса DM:

- **Data DM** — пишутся в chatlog и видны в чат-потоке.
  - `file_announce` — существующая. Анонс исходящей файловой передачи;
    само сообщение пишется в chatlog как обычный DM и дополнительно
    регистрирует receiver-mapping в `FileTransferBridge`.
- **Control DM** — в chatlog не пишутся и в чат-потоке никогда не
  появляются. Едут на отдельном wire-топике, поэтому ни узел
  отправителя, ни узел получателя их не персистит, и ни одна сторона
  не публикует `LocalChangeNewMessage`.
  - `message_delete` — новая. Просит получателя удалить ранее
    доставленный data DM из своего chatlog (и связанное состояние —
    receiver-mapping, скачанные блобы). Запрос исполняется **только
    если `MessageFlag` целевого сообщения разрешает удаление**
    запрашивающему пиру (полная матрица — в разделе «Авторизация»);
    иначе получатель отклоняет запрос через `message_delete_ack`
    (`denied` или `immutable`) и оставляет запись в chatlog и связанное
    состояние нетронутыми.
  - `message_delete_ack` — новая. Ответ получателя с финальным
    статусом обработки `message_delete`. Тоже control DM, не пишется.

#### Гарантии надёжности

Пара `message_delete` + `message_delete_ack` спроектирована так, чтобы
отправитель достоверно знал, получена ли и обработана ли команда
удаления. Полная механика — ниже по документу; опорные гарантии для
остальной системы:

1. **Получатель обязан ответить.** Каждый входящий `message_delete`
   порождает `message_delete_ack`, кроме случаев невалидного payload или
   невалидной подписи — это протокольные ошибки, и отправитель ретраит.
   Корректно аутентифицированный `message_delete` никогда не дропается
   молча.
2. **Пять явных статусов ack.** Статус всегда один из `deleted`,
   `not_found`, `denied`, `immutable`, `error`; первые четыре
   терминальны. В частности, `not_found` — задокументированный ответ
   когда у получателя целевого сообщения нет вообще (уже удалено,
   никогда не приходило, не тот ID); этот статус возвращается
   отправителю, чтобы тот прекратил retry и корректно отрисовал исход в
   UI. `error` — единственный нетерминальный ответ: получатель не смог
   принять решение, потому что его собственный chatlog был недоступен
   или упал lookup/delete. Он ничего не говорит о наличии строки,
   поэтому отправитель сохраняет intent и спрашивает снова. Отвечать на
   такой сбой `not_found` означало бы снять intent из-за временной
   ошибки БД и оставить сообщение живым на одной стороне, причём
   спросить об этом уже будет некому.
3. **Отправитель переотправляет запрос до получения ack**, и ведёт его
   durable delete intent, а не очередь в памяти: недостижимый пир не
   стоит ничего, а расписание переживает рестарт. Снять неотвеченный
   intent может только исчерпанный бюджет попыток — 720 неотвеченных
   отправок, примерно месяц достижимого пира, который не отвечает.
   Время, которое пир провёл недостижимым, не стоит ничего.

   **Локальное удаление по ack НЕ гейтится.** `chatlog.DeleteByID`,
   cleanup-хук file-transfer и вытеснение из UI выполняются внутри
   `SendMessageDelete`, ещё до какой-либо отправки: сообщение, которое
   пользователь попросил уничтожить, не ждёт чужой связности. Ack
   решает только судьбу intent-а, а ответ `denied` / `immutable`
   показывается пользователю, чтобы он знал: своей копии у него уже
   нет, а копия пира осталась.
4. **Идемпотентность на получателе.** Повторный `message_delete`
   после того, как строка уже удалена, выдаёт тот же `not_found` ack,
   что и первый. Повторный запрос после успешного удаления тоже
   возвращает `not_found`. Отправитель трактует и `deleted`, и
   `not_found` как успех и снимает pending.

### Типовая модель

`domain.DMCommand` — типизированная строка, перечисляющая команды,
которые могут появиться в `domain.OutgoingDM.Command`. Тип отделён от
`domain.FileAction`, который теперь идентифицирует только команды
file-transfer-протокола (`chunk_request`, `chunk_response`,
`file_downloaded`, `file_downloaded_ack`) внутри `FileCommandFrame` — не
внутри DM.

```
package domain

type DMCommand string

const (
    DMCommandFileAnnounce      DMCommand = "file_announce"
    DMCommandMessageDelete     DMCommand = "message_delete"
    DMCommandMessageDeleteAck  DMCommand = "message_delete_ack"
)

// Valid принимает пустую команду (обычный текстовый DM) плюс три
// именованные. Empty обязан остаться валидным: caller, который строит
// плейн-текстовый OutgoingDM, не выставляет Command — отклонять
// empty здесь означало бы заставлять каждого вызывающего
// спецкейсить пустую строку.
func (c DMCommand) Valid() bool {
    switch c {
    case "", DMCommandFileAnnounce, DMCommandMessageDelete, DMCommandMessageDeleteAck:
        return true
    default:
        return false
    }
}

// IsControl говорит, control ли это DM (message_delete,
// message_delete_ack). Пустая команда и DMCommandFileAnnounce — это
// data DM, не control.
func (c DMCommand) IsControl() bool {
    switch c {
    case DMCommandMessageDelete, DMCommandMessageDeleteAck:
        return true
    default:
        return false
    }
}
```

`OutgoingDM.Command` меняет тип с `FileAction` на `DMCommand`.
`service.DirectMessage.Command` — следом. Wire-форма plaintext
(`directmsg.PlainMessage.Command string`) не меняется — на проводе строки,
типизация на границе домена.

Пустая command остаётся обычным text-DM; call-сайты валидируют через
`DMCommand.Valid()` только непустые команды.

`DMCommand.IsControl()` — канонический предикат, отделяющий data DM от
control DM. Send и receive ветви разветвляются именно по нему, не по
сравнению строк.

`domain.FileActionAnnounce` удаляется: announce — это DM-канал, не
file-command-канал. Существующие call-сайты переключаются на
`DMCommandFileAnnounce`.

### Полезные нагрузки

```
package domain

type MessageDeletePayload struct {
    TargetID MessageID `json:"target_id"`
}

type MessageDeleteStatus string

const (
    MessageDeleteStatusDeleted   MessageDeleteStatus = "deleted"   // строка была и удалена
    MessageDeleteStatusNotFound  MessageDeleteStatus = "not_found" // строки нет; идемпотентный успех
    MessageDeleteStatusDenied    MessageDeleteStatus = "denied"    // флаг не разрешает этому пиру
    MessageDeleteStatusImmutable MessageDeleteStatus = "immutable" // флаг запрещает удаление в принципе
    MessageDeleteStatusError     MessageDeleteStatus = "error"     // получатель не смог решить; НЕ терминальный
)

type MessageDeleteAckPayload struct {
    TargetID MessageID           `json:"target_id"`
    Status   MessageDeleteStatus `json:"status"`
}
```

Обе нагрузки кодируются JSON в `command_data` зашифрованного plaintext.
`Body` для control DM пустой — control DM едут на отдельном wire-топике
(см. Send / Receive paths), и проверка «body != empty», которую
`DMCrypto.SendDirectMessage` делает для data DM, в control-пути обходится.
Receiver-handler `Body` отбрасывает в любом случае.

`MessageDeleteStatusDeleted` и `MessageDeleteStatusNotFound` — оба
успешные исходы с точки зрения протокола: отправитель прекращает
retry: стороны согласованы, поэтому `handleInboundMessageDeleteAck`
снимает intent. Он же на всякий случай повторяет `chatlog.DeleteByID` +
`OnMessageDeleted` + `evictDeletedMessageFromUI` — локальная строка
ушла ещё в момент запроса, но поздний echo от релея мог вставить её
обратно, а ack — последний момент, когда мы гарантированно смотрим на
этот id.

`Denied` и `Immutable` — терминальные неудачи: повторный вопрос дал бы
тот же ответ, поэтому intent тоже снимается, а статус поднимается в UI.
Откатывать нечего — своей копии у пользователя нет в любом случае. Он
узнаёт лишь то, что пир свою сохранил.

### Авторизация

У каждого сообщения в chatlog есть `protocol.MessageFlag` в
`chatlog.Entry.Flag`:

| Флаг                | Кто вправе удалить по сети                                       |
|---------------------|------------------------------------------------------------------|
| `immutable`         | Никто. `message_delete` отклоняется.                             |
| `sender-delete`     | Только автор сообщения.                                          |
| `any-delete`        | Автор или получатель.                                            |
| `auto-delete-ttl`   | Как `sender-delete` до истечения TTL, далее автоматически.       |
| пусто / неизвестен  | Трактуется как `sender-delete` (текущий дефолт).                 |

Флаг штампует АВТОР в момент отправки, и он едет вместе со строкой —
это собственный ответ автора на вопрос «вправе ли собеседник стереть это
и у себя». `DMCrypto.SendDirectMessage` штампует на исходящих
`any-delete` (`defaultOutgoingMessageFlag`): переписка общая, и
пользователь, удаляющий из неё сообщение, ожидает, что его не станет, а
не что оно исчезнет только с его экрана.
`immutable` — абсолютный край этой шкалы: обещание «эта строка — часть
постоянной записи» (юридические доказательства, tamper-evident логи),
которое не отменяет даже автор.

Флаг не решает ни судьбу локальной копии, ни то, просить ли пира.
Убрать строку из своей базы можно всегда и при любом статусе доставки, а
просить пира стоит всегда: флаг — это то, что смотрит ОН, когда запрос
придёт, и его ответ возвращается ack-ом, который пользователь видит.
Решать за него, что он откажет, — значит просто спрятать запрос:
сообщение уходит с экрана, ничего не встаёт в очередь и об оставшейся
копии не сказано ничего.

Получатель применяет правило при входящем `message_delete`:

1. Найти `M = chatlog.Get(target_id)`. Если нет — идемпотентный no-op и
   ответить `message_delete_ack { status: "not_found" }`; chatlog и
   file-transfer cleanup не запускаются.
2. Прочитать `M.Flag`.
3. Если `immutable` — ответить ack `immutable` с warn-логом.
4. Если `sender-delete` (или пусто) — требуется `from == M.Sender`.
   Иначе ответить ack `denied`.
5. Если `any-delete` — требуется `from == M.Sender || from == M.Recipient`.
   Иначе ответить ack `denied`.
6. Если `auto-delete-ttl` — как `sender-delete`; сам TTL применяется
   независимо служебной задачей chatlog.

`from` — это проверенный отправитель из DM-конверта (после проверки
подписи), не самопровозглашаемое поле внутри plaintext.

Локальный отправитель (UI) выполняет ту же проверку перед отправкой
`message_delete`: `immutable` отклоняется до любого сетевого вызова, а
строка, до чужой копии которой флаг не даёт дотянуться, удаляется
локально без него.

### Send-путь (control DM, sender-сторона)

Control DM не должен попасть в chatlog отправителя или в его чат-поток
и не должен породить UI-echo. Существующий `DMCrypto.SendDirectMessage`
для этого не подходит: он идёт через узловой Frame `send_message`,
который пишет outbound-строку в chatlog и эмитит
`LocalChangeNewMessage`, чтобы UI-отправителя отрисовал сообщение.
Переиспользование этого пути приведёт к тому, что `message_delete`
появится у отправителя видимой строкой `[delete]`.

Control-путь — отдельная функция и отдельный node Frame:

```
package service

// DMCrypto.SendControlMessage шифрует control DM и отправляет его на
// выделенном control-топике. Не пишет в chatlog, не возвращает echo и
// не инвалидирует UI-чат. Body пустой; Command обязан удовлетворять
// DMCommand.IsControl().
func (d *DMCrypto) SendControlMessage(
    ctx context.Context,
    to domain.PeerIdentity,
    cmd domain.DMCommand,
    payload string,
) (domain.MessageID, error)
```

Внутри он вызывает `LocalRequestFrame` с новым Frame.Type
`send_control_message` и `Topic = "dm-control"`. Узловой обработчик
`send_control_message`:

1. Проверяет, что топик `dm-control` и тело — корректный ciphertext.
2. Отдаёт зашифрованный конверт в mesh routing тем же путём, что и
   `send_message`.
3. **Пропускает** chatlog-INSERT, который делает `send_message`.
4. **Пропускает** публикацию `LocalChangeNewMessage`.
5. Возвращает аналог `message_stored`, чтобы caller знал, что
   wire-handoff удался (это **не** ack от получателя — см.
   «Подтверждение и retry» ниже).

Выделенный топик `dm-control` виден транзитным узлам — так же, как
видны `dm` и `gazeta`. Это сознательная утечка метаданных: транзит
может отличить control-DM от data-DM. Митигация — объёмом: control-DM
редкие, side-channel слабый. Скрытие отличия потребовало бы пускать
control-DM на топике `dm`, что вынудит каждый узел писать каждый
входящий DM в chatlog до классификации — это ломает инвариант
«не хранить».

### Receive-путь (control DM, recipient-сторона)

Control DM переиспользуют ту же точку входа `storeIncomingMessage` и
ветвятся внутри по `msg.Topic == TopicControlDM`. Отдельной функции
`storeIncomingControlMessage` нет — расхождения выражены гейтами
внутри общего пути, чтобы locking-дисциплина (`knowledgeMu` →
`gossipMu` sequential, см. `docs/locking.md`) осталась идентичной
data-DM и ни одного нового cross-domain edge не появилось.
Контрол-специфичное поведение:

1. `dispatchNetworkFrame` доставляет входящий frame в
   `handleInboundPushMessage` / `handleRelayMessage` ровно как для
   data DM. Non-DM verifier гейт exempts и `"dm"`, и
   `TopicControlDM` (`protocol.IsDMTopic`), поэтому единственный гейт
   аутентичности — per-message подпись в `storeIncomingMessage`
   (`VerifyEnvelope`).
2. Внутри `storeIncomingMessage` topic-ветка реализует три
   расхождения с data-DM:
   - chatlog `messageStore.StoreMessage` пропускается;
   - `s.topics[msg.Topic] = append(...)` пропускается (control
     envelopes никогда не попадают в `s.topics["dm-control"]` —
     `retryableRelayMessages` не накапливает мёртвое состояние);
   - блок `LocalChangeNewMessage` / `emitLocalChange` заменён на
     публикацию `LocalChangeNewControlMessage` на
     `ebus.TopicMessageControl`, и эта публикация срабатывает
     **только** если `msg.Recipient == s.identity.Address` —
     отправитель не получает свой outbound control DM как inbound.
3. `DMCrypto.DecryptIncomingControlMessage` (параллель
   `DecryptIncomingMessage`) расшифровывает конверт и возвращает
   `(domain.DMCommand, commandData string, sender, ok)`. Если
   внутренняя команда не `IsControl()`, событие отбрасывается —
   это закрывает дыру, через которую пир мог бы попытаться
   протолкнуть data-команду через control-провод.
4. `DMRouter` подписан на `ebus.TopicMessageControl` и
   диспетчеризует по `DMCommand`:
   - `message_delete` → `handleInboundMessageDelete`
   - `message_delete_ack` → `handleInboundMessageDeleteAck`
5. Неизвестные команды логируются на debug и отбрасываются.

Chatlog **не** трогается на входящем control-пути до тех пор, пока
ветка авторизации в `handleInboundMessageDelete` не вызовет
`Store.DeleteByID(target_id)`. Никакой переходной строки, никакого
переходного события, никакого мигания UI.

Transit-only relay (control DM проходит через узел, но recipient — не
мы, и ни прямого peer, ни table route, ни gossip-target нет) уходит в
fallback в `handleRelayMessage` и возвращает пустой статус `""`
upstream — data-DM fallback `"stored"` для control DM **не**
выбирается, потому что store-операция для control — no-op, а ack
"stored" сделал бы вид, что relay удался. Delete intent на стороне
отправителя обработает это как промах и переотправит.

### Подтверждение и retry

Control DM — ненадёжная wire-отправка: транзит может потерять, пир
может быть оффлайн, его процесс может упасть между приёмом и
обработкой. Поэтому отправитель держит durable **delete intent** на
каждый незакрытый запрос и переотправляет его, пока не придёт
`message_delete_ack` (любой терминальный статус).

```
type DeleteIntent struct {
    MessageID     domain.MessageID
    Peer          domain.PeerIdentity
    CreatedAt     time.Time // первичный запрос пользователя; отсчёт TTL
    NextAttemptAt time.Time // сдвигают только реально ушедшие попытки
    Attempts      int
}
```

Полная политика — когда запрос отправляется, во что обходится
недостижимый пир (ни во что), backoff 30 с→1 ч и бюджет попыток —
описана ниже в разделе «Плановое удаление». Здесь стоит назвать два
свойства, потому что именно их читатель прежнего in-memory дизайна
ожидал бы иными:

- Intent — строка в общей state-базе, поэтому перезапуск процесса
  продолжает retry ровно с того же места. Терять в памяти нечего.
- Локальная строка исчезает ещё до первой отправки, поэтому
  неподтверждённый запрос никогда не оставляет собственный тред
  пользователя расходящимся с его интентом. Истёкший intent означает
  лишь то, что копия *пира* может ещё существовать; сообщает об этом
  `TopicMessageDeleteCompleted` с `Abandoned=true`.

Получатель полностью идемпотентен: повторный `message_delete` после
того, как строка уже удалена, выдаёт тот же ack `not_found`, что и
первый. Отправитель трактует `not_found` как успех и снимает intent.
Stale-ack (для `target_id` без intent-а) тихо отбрасываются, а ack, чей
envelope sender не совпадает с пиром из intent-а, не решает ничего.

### Идемпотентность

`chatlog.Store.DeleteByID` возвращает `(false, nil)` если строки нет.
Control-handler маппит это в `MessageDeleteStatusNotFound` и шлёт
соответствующий ack. Пир, повторяющий `message_delete` потому что не
увидел предыдущий ack, получит ack снова; в провод никогда не уходит
ошибка.

### Cleanup-хуки

После успешного `chatlog.Store.DeleteByID(M)` control-handler вызывает
generic-цепочку cleanup. Сейчас один хук:

- `filetransfer.Manager.CleanupTransferByMessageID(domain.FileID(M.ID))` —
  если `M` был `file_announce`, удаляет соответствующий sender- или
  receiver-mapping, освобождает ref на блоб в `transmit/` (sender) и
  удаляет partial/completed в директории download (receiver).
  Идемпотентен: no-op если mapping-а нет.

  Снятие mapping-а и стирание байтов — две разные записи, и долговечной
  была только первая: неудавшийся unlink оставлял содержимое удалённого
  сообщения на диске, и больше ничто не знало, что его надо искать. Теперь
  то, что осталось стереть, записывается как НАМЕРЕНИЕ ОЧИСТКИ в том же
  сохранённом состоянии, которое снимает mapping, снимается только когда
  названные им файлы действительно исчезли, и повторяется тиком
  обслуживания трансферов — в том числе после перезапуска, а этот случай
  ретрай в памяти не покрывает в принципе.

  Порядок в одном предложении: ничто не удаляется с диска, пока на диске
  нет записи о том, что должно быть удалено. Упавший persist оставляет и
  снятый mapping, и намерение в памяти и не трогает ни одного файла; тик
  обслуживания повторяет запись прежде, чем пробовать снова.

  Счётчик ссылок уменьшается в ТОМ ЖЕ захвате, который снимает mapping
  (`FileStore.DropRef`, только память), — тогда упавший persist не
  оставляет ничего рассогласованного: перезапуск пересобирает таблицу
  ссылок из восстановленных mapping-ов. Стирает
  `FileStore.PurgeUnreferenced`, который не трогает счётчик и потому
  безопасен для повторения; блоб, на который ссылается другое сообщение,
  остаётся на месте, и намерение считается выполненным — содержимое
  должно другому.

  Повторы разрежены (`cleanupRetryDelay`, от одного тика с удвоением до
  часа): некоторые препятствия не исчезают — read-only том, пропавшее
  устройство, — и намерение не бросают, его лишь спрашивают реже. Проход
  закрывает все намерения ОДНОЙ записью файла mapping-ов, потому что файл
  всё равно переписывается целиком.

Будущие DM-типы могут регистрировать дополнительные cleanup-callback-и в
эту цепочку; порядок неважен, потому что каждый callback скоупится в свой
домен.

### Маршруты удаления (UI)

Удаление всегда убирает локальную копию сразу — достижим пир или нет.
Хранить сообщение, которое пользователь попросил уничтожить, только
потому что кто-то оффлайн, — ровно та угроза, ради которой функция и
делалась. Различается лишь то, что нужно сделать по пути.

`domain.MessageDeleteContext` классифицирует запрос по трём фактам — кто
автор строки, даёт ли флаг дотянуться до копии пира и подтвердил ли
получатель доставку (receipt `delivered` или `seen`), а
`DMRouter.SendMessageDelete` возвращает выбранный маршрут. Достижимость
пира намеренно **не** входит в классификацию: она решает, *когда* пира
попросят, — этим владеет планировщик, — но никогда не решает, *исчезнет
ли* локальная копия.

| Направление | Подтверждено пиром | Доказано, что не ушло | Маршрут     | Локальная строка | Сторона пира |
|-------------|--------------------|-----------------------|-------------|------------------|--------------|
| входящее    | —                  | —                     | `scheduled` | удалена          | запланировано |
| исходящее   | нет                | да                    | `recalled`  | удалена          | ничего не просим |
| исходящее   | нет                | нет                   | `withdraw`  | удалена          | доставка отменена, затем запланировано |
| исходящее   | да                 | —                     | `scheduled` | удалена          | запланировано |

- **`scheduled`** (входящая строка): локальная копия уходит, а автора
  просим удалить свою. Своего в полёте ничего не было, поэтому и
  отменять нечего.

  Флаг строки НЕ решает, спрашивать ли: это ответ автора, и приходит он
  его ack-ом (`deleted`, если он выполнил просьбу, `denied`, если
  оставил сообщение за собой). Трактовка сохранённого флага как «даже не
  спрашивай» и была багом, на который наткнулся пользователь: каждое
  сообщение, полученное до смены дефолта, несёт `sender-delete`, как и
  каждое сообщение от пира на старой сборке, — поэтому удаление такого
  просто исчезало с экрана: ни очереди, ни индикатора, ни слова об
  оставшейся копии.
- **`recalled`** (исходящая, не подтверждена, и отмена доказала, что
  конверт ни разу не попал на провод): сообщения не видел никто, поэтому
  ничего не планируется, а терминальный исход публикуется сразу. Просить
  пира удалить сообщение, которого он не получал, — значит сообщить ему,
  что оно было.
- **`withdraw`** (исходящая, не подтверждена, отправка не исключена):
  доставка может всё ещё лежать в очередях нашего собственного узла,
  поэтому она отменяется **первой** (см. ниже); только затем удаляется
  строка и планируется удаление у пира. План сохраняется, потому что
  копия могла уйти между отправкой и отменой, а `not_found` — дешёвый
  ответ для случая, когда не уходила.
- **`scheduled`** (исходящая, подтверждена): отменять нечего. Строка
  удаляется, удаление у пира планируется.
- **`!found`** (строки уже нет — удалена раньше, истёк TTL):
  обрабатывается как исходящая с peer-ом от вызывающего, чтобы повторно
  выданное удаление всё равно свело обе стороны.

Строка с флагом `immutable` отклоняется сразу, без единой мутации.

### Плановое удаление

Сторона пира — это durable intent, а не in-memory retry: одна строка в
`message_delete_intents` (миграция 0005) с identity пира, id сообщения и
служебными полями планировщика — без тела, без отправителя, без
исходной метки времени. Альтернатива «затереть тело в самой строке
сообщения» отвергнута: она оставляет надгробие прямо в переписке для
всякого, кто откроет базу, — то есть ровно то, что удаление и должно
устранить.

Жизненный цикл:

1. `SendMessageDelete` удаляет локальную строку, записывает intent и
   ставит отказ по этому id — **всё одной транзакцией**
   (`chatlog.DeleteWithIntent`; чисто локальные маршруты берут
   `DeleteMessageWithTombstone` — тот же коммит без intent). Это один
   инвариант с трёх сторон: копии пользователя нет, кто-то всё ещё
   должен нам копию пира, и повторная доставка того же конверта будет
   отклонена, а не вставлена заново. Раздельные коммиты оставляют окна,
   в которых копия уничтожена, а попросить пира уже некому, — либо
   строки нет, а отказа по ней нет тоже, и ближайший relay-retry
   возвращает сообщение. Если пир достижим, запрос уходит сразу — это
   лишь оптимизация относительно ближайшего свипа.
2. `deleteRetryLoop` каждые 5 с (`deleteRetryTickPeriod`) обходит
   наступившие intent-ы. Для каждого:
   - `deleteIntentGiveUpAttempts` (720) сделанных и неотвеченных
     отправок — списывается с warn-логом и публикацией
     `TopicMessageDeleteCompleted` с `Abandoned=true`. Бюджет тратится
     в ПОПЫТКАХ, а не в днях: календарный дедлайн тикает, пока пир
     недостижим, — то есть ровно в тот период, ради переживания
     которого durable intent и существует, — и сдаётся на том самом
     случае, ради которого делалась функция, рапортуя «abandoned» о
     том, кого никто не смог спросить. Попытки начисляются только
     когда пира было у кого спросить, а backoff упирается в час,
     поэтому бюджет — это примерно месяц игнорирования. Цена: запрос к
     контакту, который никогда не вернётся, хранится бессрочно — одна
     строка на удаление, адресованное отсутствующей identity, и уходит
     вместе с остальной его историей при удалении контакта. Повторная
     выдача бюджет не пополняет — и попытки, и `created_at`
     переживают её, — потому что бюджет, который сбрасывается кликом,
     пользователь может сделать бесконечным не желая того;
   - пир недостижим — **паркуется на `deleteIntentHoldInterval` (30 с),
     ничего не списывая**. Ни попытки, ни backoff-а. Парковка — не
     задержка, а средство честности: наступившие intent-ы читаются
     старейшими первыми под лимитом, и куча таких к одному отсутствующему
     контакту в голове очереди заблокировала бы удаления всем остальным.
     Подписка на `peer.connected` снимает парковку в момент возвращения
     пира, поэтому интервал — лишь потолок для пира, который стал
     маршрутизируемым, но к нам не подключался. Припаркованные строки
     помечены (`held`), и кик двигает ТОЛЬКО их: intent,
     ждущий backoff после реально ушедшей попытки, не припаркован, и
     сброс его срока выдавал бы пиру с мёртвым приложением, но живым
     транспортом, по запросу на каждый хендшейк вместо экспоненциального
     расписания;
   - пир уже получил `deleteIntentPerPeerPerSweep` (4) запроса за этот
     свип — паркуется до следующего тика, тоже без списания. Массовое
     удаление оставляет сотни наступивших intent-ов, и выпустить их в
     одного пира со скоростью чтения свипа — это то, на что ответит его
     rate limiter для control DM;
   - иначе — отправка, затем списывается попытка и назначается следующий
     срок по `deleteIntentBackoff` (30 с с удвоением до потолка в 1 ч).
     Неудачная отправка тоже списывает попытку: попытка — это одна
     реально сделанная узлом отправка, а неудача — ровно тот случай,
     ради которого backoff и нужен.
3. `handleInboundMessageDeleteAck` закрывает intent. Его снимает **любой**
   терминальный статус: `deleted` / `not_found` — потому что пир теперь
   согласован с нами, `denied` / `immutable` — потому что повторный
   вопрос ответа не изменит. Статус публикуется, чтобы пользователь
   узнал: его копии нет, а копия пира осталась. Исключение — `error`:
   intent сохраняется, расписание остаётся ровно таким, каким было, и
   не публикуется ничего, потому что ничего не завершилось. Попытка на
   ack НЕ списывается: отправка, которая его вызвала, уже списала свою
   и назначила следующий срок, поэтому второе списание считало бы один
   обмен дважды — backoff рос бы на каждый round-trip, а бюджет
   сдачи выгорал бы вдвое быстрее. Ack — это ответ на попытку, а не
   ещё одна попытка.

Поскольку intent живёт в общей state-базе, перезапуск продолжает ровно с
того же места: свип читает те же строки, а собственного состояния в
памяти у планировщика нет.

Ограничения: один свип обрабатывает не более `deleteIntentSweepLimit`
(64) intent-ов, поэтому большой backlog не монополизирует горутину
планировщика и control-путь за ней.

Сколько строка может лежать для контакта, который не возвращается, не
ограничено ничем — и это осознанно: бюджет тратится в попытках, поэтому
просто отсутствующий пир его не тратит. Цена — одна строка на удаление,
адресованное отсутствующей identity, и уходит она вместе с остальной его
историей при удалении контакта (`DeleteByPeer` забирает все таблицы,
называющие этого пира). Цена в покое — запись парковки за свип, поэтому
парковка батчится в один statement, а её интервал измеряется минутами, а
не секундами: неограниченный припаркованный набор превращает построчную
парковку в постоянный пол по записям на устройстве, которому надо спать.

Удаление контакта отменяет запланированные, но не подтверждённые
удаления у пира: их id — последние строки, называющие стёртую переписку,
и переотправка запросов по ней несла бы ровно ту метаинформацию, ради
которой удаление и делалось. Попросить обе стороны забыть переписку,
сохранив контакт, — это массовая очистка.

**Видимость.** Строка исчезает в момент клика, поэтому вешать
индикатор на само сообщение больше не на что. Счётчик несёт заголовок
переписки («N ждут удаления у собеседника», из
`DeleteIntentCountsByPeer`) — это единственная долгоживущая обратная
связь для запроса, отданного оффлайн-пиру; строка статуса по своей
природе временная и эту роль выполнять не может.

Немедленные исходы несут свой маршрут (`MessageDeleteOutcome.Route`),
чтобы строка статуса их различала: `recalled` сообщает, что сообщение не
успело уйти и пир его не получал, а `local` и более поздние ack читаются
как обычное удаление. Без этого два чата отвечают на один и тот же клик
одинаковой надписью при разных исходах, и пользователь не может понять,
что именно произошло.

### Отмена доставки, которая ещё наша

DM, который получатель ни разу не подтвердил, может лежать в состоянии
доставки нашего собственного узла, а не в сети: релеи только форвардят,
и ничто в mesh не хранит пользовательское сообщение для оффлайн-получателя
(`docs/protocol/relay.md` INV-3). Поэтому удаление такого сообщения
обязано остановить доставку — иначе пиру вручат сообщение, которое
пользователь уже уничтожил.

`node.Service.CancelOutgoingDelivery` очищает всё, откуда конверт ещё
может уйти в сеть, одной кросс-доменной секцией (`docs/locking.md`):
store-and-forward backlog (`s.topics`), очереди кадров по пирам,
sender-owned end-to-end retry (`awaitingDelivered`), тень relay-retry и
запись outbound-статуса. Сообщение, автором которого узел не является,
отклоняется — локальный вызывающий не может вычистить транзитный конверт,
угадав id.

Порядок принципиален: отмена выполняется **до** удаления локальной
строки, чтобы доставка не вручила пиру сообщение, которое пользователь
уже уничтожил. Но неудача отмены удаление НЕ останавливает: «до узла не
достучаться, чтобы отменить» — это тот же класс сбоя, вокруг которого
пользователь и удаляет, и держать его копию в заложниках у этого сбоя —
ровно то поведение, которое фича устраняет. Запрос к пиру при этом
остаётся запланированным, поэтому ушедшее сообщение всё равно будет
отозвано.

Отмена гарантирует *отсутствие новых попыток доставки*, а не «пир ничего
не видел» — если только она прямо этого не говорит. `never_emitted`
возвращается лишь тогда, когда sender-owned retry-запись ещё была на
месте и ни один кадр с этим конвертом не покидал узел
(`deliveryRetryEntry.Emitted`). Именно это утверждение повышает маршрут
с `withdraw` до `recalled` и полностью снимает запрос к пиру.

`Emitted` монотонен, но не силами одного retry-движка — именно на этой
дыре и написан данный контракт. У эмиссии ОДНА точка учёта,
`noteOwnEnvelopeEmitted`, и её вызывает каждый путь, способный
показать пиру наш конверт:

- живой push при сохранении и на каждом retry-тике;
- **backlog-replay при авторизации** (`pushBacklogToSubscriber`), который
  отдаёт `s.topics["dm"]` получателю, подключившемуся К НАМ. Запись в
  backlog НЕ гейтится reachability-hold-ом, поэтому сообщение, которое
  планировщик всё ещё удерживает, отдаётся целиком в момент подключения
  получателя — без попытки, без квитанции и без чего-либо, что заметил
  бы retry-движок. Пропуск этого пути означал, что удаление могло уйти
  по маршруту `recalled` для сообщения, которое пир уже забрал: intent
  не создан, копия у него навсегда, повторить нечем.

Gossip и relay собственного вызова не требуют: они выполняются только из
origin-отправки и retry-тика, которые сами учитывают попытку. Отметка
ставится ДО записи, поэтому ответ смещён в сторону «у пира, возможно,
есть»: излишнее «да» стоит одного control DM, на который пир ответит
`not_found`, а ошибочное «нет» — тихий и невосстановимый отказ.

Вне этого утверждения уже начатая отправка находится на проводе, а пир,
получивший сообщение раньше (с потерянной или неотправленной
квитанцией), сохранит копию — поэтому `withdraw` планирует.

Каждый удалённый id ближайший час отклоняется — включая recovery-путь,
где строки уже нет, — поэтому поздний echo конверта из in-flight буфера
какого-нибудь релея будет удалён повторно, а не создаст заново строку,
которой удаление и должно было не оставить. Отказ живёт в той же строке
`message_delete_intents`, что и долг пира (`refuse_until`), и переживает
его: ack означает, что пир не сохранит сообщение, а не что устаревшая
копия больше не может прийти.

Отказы читаются один раз на старте в память, и входящий store спрашивает
именно память, а не базу, на каждом сообщении — и только для DM-трафика.
Любой отказ называет строку переписки, а строки переписки — это `dm`,
поэтому для прочих топиков гейт мог бы ответить лишь «не отказано» или «не
знаю». Второй ответ там вреден: нечитаемый набор отказов задержал бы всё,
что узел вообще сохраняет, а у широковещательного топика нет sender-owned
retry, так что отсрочка означает не «отправитель повторит», а обычную
потерю. Топик приходит с провода, поэтому проверка заодно не даёт пиру
поставить наш приём в зависимость от таблицы, к его сообщениям отношения
не имеющей.

Гейт намеренно накрывает и ИСХОДЯЩИЕ DM: id может прийти снаружи
(файловый анонс, повтор), поэтому отправка иначе способна воссоздать
только что удалённое сообщение. Плата за это — отказ в отправке, пока
набор отказов нечитаем, и отказ обязан об этом сказать: код ответа
доезжает наружу завёрнутым в `protocol.ErrStoreDeferred`, и UI читает его
как «не сейчас», а не как «unexpected send reply», в который прежде
схлопывался любой отказ. Транзиентность — единственное, что здесь нужно
знать вызывающему, и ровно её старая формулировка теряла.

ФОРМУЛИРОВКА принадлежит UI, а не сервису: ошибка публикуется в
`TopicMessageSendFailed`, desktop-подписчик узнаёт сентинел сквозь
обёртку и ставит `status.send_deferred` из каталога — на языке
пользователя, как и любой другой статус этой фичи. За сервисом остаётся
общая англоязычная строка-фолбэк для рантаймов без UI, и собственных
фраз он не сочиняет. Если стартовое чтение
ПРОВАЛИЛОСЬ, набор помечается незагруженным, а не пустым: промах по
памяти тогда ничего не доказывает, и входящий store отвечает
`StoreDeferred`, вместо того чтобы сохранять наугад. Это НЕ ошибка записи:
конверт не попадает ни в runtime-backlog, ни в отметку дедупликации,
квитанция о доставке не отправляется, фрейм не подтверждается — сообщение
остаётся у ОТПРАВИТЕЛЯ и будет прислано снова. Ответ «не удалось» вместо
этого оставил бы его в памяти узла и всё равно подтвердил бы приём, из-за
чего отправитель перестал бы повторять сообщение, которого нет ни на одном
диске, — а перезапуск потерял бы его. Догадка
«не отклонено» позволила бы реплею заново создать строку, которую
пользователь удалил, и никакая последующая перезагрузка отказов её уже не
удалит. Попытки перезагрузки разрежены (`wipeTombstoneReloadFloor`):
заклинившая база отвечает на каждую только после своего busy-таймаута, а
reaper всё равно ретраит своим тиком; троттлинг ограничивает то, что
входящий путь ПЛАТИТ, и никогда — то, что он ЗАКЛЮЧАЕТ.

**На диске.** Удаление, которое убирает строку только из логического
представления SQLite, защищает от чтения базы через SQL и больше ни от
чего. Поэтому state-база работает с включённым `secure_delete`
(освобождённые страницы перезаписываются, см. [storage.md](storage.md)),
а каждое удаление после коммита делает `wal_checkpoint(TRUNCATE)`: в
режиме WAL само обнуление — это тоже фрейм лога, и исходные байты живут
в файле `-wal` до чекпойнта. Чекпойнт best-effort: busy — это не провал
удаления, автоматический чекпойнт всё равно придёт.

**Каждое** — это намеренно обе стороны: наши собственные удаления
(`removeLocalMessage`, локальная очистка переписки), удаления, которые мы
выполняем по просьбе пира (`applyInboundDelete`,
`sweepInboundDeleteScope`), и TTL-свип (`DeleteExpired`). Удаление на
стороне получателя — это ровно то, ради доставки чего весь протокол и
существует; убирать свои страницы сразу, а его — когда лог наполнится,
значит поставить более слабую гарантию именно туда, где обещана более
сильная.

**Остаточные следы.** `DeleteByID` тем же батчем убирает per-message
строки, которые другие репозитории держат под тем же id: `seen_ack`,
`delivery_failed` (миграция 0003) и resend-intent-ы миграции 0004.
Каждая из них — долговечная запись о том, что сообщение с таким id
существовало и как прошла его доставка; оставить их — значит сохранить
ровно ту метаинформацию, ради которой удаляли, и заново засеять
retry-планировщики id-шниками, которые больше ни во что не
разрешаются. Per-PEER состояние (`decrypt_recovery_jobs`,
`peer_established`, `decrypt_recovery_cycles`) описывает переписку, а не
сообщение, и не трогается.

### Массовая очистка («Удалить чат и попросить собеседника»)

Очистка переписки — это N удалений сообщений и ничего больше. На проводе
нет отдельной массовой команды, нет собственного запроса, собственного
планировщика и собственного подтверждения: «удали этот тред» и «удали это
сообщение» отличаются только количеством id, и записать это прямо в
данных — то, что убрало весь параллельный аппарат, который массовой форме
раньше требовался: вторую таблицу заявок, таблицу наборов строк, таблицу
ответов, замороженный набор кандидатов у получателя, набор survivors, кэш
зафиксированных ответов и границу по времени для строк, которые запрос не
мог перечислить.

Следствие — не только меньше кода. Запрос точен (id, а не часы, поэтому
пир с отстающими часами не потеряет сообщение, написанное после очистки);
он идемпотентен (повторная очистка перезаписывает те же id); ему не нужен
лимит размера (каждый id едет своим control DM); а частично доставленная
очистка — это просто те intent-ы, которые ещё не закрылись, и видны они в
том же счётчике «N ждут удаления у собеседника», что и любое другое
удаление.

`CompleteConversationDelete` под поднятым барьером и после слива
in-flight отправок:

1. читает id, которые заберёт очистка (`ConversationCandidateIDs` —
   каждая non-immutable строка треда), и помечает их против позднего echo
   ДО того, как они исчезнут: внутри транзакции нет момента, в который на
   них можно среагировать;
2. ЗАМОРАЖИВАЕТ доставки узла ровно по этим id
   (`FreezeOutgoingDeliveriesTo`): это останавливает все пути, способные
   положить их на провод, и возвращает то, что узел о них знает.
   Заморозка, а не отмена, потому что её можно отыграть — см. ниже;
3. удаляет ровно эти строки, пишет ПО ОДНОМУ DELETE-INTENT НА СООБЩЕНИЕ
   для тех, которые пир может держать, и ставит отказ по каждому id —
   **одной транзакцией** (`chatlog.DeleteConversationWithIntents`). Либо
   переписки нет И кто-то обязан спросить пира по каждому сообщению, либо
   не произошло ничего и пользователь может нажать снова; наполовину
   применённая очистка — единственный исход, которого он не увидит.
   Cleanup file-transfer и вытеснение из UI идут ПОСЛЕ коммита, по тем
   id, которые транзакция действительно удалила;
4. заканчивает заморозку. При коммите это настоящий отзыв
   (`CancelOutgoingDeliveriesTo`, одним проходом, ограниченным забранными
   id), чтобы стоящее в очереди сообщение не ушло к пиру после
   исчезновения треда, — а immutable-строка, пережившая очистку,
   сохраняет свою доставку вместо того, чтобы навсегда застрять в
   «отправляется». При УПАВШЕЙ транзакции это оттаивание: строки на
   месте, сообщения по-прежнему пользовательские, и отмена оставила бы
   его с тредом на экране и сообщениями, которые никогда не дойдут. Ради
   этой обратимости заморозка и существует: отмену не отыграть;
5. опускает барьер — он нужен, чтобы отправка не гонялась с очисткой, а
   держать его до ответа пира значило бы лишить пользователя возможности
   писать в этот чат на всё время его отсутствия.

Отсюда ничего не отправляется. Каждое сообщение треда теперь обычный
delete intent, и владеет им планировщик удалений: он дозирует запросы на
пира, паркует их, пока пира нет, будит в момент подключения и закрывает
каждый его собственным ack. Дозирование здесь важнее, чем где-либо ещё —
очистка длинного треда это много запросов, — и это то самое дозирование,
которое уже есть, а не вторая политика, которая должна с ним совпадать.

Авторство не учитывается при ЛОКАЛЬНОМ удалении: пользователь стирает
свой вид переписки, и это его право для сообщений любой из сторон. Что
позволено запросу у пира — ответ самого пира, приходящий его ack-ом по
каждому сообщению, ровно как при одиночном удалении. Immutable-строки
остаются с обеих сторон.

Единственное, что этот путь ещё планирует, — сам барьер.
`BeginConversationDelete` защёлкивает его синхронно, чтобы отправка не
проскочила между кликом и очисткой; `CompleteConversationDelete`
отпускает; а резервацию, к которой владелец не вернулся (паника между
этими двумя, застрявшее планирование дольше `convDeleteReservationTTL`),
освобождает свип удалений, публикуя неуспешный исход, — чтобы
пользователь не смотрел на чат, в который не может писать.

**Сообщения, которые не ушли.** О сообщении, которого никто не видел, не
спрашивают: запрос, называющий его, и был бы тем, из чего пир узнал бы о
его существовании, — правило маршрута `recalled` для одиночного удаления,
применённое построчно. Ушло ли сообщение на провод — ответ узла, и
авторитетен он ровно в момент отмены.

Очистка ДВУХФАЗНА относительно движка доставки, потому что иначе на
вопрос нельзя ответить атомарно. Классифицировать, потом удалить —
сообщение успеет уйти между этими шагами: строка, прочитанная как «не
уходило», уже у собеседника к моменту уничтожения, и запрос по ней не
пишется никогда. Удалить, потом классифицировать — единственным
свидетелем остаётся память узла, которая не переживает перезапуск и
опустошается, когда у ретрая кончаются попытки.

Поэтому очистка сначала ЗАМОРАЖИВАЕТ (`freeze_conversation_delivery`).
Заморозка останавливает все пути, способные положить названные сообщения
на провод, — тик ретраев пропускает их, не тратя попытку, backlog-реплей
их придерживает — и возвращает то, что узел знает о них в этот момент.
Пока заморозка держит, ничто не двигается, поэтому транзакция читает
отметку `never_emitted` (docs/chatlog.md) с каждой удаляемой строки,
объединяет её с ответом узла и пишет запрос только для сообщений, которые
собеседник действительно может держать. Удаление и классификация — один
факт, как удаление и его отказ.

Заморозка, а не отмена, потому что отмену не отыграть: если бы транзакция
затем упала, у пользователя остались бы сообщения на экране, которые уже
никто никогда не отправит. Заморозка заканчивается одним из двух способов
— отмена окончательно отзывает доставки при коммите, либо оттаивание
возвращает их при откате. Отмена, УПАВШАЯ после коммита, оставляет их
замороженными — это корректно, строк больше нет и отправлять их нельзя, —
и в ДОЛГУ: отзыв идемпотентен, поэтому свип удалений повторяет его до
успеха. До тех пор процесс держит payload удалённой переписки, и повторная
очистка не поможет — называть в chatlog уже нечего.

Удаление одного сообщения берёт ту же заморозку на свой единственный id и
по той же причине: оно классифицирует по отметке строки, а она ничего не
значит, пока сообщение может уйти. TTL у заморозки нет, поэтому её
завершает КАЖДЫЙ выход — включая ранние: immutable-сообщение или строку,
которую не удалось прочитать, — потому что заморозка, пережившая своё
решение, останавливает отправку этого сообщения на всю жизнь процесса, и
снять её уже нечем.

Заморозка же чинит и порядок. Локальное удаление теперь коммитится
ПЕРВЫМ, отзыв идёт после: отзыв необратим, и выполненный первым он
оставляет — при упавшей затем транзакции — строку на экране в состоянии
«отправляется», у которой все delivery-хуки уже уничтожены. Пока идёт
транзакция, сообщение уйти не может, поэтому удалять первым ничего не
стоит, а отказ остаётся исправимым.

Отзыв, упавший после исчезновения строки, НЕ оттаивается — это сообщение
больше не пользовательское — и не выбрасывается. Он становится ДОЛГОМ:
свип удалений повторяет его, потому что до успеха процесс держит payload
удалённого сообщения, а назвать этот id уже некому — строки нет.

Заморозка, которую вообще не удалось взять, — единственный случай, когда
классификация не делается. Отметки на строках что-то значат лишь пока
никто не может отправить сообщение за спиной транзакции, поэтому вместо
классификации запрос получает каждое сообщение в scope: пусть собеседника
спросят про id, которые он не сможет разрешить, чем сообщение будет
удалено здесь, а его копия уйдёт к нему, и отозвать её будет уже нечем.

Это заменило более раннюю схему «припарковать и освободить», где запросы
писались припаркованными, а освобождала их пришедшая позже отмена.
Парковка ставила приватное правило за таймаут: крэш, зависание или ошибка
отмены оставляли запросы неосвобождёнными, и по истечении grace они
уходили как записаны. Такого состояния больше нет: запрос либо существует
и наступил, либо не был написан вовсе.

`recalled` — утверждение, требующее ДОКАЗАТЕЛЬСТВА, и теперь
доказательство переживает перезапуск. Отправка, ПРИДЕРЖАННАЯ из-за
недоступности получателя, ставит в chatlog отметку `never_emitted`
(docs/chatlog.md), а в момент, когда сообщение всё-таки уходит, отметка
снимается — на диске, до записи фрейма, — так что восстановленная запись
наследует утверждение, а не угадывает его.

Отметка — единственное доказательство; её отсутствие не является
обратным утверждением. Всё, чего outbox не знает, читается как
«уходило»: строки от сборок до появления отметки, отметка, потерянная в
крэше, сообщение, отправленное путём, пережившим свою запись в
планировщике. Направление выбрано намеренно: объявленный id стоит
собеседнику одного запроса, на который он ответит `not_found`, а
недоказуемое «не уходило» оставило бы доставленное сообщение у него, и
попросить о нём было бы уже нечем.

**Что обещает очистка.** Тред исчезает здесь, а каждое сообщение
становится запросом, на который собеседник отвечает сам. Под текущим
дефолтным флагом (`any-delete`) он его выполняет; сообщение, которое
СТАРАЯ сборка пометила `sender-delete` и написал он сам, — его право
оставить, поэтому он ответит `denied` и его копия останется.
Подтверждение так и говорит, а не обещает безусловное удаление у обеих
сторон, и исход виден по каждому сообщению, а не спрятан за одним общим
ответом.

**Ограничение поздней доставки (без изменений).** Сообщение, которое пир
ОТПРАВИЛ до получения удалений, но которое было ещё в полёте, придёт
после и в очистку не входит. Набор тумбстоунов закрывает соседний класс —
ТОТ ЖЕ конверт, доставленный повторно после удаления, — но новое
сообщение в полёте остаётся видимым, пока пользователь его не удалит.

### Сетевой поток

```mermaid
sequenceDiagram
    autonumber
    participant A as Алиса desktop
    participant NA as узел Алисы
    participant NB as узел Боба
    participant B as Боб desktop
    A->>A: локально проверяем M.Flag (рано отказываем если запрещено)
    A->>A: cancel_message_delivery (только для неподтверждённых строк)
    A->>A: chatlog.DeleteByID(M) + cleanup + вытеснение из UI
    A->>A: записываем durable DeleteIntent{target=M, peer=B}
    A->>NA: send_control_message{topic="dm-control", Command=message_delete}
    Note over A: отправляем сразу только если B достижим; иначе свип отправит, когда B вернётся
    NA-->>NB: зашифрованный конверт на топике dm-control
    Note over NB,B: НЕТ chatlog INSERT, НЕТ LocalChangeNewMessage
    NB-->>B: LocalChangeNewControlMessage
    B->>B: DMCrypto.DecryptIncomingControlMessage
    B->>B: ищем M, проверяем envelope.From vs M.Sender и M.Flag
    alt разрешено
        B->>B: chatlog.DeleteByID(M)
        B->>B: filetransfer.CleanupByMessageID(M)
        B->>NB: send_control_message{Command=message_delete_ack, status=deleted|not_found}
    else denied / immutable
        B->>NB: send_control_message{Command=message_delete_ack, status=denied|immutable}
    end
    NB-->>NA: зашифрованный ack на топике dm-control
    NA-->>A: LocalChangeNewControlMessage
    alt status deleted или not_found
        A->>A: защитный chatlog.DeleteByID(M) + cleanup + вытеснение из UI
    else status denied или immutable
        Note over A: UI показывает отказ — своя копия у A уже удалена
    end
    A->>A: DMRouter снимает DeleteIntent{target=M}
    Note over A: спросили 720 раз, ответа нет: warn-лог, intent снят, публикуется Abandoned
```

*Диаграмма 1 — Распространение message_delete с control-топиком и ack*

### Правила хранения control DM

Control DM не попадают в chatlog **на обеих сторонах** благодаря
маршрутизации на выделенном топике `dm-control`. Две диверсии:

| Сторона     | Путь data DM                                                     | Диверсия для control DM                                                                |
|-------------|------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| Отправитель | `send_message` → INSERT outbound + `LocalChangeNewMessage`        | `send_control_message` идёт через тот же `storeMessageFrame` / `storeIncomingMessage`, и для `TopicControlDM` пропускает INSERT, пропускает append в `s.topics`, заменяет LocalChange-ветку на recipient-only `LocalChangeNewControlMessage` (на узле отправителя event'а нет) |
| Получатель  | `dispatchNetworkFrame` → `storeIncomingMessage` → INSERT + event | Тот же `storeIncomingMessage`, но для `TopicControlDM` пропускает chatlog INSERT, пропускает append в `s.topics`, и эмитит `LocalChangeNewControlMessage` на `ebus.TopicMessageControl` только когда `msg.Recipient == s.identity.Address` |

Следствия:

1. Control DM никогда не появляется в чат-потоке ни у одной стороны.
2. `LocalChangeNewMessage` для control DM нет, поэтому штатный UI
   message list не инвалидируется их приходом. Bubble удалённой целевой
   строки `M` снимается из живого кэша диалога
   (`ConversationCache.RemoveMessage`) самим delete-путём — обе ветки
   (`SendMessageDelete` на отправителе и `applyInboundDelete` на
   получателе) вызывают `evictDeletedMessageFromUI`, который снимает
   запись из cache, обновляет sidebar preview, и эмитит
   `UIEventMessagesUpdated` + `UIEventSidebarUpdated`. Терминальные
   статусы доставки (`deleted`, `not_found`, `denied`, `immutable`,
   `Abandoned`) сигналятся отдельно через
   `ebus.TopicMessageDeleteCompleted`, чтобы caller-ы / RPC-клиенты
   могли отличить реальное удаление у пира от его отказа.
3. Receipts (`delivered`/`seen`) для control DM не выписываются.
   Надёжность обеспечивается прикладным `message_delete_ack`, который
   несёт семантический статус, недоступный delivery-receipt.
4. UI-пути, фильтрующие сообщения по `Command`, видят только data DM:
   и file-таб, и чат-поток смотрят в chatlog, а chatlog никогда не
   содержал control DM.
5. Control envelopes также не попадают в `node.Service.s.topics[...]`.
   `retryableRelayMessages` (node-level retry loop) читает только
   `s.topics["dm"]`; класть control envelopes в
   `s.topics["dm-control"]` создало бы непрочитанное состояние,
   которое растёт неограниченно и не даёт реального retry — единственный
   путь, реально ретраящий control DM, это application-level delete
   intent на стороне отправителя, который завершается либо по ack от
   пира, либо по TTL. Intent — строка в общей state-базе, поэтому
   рестарт его продолжает (см. §«Плановое удаление»).
   Routing/push fan-out не страдает: `executeGossipTargets` и
   `sendTableDirectedRelay` шлют wire-фреймы на лету, независимо от
   `s.topics`.
6. Node-level `relayRetry` tracker тоже отвергает control DM на
   входе (`trackRelayMessage`). По той же причине, что и #5: retry
   loop читает только `s.topics["dm"]`, и control-запись в
   `relayRetry` была бы мёртвым состоянием, которое жжёт квоту
   `maxRelayRetryEntries` до tombstone TTL.

### Сценарии отказа

| Ситуация                                  | Поведение получателя                                   | Поведение отправителя                                |
|-------------------------------------------|--------------------------------------------------------|------------------------------------------------------|
| Target ID отсутствует в chatlog           | Ack `not_found`.                                       | Трактует `not_found` как успех; снимает intent.       |
| Envelope sender ≠ M.Sender (sender-delete)| Ack `denied`. Warn-лог с envelope sender.              | Показывает отказ в UI и снимает intent — повторный вопрос ответа не изменит. Своей копии у пользователя уже нет. |
| `M.Flag == immutable`                     | Ack `immutable`. Warn-лог.                             | Как `denied`: показываем и снимаем intent.             |
| Невалидный JSON в control-payload         | Отбросить. Debug-лог. Ack не шлём.                     | Intent остаётся наступившим и переотправляется следующим свипом. |
| Невалидная подпись control DM             | Отбросить в `DMCrypto.DecryptIncomingControlMessage`. Ack не шлём. | Intent остаётся наступившим и переотправляется следующим свипом. |
| Cleanup file transfer частично упал       | Логи; ack `deleted` (строки в chatlog уже нет).        | Трактует `deleted` как успех.                        |
| chatlog получателя недоступен / упал lookup или DELETE | Ack `error`. Warn-лог. Строка, если была, остаётся. | Сохраняет intent, списывает попытку, переспрашивает по backoff. Ничего не публикуется — удаление не завершено. |
| Пир оффлайн                               | Ack не приходит.                                       | Свип полностью пропускает intent — попытка не списывается, backoff не растёт — и отправляет в течение одного тика после того, как пир станет достижим. |
| Креш приложения во время in-flight retry  | n/a                                                    | Intent durable; следующий старт продолжает то же расписание. Локальная строка удалена ещё в момент запроса, поэтому на этой стороне ничего не осталось наполовину сделанным. |
| Пир не ответил на все 720 отправок | n/a                                                       | Intent списывается: warn-лог с `target_id` и пиром, `TopicMessageDeleteCompleted` с `Abandoned=true`. Копии пользователя нет; копия пира, возможно, осталась. |
| Пир ни разу не был достижим с момента клика | n/a                                                  | Intent сохраняется и после TTL: спрашивать ещё не начинали, значит и сдаваться не от чего. Уйдёт на первом тике после его возвращения. |

### Замечания по миграции

`chatlog.Entry.Flag` уже существует и заполняется из конверта при
приёме — миграция схемы не нужна. Существующие строки с пустым `Flag`
попадают под дефолт "трактуется как sender-delete" и остаются
удаляемыми оригинальным отправителем. Оператор, желающий более жёсткой
политики, ждёт настройки per-thread default-flag (вне скоупа этой
итерации).

`message_delete` **не** совместим по wire с пирами, которые понимают
только data DM. Control DM используют `Topic == "dm-control"` и frame
`send_control_message`, поэтому старые пиры не декодируют их как обычные
`directmsg.PlainMessage`; они отклонят или отбросят неизвестный topic /
frame. Раскатку нужно гейтить явной peer capability или минимальной
версией протокола. Пока такой capability нет, UI должен откатываться на
local-only deletion для пиров без поддержки.

### План тестирования

- Unit
  - `DMCommand.Valid()` и `IsControl()` корректно разбивают известные и
    неизвестные строки.
  - `MessageDeletePayload` и `MessageDeleteAckPayload` JSON round-trip;
    валидация `target_id` отклоняет некорректный UUID v4.
  - Матрица авторизации: каждая комбинация (flag × envelope sender ×
    M.Sender × M.Recipient) разрешается в один из `deleted`,
    `not_found`, `denied`, `immutable` согласно документу.
- Send-путь
  - `DMCrypto.SendControlMessage` **не** пишет строку в chatlog у
    отправителя и **не** эмитит `LocalChangeNewMessage`.
  - Отдаваемый Frame несёт `Type == "send_control_message"` и
    `Topic == "dm-control"`.
- Receive-путь
  - `storeIncomingMessage` для `Topic == TopicControlDM` пропускает
    chatlog INSERT, пропускает append в `s.topics["dm-control"]`,
    и публикует `LocalChangeNewControlMessage` на
    `ebus.TopicMessageControl` только когда
    `msg.Recipient == s.identity.Address` (отправитель не получает
    event для своего же исходящего control DM).
  - Fallback `handleRelayMessage` без next-hop возвращает `""` для
    `TopicControlDM` вместо data-DM статуса `"stored"` — upstream не
    считает успешным relay, который на самом деле потерял envelope.
  - `DMRouter` диспетчеризует по `DMCommand`; неизвестные команды
    отбрасываются на debug.
- DM router (control-handlers)
  - Входящий `message_delete` от `M.Sender` под `sender-delete`
    удаляет `M`, триггерит cleanup, шлёт ack `deleted`.
  - Локальный `DeleteDM` и авторизованный входящий `message_delete`
    вызывают `evictDeletedMessageFromUI`, который снимает bubble из
    `ConversationCache`, обновляет sidebar preview из chatlog, и
    эмитит `UIEventMessagesUpdated` + `UIEventSidebarUpdated`, чтобы
    активный диалог перерисовался без ручного reload.
  - Терминальные исходы (`deleted`, `not_found`, `denied`,
    `immutable`, `Abandoned=true`) публикуются ровно один раз через
    `ebus.TopicMessageDeleteCompleted`. **Только incoming local-only
    удаления** (пользователь удаляет полученное от пира сообщение)
    публикуют `Status=deleted` сразу и пропускают wire-отправку;
    исходящие удаления и отсутствующие target-ы (`!found`) записывают
    intent и публикуют исход, когда он закрывается.
  - Входящий `message_delete` от `M.Recipient` под `sender-delete`
    отклоняется; `M` остаётся; ack `denied`.
  - Входящий `message_delete` для неизвестного `target_id` даёт ack
    `not_found`.
  - Входящий `message_delete` для `immutable` `M` даёт ack `immutable`.
  - Входящий `message_delete_ack` для несуществующего intent тихо
    отбрасывается (без паники, без шума в логе); ack, чей envelope
    sender не совпадает с пиром intent-а, оставляет intent в плане.
- Планировщик удаления (durable)
  - Intent записывается до первой отправки и переживает переоткрытие
    базы; свип его продолжает.
  - Недостижимый пир пропускается без списаний: попытки и срок не
    меняются, а первый же свип после появления пира отправляет запрос.
  - Достижимому пиру запрос уходит, попытка списывается, следующий
    срок ставится по backoff 30 с→1 ч.
  - Intent старше TTL снимается с задокументированным warn-логом и
    публикацией `TopicMessageDeleteCompleted` с `Abandoned=true`.
- Filetransfer
  - `CleanupTransferByMessageID` снимает sender-mapping, освобождает
    ref, удаляет осиротевший блоб в `transmit/`.
  - То же для receiver-mapping: снимает mapping и
    partial/completed-файлы в download.
  - Идемпотентен: второй вызов без ошибки и паники.
- Integration (стиль `internal/core/node/file_integration.go`)
  - `A` посылает file announce `B`. `A` вызывает `DeleteDM(B, fileID)`.
    Строка `A` и блоб в `transmit/` исчезают ещё до возврата вызова, а
    для `B` записывается delete intent. После control round-trip и
    `message_delete_ack` (`deleted`) intent снимается; у `B` нет записи
    `M` и receiver-mapping; partial у `B` удалён.
  - Denied path: `A` вызывает `DeleteDM(B, fileID)` для строки,
    `MessageFlag` которой не авторизует `A` для пира (искусственно —
    например, Sender подделан в тестовом fixture). После ack `denied`
    локальная строка `A` **остаётся**, исход
    `TopicMessageDeleteCompleted` несёт `Status=denied`,
    `Abandoned=false`.
  - Concurrent: `A` удаляет пока `B` качает. Скачивание у `B` чисто
    отменяется; orphan partial не остаётся; ack `deleted`.
  - Offline-then-online: `B` недоступен, когда `A` удаляет. `A`
    переотправляет; как только `B` подключается, control DM
    долетает и ack замыкает round-trip.

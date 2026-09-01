# Message Delivery Engine

> Companion documents: [messaging.md](messaging.md) (the `send_message` frame and its validation), [delivery.md](delivery.md) (the receipt frames), [relay.md](relay.md) (hop-by-hop forwarding), [realtime.md](realtime.md) (live push and the auth-time backlog).

## Overview

Nothing in the mesh stores user messages for an absent recipient. Relays are forwarding-only (`transit_retention.go`), and that decision is deliberate — a node that kept other people's messages would be a mailbox, with everything a mailbox implies for storage, retention and deniability. The consequence is the whole subject of this document: **delivery is owned by the sender**, from the moment the message is written until the recipient's node confirms it.

Three properties follow, and each answers a failure this engine used to have:

- **A message is never sent into the void.** When the recipient has no route and no live session, the message is HELD on the sender's node rather than blind-gossiped at the network in hope. This is the reachability gate (`CORSA_HOLD_DM_UNTIL_REACHABLE`, on by default).
- **The engine never gives up on its own.** A delivery ends in exactly three ways: the recipient confirms it, its author withdraws it, or its own TTL expires. There is no attempt cap and no age horizon for messages — running out of patience would be the node quietly deciding something the sender is never shown and cannot undo.
- **A returning recipient is served at once.** When someone who was unreachable becomes reachable, the waiting messages have their backoff reset to its first step, so a message that has been ready since yesterday goes out in seconds rather than after another eleven-minute timer.
- **A message that has never been emitted is never overtaken by a newer one.** Per recipient the engine advances one message at a time, oldest first, and the next one leaves when the previous is confirmed. The guarantee is stated in terms of never-emitted messages on purpose — see [Ordering](#ordering) for why that is the line that matters, and for the one exception.

### Message states as the sender sees them

| State | Meaning | Where it lives |
|---|---|---|
| `queued` | Written locally, NOT known to be on the wire — no sink has confirmed taking it | the chatlog row is governed by `on_wire` and carries no stamp — see [Reading the two bits](#reading-the-two-bits) for what a row from an older release says; in memory the entry's hold answers the same question (`message_stored`'s `status`, the `message.new` event) |
| `sent` | On the wire, waiting for the recipient's confirmation | `delivery_status` in chatlog |
| `delivered` | The recipient's node has it | `delivery_status`, set by their `delivered` receipt |
| `seen` | The recipient opened the conversation | `delivery_status`, set by their `seen` receipt |

`queued` is a LOCAL state and never travels between nodes: no peer has anything to say about a message that never reached them. The chatlog persists only three statuses, because on the wire `queued` and `sent` are the same thing — the difference matters to the person who wrote the message, and to them it is the whole difference between "they have it" and "nobody has seen this yet".

```mermaid
stateDiagram-v2
  [*] --> queued: recipient unreachable at send time
  [*] --> sent: recipient reachable — goes out immediately
  queued --> sent: route or session appears, emission succeeds
  sent --> queued: never — emission is monotone
  sent --> delivered: recipient's delivered receipt
  delivered --> seen: recipient's seen receipt
  queued --> withdrawn: the author recalls it
  sent --> withdrawn: the author recalls it
  queued --> expired: its own TTL — auto-delete messages only
  sent --> expired: its own TTL — auto-delete messages only
  seen --> [*]
  delivered --> [*]
  withdrawn --> [*]
  expired --> [*]
```

**Diagram: Sender-side states of one outgoing direct message**

There is no state for "the node gave up". An ordinary DM carries `TTLSeconds=0`, so `expired` applies only to messages explicitly sent to auto-delete; `withdrawn` is `CancelOutgoingDelivery`, which the author asks for. `expired` is a terminal state of the RETRY ENGINE, not of the chatlog row: the row stays at `sent`, and a durable journal (`delivery_failed`) keeps a restart from resurrecting it.

The queued → sent transition is announced to the sender's own client (`ebus.TopicMessageEmitted`), because otherwise nothing would say it: the next event is the recipient's receipt, and a lost receipt would leave the sender believing a message was never sent while their counterpart is reading it.

Three rules keep that announcement honest, and each of them is a place the badge could otherwise outrun the wire:

- It is made by **the sink that accepted the frame**, never by the code that decided to try, and "accepted" means NetCore took it. A session's own `sendCh` is not that boundary: it is an in-process queue whose remainder is discarded when the session closes, so a relay enqueue is not a confirmation — the session's writer loop confirms when `SendTracked` returns `SendOK`, carrying the dispatch on the queue item so several sinks of one attempt charge it once. `confirmEnvelopeOnWire` is the single place any sink turns a write into "sent". It hands the stamp and the announcement to ONE background goroutine, in that order, and the announcement only happens if the stamp LANDED: off the writer loop, because the journal writes through SQLite where contention can park a statement for the whole busy timeout; stamp first and conditionally, because the event moves the conversation cache while a full reload reads the row — an event that runs before the write, or without it, lets the next reload put the badge back to `queued`, and no further event comes because the announcement is claimed once. A batch — the reconnect backlog replays a whole conversation — moves every entry in memory first and then pays for ONE journal write and one announcement.
- It is **not derived from the never-emitted claim**, which is conservative and turns false on the first attempt whether or not it went out. The badge has its own durable bit (`on_wire`) and its own in-memory flag (`deliveryRetryEntry.Announced`), so an attempt that failed to go out spends nothing and a later successful one still tells the sender.
- A local failure is **repaired locally**. Three things can leave a confirmed message half-recorded, and none of them is the network's fault: the journal refusing the stamp, the emission lane declining to queue it because enough stamps are already waiting, and the bus shedding the announcement when a subscriber's 64-slot inbox is full — which a reconnect replaying a whole conversation makes likely, because the announcements arrive as a burst. All three used to be retried only by the next NETWORK send, which put a message the peer had already taken back on the wire, and for a batch did so for the whole batch. `repairLocalDeliveryRecord` redoes only the bookkeeping, and it runs OFF the tick on a single-flight goroutine — it blocks on the journal, and a contended database would otherwise hold up the very messages a returning recipient is waiting for, plus everything else in the sequential bootstrap loop.

Bookkeeping does not get AHEAD of a pre-wire write, and keeping it that way takes a lane, because removing the ordering did not remove the contention. `ClearNeverEmitted` has to land before a frame goes out — including the first frame to a recipient who has just come back after a night offline, which is the same moment a reconnect is stamping the whole replayed conversation. SQLite takes one writer at a time and the connection pool is not clamped, so with no order of our own those two compete in the busy handler, where who goes next is decided by retry timing rather than by need.

`emissionLane` supplies the order, as a queue rather than a lock. Every journal write takes a turn in it, and three rules decide the turns. PRIORITY: a withdrawal is served before any bookkeeping, because bookkeeping records a frame already gone while a withdrawal is what a frame is waiting for. FIFO WITHIN A CLASS: turns are taken in arrival order, including by a writer that has just finished a turn and wants another — a mutex and a condition variable are not a queue, and "the next turn goes to the waiter" was true only by luck while re-entry competed for the same mutex. ONE STATEMENT PER TURN: a turn writes at most `maxIdsPerLaneWrite` ids and a writer with more goes to the BACK for the rest, so a backlog replay confirming a whole conversation cannot hold the lane for its whole call.

Coalescing sits on top: a withdrawal taking its turn absorbs the withdrawals queued behind it, up to one statement's worth, so several senders reconnecting at once cost one write rather than one each. The cap matters as much as the coalescing — an unbounded batch would re-create the defect the third rule removes, by tying a message typed a moment ago to a whole conversation's worth of ids. The price is shared fate: a failed statement fails every caller whose ids were in it, which errs towards withholding the frame and is retried on the next tick.

Stamps are additionally bounded by REFUSAL, which withdrawals cannot be: one the lane will not queue writes nothing and answers like a failed write, and that costs nothing because the repair pass re-derives the debt from state.

Two things are worth stating plainly about what this buys, so the next reader neither under- nor over-trusts it. Measured, the contention is small: with 64 goroutines writing full-conversation stamps in a loop, the worst pre-wire clear on the same database took under 3 ms, because these statements are short and the write lock changes hands quickly. The lane is not there to rescue a routine send. It is there because the cost of losing that race is asymmetric — a clear that runs out of `busy_timeout` WITHHOLDS the user's message — and that tail is reached not by stamps alone but by a stamp queued behind some other subsystem's long write, on a slow disk or a phone. Bounding the queue of waiting stamps is the second, unconditional gain: without it a reconnect backlog puts an unbounded number of goroutines on the database.

What the lane does not promise is a fast write. The database is shared with every other subsystem, so a statement inside the lane can still park on someone else's transaction, and no ordering of ours preempts that. Bounding it would mean clamping the connection pool for the whole database, which would serialise reads as well — a storage-layer decision, not a delivery one. Ordering still helps on top of the lane: the repair pass starts AFTER the tick has dispatched, single-flight, on its own goroutine.

There are THREE kinds of local debt, and only two of them live on the retry entry — which is why the pass cannot read the retry map alone. On the entry: the durable stamp and the announcement. The third is a shed event that OUTLIVES its entry, because a receipt DELETES the entry — if the emitted event and the receipt update are shed in the same burst, nothing is left to repeat and the conversation shows `queued` for a message the peer has already read, until the user reopens it. `publishRetryableLocalChange` keeps what the bus refused in a bounded map keyed by message id, drained by the same pass on the same backoff. Keying by id means a newer status REPLACES an older one, and only in that direction: `sent` → `delivered` → `seen` is a rank, a kept event is never overwritten by a lower one, and a republication that a newer status has already overtaken clears nothing. Without that, a pass working from a snapshot could publish `sent` and then delete a `delivered` that arrived while it was publishing, leaving the badge one step behind for good.

The fourth is not a debt at all any more, and that is what the commit rule bought. Handling a receipt ends three retries: the SENDER's, by deleting the retry entry; this node's suppression of the peer's next copy, by writing the dedup key; and the peer's own, by acking. All three used to be applied before the durable write that makes them true, and each had to be walked back by hand when the write failed — except the entry, which nothing walked back, so a refused write left the row at `sent` with nothing anywhere able to correct it.

A receipt's identity is ONE thing, `receiptIdentity{Recipient, MessageID, Status, Sender}`, and everything that has to decide "is this the same receipt" derives from it: the dedup key, the relay-retry key, the backlog filter, the pending-queue key, and the `ack_delete` frame. That list is the fix. Each of those used to answer the question for itself, and adding the author to one of them left the other four still spelling identity as a recipient+message+status triple — which is not an identity at all, because two peers can make a claim about one message. `ack_delete` was the sharp end: an ack names a receipt the peer holds, and applied to a triple it deleted every receipt that looked like that, the genuine one and its retry entry included. The forgery is refused at the end node, but the ack answering it is honest traffic and the relay is where it lands. `TestReceiptIdentityIsBuiltInOnePlace` and `TestEveryRelayRetryReceiptKeyComesFromTheIdentity` fail the build for an identity or a key assembled anywhere else — after seven hand-written spellings, "review will catch the next one" is not a plan.

The author travels in `Frame.ReceiptSender` and is INSIDE the signed payload (`corsa-ack-delete-v2`), so it cannot be swapped in flight. Which payload is signed follows the frame, not either side's guess: a frame carrying the field signs v2, one without it signs the exact v1 bytes older peers have always produced, and both signer and verifier go through the same function. Emission is gated on the peer advertising `ProtocolVersionReceiptSenderAck` (29) — an older verifier rebuilds the v1 payload and would score a signature it cannot reproduce as forgery, which costs ban points, not a warning. Until the floor reaches 29 an ack may arrive that cannot name its receipt, and then the rule is "delete only if unambiguous": one candidate goes, several are all KEPT. Keeping costs a duplicate push the peer discards; deleting the wrong one costs the delivery. The stamp is a VIEW of the frame for one session, never a rewrite of what is stored: the pending queue holds the full ack, and both the park and the failed-drain re-queue keep it that way. A downgraded copy in the queue would have thrown the author away permanently, and the peer that eventually drains it can be a current one — which then holds an ack that cannot say which receipt it means, keeps both contested receipts, and re-pushes them.

The dedup key is that identity written out: `recipient:messageID:status:sender`, produced by `receiptIdentity.dedupKey` and nowhere else. The sender belongs in it because a receipt is a CLAIM by whoever made it, and two claims about one message from two peers are two different facts. Keyed without the sender they were one, and the consequence was not merely a wrong count: anyone who learned a message id could send a receipt of their own and take the key, after which the real recipient's receipt was silently classed as a duplicate. The end node's sender binding cannot catch that, because on a RELAY nothing is addressed to this node — the genuine receipt is dropped a hop before any binding runs, and the sender waits out a retry interval, again for each repetition. `TestEveryDedupKeyComesFromTheOnePlaceThatSpellsIt` fails the build for a key written out by hand at a new site: the same expression stood at seven of them, and a site that kept the old spelling would insert under a key the eviction path could no longer delete.

The same identity settles the queued frames. A `relay_delivery_receipt` waiting in the pending queue IS a receipt in flight, and clearing those on a triple meant committing one receipt still discarded somebody else's — the last place where confirming the forgery-free path took the genuine one with it. `clearPendingReceiptLocked` takes the identity and compares the whole of it.

`finishReceipt` applies all three AFTER the write, so a refused write commits NOTHING: the sender keeps retrying the message, the recipient answers the re-send with a fresh receipt, and this node gets another attempt. That recovery is local. It does not lean on the peer's receipt queue, which is capped, expires after three minutes and does not survive their restart — an earlier version of this document claimed it did, and that claim was wrong.

"The peer is not told to forget it" is four separate things, each of which ends the receipt's life on their side: the `ack_delete` on an outbound peer session, the `ack_delete` on an inbound connection, the `seen_ack` that stops a reader's DURABLE retry (they journal it, so after their restart nothing re-sends), and the reply to the local `send_delivery_receipt` RPC, on which the client clears the unread mark. All four read `receiptOutcome.ackable`, and `TestEveryReceiptAckConsultsTheOutcome` fails the build for an ack that is not under such a condition — a list of blessed functions would have passed while the guard inside one of them was deleted.

The two debts on the entry are settled separately, because they fail differently. A journal write that succeeded never has to be redone, so the entry memoises it (`Stamped`); an announcement can be shed over and over, so it carries its own `AnnounceAfter` backoff. Keying both on "not announced" meant every shed event dragged another SQLite write with it, every two seconds, for as long as the subscriber stayed wedged. Both halves are taken OLDEST FIRST so a standing backlog drains in the order the user wrote it rather than re-rolling a random sample of the map each pass. The cap bounds one pass; the BACKOFF is what makes the passes converge — an inbox that is already full sheds every new event whatever the batch size, so "16 is less than 64" was never the argument.

Gossip is deliberately not one of the confirming sinks: it hands jobs to a bounded pool that sheds under load, and it is never the only sink for one of our own messages anyway — the reachability gate passed, so there is a route or a subscriber. And "accepted by the writer" is the furthest this node can see either way; an accepted frame can still be lost on the wire, which is what the receipt is for.

`message_known`, the answer to an idempotent re-send, carries the same status as `message_stored` would. A client treats it as success and reads the status off it.

The `queued` state is also carried on the `message.new` event, not derived by the client. Storing a locally-authored DM announces it to this node's own client, and that announcement races the synchronous reply to `send_message`; whichever reaches the conversation cache first decides the badge — so both say the same thing.

## The send path

```mermaid
sequenceDiagram
  autonumber
  participant UI as Desktop UI
  participant DM as DMCrypto / DMRouter
  participant Node as node.Service
  participant Log as chatlog (SQLite)
  participant Net as mesh

  UI->>DM: SendMessage(peer, text)
  DM->>DM: EncryptForParticipants, new MessageID
  DM->>Node: send_message (local RPC frame)
  Node->>Log: StoreMessage — status "sent", born never_emitted
  alt the row cannot be written
    Node-->>DM: error, code = "store-failed"<br/>nothing routed, nothing registered
  else the row is on disk
    Node->>Node: router.Route(envelope)
    Node->>Node: registerAwaitingDelivered — BEFORE any send,<br/>so a confirmation always has somewhere to land
    alt recipient reachable (route or live subscriber)
      Node->>Log: ClearNeverEmitted — BEFORE the frame,<br/>through the emission lane as URGENT
      Node->>Net: push_message / relay_message
      Net-->>Node: a sink accepted it → confirmEnvelopeOnWire
      Node-->>DM: message_stored, status = "queued"
      Node--)UI: TopicMessageEmitted → sent
    else recipient unreachable
      Node->>Node: nothing to write — the row was born<br/>with its claim and carries no on_wire stamp
      Node-->>DM: message_stored, status = "queued"
    end
  end
  DM-->>UI: echo with receipt status
```

**Diagram: What happens between pressing send and the first status the sender sees**

Three orderings in that diagram are load-bearing.

The chatlog row is written BEFORE the routing decision and in every branch that routes at all: a message the user typed exists whether or not the network is available, and losing it because nobody was reachable would be the worst failure this system could have. The corollary is the `store-failed` branch — if the row cannot be written, the message is not sent either. Routing it anyway put the message on the wire while its own author had no record of it: the RPC answered `message_stored`, the UI drew a bubble no reload would bring back, nothing reseeded it after a restart, and the recipient could end up holding a message the sender cannot see, quote, resend or recall. The rule is scoped to messages this node AUTHORED; a write failure on someone else's message still keeps it in the runtime, because dropping it would lose it from the network too, for a peer who has already done everything right.

The retry entry is registered BEFORE anything is sent, because the sinks confirm into it — a confirmation arriving before the entry existed would be lost, leaving a message that DID go out recorded as never confirmed, and a reachability kick landing in the same window would find nothing to wake.

And `ClearNeverEmitted` lands BEFORE the frame, never after it: a crash in between must read as "the peer may have it", or a later deletion would skip a peer who is holding the message. The cost of that ordering is a withdrawal standing for a frame the writer then refuses — see [Refusals](#refusals).

The reply says `queued` in BOTH branches, and that is not pessimism: at reply time the sinks are still working, so "queued" is the only answer that cannot turn out to have been a lie. It is answered by `Announced`, which is claimed only once the durable stamp has landed — a directed relay confirms synchronously, so the in-memory hold is already clear while the row still reads queued, and reading the hold here would put "sent" in the reply over a row that a reload then contradicts. `TopicMessageEmitted` moves it to `sent` when a sink confirms — the same event the held branch waits for, so there is one path rather than two.

## The retry tick

`bootstrapLoop` calls `retryDueDeliveries` every 2 seconds. The tick is a scheduler, not a sender: the pacing comes from each entry's own schedule, and the tick's job is to decide which single message per recipient may go out now.

```mermaid
flowchart TD
  A["tick, every 2 s"] --> B["planDueDeliveries — deliveryMu"]
  B --> C{"its own TTL expired?"}
  C -- yes --> D["delete entry → failDelivery<br/>journal it, do not reseed"]
  C -- no --> E["group by recipient,<br/>sort oldest first"]
  E --> F["pickQueueHeadLocked:<br/>at most ONE per recipient"]
  F --> G["router.Route — NO mutex held"]
  G --> H{"route or live<br/>subscriber?"}
  H -- no --> I["Hold = unreachable<br/>re-check in 60 s"]
  H -- yes --> J["armDueDeliveries — deliveryMu<br/>was unreachable ⇒ Attempts = 0<br/>Hold = unconfirmed"]
  J --> K["emitDueDelivery"]
  K --> L{"frozen by a wipe,<br/>withdrawn, or journal<br/>write failed?"}
  L -- yes --> I
  L -- no --> M["dispatchEnvelopeRetry:<br/>hand to the sinks"]
  M -- "gate refused" --> I
  M -- "sinks called" --> O["Hold = unconfirmed,<br/>nothing charged"]
  O -- "a sink took the frame" --> N["confirmEnvelopeOnWire:<br/>Hold = none, attempt,<br/>backoff, announcement"]
  O -- "nobody took it" --> I
```

**Diagram: One pass of the delivery retry tick**

Three things in that diagram are the corrections to the bug this engine had. Nothing is spent before the wire takes the frame — the last box is the only place the schedule advances. The "no route" branch loops back to a 60-second re-check rather than counting against anything, so a recipient offline overnight arrives to a queue that is intact. And the `armDueDeliveries` box resets the backoff when a held message finds its recipient back: `Held` means the last decision was "they are not there", so held-plus-reachable is exactly the moment they returned, and it cannot fire for a peer that never left.

`Attempts` is no longer a budget — it is only the index into `deliveryRetrySchedule`, and the eleven-minute cap at its tail is what paces re-emissions to a recipient who is reachable but silent.

### Refusals

Handing a frame to a writer is two steps, not one, and they point in opposite directions in time. `clearedToWrite` runs FIRST — it re-checks the freeze and the withdrawal, and withdraws the durable `never_emitted` claim — because a crash between the withdrawal and the write has to read as "the peer may have it". `confirmEnvelopeOnWire` runs AFTER, when a sink says it took the frame, and stamps `on_wire`.

Between those two the writer can refuse, and **a refusal writes nothing at all**. It does not have to: the row already says both of the things its two readers need. Its never-emitted claim came off at the gate, so a deletion asks the peer rather than skipping them; and no on-wire stamp was added, so the sender still reads the message as queued. There is no fact left to record — and therefore none that can be recorded wrongly.

That is the whole reason the bits are separate. Serving both readers from one flag meant putting the claim BACK on a refusal, which made it non-monotone, and a non-monotone flag under concurrent sinks needs a queue, a per-attempt stamp, a re-read and a correction. Six review rounds found six different interleavings in that machinery before it was replaced.

The transport's own answer is still classified, because two callers need it for a different purpose — deciding whether to try elsewhere, not what to write down. `SendStatus` answers about ADMISSION, not about the wire (`network_core.md`), and only half of the non-OK statuses is exact:

| Status | Proof? | Why |
|---|---|---|
| `SendBufferFull`, `SendMarshalError` | yes | refused at the door — the frame never entered the queue |
| `SendWriterDone`, `SendChanClosed` | no | the same gate read from two places; once the frame is queued it proves only that it will not be written FROM THE QUEUE |
| `SendTimeout`, `SendCtxCancelled` | no | a sync wait ended on something other than the ack, with the frame still queued behind a live writer |

The inbound writer reports sentinels instead (`sendErrorProvesNothingWasWritten`), split on the same line; `ErrUnknownConn` is exact for a stronger reason than the rest — there was no registered connection, so there was no queue to sit in.

### Reading the two bits

`on_wire` is read as a TRI-STATE, because a database predates it and an older release predates half of it:

| Row | Badge |
|---|---|
| `on_wire` present | believe it — `sent` when true, `queued` when false |
| absent, `never_emitted` present | `queued`. The previous release had one flag and set it for exactly the messages this node was HOLDING, so these are genuinely unsent — reporting them as sent after an upgrade is the reported bug, reintroduced by the migration |
| neither | leave the persisted status alone. The row says nothing about the wire, and reading that silence as "not sent" would re-badge a user's whole unreceipted history as `queued` |

An outgoing row is therefore born with BOTH keys — `never_emitted: true` and an explicit `on_wire: false` — so "governed and unconfirmed" is distinguishable from "predates all of it". In the view the field is named `AwaitingWire` rather than `OnWire`, so its zero value means "leave the status alone" instead of "queued".

### Sinks, and why there is no list of them any more

There used to be one, and keeping it correct was the problem. Five review rounds found the same defect five times — in the session queue, in the inbound-direct write, in the pending ring, in the gossip fan-out, in the online-trigger drain — never the same code twice, always the same bug: a frame carrying one of our messages reached a writer without passing the gate, so it could be handed to the recipient AFTER its author recalled it, with the deletion recording it as never emitted and scheduling no peer-side delete.

The cure is not a longer list. The delivery reference travels WITH the frame, and it is filled in at the two doors every outbound frame passes:

| Door | Fills the reference in | Gate and confirmation |
|---|---|---|
| `enqueueSessionSendItem` — everything that reaches a session queue | `withDeliveryRef`, from the frame | the serve loop, at dequeue and on `SendOK` |
| `writeDeliveryFrameToInbound` — the writes that go straight to an inbound connection | `deliveryRefForFrame`, from the frame | inline, before and after the write |

A producer that knows the dispatch — the retry tick, which fans one attempt out to several sinks — passes its own reference so one attempt is charged once. A producer that does not know is not required to: there is nothing left for it to forget. `outbound_delivery_gate.go` holds both, and a test walks the package's AST and fails when a new caller writes to an inbound connection outside that door.

Two identity rules sit underneath. `frameEnvelope` is the ONE reader of a message's identity, because it lives in two shapes on the wire — flat fields on the relay frames, a nested `Item` on `push_message` — and every place that open-coded the choice got a different subset right. `deliveryRefForFrame` then answers "is this ours": the message must have been authored here (transit passes through untouched) and the retry engine must still be waiting on it.

There is one more way a frame dies, with no writer involved at all: the session's own queue is discarded when the session is torn down (`discardSendQueue`). The retry tick withdraws the durable claim before it hands the envelope to the sinks, so a frame still sitting in that queue has a row saying the message left the machine and no writer that ever saw it. Unlike a refusal there is no status to classify — a discarded queue element provably never reached NetCore.

Gossip deserves one note. It is a real path to the wire, but the `queued → sent` transition does not wait on it: it hands jobs to a bounded pool that sheds under load, so it is never trusted as the only sink. What made it dangerous was the gate, not the confirmation.

### Ordering

Emissions are handed to background goroutines, so the order they reach the wire is the scheduler's, not ours; and Go map iteration is randomised per process, so there was no order to begin with. Both problems disappear once at most one message per recipient is in flight.

The guarantee the queue provides is precise, and it is narrower than "the backlog arrives in order":

> A message that has never been emitted is never overtaken by a newer one.

That is the line the reader perceives. Once a message has gone out, the recipient either has it or it is in transit, so a newer one passing it *on the sender's side* reorders nothing they will see — and letting it pass is what keeps one lost receipt from stalling everything behind it for the rest of an eleven-minute backoff. So `pickQueueHeadLocked` treats the two cases differently: a never-emitted message that is not due yet still holds its place in line, while an already-emitted one waiting out its backoff steps aside.

The one exception is a message frozen by a deletion. A freeze has no expiry of its own — it ends when the wipe commits or aborts — so a frozen message is skipped rather than allowed to hold the queue. A stuck freeze must cost its own message, never the whole conversation.

The same rule binds the OTHER half of the queue discipline, and reading it only into the pick is how it was broken: when a receipt frees the slot, `promoteQueueHeadLocked` pulls forward the oldest message that has NOT been on the wire, not simply the oldest one left. An already-emitted message is not waiting on that slot — its schedule measures its own dispatch — so promoting it re-sends a copy the recipient already has AND, because the re-send owns the slot for another queue window, delays the message that has never gone out at all.

```mermaid
sequenceDiagram
  autonumber
  participant S as Sender
  participant R as Recipient

  Note over S: msg1..msg4 all queued — R was offline
  Note over S,R: R comes back: session established → kickDeliveryRetriesForReachable
  S->>R: msg1
  R-->>S: delivered(msg1)
  Note over S: receipt frees the slot,<br/>promoteQueueHeadLocked pulls forward the oldest<br/>message that has NOT been on the wire
  S->>R: msg2
  R-->>S: delivered(msg2)
  S->>R: msg3
  Note over S,R: receipt for msg3 is lost
  Note over S: after deliveryQueueWindow (20 s)<br/>msg3 loses its place in line
  S->>R: msg4
  S->>R: msg3 (retry, same MessageID — R dedupes silently)
  R-->>S: delivered(msg3)
```

**Diagram: Draining a backlog to a recipient who came back, including one lost receipt**

The window exists so that a lost receipt costs ONE message its place in line rather than freezing the conversation behind it. It must stay below the shortest retry interval (`deliveryRetrySchedule[0]`, 30 s), or a message that becomes due again would re-take the slot it just released and starve everything behind it; a test pins the inequality.

## Waking a held message

A held message does not wait for its poll. Every event that can make a recipient reachable calls `kickDeliveryRetriesForReachable`, which pulls the schedule of the matching held entries forward to now — never touching the attempt counter, so a kick can never, by itself, exhaust anything.

| Call site | Event |
|---|---|
| `service.go` (`handleAuthSession`, after `auth_ok`) | an inbound peer completed authentication |
| `peer_sessions.go` | an outbound session entered its main loop |
| `routing_announce.go` (drain) | a route announcement was accepted |
| `routing_announce.go` (`triggerDrainForExposed`) | backup routes became reachable |
| `routing_query.go` | a `route_query` answer produced a route |

The inbound one fires strictly AFTER `auth_ok` has been written, not in `trackInboundConnect` where the direct route is actually added: a retry tick woken there could put a `push_message` on the connection ahead of the handshake reply the peer is still waiting for. Same boundary, same reason, as the backlog replay.

Two races could otherwise swallow a wake-up, and both are closed:

- A kick landing between the origin send's reachability check and the registration that follows it finds nothing to wake. So a held entry is registered **already due** — the backoff schedule times a wait for a receipt, and a message that never reached the wire has no receipt coming.
- A kick landing between the retry tick's lock-free reachability sample and the moment it writes `Held` finds the entry not yet held, and therefore invisible. `armDueDeliveries` compares `lastReachabilityKickAt` against the start of its pass and keeps such an entry due instead of parking it for a poll interval.

Each kick re-checks reachability itself (`router.Route`), so a call site that reports an identity which is not actually a usable delivery target produces no re-arm and wastes nothing. `deliveryHoldPollInterval` (60 s) is the safety net underneath all of them — a local routing-table lookup, so a missed event costs a minute rather than an hour.

A kick moves ONLY the entries that were waiting on reachability (`holdUnreachable`). The other two states have already been dispatched on the current reachability — `holdNone` is confirmed and waiting for a receipt on its backoff, `holdUnconfirmed` was handed to the sinks and nothing took the frame, so it is parked on the poll interval. Both announce ingest and route-query answers feed `routing.RouteUnchanged` into the kick, deliberately: a route already in the table can become usable the moment its peer answers. But a route being RE-confirmed says nothing about a message already dispatched, and waking those turned every periodic announcement into a re-send and a journal write — at announcement frequency, on a route that resolves but is dead in practice.

Separately, a recipient who dials US gets their held messages through a different door: `pushBacklogToSubscriber` replays the node's in-memory `dm` backlog right after `auth_ok`, and that path is not gated on the hold. It is not a substitute for the retry engine — it is in-memory, so it does not survive a restart, and it only fires when we are the responder.

## Lifetime bounds

| Bound | Value | Applies to | Effect |
|---|---|---|---|
| `TTLSeconds` | 0 for ordinary DMs | messages with an auto-delete flag | abandoned as `expired` |
| `CancelOutgoingDelivery` | — | any message | withdrawn by its author |
| attempt cap | 20 attempts (`CORSA_DELIVERY_RETRY_MAX_ATTEMPTS`) | outgoing SEEN RECEIPTS only | the node stops asking for the `seen_ack` |
| `seenReseedHorizon` | 7 days | the startup reseed of seen receipts | older receipts are not picked up |

Messages appear in that table twice, and both entries are things somebody asked for. There is deliberately no row for age and none for a number of tries: waiting costs nothing on the wire, because the reachability gate emits nothing while the recipient is unreachable, and the startup reseed of undelivered messages scans with no horizon at all so a restart is not a way to give up either.

Seen receipts keep their cap because the far side repairs them: every arrival of a `seen` receipt makes the original sender answer with a `seen_ack`, so an abandoned one is re-triggered rather than lost.

The cost of this is real and accepted: `awaitingDelivered` holds one entry per undelivered message for as long as the process runs, and the startup scan reads every chatlog row still in `sent`.

## Accepted limitations

1. **Both parties must be up at the same time.** If the sender goes offline before the recipient returns, nothing in the mesh is holding the message on their behalf. This is the price of not being a mailbox, and it is a design decision rather than a gap — see [relay.md](relay.md) INV-3 and `transit_retention.go`.

2. **Backlog drain is paced by the tick.** One message per recipient per 2-second tick when receipts flow, one per `deliveryQueueWindow` when they do not. A thousand queued messages take roughly half an hour in the good case — accepted, because a backlog that size means the sender kept writing to someone whose messages were visibly not being delivered. A recipient who dials US instead gets the whole in-memory backlog at once through `pushBacklogToSubscriber`, which does not go through this queue.

3. **Undelivered messages accumulate.** One `awaitingDelivered` entry per undelivered message, for the lifetime of the process, and a startup scan over every chatlog row still in `sent`. This is the direct cost of having no horizon, and it is bounded in practice by the same thing the user sees: the number of messages showing no confirmation.

---

# Движок доставки сообщений

> Смежные документы: [messaging.md](messaging.md) (фрейм `send_message` и его валидация), [delivery.md](delivery.md) (фреймы квитанций), [relay.md](relay.md) (пересылка по хопам), [realtime.md](realtime.md) (живой push и backlog на auth).

## Обзор

Ничто в mesh-сети не хранит пользовательские сообщения за отсутствующего получателя. Транзитные узлы только пересылают (`transit_retention.go`), и это осознанное решение: узел, хранящий чужие сообщения, — это почтовый ящик, со всем, что почтовый ящик означает для хранения, ретенции и отрицаемости. Следствие и есть предмет этого документа: **за доставку отвечает отправитель** — с момента, когда сообщение написано, и до подтверждения от узла получателя.

Отсюда три свойства, и каждое закрывает конкретный отказ, который у этого движка был:

- **Сообщение никогда не уходит в пустоту.** Если у получателя нет ни маршрута, ни живой сессии, сообщение УДЕРЖИВАЕТСЯ на узле отправителя, а не рассылается вслепую в надежде. Это гейт достижимости (`CORSA_HOLD_DM_UNTIL_REACHABLE`, включён по умолчанию).
- **Движок сам не сдаётся.** Доставка заканчивается ровно тремя способами: получатель подтвердил, автор отозвал, истёк собственный TTL сообщения. Ни лимита попыток, ни горизонта по возрасту для сообщений нет — «кончилось терпение» было бы решением узла, о котором отправителю не сообщают и которое он не может отменить.
- **Вернувшемуся получателю отправляем сразу.** Когда недостижимый становится достижимым, у ждущих сообщений backoff сбрасывается на первую ступень: сообщение, готовое со вчера, уходит за секунды, а не после ещё одного одиннадцатиминутного таймера.
- **Ни разу не отправленное сообщение никогда не обгоняется более новым.** На каждого получателя движок двигает по одному сообщению, от старого к новому, и следующее уходит, когда подтверждено предыдущее. Гарантия сформулирована именно про неотправленные сообщения намеренно — почему это и есть значимая граница и какое из неё исключение, см. в разделе [Порядок](#порядок).

### Состояния сообщения глазами отправителя

| Состояние | Смысл | Где живёт |
|---|---|---|
| `queued` | Записано локально, НЕ известно, что в сети — ни один приёмник не подтвердил приём | у строки chatlog нет штампа `on_wire`; в памяти на тот же вопрос отвечает удержание записи (`status` в `message_stored`, событие `message.new`) |
| `sent` | Ушло в сеть, ждём подтверждения получателя | `delivery_status` в chatlog |
| `delivered` | Узел получателя принял сообщение | `delivery_status`, ставится по квитанции `delivered` |
| `seen` | Получатель открыл переписку | `delivery_status`, ставится по квитанции `seen` |

`queued` — ЛОКАЛЬНОЕ состояние, оно никогда не передаётся между узлами: пиру нечего сказать о сообщении, которое до него не дошло. Chatlog хранит только три статуса, потому что на проводе `queued` и `sent` — одно и то же; разница важна тому, кто написал сообщение, а для него это вся разница между «оно у них» и «этого ещё никто не видел».

```mermaid
stateDiagram-v2
  [*] --> queued: получатель недостижим в момент отправки
  [*] --> sent: получатель достижим — уходит сразу
  queued --> sent: появился маршрут или сессия, отправка удалась
  sent --> queued: никогда — отправленность монотонна
  sent --> delivered: квитанция delivered от получателя
  delivered --> seen: квитанция seen от получателя
  queued --> withdrawn: автор отозвал
  sent --> withdrawn: автор отозвал
  queued --> expired: собственный TTL — только авто-удаляемые
  sent --> expired: собственный TTL — только авто-удаляемые
  seen --> [*]
  delivered --> [*]
  withdrawn --> [*]
  expired --> [*]
```

**Диаграмма: состояния одного исходящего DM на стороне отправителя**

Состояния «узел сдался» тут нет. Обычный DM несёт `TTLSeconds=0`, поэтому `expired` относится только к сообщениям, явно отправленным с авто-удалением; `withdrawn` — это `CancelOutgoingDelivery`, о котором просит автор. `expired` — терминальное состояние ДВИЖКА РЕТРАЕВ, а не строки chatlog: строка остаётся в `sent`, а долговременный журнал (`delivery_failed`) не даёт перезапуску её воскресить.

Переход queued → sent объявляется собственному клиенту отправителя (`ebus.TopicMessageEmitted`), потому что иначе о нём никто не скажет: следующее событие — квитанция получателя, а потерянная квитанция оставила бы отправителя в уверенности, что сообщение не ушло, пока собеседник его читает.

Честность этого объявления держат три правила, и каждое закрывает место, где бейдж мог бы обогнать провод:

- Объявляет **приёмник, принявший фрейм**, а не код, который решил попробовать, — и «принял» значит «принял NetCore». Собственная очередь сессии `sendCh` этой границей не является: это внутрипроцессная очередь, чей остаток выбрасывается при закрытии сессии, поэтому relay-enqueue подтверждением не считается — подтверждает writer-цикл сессии, когда `SendTracked` вернул `SendOK`, а ссылка на отправку едет на элементе очереди, чтобы несколько приёмников одной попытки списали её один раз. `confirmEnvelopeOnWire` — единственное место, где приёмник превращает запись в «отправлено». Штамп и объявление он отдаёт ОДНОЙ фоновой горутине, именно в таком порядке, и объявление происходит, только если штамп ЛЁГ: вне writer-цикла, потому что журнал пишет через SQLite, где конкуренция может задержать statement на весь busy_timeout; сначала штамп и с условием, потому что событие двигает кэш переписки, а полная перезагрузка читает строку — событие, выполненное до записи или без неё, позволит следующей перезагрузке вернуть бейдж в `queued`, и следующего события уже не будет, потому что объявление берётся один раз. Пакет — реконнект переигрывает целую переписку — сначала двигает все записи в памяти, а затем платит за ОДНУ запись в журнал и одно объявление.
- Оно **не выводится из утверждения `never_emitted`**, которое консервативно и становится ложным на первой же попытке — независимо от того, ушло ли что-нибудь. У бейджа свой durable-бит (`on_wire`) и свой флаг в памяти (`deliveryRetryEntry.Announced`), поэтому неудавшаяся попытка ничего не тратит, а следующая удачная всё-таки сообщит отправителю.
- Локальный сбой **чинится локально**. Оставить подтверждённое сообщение записанным наполовину могут три вещи, и ни одна не вина сети: журнал, отказавший в штампе; полоса эмиссии, не принявшая штамп, потому что в ней уже ждёт достаточно других; и шина, сбросившая объявление при переполненном 64-слотовом inbox подписчика — что реконнект, переигрывающий целую переписку, делает вероятным, потому что объявления приходят пачкой. Раньше все три чинились только следующей СЕТЕВОЙ отправкой: сообщение, уже принятое пиром, снова уходило в сеть, а для пакета — весь пакет целиком. Вместо этого `repairLocalDeliveryRecord` переделывает только учёт и работает ВНЕ тика, на single-flight горутине: он блокируется на журнале, а занятая база иначе задержала бы ровно те сообщения, которых ждёт вернувшийся получатель, и весь остальной последовательный bootstrap-цикл.

Учёт не опережает pre-wire запись, и чтобы так и оставалось, нужна полоса: снятие порядка не сняло конкуренции. `ClearNeverEmitted` обязан лечь до того, как уйдёт кадр, — включая первый кадр получателю, который только что вернулся после ночи оффлайна, а это ровно тот момент, когда реконнект штампует всю переигрываемую переписку. SQLite пишет по одному writer'у за раз, а пул соединений не ограничен, поэтому без нашего собственного порядка эти двое соревнуются в busy-handler'е, где очередь решает тайминг ретраев, а не нужда.

Порядок задаёт `emissionLane` — очередь, а не лок. Каждая запись в журнал берёт в ней ход, и ходы решают три правила. ПРИОРИТЕТ: снятие обслуживается раньше любого учёта, потому что учёт записывает уже ушедший кадр, а снятия кадр ждёт. FIFO ВНУТРИ КЛАССА: ходы берутся в порядке прихода, в том числе тем, кто только что закончил ход и хочет следующий, — мьютекс с condition variable очередью не являются, и «следующий ход достаётся ждущему» было верно лишь по везению, пока повторный вход соревновался за тот же мьютекс. ОДИН STATEMENT НА ХОД: ход пишет не больше `maxIdsPerLaneWrite` id, а у кого осталось больше — уходит в КОНЕЦ очереди, поэтому backlog-реплей, подтверждающий целую переписку, не может удерживать полосу на весь свой вызов.

Поверх этого — коалесцирование: снятие, берущее ход, вбирает снятия, стоящие за ним, но не больше чем на один statement, так что несколько одновременно реконнектящихся отправителей стоят одной записи, а не одной на каждого. Ограничение здесь так же важно, как само коалесцирование: пачка без потолка воссоздала бы ровно тот дефект, который убирает третье правило, — привязала бы только что набранное сообщение к целой переписке. Цена — общая судьба: упавший statement роняет всех, чьи id в него попали, что склоняет к придерживанию кадра и повторяется следующим тиком.

Штампы вдобавок ограничены ОТКАЗОМ, которого снятиям нельзя: не поставленный полосой в очередь штамп не пишет ничего и отвечает как неудавшаяся запись, а это бесплатно — долг заново выводит проход-чинильщик.

Две вещи стоит сказать прямо, чтобы следующий читатель не переоценил и не недооценил выигрыш. По замеру конкуренция мала: при 64 горутинах, в цикле штампующих переписку целиком, худшее pre-wire снятие на той же базе уложилось в 3 мс — statement-ы короткие, и writer-лок меняет владельца быстро. Полоса нужна не для спасения рядовой отправки. Она нужна потому, что цена проигрыша в этой гонке асимметрична: снятие, исчерпавшее `busy_timeout`, ПРИДЕРЖИТ сообщение пользователя, — а в этот хвост попадают не из-за штампов самих по себе, а из-за штампа, вставшего за чужой длинной записью, на медленном диске или на телефоне. Второй выигрыш безусловный: без ограничения очереди реконнект-бэклог кладёт на базу неограниченное число горутин.

Чего полоса НЕ обещает — быстрой записи. База общая со всеми подсистемами, поэтому statement внутри полосы всё ещё может встать на чужой транзакции, и никакой наш порядок этого не прервёт. Ограничить это значило бы зажать пул соединений всей базы, а вместе с записями сериализовать и чтения — решение уровня storage, а не доставки. Порядок поверх полосы всё равно полезен: проход-чинильщик запускается ПОСЛЕ того, как тик разослал кадры, в single-flight, на собственной горутине.

Локальных долгов ТРИ, и на retry-записи живут только два — поэтому проходу и мало одной retry-карты. На записи: durable-штамп и объявление. Третий — сброшенное событие, которое ПЕРЕЖИВАЕТ свою запись, потому что receipt запись УДАЛЯЕТ: если в одном всплеске сброшены и emitted-событие, и receipt-обновление, повторять становится нечего, и переписка показывает `queued` для сообщения, которое собеседник уже прочитал, — пока пользователь её не переоткроет. `publishRetryableLocalChange` сохраняет отвергнутое шиной в ограниченной карте по ключу message id и разгребает тем же проходом с тем же backoff. Ключ по id означает, что более новый статус ЗАМЕНЯЕТ более старый — и только в эту сторону: `sent` → `delivered` → `seen` образуют ранг, сохранённое событие никогда не перезаписывается более низким, а републикация, которую уже обогнал более новый статус, ничего не удаляет. Иначе проход, работающий по снимку, опубликовал бы `sent` и удалил пришедший за время публикации `delivered`, навсегда оставив бейдж на шаг позади.

Четвёртый — уже вообще не долг, и это то, что дало правило коммита. Обработка квитанции заканчивает три ретрая: ОТПРАВИТЕЛЯ — удалением retry-записи; подавление следующей копии от пира на этом узле — записью dedup-ключа; и ретрай самого пира — подтверждением. Все три применялись ДО записи, которая делает их правдой, и каждый приходилось откатывать руками, когда запись падала, — кроме retry-записи, которую не откатывал никто, поэтому отказ записи оставлял строку в `sent`, и исправить её было уже нечем.

Идентичность квитанции — ОДНА сущность, `receiptIdentity{Recipient, MessageID, Status, Sender}`, и из неё выводится всё, что решает «та же это квитанция или нет»: dedup-ключ, ключ relay-retry, фильтр backlog'а, ключ очереди отложенных кадров и кадр `ack_delete`. Этот список и есть фикс. Раньше каждый из них отвечал на вопрос сам, поэтому добавление автора в один оставляло остальные четыре с прежней тройкой recipient+message+status — а это не идентичность вовсе, потому что об одном сообщении могут заявить два пира. Острым концом оказался `ack_delete`: ack называет квитанцию, которую пир держит, и применённый к тройке он удалял ВСЕ похожие — вместе с настоящей и её retry-записью. Подделку конечный узел отвергает, но ack на неё — честный трафик, и приземляется он на релее. `TestReceiptIdentityIsBuiltInOnePlace` и `TestEveryRelayRetryReceiptKeyComesFromTheIdentity` роняют сборку на идентичности или ключе, собранных где-то ещё: после семи рукописных написаний «ревью поймает следующее» планом не является.

Автор едет в `Frame.ReceiptSender` и входит В подписываемый payload (`corsa-ack-delete-v2`), поэтому подменить его на проводе нельзя. Какой payload подписан — определяет сам кадр, а не догадка одной стороны о версии другой: кадр с полем подписывает v2, кадр без поля — ровно те байты v1, что всегда производили старые пиры, и подписывающий с проверяющим идут через одну функцию. Отправка поля гейтится на объявленной пиром версии `ProtocolVersionReceiptSenderAck` (29): старый проверяющий соберёт payload v1 и посчитает невоспроизводимую подпись подделкой, а это ban-очки, а не предупреждение. Пока floor не дошёл до 29, ack может прийти без имени автора — тогда действует правило «удалять только при однозначности»: один кандидат удаляется, несколько СОХРАНЯЮТСЯ все. Сохранение стоит дубликата, который пир отбросит; удаление не той квитанции стоит доставки. Штамп — это ВЗГЛЯД на кадр со стороны одной сессии, а не переписывание того, что хранится: в очереди лежит полный ack, и это верно и для парковки кадра, и для возврата после неудачного слива. Пониженная копия в очереди выбросила бы автора навсегда, а слить её может уже текущий пир — и он получит ack, который не может назвать свою квитанцию, оставит обе спорные и продолжит их слать.

Dedup-ключ — это та же идентичность, выписанная строкой: `recipient:messageID:status:sender`, и производит её `receiptIdentity.dedupKey`, больше ничто. Sender входит в ключ потому, что квитанция — это УТВЕРЖДЕНИЕ того, кто её сделал, а два утверждения о одном сообщении от двух пиров — два разных факта. Без sender'а они были одним, и следствием был не просто неверный счёт: любой, кто узнал message id, мог послать свою квитанцию и занять ключ, после чего настоящая квитанция получателя молча считалась дубликатом. Проверка отправителя на конечном узле здесь не спасает: на РЕЛЕЕ этому узлу не адресовано ничего — настоящая квитанция отбрасывается хопом раньше, чем эта проверка вообще выполняется, и отправитель ждёт целый интервал ретрая, и так на каждое повторение. `TestEveryDedupKeyComesFromTheOnePlaceThatSpellsIt` роняет сборку на ключе, выписанном руками в новом месте: одно и то же выражение стояло в семи, и место, оставшееся со старым написанием, писало бы под ключом, который путь вытеснения уже не может удалить.

Та же идентичность решает и судьбу кадров в очереди. `relay_delivery_receipt`, ждущий в pending, — ЭТО квитанция в полёте, и очистка по тройке означала, что коммит одной квитанции всё равно выбрасывает чужую: последнее место, где подтверждение забирало с собой настоящую. `clearPendingReceiptLocked` принимает идентичность и сравнивает её целиком.

`finishReceipt` применяет все три ПОСЛЕ записи, поэтому отказавшая запись не коммитит НИЧЕГО: отправитель продолжает ретраить сообщение, получатель отвечает на повтор новой квитанцией, и у этого узла появляется ещё попытка. Это восстановление локальное. Оно не опирается на очередь квитанций пира — та ограничена по размеру, истекает через три минуты и не переживает его рестарт; более ранняя версия этого документа утверждала обратное, и это было неверно.

«Пиру не говорят забыть» — это четыре разные вещи, и каждая заканчивает жизнь квитанции на его стороне: `ack_delete` на исходящей peer-сессии, `ack_delete` на входящем соединении, `seen_ack`, останавливающий ДОЛГОВРЕМЕННЫЙ ретрай читателя (он его журналирует, поэтому после его рестарта повторов уже не будет), и ответ на локальный RPC `send_delivery_receipt`, по которому клиент снимает отметку непрочитанного. Все четыре читают `receiptOutcome.ackable`, а `TestEveryReceiptAckConsultsTheOutcome` роняет сборку на подтверждении, которое не стоит под таким условием: список «разрешённых функций» остался бы зелёным, если бы гейт внутри одной из них удалили.

Два долга на записи гасятся раздельно, потому что ломаются по-разному. Удавшаяся запись в журнал не нуждается в повторе никогда, поэтому запись её запоминает (`Stamped`); объявление может сбрасываться снова и снова, поэтому у него собственный backoff `AnnounceAfter`. Пока оба ключились на «не объявлено», каждое сброшенное событие тянуло за собой ещё одну запись в SQLite — каждые две секунды, пока подписчик заклинен. Обе половины берутся ОТ СТАРЫХ К НОВЫМ, чтобы накопившийся долг разгребался в порядке, в котором пользователь писал, а не пересэмплировался из map каждый проход. Кэп ограничивает один проход, а сходимость даёт именно BACKOFF: уже переполненный inbox сбрасывает любое новое событие независимо от размера пачки, так что «16 меньше 64» аргументом никогда не было.

Gossip намеренно не входит в число подтверждающих приёмников: он отдаёт задания в ограниченный пул, который под нагрузкой их сбрасывает, — и единственным приёмником НАШЕГО сообщения он всё равно не бывает, потому что гейт достижимости прошёл, а значит есть маршрут или подписчик. Да и «принято writer-ом» в любом случае предел видимости узла: принятый фрейм всё ещё может потеряться на проводе, ради чего и существует квитанция.

`message_known` — ответ на идемпотентный повтор — несёт тот же статус, что нёс бы `message_stored`: клиент считает его успехом и читает статус именно оттуда.

Состояние `queued` едет и на событии `message.new`, а не выводится клиентом. Сохранение локально написанного DM объявляется собственному клиенту узла, и это объявление состязается с синхронным ответом на `send_message`: чей результат первым дойдёт до кэша переписки, тот и решает бейдж, — поэтому оба говорят одно и то же.

## Путь отправки

```mermaid
sequenceDiagram
  autonumber
  participant UI as Десктопный UI
  participant DM as DMCrypto / DMRouter
  participant Node as node.Service
  participant Log as chatlog (SQLite)
  participant Net as mesh

  UI->>DM: SendMessage(peer, текст)
  DM->>DM: EncryptForParticipants, новый MessageID
  DM->>Node: send_message (локальный RPC-фрейм)
  Node->>Log: StoreMessage — статус "sent", рождается never_emitted
  alt строку записать не удалось
    Node-->>DM: error, code = "store-failed"<br/>ничего не маршрутизируется и не регистрируется
  else строка на диске
    Node->>Node: router.Route(envelope)
    Node->>Node: registerAwaitingDelivered — ДО любой отправки,<br/>чтобы подтверждению было куда лечь
    alt получатель достижим (маршрут или живой подписчик)
      Node->>Log: ClearNeverEmitted — ДО кадра,<br/>через полосу эмиссии как URGENT
      Node->>Net: push_message / relay_message
      Net-->>Node: приёмник принял → confirmEnvelopeOnWire
      Node-->>DM: message_stored, status = "queued"
      Node--)UI: TopicMessageEmitted → sent
    else получатель недостижим
      Node->>Node: nothing to write — the row was born<br/>with its claim and carries no on_wire stamp
      Node-->>DM: message_stored, status = "queued"
    end
  end
  DM-->>UI: эхо со статусом
```

**Диаграмма: что происходит между нажатием «отправить» и первым статусом, который видит отправитель**

Три порядка на этой диаграмме несущие.

Строка chatlog пишется ДО решения о маршрутизации и в каждой ветке, которая вообще маршрутизирует: набранное пользователем сообщение существует независимо от доступности сети, и потерять его из-за того, что никто не был доступен, — худший отказ, который эта система могла бы допустить. Следствие — ветка `store-failed`: если строку записать не удалось, сообщение и не отправляется. Маршрутизация вопреки этому выкладывала сообщение в сеть, пока у его собственного автора не было о нём никакой записи: RPC отвечал `message_stored`, UI рисовал пузырь, который не вернёт ни одна перезагрузка, после рестарта его ничто не пересевало, — а получатель мог остаться с сообщением, которое отправитель не видит, не может процитировать, переотправить или отозвать. Правило распространяется только на сообщения, АВТОРОМ которых является этот узел: чужое сообщение при отказе записи по-прежнему остаётся в рантайме, потому что выбросить его значило бы потерять его и из сети — для пира, который всё сделал правильно.

Запись ретраев регистрируется ДО любой отправки, потому что приёмники подтверждают именно в неё: подтверждение, пришедшее раньше записи, потерялось бы, и реально ушедшее сообщение осталось бы «неподтверждённым», а kick в том же окне не нашёл бы, что будить.

А `ClearNeverEmitted` ложится ДО кадра, а не после: крах между ними обязан читаться как «возможно, у пира это есть», иначе позднейшее удаление пропустит пира, который держит сообщение. Цена этого порядка — снятое утверждение для кадра, который writer потом отверг; см. [Отказы](#отказы).

Ответ говорит `queued` в ОБЕИХ ветках, и это не пессимизм: в момент ответа приёмники ещё работают, поэтому «в очереди» — единственное, что не может оказаться ложью задним числом. Отвечает на это `Announced`, который берётся только после того, как лёг durable-штамп: directed relay подтверждает синхронно, поэтому удержание в памяти уже снято, пока строка ещё читается как queued, — и чтение удержания здесь положило бы в ответ «sent» поверх строки, которой перезагрузка сразу возразит. `TopicMessageEmitted` переводит статус в `sent`, когда приёмник подтвердит, — то же самое событие, которого ждёт удержанная ветка, так что путь один, а не два.

## Тик ретраев

`bootstrapLoop` вызывает `retryDueDeliveries` каждые 2 секунды. Тик — планировщик, а не отправитель: темп задаёт расписание внутри каждой записи, а дело тика — решить, какое ОДНО сообщение на получателя может уйти сейчас.

```mermaid
flowchart TD
  A["тик, каждые 2 с"] --> B["planDueDeliveries — deliveryMu"]
  B --> C{"истёк собственный TTL?"}
  C -- да --> D["удалить запись → failDelivery<br/>записать в журнал, не пересевать"]
  C -- нет --> E["сгруппировать по получателю,<br/>отсортировать от старых"]
  E --> F["pickQueueHeadLocked:<br/>НЕ БОЛЕЕ одного на получателя"]
  F --> G["router.Route — БЕЗ мьютекса"]
  G --> H{"есть маршрут или<br/>живой подписчик?"}
  H -- нет --> I["Hold = unreachable<br/>перепроверка через 60 с"]
  H -- да --> J["armDueDeliveries — deliveryMu<br/>был unreachable ⇒ Attempts = 0<br/>Hold = unconfirmed"]
  J --> K["emitDueDelivery"]
  K --> L{"заморожено очисткой,<br/>отозвано или не удалась<br/>запись в журнал?"}
  L -- да --> I
  L -- нет --> M["dispatchEnvelopeRetry:<br/>раздать приёмникам"]
  M -- "гейт отказал" --> I
  M -- "приёмники вызваны" --> O["Hold = unconfirmed,<br/>ничего не списано"]
  O -- "приёмник принял фрейм" --> N["confirmEnvelopeOnWire:<br/>Hold = none, попытка,<br/>backoff, объявление"]
  O -- "никто не принял" --> I
```

**Диаграмма: один проход тика ретраев доставки**

Три вещи на этой диаграмме — это и есть исправление бага, который у движка был. Ничего не тратится до того, как провод принял фрейм: последний блок — единственное место, где двигается расписание. Ветка «нет маршрута» уходит на перепроверку через 60 секунд и ни к чему не приплюсовывается, — поэтому получатель, отсутствовавший всю ночь, утром застаёт очередь целой. И блок `armDueDeliveries` сбрасывает backoff, когда удержанное сообщение застаёт получателя вернувшимся: `Held` означает «в прошлый раз решили, что его нет», поэтому held-плюс-достижим — это ровно момент возвращения, и для пира, который никуда не уходил, оно сработать не может.

`Attempts` больше не бюджет — это только индекс в `deliveryRetrySchedule`, а одиннадцатиминутный потолок в его хвосте и есть то, что задаёт темп повторов достижимому, но молчащему получателю.

### Отказы

Передача кадра writer'у — это два шага, а не один, и во времени они направлены в разные стороны. `clearedToWrite` идёт ПЕРВЫМ: он перепроверяет заморозку и отзыв и снимает durable-утверждение `never_emitted`, — потому что крах между снятием и записью обязан читаться как «возможно, у пира это есть». `confirmEnvelopeOnWire` идёт ПОСЛЕ, когда приёмник сказал, что взял кадр, и ставит `on_wire`.

Между этими двумя шагами writer может отказать, и **отказ не пишет вообще ничего**. Ему и не нужно: строка уже говорит обе вещи, нужные её двум читателям. Утверждение `never_emitted` снято на гейте, поэтому удаление спросит пира, а не пропустит его; штамп `on_wire` не добавлен, поэтому отправитель по-прежнему видит `queued`. Записывать нечего — а значит, нечего и записать неверно.

Ровно ради этого биты и разделены. Обслуживать обоих читателей одним флагом значило ВОЗВРАЩАТЬ утверждение на отказе, то есть сделать флаг немонотонным, а немонотонный флаг под конкурентными приёмниками требует очереди, штампа попытки, перечитывания и коррекции. Шесть раундов ревью нашли в этой машинерии шесть разных переплетений, прежде чем её заменили.

Ответ транспорта по-прежнему классифицируется, но для другой цели: решить, пробовать ли иначе, а не что записать. `SendStatus` отвечает про ДОПУСК, а не про провод (`network_core.md`), и точна лишь половина не-OK статусов:

| Статус | Доказательство? | Почему |
|---|---|---|
| `SendBufferFull`, `SendMarshalError` | да | отказ на входе — кадр вообще не попал в очередь |
| `SendWriterDone`, `SendChanClosed` | нет | один и тот же гейт, прочитанный из двух мест; для уже поставленного в очередь кадра это доказывает лишь, что он не будет записан ИЗ ОЧЕРЕДИ |
| `SendTimeout`, `SendCtxCancelled` | нет | синхронное ожидание закончилось не на ack, кадр остался в очереди за живым writer'ом |

Входящий writer вместо статусов отдаёт сентинелы (`sendErrorProvesNothingWasWritten`), делятся они по той же границе; `ErrUnknownConn` точен по более сильной причине: зарегистрированного соединения не было, значит не было и очереди.

### Как читаются два бита

`on_wire` читается как ТРИ-состояние, потому что база данных старше этого бита, а предыдущий релиз старше его половины:

| Строка | Бейдж |
|---|---|
| `on_wire` присутствует | верить ему — `sent` при true, `queued` при false |
| отсутствует, есть `never_emitted` | `queued`. У прошлого релиза был один флаг, и он ставил его ровно для сообщений, которые узел УДЕРЖИВАЛ, — значит они действительно не отправлены, и показать их как sent после апгрейда значит вернуть исходный баг силами самой миграции |
| нет ни того, ни другого | не трогать сохранённый статус. Строка ничего не говорит о проводе, а прочитать это молчание как «не отправлено» значит перекрасить всю неполученную историю пользователя в `queued` |

Поэтому исходящая строка рождается с ОБОИМИ ключами — `never_emitted: true` и явным `on_wire: false`, — чтобы «под управлением бита и не подтверждено» отличалось от «старше всего этого». В структуре для UI поле называется `AwaitingWire`, а не `OnWire`, чтобы zero value значил «не трогать статус», а не «queued».

### Приёмники, и почему их списка больше нет

Список был, и проблемой было именно поддерживать его в актуальном состоянии. Пять раундов ревью нашли один и тот же дефект пять раз — в очереди сессии, в прямой записи во входящее, в pending-кольце, в веере gossip, в drain'е по онлайн-триггеру, — ни разу в одном и том же коде и всегда один и тот же баг: кадр с нашим сообщением доходил до writer'а, не пройдя гейт, и мог быть отдан получателю ПОСЛЕ того, как автор его отозвал, — а удаление записывало сообщение как неотправлявшееся и не планировало peer-side delete.

Лечение — не более длинный список. Ссылка на доставку едет ВМЕСТЕ с кадром и до-заполняется в двух дверях, через которые проходит любой исходящий кадр:

| Дверь | Заполняет ссылку | Гейт и подтверждение |
|---|---|---|
| `enqueueSessionSendItem` — всё, что попадает в очередь сессии | `withDeliveryRef`, из самого кадра | serve-цикл: при снятии с очереди и на `SendOK` |
| `writeDeliveryFrameToInbound` — записи напрямую во входящее соединение | `deliveryRefForFrame`, из самого кадра | инлайн, до и после записи |

Производитель, который знает отправку, — тик ретраев, раздающий одну попытку нескольким приёмникам, — передаёт свою ссылку, и одна попытка списывается один раз. Производитель, который её не знает, знать и не обязан: забывать ему больше нечего. Обе двери лежат в `outbound_delivery_gate.go`, а тест обходит AST пакета и падает, когда появляется новый вызывающий, пишущий во входящее соединение мимо этой двери.

Под этим лежат два правила идентичности. `frameEnvelope` — ЕДИНСТВЕННЫЙ читатель идентичности сообщения, потому что на проводе она живёт в двух формах (плоские поля у relay-кадров, вложенный `Item` у `push_message`), и каждое место, где выбор был написан руками, угадывало свой набор полей. `deliveryRefForFrame` затем отвечает на вопрос «наше ли это»: сообщение должно быть написано здесь (транзит проходит нетронутым), и движок ретраев должен всё ещё его ждать.

Есть ещё один способ погибнуть, вообще без writer'а: собственная очередь сессии выбрасывается при её разборке (`discardSendQueue`). Тик ретраев снимает durable-утверждение ДО того, как отдаёт конверт приёмникам, поэтому у кадра, оставшегося в этой очереди, строка утверждает, что сообщение ушло с машины, а видевшего его writer'а не было. В отличие от отказа классифицировать нечего: выброшенный элемент очереди заведомо не дошёл до NetCore.

Про gossip одна оговорка. Это реальный путь в сеть, но переход `queued → sent` его не ждёт: он отдаёт задания ограниченному пулу, который сбрасывает под нагрузкой, поэтому единственным приёмником его никогда не считают. Опасен он был гейтом, а не подтверждением.

### Порядок

Отправки уходят в фоновые горутины, поэтому порядок, в котором они достигают провода, задаёт планировщик, а не мы; а обход Go-карты рандомизирован на каждый процесс, так что порядка не было изначально. Обе проблемы исчезают, как только в полёте не больше одного сообщения на получателя.

Гарантия, которую даёт очередь, точна и уже, чем «хвост приходит по порядку»:

> Ни разу не отправленное сообщение никогда не обгоняется более новым.

Это та граница, которую воспринимает читатель. После того как сообщение ушло, копия либо уже у получателя, либо в пути, поэтому более новое, обогнавшее его *на стороне отправителя*, не переставляет ничего из того, что получатель увидит, — а разрешение обгонять как раз и не даёт одной потерянной квитанции застопорить всё, что за ней, до конца одиннадцатиминутного backoff-а. Поэтому `pickQueueHeadLocked` разбирает два случая по-разному: ни разу не отправленное сообщение держит своё место в очереди, даже когда ещё не наступил его срок, а уже отправленное, досиживающее backoff, уступает.

Единственное исключение — сообщение, замороженное удалением. У заморозки нет собственного срока: она заканчивается коммитом или откатом очистки, — поэтому замороженное сообщение пропускается, а не держит очередь. Залипшая заморозка должна стоить своего сообщения, но не всей переписки.

То же правило связывает и ВТОРУЮ половину очередной дисциплины, и прочтение его только в выборе — как раз то, чем оно было сломано: когда квитанция освобождает слот, `promoteQueueHeadLocked` подтягивает самое старое сообщение, которое ЕЩЁ не уходило, а не просто самое старое из оставшихся. Уже отправленное этого слота не ждёт — его расписание отмеряет его собственную отправку, — поэтому его продвижение переотправляет копию, которая у получателя уже есть, И, поскольку повтор снова занимает слот на целое окно, задерживает сообщение, не уходившее ни разу.

```mermaid
sequenceDiagram
  autonumber
  participant S as Отправитель
  participant R as Получатель

  Note over S: msg1..msg4 в очереди — R был оффлайн
  Note over S,R: R вернулся: сессия установлена → kickDeliveryRetriesForReachable
  S->>R: msg1
  R-->>S: delivered(msg1)
  Note over S: квитанция освобождает слот,<br/>promoteQueueHeadLocked подтягивает самое старое<br/>сообщение, которое ЕЩЁ не уходило
  S->>R: msg2
  R-->>S: delivered(msg2)
  S->>R: msg3
  Note over S,R: квитанция на msg3 потеряна
  Note over S: через deliveryQueueWindow (20 с)<br/>msg3 теряет место в очереди
  S->>R: msg4
  S->>R: msg3 (повтор, тот же MessageID — R дедуплицирует молча)
  R-->>S: delivered(msg3)
```

**Диаграмма: слив накопленной очереди вернувшемуся получателю, включая одну потерянную квитанцию**

Окно существует ради того, чтобы потерянная квитанция стоила места в очереди ОДНОМУ сообщению, а не замораживала всю переписку за ним. Оно обязано оставаться меньше кратчайшего интервала повтора (`deliveryRetrySchedule[0]`, 30 с), иначе сообщение, снова ставшее due, забирало бы обратно слот, который только что освободило, и морило бы голодом всё, что за ним; неравенство закреплено тестом.

## Как будят удержанное сообщение

Удержанное сообщение не ждёт своего опроса. Каждое событие, способное сделать получателя достижимым, вызывает `kickDeliveryRetriesForReachable`, который подтягивает расписание подходящих held-записей к «сейчас» — не трогая счётчик попыток, поэтому сам по себе kick ничего исчерпать не может.

| Точка вызова | Событие |
|---|---|
| `service.go` (`handleAuthSession`, после `auth_ok`) | входящий пир прошёл аутентификацию |
| `peer_sessions.go` | исходящая сессия вошла в главный цикл |
| `routing_announce.go` (drain) | принят анонс маршрута |
| `routing_announce.go` (`triggerDrainForExposed`) | стали доступны резервные маршруты |
| `routing_query.go` | ответ на `route_query` дал маршрут |

Входящий срабатывает строго ПОСЛЕ записи `auth_ok`, а не в `trackInboundConnect`, где прямой маршрут на самом деле и добавляется: разбуженный там тик мог бы положить `push_message` в соединение раньше ответа на рукопожатие, которого пир ещё ждёт. Та же граница и по той же причине, что и реплей backlog-а.

Две гонки могли бы проглотить сигнал пробуждения, обе закрыты:

- Kick, попавший между проверкой достижимости при первичной отправке и следующей за ней регистрацией, не находит, что будить. Поэтому удержанная запись регистрируется **уже наступившей**: расписание backoff-а отмеряет ожидание КВИТАНЦИИ, а у сообщения, не дошедшего до провода, никакой квитанции не будет.
- Kick, попавший между lock-free выборкой достижимости в тике и моментом записи `Held`, видит запись ещё не удержанной — то есть не видит её вовсе. `armDueDeliveries` сверяет `lastReachabilityKickAt` с началом своего прохода и оставляет такую запись наступившей, вместо того чтобы парковать её на интервал опроса.

Каждый kick сам перепроверяет достижимость (`router.Route`), поэтому точка вызова, сообщившая identity, которая на деле не является пригодной целью доставки, не порождает пробуждения и ничего не тратит. `deliveryHoldPollInterval` (60 с) — страховка под всем этим: это локальный просмотр таблицы маршрутов, поэтому пропущенное событие стоит минуты, а не часа.

Kick двигает ТОЛЬКО записи, которые ждали достижимости (`holdUnreachable`). Остальные два состояния уже отправлены по текущей достижимости: `holdNone` подтверждён и ждёт receipt по своему backoff, а `holdUnconfirmed` отдан приёмникам, и кадр никто не взял, поэтому он припаркован на интервал опроса. И приём announce, и ответы на route-query намеренно передают в kick `routing.RouteUnchanged`: маршрут, уже лежащий в таблице, может стать пригодным ровно в тот момент, когда пир ответил. Но ПЕРЕподтверждение маршрута ничего не говорит об уже отправленном сообщении, и пробуждение таких записей превращало каждое периодическое объявление в переотправку и запись в журнал — с частотой объявлений, на маршруте, который резолвится, но фактически мёртв.

Отдельно: получатель, который дозвонился ДО НАС, получает свои удержанные сообщения через другую дверь — `pushBacklogToSubscriber` реплеит in-memory backlog топика `dm` сразу после `auth_ok`, и этот путь гейтом удержания не закрыт. Заменой движку ретраев он не является: backlog живёт в памяти и не переживает перезапуск, а срабатывает только когда отвечающая сторона — мы.

## Границы жизни сообщения

| Граница | Значение | К чему применяется | Эффект |
|---|---|---|---|
| `TTLSeconds` | 0 для обычных DM | сообщения с флагом авто-удаления | брошено как `expired` |
| `CancelOutgoingDelivery` | — | любое сообщение | отозвано автором |
| лимит попыток | 20 (`CORSA_DELIVERY_RETRY_MAX_ATTEMPTS`) | только исходящие SEEN-КВИТАНЦИИ | узел перестаёт ждать `seen_ack` |
| `seenReseedHorizon` | 7 суток | стартовый пересев seen-квитанций | более старые не поднимаются |

Сообщения встречаются в этой таблице дважды, и оба раза это то, о чём кто-то попросил. Строки про возраст и про число попыток нет намеренно: ожидание ничего не стоит на проводе, потому что гейт достижимости не отправляет ничего, пока получатель недостижим, а стартовый пересев недоставленных сканирует вообще без горизонта — чтобы перезапуск тоже не был способом сдаться.

У seen-квитанций лимит остаётся, потому что их чинит дальняя сторона: каждое прибытие `seen` заставляет исходного отправителя ответить `seen_ack`, так что брошенная квитанция запускается заново, а не теряется.

Цена этого реальна и принята: `awaitingDelivered` держит по записи на каждое недоставленное сообщение всё время работы процесса, а стартовое сканирование читает все строки chatlog, оставшиеся в `sent`.

## Принятые ограничения

1. **Обе стороны должны быть в сети одновременно.** Если отправитель ушёл в оффлайн раньше, чем вернулся получатель, никто в сети не держит сообщение за него. Это цена отказа от почтового ящика, и это принятое решение, а не пробел, — см. [relay.md](relay.md) INV-3 и `transit_retention.go`.

2. **Скорость слива хвоста задаёт тик.** Одно сообщение на получателя за 2-секундный тик, когда квитанции ходят, и одно за `deliveryQueueWindow`, когда нет. Тысяча накопленных сообщений уходит примерно за полчаса в хорошем случае — это принято, потому что хвост такого размера означает, что отправитель продолжал писать человеку, чьи сообщения на глазах не доставлялись. Получателю, который дозвонился ДО НАС, весь in-memory backlog отдаётся разом через `pushBacklogToSubscriber`, мимо этой очереди.

3. **Недоставленные сообщения накапливаются.** По одной записи `awaitingDelivered` на каждое недоставленное сообщение на всё время работы процесса, плюс стартовое сканирование всех строк chatlog, оставшихся в `sent`. Это прямая цена отсутствия горизонта, и на практике её ограничивает ровно то же, что видит пользователь: количество сообщений без подтверждения.

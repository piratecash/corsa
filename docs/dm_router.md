# DMRouter — Service Layer

## English

### Overview

The `DMRouter` is the central service layer between the network node and the desktop UI.
It owns all DM business logic: event routing, sidebar management, conversation cache,
health polling, mark-seen, and message sending. The UI communicates with it through
a small, well-defined public API.

Source: `internal/core/service/dm_router.go`

### Modular layered architecture

The desktop application follows a strict modular layered architecture
designed for clean separation of concerns and easy extensibility:

```mermaid
flowchart TB
    subgraph L1["Layer 1 — Network (node.Service)"]
        direction LR
        TCP["TCP connections\ngossip / relay"]
        MSTORE["MessageStore interface\n(delegates persistence\nto registered handler)"]
    end

    subgraph EBUS["Event Bus (ebus.Bus)"]
        direction LR
        TOPICS["Topics:\n• message.new\n• receipt.updated\n• peer.health.changed\n• slot.state.changed\n• peer.traffic.updated\n• route.table.changed\n• contact.added/removed\n• identity.added\n• aggregate.status.changed\n• version.policy.changed\n• message.sent / file.sent"]
    end

    subgraph L2["Layer 2 — Service (DesktopClient + DMRouter)"]
        direction LR
        CHATLOG["chatlog.Store\n(SQLite, owned by\nDesktopClient)"]
        BUSINESS["Business logic:\n• event routing\n• sidebar management\n• conversation cache\n• mark-seen\n• message sending"]
        SNAPSHOT["Snapshot() → immutable\nRouterSnapshot"]
        API["Public API:\nSelectPeer() / SendMessage()\nSetSendStatus()"]
        UIEVENTS["UIEvent channel\n(buffered 32, non-blocking)\nincl. UIEventBeep"]
    end

    subgraph L3["Layer 3 — UI (Window)"]
        direction LR
        GIO["Gio widgets\n(pure rendering)"]
        FRAME["Per-frame:\nSnap → render → done"]
        BEEP["go systemBeep()\n(overlapping playback)"]
    end

    L1 -->|"Publish()"| EBUS
    EBUS -->|"Subscribe()"| L2
    EBUS -->|"Subscribe()"| L3
    L2 --> L3
    MSTORE -->|"StoreMessage()\nUpdateDeliveryStatus()"| CHATLOG
    UIEVENTS -->|"UIEventBeep"| BEEP

    style L1 fill:#1a2332
    style EBUS fill:#2a1a33
    style L2 fill:#1e3050
    style L3 fill:#22364a
```

*Diagram 1 — Modular layered architecture overview*

Each layer communicates with the next through a well-defined interface:

- **Network → ebus**: node publishes short delta events (peer health, messages, receipts, routing changes) via `ebus.Bus.Publish()`
- **ebus → Service**: DMRouter subscribes to all relevant topics; handlers are async (64-slot inbox per subscriber, dedicated drain goroutine)
- **ebus → UI**: the console modal subscribes directly to peer health and aggregate status topics for real-time updates while it is open
- **Service → UI**: `UIEvent` channel (non-blocking notifications) + `Snapshot()` (read-only state copy)
- **UI → Service**: method calls (`SelectPeer`, `SendMessage`, `ConsumePendingActions`)
- **RPC** remains for commands/queries (fetch messages, send messages, get routing table). RPC handlers may publish ebus events as side effects

No layer reaches past its neighbor. The UI never touches `DesktopClient`
or SQLite directly. The router never manipulates Gio widgets.
Node does not own message persistence — it delegates to a `MessageStore` handler
registered by `DesktopClient` at construction time. Relay-only nodes
(`corsa-node`) leave `MessageStore` nil and relay messages without persisting them. This makes
it straightforward to add new features (group chats, file transfers, etc.)
by extending the router layer without touching the UI, or to swap the UI
framework entirely without modifying business logic.

### Three-layer architecture (Network → Service → UI)

The desktop application uses a clean three-layer architecture:

```mermaid
flowchart TB
    subgraph NET["Network Layer (node.Service)"]
        NODE["Local node\n(TCP, gossip, relay)"]
        MSTORE["MessageStore\n(callback interface)"]
    end

    subgraph EBUS["ebus.Bus"]
        EB["Async event bus\n(64-slot inbox,\ndedicated drain goroutine)"]
    end

    subgraph SVC["Service Layer (DesktopClient + DMRouter + NodeStatusMonitor)"]
        DC["DesktopClient\n(desktop.go)\nholds chatlog.Store\nimplements MessageStore"]
        DR["DMRouter\n(dm_router.go)"]
        NSM["NodeStatusMonitor\n(node_status_monitor.go)\nowns NodeStatus"]
        CACHE["ConversationCache\n(active chat only)"]
        STATE["Router State\n(peers, peerOrder, activePeer,\nactiveMessages, etc.)"]
        UIEVENTS["UIEvent channel\n(buffered 32, non-blocking)"]
    end

    subgraph UI["UI Layer (window.go)"]
        WIN["Window\n(Gio widgets only)"]
        SNAP["RouterSnapshot\n(immutable per frame)"]
        BEEP["go systemBeep()\n(notify.go — overlapping\nplayback via oto)"]
    end

    NODE -->|"Publish(TopicMessageNew,\nTopicReceiptUpdated, ...)"| EB
    EB -->|"Subscribe(TopicMessageNew,\nTopicReceiptUpdated)"| DR
    EB -->|"Subscribe(TopicPeerHealth*,\nTopicAggregate*, ...)"| NSM
    EB -->|"Subscribe()"| WIN
    NSM -->|"onChanged → NotifyStatusChanged()"| DR
    MSTORE -->|"StoreMessage()\nUpdateDeliveryStatus()"| DC
    DR --> CACHE
    DR --> STATE
    DR -->|"notify(UIEventBeep)\nnotify(UIEvent*Updated)"| UIEVENTS
    UIEVENTS -->|"Subscribe() → for ev := range"| WIN
    UIEVENTS -->|"ev.Type == UIEventBeep"| BEEP
    WIN -->|"Snapshot()"| SNAP
    WIN -->|"SelectPeer() / SendMessage()"| DR
    WIN -->|"ConsumePendingActions()"| DR
```

*Diagram 2 — Three-layer architecture with data flow*

**DesktopClient** (`internal/core/service/desktop.go`) is the composition
root for the desktop sub-services. It no longer holds the SQLite handle
itself; `ChatlogGateway` owns `chatlog.Store` and `MessageStoreAdapter`
satisfies `node.MessageStore`. At construction, `NewDesktopClient` wires
all sub-services (`AppInfo`, `LocalRPCClient`, `ChatlogGateway`,
`MessageStoreAdapter`, `DMCrypto`, `NodeProber`) and registers the
adapter with `node.Service` via `RegisterMessageStore()`. The node calls
`StoreMessage()` / `UpdateDeliveryStatus()` on the adapter before
publishing `TopicMessageNew` / `TopicReceiptUpdated` via ebus, maintaining
the "DB first, then event" invariant. Public `DesktopClient` methods are
thin delegators — callers that want a narrower dependency can reach
through the sub-service accessors (`DMCrypto()`, `NodeProber()`,
`ChatlogGateway()`, `RPC()`, `AppInfo()`).
`FetchConversation`, `FetchConversationPreviews`, `FetchSinglePreview`, and
`MarkConversationSeen` live on `DMCrypto` (exposed through the `DesktopClient`
delegators). They accept `context.Context` and propagate it through
`LocalRPCClient.LocalRequestFrameCtx` — the context-aware variant of
`LocalRequestFrame`. In TCP mode, the context fully controls the dialer
deadline. In embedded mode, `ctx.Err()` is checked before and after
`HandleLocalFrame` as a best-effort gate (the synchronous handler itself
cannot be interrupted). Contact fetching is deduplicated via
`DMCrypto.fetchContactsForDecrypt(ctx, senders)` (shared by all three
Fetch methods), which skips the local identity address when checking for
missing senders to avoid spurious `fetch_contacts` roundtrips on
conversations with outgoing messages.

**DMRouter** (`internal/core/service/dm_router.go`) owns DM business logic:
event routing, sidebar management, conversation cache, mark-seen, message
sending. Network-layer state aggregation (PeerHealth, AggregateStatus,
contacts, reachability) is delegated to **NodeStatusMonitor**
(`internal/core/service/node_status_monitor.go`), which DMRouter accesses
through the `NodeStatusProvider` interface. The UI communicates with DMRouter
through a small public API.

**Window** (`internal/app/desktop/window.go`) is a pure rendering layer. It has
no `sync.Mutex`, no direct access to `DesktopClient`, and no business logic.
At the start of each frame it calls `Snapshot()` to get an immutable copy of
router state, then renders from that snapshot.

### Public API

| Method | Direction | Description |
|---|---|---|
| `Subscribe()` | Router → UI | Returns `<-chan UIEvent` for change notifications |
| `Snapshot()` | UI → Router | Returns immutable `RouterSnapshot` under `RLock` |
| `ConsumePendingActions()` | UI → Router | Atomically reads and clears deferred widget mutations |
| `SelectPeer(id)` | UI → Router | User click: delegates to `selectPeerCore(id, true)`. Switches peer, clears stale messages, **optimistically clears unread badge** (with rollback) and emits `UIEventSidebarUpdated` synchronously, then loads conversation and sends seen in background. If `loadConversation` or `doMarkSeen` fails, the badge is **restored** via `restorePeerUnread`. Same-peer re-click: retries a failed load (cache mismatch) or retries `doMarkSeen` when `Unread > 0` (stuck badge after rollback). Same-peer with valid cache and `Unread == 0` is a no-op. |
| `AutoSelectPeer(id)` | UI → Router | Programmatic auto-select: delegates to `selectPeerCore(id, false)`. When the peer changes, behaves identically to `SelectPeer`: clears unread badge, loads conversation, sends seen receipts with rollback on failure. When the peer is the same (re-selection), it is a **true no-op** — no unread clear, no `doMarkSeen`, no UI events, no goroutines launched. This prevents redundant UI churn from programmatic re-selection. |
| `SendMessage(to, body)` | UI → Router | Encrypts and sends DM |
| `ActivePeer()` | UI → Router | Returns current active peer address |
| `MyAddress()` | UI → Router | Returns local identity address |
| `SetSendStatus(s)` | UI → Router | Updates send status text |
| `Start()` | UI → Router | Subscribes ebus events, launches `runStartup` goroutine |

### Key types

```go
type RouterSnapshot struct {
    ActivePeer     domain.PeerIdentity
    PeerClicked    bool
    Peers          map[domain.PeerIdentity]*RouterPeerState
    PeerOrder      []domain.PeerIdentity
    ActiveMessages []DirectMessage
    CacheReady     bool       // true when cache is loaded for ActivePeer
    NodeStatus     NodeStatus
    SendStatus     string
    MyAddress      domain.PeerIdentity
}

type RouterPeerState struct {
    Preview        ConversationPreview
    LastIncomingAt domain.OptionalTime // when the peer last wrote to us
    Unread         int
}

type PendingActions struct {
    ScrollToEnd   bool
    ClearEditor   bool
    ClearReply    bool
    RecipientText domain.PeerIdentity
}

type UIEventType int
const (
    UIEventMessagesUpdated UIEventType = iota + 1
    UIEventSidebarUpdated
    UIEventStatusUpdated
    UIEventBeep
)
```

`LastIncomingAt` is the sidebar's chat-derived half of "last online": the
newest message this peer wrote. It lives only in memory — `seedHistoryEvidence`
recomputes it from the chatlog at startup (off the startup path, in its own
goroutine, retrying a failed read three times, and it publishes a snapshot
itself: `Snapshot()` serves a cache only `notify` rebuilds, and on the retry
path there is no later event to ride on — the same rule the post-delete retry
sweep follows),
every incoming message advances it, and the delete path recomputes it — because a durable copy would be a second
value to keep in step with the rows it comes from. It is not an observation
and ranks below anything the node saw itself. `Preview` and `LastIncomingAt`
answer different questions and are written by one helper,
`setPeerPreviewLocked`. `Preview` is the last row of the thread,
whoever wrote it; `LastIncomingAt` moves only when the peer is the sender, and
only forward, so out-of-order history (startup replay, a relayed message that
took the long way) cannot walk it backwards. Every path that learns of a new
message goes through that helper — assigning `Preview` alone would leave the
presence evidence behind on whichever path forgot it. The delete path is the
single exception and assigns `LastIncomingAt` directly: it recomputes the
value from SQL through `LastIncomingAtFor`, because the message that carried
the evidence may be the row the user just removed, and clears the field when
no incoming message survives.

`Preview`, `Unread` and `LastIncomingAt` are the peer state DERIVED from the
chatlog, and they are fed by two sources that cannot be ordered against each
other: a SQL read and the event stream. The database is AHEAD of the events —
a message is committed before the event announcing it is delivered — so the
same message reaches the sidebar twice, in either order. Rather than policing
that with versions, the merges are made idempotent, which removes the ordering
question instead of answering it:

- **Unread is a set of message ids**, not a counter. `RouterPeerState.Unread`
  is its size. Adding an id the set already holds changes nothing, so a
  message counted by both the startup read and its own event is one unread
  message. Reading (only the ids whose receipts were actually sent), deleting
  and the post-delete reconciliation remove ids. Two places re-derive the set
  from `delivery_status`, because the event stream carries no status and can
  therefore only ever add: the post-delete reconciliation, and the rebuild
  after a conversation failed to open (`repairBadgeFromStore`, where the
  rollback has only what was in memory to restore and a half-completed
  mark-seen may have left ids the database now calls read). Both keep the ids
  the database does not hold at all, and the rebuild also keeps whatever
  arrived while it was reading — additions move no counter, so its epoch
  check cannot see them. It keeps the
  badged ids the database does not hold AT ALL (`StoredMessageStatuses`): a header
  can badge a message before its row is written, and "absent from the unseen
  list" would otherwise read as "read" for an id nothing can re-add.
- **LastIncomingAt is a maximum** over the incoming timestamps, so a late
  reader can only lose.
- **Preview takes the newer message**, comparing timestamps; an older read
  never overwrites a newer one.

Idempotent merges order ADDITIONS against each other and nothing else. A read
taken before something moved the peer BACKWARDS — a deletion, a mark-seen, an
optimistic clear, a removal — and applied after it puts back exactly what was
removed, and no rule about maxima or sets prevents that. So every peer carries
`backwardsEpoch`, bumped by each of those movers, and every chatlog-derived
write follows one rule: **capture the epoch before its own query, apply only
if it is unchanged.** A changed epoch means the answer describes a
conversation that no longer exists in that form, and the work is redone rather
than merged. Each read captures its own snapshot immediately before its own
query: two reads sharing one baseline would make the second refuse everything
the first one's retries allowed to change.

It is TWO counters, because the two kinds of backwards move are not the same.
`unread` counts what lowers only the BADGE — a mark-seen, the optimistic clear
when a conversation is opened — and `history` counts what removes ROWS: a
message deletion, a conversation wipe, a contact removal, an identity reset. A
history move bumps both; a mark-seen bumps only `unread`, because it cannot
make a last-incoming answer wrong. One counter for both would cost the feature
its most common case: the conversation that opens automatically at launch is
marked read while the startup scan is still running, so its contact — the
first row in the sidebar — would spend the whole session with no "last online"
line. The startup scan, `seedPreviews`, the post-delete reconciliation, the
header repair and the badge rebuild after a failed open all go through this
check; the
`peerGen` lifecycle generation answers the narrower question of whether the
contact still exists at all, and is captured before any slow step (a decrypt
RPC, a header scan) that would otherwise recreate its row — including the
side effects, not just the row: an inbound file announcement is registered
only after that check, and never speculatively-then-rolled-back, because a
rollback by message id cannot tell its own registration from an identical
one made by a newer generation and would take that one's downloaded file
with it. Every slow step carries the same pair — the
generation AND the history counter — as one `peerStamp`, because the two
answer different questions and a branch that checks half of it is a branch
that applies a message whose row is already gone. A step that ASKS the
database something and then acts on the answer takes its stamp BEFORE the
question and checks it at the commit: a fresh stamp would describe the wrong
moment.

The version check and the file registration cannot be one atomic step by
themselves — the check needs the router lock and the registration goes
through the file bridge, which a domain mutex may never be held across. They
are made atomic against DELETION instead, by a per-peer file barrier: every
path that cleans transfers up takes it, moves the history counter under it,
and only then cleans up, while a registration holds it from its check until
the mapping exists. A registration already in flight therefore either
finishes first or finds the moved counter and stands down. One deletion is
one move of that counter, and a cleanup that removed nothing does not move
it at all — an ack usually names a row deleted long ago, and every false move
marks a load or a decrypt that is perfectly current as stale.

Removing a contact holds that same barrier across its whole tail: the version
move, the transfer cleanup and the drop of the in-memory state. A
registration that was already waiting on it therefore resumes after the last
cleanup and finds both a new generation and no row — which is why the
registration checks the row's existence too, not only the stamp. The mutex
itself is never dropped when the contact is: a waiter already holds a pointer
to it, and replacing it on re-add would leave the old cleanup and the new
registration excluding nobody.

One more thing is needed while a removal runs, because no stamp can express
it: the removal bumps the counters itself, so a message arriving right after
that carries a stamp which MATCHES, and the apply would create the row again
— behind the removal, for the cleanup to leave orphaned.

That is the job of the **removal gate** (`removalGate`) — one object with two
doors. `tryEnsurePeerLocked` consults it before creating a sidebar row, and
the message store adapter consults it before writing an inbound DM, because
the store is the door the node's own writes go through: the node persists a
message BEFORE the router hears about it, so a message accepted mid-removal
would land in the database no matter what the router refuses afterwards, and
the next startup would rebuild the deleted conversation out of that row. The
store answers `StoreDeferred` — not stored and not dropped: the sender keeps
the message and re-delivers it once the removal is over, at which point it is
simply a new message to a conversation that no longer exists, and opens it
again as a message from any stranger would. This is a window, not a ban.

The store's door is a **lease**, not a check. Checking is not writing: a
store let through can be stopped for as long as the database takes, and a
removal that only read a flag would run both of its history deletes in that
gap, leaving the row behind them where nothing looks again. So the store
takes the lease (`admitWrite`) before anything else and holds it until its
row is committed, and `begin` does not return until every lease already
handed out for that conversation is back. After `begin` returns, the removal
knows two things it could not know from a flag: no write is in progress, and
no new one will be admitted. The router's row check needs no lease — it
decides and acts under one lock, with no I/O in between.

The gate goes up as the FIRST statement of `RemovePeer`, before the history
delete and before the file barrier: raised later, it would be open for
exactly the length of those waits, and that is the window a concurrent write
walks through. It is counted, not flagged, because two removals of the same
contact can overlap and the first to finish must not open the door under the
second. A write that was already past the store door when the gate went up is
covered by one last history sweep at the end of the removal — and if that
sweep FAILS, `RemovePeer` returns the error: the in-memory state is gone
either way and the UI is told either way, but reporting a contact as removed
while its history may still be on disk is the one answer this function must
not give.

Both the removal and the **conversation wipe** also STOP the reaction send queue
for the length of the delete (`HoldReactionSends`), before raising or while
holding the gate. The gate reaches writes and re-offers; it does not reach the
queue, which by then may already hold facts of this conversation resolved from
the record — and a pass that read them a moment earlier would hand its frame over
after the rows are gone, with the queue's own clearing afterwards only waiting
for a frame that has already left.

The **conversation wipe** raises the same gate, around its transaction and
around the drop of the reaction queue. Its own barrier (`convDeleteRetry`)
stops this node's own sends and says nothing to the paths that write the
conversation from the side: the reaction re-offer reads a page of the user's
facts and hands a COPY of them to the node's outbox, so a wipe landing between
those two steps deletes rows that are already on their way out again, and then
empties a queue the callback refills a moment later. `begin` waits for the
lease such a re-offer already holds and refuses new ones until the queue has
been dropped too. Incoming messages are deferred for that window exactly as
during a contact removal — and a message arriving after the wipe is outside it
on both sides in any case.

The two failures a removal can report are not the same failure, so callers
can tell them apart with `errors.Is(err, ErrHistorySweepFailed)`. The FIRST
history delete fails before anything is touched: the contact is still there,
and the caller must leave its own state alone. The FINAL sweep fails when the
contact is already out of the sidebar, the cache and the trust store, with
only its history in doubt: the caller has to finish its own cleanup — drafts,
attachments, aliases, picking the next conversation — and report the failure,
because stopping there would strand the composer state of a conversation the
user can no longer open and leave the deleted chat selected. Only the HISTORY half is
compared: marking a conversation read bumps the unread counter and removes
no rows, so comparing the whole pair would make opening a chat discard the
messages arriving into it and refuse its own file transfers.

A history conflict is not a reason to DROP the message. The counter is per
peer, so a deletion anywhere in the conversation looks exactly like the
deletion of the row being decrypted — and the message's id is already
through the dedup gate, so a wrong guess loses it for good. The conversation
is re-read from the database instead: the reconciliation restores the
preview and the last-online evidence, the badge is re-derived from
`delivery_status`, and the message either comes back with them or does not,
which is the distinction the counter could not make.

The same "the answer is older than the work" rule governs a message being
decrypted: which conversation is on screen is re-read after the decrypt
(against the selection AND the cache), because appending to a cache that
has since been loaded for someone else splices the message into the wrong
thread, and treating it as visible skips its badge for good.

Deletion is the single exception: it is the only step that legitimately moves
these values BACKWARDS, because the row they described is gone. It is
therefore the only path that needs ordering, and it gets it from a per-peer
refresh lock — two deletions in one conversation run in their own goroutines,
and the slower query must not land last with the older answer. Its reads are
all-or-nothing (half a read publishes a moment that never existed), and a
failed one is queued and retried by the delete sweep, because nothing else
re-reads a peer's history.

A reconciliation UPDATES a peer and never CREATES one. It runs asynchronously,
so the conversation may have been removed before it was scheduled; callers
that introduce a new one create the row themselves, synchronously with the
event that justifies it.

### Concurrency protection

The DMRouter runs two background goroutines:

- **Startup goroutine** (`runStartup`) — runs `initializeFromDB` to load
  previews, contacts, identities, and diagnostic fields from SQL. While
  startup is in progress, ebus events are buffered in `startupEventBuf`
  (capped at 256 entries to prevent memory spikes). After initialization,
  buffered events are replayed under `replayingStartup=true`, which
  suppresses the BEEP and nothing else: the badge is a set, so a message
  counted by both a SQL read and its own replayed event is one unread
  message, while suppressing the replay would lose the messages stored
  after the read was taken. Events that arrive during Phase 1 replay are
  re-buffered and processed as live in Phase 2 (`replayingStartup=false`),
  where the beep is no longer suppressed.
- **ebus subscriptions** — DMRouter subscribes only to DM-specific topics
  in `subscribeEvents()` before startup so no events are missed:
  - `TopicMessageNew` / `TopicReceiptUpdated` — new DMs and delivery
    receipt changes (buffered before startup, processed by `handleEvent`)
  - `TopicMessageSent`, `TopicMessageSendFailed` (send results, published
    by DMRouter itself after send operations complete)
  - `TopicFileSent`, `TopicFileSendFailed` (file send results)

  Network-layer ebus topics are handled by **NodeStatusMonitor**
  (`node_status_monitor.go`), which subscribes to:
  - `TopicPeerHealthChanged` (peer state/connected/score/ping/pong).
    PeerHealth rows are keyed by `(Address, ConnID)` composite key.
    `peerHealthFrames()` emits multiple rows for the same overlay address
    when several inbound connections exist, each distinguished by ConnID.
    The delta carries the outbound `ConnID` (0 when no outbound session)
    and the full set of active `InboundConnIDs` — this gives the monitor
    a complete view of the connection topology for reconciliation.

    `applyPeerHealthDelta` uses a 5-step reconciliation model:
    1. **Build expected ConnIDs** from delta (`ConnID` + `InboundConnIDs`).
    2. **Update existing rows**: outbound row gets full session-scoped write
       (`writeSession=true`); inbound rows receive address-level fields only
       (`writeSession=false`) — their ConnID and Direction are immutable
       row identifiers. A `ConnID=0` placeholder is promoted if the delta
       carries a specific outbound ConnID. When the delta carries live
       `InboundConnIDs` and `ConnID=0`, the placeholder is left untouched
       in step 2 — it will be pruned in step 5 and its address-level slot
       metadata (`SlotState`, `PendingCount`) migrated onto the surviving
       per-ConnID rows. Mutating the placeholder here would clobber those
       fields with the health delta's values before migration could
       capture them.
    3. **Create outbound row** if no matching row was found. For `ConnID=0`
       deltas (no outbound session), a new row is only created when no rows
       exist for the address or the peer is disconnected (the surviving
       address-level row after pruning).
    4. **Create inbound rows** for `InboundConnIDs` not yet present — these
       carry `Direction="inbound"` and address-level fields from the delta.
    5. **Prune dead connection rows** whose ConnID is no longer in the
       expected set. A `ConnID=0` "address row" is pruned when per-ConnID
       rows authoritatively represent the address (`expectedConnIDs`
       non-empty); it survives otherwise. Before the placeholder is
       dropped, its `SlotState` and `PendingCount` — which ride on
       `TopicSlotStateChanged` / `TopicPeerPendingChanged`, not on
       `PeerHealthDelta` — are migrated onto surviving per-ConnID rows
       where those fields are still empty, so a prior `applySlotStateDelta`
       value on an existing inbound row is never stomped. Pruning also
       fires on full disconnect (`!delta.Connected`) even when
       `expectedConnIDs` is empty — all per-ConnID rows are dead and the
       freshly-created `ConnID=0` row from step 3 carries the
       disconnected state.

    Session-scoped fields (Direction, ClientVersion, ClientBuild, ConnID,
    ProtocolVersion) are cleared unconditionally on disconnect deltas
    (`!delta.Connected`) and backfilled only when zero/empty on connect.

    During probe merge, `mergePeerHealth` indexes by `(Address, ConnID)`
    via the `peerHealthKey` struct, so multiple per-ConnID probe rows for
    the same overlay address are preserved, not collapsed. It uses
    `ebusHealthSeeded` and two-tier enrichment. Addresses that received at
    least one `applyPeerHealthDelta` are "seeded": state fields (Connected,
    Score, State, PendingCount, ConsecutiveFailures, LastError),
    session-scoped fields (Direction, ClientVersion, ClientBuild, ConnID,
    ProtocolVersion), slot-lifecycle fields (SlotState, SlotRetryCount,
    SlotGeneration, SlotConnectedAddr), and the full diagnostic block —
    ebus-authoritative after the switch to one-shot `FetchAndSeed()` —
    (BannedUntil, LastErrorCode, LastDisconnectCode,
    IncompatibleVersionAttempts, LastIncompatibleVersionAt,
    ObservedPeerVersion, ObservedPeerMinimumVersion, VersionLockoutActive)
    are all authoritative and never overwritten by probe. Zero/nil values
    are meaningful signals (disconnect clears session metadata, slot
    removal clears SlotState, `resetPeerHealthForRecoveryLocked` clears
    bans and diagnostics after successful recovery — every
    `PeerHealthDelta` carries the complete current diagnostic value, so a
    probe backfill would resurrect stale state that the node already
    cleared). Only truly persistent fields (PeerID, activity timestamps,
    traffic counters) are backfillable via
    `enrichPeerHealthIdentityFromProbe` — this handles the case where
    PeerID is resolved out-of-band after the first health delta.
    True placeholders (from `applySlotStateDelta`/`applyPeerPendingDelta`
    without a health delta) get full enrichment via
    `enrichPeerHealthFromProbe`, which does populate the diagnostic block
    from the probe because no ebus delta has claimed authority yet.
  - `TopicPeerPendingChanged` (per-peer pending queue depth; creates a
    minimal `PeerHealth` entry when the peer is not yet known so the count
    is not lost before the first health delta arrives). Address-level:
    updates ALL per-ConnID rows for the address.
  - `TopicPeerTrafficUpdated` (byte counters, ~2 s batch). Address-level:
    updates ALL per-ConnID rows for the address.
  - `TopicSlotStateChanged` (CM slot lifecycle). Address-level: updates
    ALL per-ConnID rows for the address.
  - `TopicAggregateStatusChanged`, `TopicVersionPolicyChanged`
  - `TopicContactAdded/Removed`, `TopicIdentityAdded`
  - `TopicRouteTableChanged` (route-based reachability tracking — on
    every routing table modification the monitor rebuilds `ReachableIDs`
    from `BuildReachableIDs()`, which reads the authoritative routing
    snapshot. This covers direct-peer add/remove, announcement acceptance,
    transit invalidation, and TTL expiry. The `routingTableTTLLoop`
    emits `TopicRouteTableChanged` with reason `"ttl_expired"` whenever
    `TickTTL()` removes one or more expired routes, ensuring the monitor
    learns about reachability changes even when no explicit routing
    mutation triggered them.)

  Each monitor handler updates `NodeStatus` under its own `mu` and calls
  the `onChanged` callback, which triggers `DMRouter.NotifyStatusChanged()`
  to rebuild the snapshot.
- **UI goroutine** — calls `Snapshot()`, `ConsumePendingActions()`,
  `SelectPeer()`, `SendMessage()` from the Gio event loop.

To prevent data races (which cause Go runtime fatals that are uncatchable):

1. `mu sync.RWMutex` (on DMRouter) — protects all shared router fields:
   `activePeer`, `peerClicked`, `peers`, `peerOrder`, `activeMessages`,
   `seenMessageIDs`, `initialSynced`, `replayingStartup`,
   `sendStatus`, `pendingScrollToEnd`, `pendingClearEditor`,
   `pendingRecipientText`, and the four per-peer maps: `unreadIDs`
   (the badge sets), `peerGen` (lifecycle generations), `backwardsEpoch`
   (the two backwards-move counters, see below), `pendingDeleteReconcile` (the delete retry queue) and
   `peerRefreshMu` (the per-peer reconciliation locks — the MAP is guarded
   by `mu`, the mutexes in it are not). Note: `NodeStatus` is owned by
   `NodeStatusMonitor` (with its own `mu`), not by DMRouter.

   **Ordering rule**: a per-peer reconciliation lock from `peerRefreshMu` is
   held across SQL reads, so it must never be taken while `mu` is held.
   `peerRefreshLock` looks the mutex up under `mu`, releases `mu`, and only
   then locks it.

   Background goroutines acquire `mu.Lock()` for writes and `mu.RLock()` for
   reads. `Snapshot()` acquires `mu.RLock()` and returns a deep copy.

   **Identity normalization**: All public ingress points (`SelectPeer`,
   `AutoSelectPeer`, `SendMessage`, `RemovePeer`, `peerForMessage`,
   `repairUnreadFromHeaders`) normalize `PeerIdentity` via `normalizePeer()`
   (whitespace trim) before any map/slice access. This prevents
   whitespace-padded identities from creating duplicate keys in `peers` or
   `peerOrder`.

2. **Snapshot pattern** — the UI goroutine never reads router fields directly.
   Instead, `Snapshot()` takes a consistent point-in-time copy under a single
   `RLock`, returning an immutable `RouterSnapshot` struct. The UI reads only
   from this snapshot for the entire frame. This eliminates all lock contention
   in the rendering path.

3. **Widget safety via PendingActions** — Gio widgets (`widget.Editor`,
   `widget.List`, etc.) are NOT thread-safe. Background goroutines set
   deferred action flags (`pendingScrollToEnd`, `pendingClearEditor`,
   `pendingRecipientText`) under `mu`. The UI goroutine calls
   `ConsumePendingActions()` at the start of each frame, which atomically
   reads and clears these flags, then applies them to Gio widgets.

4. **Non-blocking UIEvent channel** — the router sends `UIEvent` values to a
   buffered channel (capacity 32) via `notify()`. If the channel is full,
   each overflowed event gets its own background retry goroutine with
   exponential backoff (50ms → 100ms → 200ms, 3 attempts). An atomic
   counter (`uiOverflowCount`) caps concurrent retry goroutines at 8 to
   prevent accumulation during sustained bursts; events beyond the cap are
   dropped with a warning. This per-event retry ensures distinct event
   types (e.g. `UIEventBeep`) are not silently lost when the channel
   overflows. The UI bridge goroutine calls `window.Invalidate()` for
   each event, triggering a new frame.

5. **Event-driven architecture** — the router logic is split into three clean
   paths:

   **Startup** (`initializeFromDB`): runs once asynchronously so the window
   appears immediately. Fetches conversation previews with retry (up to 3
   attempts with linear backoff) to handle transient DB/node failures.
   Calls `resetIdentityState()` to clear all identity-specific state, then
   `seedPreviews()` to populate the `peers` map (sorted: unread first by
   count desc, then by most recent timestamp). Delegates peer selection to
   `AutoSelectPeer()`, which handles the full lifecycle: optimistic unread
   clear, `loadConversation()`, `doMarkSeen()`, and rollback on failure.
   Before the call, `activePeer` is cleared so `selectPeerCore` always
   sees a peer switch and triggers a full load (important for reconnect
   when `activePeer` was already set). Finally runs an initial
   `pollHealth()` (via `defer`) so DMHeaders, DeliveryReceipts, and
   diagnostic fields are seeded. After startup, ebus events keep all
   UI-critical fields fresh without polling.

   Because ebus events arrive in parallel with `initializeFromDB`,
   `seedPreviews()` guards against the startup race: if the ebus event-path
   already delivered fresher data for a peer (newer timestamp),
   `seedPreviews` skips that peer instead of overwriting with stale startup
   snapshot data.

   `resetIdentityState()` clears `peers`, `peerOrder`, `activePeer`,
   `peerClicked`, `activeMessages`, `seenMessageIDs`, `initialSynced`,
   `sendStatus`, `pendingScrollToEnd`, `pendingClearEditor`,
   `pendingRecipientText`. The `cache` (ConversationCache) is emptied via
   `Load("", nil)` rather than pointer replacement, because event goroutines
   hold a reference to the same cache object and call its methods concurrently.

   **Event handler** (`handleEvent` → `onNewMessage` / `onReceiptUpdate`):
   Active peer detection uses `isActivePeer()` (checks `r.activePeer`
   under lock), NOT `cache.MatchesPeer()`. This is critical because
   during a peer switch, `activePeer` is updated immediately by
   `selectPeerCore()`, but the cache is only updated after
   `loadConversation()` completes asynchronously.

   - New messages for the **active conversation** where the cache is
     loaded are decrypted inline via `DecryptIncomingMessage`, appended
     to `ConversationCache`, and `activeMessages` is refreshed.
     `RouterPeerState.Preview` is updated to reflect the new message.
     If inline decryption fails, `loadConversation` reloads the full
     history and `updatePreviewFromStore` refreshes the preview from
     SQLite. `doMarkSeen` is called for every incoming message — the
     chat is on screen and counts as read.
   - New messages for the **active conversation** where the cache is
     NOT yet loaded (mid-switch) decrypt the message inline via
     `DecryptIncomingMessage` and capture the resulting `*DirectMessage`.
     If decryption succeeds, `RouterPeerState.Preview` is updated
     immediately and the peer is promoted in `peerOrder`. A background
     `reloadAndRefreshPreview()` always runs. If the reload **succeeds**,
     `updatePreviewFromStore` refreshes the preview from SQLite for
     consistency. If the reload **fails** and a decrypted message was
     captured, the fallback path seeds the cache with that single
     message via `cache.Load()` and copies it into `activeMessages` —
     so the user sees the message in the open chat instead of a blank
     screen. Without this fallback, a transient chatlog failure during
     mid-switch would silently discard a successfully decrypted message.
   - **Sound notifications**: `UIEventBeep` is emitted for every incoming
     message (sender ≠ us) in `onNewMessage`, covering three code paths:
     (1) non-active peer, (2) active peer mid-switch (cache not yet loaded),
     (3) active peer with cache ready. The repair-path in
     `repairUnreadFromHeaders` emits `UIEventBeep` **only for non-active
     peers** — active peer messages are already visible on screen, so
     beeping on repair would produce duplicate notifications after a
     transient failure recovery.
   - New messages for **non-active chats** go through `updateSidebarFromEvent`,
     which decrypts the preview and updates `RouterPeerState.Preview` + `Unread`,
     promotes the peer in `peerOrder`. If decryption fails (contact keys not
     yet available), the router falls back to `updatePreviewFromStore` in a
     background goroutine, increments `Unread` for incoming messages, and
     promotes the peer in `peerOrder` — matching the behavior of the
     successful inline-decrypt path.
   - Receipt updates for the active peer update the cache in-place via
     `ConversationCache.UpdateStatus()`. If the cache hasn't loaded yet
     for the active peer, a `loadConversation()` is triggered. If the
     message is missing from cache, a full reload is also triggered.

   **Startup `pollHealth`**: runs `ProbeNode` + `repairUnreadFromHeaders`.
   `repairUnreadFromHeaders` scans DMHeaders for message IDs not yet seen in
   `seenMessageIDs`, adds non-active incoming ones to the unread SET, and
   triggers `loadConversation` + `doMarkSeen` if the active chat has
   messages missing from cache. Since the badge became a set, no first-sync
   rule is needed against double counting — the same message from the SQL
   read and from a header is one member. What a header still cannot say is
   whether the message was already READ: DMHeaders carry no
   `delivery_status`, and the node's in-memory topic outlives a desktop
   session, so on the first sync a UI attaching to a running node is offered
   back every message of the previous session. On that sync only,
   `alreadyReadHeaderIDs` asks the database for the stored status of the
   candidate ids (`StoredMessageStatuses`) and suppresses exactly those it
   calls `seen`; a stored-but-unread id and an id the database does not hold
   at all are both badged from the header. That independence is deliberate —
   an earlier version deferred to the startup badge seed, and a seed that
   never ran left every stored message badgeless for the session. A failed
   read suppresses nothing: a badge too many clears by opening the
   conversation, a badge lost does not.
   Two more rules govern this path, both about work that outlives the
   answer it was based on. Which conversation is on screen is decided in
   phase 3, under the lock, and NOT during the scan: the header scan and
   the stored-status query both run outside the lock, and a message
   classified as visible after the user has left it loses its badge for
   good — its id passes the dedup gate either way, and this repair runs
   once per process.

   To prevent double-counting
   with the event-path, `onNewMessage()` registers `event.MessageID` in
   `seenMessageIDs` up-front — before any other processing — so the
   repair-path skips messages already handled by the event-path.
   However, if a background fallback fails (e.g. `loadConversation` or
   `updatePreviewFromStore` returns `false`), the message ID is **evicted**
   from `seenMessageIDs` via `evictSeenMessages()` so that
   `repairUnreadFromHeaders` can rediscover it on the next health poll.
   Without this rollback, the dedup gate would permanently suppress the
   message. The same rollback applies to the repair-path itself:
   `refreshPreviewForPeer` evicts message IDs when `updatePreviewFromStore`
   fails, so the next repair cycle retries the preview refresh.
   The active peer is excluded from `refreshPreviewForPeer` — its preview
   is updated by the `loadConversation` + `updatePreviewFromStore` path.
   If `loadConversation` succeeds but `updatePreviewFromStore` fails,
   `seenMessageIDs` is **not** evicted — the messages are already in cache
   and visible on screen. Evicting would cause rediscovery and a spurious
   `UIEventBeep`. The stale preview will be updated on the next message or
   peer switch. `UIEventBeep` is only emitted for non-active peers —
   active peer messages are already visible so notification is unnecessary.
   All rollback logic is centralized in `evictSeenMessages()` and
   `reloadAndRefreshPreview()` to avoid duplication across paths.

   **Seen receipts** (`doMarkSeen`): `MarkConversationSeen` is triggered
   by both `SelectPeer` and `AutoSelectPeer` via `selectPeerCore`, and
   by `onNewMessage` for every incoming message in the active chat. The
   principle: if the chat is on screen, messages count as read. The unread
   badge is optimistically cleared; if `doMarkSeen` fails the badge is
   restored to its previous value.
   `doMarkSeen` first verifies that `activePeer` still matches
   `peerAddress` — if the user switched peers before the goroutine ran,
   `activeMessages` belong to the new peer and using them would send a
   vacuous `MarkConversationSeen` that succeeds without real receipts,
   falsely clearing unread for the old peer. On mismatch, `doMarkSeen`
   returns `false` so the caller restores the badge. It also requires
   non-empty `activeMessages` — if the conversation hasn't loaded yet,
   it returns `false`. `selectPeerCore` only calls `doMarkSeen` after
   `loadConversation()` succeeds.

6. **Stale-load protection** — `loadConversation()` re-checks `activePeer`
   after `FetchConversation` returns. If the user switched peers during the
   fetch, the result is discarded.

7. **Stale-message protection** — `selectPeerCore()` (shared by both
   `SelectPeer` and `AutoSelectPeer`) clears `activeMessages` to nil
   synchronously before launching the background `loadConversation()`,
   and emits `UIEventMessagesUpdated` synchronously when the peer changed
   so the UI re-renders with an empty message list in the same frame.

8. **Failed-load retry / stuck-badge recovery** — When the user re-clicks the
   already-selected peer, `selectPeerCore` (with `userClicked=true`) checks
   two conditions: (a) cache miss (`!cache.MatchesPeer()`) → retries
   `loadConversation` + `doMarkSeen`; (b) cache valid but `Unread > 0` (badge
   stuck after `restorePeerUnread` rollback) → retries `doMarkSeen` only.
   When cache is valid and `Unread == 0` the click is a no-op.
   `AutoSelectPeer` (`userClicked=false`) same-peer is always a true no-op.

9. **Panic-safe startup** — `runStartup()` uses two separate `defer`
   statements: `defer close(startupDone)` (registered first, runs last) and
   `defer recoverLog("initializeFromDB")` (registered second, runs first).
   Go's LIFO defer order ensures `recoverLog` catches the panic via `recover()`
   before `close(startupDone)` unblocks the event listener. Both must be
   top-level `defer` calls — wrapping them in a single `defer func() { ... }()`
   would make `recover()` a nested call, which does not catch panics in Go.
   Without this, a panic in `initializeFromDB` would permanently disable the
   entire event-driven layer for the session. `runStartup()` and
   `runEventListener()` are extracted as named methods (not anonymous
   goroutines) so that unit tests can call them directly against a controlled
   DMRouter without duplicating production logic.

### DeliveredAt after restart

After a node restart, in-memory delivery receipts are empty, but the SQLite
`delivery_status` column retains "delivered" or "seen" values. Without special
handling, `DeliveredAt` would be nil and the UI would not render status
checkmarks (✓/✓✓).

Fix: `decryptDirectMessages()` synthesizes `DeliveredAt` from the message
timestamp when `PersistedStatus` is "delivered" or "seen" but no in-memory
receipt exists. The rendering switch also explicitly handles "delivered" and
"seen" status strings so badges appear even if `DeliveredAt` is nil for any
reason.

When a real delivery receipt later arrives with the same status rank (e.g.
"delivered" → "delivered"), `ConversationCache.UpdateStatus()` allows the
update if it upgrades a nil/zero `DeliveredAt` to a real timestamp. This
replaces the synthetic value with the actual receipt time without requiring
a status rank advance.

---

## Русский

### Обзор

`DMRouter` — центральный сервисный слой между сетевой нодой и desktop UI.
Он владеет всей DM бизнес-логикой: маршрутизация событий, управление sidebar,
кеш диалогов, health polling, mark-seen, отправка сообщений. UI общается
с ним через небольшой, чётко определённый публичный API.

Исходник: `internal/core/service/dm_router.go`

### Модульная многослойная архитектура

Desktop-приложение следует строгой модульной многослойной архитектуре,
спроектированной для чистого разделения ответственности и лёгкой расширяемости:

```mermaid
flowchart TB
    subgraph L1["Слой 1 — Сеть (node.Service)"]
        direction LR
        TCP["TCP соединения\ngossip / relay"]
        MSTORE["Интерфейс MessageStore\n(делегирует персистентность\nзарегистрированному обработчику)"]
    end

    subgraph EBUS["Шина событий (ebus.Bus)"]
        direction LR
        TOPICS["Топики:\n• message.new\n• receipt.updated\n• peer.health.changed\n• slot.state.changed\n• peer.traffic.updated\n• route.table.changed\n• contact.added/removed\n• identity.added\n• aggregate.status.changed\n• version.policy.changed\n• message.sent / file.sent"]
    end

    subgraph L2["Слой 2 — Сервис (DesktopClient + DMRouter)"]
        direction LR
        CHATLOG["chatlog.Store\n(SQLite, владеет\nDesktopClient)"]
        BUSINESS["Бизнес-логика:\n• маршрутизация событий\n• управление sidebar\n• кеш диалогов\n• mark-seen\n• отправка сообщений"]
        SNAPSHOT["Snapshot() → неизменяемый\nRouterSnapshot"]
        API["Публичный API:\nSelectPeer() / SendMessage()\nSetSendStatus()"]
        UIEVENTS["UIEvent канал\n(буфер 32, неблокирующий)\nвкл. UIEventBeep"]
    end

    subgraph L3["Слой 3 — UI (Window)"]
        direction LR
        GIO["Gio виджеты\n(чистый рендеринг)"]
        FRAME["Каждый кадр:\nSnap → рендер → готово"]
        BEEP["go systemBeep()\n(параллельное воспроизведение)"]
    end

    L1 -->|"Publish()"| EBUS
    EBUS -->|"Subscribe()"| L2
    EBUS -->|"Subscribe()"| L3
    L2 --> L3
    MSTORE -->|"StoreMessage()\nUpdateDeliveryStatus()"| CHATLOG
    UIEVENTS -->|"UIEventBeep"| BEEP

    style L1 fill:#1a2332
    style EBUS fill:#2a1a33
    style L2 fill:#1e3050
    style L3 fill:#22364a
```

*Диаграмма 1 — Обзор модульной многослойной архитектуры*

Каждый слой общается со следующим через чётко определённый интерфейс:

- **Сеть → ebus**: нода публикует короткие дельта-события (health пиров, сообщения, квитанции, изменения роутинга) через `ebus.Bus.Publish()`
- **ebus → Сервис**: DMRouter подписывается на все релевантные топики; обработчики асинхронные (64-слотовый inbox на подписчика, выделенная drain-горутина)
- **ebus → UI**: консольное окно подписывается напрямую на топики health пиров и агрегатного статуса для обновлений в реальном времени
- **Сервис → UI**: канал `UIEvent` (неблокирующие уведомления) + `Snapshot()` (read-only копия состояния)
- **UI → Сервис**: вызовы методов (`SelectPeer`, `SendMessage`, `ConsumePendingActions`)
- **RPC** остаётся для команд/запросов (fetch сообщений, отправка сообщений, таблица роутинга). RPC-обработчики могут публиковать ebus-события как side-эффект

Ни один слой не «перепрыгивает» через соседний. UI никогда не обращается к
`DesktopClient` или SQLite напрямую. Роутер никогда не манипулирует виджетами
Gio. Нода не владеет хранением сообщений — делегирует обработчику `MessageStore`,
зарегистрированному `DesktopClient` при создании. Relay-only ноды (`corsa-node`)
оставляют `MessageStore` = nil и ретранслируют сообщения без персистентности. Это позволяет легко добавлять новые функции (групповые чаты, передачу
файлов и др.) расширяя слой роутера без изменений UI, или полностью заменить
UI-фреймворк без модификации бизнес-логики.

### Трёхуровневая архитектура (Network → Service → UI)

Desktop-приложение использует трёхуровневую архитектуру:

```mermaid
flowchart TB
    subgraph NET["Сетевой уровень (node.Service)"]
        NODE["Локальная нода\n(TCP, gossip, relay)"]
        MSTORE["MessageStore\n(callback интерфейс)"]
    end

    subgraph EBUS["ebus.Bus"]
        EB["Асинхронная шина событий\n(64-слотовый inbox,\nвыделенная горутина drain)"]
    end

    subgraph SVC["Сервисный уровень (DesktopClient + DMRouter + NodeStatusMonitor)"]
        DC["DesktopClient\n(desktop.go)\nвладеет chatlog.Store\nреализует MessageStore"]
        DR["DMRouter\n(dm_router.go)"]
        NSM["NodeStatusMonitor\n(node_status_monitor.go)\nвладеет NodeStatus"]
        CACHE["ConversationCache\n(только активный чат)"]
        STATE["Состояние роутера\n(peers, peerOrder, activePeer,\nactiveMessages, etc.)"]
        UIEVENTS["UIEvent канал\n(буфер 32, неблокирующий)"]
    end

    subgraph UI["UI уровень (window.go)"]
        WIN["Window\n(только Gio виджеты)"]
        SNAP["RouterSnapshot\n(неизменяемый на кадр)"]
        BEEP["go systemBeep()\n(notify.go — параллельное\nвоспроизведение через oto)"]
    end

    NODE -->|"Publish(TopicMessageNew,\nTopicReceiptUpdated, ...)"| EB
    EB -->|"Subscribe(TopicMessageNew,\nTopicReceiptUpdated)"| DR
    EB -->|"Subscribe(TopicPeerHealth*,\nTopicAggregate*, ...)"| NSM
    NSM -->|"onChanged → NotifyStatusChanged()"| DR
    MSTORE -->|"StoreMessage()\nUpdateDeliveryStatus()"| DC
    DR --> CACHE
    DR --> STATE
    DR -->|"notify(UIEventBeep)\nnotify(UIEvent*Updated)"| UIEVENTS
    UIEVENTS -->|"Subscribe() → for ev := range"| WIN
    UIEVENTS -->|"ev.Type == UIEventBeep"| BEEP
    WIN -->|"Snapshot()"| SNAP
    WIN -->|"SelectPeer() / SendMessage()"| DR
    WIN -->|"ConsumePendingActions()"| DR
```

*Диаграмма 2 — Трёхуровневая архитектура с потоком данных*

**DesktopClient** (`internal/core/service/desktop.go`) — composition root
desktop-овых суб-сервисов. Сам `chatlog.Store` больше не хранит;
владеет им `ChatlogGateway`, а `node.MessageStore` реализует
`MessageStoreAdapter`. При создании `NewDesktopClient` собирает все
суб-сервисы (`AppInfo`, `LocalRPCClient`, `ChatlogGateway`,
`MessageStoreAdapter`, `DMCrypto`, `NodeProber`) и регистрирует адаптер
в `node.Service` через `RegisterMessageStore()`. Нода вызывает
`StoreMessage()` / `UpdateDeliveryStatus()` на адаптере перед генерацией
`LocalChangeEvent`, сохраняя инвариант «сначала БД, потом UI-событие».
Публичные методы `DesktopClient` — тонкие делегаторы; новые потребители
должны пользоваться узкими суб-сервисами через акцессоры (`DMCrypto()`,
`NodeProber()`, `ChatlogGateway()`, `RPC()`, `AppInfo()`).
`FetchConversation`, `FetchConversationPreviews`, `FetchSinglePreview` и
`MarkConversationSeen` живут на `DMCrypto` (проброшены делегаторами на
`DesktopClient`). Они принимают `context.Context` и пробрасывают его
через `LocalRPCClient.LocalRequestFrameCtx` — context-aware вариант
`LocalRequestFrame`. В TCP-режиме context полностью контролирует дедлайн
dial. В embedded-режиме `ctx.Err()` проверяется до и после
`HandleLocalFrame` как best-effort gate (сам синхронный handler не может
быть прерван). Загрузка контактов дедуплицирована в хелпере
`DMCrypto.fetchContactsForDecrypt(ctx, senders)` (общем для всех трёх
Fetch-методов), который исключает собственный адрес identity при
проверке missing senders, избегая лишних `fetch_contacts` roundtrip'ов
на диалогах с исходящими сообщениями.

**DMRouter** (`internal/core/service/dm_router.go`) владеет DM бизнес-логикой:
маршрутизация событий, управление sidebar, кеш диалогов, mark-seen, отправка
сообщений. Агрегация сетевого состояния (PeerHealth, AggregateStatus,
контакты, достижимость) делегирована **NodeStatusMonitor**
(`internal/core/service/node_status_monitor.go`), к которому DMRouter обращается
через интерфейс `NodeStatusProvider`. UI общается с DMRouter через небольшой
публичный API.

**Window** (`internal/app/desktop/window.go`) — чистый слой рендеринга.
Без `sync.Mutex`, без прямого доступа к `DesktopClient`, без бизнес-логики.
В начале каждого кадра вызывает `Snapshot()` для получения неизменяемой копии
состояния роутера, затем рендерит из этого снимка.

### Публичный API

| Метод | Направление | Описание |
|---|---|---|
| `Subscribe()` | Роутер → UI | Возвращает `<-chan UIEvent` для уведомлений об изменениях |
| `Snapshot()` | UI → Роутер | Возвращает неизменяемый `RouterSnapshot` под `RLock` |
| `ConsumePendingActions()` | UI → Роутер | Атомарно читает и очищает отложенные мутации виджетов |
| `SelectPeer(id)` | UI → Роутер | Клик пользователя: делегирует в `selectPeerCore(id, true)`. Переключает peer'а, чистит stale сообщения, **оптимистично сбрасывает unread-бейдж** (с откатом) и эмитит `UIEventSidebarUpdated` синхронно, затем в фоне загружает диалог и отправляет seen. Если `loadConversation` или `doMarkSeen` упадёт, бейдж **восстанавливается** через `restorePeerUnread`. Повторный клик по тому же peer'у: повторяет упавшую загрузку (cache miss) или повторяет `doMarkSeen` при `Unread > 0` (застрявший бейдж после отката). При валидном кеше и `Unread == 0` — no-op. |
| `AutoSelectPeer(id)` | UI → Роутер | Программный авто-выбор: делегирует в `selectPeerCore(id, false)`. При смене peer'а поведение идентично `SelectPeer`: сброс unread, загрузка диалога, seen-квитанции с откатом при ошибке. При повторном выборе того же peer'а — **полный no-op**: без сброса unread, без `doMarkSeen`, без UI-событий, без запуска горутин. Это предотвращает избыточные UI-обновления при программном переизбрании. |
| `SendMessage(to, body)` | UI → Роутер | Шифрует и отправляет DM |
| `ActivePeer()` | UI → Роутер | Возвращает адрес текущего активного peer'а |
| `MyAddress()` | UI → Роутер | Возвращает адрес локальной identity |
| `SetSendStatus(s)` | UI → Роутер | Обновляет текст статуса отправки |
| `Start()` | UI → Роутер | Регистрирует ebus-подписки, запускает горутину `runStartup` |

### Ключевые типы

```go
type RouterSnapshot struct {
    ActivePeer     domain.PeerIdentity
    PeerClicked    bool
    Peers          map[domain.PeerIdentity]*RouterPeerState
    PeerOrder      []domain.PeerIdentity
    ActiveMessages []DirectMessage
    CacheReady     bool       // true when cache is loaded for ActivePeer
    NodeStatus     NodeStatus
    SendStatus     string
    MyAddress      domain.PeerIdentity
}

type RouterPeerState struct {
    Preview        ConversationPreview
    LastIncomingAt domain.OptionalTime // when the peer last wrote to us
    Unread         int
}

type PendingActions struct {
    ScrollToEnd   bool
    ClearEditor   bool
    ClearReply    bool
    RecipientText domain.PeerIdentity
}

type UIEventType int
const (
    UIEventMessagesUpdated UIEventType = iota + 1
    UIEventSidebarUpdated
    UIEventStatusUpdated
    UIEventBeep
)
```

`LastIncomingAt` — выведенная из переписки половина «последний раз онлайн»:
самое свежее написанное этим peer-ом сообщение. Оно живёт только в памяти —
`seedHistoryEvidence` пересчитывает его из chatlog при старте (вне стартового
пути, в своей горутине, с тремя попытками на неудачное чтение, и сам публикует
снапшот: `Snapshot()` отдаёт кэш, который перестраивает только `notify`, а на
пути ретраев позднего события, на котором можно было бы уехать, уже нет; тому
же правилу следует и sweep повторных сверок после удаления), каждое входящее двигает вперёд, путь
удаления пересчитывает заново, — потому что durable-копия
была бы вторым значением, которое надо согласовывать со строками, из которых
оно выведено. Наблюдением оно не является и стоит ниже всего, что нода видела
сама. `Preview` и `LastIncomingAt` отвечают на разные вопросы и пишутся одним
хелпером `setPeerPreviewLocked`. `Preview` — последняя строка треда, кто бы её
ни написал; `LastIncomingAt` двигается только когда отправитель — сам
собеседник, и только вперёд, поэтому история, пришедшая не по порядку (startup
replay, реле-сообщение, шедшее долгим путём), не уводит значение назад. Все
пути, узнающие о новом сообщении, идут через этот хелпер: присваивание одного
`Preview` оставило бы свидетельство присутствия позади на том пути, который о
нём забыл. Единственное исключение — путь удаления, который присваивает
`LastIncomingAt` напрямую: он пересчитывает значение из SQL через
`LastIncomingAtFor`, потому что подтверждавшим его сообщением могла быть
только что удалённая строка, и очищает поле, если входящих сообщений не
осталось.

`Preview`, `Unread` и `LastIncomingAt` — это состояние peer-а, ВЫВЕДЕННОЕ из
chatlog, и питают его два источника, которые невозможно упорядочить друг
относительно друга: SQL-чтение и поток событий. База ОПЕРЕЖАЕТ события —
сообщение коммитится раньше, чем доставляется извещающее о нём событие, — то
есть одно и то же сообщение приходит в сайдбар дважды и в любом порядке.
Вместо того чтобы сторожить это версиями, слияния сделаны идемпотентными: так
вопрос порядка исчезает, а не решается.

- **Unread — множество id сообщений**, а не счётчик; `RouterPeerState.Unread`
  это его размер. Добавление уже имеющегося id ничего не меняет, поэтому
  сообщение, посчитанное и стартовым чтением, и собственным событием, остаётся
  одним непрочитанным. Убирают id: чтение диалога (только те, по которым
  квитанции реально ушли), удаление и сверка после удаления. Пересчитывают
  множество из `delivery_status` два места — поток событий статуса не несёт и
  умеет только добавлять: сверка после удаления и перестройка после
  неудавшегося открытия диалога (`repairBadgeFromStore`, где откату нечего
  восстанавливать, кроме того, что было в памяти, а наполовину прошедший
  mark-seen мог оставить id, которые база уже считает прочитанными). Оба
  сохраняют id, которых база не держит вовсе, а перестройка — ещё и то, что
  пришло, пока она читала: добавления не двигают счётчик, и её проверка эпохи
  их не видит. При этом сверка сохраняет
  id, которых база не держит ВООБЩЕ (`StoredMessageStatuses`): header может
  забейджить сообщение раньше, чем запишется строка, а «нет в списке
  непрочитанных» иначе прочиталось бы как «прочитано» для id, который уже
  некому вернуть.
- **LastIncomingAt — максимум** по временам входящих, поэтому опоздавший
  читатель может только проиграть.
- **Preview берёт более новое сообщение** по timestamp; старое чтение никогда
  не затирает более новое.

Идемпотентные слияния упорядочивают между собой только ДОБАВЛЕНИЯ. Чтение,
снятое до того, как что-то сдвинуло peer-а НАЗАД — удаление, mark-seen,
оптимистичная очистка, удаление контакта, — и применённое после, возвращает
ровно то, что было убрано, и никакой максимум или множество этого не
предотвращают. Поэтому у каждого peer-а есть `backwardsEpoch`, который бампает
каждый такой движитель, и любая запись, выведенная из chatlog, следует одному
правилу: **снять эпоху перед СВОИМ запросом и применить, только если она не
изменилась.** Изменившаяся эпоха означает, что ответ описывает диалог,
которого в таком виде больше нет, и работу надо переделать, а не сливать.
Каждое чтение снимает свой снимок непосредственно перед своим запросом: один
базис на два чтения заставил бы второе отвергать всё, что первому позволили
изменить его же ретраи.

Счётчиков ДВА, потому что движения назад бывают двух разных родов. `unread`
считает то, что опускает только БЕЙДЖ — mark-seen, оптимистичную очистку при
открытии диалога, — а `history` считает то, что убирает СТРОКИ: удаление
сообщения, зачистку диалога, удаление контакта, сброс identity. Движение
history бампает оба; mark-seen бампает только `unread`, потому что сделать
ответ про last-incoming неверным оно не может. Один общий счётчик стоил бы
фиче самого частого случая: диалог, открывающийся при запуске автоматически,
помечается прочитанным, пока стартовый скан ещё идёт, — и его контакт, первая
строка сайдбара, всю сессию оставался бы без строки «последний раз онлайн».
Через эту проверку идут стартовый скан, `seedPreviews`, сверка после удаления,
ремонт по headers и восстановление бейджа после неудачного открытия;
поколение `peerGen` отвечает на более узкий
вопрос — существует ли контакт вообще — и снимается перед любым медленным
шагом (RPC расшифровки, скан headers), который иначе создал бы его строку
заново, — причём это касается и побочных эффектов, а не только строки:
входящее файловое объявление регистрируется только после этой проверки, а
удаление, успевшее пройти всё равно, компенсируется повторной очисткой
трансфера, потому что file bridge нельзя звать под замком роутера.

Поколение — ответ длиной в процесс, и после рестарта оно не значит ничего.
Проверка охватывает и побочные эффекты, а не только строку: входящее файловое
объявление регистрируется только после неё и никогда не «сначала
зарегистрируем, потом откатим» — откат по id сообщения не отличает свою
регистрацию от такой же, сделанной новым поколением, и утащил бы вместе с ней
уже скачанный файл. Каждый медленный шаг несёт одну и ту же пару —
поколение И счётчик history — как единый `peerStamp`: они отвечают на разные
вопросы, и ветка, проверяющая половину, применяет сообщение, строки которого
уже нет. Шаг, который СПРАШИВАЕТ базу и потом действует по ответу, снимает
stamp ДО вопроса и сверяет его на коммите: свежий stamp описывал бы уже
другой момент.

Проверку версии и регистрацию файла нельзя сделать одним атомарным шагом:
проверке нужен замок роутера, а регистрация идёт через file bridge, под
доменным мьютексом которого звать нельзя. Поэтому их делают атомарными
относительно УДАЛЕНИЯ — per-peer файловым барьером: каждый путь очистки
берёт его, двигает под ним счётчик history и только потом чистит, а
регистрация держит его от своей проверки до появления mapping. Регистрация,
уже летящая, либо успевает раньше, либо видит сдвинутый счётчик и отступает.
Одно удаление — одно движение счётчика, а очистка, которая ничего не удалила,
не двигает его вовсе: ack обычно называет строку, удалённую давно, и каждое
ложное движение помечает устаревшими совершенно актуальные загрузку или
расшифровку.

Удаление контакта держит тот же барьер на всём хвосте: движение версии,
очистка трансферов и сброс состояния в памяти. Регистрация, ждавшая барьер,
продолжится уже после последней очистки и найдёт и новое поколение, и
отсутствие строки — поэтому она проверяет и наличие строки, а не только
stamp. Сам мьютекс при удалении контакта не выбрасывается: указатель на него
уже держит ожидающий, и замена при повторном добавлении оставила бы старую
очистку и новую регистрацию без взаимного исключения.

Пока удаление идёт, нужна ещё одна вещь, которую stamp выразить не может:
удаление само двигает счётчики, поэтому сообщение, пришедшее сразу после,
несёт stamp, который СОВПАДАЕТ, — и apply создал бы строку заново, за спиной
удаления, чтобы очистка оставила её сиротой.

Этим занят **шлюз удаления** (`removalGate`) — один объект с двумя дверьми.
`tryEnsurePeerLocked` сверяется с ним перед созданием строки сайдбара, а
адаптер хранилища сообщений — перед записью входящего DM, потому что
хранилище и есть та дверь, через которую идут собственные записи ноды: нода
сохраняет сообщение РАНЬШЕ, чем о нём узнаёт роутер, поэтому сообщение,
принятое во время удаления, попадёт в базу независимо от того, что роутер
откажется делать потом, — и следующий запуск соберёт удалённый диалог из этой
строки. Хранилище отвечает `StoreDeferred` — не сохранено и не выброшено:
сообщение остаётся у отправителя, который доставит его повторно после
удаления, и тогда это просто новое сообщение в несуществующий диалог,
открывающее его так же, как сообщение любого незнакомца. Это окно, а не
запрет.

Дверь хранилища — это **аренда** (lease), а не проверка. Проверить не значит
записать: пропущенное хранилище может встать ровно на столько, сколько займёт
база, и удаление, прочитавшее лишь флаг, выполнит в этот промежуток оба
удаления истории, оставив строку позади них — там, куда больше никто не
смотрит. Поэтому хранилище берёт аренду (`admitWrite`) раньше всего
остального и держит её до коммита своей строки, а `begin` не возвращается,
пока не вернутся все выданные по этому диалогу аренды. После возврата `begin`
удаление знает то, чего флаг сказать не мог: ни одна запись не идёт и ни одна
новая допущена не будет. Проверке строки в роутере аренда не нужна — он
решает и действует под одним замком, без I/O между решением и действием.

Шлюз поднимается ПЕРВЫМ оператором `RemovePeer` — до удаления истории и до
файлового барьера: поднятый позже, он был бы открыт ровно на длину этих
ожиданий, а это и есть окно для параллельной записи. Он считающий, а не
флаг: два удаления одного контакта могут пересечься, и первое завершившееся
не вправе открыть дверь под вторым. Запись, успевшая пройти дверь хранилища
до подъёма шлюза, закрывается последней зачисткой истории в конце удаления —
и если эта зачистка ПАДАЕТ, `RemovePeer` возвращает ошибку: состояние в
памяти снято в любом случае и UI уведомляется в любом случае, но сообщить об
удалении контакта, чья история, возможно, осталась на диске, — единственный
ответ, который эта функция давать не должна.

И удаление контакта, и стирание беседы вдобавок ОСТАНАВЛИВАЮТ очередь отправки
реакций на время удаления (`HoldReactionSends`) — до подъёма шлюза или под ним.
Шлюз достаёт до записей и переанонса, но не до очереди, а та к этому моменту
может уже держать факты беседы, разрешённые по записи; проход, прочитавший их
мгновением раньше, отдал бы кадр уже после исчезновения строк, и последующая
очистка очереди дождалась бы лишь кадра, который давно ушёл.

**Стирание беседы** поднимает тот же шлюз — вокруг своей транзакции и вокруг
сброса очереди реакций. Собственный барьер стирания (`convDeleteRetry`)
останавливает только отправки этого узла и ничего не говорит путям, которые
пишут беседу сбоку: переанонс реакций читает страницу фактов пользователя и
отдаёт КОПИЮ в очередь ноды, поэтому стирание, попавшее между этими двумя
шагами, удаляет строки, которые уже снова в пути, а затем опустошает очередь,
которую колбэк через мгновение наполняет заново. `begin` дожидается аренды,
которую такой переанонс уже держит, и не пускает новые, пока не будет сброшена
и очередь. Входящие сообщения на это окно откладываются ровно так же, как при
удалении контакта, — а сообщение, пришедшее после стирания, и так лежит вне
него с обеих сторон.

Две ошибки, которые может вернуть удаление, — разные ошибки, и вызывающий
различает их через `errors.Is(err, ErrHistorySweepFailed)`. ПЕРВОЕ удаление
истории падает до того, как что-либо тронуто: контакт на месте, и вызывающий
обязан не трогать своё состояние. ФИНАЛЬНАЯ зачистка падает, когда контакта
уже нет ни в сайдбаре, ни в кэше, ни в trust store, и под вопросом только его
история: вызывающий обязан довести свою очистку — черновик, вложение, алиас,
выбор следующего диалога — и сообщить об ошибке, потому что остановка здесь
оставит состояние композера у диалога, который пользователь уже не может
открыть, и удалённый чат выбранным. Сверяется именно половина history: пометка диалога прочитанным
двигает счётчик unread и не убирает строк, поэтому сравнение всей пары
заставляло бы открытие чата выбрасывать приходящие в него сообщения и
отклонять его же файловые трансферы.

Конфликт по history — не повод ВЫБРОСИТЬ сообщение. Счётчик один на peer-а,
поэтому удаление любой строки диалога выглядит ровно как удаление той, что
сейчас расшифровывается, а id сообщения уже прошёл dedup-гейт: неверная
догадка теряет его насовсем. Вместо этого диалог перечитывается из базы:
сверка восстанавливает превью и свидетельство last-online, бейдж
пересчитывается из `delivery_status`, и сообщение либо возвращается вместе с
ними, либо нет — то самое различение, которого счётчик сделать не мог.

То же правило «ответ старше работы» действует и для расшифровываемого
сообщения: какой диалог на экране, перечитывается ПОСЛЕ расшифровки — и по
выбору, и по кэшу, — потому что добавление в кэш, уже загруженный для другого
собеседника, вклеивает сообщение в чужой тред, а признание его видимым
навсегда съедает его бейдж.

Единственное исключение — удаление: только оно законно двигает эти значения
НАЗАД, потому что описанной ими строки больше нет. Поэтому упорядочивание
нужно только ему, и оно получает его от per-peer refresh lock: два удаления в
одном диалоге работают каждый в своей горутине, и более медленный запрос не
должен приземлиться последним со старым ответом. Его чтения работают по
принципу «всё или ничего» (половина чтения публикует момент, которого не
было), а неудавшееся ставится в очередь и добивается delete-петлёй — историю
peer-а больше никто не перечитывает.

Пересчёт ОБНОВЛЯЕТ peer-а и никогда его не СОЗДАЁТ. Он работает асинхронно,
поэтому диалог мог быть удалён ещё до того, как его запланировали; вызывающие,
вводящие новый диалог, создают строку сами — синхронно с событием, которое её
оправдывает.

### Защита конкурентного доступа

DMRouter запускает две фоновые горутины:

- **Startup горутина** (`runStartup`) — запускает `initializeFromDB` для
  загрузки превью, контактов, identity и диагностических полей из SQL. Пока
  startup выполняется, ebus-события буферизуются в `startupEventBuf` (лимит
  256 записей для предотвращения memory spike). После инициализации
  буферизованные события воспроизводятся под `replayingStartup=true`, и это
  подавляет ТОЛЬКО звук: бейдж — множество, поэтому сообщение, посчитанное и
  SQL-чтением, и собственным повторным событием, остаётся одним непрочитанным,
  а подавление replay, наоборот, теряло бы сообщения, записанные после
  чтения. События, пришедшие во время Phase 1
  replay, ре-буферизуются и затем обрабатываются как live в Phase 2 (с
  `replayingStartup=false`), где звук больше не подавляется.
- **ebus подписки** — DMRouter подписывается только на DM-специфичные
  топики в `subscribeEvents()` до startup, чтобы не пропустить события:
  - `TopicMessageNew` / `TopicReceiptUpdated` — новые DM и изменения
    квитанций доставки (буферизуются до startup, обрабатываются `handleEvent`)
  - `TopicMessageSent`, `TopicMessageSendFailed` (результаты отправки,
    публикуются самим DMRouter после завершения операций)
  - `TopicFileSent`, `TopicFileSendFailed` (результаты отправки файлов)

  Сетевые ebus-топики обрабатываются **NodeStatusMonitor**
  (`node_status_monitor.go`), который подписывается на:
  - `TopicPeerHealthChanged` (состояние/connected/score/ping/pong пира).
    Строки PeerHealth индексируются по составному ключу `(Address, ConnID)`.
    `peerHealthFrames()` генерирует несколько строк для одного overlay-адреса
    при наличии нескольких входящих соединений, различаемых по ConnID.
    Дельта несёт исходящий `ConnID` (0 когда нет исходящей сессии)
    и полный набор активных `InboundConnIDs` — это даёт монитору
    полное представление о топологии соединений для реконсиляции.

    `applyPeerHealthDelta` использует 5-шаговую модель реконсиляции:
    1. **Построить expected ConnIDs** из дельты (`ConnID` + `InboundConnIDs`).
    2. **Обновить существующие строки**: исходящая строка получает полную
       запись session-scoped полей (`writeSession=true`); входящие строки
       получают только address-level поля (`writeSession=false`) — их
       ConnID и Direction являются неизменяемыми идентификаторами строки.
       Placeholder с `ConnID=0` промотируется если дельта несёт конкретный
       исходящий ConnID. Когда дельта несёт живые `InboundConnIDs` и
       `ConnID=0`, placeholder на шаге 2 не трогается — он будет удалён
       на шаге 5, а его address-level slot-метаданные (`SlotState`,
       `PendingCount`) мигрируют на выживающие per-ConnID строки.
       Мутация placeholder'а здесь затёрла бы эти поля значениями из
       health-дельты раньше, чем миграция смогла бы их захватить.
    3. **Создать исходящую строку** если совпадение не найдено. Для дельт
       с `ConnID=0` (нет исходящей сессии) новая строка создаётся только
       когда нет строк для адреса или пир отключён (выживающая
       address-level строка после pruning).
    4. **Создать входящие строки** для `InboundConnIDs` ещё не
       представленных — с `Direction="inbound"` и address-level полями из
       дельты.
    5. **Удалить мёртвые строки соединений** чей ConnID больше не в
       expected-наборе. Строка с `ConnID=0` (address-level) удаляется
       когда per-ConnID строки авторитетно представляют адрес
       (`expectedConnIDs` непуст); иначе выживает. Перед удалением
       placeholder'а его `SlotState` и `PendingCount` — приходящие через
       `TopicSlotStateChanged` / `TopicPeerPendingChanged`, а не через
       `PeerHealthDelta` — мигрируют на выживающие per-ConnID строки,
       где эти поля ещё пусты, чтобы ранее установленное
       `applySlotStateDelta` значение на существующей входящей строке
       не было затёрто. Pruning также срабатывает при полном отключении
       (`!delta.Connected`) даже когда `expectedConnIDs` пуст — все
       per-ConnID строки мертвы, а свежесозданная на шаге 3 строка
       `ConnID=0` несёт disconnected-состояние.

    Поля сессии (Direction, ClientVersion, ClientBuild, ConnID,
    ProtocolVersion) очищаются безусловно при disconnect-дельтах
    (`!delta.Connected`) и заполняются (backfill) только при нулевых/пустых
    значениях на connect.

    При merge с probe-снимком `mergePeerHealth`
    индексирует по `(Address, ConnID)` через структуру `peerHealthKey`,
    так что несколько per-ConnID probe-строк для одного overlay-адреса
    сохраняются, а не схлопываются. Используется `ebusHealthSeeded` и
    двухуровневое обогащение. Адреса, получившие хотя бы один
    `applyPeerHealthDelta`, считаются «seeded»: поля состояния (Connected,
    Score, State, PendingCount, ConsecutiveFailures, LastError), поля
    сессии (Direction, ClientVersion, ClientBuild, ConnID,
    ProtocolVersion), поля жизненного цикла слота (SlotState,
    SlotRetryCount, SlotGeneration, SlotConnectedAddr) и полный
    диагностический блок — ebus-авторитетный после перехода на
    однократный `FetchAndSeed()` — (BannedUntil, LastErrorCode,
    LastDisconnectCode, IncompatibleVersionAttempts,
    LastIncompatibleVersionAt, ObservedPeerVersion,
    ObservedPeerMinimumVersion, VersionLockoutActive) авторитетны и не
    перезаписываются probe. Нулевые/пустые значения являются значимыми
    сигналами (disconnect очищает метаданные сессии, удаление слота
    очищает SlotState, `resetPeerHealthForRecoveryLocked` очищает баны и
    диагностику после успешного восстановления — каждый
    `PeerHealthDelta` несёт полное текущее значение диагностических
    полей, поэтому backfill из probe воскресил бы состояние, которое нода
    уже очистила). Только действительно персистентные поля (PeerID,
    timestamp'ы активности, счётчики трафика) могут быть дополнены из
    probe через `enrichPeerHealthIdentityFromProbe` — это покрывает
    случай, когда PeerID разрешается вне потока после первого health
    delta. Настоящие placeholder'ы (из `applySlotStateDelta`/
    `applyPeerPendingDelta` без health delta) получают полное обогащение
    через `enrichPeerHealthFromProbe`, который заполняет диагностический
    блок из probe, потому что ни одна ebus-дельта ещё не заявила
    авторитет.
  - `TopicPeerPendingChanged` (глубина per-peer pending-очереди; создаёт
    минимальную запись `PeerHealth` если пир ещё не известен). Адресный
    уровень: обновляет ВСЕ per-ConnID строки для адреса.
  - `TopicPeerTrafficUpdated` (счётчики байт, батч ~2 с). Адресный
    уровень: обновляет ВСЕ per-ConnID строки для адреса.
  - `TopicSlotStateChanged` (жизненный цикл CM-слота). Адресный уровень:
    обновляет ВСЕ per-ConnID строки для адреса.
  - `TopicAggregateStatusChanged`, `TopicVersionPolicyChanged`
  - `TopicContactAdded/Removed`, `TopicIdentityAdded`
  - `TopicRouteTableChanged` (отслеживание достижимости на основе
    таблицы маршрутизации — при каждом изменении таблицы монитор
    перестраивает `ReachableIDs` из `BuildReachableIDs()`, читающего
    авторитетный снимок маршрутизации. Покрывает добавление/удаление
    direct-peer, принятие announcement, инвалидацию transit-маршрутов
    и истечение TTL. `routingTableTTLLoop` публикует
    `TopicRouteTableChanged` с reason `"ttl_expired"` всякий раз, когда
    `TickTTL()` удаляет хотя бы один истёкший маршрут, обеспечивая
    актуальность ReachableIDs даже без явных routing-мутаций.)

  Каждый обработчик монитора обновляет `NodeStatus` под своим `mu` и вызывает
  callback `onChanged`, который запускает `DMRouter.NotifyStatusChanged()`
  для пересборки snapshot.
- **UI горутина** — вызывает `Snapshot()`, `ConsumePendingActions()`,
  `SelectPeer()`, `SendMessage()` из event loop Gio.

Для предотвращения гонок данных (которые вызывают фатальный крэш Go runtime):

1. `mu sync.RWMutex` (на DMRouter) — защищает все разделяемые поля роутера:
   `activePeer`, `peerClicked`, `peers`, `peerOrder`, `activeMessages`,
   `seenMessageIDs`, `initialSynced`, `replayingStartup`,
   `sendStatus`, `pendingScrollToEnd`, `pendingClearEditor`,
   `pendingRecipientText`, а также per-peer карты: `unreadIDs` (множества
   бейджа), `peerGen` (поколения жизненного цикла), `backwardsEpoch` (два счётчика
   движений назад, см. ниже), `pendingDeleteReconcile` (очередь ретраев удаления) и
   `peerRefreshMu` (per-peer замки сверки — под `mu` защищена КАРТА, но не
   сами мьютексы в ней). Примечание: `NodeStatus` принадлежит
   `NodeStatusMonitor` (со своим `mu`), а не DMRouter.

   **Правило порядка**: per-peer замок сверки из `peerRefreshMu` удерживается
   через SQL-чтения, поэтому его нельзя брать под `mu`. `peerRefreshLock`
   находит мьютекс под `mu`, отпускает `mu` и только затем запирает его.

   Фоновые горутины берут `mu.Lock()` для записи и `mu.RLock()` для чтения.
   `Snapshot()` берёт `mu.RLock()` и возвращает глубокую копию.

   **Нормализация идентификаторов**: Все публичные точки входа (`SelectPeer`,
   `AutoSelectPeer`, `SendMessage`, `RemovePeer`, `peerForMessage`,
   `repairUnreadFromHeaders`) нормализуют `PeerIdentity` через
   `normalizePeer()` (trim пробелов) перед любым доступом к map/slice.
   Это предотвращает создание дублирующих ключей в `peers` или `peerOrder`
   из-за пробелов в идентификаторах.

2. **Паттерн Snapshot** — UI горутина никогда не читает поля роутера
   напрямую. `Snapshot()` создаёт консистентную копию под единым `RLock`,
   возвращая неизменяемый `RouterSnapshot`. UI читает только из этого
   снимка весь кадр. Это исключает всю конкуренцию за блокировки в пути
   рендеринга.

3. **Безопасность виджетов через PendingActions** — виджеты Gio НЕ
   потокобезопасны. Фоновые горутины устанавливают флаги отложенных
   действий под `mu`. UI горутина вызывает `ConsumePendingActions()` в
   начале каждого кадра, атомарно читая и очищая флаги, затем применяет
   их к виджетам Gio.

4. **Неблокирующий UIEvent канал** — роутер отправляет `UIEvent` в
   буферизированный канал (ёмкость 32) через `notify()`. При переполнении
   каждое событие получает собственную retry-горутину с экспоненциальным
   backoff (50мс → 100мс → 200мс, 3 попытки). Атомарный счётчик
   (`uiOverflowCount`) ограничивает количество одновременных retry-горутин
   до 8, предотвращая накопление при sustained bursts; события сверх лимита
   отбрасываются с предупреждением. Per-event retry гарантирует, что
   distinct event types (например, `UIEventBeep`) не теряются при
   переполнении канала. Bridge-горутина UI вызывает
   `window.Invalidate()` на каждое событие, запуская новый кадр.

5. **Event-driven архитектура** — логика роутера разделена на три пути:

   **Startup** (`initializeFromDB`): выполняется один раз асинхронно.
   Загружает превью с retry (до 3 попыток с линейным backoff) для
   обработки временных ошибок БД/ноды. Очищает состояние через
   `resetIdentityState()`, заполняет `peers` через `seedPreviews()`
   (сортировка: сначала непрочитанные по убыванию count, затем по
   времени последней активности). Делегирует выбор peer'а в
   `AutoSelectPeer()`, который выполняет полный цикл: оптимистичный
   сброс unread, `loadConversation()`, `doMarkSeen()` и rollback при
   ошибке. Перед вызовом `activePeer` сбрасывается, чтобы
   `selectPeerCore` всегда видел переключение peer'а и запускал
   полную загрузку (важно для reconnect, когда `activePeer` уже
   был установлен). В конце запускает начальный `pollHealth()` (через
   `defer`) для заполнения DMHeaders, DeliveryReceipts и диагностических
   полей. После запуска ebus-события поддерживают все критичные для UI
   поля актуальными без поллинга.

   Поскольку ebus-события приходят параллельно с `initializeFromDB`,
   `seedPreviews()` защищает от startup race: если ebus event-path
   уже доставил для peer'а более свежие данные (новый timestamp),
   `seedPreviews` пропускает этого peer'а, а не перезаписывает stale
   данными из стартового снимка.

   `resetIdentityState()` очищает `peers`, `peerOrder`, `activePeer`,
   `peerClicked`, `activeMessages`, `seenMessageIDs`, `initialSynced`,
   `sendStatus`, `pendingScrollToEnd`, `pendingClearEditor`,
   `pendingRecipientText`. `cache` (ConversationCache) очищается через
   `Load("", nil)`, а не заменой указателя, потому что event-горутины
   держат ссылку на тот же объект cache и вызывают его методы конкурентно.

   **Обработчик событий** (`handleEvent`):
   Определение активного peer'а использует `isActivePeer()` (проверяет
   `r.activePeer` под блокировкой), а НЕ `cache.MatchesPeer()`. Это
   критично, потому что при переключении peer'а `activePeer` обновляется
   сразу в `selectPeerCore()`, а cache — только после завершения
   асинхронного `loadConversation()`.

   - Новые сообщения для **активного разговора** с загруженным cache
     расшифровываются inline через `DecryptIncomingMessage`, добавляются
     в `ConversationCache`, и `activeMessages` обновляется.
     `RouterPeerState.Preview` обновляется для отражения нового
     сообщения. При неудачной inline-расшифровке `loadConversation`
     перезагружает историю, а `updatePreviewFromStore` обновляет превью
     из SQLite. `doMarkSeen` вызывается для каждого входящего
     сообщения — чат на экране = прочитан.
   - Новые сообщения для **активного разговора** с НЕзагруженным cache
     (в процессе переключения) расшифровываются inline через
     `DecryptIncomingMessage`, результат `*DirectMessage` сохраняется.
     При успешной расшифровке `RouterPeerState.Preview` обновляется
     немедленно, peer продвигается в `peerOrder`. Фоновый
     `reloadAndRefreshPreview()` запускается всегда. При **успешной**
     перезагрузке `updatePreviewFromStore` обновляет превью из SQLite
     для консистентности. При **неудачной** перезагрузке, если
     расшифрованное сообщение было сохранено, fallback-путь загружает
     его в cache через `cache.Load()` и копирует в `activeMessages` —
     пользователь видит сообщение в открытом чате вместо пустого экрана.
     Без этого fallback транзиентная ошибка chatlog при mid-switch
     молча теряла бы успешно расшифрованное сообщение.
   - Новые сообщения для **неактивных чатов** обновляют превью через
     `updateSidebarFromEvent` (`RouterPeerState.Preview` + `Unread`),
     продвигают peer'а в `peerOrder`. При неудачной расшифровке
     (ключи контакта ещё недоступны) роутер переходит к
     `updatePreviewFromStore` в фоновой goroutine, увеличивает
     `Unread` для входящих сообщений и продвигает peer'а в `peerOrder` —
     поведение идентично успешному inline-decrypt пути.
   - **Звуковые уведомления**: `UIEventBeep` эмитится для каждого входящего
     сообщения (sender ≠ мы) в `onNewMessage`, покрывая три code-path:
     (1) неактивный peer, (2) активный peer mid-switch (cache ещё не
     загружен), (3) активный peer с ready cache. Repair-path в
     `repairUnreadFromHeaders` эмитит `UIEventBeep` **только для
     неактивных peer'ов** — сообщения активного peer уже видны на экране,
     повторный beep при repair привёл бы к дублированию уведомления после
     восстановления от транзиентной ошибки.
   - Обновления квитанций для активного peer'а обновляют кеш in-place
     через `ConversationCache.UpdateStatus()`. Если cache ещё не загружен
     для активного peer'а — запускается `loadConversation()`. Если
     сообщение отсутствует в cache — также запускается полная перезагрузка.

   **Стартовый `pollHealth`**: `ProbeNode` + `repairUnreadFromHeaders`.
   `repairUnreadFromHeaders` сканирует DMHeaders на предмет ID сообщений,
   ещё не виденных в `seenMessageIDs`, добавляет неактивные входящие в
   МНОЖЕСТВО непрочитанных и запускает `loadConversation` + `doMarkSeen`,
   если в активном чате есть сообщения, отсутствующие в cache. С тех пор как
   бейдж стал множеством, отдельное правило про первый sync не нужно: одно и
   то же сообщение из SQL-чтения и из header — один элемент. Чего header
   по-прежнему не может сказать — было ли сообщение ПРОЧИТАНО: DMHeaders не
   несут `delivery_status`, а in-memory топик ноды переживает сессию
   desktop-а, поэтому на первом синке UI, подключившийся к работающей ноде,
   получает назад все сообщения прошлой сессии. Только на этом синке
   `alreadyReadHeaderIDs` спрашивает у базы сохранённый статус кандидатов
   (`StoredMessageStatuses`) и гасит ровно те, которые она называет `seen`;
   id, сохранённый но непрочитанный, и id, которого база не держит вовсе,
   одинаково бейджатся из header. Независимость эта намеренная: прежняя
   версия полагалась на стартовый seed бейджей, и seed, который не отработал,
   оставлял все сохранённые сообщения без бейджа на всю сессию. Неудавшееся
   чтение не гасит ничего: лишний бейдж снимается открытием диалога,
   потерянный — нет. Ещё два правила на этом пути — оба про работу, которая переживает
   ответ, на котором была основана. Какой диалог на экране, решается в
   фазе 3 под замком, а НЕ во время скана: и скан headers, и запрос
   статусов идут вне замка, а сообщение, классифицированное как видимое
   после того, как пользователь ушёл из диалога, теряет бейдж навсегда —
   его id всё равно проходит dedup-гейт, а этот ремонт выполняется один
   раз за процесс.

   Для предотвращения
   двойного подсчёта с event-path `onNewMessage()` регистрирует
   `event.MessageID` в `seenMessageIDs` в самом начале — до любой другой
   обработки — так repair-path пропускает сообщения, уже обработанные
   event-path.
   Однако если фоновый fallback завершается неудачей (например,
   `loadConversation` или `updatePreviewFromStore` возвращает `false`),
   ID сообщения **удаляется** из `seenMessageIDs` через
   `evictSeenMessages()`, чтобы `repairUnreadFromHeaders` мог обнаружить
   его при следующем health poll. Без этого отката dedup-gate навсегда
   подавлял бы сообщение. Тот же откат применяется и к самому repair-path:
   `refreshPreviewForPeer` удаляет ID сообщений когда
   `updatePreviewFromStore` возвращает ошибку, так что следующий цикл
   repair повторит обновление preview.
   Активный peer исключён из `refreshPreviewForPeer` — его preview
   обновляется через `loadConversation` + `updatePreviewFromStore`.
   Если `loadConversation` прошёл, но `updatePreviewFromStore` не удался,
   `seenMessageIDs` **не откатывается** — сообщения уже в кеше и на
   экране. Откат привёл бы к повторному обнаружению и ложному
   `UIEventBeep`. Stale preview обновится при следующем сообщении или
   переключении peer. `UIEventBeep` эмитится только для неактивных
   peer'ов — сообщения активного peer уже видны, уведомление не нужно.
   Вся логика отката централизована в `evictSeenMessages()` и
   `reloadAndRefreshPreview()` для устранения дублирования.

   **Seen-квитанции** (`doMarkSeen`): вызывается из `SelectPeer` и
   `AutoSelectPeer` через `selectPeerCore`, а также из `onNewMessage`
   для каждого входящего сообщения в активном чате. Принцип: если чат
   на экране — сообщения считаются прочитанными. Бейдж сбрасывается
   оптимистично; при неудаче `doMarkSeen` восстанавливается.

   `doMarkSeen` сначала проверяет, что `activePeer` всё ещё совпадает
   с `peerAddress` — если пользователь успел переключиться на другой чат
   до выполнения горутины, `activeMessages` принадлежат новому peer'у и
   их использование привело бы к пустому `MarkConversationSeen` (который
   успешно завершается без реальных квитанций), ложно обнуляя unread
   старого peer'а. При несовпадении `doMarkSeen` возвращает `false`,
   чтобы вызывающий код восстановил бейдж. Также требуются непустые
   `activeMessages` — если диалог ещё не загрузился, возвращает `false`.
   `selectPeerCore` вызывает `doMarkSeen` только после успешного
   `loadConversation()`.

6. **Защита от stale-загрузки** — `loadConversation()` перепроверяет
   `activePeer` после возврата `FetchConversation`. Если пользователь
   переключил peer'а во время fetch, результат отбрасывается.

7. **Защита от stale-сообщений** — `selectPeerCore()` (общий для
   `SelectPeer` и `AutoSelectPeer`) синхронно очищает `activeMessages`
   в nil перед запуском фонового `loadConversation()` и эмитит
   `UIEventMessagesUpdated` синхронно при смене peer'а, чтобы UI
   перерисовался с пустым списком сообщений в том же фрейме.

8. **Повтор упавшей загрузки / восстановление застрявшего бейджа** — Когда
   пользователь повторно кликает по уже выбранному peer'у, `selectPeerCore`
   (с `userClicked=true`) проверяет два условия: (а) cache miss
   (`!cache.MatchesPeer()`) → повтор `loadConversation` + `doMarkSeen`;
   (б) cache валиден, но `Unread > 0` (бейдж застрял после отката
   `restorePeerUnread`) → повтор только `doMarkSeen`. При валидном кеше
   и `Unread == 0` клик — no-op. `AutoSelectPeer` (`userClicked=false`)
   при same-peer всегда полный no-op.

9. **Panic-safe startup** — `runStartup()` использует два отдельных `defer`:
   `defer close(startupDone)` (зарегистрирован первым, выполняется последним) и
   `defer recoverLog("initializeFromDB")` (зарегистрирован вторым, выполняется
   первым). LIFO-порядок defer в Go гарантирует, что `recoverLog` ловит panic
   через `recover()` до того, как `close(startupDone)` разблокирует event listener.
   Оба должны быть top-level `defer` вызовами — оборачивание их в одну
   `defer func() { ... }()` сделало бы `recover()` вложенным вызовом, который
   в Go не ловит panic. Без этого panic в `initializeFromDB` навсегда
   отключала бы весь event-driven слой на время сессии. `runStartup()` и
   `runEventListener()` вынесены в именованные методы (а не анонимные горутины),
   чтобы unit-тесты могли вызывать их напрямую на контролируемом DMRouter
   без дублирования production-логики.

### DeliveredAt после рестарта

После рестарта ноды in-memory receipts пусты, но столбец `delivery_status`
в SQLite сохраняет значения "delivered" или "seen". Без специальной обработки
`DeliveredAt` будет nil и UI не отрисует галочки статуса (✓/✓✓).

Решение: `decryptDirectMessages()` синтезирует `DeliveredAt` из timestamp
сообщения, когда `PersistedStatus` = "delivered" или "seen", но in-memory
receipt отсутствует. Рендеринг switch также явно обрабатывает строки статуса
"delivered" и "seen", чтобы бейджи отображались даже если `DeliveredAt` nil.

Когда позже приходит реальная delivery-квитанция с тем же рангом статуса
(например "delivered" → "delivered"), `ConversationCache.UpdateStatus()`
разрешает обновление, если оно заменяет nil/zero `DeliveredAt` на реальную
временную метку. Это заменяет синтетическое значение на фактическое время
доставки без необходимости повышения ранга статуса.

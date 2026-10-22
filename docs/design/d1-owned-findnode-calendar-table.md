# Original owner calendar pointer table

S3C2b replaces only the inactive `intakeCoordinator.calendars` lookup map with
`[MaxPendingTotal]*ForwardCalendar`, using the existing bound of 512. It keeps
the same owner, mutex, pending table, source owner, group ledger, capacity
companion and calendar objects. It introduces no public API, activation, Ready
publication or new policy limit.

Each nonnil cell holds an original calendar pointer. Its immutable target is
the complete identity key; nil is the vacant marker. There is no mirrored key,
occupancy counter, pool, reusable mutex cell or generation. Existing identities
keep their exact calendar pointer and projected/actual debt across connections
and priority levels. The two private read helpers scan at most 512 cells and
return only a pointer or logical count, never an index or table view.

For a new target, commit first selects a vacant cell without publishing it.
Only a full table may choose a victim. A victim must have no pending target
obligation and satisfy the unchanged `retireableLocked` predicate while its
original mutex is held: no aliases, nonregressing time past both bucket times
and last use, and both independently refilled bucket copies full. These trials
do not refill or reset live debt. The calendar mutex is released before trying
the next victim; original lock order is preserved.

The selected index is local scratch under continuously held owner ownership.
A genuinely fresh `NewForwardCalendar` is still constructed before subsequent
trials. All source, group, ID, correlation and capacity checks and the final
source CAS retain their order. Only after that claim succeeds does one scalar
store publish the pointer in the selected cell. Every earlier refusal leaves
the full pointer set unchanged. Physical indices never enter pending state,
reservations, permits, source references or exported snapshots.

Removing a table pointer does not retarget, clear or recycle the old object.
Ended handles may retain that original object but cannot gain authority over a
replacement. Cancel, close and expiry release existing occupancy and aliases
without refunding projected or actual tokens. An answer during dispatch keeps
the terminal writer obligation; retirement remains blocked until completion.
Capacity shrink does not rebuild the table or erase source/group history.

The former map could select any safe victim. Physical first-safe selection
refines that permissible set; it does not reproduce Go map iteration order or
create a fairness guarantee. Different safe choices can affect which returning
identity later receives a fresh initial budget. Tests compare exact states for
zero/unique safe victims and independently compute the lawful set before
branching the frozen map reference to a multi-safe observed member. They retain
all survivor pointers and scalar calendar/alias histories. Frozen bucket and
retirement arithmetic is separate from the production helpers.

A hit scans at most 512 cells. A miss scans at most 512 lookup, 512 vacancy and
512 retirement cells, with at most 512 sequential old-calendar mutex trials.
Counting and snapshot traversal each scan 512 cells. Existing calendar alias,
pending and group work remains additional. On an 8-byte-pointer ABI the table
payload is 4096 bytes; this is not a measured owner/heap/RSS/GC or CPU bound.
Eager owner initialization and layout are unmeasured. Retained ended handles
can keep old heap objects alive through unlimited replacement cycles; this
slice does not establish a global retained-handle bound.

`budget.go` and `pending.go`, including standalone public behavior, remain
unchanged. Calendar alias maps, pending correlation maps, fresh calendar/slot
allocation, diagnostic publication and later lock waits remain. This is not
full C2 or an allocation-free C3 commit. SOURCE and functional definitions,
including eight prospective causal mutations, require independent CODE,
trusted FORMAT/rebinding, TYPE/LIST/input closure and qualified ordinary/race
and physical tests. G2, resource acceptance, activation, S6 and release remain
separate gates.

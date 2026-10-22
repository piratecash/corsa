# Fixed storage for the inactive FindNode group ledger

S3C2a replaces the two maps inside the original `candidateGroups` value with
fixed arrays. It is an inactive source implementation. It does not enable
admission, publish Ready, complete G2, or accept resource costs.

The original coordinator mutex still owns one ledger, one generation and the
existing debt and references. The named prefix contains at most 256 unique
groups in first-successful-name order; all later physical groups share cell
256. Names remain sticky until the existing idle reset succeeds. The record
prefix contains at most 7952 unique `(cell, target identity)` keys. Its two
lengths describe these authoritative prefixes; they are not additional quota
or history books. Both inactive tails are zero.

Plans keep the original validation, checked slot plus 600 seconds, active-cell
share and hard-H rules. Active cells come from record keys plus the candidate
cell, not retained names. A new key is refused at the current share or hard
limit. An existing key can acquire another reference even above a reduced
share or H after a capacity shrink. Expiry and cancellation do not refund debt.
The early ordering cell for an unnamed group remains 256 even when its first
successful commit could give it a named cell.

A plan is consumed by the same groups value under one uninterrupted original
coordinator lock. Commit re-finds the semantic key and writes only a fresh
record slot for a genuinely new key. The old maximum-reference check remains
before the source CAS through a value read. No new fallible check, allocation
or callback is added to this group portion of the commit suffix. Other
coordinator, pending and calendar maps and allocations remain.

Expiration removes only zero-reference rows at or beyond their expiry. It
moves the last active row into a hole, clears the former last slot and checks
the hole again. No physical index enters a plan, pending reference or public
handle. Finish, release, Cancel and Answer continue to use the semantic key
behind the original owner, pointer, generation and phase checks. Finish keeps
the maximum of prior expiry and completion plus 600 seconds. Release requires
a current owned reference; a corrupt missing key does not create a ghost row.
Reset retains the old empty-row, idle-window and generation-overflow checks.

For N names, R records, G=257 cells and maximum compared group byte length L,
plan has conservative work O(1+N*L+R+G); record lookup, commit, release and finish
have O(1+R) work. Counts are O(1). Expiration is O(1+R_initial), and successful
reset is O(1+N). N is at most 256 and R at most 7952. String backing bytes are
not bounded by these header counts. Eager initialization, layout, heap/RSS,
GC, lock duration and CPU cost are unmeasured. Fixed group slots do not prove
the entire admission path is allocation-free or bounded in elapsed time.

The new differential fixtures contain a mechanically renamed, independent
frozen copy of the preceding map implementation. They compare every operation
and semantic state, including the full-H witness 3840+1920+1280+912=7952 and
4640 retained rows above the reduced H=4624. Additional authentic coordinator
fixtures cover refusal without changed authority and semantic references across
compaction. SOURCE controls pin the two production files, shapes and finite
call/type/field roots; this is syntax and raw-byte closure, not typed dataflow.
Eight prospective physical mutations require qualified positive targets and
independent causal behavioral failures. Definitions alone are not test results.

S3C2b/C3, remaining ownership and commit closure, loaded-hint bootstrap,
resource acceptance, activation, S6 and D1 release remain separate work.

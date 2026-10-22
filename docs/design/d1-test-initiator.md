# D1 test and 13b initiator driver

D1 has no public lookup API (18a §5.9). This driver is compiled only into the
Node test executable. Its workload supplies a finite set of target identities
before running. The original bounded reader owner supplies received bytes;
the driver creates no reader, per-request goroutine, routing table operation,
negative wire response, capability advertisement or construction Ready path.
This is executable functional test Source, not a completed T5 process launcher,
13b load measurement or resource acceptance.

Enumeration uses the real peerSendableConnectionsLocked ordering for
CapOverlayResponderV1 and then checks each original ProvenSession, original
common, published findnode Session, handshake capability and exact writer.
It chooses one connection per relay identity, at most three. Declared identity
metadata cannot replace proof. Before sending it rechecks the retained exact
common/core/session after Node waits, context and the original timeout.

Each lookup retains one fresh random challenge, ID, target, context, start time
and up to three exact originals. Pending storage has sixteen slots. The finite
workload target set owns three start timestamps per target, enforcing at most
three lookup starts in thirty seconds. Cancellation and local send failure do
not refund an already charged lookup or ID. IDs never wrap into reuse. Poll
releases cancelled or twelve-second-expired original attempts; a late reply
cannot complete the released attempt again. Time is the workload's monotonic
elapsed clock, distinct from product admission/owned-clock authority.

The closed outcomes are Found, NoOverlayNeighbours, Timeout, InvalidAnswer and
Limited. Cancellation is a context error, with no invented network status.
An answer must arrive on a retained exact original, have the saved envelope ID,
strict target/key relation and valid target signature over the retained
challenge. An unrelated ID is ignored. One invalid relay leaves another
original relay's response eligible. Only local state reports a negative result.

Seven behavioral roots cover original proof/capability enumeration and the
three-relay bound, declared identity rebind and End, a real protected-stream
Found exchange, no-neighbor/timeout-equality/cancel, malformed/wrong-key-target/
signature/challenge/foreign-original/stray-ID cases, invalid-then-valid parallel
relays, sixteen pending slots and the three-per-thirty-second window. The
existing TLS fixture accepts optional test capabilities BEFORE the authenticated
handshake; existing callers retain their old capability set.

The Found fixture uses genuine original writers and bounded readers. Its
remote target signs in the test owner outside Node locks; C3f/C3g own the
separately reviewed production self-target and relay paths. Directed invalid
and parallel-response tests feed strict bytes to the retained reader endpoint
contract and do not claim those injections crossed a live socket.

Remaining release work includes T5 process/transport/isolation integration,
final 13b-D1 measurements and thresholds under separate authorization, PR5
accounting and observability, full construction/inverse closure, final QA and
release decisions. No previous native result transfers to this Source.

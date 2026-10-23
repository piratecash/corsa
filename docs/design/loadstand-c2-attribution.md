# C2 local quota attribution control

The functional stand retains the existing `fetchDatagramSummary.local_senders`
block alongside pipeline metrics and reverse occupancy/refusals. This adds no
core quota, sender, fairness or admission policy. In particular it does not
promise that DM or identity resolution will succeed while another subsystem
holds a shared quota.

The block is optional for old providers/samples: omission remains not measured,
not measured zero, and does not change existing `FullSampleAvailable` or default
executor acceptance. An explicitly present null/malformed block refuses. Present
blocks require the original counter period, the closed existing sender labels,
and every typed counter. Live counters are independently loaded atomics; normal
samples do not impose row sums or equality with a separately read reverse table.
The retained block/map/timestamps are copied before report ownership.

The explicit `original-local-get-identity-quota-v1` functional C2 profile is
recorded in the original journal key, including node, mutation, capacity, request/refusal counts and both
senders. Only this profile enables a strict C2 check. It uses the same two original
Full Service lifecycle owners and the actual local pipeline: 64 successful
`SendLocal` get_identity calls attributed to presence_prober fill the existing
64-slot bucket, then an identity_resolver call must actually be rejected and
counted QuotaCapped. Valid unknown required payload semantics keep the original
target silent; no answer/evidence, clock, limit or queue is fabricated. The fixture
refuses an occupied starting bucket, unexpected command result, concurrent counter
interference, changed Service/incarnation or a missing original metric source.
No delay, tolerance, retry or new numeric resource/performance acceptance follows.

An unsaturated calibration branch sends one original request and must retain zero
refusal growth with one occupied slot. This neither invents a successful DM nor
changes the sharing policy.

A true source read then flows through the ordinary member sample and executor.
The unmodified control must retain exact sender/dtype/occupancy attribution in its
completed functional journal. Two causal controls alter only the projection of
this genuine source sample: missing sender block or swapped sender rows. The
original executor returns its typed C2 defect; the existing runJournal path records
one sanitized failed attempt and no completed result. Original Stop/Run joins and
exact final accounting remain required in all branches. Nil C2 configuration keeps
the old executor/journal behavior and keys unchanged.

These are lightweight functional calibration controls, not measured C2 isolation,
T6 closure, Linux namespace/kernel evidence, public D1 activation or a new timing
profile. Heavy/resource comparisons and final T6 runs remain separate.

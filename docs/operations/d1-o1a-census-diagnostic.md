# O1a census: short component diagnostic

Attempt `0a66a114a62a5cbcbd6dd4a671288ee4` ended in **Go FAIL after 30.65 s**.
The first specific diagnostic was `stage=full_receipt`: the existing live
census callback's `CaptureLoadstandRouteWireFullReceipts` returned an error.
Its exact internal predicate remains UNKNOWN. This does not prove the cause
of the separate [480.60-second baseline failure](d1-o1a-baseline-census.md).

The diagnostic used original `4fcc2f7245095fc43d2225f0f0065f4ce3b26113`
plus three existing namespace-finalizer posts and nine closed stage-diagnostic
posts. The measured 640 sources remain `b70de373…`; the stand 708 sources are
`6aacd581…`. This ordinary-Off subject is separate from the tagged FullResponder
candidate. No production node, public connection or FIND_NODE load was used.

Closed stage labels preserve existing error families and refuse malformed or
unknown diagnostics; Ready replies cannot contain the diagnostic field.
No arbitrary error text, secret echo or new RPC command was added. Targeted
source QA passed 21 outer and 98 child cases. Four baseline ELF artifacts and
three derived helpers were compiled and independently checked; compile phases
did not execute those application ELF artifacts. The Go helper's ten tests
and five child cases passed. The C factory policy retained 147 instructions and
52 controls; its bytes match the prior policy.

The dedicated profile kept two Full nodes, 10,000 routes, original connect/
replacement-session and full-receipt oracles. Startup lead was 10 s, duration 20 s,
window 2–15 s and disconnect 6 s. Go timeout 90 s and C timeout 120 s preserve the
existing 30-second margin. Original periodic/reconnect metric profiles were not
changed or repeated. The same kernel `9ffe…`, worker `7b75…`, 2-vCPU/1536-MiB
guest, no NIC/disks/host sharing, lifecycle fences and three-role stop ACK were
retained. A short component result cannot establish periodic behavior or O1a
byte rates.

All 1366 boot inputs and strict initial loopback isolation passed. Console/
native/stderr are complete: **6,920,465 / 994 / 0 bytes**. Native shutdown completed
at about 32.188 s; worker exited0/reaped. The Go test exited1 and exporter exited0;
both were reaped with complete logs, without TERM/KILL or controller intervention.
The capsule contains 1348 source files, two manifests and one 2906-byte failed
record with `body_bytes=0`. No accepted metric report was produced.

ROOT, TESTER and REVIEWER independently confirmed shutdown; ACK removed only
the exact own lock. **Physical XPC release remains UNKNOWN.** No identical VM
retry was performed. The next investigation concerns the full-receipt predicate;
no original oracle or resource limit is relaxed.

A separate deterministic unit passed four outer and eleven child cases using
the original handler, identity-keyed limiter, withdrawal and strict receipt code;
35 relevant source files match the diagnostic build. With a valid newer
replacement sequence and all 100 frames/10,000 claims fully observed, 16 s of
manual-clock refill admitted 3200 claims and the strict receipt failed; 50 s
admitted 10,000 and it passed. No limiter reset, oracle relaxation, socket,
Service.Run or VM was involved. This proves a reachable short-profile confound,
not the cause of either saved 30-second or 480-second failure. The unchanged
source snapshot covers 2100 tracked Go files plus `go.mod`/`go.sum` (2102 records).

The regression's applied formatting is bound to that original unit source by
`gofmt` byte equality. Final source guards are separate results: 20 Node/105 child
PASS, followed by a failed findnode import inventory on ignored artifact copies;
then only the five findnode guards reran on the isolated tracked source and
passed 5 outer/2 child cases. The scanner was unchanged, the failed RAW remains,
and the fresh 2103-record Go manifest matches the first runner's before snapshot.
This is not presented as one combined green invocation or a production change.

[Portable evidence](evidence/d1-census-diagnostic-2026-11-20/README.md) includes
the complete RAW, source changes, build/input bindings, reviews and ACK.
This result closes neither O1a metrics, standalone 512 MB, D1, G2, public activation
nor release.

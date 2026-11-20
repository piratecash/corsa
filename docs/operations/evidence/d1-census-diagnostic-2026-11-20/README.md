# Census component diagnostic: retained failure

**Go FAIL, stage `full_receipt`; no accepted O1a rates.** See the
[English report](../../d1-o1a-census-diagnostic.md),
[Russian report](../../d1-o1a-census-diagnostic.ru.md) and `SUMMARY.json`.

This is a separate short diagnostic based on `4fcc2f7…` plus three existing
finalizer posts and nine closed census-diagnostic posts. It is neither the
current FullResponder candidate nor a repeat of the original periodic/reconnect
measurement. The historical 480.60-second failure's inner cause remains UNKNOWN.

`RAW.tar.xz` retains complete command streams and available persisted process
metadata from source QA, all four ELF builds, input preparation, both helper
builds and archive assembly. It also contains the full native console/events/
stderr, failed record, independent stop reviews and exact-own-lock ACK.
The console's complete capsule contains all 1348 selected source files.

`SOURCE.tar.xz` retains the exact diagnostic and finalizer posts, combined source
manifests, controllers, component profile, actual bindings, Go/C helper sources,
the test-only helper parameter correction, runtime/decoder sources and the pure
factory policy dump. Duplicate diagnostic posts in the combined QA overlay are
represented by its manifest and the identical `diagnostic-source` bytes.
Source-stage PENDING records describe preparation history; actual FROZEN,
build results and the runtime's final ready handoff remain separately bound.
No application ELF, kernel, initrd, cache or private credential file is included.

The additional refill unit retains its exact one-file test overlay, 35 relevant
source pins, full command streams/closure and before/after hash manifests:
2102 records mean 2100 tracked Go files plus `go.mod`/`go.sum`, not the earlier
2642-file whole-source QA inventory. Four outer and 11 child cases passed.
The 16s/50s contrast establishes a reachable mechanism only; neither saved
native failure's inner cause is proved by the unit.

The formatted v2 test and `FORMAT.json` preserve the original 43d unit evidence
and bind `gofmt(original)` to the applied 3d619 source. Final guard evidence is
split honestly: 20 Node/105 child cases passed in a runner that subsequently
FAILED the findnode import inventory on ignored artifact copies. Only those
five findnode guards were rerun on the isolated 2659-file tracked subject plus
the formatted test: 5 outer/2 child PASS, original scanner unchanged. Both
controllers, failed and successful RAW, results and independent review are
retained. The fresh 2103-record Go manifest equals the earlier runner's before
manifest; this is distinct from the original 2102-record unit snapshot.

Every archive member has a size/SHA entry in `*-MEMBERS.json`; `SHA256SUMS`
covers every top-level file. Archives use regular USTAR files with fixed
metadata and are fully read back against their source bytes. The preparer reads
only enumerated paths and performs no builds, Docker operations or VM launches.
Temporary absolute paths are original-job provenance, not a claim that those
paths exist on another machine.

The VM and all guest children stopped normally. Stop-only ACK does not turn
the Go failure into PASS or prove physical release of every XPC resource.
O1a metrics, standalone 512 MB, D1, G2, public activation and release remain outside
this result.

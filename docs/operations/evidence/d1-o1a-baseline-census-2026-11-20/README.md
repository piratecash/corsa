# O1a corrected-fixture baseline: retained failure

**Go FAIL; no accepted rates.** See [report](../../d1-o1a-baseline-census.md)
and `SUMMARY.json` for exact scope and limitations. Baseline is original
`4fcc2f7245095fc43d2225f0f0065f4ce3b26113` plus exactly three fixture posts,
not published K11. Reconnect remains NOT_STARTED.

`RAW.tar.xz` retains every command stdout/stderr/receipt from the four-artifact
build, Go API/PID1 QA, capexec build and archive assembly, plus the complete
native console/events/stderr, failed record and stop-review metadata.
The console capsule contains all original selected source bytes.

`SOURCE.tar.xz` retains the frozen controllers, exact three posts/diff,
before/after inventories, helper source bindings, canonical manifests,
original Go API source reader and generated runtime packages. Embedded absolute
paths are provenance for the original local jobs, not claims that those temporary
paths exist on another machine. ELF/kernel/image binaries and Go caches are
excluded. The kernel/root-permission prerequisites are also documented in
[root-permission evidence](../d1-o1a-root-permissions-2026-11-20/README.md).

Each archive has an exact `*-MEMBERS.json`; `SHA256SUMS` covers every top-level
file. The original failed attempt, full RAW and conditional stop-only ACK remain
distinct. No passing measurement, hidden FD proof, physical XPC release,
standalone512MB qualification or release decision is inferred from this bundle.

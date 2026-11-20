# O1a: baseline final-evidence failure

Periodic attempt `5a3698c0ef0e437eeefebfac4d04e933` ended in **Go FAIL**
after 480.60 seconds. The first error was `final process accounting unavailable`.
O1a and the current PR6 comparison remain unaccepted; rates are **UNKNOWN**.
Reconnect was not started and no identical retry was performed.

The subject was rebuilt v2.6.72 `4fcc2f7245095fc43d2225f0f0065f4ce3b26113`
plus three exact fixture changes: namespace finalization after carrier setup
and the corresponding existing source guard. The measured 640 Core sources
remain unchanged (`b70de373…`); the 705 fixture sources have stamp
`52e346f971a30a1d`. This corrected-fixture baseline is not published K11.

The four ELF artifacts, source archive and derived PID1/capexec were independently
checked. The original Go API matched both full manifests and `VerifyTree`;
all ten PID1 tests passed. Historical seccomp policy bytes remain identical:
147 instructions and 52 controls. Capexec consumes three environment SHA values;
its historically unused `NODE_SHA` is not claimed as a fourth guard. PID1 checks
the Node ELF through `FrozenArtifacts` and boot inputs.

The original 10,000 routes, profiles, 240/55-second windows, kernel `9ffe…`,
full Native worker `7b75…` and 2-vCPU/1536-MiB guest without NIC/disks/host sharing
were retained. This does not measure standalone memory under 512,000,000 bytes.

The retained summary reaches the action/sample at second 390, sample indices
52/53. It does not replace a complete report. Both target/helper pairs subsequently
exited1; `ended_not_exact` and poison are consequential refusals, not a proved
primary cause. `WaitFailed=true` alone does not establish a failed wait syscall.

Exporter exited0. Its complete capsule contains 1345 sources, two manifests and
one 2843-byte `.failed` record with `body_bytes=0`. The exact first failing branch
cannot be reconstructed. Source inspection shows the census chain collapses
clock/RPC/registry/full-receipt errors into one message. The next step is closed,
safe stage codes and separate component controls, preserving metric criteria.

Full console/native/stderr coverage is 6,913,656 / 992 / 0 bytes. VM stopped
normally, worker exited0/reaped, all guest children were reaped, without TERM/KILL
or controller intervention. Independent ROOT, TESTER and REVIEWER stop reviews
supported ACK removal of only the exact own lock. Physical release of all XPC
resources remains UNKNOWN.

[Portable evidence](evidence/d1-o1a-baseline-census-2026-11-20/README.md) includes
fixture sources, build metadata, complete RAW, failed record, reviews and ACK.
This result closes neither O1a, D1, G2 nor release admission.

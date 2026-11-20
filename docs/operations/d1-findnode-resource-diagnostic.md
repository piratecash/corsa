# Closed FindNode resource diagnostic construction

The closed Linux arm64 churn32 run on November 17 confirmed 32 → 16 → 32 → 0 TLSv2/F1 sessions, 48 distinct identities and 56 verified signed answers. The daemon and keeper peaked at 42,528,768 bytes under a 512,000,000-byte cgroup limit with swap disabled and no memory-limit or OOM events. The stock CLI keeps FIND_NODE off. [Detailed churn32 results](d1-findnode-resource-diagnostic.ru.md#churn32--восстановление-после-смены-16-соединений).

## Full512 functional coverage — November 20

Two separate NativeVZ runs passed on one frozen Linux arm64/CGO0 test ELF (Go 1.27.4, base `5a4713c1a1c3d3ca60dc35de2fc3ad7500985c21` plus the [nine-file overlay manifest](evidence/d1-full512-2026-11-20/SOURCE-CHANGES.sha256)). Active verified 512→513→512, two signed answers and refusal before intake (Go PASS 1.13 s). Mixed verified registry/TLSv2 1024→1025→1024, complete originals 512→0→512, no admission during overflow, a retained signed answer, and exactly one new admission on recovery (Go PASS 2.58 s). Application quotas were 1037 frames/38,186 bytes and 1035 frames/37,699 bytes, respectively, without exceeding their limits.

Each guest had **2 vCPU and 1536 MiB**, with Service and synthetic clients together, no NIC/disks/host sharing. This does not measure standalone corsad under 512,000,000 bytes. Host work/cleanup/kill/reap fences stayed 360/380/398/400 seconds; console/native caps were 4 MiB/16 KiB. No aggregate host hard caps or guaranteed host no-swap apply. All RAW/input hashes, normal VM stop, worker reap and owned process absence were independently checked; exact owned locks were released after ROOT/TESTER/REVIEWER review. Physical release of all XPC resources remains UNKNOWN.

Preserved preparation failures identified a missing initramfs `/dev/null` and a later startup timeout. A bounded diagnostic demonstrated unready guest entropy; adding the standard virtio entropy device made `getrandom` ready and the same test started. No product RNG was replaced; the exact originally blocked stack was not captured. The replay's stale preflight filename was corrected to the exact frozen caller selected by `t.Helper()`, with strict negative checks retained. The v8 replay/admission QA suite passed all 21 tests.

[Result and scope](evidence/d1-full512-2026-11-20/RESULT.json) · [Active replay](evidence/d1-full512-2026-11-20/active/replay.json) · [Mixed replay](evidence/d1-full512-2026-11-20/mixed/replay.json) · [RAW hashes](evidence/d1-full512-2026-11-20/SHA256SUMS). Historical HANDOFF paths record input provenance, not instructions to rerun.

## Churn512 under a shared 512 MB limit — November 20

A private standalone Linux arm64/CGO0 corsad completed **512→256→512→0** TLSv2/F1 sessions, **768** distinct TLS-proven participants and **896** verified signed answers (256 + 128 + 512). All 768 identities issued requests; 512 distinct targets signed answers about themselves. Surviving ConnIDs stayed unchanged, replacement ConnIDs were fresh, and the final roster was empty. The driver Go test passed in **101.23 seconds**.

Corsad + keeper + sampler shared **1 CPU / 512,000,000 bytes / 64 PID**, with swap disabled. The final kernel-charged peak after daemon and sampler reap was **229,769,216 bytes (219.13 MiB)**; inclusive group CPU was **8,782,556 µs**, without deductions. All 201 samples were retained, maximum gap 0.538626167 seconds. Intake Offered/Committed/Taken/Answered were each 896; Refused/Replaced/Violations, memory max/OOM/OOM-kill and PID-limit events were zero. The separate 1 CPU / 256 MiB / 32 PID driver peaked at **94,429,184 bytes**, CPU 1,305,476 µs. Both streams reached full EOF and normal exit 0. The four own containers and three volumes were removed; the own heavy-lock was released after complete cleanup.

The subject is base `93eb8fe75d2a90710d21409c535276c6c7ed28f2` plus the exact private diagnostic overlay, built with Go 1.27.4. The preparer and topology helper had separate fixed budgets and were removed before useful work; the existing offline 180-second work / 30-second cleanup profile stayed unchanged. Ordinary factories remain off. [Portable evidence](evidence/d1-churn512-2026-11-20/README.md) includes complete compressed RAW, the source patch, BuildInfo and three independent DATA reviews.

The preserved v6 failed after 512 answers on the 513th new target in one source group. Disconnect does not erase its ten-minute target history. Reversing only the first block's request directions retained 896 operations and 768 participants while limiting distinct targets to 512; policy caps and deadlines were unchanged. The verifier additionally requires that exact target union. A separate Go temporary-file accounting race was fixed in the build wrapper; 19 fixture/build and 61 runtime checks passed.

This qualifies the **selected F1/churn resource workload**. Late keeper tail, shared-page attribution, process RSS and preparer costs remain UNKNOWN. It does not establish mixed1024 memory, every D1 workload, Responder authorization or final PR‑6 readiness. [Closed role enforcement has a separate source/QA result](d1-findnode-local-role.md) and the current-subject repeat below. O‑1a, evidence applicability and remaining final-candidate 13b-D1 checks, G2 and public activation decisions remain open. S6 observation runs in parallel for public activation.

## Current closed-role Churn512 — November 20

One current shoulder on signed Go commit `08c3c5ea0c7d6bc34c634060d1a5990d0ca745a6` plus five private diagnostic posts repeated the **same** workload, driver, topology and limits: **512→256→512→0**, **768 participants**, **896 verified answers**, **512 distinct signing targets**. The typed closed FullResponder role was retained; ordinary constructors stay Off. The complete 2498-file build snapshot retains ancillary files from base `1b97da9`, with exact before/after manifests and CLI SHA `4010d590…`. [Portable evidence](evidence/d1-churn512-current-2026-11-20/README.md).

| Inclusive measurement | Retained old | Current | Difference |
| --- | ---: | ---: | ---: |
| Service-group peak, bytes | 229,769,216 | **210,948,096** | −18,821,120 |
| Service-group CPU, µs | 8,782,556 | **8,653,066** | −129,490 |
| Driver peak, bytes | 94,429,184 | 92,295,168 | −2,134,016 |
| Driver CPU, µs | 1,305,476 | 1,336,855 | +31,379 |

These are two retained single observations; the old shoulder was not rerun. They establish no causal saving, statistical CPU change or cost per session. All202 samples were retained (maximum gap0.538766084s), four complete membership/ConnID and STATE phases passed, intake counters were896 with zero refusals/violations, and no memory max/OOM/PID-limit events occurred. Actual `/proc` executable hashing bound the CLI. Daemon/sampler reap preceded the final inclusive peak/CPU read; final keeper membership was `[1]`. Both streams reached EOF/normal0. Four owned containers, three volumes and the heavy-lock were removed; ROOT/TESTER/REVIEWER accepted retained inputs, RAW and cleanup evidence.

Only a public0444 copy fixed an input-staging permission issue before execution; original build permissions, all eight reused inputs, CHOWN-only preparer and oracles stayed unchanged. Preparer/topology cost, process RSS, shared-page attribution and late keeper tail remain UNKNOWN. This updates applicability for the selected closed-role F1/churn workload; O‑1a, remaining13b-D1 scenarios, G2/public activation and parallel S6 observation remain separate. [Result](evidence/d1-churn512-current-2026-11-20/result.json) · [Hashes and complete RAW](evidence/d1-churn512-current-2026-11-20/SHA256SUMS).

## Selection and ownership

The Core and headless App constructors named `NewD1FindNodeResourceDiagnostic` exist only with the exact `d1_resource_probe` build tag. There is no configuration flag, mutable capability setter, generic construction callback, caller clock, or clock-authority export. The ordinary Core/App constructors and stock `cmd/corsa-node` remain F1-off, including when the tag is present. The ordinary tracked small2 source has no diagnostic CLI or external request driver; the private measurement overlays above are separate.

The closed constructor uses the existing real RuntimeDomain → Ready construction → original checked bindings → original Seal path. F1 is selected before the new Service escapes or a handshake starts. It does not select FixtureDomain, signing test gate, loaded N-in, contact reconciliation or own-DM runtime. It requires a full node, enabled listener, literal127.0.0.1 listen/bootstrap addresses, at most one bootstrap endpoint, and no proxy or connect-only configuration; invalid configuration is refused rather than silently rewritten. Construction alone does not start Run/listen.

Fixed policy hypothesis:

| Parameter | Value |
| --- | --- |
| Coverage limits | 2 active / 2 members / 4 registry visits |
| Relay target/network/runtime budgets | 2s / 2s / 1s |
| Round/slack | 100ms / 100ms |
| Initial relay tokens | 4 |
| Target service bound/runtime reserve | 2s / 500ms |
| Signing spacing/reservations | 250ms / 8 |

These are fixed diagnostic inputs, not demonstrated physical latency bounds, full512/512/1024 coverage, an eight-request service guarantee, G2 or an activation decision.

`D1FindNodeResourceReadout.SnapshotForService(expected)` binds the exact original Service, capture/intake/terminal/consumer/publisher, identity and done lifetime before reading the owner. The App wrapper `SnapshotForApp(expected)` additionally checks that the original App still owns that original Service. Nil, foreign, changed or closed binding returns UNKNOWN before owner reads. The final check can observe closure during the reads but does not make lifecycle and snapshot atomic. The readout retains no Ready object or additional peer-state copy; its same-owner handle retains the original Service. It exposes no admission or mutation handle.

The returned value contains existing `ResourceSnapshot` publications and `OwnedRoundPending`, not a new collector. These are independent last-publication/operational reads; a zero, pending hint or structural byte floor is not a coherent heap/RSS/cgroup measurement. `ExternalClockBound` and `PhysicalTimeBoundsProven` remain false. A returned copy cannot mutate the owner.

## Original membership checkpoint

The same tagged readout now provides `CheckpointForService(expected)` and App `CheckpointForApp(expected)`. This is a fixed-value **membership-only** checkpoint for at most two active original proven listener lifetimes, at most two F1 members and four registry visits. It returns sorted original ConnID/identity rows and whether each exact original Session was published as F1. A proven listener without F1 uses identity from its original genuine degree witness, not a cached hello/address label. Outbound scope, missing or inconsistent original evidence, poisoned/unknown coverage, overflow, changed ownership/lifetime or changed generation refuse the **whole** checkpoint with UNKNOWN and zero rows/counts; no partial result becomes complete.

Original registry/common/degree mutexes protect the copied mutable fields. Before/after private original capture stamps must denote the same completed shared-W generation; the registry rows join degree rows by exact original lifetime **and witness** pointers, original ConnID and identity. No clock is sampled or exported, and no admission, Seal, degree publication, collector, retained Session handle or caller callback is added. The DTO is scalar/fixed-value only. Existing mutex waits can block: the cardinality bound is not a no-wait or physical-time guarantee. The result describes registered original listener membership in that generation, not a durable promise of future membership. Ended registry tombstones still consume visits; zero active rows do not imply an empty registry.

`RetirementStatus` and `DrainStatus` remain unconditionally UNKNOWN. Published membership ending does not prove that deferred intake/local retirement callbacks or request lanes have drained. The existing resource snapshot remains separately sampled and is not included in this DTO. Neither equal counts, `Intake.Refused==2`, silence nor driver exit is a completion/admission/RAM oracle.

### Membership GoQA evidence

A new separately admitted offline job exited0 with `SCOPED_GOQA_SUCCESS_NOT_RUNTIME` and known scoped process closure. Both native Darwin/arm64 test commands passed, with and without `d1_resource_probe`. Their static inventory is35 selected functions with the tag/28 without, including the original raw guard baseline and negative families; these are function selection counts, not an invented subtest/full-suite census. Seventeen recorded commands exited0, with159055 bytes across34 RAW files and zero stderr. The new tests use genuine sessionv2/TLS over owned **in-memory net.Pipe**, original proof/auth/registry/publication/End, a causal generation-change barrier and exact-pointer-join negatives. Test-only background-admission quiescence isolates that barrier; it is not a production drain claim. End race probes are race-tolerant, not proof of forced concurrent scheduling. No Service.Run/App.Run/listener, diagnostic CLI, external request driver or daemon memory workload ran.

Linux/arm64 CGO0 Core/App test ELFs were compiled and statically inspected, **never executed**. Go1.27.3 used-dependency metadata, source/module/SDK invariance and `go mod verify` were checked within the same scoped job, with the same composite h1 boundary described below. The subject was retained Source8afa7f5 plus a closed22-file overlay: constructor16 and checkpoint11 with five exact chained overlaps, producing2193 copied source files. This verifies the specific original-membership construction/test subject, not all contemporary HEAD or useful signed FindNode request processing.

| Membership evidence | SHA-256 |
| --- | --- |
| Checkpoint11-file Source | `58f5b99ee7b1846a59628708962a56305f057a5d468d7d9b8d2c0cc8bdb0b062` |
| Merged22-file build overlay | `a5a573e0429c5b6585e38487aa432e9095db8920e1a7d78c6e5a9f3f7675665a` |
| Copied source manifest | `331bd72bfcddcb1c88697af5bafdd5ab5483dcba26d7e14aa986160f128c32d2` |
| Copied module manifest | `4b761388c2f3b52c3e18c720ef4ed7285c146edff2e0b4662ee154bd6a1c8e13` |
| Unexecuted Core Linux test ELF | `79b599f32661e399307ce84fd30848a8c7decd60bb03ce15c253d28e5999877e` |
| Unexecuted App Linux test ELF | `ac483086559a5beffca3c3ba01116b2a9183bd7df8324bf752fb1f61e1e8d45e` |

The soft host controls and clean-source reproducibility limitations below still apply. This is no standalone512000000-byte qualification, coherent RAM observation, physical P0/G2 guarantee, activation or release approval.

## Earlier constructor-only verification and provenance

The corrected job exited0 as `SCOPED_GOQA_SUCCESS_NOT_RUNTIME`, with known scoped process closure.17 recorded commands, including the helper's ps checks, exited0. The fixed native Darwin/arm64 selection passed with and without the tag. Its inventory is28 selected functions with the tag/25 without, covering actual constructor/readout/default-off and original Source closure/negative families. These are selected function counts, not invented per-subtest or full-suite counts. No Service.Run/App.Run/listen/TLS workload was selected.

Linux/arm64 CGO0 Core/App test binaries were compiled and inspected as static ELF; they were **not executed**. Go1.27.3 metadata settings and used-dependency h1 were checked against source go.sum, together with successful `go mod verify` and unchanged pinned source/module/SDK trees. `go mod verify` alone is not an upstream h1 guarantee. No stock CLI was rebuilt.

| Evidence | SHA-256 |
| --- | --- |
| Closed16-file Go overlay | `5622cb959a652e8896497d9500c6208ef200ec8d4a04970fc750fdd2fdc05171` |
| Copied source manifest | `8befc9226535171616d996c64b5d837c2edc660cd019df12afec6325f0cc9da3` |
| Copied module manifest | `6347fce1db91ab7d047cfa18633cc370b79fe9480abb8df392ed6e9caf8176c9` |
| Unexecuted Core Linux test ELF | `5567a72a3d0fff4e5cfff3353baeaaafb606e1b24532eaf385093cd41ff0fcde` |
| Unexecuted App Linux test ELF | `6a1578a5880d1d27817347770f1d887a2f0ba4d7292df42275ef0b0394d524a3` |

Reproduce these Source guards from a clean tracked-source snapshot that excludes ignored artifact/cache trees. The existing whole-tree bridge-importer scanner does not exclude `docs/_internal`; direct execution in a workspace containing ignored Go artifact copies can count those copies. They are outside this verified subject. The scanner was neither broadened nor skipped to accommodate them; the clean-snapshot boundary is a reproducibility condition, not a workspace-wide PASS claim.

The source subject was retained Source8afa7f5 plus the exact16-file overlay, not all contemporary HEAD bytes. The earlier job's genuine Degree guard FAIL and logs remain preserved: seven exact rows for an existing K5 reader's nine `origin` identifiers were absent from its all-Node inventory. The correction adds those literal roles and four causal missing/duplicate/unlisted/alias negative cases; all original92 rows, the scanner/predicate and production constructor bytes remain unchanged. The successful job used a fresh owned work root and a separate one-attempt grant.

Native Go settings2/-p1/GOMEMLIMIT768MiB were soft host controls, not a512000000-byte daemon measurement or hard host RAM/CPU/PID/no-swap/network sandbox. The earlier stock SG32 memory result is a different binary/scenario. It does not measure this selected F1 path.

## Next bounded boundary

Before any resource conclusion, separately review a closed tagged diagnostic entry point, its same-App value-only readout channel, and an external driver using actual TLS/F1 publication and signed wire answers. The proposed small case is two identities plus one later unnegotiated identity, at most two concurrent sessions, four distinct request IDs/challenges: one remote and one local accepted request, one ended-target attempt and one unnegotiated attempt. Refused attempts may stop before intake, so do not assume `Intake.Refused==2`; prove no new admission/commit with phase-specific owner evidence and protocol behavior. Silence or a successful driver exit alone proves neither refusal nor completion.

That boundary, exact CLI/driver builds and one resource profile require separate preparation/admission. Preserve full complete-or-refuse behavior; do not infer latency, fairness, full-load512MB, G2 or release readiness from this constructor check.

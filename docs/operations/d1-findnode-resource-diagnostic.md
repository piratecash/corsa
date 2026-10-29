# Closed FindNode resource diagnostic construction

Status: constructor, original-membership checkpoint and Source checks passed in separate scoped offline GoQA jobs. This supplies a deliberately selected diagnostic construction path; it does not enable FindNode in the stock CLI, run useful FindNode workload, measure daemon memory, approve G2 or authorize release. [Русская версия](d1-findnode-resource-diagnostic.ru.md).

## Selection and ownership

The Core and headless App constructors named `NewD1FindNodeResourceDiagnostic` exist only with the exact `d1_resource_probe` build tag. There is no configuration flag, mutable capability setter, generic construction callback, caller clock, or clock-authority export. The ordinary Core/App constructors and stock `cmd/corsa-node` remain F1-off, including when the tag is present. There is no diagnostic CLI or external request driver in this slice.

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

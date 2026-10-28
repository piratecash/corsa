# SG32 preparer-only diagnostic: admission proposal

Historical proposal: the owner selected a full SG32 attempt instead; this preparer-only packet was never activated or run. Its Host5221 byte binding predates the later manifest-mode correction and is not a current execution admission.

Status: reviewed source with a successful guarded pure test run; the final resource packet requires separate owner admission. This document does not authorize execution. Both earlier SG32 admissions are consumed; their separate claims, failures and logs remain retained.

The purpose is to observe the exact failing preparer operation, numeric errno and bounded metadata. The cause of attempt2 PermissionError is UNKNOWN. The diagnostic helper passed 28 methods/128 pure subtests; those controls do not exercise Linux root DAC with only CAP_CHOWN.

A distinct SG32-PREPARER-ONLY-v1 admission and fixed bootstrap must load qualified captured common/Host/controller bytes. The controller creates and starts exactly one preparer, then cleans up after PREPARED, ERROR, malformed output or timeout. It never creates or starts corsad, driver, keeper or sampler. The existing corsad/driver binaries are copied only as opaque qualified input bytes from the original scenario. An independent exact command boundary includes interactive start. Calling the full sg32_host.run is forbidden; the old SG32-v1 entry refuses the new profile.

One preparer: 0.25 CPU /134217728 bytes/32 PID, MemorySwap=Memory, UID:GID0:0; CapDropALL and only CHOWN/CAP_CHOWN, no-new-privileges, read-only root, network none. No host mounts, privileged mode or piratecashd. Three fresh owned volumes; secret tmpfs <=1MiB. Existing exact Linux arm64 image910cfcaf… with pull=never. No builds, downloads or application execution. Original copy modes, ownership and operation order are retained: this observes the failure rather than testing an assumed repair.

Work/setup <=180 seconds and cleanup <=30 seconds, with preparer <=20 seconds inside that envelope. Deadlines are programmed; there is no unconditional bound on a stuck FS/syscall. Aggregate host CPU/RAM/PID caps, host no-swap and hard disk quota are not provided. Keep canonical heavy-lock/PGID/local Docker endpoint. Before admission, require >=10GiB free on the host plus preparation; this does not prove free space in Docker-store. Control output <=4MiB. Only exact owned resources may be removed after label/name checks; retain available RAW before removal. Temporary volume contents are not retained or reconstructed from logs.

Error/OOM/timeout/unreliable required evidence stops the attempt without retry or limit growth. Unknown closure keeps the live controller and heavy lock for operator recovery. Confirmed owned-process/resource absence establishes scoped cleanup, not physical release of all Docker-backend resources.

Retain actual preparer interactive CLI exit, OOM/inspection, bounded original RAW and ERROR.facts: fixed operation/path labels, errno/null and available pre-operation actor/file metadata. Metadata is advisory and non-atomic, not descriptor custody proof. Do not log secrets, operation arguments or exception text. A new observation cannot retrospectively establish the historical attempt2 cause.

Before requesting execution: complete controller/bootstrap, refusal and no-service-continuation tests, independent CODE/TESTER/REVIEWER acceptance, exact source/ELF/input hashes and an inert admission with a new purpose, owner decision and one-attempt claim. owner_approved/allow_recovery_hold remain false until explicit owner admission.

PREPARED establishes only preparation. It proves no SG32 useful workload, corsad/service-group memory result, full512MB suitability, G2, activation or release. The owner handles commits separately.

## Source checkpoint and byte binding

Root executed the independently reviewed guarded pure suite: **34 methods /152 subtests PASS**, zero errors, failures or skips. Original stdout/stderr, result and identical before/after snapshots are retained. Result SHA-256: `f95ddf3b55afb5ba08bcfa09602eb68fbf1f575b09d530af0a03040740ed17eb`. The existing28/128 controls were preserved; six methods/24 subtests cover crossed profiles and owner/source refusals, captured bootstrap modules, denied runtime commands, failed copy/start-once checks, all four completion/error paths and recovery HOLD despite receipt failure. Actual controller/command/cleanup functions run against a controlled parent CLI/interactive-child boundary and ordinary owned files; this is not real Docker, kernel DAC, OOM or physical-closure evidence. No application or compiler was executed.

The immutable execution packet must bind these source bytes and the existing artifact pair below. Both binaries come from the qualified earlier Go snapshot at `8afa7f5b365a56d55f116c8a0dfb0ac581c79000`; this does not identify the entire current non-Go worktree as that commit. They are opaque inputs for this diagnostic and are not executed.

| Input | SHA-256 |
| --- | --- |
| `sg32_host.py` | `5221ad26af060e9d0b5e5dbff25b3b9df6910fc11cdc2ccb7ad5a5fe2b4348b0` |
| `preparer_only.py` | `5c2538ba6f4e9b528d0f188cde6a12f7c614189917198154f81677acf9c1c0b3` |
| `preparer_bootstrap.py` | `41ee5c8ba57d8c3a434517554bcd4052295afdd4d736921591e9cfbd5f34d932` |
| `sg32_service.py` | `4575c4a8d4e18c3d14124beb8a62559fb4f4a4833aac449a88644628b19d26aa` |
| `corsad` | `9c5764b19c5be6b635d5a1dde774c0fc4f5bcc64ac960c8d31c7f6121b4eebc4` |
| `c2driver` | `c836814111c5fc6fad55f0e4c7888060d252919a9278d57efb270594b42fafea` |

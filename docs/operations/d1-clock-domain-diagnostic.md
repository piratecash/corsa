# D1 clock-domain diagnostic

[Русская версия](d1-clock-domain-diagnostic.ru.md)

Status on 2026-10-28: the one corrected Docker attempt completed its diagnostic matrix with exit0. Keeper/observer process binding matches, but their CURRENT time namespace identities differ, so the shared-time policy refuses. This is not a resource PASS or historical64c0 root-cause proof. Both actors exited0 and owned-container cleanup is verified. No further run is authorized.

## Purpose and evidence availability

The 2026-10-27 instrumented `64c0…` OFF attempt refused with `time-clock-reader-domain`; ON was skipped. The [historical memory-screen report](d1-corsad-memory-screen.md) retains that outcome and its exact old hashes. By 2026-10-28 the original private `/private/tmp/corsa-D1-*` source, runtime package, ELF and RAW paths were no longer available. Their disappearance was observed; its cause was not established by this work.

A bounded static recovery from this D1 conversation retained the complete `CurrentBootContext` source fragment, its `fstat(fd) → [st_dev, st_ino]` comparator and relevant author edit strings. It did not reconstruct any of the six complete runtime files to their declared final SHA, or restore the `64c0` ELF. Historical reports and recovered excerpts preserve different evidence. They do not replace the missing full source package or runtime RAW. The historical root cause remains **UNKNOWN**.

This diagnostic asks a smaller new question: what process/proc and time-namespace relationships do two intended Docker actors actually observe? It is a new subject with fresh Source and future artifact bindings. It is not an exact reproduction of the old attempt or a demonstrated fix for its cause.

## Two finite actors

Proposed repository homes are `internal/testutil/loadstand/clockprobe/probe.py` and `host.py`. Import is inert. The host remains inactive without an exact future owner-admission record and verified Source/wrapper inputs.

The keeper is PID 1 in its container. It records its own self, thread-self and numeric-PID witnesses, holds labelled namespace descriptors through the interval, and takes its own CLOCK_BOOTTIME samples before and after the observer. The observer shares the keeper's PID/network namespace and records both its own three reader views and its view of keeper PID 1. The observer's view of PID 1 is not a substitute for the keeper's own clock reading. No daemon, Go ELF, peer workload, Capture, credentials, preparer, volume, host mount, published port, `setns`, `unshare` or offset translation is involved. `piratecashd` is outside the diagnostic.

For each actor, bounded facts include process birth/status/NSpid, namespace readlink diagnostics, descriptor device/inode identities, proc-mount information and raw offsets. Held and freshly reopened CURRENT and FOR_CHILDREN descriptors are rechecked around clock/offset acquisition. Offset bytes are read again and compared for stability. Offsets remain labelled **FOR_CHILDREN**, while an actor's CLOCK_BOOTTIME samples remain in its **CURRENT** context. Raw readlink text does not replace descriptor identity. No zero-offset prerequisite or inferred clock conversion is introduced.

The matrix keeps raw numeric-reader equality and actual actor-current equality separate. Self/thread/numeric view agreement, keeper before/after identity and the observer's target-PID-1 process/birth/namespace binding must also support a compatibility result. Stable mismatches can form a complete diagnostic matrix while `supported_source_guard` and `shared_time_domain` are false. Both sides are retained. Missing or changed facts, malformed values and unsupported reads remain incomplete/UNKNOWN; boolean aliases are not accepted as numeric identities. A complete diagnostic capture is not an old-runtime guard PASS, shared-domain proof under missing assumptions, or a resource PASS.

## Proposed profile — requires a fresh owner decision

| Item | Proposed bound |
|---|---|
| Actors | Two own Linux arm64 containers; keeper network none, observer shares its PID/network namespace |
| Each container | 0.25 CPU; 128 MiB (134,217,728 bytes) RAM with swap disabled; 32 PIDs; UID/GID 501:20 |
| Isolation | Read-only root; all capabilities dropped; no-new-privileges; no mounts or volumes |
| Image | Exact `sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e`; `--pull=never`; fresh platform and empty image `Config.Volumes` checks |
| Source injection | Bounded SHA-bound Python `-c` argv; no shell, new image or preparer |
| Docker endpoint | Fixed local `unix:///Users/hades/.docker/run/docker.sock`; no remote API or automatic Docker/VM start |
| Normal work / cleanup | 30 seconds including setup, then 30 seconds cleanup; programmed bounds |
| RAW | At most 64 KiB per role and 256 KiB across captured CLI streams |
| Metadata / total output | Two receipts at most 256 KiB each, plus bounded claim and console; overall logical runtime output at most 1 MiB |
| Host prerequisites | Original canonical heavy-run wrapper/lock; at least 10 GiB free plus preparation; no aggregate host CPU/RAM/PID hard caps or guaranteed host no-swap |

The host checks local availability, exact inputs, image/platform, absence of implicit volumes, space and lock ownership before attempting the one-owner claim or creating actors. A clean pre-start refusal creates no actor and consumes no claim. Any ambiguous claim or create outcome must be reported conservatively. No automatic repeat or increase of limits is proposed. A read-only preflight on 2026-10-28 found the Docker socket unavailable; this is an availability result, not a conclusion about Linux time-namespace support.

## Closure and uncertainty

The 30+30-second schedule does not guarantee termination of blocked filesystem/Docker syscalls or physical process lifetime. CLI children inherit the original heavy process group. Cleanup is restricted to exact owned IDs and checked nonce/name/role labels, followed by fresh own-resource queries and CLI reaping. Known forced removal may establish resource closure while the diagnostic remains incomplete; it does not establish normal actor exit, drain or useful observations.

If physical completion is UNKNOWN, the host controller must stay alive in the actual heavy process group, holding the existing wrapper/lock until operator recovery. A text marker or returning an error is insufficient. This idle hold can exceed 60 seconds. There is no automatic ACK. Recovery requires independently confirmed physical closure and an explicit operator/Root decision. Unexpected forced termination of the controller or host crash is outside the live-controller guarantee; the original wrapper's existing rules still apply.

RAW and metadata are bounded separately. Metadata is provisional until joined with the actual outer terminal result, checked output close and fresh custody/closure readback. Missing final data, output failure or partial cleanup cannot become successful evidence. Exact Source, admission, nonce and actor results must be bound by a later reviewed packet; labels alone do not establish shared clocks, a historical root cause or release qualification.

## Validation and retention

Independent source/adapter CODE is accepted (`35ed915e`, test-only supplement `7808743f`). The first pure invocation ended CLOSED2: eight methods passed and two failed on the fixture's noncanonical macOS temporary path; its FAIL and RAW are retained. Only the fixture was repaired, with an additional actual negative symlink-path refusal check; the Host guard and two implementation files were unchanged. The fresh full rerun (`49927c`, result `181566d1…`) passed all 10 unittest methods and 33 subTests, with zero failures, errors or skips and observed outer exit 0. Source/Main bodies and recorded metadata, and Git stage entries, were preserved. The test-region alarm is 30 seconds; fixed administrative Git metadata reads are separate. Subjects/tests ran with external process/network/fork/exec guards, controlled proc/clock/Docker boundaries and owned ordinary temporary files/FDs. This does not test real nsfs, Linux time namespaces, Docker, cgroups, applications or resource fitness. At that preparatory point the Docker/kernel diagnostic had not run; the later admitted incomplete attempt is recorded below.

Accepted Source should have a tracked repository home. New private manifests, recovery data and bounded RAW use `docs/_internal/d1-artifacts/` rather than relying solely on `/private/tmp`. That directory remains local and ignored; this arrangement does not guarantee survival of disk loss or availability on another host. The recovered historical class and caller excerpts are inert source material, not executable instructions or current authorization. No cache or previous evidence was deleted by this preparation.


The pure test entry is `python3 -I -S -B internal/testutil/loadstand/clockprobe/test_clockprobe.py`. Its controlled subprocess boundaries do not start Docker. The selected implementation hashes are Probe `c58fee4558ba4f4e5952be72ad581d1085382453ab9a914bb7fc5a931295f3f6`, Host `c7b023ba102f1b41ba479244caf1cc747cd4ac21ec4e3ec7bda853507eec7231`; the tested file is `901c0bd9d5fa9fb4950e1bd4ed8c78245f4a427b0f4010cbf29530455d35e9a2`. Workspace-local QA: `docs/_internal/d1-artifacts/2026-10-28/clock-probe-thread-offset-qa/`; availability on other hosts is not assumed.


## Actual Docker check — 2026-10-28

After the owner said “докер запустил можешь проверять” in response to the exact one-probe profile, Root explicitly proceeded with that one unchanged resource profile. Docker reports29.8.0 and kernel7.0.12-linuxkit; the fixed image is Linux arm64. A read-only preflight found that `.Config.Volumes` is omitted and the old direct-key template fails. The sole Host change uses `{{json (index .Config "Volumes")}}`; the same real image then returns `Volumes:null`. The strict image/isolation/resource guards are unchanged. Independent CODE949b06b7 and fresh pure invocation7fd163 passed the same10methods/33subTests, without subject subprocess/network execution. This is a fresh run of the existing inventory, not20unique methods. Host676a was a private tested overlay over the unchanged staged Hostce8d during the actual attempt.

The admitted one attempt (`a9c3a3`, outer exit2) returned **INCOMPLETE**: keeper emitted `FileNotFoundError`/ENOENT for `/proc/thread-self/timens_offsets`; Host refused with `keeper-capture-incomplete` and did not create observer. Initial keeper self/thread/numeric namespace FD identities were all `[5,4026532960]`; self FOR_CHILDREN offsets were zero. The thread-path read failed before a valid complete BOOTTIME bracket; there was no keeper-after/observer matrix. These partial facts do not establish shared clocks, absence of time-namespace support, compatibility with the old collector or the historical64c0 cause. The path failure needs a concrete source correction, not a weaker guard or clock translation.

One own keeper container was created with the declared isolation and budgets. The reviewed Host closed/reaped its CLI path, verified ownership and forcibly removed that container; all11 retained Docker command records exited0. Fresh own-label and exact-ID queries (`611a78`) were empty with exit0; the outer wrapper returned and its lock was absent. **KNOWN closure is bounded to this owned-container/API/CLI model**, not normal keeper exit, drain or physical release of every backend allocation. No observer, corsad or piratecashd ran. The logical retained runtime output was22,241 bytes. ROOT-ACTUAL.json SHA256 `1fe033b3f48e5d30785a98e39caeba1dfb1d2f1765c416286bfd4d10580770db` binds the local RAW manifest; TESTER38a27b05 and REVIEWERfe85da3f accepted the incomplete result and scoped closure. Their independent file readback is distinct from Root's directly observed tool terminals.

Local evidence is `docs/_internal/d1-artifacts/2026-10-28/clock-probe-runtime-docker29-v2/`, with keeper/commands/result under `runs/clockprobe-4f50de4c25fe4264a8574deb35694c58/`. Original failure and metadata/pure evidence remain retained. The one-attempt claim is consumed; **no retry was performed or authorized**. Any corrected runtime attempt needs a new decision after its source/tests/package are accepted. Current shared-time-domain and the historical root cause remain UNKNOWN; no whole-daemon memory, K5/S6, G2, activation or release qualification follows.


## Corrected thread offsets reader — pure preparation

The failed task-directory path is corrected without an offsets fallback. The reader derives a strictly bounded proc-visible TGID/TID from `/proc/thread-self` and selects `/proc/<TID>/timens_offsets`. It does not assume the native TID is the ID visible through the mounted proc. Before and after reading offsets and the actor's own BOOTTIME bracket, it binds the selected task's process birth, full parsed status and both namespace FD identities to the retained thread-self witness. It rereads the link/raw offsets and reopens held/named FD identities. Host requires the exact typed `offsets_reader` provenance and stable bookends. Offsets remain FOR_CHILDREN; BOOTTIME remains CURRENT. The old numeric-PID comparison and unsupported/current-versus-children guards are unchanged. Missing or changing proof remains UNKNOWN, without copying self offsets or translating clocks.

The structural explanation is supported by official upstream [Linux v7.0 proc entries](https://github.com/torvalds/linux/blob/v7.0/fs/proc/base.c), [thread-self link](https://github.com/torvalds/linux/blob/v7.0/fs/proc/thread_self.c) and [time-namespace offsets](https://github.com/torvalds/linux/blob/v7.0/kernel/time/namespace.c). This is an upstream reference, not proof of exact LinuxKit7.0.12 patch identity. The retained actual ENOENT and successful self offsets are the observations of this installed environment.

Sourceb312dfa1/Testdd1d205e received independent combined CODE835ccaa5. Fresh Root invocation8475ed exited0 with14methods/58subTests, no failures/errors/skips; Source/Main and index entries were preserved. The corrected fixture actually denies task-directory offsets, while cases cover nonleader/proc-native mismatch, distinct raw offsets without self-copy, malformed links, changed/missing process/status/both namespace facts, FD-close failure and strict Host provenance. This remains pure controlled-proc/clock/Docker testing with owned regular FDs, not Linux/nsfs or resource proof. A later CODE receipt corrected only explanatory total-byte text (88855→88555); the original active plan/CODE and actual run binding remain unchanged, without retroactive authorization or a repeated test.

The selected current source/test hashes above bind these tested files; fresh local evidence is `clock-probe-thread-offset-qa/`. Historical Docker Source/claim/result remains intact under the separate Docker29 packet. A new inert packet must bind these corrected bytes and the same resource profile before requesting one further attempt. At that pure-preparation point no corrected-container run had occurred; the subsequently admitted single attempt is recorded below. The original INCOMPLETE is retained. At that preparatory point shared-time qualification was UNKNOWN; the current actual result is recorded below. The historical cause remains UNKNOWN and all K5/S6/512MB/G2/release limits remain.


## Corrected attempt: complete matrix, shared-time policy refused

The owner explicitly approved one corrected attempt. Fresh input/Docker checks passed; the unchanged exact image ran on Docker29.8.0/LinuxKit7.0.12. Tooladb93c/session54324 finished at733f46 with outer exit0. All three packets (keeper-before, observer, keeper-after) are complete, with valid own-clock brackets, proc-visible thread-reader provenance and keeper lifetime bookends. `cross_actor_binding_equal=true`: observer's PID1 view matches the original keeper process, status, birth and namespace witnesses before/after. This current result does not support a PID-view mix-up as its explanation.

| Witness in this run | CURRENT namespace `(st_dev,st_ino)` | FOR_CHILDREN same as CURRENT | Selected offsets path |
| --- | --- | --- | --- |
| Keeper before and after, PID/TID1 | `(5,4026532960)` | yes | `/proc/1/timens_offsets` |
| Observer own aliases, PID/TID7 | `(5,4026532965)` | yes | `/proc/7/timens_offsets` |
| Observer's target-keeper PID1 view | `(5,4026532960)` | yes | `/proc/1/timens_offsets` |

The observer's own self/thread/numeric views agree, and its keeper view is correctly joined. Nevertheless `actor_current_equal=false` and `shared_time_domain=false`; the observer's supported guard is false while both keeper guards are true. Every observed monotonic/boottime FOR_CHILDREN offset is zero. Zero offsets and nearby numerical timestamps do not replace the retained namespace-identity guard or justify cross-domain subtraction. The actual result is **COMPLETE_DIAGNOSTIC with an unsupported shared-time policy**, not evidence that every physical clock value differs or that the old64c0 cause has been proved. Namespace identities are qualified only within this retained run/lifetime, not as durable IDs across runs.

Before removal, both container states were `exited`, ExitCode0 and OOMKilledfalse. The full19 Docker command records exited0; ownership was verified before removing only the two own containers. A fresh own-label query and both exact-ID queries (`f1f454`) were empty/0; the wrapper returned and lock was absent. This qualifies owned-process/container/API cleanup under the reviewed model, not physical release of every Docker backend allocation. Logical Host runtime output (role/command RAW, its two receipts, claim and console) was71,647 bytes. No corsad, Capture, Go ELF or piratecashd ran; no memory-fitness, K5/S6, G2 or release claim follows. The original first INCOMPLETE remains retained. Both one-attempt grants are consumed; no automatic repeat is permitted.

Local evidence: `docs/_internal/d1-artifacts/2026-10-28/clock-probe-runtime-thread-v3/`, run `clockprobe-22f567eed40b5a212c2bc14fd38e6b1a`. ROOT-ACTUAL.json SHA256 `230bf04cf1e060b9548976d8c6c5c4697de62b1f9f7f205bf9a92fae2135c786` binds the full manifest. TESTER8c5eaa27 and REVIEWER7b99719d independently accepted file contents, matrix joins and scoped closure; actual tool terminals/fresh Docker queries were observed by Root. Private availability on other hosts is not assumed.

### Next engineering direction — proposal, no new run approval

For the next useful512000000-byte screen, place the sampler/clock reader in the service container, preferably using the keeper's owned-child lifecycle. Include **corsad+keeper+sampler together** in the unchanged service memory limit and its declared total CPU/PID limits, with no observer128MiB added and no subtraction. Keep external workload drivers separate and comparable instrumentation identical. The sampler competes with the daemon for CPU and may reduce completed work; a small peak is not PASS without the exact declared workload/state-completion counts and gap-free coverage. Such a group can conservatively bound the daemon's observed charge in that declared scenario; it does not isolate sampler charge, establish an uninstrumented peak or transfer older figures to a changed configuration. Prove actual process/cgroup membership and CURRENT/FOR_CHILDREN identities before useful sampling, retain peak/OOM/closure evidence, and keep unqualified quantities UNKNOWN.

This changes the earlier separately budgeted observer layout and needs a concrete new resource-profile decision before execution. It is an engineering proposal, not implemented or admitted here. Include namespace/cgroup preflight in useful resource work rather than building another empty-clock campaign. The reviewed [Docker run options](https://docs.docker.com/reference/cli/docker/container/run/) and [CLI29.3 namespace flags](https://github.com/docker/cli/blob/v29.3.0/cli/command/container/opts.go) provide no time-namespace-container join flag; sharing PID/network alone did not supply one in this actual attempt. Do not invent a `--time` flag, introduce privileged/custom-runtime namespace switching, or weaken the guard to force a green result.

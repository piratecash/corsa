# C1/C2a/C2b32/SG32: production daemon memory screens

[Русская версия](d1-corsad-memory-screen.ru.md)

Latest bounded result: [SG32-v6: daemon, keeper and sampler charged together](#sg32-v6-service-group-result--2026-10-28): **41,328,640 bytes** for the complete fixed32-session case. Full D1/512MB suitability remains UNKNOWN; earlier sections retain their exact subjects and history.

2026-10-25. **This limited screen completed without an observed OOM or budget overrun. Full D1 compliance with512MB remains UNKNOWN.** These are our own measurements, not external sizing figures. The resource checkpoints and remaining cases are in [D1 to DHT](../design/d1-dht-resource-checkpoints.md).

## Exact subject and execution

The real headless `./cmd/corsa-node` was built with Go1.27.3/CGO1 for Linux arm64/musl from `6421c8110a4c668fcf688bec6eb4c957ae37b852`; ELF SHA256 `9ba93241a3f717fb50a6221957e4e3146f982ef5fc7b68a37f90bfe7a49e55bf`,21627143bytes. Source domain: `1f3a92ee80123f59502b4fe39cc5119c367e4b37a247ec047550d4441b811519`. The later staged inactive ready-cache correction is not part of this binary.

Execution was native arm64 in Docker Desktop's Linux VM on the macOS host, not QEMU TCG. The exact local image is `sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e`. This is not a bare-metal Linux, native macOS or amd64 measurement.

The daemon ran in full/listener mode with fresh private HOME/temp/data directories, listener127.0.0.1:64646, authenticated RPC127.0.0.1:46464 and explicit bootstrap input127.0.0.1:64647 to avoid production-seed fallback. Networking was offline except loopback; there were no external clients or generated application-message workload. GOMAXPROCS1 was set; no GOMEMLIMIT/GOGC override was used. SQLite workload, Desktop, SDK-client cost, persistent-state growth and production peer populations were not exercised.

The owner-admitted envelope was1CPU/512000000bytes/no swap/64PIDs for the service group,0.25CPU/134217728bytes/no swap/32PIDs for the outside collector;100seconds of work including preparation and30seconds cleanup, under the existing heavy-run lock and fresh10GiB-plus-inputs admission. A single own1MiB tmpfs and unprivileged preparer used the collector envelope before the collector. Brief preparer/service overlap retained the tmpfs; preparer exit0/removal preceded collector creation. Maximum simultaneous containers was two. UID501:20,0600credentials, read-only consumer mounts, cap-dropALL, no-new-privileges and no external network were checked. `piratecashd` was not launched, inspected or sized.

## Measurements and coverage

| Metric | Bytes | Scope |
|---|---:|---|
| Service cgroup kernel peak |35913728|Fresh group containing the daemon, Python keeper and service descendants; final read after daemon drain, before keeper release. Nothing subtracted. |
| Maximum captured daemon VmHWM |19533824|Kernel per-process HWM observations; complete lifetime VmHWM coverage remains UNKNOWN. |
| wait4 command HWM |20766720|Waited command, potentially including descendants/pre-exec; not a replacement for complete daemon-only HWM. |
| Collector cgroup peak |35758080|Separate group, through its last peak read before final record; complete post-exit collector peak UNKNOWN. |
| Preparer peak / shared-tmpfs charge location |UNKNOWN|Not subtracted from the service peak or claimed included in it.1MiB is a storage bound, not measured RAM. |

The service peak is35.913728 decimalMB (34.25MiB), below the declared512000000-byte ceiling in this case. Cgroup charge, RSS/HWM and optional sampled PSS have different scopes; these values are not summed into a physical total. The collector's cost is not subtracted to manufacture a smaller daemon result.

The retained stream has286 records:61 regular samples,122 process records,93 authenticated RPC results (each of the three methods once at startup and30times during useful RPC work), one rejected unauthenticated request, listener milestone, stop/drain, initial/final network and final-scope records. The milestone ties the daemon PID/startticks to its existing listener's socket inode/FD; it is not full-service health. The collector's first-to-final cost-read interval was60.118204152seconds; that is not the entire daemon lifetime.

The service's63 event observations (initial plus62deltas) retained zero OOM/oom_kill and zero budget-limit events. Final cgroup identity and held FDs remained stable through daemon exit and the final read; host receipt flush preceded keeper release. All three containers exited0 and were removed, then the own volume was removed. Independent exact-owner container/volume queries were empty. No sample gaps or lost final scope were accepted as success.

## Retained proof and limits

Attempt nonce `4138adbe26d1aa3b5a17153df155cedd`; host receipt SHA256 `a7a5a242d112c877e1b584cc296f7b8afcf7ab6f4eb203636dfede94c6e57cec`; complete NDJSON SHA256 `5dfa6e86384932a9a2961cb2c24d0a80c8c77205401259d3bed93add7980d85a`. Local proof: `/private/tmp/corsa-D1-C1-native-secret-ROOT-package-v1/ATTEMPT-9-TERMINAL.json` (SHA256 `e9885261525738740682d15fb4d9f1b025a83fdbedc9a16f3b041076a13213d3`). Sourcev13/CODE `b782bb27…`/`4d4e87a5…`, pure controls12methods/118subtest events. Owner receipt `4ed5b722…` records the approved native-tmpfs profile and necessary reviewed repeats. These workspace-local files are not assumed available elsewhere.

Earlier attempts stopped before a daemon measurement. They uncovered host-bind ownership mapping and additional Linux interfaces, not high daemon memory usage. The final C1 predicate accepts only the nine enumerated name/type pairs when DOWN, addressless and unrouted, retains all loopback/socket checks, and reports original_T5_guard_qualified=false. This does not close the original T5 lo-only kernel checks. Static Linux type/driver references explain identifiers, not RAM use or installed-kernel attestation. [UAPI types](https://github.com/torvalds/linux/blob/master/include/uapi/linux/if_arp.h), [GRE drivers](https://github.com/torvalds/linux/blob/master/net/ipv4/ip_gre.c).

This result qualifies only the declared startup/idle/local-RPC screen. It proves neither useful FindNode admission under load nor512/512/1024 coverage, maximum burst, large state, churn, delivery, long-term retention, co-tenant safety or the final DHT footprint. Next is a concrete separately admitted C2 case with real work and separate drivers. C3–C5, final-candidate repetition, G2, activation and release remain open; Windows/Tor remain unverified/deferred.

## Later current-candidate compilation — 2026-10-25

A separate production CLI was subsequently compiled from the current indexed code: base `6421c811` plus eight modified and two added selected Go files (the ready-cache correction and connection-total observation source/tests). Its1836-file/54894920-byte input has domain `936d030d38ad88e799de8351a895b907855f591a3cbc494f3b436c0f429f6ba3`, bound to input index seal `971349dc01af28aa1a9c21c29bff3277ea70c859315f28dd702ae605024da4ba`. It is not the unmodified committed HEAD tree. No additional Go/embed/module/build-input drift was found.

The Linux arm64/Go1.27.3/CGO1 production `./cmd/corsa-node` build completed23 commands normally, with static ELF/buildinfo/musl closure recorded. The resulting21627143-byte ELF has SHA256 `e651bb6c71419af826d0d268b61ae2b6552c267e4f5cef3172bd2f49387c55c6`. Retained build result: `/private/tmp/corsa-D1-current-corsad-compile-runtime-ROOT-v1/result.json`, SHA256 `1075e26013d463d2e76f06338fe10bb65454db13ba405eb2d49093a648827cf6`. This was a compile/static check; the application was not executed.

The earlier35913728-byte service peak belongs only to `9ba93241…`. Equal ELF byte size does not transfer that memory result to `e651bb6c…`. The pending C2 package retains its separately identified `9ba93241…` binary; it has not been silently replaced. The current candidate still needs its own admitted runtime/resource checks. Observation installation, full512MB/D1, G2, activation and release remain open.


## C2a: eight TLSv2 peers and four-identity churn — 2026-10-25

Attempt 3 completed the admitted offline Linux arm64 C2a scenario on the same production CLI as C1: `9ba93241…`, base `6421c811`, source domain `1f3a92ee…`. Driver `63782d07…` used twelve distinct peer identities across four phases: eight proven TLSv2 channels, four original survivors after closing peers 1–4, eight channels after adding four new identities, then zero drained channels. The exact phase ordinals were 1–8 → 5–8 → 5–12 → none. Each nonempty phase included successful authenticated `get_peers`; cached session diagnostics were not used as the acceptance oracle. This stock binary does not advertise F1.

| Observed scope | Kernel cgroup peak, bytes | Qualification |
|---|---:|---|
| Service group | 35,516,416 | Includes the Python keeper and descendants; no keeper, baseline or shared-cost subtraction. |
| Driver group | 21,585,920 | Retained final read after the Go child drained; includes its keeper. No claim about later keeper-exit costs. |
| Observer group | 41,746,432 | Through its last peak read before the final record; complete post-exit lifetime peak remains UNKNOWN. |
| Preparer and shared tmpfs charge | UNKNOWN | Separate accounting; configured tmpfs ceiling 1 MiB is not a measured charge. Nothing is deducted from the service peak. |

Daemon sampled VmHWM was 20,586,496 bytes; the waited-command rusage HWM was 23,830,528 bytes, a different scope. Complete daemon lifetime VmHWM and continuous process-tree RSS remain UNKNOWN. These role peaks are not an instantaneous sum of physical memory, and the single C1/C2a runs do not establish a footprint improvement.

The daemon limit was 512,000,000 bytes/1 CPU/64 PIDs; observer 128 MiB/0.25 CPU/32 PIDs; driver 256 MiB/1 CPU/32 PIDs, all without swap. Preparer used the observer budget and was removed before the observer started; at most three roles ran together. The work allowance was 120 seconds including preparation, plus 30 seconds cleanup. Retained scope checks reported no gaps or observed OOM/budget failure. The complete 105-record NDJSON contains 22 service samples, 22 service-process observations, 20 driver samples, 20 driver-process observations and all four protocol milestones. The driver charged 336 application frames/10,304 bytes; these are not TLS wire-byte totals. All four owned containers exited normally with code 0, OOMKilled=false, and were removed; the secret volume was removed. Fresh nonce-filtered queries found no remaining owned containers/volumes; the heavy-run lock was released.

Attempts 1 and 2 stopped before container creation because the launch omitted the required heavy-lock environment and then used the wrong fixed wrapper owner name. They remain LOSTRESULT/UNKNOWN, not measured failures or successful zero-memory cases. A separate read-only context check passed before attempt 3; the runtime code, resource limits and binaries were unchanged.

Retained proof: `/private/tmp/corsa-D1-C2a-eight-peer-ROOT-package-v1/ATTEMPT-3-TERMINAL.json`, SHA-256 `63a977a7fa18acf8a1ad89fe9d12be5d718a540a47e9c7d096ecc5882c85db25`; run `runs/corsa-c2a-90e20c6d45317c03a57ae579932d028a/host.json`, SHA-256 `07b19dfbfe21daf0f3d8624a423a1cd1703b4951d300b4bc99e1e3594596ae58`, under that package. Original samples and both preflight failures are retained locally; availability outside this workspace is not assumed.

This is only the fixed binary and bounded C2a TLS/get_peers/churn scenario. It does not establish overload admission, FindNode/F1, full 512-peer coverage, long-term retention, current post-K12 candidate memory, SQLite/DesktopClient cost, Tor/Windows, G2 or release readiness. The later `e651bb6c…` static CLI build and the staged K12 integration are different scopes. Full C2 and the remaining D1 resource checkpoints remain open.


## C2b32: current candidate, 32 TLSv2 peers and 16-identity churn — 2026-10-26

The first C2b32 attempt completed its separately owner-admitted case: **32 → 16 → 32 → 0**, with 48 distinct identities. Proven channels used ordinals1–32, original survivors17–32, recovered channels17–48, then none. Each nonempty phase verified genuine TLSv2/session-v2 identity/channel ownership and successful `get_peers`; sixteen new identities were disjoint from the original32. Four5s waits also included serialized exchanges. The driver charged **1344 application frames / 41216 bytes**, within the unchanged2048-frame/8MiB limits; this is not TLS-wire accounting. Cached live census was diagnostic only.

The final0 is a **source-bound fresh authenticated Node RPC witness**, not an inference from driver exit or an empty peer list. After closing/joining all owned channels, the pinned compiled driver required fresh `getNodeStatus`/`fetchRouteSummary` to return the same server incarnation and enforced instantaneous loopback `active==0`, before emitting `zero-drained`. The final RPC response body/atomic field/read timestamp was not retained as a separate RAW response; independent review joined the executed source and artifact to the ordered protocol record. Subsequent child drain/reaping and daemon drain are separate lifecycle facts.

| Observed scope | Bytes | Qualification |
|---|---:|---|
| Service cgroup peak |36,233,216|Includes Python keeper and service descendants; no subtraction. |
| Maximum sampled daemon VmHWM |25,358,336|Observed process HWM; complete lifetime VmHWM remains UNKNOWN. |
| wait4 command HWM |26,140,672|May include descendants/pre-exec; separate from daemon-only HWM. |
| Driver cgroup peak |24,952,832|Keeper+child group through final read after Go drain; later keeper-exit costs unmeasured. |
| Observer cgroup peak |41,172,992|Through its last peak read before final record; full post-exit lifetime UNKNOWN. |
| Preparer/shared-tmpfs charge |UNKNOWN|Not subtracted;1MiB storage ceiling is not measured RAM. |

The service peak was36.233216 decimalMB, below512000000bytes **for this case**. These separate scope peaks are not a simultaneous physical-memory sum. Continuous process-tree RSS and full daemon lifetime HWM remain UNKNOWN. No per-additional-peer RAM delta or saving is inferred from the older8-peer/`9ba93241…` runs: code, CGO build variant and workload changed. K9/K10/K12 opt-ins were OFF, so this is not their active collection cost.

Production CLI SHA256 `ae61fc926e7f5c2353a21075041300fb722f2c8346389344a6e2e145931632c7`,21627040bytes, was the ordinary `./cmd/corsa-node`, Go1.27.3/Linuxarm64v8.0/CGO0 with no tags or benchmark wrapper. Its source is base `e50f97511d88d7ffd7a08daa3490bee7d63f0f6a` **plus19 staged paths**, snapshot `1fcca8012a39015fcc377472d2e56cdb82cbedef0562f07ee6acf8f6895f9f29` (2119files/70186204bytes), not pure committed HEAD. Driver SHA256 `90d1c2d68524a5507aa09d4e443cb70cf3c52fb6fe936a1d9280917bad7f7aa7`,6750368bytes, used that current source plus four private component posts and the actual CLI literal; manifest `2806e1feef432c56b4db0824ea5cfe6b9e6694178c4df7d37a890b24599cd734` (2119files/70197026bytes). Both ELFs have static self-only closure/no interpreter or needed DSO. They were cross-built on macOS, then executed natively in the same arm64 Linux Docker image910cfcaf…; these are different facts. The original CLI metadata-only qualifier FAIL was retained; a separate static readback qualified the existing ELF without rebuilding it.

Limits remained service1CPU/512000000bytes/64PIDs, observer0.25CPU/128MiB/32PIDs, driver1CPU/256MiB/32PIDs, all no swap;120s work including preparation plus30s cleanup, max3 measured roles. The owned1MiB native credential tmpfs,501:20/0600/read-only consumers, prep-before-observer/driver handoff, offline loopback namespace and held cgroup/FD checks were retained. The complete105-record stream has22 service samples,22 service process records,20 driver samples,20 driver process records and four protocol milestones. Initial absolute events and subsequent deltas showed no OOM/overrun; no gaps were accepted. Final held reads and host flush preceded keeper release; all four owned containers exited0/noOOM and were removed, then the own volume was removed. Fresh exact-owner queries were empty and the heavy lock was released.

Local proof: `/private/tmp/corsa-D1-C2b32-current-ROOT-package-v1/runs/corsa-c2b32-f81167849ca5419213464d803dc85831/host.json`, SHA256 `e2ce81ab4e0dfa096673bb1ac1480f139f7997d0248b2c82f89eb9afe95a3b68`; complete NDJSON SHA256 `68d63989d4bc432c1560801d55c250317db30a6e4cc85835d4c98a2b614e618c`. Independent TESTER and reviewer accepted this bounded scope, including the source-bound zero-witness limitation. Files are workspace-local; availability elsewhere is not assumed.

This closes only this32-channel TLS/get_peers/churn screen. Full C2/512-peer workload, F1/FindNode, overload, persistent/long-duration state, co-tenants, active observations, T5/Member4, G2 and D1 release remain open. It does not establish the final DHT footprint or universal512MB suitability; Windows/Tor remain unverified/deferred. `piratecashd` was not run or sized.


## S6 EMPTY64 instrumentation preparation — 2026-10-27

The private instrumented CLI was built for Linux arm64/CGO0: SHA-256 `9bea7a6ed0c50bc293f105bfbf495295770e2daf8b59fd0d187d0a0873b62d3c`, 31,355,161 bytes. Its explicit observation profile leaves project defaults unchanged. Static build evidence and the six-group integration result below do not transfer earlier memory measurements to this binary.

The complete OFF→ON controller passed six pure integration test groups in one closed30 Python command (normal exit 0, child reaped). Tests exercised the actual scheduling/metric helpers with a simulated clock and RPCs, temporary counter/store files, synthetic mount/process metadata, and keeper→receiver RAW/EOF handling. They covered the full 50-second schedule and 51 calls per method, omitted metrics/boundaries, daemon membership, late returns, adverse outcomes and cleanup failures suppressing ON. This was not 50 seconds of real daemon work or a kernel-counter/permission test.

Review corrected Linux device-number decoding on the macOS host, final result normalization after failed volume cleanup, and the attempt-budget origin. The proposed physical case uses the same instrumented binary in both modes, fresh disposable stores, serial 64 captures only in ON and matching RPC instrumentation. The complete source remains inert/private; runtime binding, resource admission and the physical OFF/ON measurement are still pending. No new memory, CPU or disk-cost result is claimed. Full D1/512 MB, K5 qualification, G2 and release remain open.

Workspace-local evidence: Source `7d1018c4…`; CODE `0e31c369…`; `/private/tmp/corsa-D1-C2-S6-empty64-fullcase-QA-ROOT-v1/result.json`, SHA-256 `e85a373010de57691209844da9b3a5be057b503eaa22fb08caf00241e5e6b9b3`. No Docker container, daemon or VM was executed in this pure check.


## S6 live EMPTY64: the admitted attempt stopped in OFF — 2026-10-27

The single newly authorized OFF→ON pair ended with outer exit 2 (`83f7fd` / session `27510` → `5a6a04`). OFF launched the daemon, but the observer refused with `time-clock-reader-domain`; no complete authenticated-RPC 50-second window or resource measurement was obtained. Host status is `LOSTRESULT`, measurement `partial`, adverse outcome `FAIL`, and runtime custody `INCOMPLETE/UNKNOWN`. The reported refusal is an observed outcome, not a qualified root cause. ON was skipped. In OFF the observation selector was absent: 0 Capture started/completed/interrupted and 0 marker bytes. ON has no Capture observations; this is not a successful zero-cost comparison.

The input was the private instrumented Linux arm64/CGO0 `./cmd/corsa-node`, **31,430,248 bytes**, SHA-256 `64c0b82bd51842326cb279c2a7d400a8097284e4f952ec5557f6c8eff031df90`, source domain `98ce7427d3c15ddba5e19065cb0876af0b8cc0906e314dfa32cbdd14fc0468ed` (base `e50f97511d88d7ffd7a08daa3490bee7d63f0f6a` with private instrumentation). It is distinct from the earlier `9bea7a6e…` preparation and subsequent Main changes. Static build/source correspondence does not qualify this failed runtime measurement. Service peak, observer cost and the requested RAM/CPU/disk-cost comparison remain **UNKNOWN**, including suitability for the 512,000,000-byte target.

Cleanup removed the three owned containers and both owned volumes and reaped local children; fresh exact-owner container/volume queries were empty and the heavy lock was absent. Host retained `known_service_forced_kill=true`, so cleanup is not proof of normal CLI exit or successful drain. The disposable store was deleted, not archived: its contents are unavailable for later K5 readback, and no archive/retention result is claimed. The one-pair authorization was consumed; no repeat is authorized. Full S6/K5, D1/512 MB, G2 and release remain open. `piratecashd` was not run or sized.

Workspace-local evidence: `/private/tmp/corsa-D1-K5-live-empty64-owner-package-ROOT-v1/ATTEMPT-1-ROOT-TERMINAL.json`, SHA-256 `59a379a13683b809c69a99a7fac3ad4d558df4170d73b469f86f6c4accb8ffdf`; `ATTEMPT-1-OUTPUT-MANIFEST.json` in the same package, SHA-256 `9bd1818ef942bac8bc5d4adf035a12d6fecb1a5793ffb1907fa0523eb1ca447f`. The manifest binds retained Host, keeper/observer RAW, empty marker RAW, samples and exact staged input copies. These are retained failure evidence, not resource PASS.


## Historical evidence availability — 2026-10-28

The private `/private/tmp/corsa-D1-*` paths cited above are now unavailable, including the old `64c0` runtime package and RAW. The committed outcomes/hashes remain historical records; live access to those files is not claimed. Static recovery preserved the `CurrentBootContext` fragment and relevant caller edit strings, with zero of six complete runtime files qualified to their final hashes; the old ELF was not restored. At that recovery checkpoint the new [clock-domain diagnostic](d1-clock-domain-diagnostic.md) had CODE/local pure QA but awaited owner admission and Docker execution. The later current diagnostic completed and found distinct time namespaces across separate containers; SG32 instead uses colocated parent/child clock proofs. It does not establish the old refusal's cause or memory fitness. Existing reports above are preserved unchanged.

## SG32-v6 service-group result — 2026-10-28

**SCOPED_SUCCESS:** the real daemon, keeper and sampler together had a kernel-charged cgroup peak of **41,328,640 bytes =41.32864MB =39.4140625MiB**, under the declared512000000-byte/1CPU/64task/no-swap service limit, with no subtraction. This is our actual Linuxarm64 Docker measurement. The fixed workload completed32→16→32→0 TLSv2 sessions,48 distinct identities and224 source-bound completed ping/get_peers/ping sequences (672 primary request/reply pairs,1344 main application frames/41216bytes, not TLS wire volume). No active FindNode/F1, full512-peer population, overload, long state history, enabledS6-observer cost, Desktop/SQLite or native macOS measurement is established.

| Observation | Value / scope |
|---|---|
| Final service memory.current |13,963,264bytes, read after daemon and sampler reap; not a daemon-only RSS figure |
| Kernel peak window |Fresh service cgroup creation through bounded final keeper peak-read, including its handoff waiting prefix and helper costs |
| Peak read bracket |53711252221516..53711252227683ns in the qualified guest BOOTTIME domain; not host/UTC |
| Sampling |41 periodic SAMPLE records plus SAMPLER_READY/SAMPLER_FINAL =43;45 total cgroup facts with keeper initial/final |
| Coverage |Maximum pre-reap sample-end gap511126917ns; last owned-reap edge57810584ns, both below2s |
| Final events and members |OOM/oom_kill/oom_group_kill0 throughout retained facts; keeper-only member[1] after node/sampler normal0/reap |
| Logical state |Four phase walks7113/7268/9904/9904bytes; maxima12entries, not physical disk I/O or growth acceptance |

The owner approved exactly one live handoff: PREPARED remained nonterminal; keeper read the501:20/0600 secret through its RO mount and emitted SECRET_ATTACHED, then preparer exited0/reaped/was removed with fresh absence. Only afterwards did the exact Host send CONTINUE; the exact keeper repeats its original secret checks/equality before sampler/node, and driver starts later. Retained RAW/command ordering plus exact executed Source attest this control flow. Secret values/hashes and separate inner-read/mount-inode traces were not exported. This validates the declared handoff in this run, not the sole physical cause of v5 retrospectively.

All59 normal command records exited0, separately from three interactive CLI exits0 (preparer20/service24/driver33). Node and sampler also exited0/reaped, driver/container completion was normal, and final service seal retained stable cgroup/process births/executable/clock proofs and observed loopback-only network snapshots. Only the three exact owned containers and three volumes were removed; final eight own-label/ID/name absence queries were empty/exit0 and the canonical lock was absent. This is scoped cleanup, not proof of physical release of all backend allocations. All2195 tracked bodies/full metadata and indexbd6e were preserved during execution.

Artifact lineage: CLI `9c5764b19c5be6b635d5a1dde774c0fc4f5bcc64ac960c8d31c7f6121b4eebc4`, driver `c836814111c5fc6fad55f0e4c7888060d252919a9278d57efb270594b42fafea`; Go1.27.3/Linuxarm64v8/CGO0 frozen Go source8afa, no rebuild. The service helper/Host/Common hashes and exact build-source manifests are in the [profile/provenance](d1-service-group-memory-profile-proposal.md). Root838a8f/session6395→8c64e5/exit0, noncebd3ecb3f03e6cea4b7e3497b27df15e3; actual receipt SHA427feb830c9e8bce14091213765363b392f3b3b416ae343cd9019e149e44c439, result SHA`a52d2ec96705a058e2c671c945e91092ce19475f93becaad59fa17bc43eb2d66`. Testerabd1d980/reviewer9f72b776 independently accepted whole DATA. RAW remains under ignored durable `docs/_internal/d1-artifacts/2026-10-28/sg32-runtime-package-v6/`; earlier failures remain separate.

Bounds remain explicit:224 sequences and the fresh authenticated final0 witness are tied to exact stock-driver control flow; there is no full terminal per-peer RAW or separately retained final atomic-RPC response body, and cached_live is not the oracle. Peak/current are non-atomic ordered reads; a later current may exceed an earlier peak without contradiction. Coverage uses read_bracket_ns and owned REAP; SAMPLER_FINAL.requested_at_ns is a scheduled due value, not an actual request instant. Keeper-only exit-tail, physical shared-page attribution, isolated daemon/role charges, optional RSS/HWM/PSS and preparer/driver peaks remain UNKNOWN. Separate role limits are not measured peaks. Do not derive per-peer cost or a marginal delta from earlier binaries/collector arrangements. The one grant is consumed; no repeat, fullD1/512MB acceptance, G2, activation, commit or release is approved by this result.

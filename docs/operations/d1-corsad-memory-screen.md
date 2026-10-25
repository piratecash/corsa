# C1/C2a: production daemon memory screens

[Русская версия](d1-corsad-memory-screen.ru.md)

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

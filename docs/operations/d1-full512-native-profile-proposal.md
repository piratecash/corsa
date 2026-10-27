# Full512: proposed NativeVZ diagnostic profile

Status: **owner-approved first attempt consumed: genuine Go/fixture FAIL**. Direct answer `6115ae3f21964964b596c13c5a875c54` authorized one execution of this exact profile and conditional ACK. That attempt ran on 2026-10-26 and failed before the 512 population; it does not authorize an identical retry. The earlier Member4 grant remains separate.

## Purpose and scope

One offline execution of the Linux arm64/CGO0 `TestOwnedP0FullPopulationAndRegistryOverflow` ELF: useful forwarding with 512 F1 and 512 legacy members → refusal at 1025 registry visits → removal of the extra legacy member, a fresh complete 1024 cut and useful forwarding. Check retained obligations and that a rejected request does not reappear after recovery.

This fixture places the Service and 1025 clients in one guest. The whole-guest budget **is not standalone corsad memory usage** and does not establish D1 suitability within 512,000,000 bytes. Physical timing bounds, G2, activation and release remain separate.

## Proposed limits

| Parameter | Value and scope |
| --- | --- |
| Guest | 2 vCPU, 1,610,612,736 bytes RAM (1536 MiB) |
| Isolation | No NIC, disks, host sharing, virtio sockets or balloon; no external network |
| Guest swap | Header-only `/proc/swaps`; no backing disks or swapon |
| Guest PID | No hard cap; UID 0 makes RLIMIT_NPROC unsuitable as that guarantee |
| Go | GOMAXPROCS=1; test timeout 180 s |
| FD | soft=hard=4096; 2050 endpoint FDs + 2046 planned overhead, not measured sufficiency |
| Guest supervisor | Its own monotonic origin; software deadlines: work300 / KILL310 / reap-refusal-UNKNOWN320 |
| Host worker | Shared origin at launch.py entry; software work360 / cleanup end380 |
| Outer | Software fence: KILL398 / reap wait400 from the same origin |
| Application quota | 131,072 frames, 134,217,728 payload bytes; not TLS wire traffic |
| RAW | Console 4 MiB; native events 16 KiB with received/retained/EOF/hash; stderr prefix 4096 B; outer combined prefixes 65,536 B |
| Disk | Fresh 10 GiB plus preparation and actually allocated input copies before every attempt |

**Aggregate hard host RAM/CPU/PID caps and guaranteed host no-swap are not provided.** Guest vCPU/RAM do not provide those limits. The planned deadlines and outer fence do not guarantee physical VM/XPC termination or complete resource release after a crash. Planned timer actions also depend on scheduling/syscall completion; late completion cannot qualify as PASS. Manual recovery may be needed when the outcome is unknown.

Per-operation BusyBox setup timeouts were removed because their watchdog daemonizes. Address setup is covered by the aggregate guest supervisor; the former separate setup2s promise is withdrawn. Existing internal Go guards remain. Insufficient budget means refusal/timeout, with no automatic limit increase. Do not run `piratecashd`.

## Pinned artifacts

| Artifact | SHA-256 |
| --- | --- |
| node.test, 65,193,127 B | `3196e734087c80ee0701f190f573fa6ea7657b1e3276386bb92cb4e83ab2a48b` |
| Kernel Image, 36,306,944 B | `256d5fff4bb892edcb9218c1e6353e388bc3646e65f76c2bc9d958b635d1b00e` |
| initrd, 100,940,160 B | `2f44233a93bccb85bec0f89ee36bf6845213b203f2f4189e3c576ec9b597f293` |
| Signed launcher, 220,720 B | `796596c2cb841a0108821f17bd5c271de410ffba7d5b92d98313af81014bf983` |

Go Source domain: `827a3f550d9b5209f6ce2228dcb57fead80abe4e7fc9f5064260c266646e3ad8`. The initrd retains historical auxiliary artifacts/source domain `1f3a92ee…`; it is not a uniform current application build. There are 2896 entries: exactly three replacements and five additions, with the other 2888 entries preserved byte for byte.

Launcher: ad-hoc identifier `org.corsa.d1.p0-full512.native-vz`, CDHash `b9224c7850c6ebb6ecb6e29206da236e5ccb35c6`, exactly `com.apple.security.virtualization=true`. Before the first attempt, effective entitlement and support for the new VZ configuration were unproven; the observed execution and normal stop below are limited to that attempt. File length is not RAM usage.

Tester and reviewer accepted the preparation scopes: static C compilation, eight fake-syscall controls, seven Python oracle/RAW controls, six archive test groups, a separate retained scan, whole new-initrd readback and six compile/sign/static commands. These are distinct scopes and do not replace a full512 runtime PASS.

## Stop, lock and retry

Error, OOM, timeout or untrustworthy data stops the current attempt; retain its results. The canonical heavy lock survives **every outcome**. There is no automatic ACK or lock removal. The owner conditionally authorized ACK only after independent ROOT + TESTER + REVIEWER checks on either basis:

1. Confirmed stopped callback/terminal state, native normal exit0/reaped and owned process closure.
2. A narrowly proven pre-creation input/unsupported/validation refusal: complete RAW, created=false throughout, no start request, native exit2/reaped and owned process closure.

Silence, exit2 alone, wrapper-context failure or incomplete RAW do not qualify. Remove only the exact unchanged owned lock. ACK does not prove physical release of all XPC resources; unknown state retains the lock until operator recovery.

Proposed retries require a localized, independently reviewed harness repair, the same profile and authorized ACK. A genuine Go/fixture FAIL does not authorize an identical retry, backend/oracle changes or increased limits. Preflight may refuse a future separately admitted attempt; the separate Member4 guard3 remains unresolved. New diagnostics retain the first rejected route row while preserving the predicate.

## Actual first attempt: 2026-10-26

One RUN, one FAIL, zero PASS: `find_node_p0_full_coverage_linux_test.go:390`, “genuine original F1/auth/publication binding absent”. The helper combines `valid`, `proven` and `caps == 1`; RAW does not identify which individual conjuncts failed. The first client reached v2 establishment and ping/pong. None of POSITIVE, OVERFLOW or RECOVERY was emitted, so the 512 population, useful-forward/1025-refusal/recovery sequence was not proved.

Runtime used the frozen Source domain `827a3f55…`, not current MAIN. Host elapsed was about 4.47 s; console RAW is 28,961 B, SHA-256 `03bbb991092bf6c5e14849ce200a8317417af5466b85e54c4c2f4d4accc82553`. Native event RAW is complete: 342 B, EOF, no truncation, exact semantic replay; normal guest stop and native exit0 were observed. Outer exit was2 and the console oracle refused. Native stop/exit0 does not turn the Go FAIL into PASS or prove physical XPC release. This attempt accepts no 512 MB/RAM, G2, T5, activation or release criterion.

A read-only source finding, pending independent review: the fixture expects typed NetCore F1 capability count1, while the test-only flag adds F1 to wire advertisement and the ordinary typed capability intersection excludes it. This is a source-derived mismatch candidate; RAW does not separately record `valid/proven/caps`. No fix, assertion bypass, oracle change or default activation is implied.

ACK status: **completed for the exact owned lock** after independent ROOT + TESTER + REVIEWER checks and review of the new P0-specific script. ROOT read-only check `a6127e` and ACK `3da7c9` completed with exit0; normal VM stopped/native0, owned PID/PGID completion and the unchanged inode/two owned files were revalidated before removal. Only those two files and their own lock directory were removed; the canonical lock is absent. ACK receipt SHA-256: `756258c33322a67cbd79be69b8f6d58b6e083ee98fb31ce7e349131eb81a78ce`. Physical XPC release remains UNKNOWN; no blanket cleanup is claimed. The genuine fixture FAIL is retained and does not authorize an identical retry.

## Prepared capability correction; native result unchanged

The source finding above has since received independent CODE review. The prepared correction moves the existing private F1 selection into one typed local capability function, used by wire rendering, original inbound negotiation and both outbound metadata callers. Wire rendering now only projects that typed set and has no separate F1 append. Ordinary defaults remain off; the original intersections, proof/auth requirements and genuine fixture assertion are preserved. Each invocation still recomputes datagram capabilities under the existing live-plane rules: this shared function is not a frozen handshake snapshot or an atomic cross-time capability cut.

The new four-case native regression exercises real capability/metadata functions and NetCore mirrors: default-off, selected peer-present, peer-absent and duplicates. It checks that metadata does not create auth or open the original capability gate. Its scope is metadata only, not a TLS/publication or full512 proof.

The guard correction separately accounts for eight omitted historical K10 observation-hook pin updates, then the two new capability-caller body deltas. All 27 Registry keys, publication checks and 17 original negative children (16 table cases plus OTHERFILE) remain. The unchanged Registry baseline was run once and failed on the stale pins; that result is retained. Source/guard CODE acceptance does not replace corrected functional QA.

**Actual focused QA on Darwin/arm64:** ordinary59 and race59 passed (118 RUN/PASS total, Node58+driver1 per mode), without FAIL/SKIP, a race report or stderr; all six commands completed normal0/reaped. Scoped Node/driver lint passed with zero issues on Darwin and Linux arm64 with CGO disabled. Private2142 source, MAIN2141, Git stage entries and the original held telemetry FD were preserved; existing owned lint caches were reused. The original Registry baseline1FAIL is retained separately and is not counted in118PASS. These checks did not build a corrected full512 ELF or execute a second Native run; no Native PASS is claimed. The first genuine FAIL and exact ACK above remain unchanged. No standalone512MB/RAM, G2, T5, activation or release criterion is accepted; this QA grants no Native retry.

## Corrected artifact preparation; no second Native result

After the capability correction landed, MAIN2142 plus two private fixture files formed 2144 files, Source domain `cb0851c9d3bb493d8996dc0f30dc0e52537edb8f4cef466ffabd0c13bd73d67f`. The genuine fixture predicate is unchanged; its failure message now reports the captured `valid/proven/caps` scalars. Source controls passed 21 ordinary + 21 race (42 PASS). Linux arm64/CGO0 compilation produced node.test, 66,009,232 B, SHA-256 `a3a284686d2b03da79a43f59195d9c39d782cc5b0e4c7f644700747b3f0e4d23`; the ELF was not executed.

Seven archive checks passed: six controls plus the retained read-only scan. The corrected initrd is 101,266,775 B, SHA-256 `c56a3edbea7f1a96bddeabd6d690a5cfcad00220bf887a175003fe8ad6f62137`; full readback verified 2896 entries and byte-for-byte carry of 2888 unchanged entries. Independent archive DATA was accepted (reviewer `5052cfb6…`, tester `6b0e0dcf…`). Historical auxiliary artifacts remain mixed with current Node source. Native Source review accepted the narrow input/path/hash/domain literal retarget with operational bodies, profile, lifecycle and oracle unchanged; prior controls remain historical evidence.

The six compile/sign/static commands completed normal0/reaped in 11.224 s, with source/tool/selected-SDK/MAIN custody preserved. Compiler stdout and stderr were empty; no compiler warning was recorded. The new ad-hoc launcher is 220,720 B, SHA-256 `171dddf835d88122154b938b14d38e6bcc5470b689878e0069d50044e98d2137`, CDHash `96f31da31b9bf6b3246117ce0ef2cbe1a0d494d8`; identifier `org.corsa.d1.p0-full512.native-vz`, exactly `com.apple.security.virtualization=true`. Independent build DATA was accepted (reviewer `3f77fd33…`, tester `39f6c044…`). This qualifies only the static artifact. Native/API/VM were not executed; effective entitlement and VZ support remain UNKNOWN. No corrected runtime grant, standalone 512 MB/RAM, G2 or release acceptance is established. The first FAIL, ACK and artifact table above are retained.

## Actual corrected attempt: controlled stop, incomplete Go verdict

The preceding preparation section records the state before this new attempt.

A new direct grant `14eefab2…` authorized exactly one distinct corrected attempt. ROOT `af97e9` / session 13451 → `2acc72` CLOSED2 ran on 2026-10-26 for about 5.79 s. Result: **STOP-NOT-PASS**, generic `console-error`. Retained console is 4,194,251 B against the unchanged 4 MiB cap (SHA-256 `5467f7b7fa9ef301a87f7068c319bc765dfd099e79636d67c7fb479279755286`). The exact rejected block and caught exception were not recorded; overflow on the next block is a source-consistent inference, not a retained exception witness. There is one Go RUN, no complete PASS/FAIL verdict and no POSITIVE/OVERFLOW/RECOVERY or final child/parent witnesses. Connection IDs and progress log counts do not certify a retained session population.

Complete native RAW 342 B/EOF/replay contains ready → start_ok → stop_complete, with the final stopped=true; native exit0 and input recheck were recorded. This was a controlled host stop: guest_normal_stop=false, not successful guest completion. Actual DATA was accepted by tester `4a17425d…` and reviewer `b60baf28…`. Physical XPC release remains UNKNOWN. No full512 success, standalone 512 MB/RAM, G2, T5, activation or release criterion is accepted.

The exact correction record `f46ddac4…` binds the four primary capability files (capabilities, datagram integration, peer sessions and Service) and their patch SHA-256 `0fb30dbcb9f4e108a43eb30b31d8c658f16d346778690236fb09efb3f07f3c89`; the reviewed 18-Go scope patch is `aaf09376360b9868d2dc00bc0d5aa6b96cf0403865c33a92669173481053b526`. The attempt binds the prepared cb085/a3a Node, c56a initrd and 171ddd signed launcher above; it does not claim the capability correction is the only difference between whole bundles. Profile, quota and oracle are unchanged. This one grant is consumed; further repeats require a separate decision. The first genuine FAIL and its ACK remain preserved.

The exact outcome-specific ACK completed after independent eligibility and Source CODE acceptance plus fresh live checks: ROOT `--check` 81f9e2 and `--ack` 092403 both CLOSED0. Only the two recorded owned files and their exact lock directory were removed; canonical lock absence was recorded at completion. ACK receipt SHA-256: `007908c8c427a00655db8dcaa770f45250a67a09923f43a06ccec888c0e72ef4`; independent readback accepted (tester `102da4a1…`, reviewer `a2ddd70c…`). The receipt remains SOURCE_BOUND_CONTROLLED_KNOWN_STOP / STOP_NOT_PASS, with physical XPC release UNKNOWN. ACK grants no repeat and proves no fixture success or blanket resource cleanup.

## Quiet fixture preparation; no further Native attempt

The private full fixture now scopes the actual zerolog global severity to Warn after its exact opt-in and before construction, restoring the saved level after later owned cleanup through LIFO. Writers, Warn/Error events, Go/phase witnesses, profile, quotas, timers and oracle are unchanged. This is a test-harness change; ordinary daemon logging defaults and admission remain unchanged.

The 2147-file private source has domain `99f9f1f0ebc928cd5ce433084abd338dfc498d5702b4a4651c789ba23ab739bf`. Its 26 ordinary +26 race events all passed: 52 freshly executed portable/source-control RUN/PASS, including five focused logging events per mode alongside the existing 21. Linux arm64/CGO0 compile produced node.test, 66,015,964 B, SHA-256 `bc077f581c4e56f8345d1d82054337f5c3bf9c01d0e450e8126f923990993dba`. The ELF/full fixture was not executed. This does not claim fresh execution of the historical 29 or Native Python7 controls.

The quiet initrd is 101,276,791 B, SHA-256 `2e5801794457ed9e6e050b79afb50858ac8c40c35ab5f1a330bb704973541b50`. Full gzip/newc readback accepted 2896 entries: the same three replacements, five additions and 2888 unchanged raw members, with kernel/generic C helpers and archive algorithm preserved (tester `511017e4…`, reviewer `89ef2c6d…`). The first readback attempt's FileNotFound at final-receipt write is retained. ROOT repaired only the output-path metadata binding and reran the unchanged reader against the unchanged archive; that first attempt is not relabeled PASS. Auxiliary artifacts remain historical/mixed.

ROOT static build `5cf9a4/session97910 ->39c02e CLOSED0` completed in 11.937647 s: all six commands returned normal0 and were reaped; compiler stdout/stderr were both empty and custody was preserved. The new launcher is 220,720 B, SHA-256 `5e0508c4445b412740b1f2c37c9ac0d8bbac75264d8443f015da8ca36cc7344d`, with ad-hoc signature, CDHash `5fba5f7dccb72fe74ee7e5262c07640b35d742ec` and the single static virtualization entitlement. ROOT result SHA-256: `26bffe3f60668b2060bff3ad9c96a252cb0fce0a030bb877f90998affc44e465`. Independent actual static-build DATA accepted (tester `93344451…`, reviewer `e27852c4…`); qualification covers these six static phases only. The launcher was not executed; effective entitlement and host VZ support remain UNKNOWN. No further Native/API/VM execution or owner grant exists; the prior 14ee grant is consumed. No full512 success, standalone 512 MB/RAM, G2, activation or release criterion is accepted. Earlier FAIL, STOP, exact ACKs and their artifact table remain intact.

[Русский](d1-full512-native-profile-proposal.ru.md)

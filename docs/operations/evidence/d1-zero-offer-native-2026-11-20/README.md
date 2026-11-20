# Corrected zero-offer short diagnostic — functional FAIL retained

Attempt 2c75180d2872646aa2f7201b240cb175 failed the original Go test after 30.62 seconds. The first console leaf is observer_full_send_finish_sender_false, observer 31 / transport 95. The corrected bound source reaches it only with sentAll=false and at least one supplied-frame observation (p.send != nil || p.next != 0). Supplied is not enqueued or written. The underlying producer return site remains UNKNOWN; the decoded .failed record contains generic failed lifecycle/observation metadata, not a more detailed refusal reason.

The ordinary zero-offer unit fix was valid and committed as 13e4d4f9195c333ee3307b1d55e387b33aa4c677. It removes only false poisoning when no frame was supplied; it does not resolve this actual supplied-frame refusal. The separate interop lint commit 2c97bf91f1bc1afca14b4a55a00dbd1639aec1b1 changes only tests. Earlier cecc/97a9 and 480-second causes remain UNKNOWN. No earlier outcome is relabelled.

| Actual stage | Result |
| --- | --- |
| Private source QA | 46 named outer tests / 311 child PASS; artifact snapshot 2450 files, QA snapshot 2457 |
| Four test/stand ELF builds | 17 commands, all exit 0 with confirmed closure; fresh original Go source API; static/BuildInfo checks |
| Source DATA | Exactly 1348 Go paths; one source body plus Core manifest changed; Stand manifest unchanged |
| PID1 / capexec builds | Ten host Go QA cases plus four commands; eight C commands with unchanged 147-instruction / 52-control policy |
| Initrd DATA | All 1439 members streamed; exact seven-member and five-boot-row delta from leaf97a9 |
| Native attempt | Original test exit 1 / exporter exit 0; both normal, all children reaped; worker 0 |
| Stop | Four native events, complete RAW, ROOT/TESTER/REVIEWER stopped-only ACK; exact owned lock removed |

SOURCE-CHECKPOINT.json separates this disposable artifact from ordinary Root. Its 2450-file source is the previous actual private leaf composition plus one corrected full_send post (638ba3f3…); the ordinary Root implementation is 1cb353cc…. Existing private full-receipt/census diagnostic posts remain unchanged. Seven QA-only files are excluded from the built ELF artifacts and 1348-source guest archive. Canonical groups are Core/measured640 ae4710dc… and Stand708 78d671b1…, verified by the actual original Go source API and VerifyTree. Linuxhelper 4a1bd9c2… and exporter 7ef3b4a1… are unchanged. No new source paths, wire schema, limits, windows, policy or capabilities were introduced. The workload configuration remains f848 and is diagnostic only; native profile d1-o1a-native-v1 is unchanged.

RAW.tar.xz preserves every new command stdout/stderr/step receipt, all three complete native streams, the original failed record, two capsule manifests and stop/review records. CAPSULE-MEMBERS.json indexes all 1351 decoded files; every one of the 1348 Go payloads was compared with the actual compiled source tree. The capsule remains fully recoverable from RAW/native/console.raw.

SOURCE.tar.xz preserves the exact new prepared-data/guest-sources.tar.gz archive, private post and QA revisions, full source maps, original-Go-API module, helper source files, build/prepare/pack/runtime controllers and independent reviews. This deliberately retains the exact new guest archive as well as its capsule transport; it excludes all ELF binaries, Docker layers and caches. Previous native archives/capsules are linked instead of duplicated. Both outer archives are deterministic USTAR regular files with UID/GID/mtime zero and mode 0644; all members are read back after creation. The *-MEMBERS.json inventories give every byte count/SHA-256; ORIGINS.json maps each member to its actual retained file or explicit capsule decoding. SHA256SUMS covers the readable receipts, generator and archives. Parent evidence .gitattributes prevents text conversion.

Validate SHA256SUMS, decompress both archives, and compare every recovered file against its member inventory. The retained decoder is SOURCE/accepted-lineage/journal_capsule.py; decode the console with the exact attempt/profile/source-commit binding in SUMMARY.json and compare CAPSULE-MEMBERS.json. The generator prepare_evidence.py reproduces this fixed snapshot from the retained local work; it performs data reads/compression only, with no Git, Go, app or VM execution. Compressed source diffs retain their original bytes without Git whitespace exceptions.

The [ordinary zero-offer evidence](../d1-zero-offer-accounting-2026-11-20/README.md) and [previous cecc/97a9 evidence](../d1-full-send-sender-2026-11-20/README.md) remain separate immutable references. Guest shutdown proves stopped lifecycle, not functional success or physical XPC resource release. This packet supplies no original periodic/reconnect O1a metric, baseline, 512 MB, D1, public activation or release verdict.

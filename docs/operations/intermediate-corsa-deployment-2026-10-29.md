# S6 intermediate release from dev — scope and readiness

> **Current scope notice — 2026-10-29.** The owner's newer
> [urgent full-product release scope](urgent-full-product-release-scope.md)
> supersedes the broader activation/conformance engineering boundary below.
> The release scope includes current DEV, daemon and clients; session K2/R1b/R2/N-in
> are deferred and off until the next release. For the selected new PirateCash,
> the accepted criterion is protocol only, satisfied by the source check 31≥31.
> The selected matrix is daemon+Desktop on Linux amd64, macOS arm64/amd64
> and Windows amd64/arm64; Android APK/AAB follow separately. Only overlaysim
> is DEFERRED to D1, not PASS; generic make test-all is not green. All other
> applicable emitted-product/routing/TLS/pin tests, builds, crypto review and
> 17-condition admission requirements remain; admission stays closed until
> evidence and owner decision are accepted. The text below retains historical
> evidence and the former work sequence; its earlier requests to decide
> activation of those deferred features no longer apply.

The owner selected a new release from **dev** and changed the orchestration boundary to **S6** on 2026-10-29. The current preparation commit is `cac8b80140dc9dca80d1cc9819c902a54ddac393`. It is not yet the final release candidate, a release approval or a deployment instruction.

## Release subject

| Subject | Exact commit | Protocol / minimum |
| --- | --- | --- |
| Previous published stable, v2.4.70 | `5abb920d273632a10620795d30c41823dd7ce6c5` | 30 / 26 |
| Supplied deployment baseline, v2.2.68 | `d7ddd8ad458a41a52f784c1b4dbd3bfd62b467f5` | 29 / 26 |
| Current dev preparation | `cac8b80140dc9dca80d1cc9819c902a54ddac393` | 31 / 28 |

The current dev client version still says 2.4.70, although its session protocol differs from the published binary. Choose a distinct version before final-candidate checks; identify every build by commit and SHA-256 as well as version. No version or tag has been changed by this scope document. Release metadata/refs were checked through the official GitHub API on 2026-10-29; no release assets were downloaded in this assessment.

S6 includes the ordinary node's active Transition secure-session path: TLS 1.3 and exporter-bound identity proofs, publication only after authentication, expected-peer checks, persistent pins and endpoint protection, strict legacy probe and downgrade/backoff behavior, proxy-aware dialing, loopback limits, unsafe-store startup refusal, self-record minimum session version, and the relevant SDK/UI/operator paths.

Every active change since the previous release is part of regression review, including routing, datagram/transport lifetimes, resource diagnostics, configuration and packaging outside the session package. The full stable-to-preparation diff is 61 commits / 1558 paths. The separate deployed-baseline-to-preparation range is 83 commits / 1672 paths. These counts are tied to the exact commits above, not a review-completion claim.

## Scope boundaries to preserve

- Ordinary construction keeps D1 FindNode/Ready/overlay activation off. Full D1, G2 timing proofs, Member4/full512, the diagnostic entry and the future four-request driver are outside this S6 engineering boundary. Previously committed inactive helpers do not become release activation.
- K1/contact and N-sel pin lifecycles are active. Wider K2/charged-record/N-in candidates are not ordinary defaults. Reconcile the exact promised pin-class behavior with `owner_session_choices` before final acceptance; do not declare every candidate complete or silently narrow an approved promise.
- Public identity-pin removal currently returns unavailable. A supported removal path requires the previously approved independently enforced restart fence and deployment admission. Its precise S6 conformance/disposition must be recorded; an interface or candidate broker is insufficient evidence to enable it.
- Windows/DACL and real Tor checks remain explicitly deferred and UNVERIFIED under the later owner decision. macOS/Linux checks, proxy/no-direct-fallback protections and other admission conditions remain required. Deferred never means PASS.
- No configuration change, node rollout, destructive RPC, D1 activation or release publication is authorised by this document. The owner makes commits; only independently accepted scopes enter the index.

## Current blocking findings and evidence

**S6-R1 — ordinary pin/marker persistence:** preparation review found file-sync/rename without directory-entry confirmation. The POSIX repair now uses the durable writer, confirms the ancestor namespace and re-saves exact validated loaded bytes before marker/startup acknowledgement. Its ordinary-store, marker, namespace, write-outcome, competing proof/expiry and stale-selection checks passed in the combined QA below. Windows retains its existing behavior and is explicitly UNVERIFIED for this durability claim. This is not a reproduced power-loss experiment or a global filesystem guarantee. Public endpoint mutation still refuses unconditionally; passing private-engine tests does not activate it.

Private repairs for **R2** (late dial retry after concurrent v2 reset), **R3** (local pin I/O must not penalise a healthy peer), **R4** (remove the unpublished linear orphan audit from ordinary resource polling) and **R5** (attribute hop-ACK to the captured sender and atomically consume the current attempt) have independent source review. Their regression sources are prepared. A first admitted run on the private `cac8b801` + exact 17-file overlay `9f6eb935…` passed all 60 selected Node and seven routing top-level tests, with 21 child RUN/PASS rows (16 newly added), no FAIL/SKIP. Both Linux arm64/CGO0 test ELF files compiled and were inspected without execution. Independent Tester and Reviewer DATA reviews are accepted. The exact 17 tested files are now in the working tree; the index is unchanged and fresh lint remains pending. R4 omits the new diagnostic series rather than returning a fabricated zero; retain this in release notes. A separate small RPC client fix sequences decoding before reading the return value, whose original expression depended on unspecified evaluation order; its existing nonempty roundtrips also need execution.

The first 67-test QA is historical: its 17 command records exited zero, with 34 RAW files / 383164 bytes and empty stderr. The subsequent exact17 working-tree transfer preserved unrelated paths and the index. A later separate combined run used the approved 25-file overlay (one comment-only file) on 2214 inputs, producing 2227 source paths. All 96 selected top-level tests and 114 named children passed with no FAIL/SKIP; this includes R1/R6, public OFF guards and in-memory HTTP/client error receipts. All 29 records exited zero (14 Go +15 ps), 58 RAW files / 613757 bytes, stderr empty. Four Linux arm64/CGO0 ELF files compiled with metadata inspection only. Main bodies/metadata and the index were preserved; direct normal exit and an absent lock establish scoped process closure, not physical release of every host resource. Independent Tester/Reviewer DATA acceptance precedes the exact25 working-tree transfer. Fresh lint and the full suite remain open. The owner now permits continued necessary tests within agreed resource bounds; no activation, resource growth or release is inferred. Per-path static coverage is separate from runtime acceptance.

**S6-R6 — connection budget lifetime:** the repair retains a removed session reservation until Close returns and transfers the same outbound unit to the exact manual replacement before its dial. The five-function/ten-case regression checks passed in the combined QA, including held Close, publication callbacks, late generations and manual progress. The test transport is owned net.Pipe; this is not proof of physical kernel/socket resource release. Shared Total defaults are unchanged.

On 2026-10-29 the owner agreed to arrange external crypto review once given a concrete task. After the corrected candidate is fixed, provide its exact commit, source/vector package, questions and required attributable report. An expert and report are still pending; internal AI review does not satisfy this condition.

The manifest at `packaging/release-admission.json` has 17 conditions: seven historically done, two partial, seven not_done and one explicitly deferred Tor measurement. `release_decision` remains null. Historical evidence is not automatically final-candidate evidence. Open work includes independent cryptographic review, first-contact timing and timeout derivation, candidate load/resource checks, numerical loopback acceptance, remaining unsafe-store and owner-choice conformance, the full observation plan, final tests and release notes.

The owner approved the [four endpoint-write outcomes](../design/endpoint-binding-write-outcomes-decision.md) and their restart limitation. Outcome/error-transport implementation and public refusal tests are now in the working tree after the scoped combined QA and independent review. Mutating stock Service entrypoints remain unconditionally unavailable; list/preview stay read-only. Identity pins and their separate removal conditions are unchanged. Test completion is not endpoint activation or S6 release admission.

The private diagnostic entry's failed native pipe tests and its unexecuted repair remain outside this candidate scope. Those results do not invalidate the separately accepted constructor/membership commit and do not count as S6 regression tests.

## Review, implementation and verification sequence

1. Maintain a per-path coverage ledger for the complete stable-to-dev diff. Distinguish reviewed, partial and unreviewed paths; record concrete findings and their regression tests. Also trace the update from v2.2.68, state migration, mixed peers, startup paths and secure-session-capable rollback, including pin-store and trust-store format compatibility.
2. Fix confirmed S6 defects and resolve the exact operator/pin-class scope. Preserve default-off boundaries. Review each fix and its fault model before execution; update the candidate identity when source changes.
3. Prepare separately admitted macOS/Linux test profiles: focused regressions, complete applicable suite and lint, CGO paths, real old protocol 28/29/30 interoperability, release-build startup and migration. Compilation or synthetic fixtures do not replace native execution; skipped opt-in checks remain visible.
4. Measure first contact, loopback admission and churn/handshake CPU, pin-store writes and memory on the actual candidate and a clearly identified previous-release baseline using comparable instrumentation. Keep process, service-group and observer costs distinct; `HeapAlloc`, `MemSys` and structural resource floors are not complete RSS/cgroup peak. The 512,000,000-byte goal needs an agreed boundary and completed useful work, not a documentation estimate.
5. Obtain the independent external cryptographic expert's attributable report on the exact candidate, implementation and vectors. Developer/AI review and passing tests do not replace that requirement. Fix and re-review critical findings.
6. Approve the complete observation criteria, sources, anchors, gaps/restarts, budgets and thresholds before release. The existing calendar decision is seven days of baseline before the tag, then the approved acute/spread/stability phases. Preparing that plan is a pre-release obligation; post-tag observation must not be reported as already completed. It precedes later overlay activation.
7. Freeze the new version and exact final candidate, run its required checks, approve release notes and the version-specific release decision. The gate permits only its defined release inputs after the checked candidate. Affected code changes require renewed checks. Publication and the node rollout are separate final actions.

Ordinary polling may improve diagnostics but cannot substitute for the approved strong S6 criteria or the first durable application-message commit at the final recipient in K11. Missing evidence stays UNKNOWN. The S6 boundary is complete only when its applicable release obligations are satisfied, not when a planning document or intermediate commit is ready.

Source anchors: `internal/core/config/config.go`, `internal/core/sessionv2`, `internal/core/node/{session_secure,session_dial_choice,secure_session_store,pin_store_marker,identity_pin_operator}.go`, `internal/release/admission`, `.github/workflows/release.yml`; see also [external review package](s6-external-review-package.md), [measurement/deferral decision](d1-release-decision-proposal.md#measurement-scope-2026-10-23), [observation proposal](s6-observation-plan-proposal.md) and [published stable](https://github.com/piratecash/corsa/releases/tag/v2.4.70).

## Validated construction checkpoint — 2026-10-29

The working tree based on `cac8b801` now contains five tested files for the closed session-only K/R constructor and hermetic release-admission history tests. On the exact clean 2,228-file candidate, native macOS arm64/CGO1 passed **39 Node functions and the full 13-test release-admission package, with 58 observed subtests**, zero FAIL/SKIP and both package verdicts PASS. The two history tests use owned temporary real Git repositories with different parentless commits of one tree. They verify known/unknown commits and non-ancestry independently of the main checkout; this is not proof of the release candidate's actual ancestry.

The K/R checks cover the original Service/resolver/store, one physical 256-slot table, genuine K push/reconciliation authority, R startup/rate/lifetime rules, generation/cancellation refusals and shutdown waiting for actual strict-backend Close. The ordinary constructor remains unchanged; the new factory is closed and unexported. R2 business-port integration, N-in installation/readiness and final ordinary S6 wiring remain separate work.

Two Linux arm64/CGO0 test ELF binaries were compiled and inspected without execution: Node SHA-256 `7df3d0b8ae8f7b0e5d924bb1d5ce0c88c5d5e43bf4f0d8492de5252c5662c1fa` (67,064,249 bytes) and release-admission `de3cb1f0cd4dd78d24ae4e1ab0beb4e45b3e12027502160774407bf8d17c4617` (8,313,404 bytes). Both identify Go1.27.3 and static AArch64 metadata; these are test artifacts, not release binaries. Package discovery returned 73 default packages and three SQLite/CGO-root packages, without executing their tests.

All 26 commands exited normally with zero. All 52 RAW streams were retained: 1,686,459 bytes (1,503,986 test output, 182,473 other metadata), empty stderr. Earlier 1 MiB-limited attempts remain incomplete historical results. Independent tester/reviewer accepted this checkpoint and exact five-file Main transfer, preserving the other 2,223 original files and Git index; the transfer did not stage or commit. Fresh lint, one-process full Node, remaining native/CGO suite and final-candidate checks still follow. This targeted result supplies no standalone corsad memory/load measurement, crypto verdict or release decision.

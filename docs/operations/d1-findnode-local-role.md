# Closed directional FIND_NODE roles

The closed D1 path now authorizes forwarding from an immutable **local** role chosen before Service/Ready construction. Ordinary Core/App factories stay Off. There is no new environment, RPC, configuration or public activation switch.

| Local role | Allowed node type | Own authenticated TLS declaration | Request handling |
| --- | --- | --- | --- |
| Off | Existing types | No role-derived F1/R | No closed FIND_NODE admission |
| Target | Full or Client | F1 only | Answer a request for self; refuse forwarding |
| Responder | Full only | F1 + R | Answer for self or enter the original forwarding path |

The same construction value controls the first Service literal, Ready binding and TLS declaration. Unknown roles and Responder/Client combinations are refused. The nonlocal gate runs after the original proof/F1/frame checks and before full registry capture or intake. Existing ConnID, session lifetime, fairness, quotas and signed-answer checks remain in place. A peer's R declaration cannot grant local authority; an F1-only client may receive a forwarded request and answer for itself. Symmetric peer-R intersection is not required for this closed target path.

The tagged diagnostic explicitly chooses its private role. This implements a closed engineering check, not public role selection. The directional interpretation of the public §2 wire contract remains pending G2; no owner approval is inferred.

## Executed portable QA — November 20

Subject: base `1b97da9a8c581f7a40664aa5b5eb78c59f104f3c` plus 34 exact source files, manifest `fdd45820d3af22e46101ba3b7e9212cfdf59cfcaa1232b5169e59aba6a90dd7e`. This is the tested closed engineering subject, not public activation or final PR‑6 admission.

| Check | Actual result |
| --- | --- |
| Ten new role/closure tests | PASS, including genuine TLS introductions, self-answer by Full and Client, refusal to forward as Target, Responder → F1-only Target, original outbound Client CM/read path, no-F1/unproven/stale-session refusal and construction-tampering controls |
| Selected default/tagged regressions and source guards | **147 outer executions / 311 child passes** across the complete selected scope; no FAIL, SKIP or missing selected test. The ten new tests are included in this total and repeat in the tagged scope; all seven original receive/read-to-offer tests also ran default and tagged. |
| Linux arm64 Node/App test ELF | Compile/BuildInfo/static-ELF checks PASS; **not executed**. SHA-256: Node `7be59a4b0346761b75474c58e5a4ccae66c4c0fba2a879dd1b15c0dbfd610f08`; App `beef25f18fe27f2101d5aa2ed26e49f31bb0cc49ef0910b3c23ba22f2c391023`. |
| Integrity and closure | All 2498 snapshot files unchanged; exact current 34-file overlay verified; all 16 commands exit 0; original outputs retained, own command groups absent and own lock released. |

Native tests used local Go 1.27.4/macOS arm64 with CGO0 and loopback-only networking. Linux-only runtime cases were not run. Selected checks are not a full product suite. The earlier 133-execution PASS remains bound to its separate 33-file subject. Product-suite failures then exposed an old forwarding fixture still choosing Off, nested Seatbelt refusal and inherited umask077 changing permission fixtures. The fixture now chooses FullResponder before Service construction and asserts exact F1/R; original read/CM/duplicate-ID assertions remain. Failed runs and environment contrasts are preserved separately. The ordinary CGO1 macOS release-product v5 run on the final 34-file source **PASSED all 73 selected packages: 51 tested, 22 without test files**; its unchanged script explicitly defers overlaysim, without assigning PASS. All four commands exited0; source hashes and own process closure were independently checked. This is not a green full-repository `make test-all` or an all-platform verdict. Individual SKIP counts are unavailable from the unchanged nonverbose script. It retains the SDK’s original inner sandbox, removes the incompatible outer sandbox, and sets022 only for the release-test child; no outer kernel network/source-write isolation is claimed.

The earlier churn512 result is preserved; [one current closed-role repeat](d1-findnode-resource-diagnostic.md#current-closed-role-churn512--november-20) now covers this Go commit plus five private diagnostic posts under the same512,000,000-byte profile:896 answers/768 participants/512 targets, inclusive service peak210,948,096 bytes and CPU8,653,066µs. No causal or statistical inference is made from the two single observations. O‑1a, final-candidate evidence applicability and remaining 13b-D1 checks, G2/public activation and the parallel S6 observation remain separate.

[Portable QA evidence](evidence/d1-local-role-2026-11-20/README.md) · [Original result](evidence/d1-local-role-2026-11-20/result.json) · [Hashes](evidence/d1-local-role-2026-11-20/SHA256SUMS).

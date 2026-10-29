# Endpoint-binding write outcomes — owner decision, 2026-10-29

[Русский](endpoint-binding-write-outcomes-decision.ru.md)

The owner approved four explicit outcomes for an exact, explicitly confirmed and still-valid endpoint removal or protected-binding demotion. This replaces the old blanket promise that every write error means neither memory nor disk changed. A rename can make the new file visible before directory confirmation fails; a failed protective rewrite does not establish rollback.

| Outcome | Required result |
| --- | --- |
| Not applied (`not_applied`) | A proven refusal before publication. This attempt did not apply its endpoint edit; do not make claims about unrelated concurrent changes. |
| Indeterminate (`indeterminate`) | Return a non-success result, operation ID and exact endpoints. A lowered valid file may already be visible. Retain stronger current in-memory protection; protective restoration has its own result. After restart, the node may load changed valid state: the previous endpoint binding is not guaranteed to survive. |
| Applied, cleanup error (`applied_cleanup_error`) | Distinguish a confirmed commit followed by a resource-close error from both an unchanged disk and an unconfirmed commit. Report the operation and exact endpoints without claiming normal success or complete resource release. |
| Confirmed success (`committed`) | Required persistence is confirmed and the exact still-valid edit is applied; return ordinary success. |

The disposition covers the complete required main/marker/namespace persistence sequence, not merely one low-level write. Unknown errors cannot be treated as proven pre-publication refusals. A successful readback is not proof of durability or historical rollback. Failed restoration is not confirmed rollback. Do not silently repeat destructive operations; clients and console must retain the disposition, operation ID and exact selection even with a nonnil error.

## Availability and selection authority

**Activation of endpoint mutation is not authorised.** Until all competing mutations are checked, mutating calls must be unavailable at their actual domain entrypoints, including calls reached through RPC or UI. Read-only listing and preview do not authorise a commit. Exhaustive selection-mutation coverage, implementation tests and independent review are prerequisites; completing a source inventory does not itself enable the operations.

The approved uncertainty tradeoff applies only while the exact displayed/confirmed selection remains valid. An old token does not authorise lowering a binding whose owner or tier changed. Prevent invalidation across write/apply using a reviewed finite ordering; include proof, renewals, expiry, other operators and recovery writers. No filesystem I/O under the store state mutex. Stale/invalidated selection must never receive an applied result or be justified by general operator intent. An uncertain compensating rewrite remains an unresolved safety path, not approval to lose a newer protection.

Identity pins, other protection grounds and the separate conditions for identity-pin removal are unchanged. This decision does not add a permanent exception, a bulk reset or an automatic fallback to legacy.

## Implementation status

The implementation and targeted checks are now in the working tree: ordinary POSIX persistence, the private endpoint engine, competing proof/expiry ordering, structured RPC/client/console error receipts and the unconditional public mutation refusal. The combined macOS arm64 run passed 96 selected top-level tests and 114 named children with no FAIL/SKIP. Four Linux arm64/CGO0 test ELF files compiled and were inspected without execution. This is scoped test evidence; full regression, fresh lint, real Linux runtime and filesystem crash qualification remain separate. The former no-change-on-every-error statement is superseded.

The contract decision itself did **not** authorise Go runs, activation or release. A later separate GoQA admission enabled the scoped run above; it does not enable endpoint mutation. The owner subsequently authorised necessary testing within agreed resource bounds without a new question for each run. Activation and release remain separate. Filesystem crash behavior, Windows/DACL and full S6 readiness are not certified.

Related: [endpoint RPC](../rpc/secure_session.md), [session protocol](../protocol/session_v2.md), [identity-pin decision](identity-pin-removal-decision.md).

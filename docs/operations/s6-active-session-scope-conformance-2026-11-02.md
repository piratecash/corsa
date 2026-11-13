# S6: conformance to the current owner-approved scope

Date: 2026-11-02. [Русский](s6-active-session-scope-conformance-2026-11-02.ru.md).

This source review covers ordinary daemon, Desktop and SDK construction,
K2/R1b/R2/N-in remaining OFF, and public destructive-operation refusals.
These mechanisms are **deferred, not complete**. It supplies evidence for the
[accepted release scope](urgent-full-product-release-scope.md), not full
`owner_session_choices` or S6 acceptance or permission to release. This source review does not change gate logic or close release conditions by itself.

## Sources and evidence carry

The reviewed code is `ac53899e47970217662f6e45fe199196ea184072`.
HEAD `2c5262a14510e26b18688d5c06821af4fe2e81d3` differs only in four Markdown
files: the Common32 and pin-write reports, external-review package and proposed
pin-write profile. Go code, tests, modules and CI scripts are unchanged.
This carries evidence for unchanged sources; it is not a new build or execution.

| Decision | Checked behavior and source entry |
|---|---|
| K2/R1b remain OFF | [NewService](../../internal/core/node/service.go#L1961) passes an empty `recordAdmissionConstruction{}`; [NewServiceWithNetwork](../../internal/core/node/service.go#L2933) follows the same path. Ordinary configuration and RPC do not select the explicit private candidate factories. |
| Daemon and Desktop follow ordinary construction | [App.New](../../internal/app/node/app.go#L34) passes nil; [zero ObservationOptions](../../internal/core/node/s6_terminal_construction.go#L153) delegate to NewService. [Desktop](../../internal/app/desktop/app.go#L145) directly calls NewService. |
| Ordinary SDK/client leave R2 OFF | [NewWithContext](../../sdk/runtime.go#L242) passes a nil profile; ordinary [Node](../../sdk/runtime.go#L300) and [DesktopClient](../../sdk/runtime.go#L317) receive no own-DM port. [NewServiceForRuntime](../../internal/core/node/runtime_unstarted_cleanup.go#L13) delegates to NewService. Source opt-in APIs exist but do not activate shipped products. |
| N-in remains OFF | Ordinary construction installs neither the inbound producer nor journal and does not migrate the store to format3. Mandatory ordinary pinning remains available; that is not N-in activation. |
| Active pins preserve protection | The [total limit](../../internal/core/node/secure_session_store.go#L80) is 20,000 identities. At capacity, a new identity is refused without evicting existing pins or automatically falling back to legacy. |
| C3 removal remains unavailable | [ConfirmIdentityPinRemoval](../../internal/core/node/identity_pin_operator.go#L193) unconditionally returns `ErrIdentityPinRemovalUnavailable` after the context check; preview reports `RemovalAvailable=false`. |
| Endpoint Remove/Demote remain unavailable | [Remove](../../internal/core/node/endpoint_binding_operator.go#L652) and [Demote](../../internal/core/node/endpoint_binding_operator.go#L674) unconditionally return `ErrEndpointBindingMutationUnavailable` after checking context. Reads and preview do not authorize mutation. |

C3 broker/fence provisioning is not required for a release with the **public
operation unavailable**. This does not waive external fencing/recovery:
implementation, validation of every startup path and separate admission remain
required before future removal activation. Unsafe pin storage and recovery
state must still refuse network startup; disabling removal does not weaken that protection.

## Existing checks and evidence limits

| Area | Specific existing checks |
|---|---|
| K2/R1b and closed activation roots | [TestRecordAdmissionClosedProductionConstructionAndReferences](../../internal/core/node/identity_record_admission_guard_test.go#L107), [TestRelationRecordServiceConstructorFinalStoreAndDefaults](../../internal/core/node/identity_record_admission_service_test.go#L182) |
| R2 and ordinary SDK | [TestOwnDMRuntimeClosedOriginalComposition / TestOwnDMRuntimeRejectsDefaultActivationAndAlternateMint](../../internal/core/node/identity_pin_own_dm_runtime_guard_test.go#L30) check original cross-package paths and negative activation cases |
| N-in | [TestInboundN1DefaultConstructorsDoNotInstallOrMigrateJournal](../../internal/core/node/identity_pin_inbound_lifecycle_test.go#L1107), [TestInboundN2PublicDefaultsAndUnavailableFactoryRemainOrdinary](../../internal/core/node/identity_pin_inbound_install_test.go#L803) |
| Full store and downgrade | [TestAFullPinStoreRefusesANewIdentityAndKeepsEveryExistingOne](../../internal/core/node/pin_store_full_test.go#L35), [genuine inbound session](../../internal/core/node/pin_store_full_test.go#L96), [dialler does not fall back](../../internal/core/node/pin_store_full_test.go#L139), [restart preservation](../../internal/core/node/secure_session_store_test.go#L46) |
| Unavailable mutations | [TestIdentityPinPreviewIsReadOnlyAndRemovalRequiresFence](../../internal/core/node/identity_pin_operator_test.go#L93), [TestServiceEndpointMutationsStayUnavailableWhileReadsWork](../../internal/core/node/endpoint_binding_operator_test.go#L563); RPC and SDK checks also cover unavailability and transport/auth boundaries |

An observed successful [CI Tests job](https://github.com/piratecash/corsa/actions/runs/41098494039/job/124210372902)
is retained for exact `ac53899`. The [workflow](../../.github/workflows/ci.yml#L146)
calls `make test-release`; its [script](../../scripts/test-release.sh) runs
`go test -count=1` without a test-name filter and excludes only
`internal/overlaysim`. The normal test packages listed above are included.
This is a CI conclusion plus the checked command scope, **not downloaded
per-test RAW census**. This review ran no Go tests or applications.
Local retained evidence: `docs/_internal/d1-artifacts/2026-11-02/s6-ac53899-ci-resume-v1/RESULT.json`.

The [pin-write A/B](s6-pin-write-baseline-dev-2026-11-02.ru.md) additionally
checked 19,999→20,000, refusal of the next identity and reload in a bounded
component scenario. It does not replace genuine session/downgrade tests or
prove deferred classes complete or overall resource sufficiency.

This document does not close independent crypto review, loopback/first-contact
measurements and their assessment, platform startup/DACL checks, the full A
observation plan or final candidate/owner release decision. Windows/DACL and Tor
retain their recorded DEFERRED/UNVERIFIED limits. Future changes to affected
code require reassessing this evidence carry.

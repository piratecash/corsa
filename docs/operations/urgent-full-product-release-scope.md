# Current DEV: full-product release scope and remaining blockers

> **Superseding decision, 2026-10-30:** interim 2.5.71 additionally defers the two exact inactive D1 timing diagnostics named in [Releases](../release.md#interim-2571-publication--2026-10-30). They remain in the full suite; every other product test remains required. DEFERRED is not PASS.


The owner selected **current DEV plus necessary regression fixes** for the
usual GitHub release matrix: **daemon and Desktop on Linux amd64, macOS arm64
and amd64, and Windows amd64 and arm64**. Android APK/AAB are a separate later
release step. This replaces the earlier corsad-only proposal; publication
and rollout still require the final admission and owner decision.

Only the `internal/overlaysim` test package is **DEFERRED to D1**, not PASS.
The generic `make test-all` result is therefore not green. All applicable
tests of emitted products, routing, TLS and pins, platform builds, independent
crypto review and the applicable conditions of the 17-condition gate remain
required; no other test-package deferral is implied.

K2/R1b/R2/N-in activation is deferred to the next release and stays off.
Inactive implementations and their tests do not enable them.
The off state refers to ordinary shipped constructors, configuration and RPC.
Explicit internal source opt-in APIs `NewInactiveOwnDMAdmissionCandidate` and
`NewInactiveOwnDMRuntimeCandidate` remain available to source integrators
outside release configuration. D1/G2/FindNode/F1
activation remains outside this release; ordinary roots and destructive
operator refusals stay unchanged. Record this owner scope in final admission
without presenting a deferred feature as implemented or a failed criterion.

The candidate speaks protocol 31, minimum 28. A native full Node package run
has passed; focused regression checks also exist. Those results do not close
the full suite, every platform/client, final binary or release admission.
All applicable release conditions and independent crypto review remain
required for the emitted daemon/Desktop products. Windows runtime/DACL and real Tor checks
retain their recorded **DEFERRED / UNVERIFIED** disposition, not PASS; this
does not silently remove their applicable build/artifact obligations.

## New piratecashd: selected ref and accepted protocol criterion

The owner confirmed exact PirateCash target commit
`071855db490b1f493bd2a985fa390e9620c8db0e` and limited compatibility with it
**to the required protocol only**. Its `src/chainparams.cpp` and startup check
in `src/init.cpp` require Corsa protocol 31. Current Corsa
[config](../../internal/core/config/config.go) reports 31 (its own peer minimum
is 28). The source check 31≥31 satisfies that agreed new-PirateCash compatibility
criterion. Published Corsa v2.4.70/protocol 30 does not. Separate compatibility
with older Corsa peers remains required.

For context, the inspected PirateCash source uses HTTP Basic
`POST /rpc/v1/system/node_status` on 127.0.0.1; it checks a 200 response with
`identity`, `address`, `public_key`, `protocol_version`, `client_version`,
and rejection with 401/403 for a wrong password. Corsa retains the
[route](../../internal/core/rpc/server.go),
[fields](../../internal/core/domain/node_status.go) and
[ordinary daemon RPC](../../internal/app/node/app.go); both
`CORSA_RPC_USERNAME`/`CORSA_RPC_PASSWORD` are required, default 127.0.0.1:46464.
K2/R1b/R2/N-in/D1 are not dependencies of this interface.

**Real interaction with piratecashd has not been run.** This is an evidence
limit, not an additional blocker after the owner's protocol-only decision.
A PirateCash binary hash, external real instance and paired runtime test are
not requested as conditions of this compatibility admission. This document
authorizes no piratecashd launch or live-instance change.

## Remaining release blockers

| Work | Evidence still required |
|---|---|
| Full candidate and products | Complete applicable unfiltered default/tagged tests for emitted products, routing, TLS and pins; overlaysim alone is DEFERRED to D1, not PASS, so generic make test-all is not green. Build daemon/Desktop for all five selected platforms and complete applicable startup checks, preserving the recorded Windows runtime/DACL deferral; active-path review; exact final source, version and artifact SHA-256s. Validate [storage migration 8](delete-intent-time-order-migration.md), its cost/refusal/backup/rollback limits on applicable client/SDK paths. Focused default/CGO passes do not close every deployment database. |
| Compatibility and resources | Real interoperability with older Corsa peers on protocols 28/29/30; final-candidate load/memory, first-contact clearnet timing and loopback acceptance with agreed boundaries. Compilation, logical counts or a Corsa-only measurement do not establish safe host coexistence. |
| Independent security | An attributable external crypto review of the exact implementation/candidate and vectors; resolve blocking findings. Developer/AI review is not the required independent verdict. |
| Observation | Approve complete criteria, sources, thresholds, gaps/restarts and budgets. Preserve the recorded **seven-day baseline before the tag**; the 28-day post-tag observation is not an additional pre-tag wait or already completed evidence. |
| Final admission | Distinct release version, accurate notes/migration/default-off disclosures, final candidate checks and the version-specific release decision. Release admission remains CLOSED until the required evidence and owner decision are accepted; a proposal or historical pass is not admission. |

The owner's remaining participation is to obtain the external crypto
expert/report, approve measurement criteria and finally approve the tag and
publication. Necessary engineering tests continue within their reviewed
bounds; those permissions do not imply criteria waivers or release approval.

See [existing DEV scope](intermediate-corsa-deployment-2026-10-29.md),
[external review](s6-external-review-package.md),
[observation plan](s6-observation-plan-proposal.md),
[recorded measurement decision](d1-release-decision-proposal.md#measurement-scope-2026-10-23),
the [admission manifest](../../packaging/release-admission.json) and
[release workflow](../../.github/workflows/release.yml).

# C2a/C2b32 wire driver: component reproduction

[Русский](c2a-wire-driver-component.ru.md)

The original2026-10-25 C2a recipe below is preserved as history. The current two-profile extension and45-event inventory are recorded in the dated section at the end.

The four files in [c2driver](../../internal/testutil/loadstand/c2driver/main.go) retain the fixed C2a experiment component. They use the production identity/session-v2 APIs to prove eight TLS channels, exchange `get_peers`, close four, rejoin four with fresh identities, and drain. They are neither eight SDK/storage clients nor an F1/FindNode, overload or full 512-node test.

The runtime bytes match the exercised private driver source. Only two stock-source SHA literals in `main_test.go` are updated for the reviewed K12 App/Service integration. The fixed server artifact remains `9ba93241a3f717fb50a6221957e4e3146f982ef5fc7b68a37f90bfe7a49e55bf`; this does not claim that a newer MAIN daemon was measured. The measured native driver artifact is `63782d0722ac1cacace1b3caf97d552d07c916734d94d95bedf9f11aff4c4322`. Rebuilding can produce different bytes.

## Component checks

Run from the repository root with Go 1.27.3 and dependencies already available offline. The race check needs a working local race/CGO toolchain. These commands test only this package; they do not execute the experiment's `main` or establish container resource limits.

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 GOMEMLIMIT=768MiB GOGC=50 \
  go test -mod=readonly -count=1 -timeout=2m -p=1 -json ./internal/testutil/loadstand/c2driver
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 GOMEMLIMIT=768MiB GOGC=50 \
  go test -mod=readonly -count=1 -timeout=2m -p=1 -race -json ./internal/testutil/loadstand/c2driver
```

The fixed inventory is eight parents and 24 children: 32 RUN/32 PASS per mode, with no FAIL/SKIP. Controls include genuine production `sessionv2.Dial/Accept` proof, wrong-identity refusal, bounded frames, partial accounting, intentional-close ownership and single outstanding exchanges. They are not a server-memory measurement. `TestStockSourceClosure` intentionally binds eight stock files; later legitimate source changes require semantic review before pin updates, not deletion of the guard.

An optional native Linux/arm64 component build uses an already configured CGO compiler. Replace `FRESH_PRIVATE_OUTPUT` with an owned fresh output directory:

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off CGO_ENABLED=1 GOMAXPROCS=1 GOMEMLIMIT=768MiB GOGC=50 \
  go build -mod=readonly -p=1 -o "FRESH_PRIVATE_OUTPUT/c2driver" ./internal/testutil/loadstand/c2driver
```

This is a component build example, not the original pinned compiler/provenance recipe or an execution grant. The accepted native recipe and its caps remain separate. No public builder target or module dependency is added here.

## Fixed runtime boundary

Actual execution is Linux-only. `credentials_other.go` deliberately refuses on other systems. Linux opens `/run/c1/auth.json` without following links, requires a stable regular 0600 UID501/GID20/nlink1 file on a read-only mount, and accepts at most 1024 bytes. Credentials are not passed in argv or activation JSON.

Execution is inert unless `CORSA_C2A_RUN=1`, no extra argv, and a closed stdin activation record passes validation. It binds `C2A8-v1`, nonce/approval SHA, the fixed CLI SHA, exact 40hex server identity, original `started_at` and residual work ≤80s; cleanup is bounded by 10s. These input fields are declarations, not proof of resource enforcement or approval. No ready-made approval record or unbudgeted runtime command is supplied.

Endpoints remain loopback TCP 64646/RPC 46464 on `gazeta-devnet`. Fresh instantaneous loopback active gauges support 8→4→8→0; cached live census is diagnostic only. Bounds remain 64KiB per frame, 2048 application frames and 8MiB application bytes, not TLS-wire byte accounting. Four 5s holds also include exchange time; they are not exact 20s wall-clock.

This recipe publishes the Go component only. The private host/keeper/observer/preparation and held-cgroup/flush/cleanup code is still needed to reproduce the [scoped memory result](d1-corsad-memory-screen.md). Building or testing this package does not reproduce that complete measurement, its resource isolation, a full 512MB workload, G2 or release admission. Local-only proof availability outside the original workspace is not assumed.


## Current retained two-profile component — 2026-10-26

The reviewed extension preserves `C2A8-v1`/8→4→8→0/12identities and adds only `C2B32-v1`/32→16→32→0/48identities. There is no free-N option. The two unchanged credential files and qualified main/test updates retain production identity/session-v2 APIs and original bounded reader/write/close ownership. Fixed Source2806/CLI-literal binding: `/private/tmp/corsa-D1-C2b32-wire-driver-QA-ROOT-v1/BINDING.json`; manifest `2806e1feef432c56b4db0824ea5cfe6b9e6694178c4df7d37a890b24599cd734`. Local artifacts/proof are not assumed available elsewhere.

The current package passed **45 RUN/45 PASS per ordinary/race mode** (10parents/35children), no FAIL/SKIP; scoped Darwin/Linux CGO0 lints each0issues. All32 original events remain, plus13 closed-profile/activation/receipt controls, including fixed phase dimensions and recovered ordinal order, the present-empty old flag and bounded records. `TestStockSourceClosure` retains eight exact stock pins for reviewed K9 App/Service; do not delete or blindly refresh the guard. These tests and a component build are not resource-runtime acceptance.

Legacy8 still binds CLI9ba93241… and `CORSA_C2A_RUN=1`. Profile32 requires `CORSA_C2B32_RUN=1` **and CORSA_C2A_RUN absent, including not present-empty**, with distinct activation `C2B32-v1`. Its compiled CLI is `ae61fc926e7f5c2353a21075041300fb722f2c8346389344a6e2e145931632c7`; old8 flags/grants cannot enable32. Linux-only/RO501:20/0600 credential checks, loopback endpoints, exact40hex server binding,80s residual+10s cleanup,2s operations, four5s waits plus exchanges,64KiB/frame and2048application frames/8MiB remain unchanged. Activation declarations alone never grant permission; no unbudgeted launch command is supplied.

Current driver ELF `90d1c2d68524a5507aa09d4e443cb70cf3c52fb6fe936a1d9280917bad7f7aa7` (6750368bytes) and CLIae61… (21627040bytes) are Go1.27.3/Linuxarm64v8.0/**CGO0 static self-only** artifacts, no interpreter/needed DSO. CLI source is basee50f97511d88d7ffd7a08daa3490bee7d63f0f6a **plus19 staged paths**, snapshot1fcca8012a39015fcc377472d2e56cdb82cbedef0562f07ee6acf8f6895f9f29 (2119files/70186204bytes); driver2806 adds four component posts and the actual CLI literal (2119files/70197026bytes). Host cross-build and later native Linux Docker execution are separate facts; rebuilding may change bytes. A static component build variant of the example above uses `GOOS=linux GOARCH=arm64 GOARM64=v8.0 CGO_ENABLED=0`, `-trimpath -buildvcs=false -mod=readonly -p=1 -ldflags='-s -w'`; it does not execute the artifact or reconstruct the private resource harness.

The accepted C2b32 run completed32→16→32→0,48 distinct identities,1344application frames/41216bytes. Fresh authenticated `getNodeStatus`/`fetchRouteSummary` verified the same server incarnation and instantaneous atomic `active==0` **before** `zero-drained`; child exit was not the zero witness. The final RPC body/atomic field/read timestamp was not separately retained as RAW; qualification is source-bound to the executed driver. Cached live census remains diagnostic only. See the [exact memory scopes and proof](d1-corsad-memory-screen.md). K9/K10/K12 opt-ins were OFF; old9ba memory is not a current baseline/per-peer delta. Private host/keeper/observer/secret/cgroup custody remains necessary for full measurement reproduction. Full512/F1/overload/G2/release remain open; this result does not authorize another run.

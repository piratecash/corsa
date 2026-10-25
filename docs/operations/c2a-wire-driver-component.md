# C2a wire driver: component reproduction

[Русский](c2a-wire-driver-component.ru.md)

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

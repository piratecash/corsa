# T5g build provenance, schema 2

## English

This is an inactive build/provenance tool, separate from the node and from runtime acceptance. `scripts/loadstand-build.py` has one closed request and two independent build lanes. It never executes a built node, SDK edge or helper, including a presumed version-only invocation. A `verified_equal` receipt means equal raw artifact bytes on the recorded host under the recorded inputs. It does not approve T5, resource limits, G2, D1, a release, image authenticity, hermetic compilation, network isolation or cross-host reproducibility.

The initial supervisor backend requires Linux, Python 3.11 or later, `fork`, `setsid`, two directional nonblocking CLOEXEC pipes, FD `execve`, `/proc`, `waitid(WNOWAIT)`, verified child-subreaper support and no pre-existing children or threads. Darwin and other unsupported hosts return `UnsupportedSupervisor` before executable discovery or external tools; there is no portable process-kill fallback. The pure Python tests run on Darwin, but their explicitly skipped Linux controls are pending rather than evidence that the Linux lifecycle passed.

### Request, targets and flavours

The only CLI is:

```text
python3 scripts/loadstand-build.py --request /absolute/regular/request.json --output /absolute/fresh/output
```

Execution needs a separately supplied complete, approved source/seed/toolchain inventory. The command above documents the interface; it is not permission to run two real builds. The output must be fresh, disjoint from immutable input roots, owned and opened as a retained directory capability. There are no arbitrary commands, package paths, targets, build flags, environment imports, fixture switches, shell expansion, Make invocation or `.env` processing. The script does not install or download tools or create Git repositories/objects.

[The checked-in schema](loadstand-build-provenance.schema.json) defines exact fields. Request version is `2`; every field is required, including explicit nulls. `git` names the read-only object directory, pinned Git executable, base commit and base tree. `overlay` is a sequence of exact add/modify/delete bindings. `seed` names its original root and manifest SHA256. `toolchain` pins Go and the complete GOROOT manifest. `native` is either the full native-input declaration or null according to flavour. `targets`, `keep_work` and the explicit image-evidence discriminant complete the request. Unknown fields, defaults, duplicate keys, floats, nonfinite numbers, boolean-as-integer, invalid UTF-8, missing/null confusion, zero hashes and malformed paths are refusals.

| Target | Fixed package | Boundary |
|---|---|---|
| `corsa-node` | `./cmd/corsa-node` | Headless node |
| `sdkedge` | `./internal/testutil/loadstand/sdkedge` | Real DesktopClient/SQLite edge, not the GUI |
| `linuxhelper` | `./internal/testutil/loadstand/linuxhelper` | Must exist in the supplied snapshot; absent in the initial frozen 374 baseline |
| `fullnode` | `./internal/testutil/loadstand/fullnode` | Planned Full Member with the original headless Service/RPC/final-accounting lifecycle; distinct from `corsa-node` |

The planned T5 Member build selects exactly `fullnode`, `sdkedge` and `linuxhelper`; the existing `corsa-node` identity and package remain unchanged. One to three unique requested targets are all mandatory. Missing source is `TargetSourceMissing`, never a stub, moving checkout or smaller target set.

| Flavour | Fixed build settings | Claim |
|---|---|---|
| `native-linux-arm64` | Native Linux arm64, Go1.27.3, GOOS=linux, GOARCH=arm64, GOARM64=v8.0, CGO=1 | Recorded-input native compile only; requires supported direct native tools/inputs |
| `cross-compile-cgo0` | Linux supervisor host, same Go and target settings, CGO=0 | CompileOnly; never a fallback for a native request |

Every build uses `build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' -o <fixed lane output> <fixed package>`. No custom tags, `-X`, build-ID edits, hidden concurrency tuning or artifact rewriting. VCS metadata is deliberately `disabled_archive`, not `vcs.modified=false`. The base commit timestamp supplies SOURCE_DATE_EPOCH; this is recorded input, not a normalization guarantee. An external image digest is declared/unverified; native-host absence is explicit.

### Read-only source and original seed authority

All external Git, Go, compiler-inspection and metadata calls use one `LinuxSupervisor.run_owned` edge. Git is read-only and uses a clean fixed environment with system/global configuration, replacements, lazy fetching, prompts and helper paths disabled; unsupported alternates, shallow storage, grafts, replacement refs and relevant unsafe configuration refuse. Actual object identity is recomputed as Git SHA1 of `type + space + decimal byte length + NUL + bytes`; commit→tree→path→blob is checked independently of the caller's origin label. Only `cmd/`, `internal/`, `sdk/`, `go.mod` and `go.sum` are extracted. No `.git`, `.env`, caches, refactoring archives or whole checkout is copied.

A versioned overlay binds a real origin commit/tree/path/blob. A staged overlay has null commit/tree and a required frozen capture-manifest SHA256; it is explicitly uncommitted caller evidence. Actual bytes/OIDs are checked, but a supplied stage capture does not prove historical authorship, review or approval. A harness transplanted from a newer revision retains its own origin. Delete operations carry no invented after blob.

The effective source hash is the one framed SHA256 algorithm: literal `corsa.t5g.v2.source` plus NUL, then sorted entries containing the 8-byte big-endian path-byte length, UTF-8 path, six ASCII mode bytes, 8-byte big-endian file length and 32 content-hash bytes. It is not a fabricated Git commit. Origins are retained separately. Other domains use SHA256 of literal `corsa.t5g.v2.<name>` plus NUL plus canonical JSON, including `seed`, `toolchain`, `native`, `graph`, `closure`, `request.canonical` and `outputs`. Raw request SHA256 is separate. Canonical JSON is UTF-8, sorted keys, fixed separators, no whitespace/newline or floats.

The original seed root and each original regular file FD remain retained. No-follow traversal, actual FD type/owner/mode/link-count/size checks, before/after fstat and bounded reads apply. Hardlinks and unexpected entries refuse. A and B copy independently from those original FDs into distinct initially empty module/build caches and HOME/TMPDIR/GOPATH roots; they never share a cache or seed B from A. Actual destination FD bytes are rehashed before close. Replacement of an input path cannot make the script consume the replacement as the original authority.

Only the identified source `go.sum` authenticates supplied `.mod` and module ZIP contents. Its exact blob and origin are retained; an uncommitted go.sum remains labelled uncommitted. Caller seed SHA256 and caller `.ziphash` do not replace that authority. Go h1 follows pinned x/mod v0.41.0 `sumdb/dirhash`: SHA256 each file, sort names, hash `<hex>  <name>\n`, standard base64 with `h1:`. `.mod` uses the single filename `go.mod`; ZIP h1 includes the actual module@version prefix and all ZIP entries, including directory entries. A later extracted-tree h1 contains actual regular files only, so a ZIP/tree h1 disagreement refuses instead of being normalized away. Destination `.ziphash` is regenerated from the verified ZIP h1 and rechecked even when absent from the input seed.

Module ZIP validation checks actual central-directory membership before allocating its inventory, local names, CRC, raw compressed stream size and bounded expansion; declared sizes/counts alone are not trusted. Traversal, duplicate/case collisions, bad module roots, irregular entries, unsupported compression, encryption, invalid go.mod placement and bombs refuse. Applicable Go module/path rules follow x/mod v0.41.0 with literal Go1.27.2 Unicode17 Letter/SimpleFold tables (tables.go SHA256 `ac0e5d5d1b58ed4b02d7cb4a0085bdefbdb4f7aae6fadddbc9477f8463da8bd8`), not Python full case folding or its Unicode category version. In particular, ß and SS are distinct, while ß and ẞ collide. Source-tree collision checks remain deliberately conservative. `.info` version and real calendar timestamp are syntax-checked and its SHA is retained, but it is not represented as signed or go.sum-authenticated metadata.

Offline `go list -m -json all` establishes the selected graph and `go list -deps -json` establishes the requested package/file closure. Every selected non-main module needs verified `.mod`; every used module additionally needs verified ZIP, exact go.sum and actual extracted-tree h1. Graph-only modules are explicit. Workspace/local replacements, undeclared files, unsupported Fortran/SWIG and missing inputs refuse. `go mod verify` is an additional check, not the primary sum or completeness authority. `GOPROXY=off`, `GOSUMDB=off`, `GOENV=off`, `GOTOOLCHAIN=local` and a fixed environment prevent Go download fallbacks; they do not establish kernel egress isolation.

Original seed/manifest FDs, both destination seeds, actual used module trees, source files, complete GOROOT, generator/schema/Python/Git identity and native inputs are checked before and after each lane and A is checked again after B. Artifact bytes are retained/rehashed through the Go build-ID and `version -m` metadata readers. The artifact itself is never run. Actual runtime version and `/proc/pid/exe` binding remain `deferred_to_owned_runtime`; source-declared Corsa version is not a runtime observation.

### Native files and explicit unsupported cases

Native manifests declare complete, disjoint scoped roots within `/usr`, `/lib`, `/lib64` or `/opt`, direct ELF CC/CXX/pkg-config/linker/archiver tools, their hashes, observed include/search/sysroot paths and all scoped regular/file-link inputs. Versions and paths are inspected only through owned commands. Missing native support refuses, never CGO0 fallback. Cgo/pkg-config flags use a finite allowlist; include/library operands must remain within the fingerprinted source/module or declared native roots. Response files, specs, plugin/include mechanisms and unknown flags refuse rather than importing unrecorded files. The same selected flag/pkg-config gate applies to Standard packages, including runtime/cgo; complete GOROOT source hashing never bypasses it. Standard relative operands resolve from the actual package directory within the fingerprinted GOROOT scope. This is observational recorded-input checking, not a statement that a CC hash covers every auxiliary or compiler read.

Only native header/library records allow the closed `regular|symlink` union. A symlink record stores each exact strict-UTF8 `readlink` byte string's SHA256/byte length and lstat identity separately from the resolved regular target's public path, SHA256, size and actual FD identity. Parent capabilities and the final target FD stay retained. At most32 file-link hops may remain in the declared public-root union. Cycles, escape, changed link text/inode/target, invalid encoding, undeclared entries and same-path target replacement refuse. Directory symlinks are `NativeInputsUnsupported`. Source, seed and GOROOT never gain this exception. Top-level executable links resolve once to an actual pinned regular executable FD.

Recorded before/after checks cannot detect a hostile host that changes bytes and restores them between observations. Unsupported process-tree escapes, unavailable `/proc`/subreaper/wait APIs, unknown native inputs, directory links and incomplete inventories cannot be upgraded to a successful receipt. This slice does not claim universal native-host support.

### Owned tools, limits and terminal receipt

The single-threaded wrapper has no pre-existing children. Each tool gets exclusive0600 ordinary stdout/stderr files and a private stub. Exactly two nonblocking CLOEXEC unidirectional pipes carry READY and GO, never logs. The stub creates its session/group and reports a nonce; the parent checks actual direct-child PID/start identity, PPID/SID/PGID while unreaped, then sends matching GO. No requested tool runs before GO. Control-FD ownership begins at the first successful pipe call: failure during the second pipe, nonce or fork closes every acquired end, abandons both open log FDs and re-raises the original exception. No child/joined command result is invented for that pre-fork failure. Before GroupReady, refusal/cancel signals only the exact unreaped direct PID, never a guessed process group. Executable FD, fixed argv and explicit environment are the only exec inputs.

Leader exit is observed with WNOWAIT. The leader is retained as the PID/PGID anchor until subreaper direct/adopted children and group/session membership are joined. Adopted exited children are collected individually; the leader is reaped last. A child retaining a regular log FD prevents premature completion. TERM/KILL, wait, command failure and collection outcomes are immutable records; no signal is sent after leader reap. Unknown/overflow/escaped/unjoined membership poisons the supervisor, retains possibly written work, refuses lane B/further tools and yields incomplete. This handles ordinary descendants, not malicious-kernel containment.

| Observed ceiling / budget | Value |
|---|---|
| Request / overlay / seed / receipt JSON | 1 / 16 / 16 / 16MiB |
| JSON depth / values / array / string / path / argv | 16 / 300000 / 100000 / 64KiB / 1024bytes and32segments / 4096bytes |
| Targets / source files; source file / total | 3 / 100000; 64MiB / 2GiB |
| Modules / seed objects; mod / info / ziphash | 4096 / 20480; 1MiB / 64KiB / 128bytes |
| ZIP compressed file / all seed; entries per module / all | 256MiB / 8GiB; 100000 / 500000 |
| Expansion per module / lane | 2GiB / 16GiB, additionally Go500MiB module ZIP bound and64MiB individual file bound |
| Fingerprint entries / JSON / file / total | 100000 / 32MiB / 512MiB / 8GiB |
| Closure files / packages / JSON | 250000 / 50000 / 32MiB |
| Stdout/stderr / Git or go-list stdout / retained nonbinary evidence | 8MiB / 64MiB / 512MiB |
| Artifact / lane artifacts / cache / pair work | 512MiB / 1536MiB / 8GiB / 64GiB |
| Children / kernel child list / control | 4096 / 128KiB / 256bytes |
| READY / ordinary command / graph / target build / lane / pair | 5 / 30 / 300 / 1800 / 2700 / 6000seconds |
| Monitor / TERM / KILL-to-join / total stop / safe deletion | 100ms / 2 / 3 / 10 / 10seconds |

Bounds compose: reaching a smaller byte/value/time limit refuses before an individual maximum can be reached. These are observed implementation ceilings and cooperative deadlines, not physical quotas, D1 admission or a hard bound on uninterruptible filesystem/kernel I/O. Logs may exceed a threshold between ticks; required oversize evidence is not truncated to manufacture success.

`artifacts-manifest.json` lists sorted retained evidence `(path,size,SHA256,mode,role,completeness)` and excludes itself, `receipt.json`, `receipt.sha256` and any later runtime receipt. The output-manifest hash is the `outputs` domain hash. Complete raw request, origin/source/seed/toolchain/native/graph/closure/checksum records, ordinary command outputs/exit records, checks, artifact bytes, metadata and comparison/failure evidence are retained. A checkpoint precedes safe work deletion. Cleanup only touches known owned work after confirmed tree join, or intentionally keeps it. Referenced FDs are verified and closed; cleanup facts are final before the immutable manifest and terminal receipt. Receipt publication uses an exclusive temp, file fsync, no-replace atomic rename and directory fsync. A sidecar is the raw receipt hash; it introduces no cycle or external signature.

`verified_equal` requires SAME effective-source digest across A.before/A.after/B.before/B.after, identical seed/toolchain/closure, both exact requested target sets, independently empty caches, equal raw artifact hashes, all tool trees joined, required complete evidence and confirmed/intentional cleanup. `failed` is a determinate joined refusal/build/mismatch. Cancellation, deadline, input mutation, unknown I/O/writers or cleanup is `incomplete`; missing/unsynced terminal receipt is also incomplete to the caller. There are no invented zero hashes, exit0, absent-lane successes or initially successful receipts later revoked by cleanup.

### Validation evidence and remaining gates

The stdlib validator enforces the closed tables and dynamic cross-field/FD authorities. The schema expresses structural discriminants; dynamic hash equality, exact ownership and UTF-8 byte/count bounds remain runtime gates. Tests use a separately bounded interpreter of only the emitted schema subset, rejecting unknown keywords and external/cyclic refs. Fixture Git objects and Python child tools never become CLI authorities or real Go/native-build evidence.

Portable tests cover strict parser/schema, real own-temp FD/no-follow/FIFO/hardlink behavior, original seed copies and destination corruption, x/mod h1 agreement, malformed/expanded ZIPs, versioned/staged origin, same-source conjunction, native link replacement and clean environment. Explicit Linux-only tests exercise future owned Python child READY/GO, wrong nonce, pre-group cancellation, inherited regular logs, TERM-ignoring descendants, anchor-last collection, output/deadline and poisoned membership. They require a separately qualified Linux run; Darwin skip is not a passed kernel check. A recorded-input native two-lane qualification is documented below; runtime binding and qualification of a final candidate remain separate gates.

---

## Русский

Плановый T5 Member использует три точные цели `fullnode` → `./internal/testutil/loadstand/fullnode`, `sdkedge` и `linuxhelper`. `corsa-node` остаётся отдельной прежней целью `./cmd/corsa-node`; это не псевдоним Full Member. Четыре доступных имени не увеличивают предел запроса: не более трёх целей. Probe, schema/receipt и reader metadata используют одно ожидаемое значение Go1.27.3; исторические Unicode/table pins не меняются.


Это неактивный инструмент сборки и provenance, отдельный от узла и допуска runtime. У `scripts/loadstand-build.py` один закрытый запрос и две независимые lane сборки. Он никогда не запускает собранный node, SDK edge или helper, включая предполагаемый запрос версии. `verified_equal` означает равенство исходных байтов артефактов на записанном хосте при записанных входах. Это не допуск T5, ресурсов, G2, D1, релиза, подлинности образа, герметичности, сетевой изоляции или воспроизводимости между хостами.

Первый backend supervisor требует Linux, Python3.11+, `fork`, `setsid`, два направленных неблокирующих CLOEXEC pipe, FD `execve`, `/proc`, `waitid(WNOWAIT)`, подтверждённый child subreaper и отсутствие прежних детей/потоков. Darwin и другие неподдерживаемые хосты возвращают `UnsupportedSupervisor` до поиска исполняемых файлов и внешних инструментов. Переносимого fallback с недоказанным kill нет. Чистые Python-тесты работают на Darwin, но пропущенные Linux-контроли остаются pending.

### Запрос, цели и варианты

Единственный CLI:

```text
python3 scripts/loadstand-build.py --request /absolute/regular/request.json --output /absolute/fresh/output
```

Выполнение требует отдельно предоставленного полного одобренного inventory source/seed/toolchain. Команда показывает интерфейс, а не разрешает два реальных build. Output должен быть новым, отделённым от неизменяемых входных корней, принадлежащим владельцу и открытым через удерживаемую directory capability. Произвольных команд, package paths, целей, flags, импорта env, fixture-switch, shell expansion, Make или `.env` нет. Скрипт не устанавливает/скачивает инструменты и не создаёт Git repository/objects.

[Схема](loadstand-build-provenance.schema.json) задаёт точные поля. Версия запроса `2`; обязательны все поля, включая явный null. `git` задаёт read-only object directory, pinned Git executable, base commit/tree; `overlay` — точные add/modify/delete bindings. `seed` задаёт исходный root и SHA256 manifest; `toolchain` — Go и полный GOROOT manifest. `native` содержит весь native declaration либо null согласно flavour. `targets`, `keep_work` и явное состояние image evidence завершают запрос. Unknown keys, defaults, duplicate keys, float/NaN, bool вместо int, invalid UTF-8, отсутствующий null, нулевые hash и неправильные пути отвергаются.

| Цель | Фиксированный package | Граница |
|---|---|---|
| `corsa-node` | `./cmd/corsa-node` | Headless node |
| `sdkedge` | `./internal/testutil/loadstand/sdkedge` | Настоящий DesktopClient/SQLite edge, не GUI |
| `linuxhelper` | `./internal/testutil/loadstand/linuxhelper` | Должен присутствовать в snapshot; в исходном frozen374 отсутствует |

От одной до трёх уникальных запрошенных целей обязательны целиком. Нет source — `TargetSourceMissing`, без stub, moving checkout или уменьшения набора.

| Flavour | Фиксированные настройки | Допустимый вывод |
|---|---|---|
| `native-linux-arm64` | Native Linux arm64, Go1.27.3, GOOS=linux, GOARCH=arm64, GOARM64=v8.0, CGO=1 | Только native compile с записанными входами; необходимы поддерживаемые прямые native tools/inputs |
| `cross-compile-cgo0` | Linux supervisor host, те же Go/target settings, CGO=0 | CompileOnly; не fallback для native-запроса |

Каждая сборка использует `build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' -o <fixed lane output> <fixed package>`. Нет custom tags, `-X`, правки build-ID, скрытого изменения concurrency или переписывания binary. VCS — `disabled_archive`, а не `vcs.modified=false`. SOURCE_DATE_EPOCH равен timestamp base commit: это записанный вход, не гарантия нормализации. Внешний image digest остаётся declared/unverified; отсутствие образа у native host явно обозначено.

### Read-only source и authority исходного seed

Все внешние Git, Go, compiler-inspection и metadata команды проходят единственный `LinuxSupervisor.run_owned`. Git read-only с чистым фиксированным env; system/global config, replacements, lazy fetch, prompts и helper paths отключены. Неподдерживаемые alternates, shallow storage, grafts, replacement refs и опасные настройки отвергаются. Git SHA1 независимо вычисляется по `type + space + decimal byte length + NUL + bytes`; проверяется commit→tree→path→blob, а не доверяется origin label. Извлекаются только `cmd/`, `internal/`, `sdk/`, `go.mod`, `go.sum`; `.git`, `.env`, cache, refactoring archives и checkout целиком не копируются.

Versioned overlay привязан к реальным origin commit/tree/path/blob. Staged overlay имеет null commit/tree и обязательный SHA256 frozen capture manifest: это явно uncommitted caller evidence. Байты/OID проверяются, но capture не доказывает автора, review или approval. Новый harness, перенесённый в старую версию, сохраняет собственное происхождение. У delete нет выдуманного after blob.

Единственный effective source hash: SHA256 literal `corsa.t5g.v2.source` + NUL, затем отсортированные entries: 8-byte big-endian длина UTF-8 path, path, шесть ASCII mode bytes, 8-byte big-endian длина файла, 32 байта content hash. Это не искусственный Git commit. Origins хранятся отдельно. Остальные hash domains — SHA256 literal `corsa.t5g.v2.<name>` + NUL + canonical JSON, включая `seed`, `toolchain`, `native`, `graph`, `closure`, `request.canonical`, `outputs`. Raw request SHA256 отдельный. Canonical JSON: UTF-8, sorted keys, фиксированные separators, без whitespace/newline/float.

Исходный seed root и каждый исходный regular FD удерживаются. Действуют no-follow traversal, проверки реального FD type/owner/mode/link-count/size, before/after fstat и ограниченное чтение. Hardlinks и лишние entries запрещены. A и B независимо копируют из исходных FD в разные исходно пустые module/build caches, HOME/TMPDIR/GOPATH; нет общей cache или B из результатов A. Перехэшируются байты реального destination FD до close. Подмена имени не превращает replacement в исходную authority.

Только идентифицированный source `go.sum` аутентифицирует `.mod` и module ZIP. Сохраняются точный blob и origin; uncommitted go.sum так и обозначается. Caller seed SHA256 и `.ziphash` не заменяют эту authority. Go h1 соответствует pinned x/mod v0.41.0 `sumdb/dirhash`: SHA256 каждого файла, sorted names, transcript `<hex>  <name>\n`, standard base64 с `h1:`. `.mod` использует единственное имя `go.mod`; ZIP h1 включает module@version prefix и все ZIP entries, включая directory entries. Последующий extracted-tree h1 содержит реальные regular files, поэтому расхождение ZIP/tree отвергается, не нормализуется. Destination `.ziphash` создаётся заново из проверенного h1 и повторно проверяется даже без такого исходного seed entry.

ZIP validation проверяет фактический central inventory до выделения списка, local names, CRC, реальную compressed stream и ограниченное expansion. Объявленные размеры/counts не authority. Traversal, duplicates/case collisions, неправильный module root, irregular entries, compression/encryption вне поддержки, неправильный go.mod и bombs отвергаются. Применимые Go module/path rules следуют x/mod v0.41.0 с literal Go1.27.2 Unicode17 Letter/SimpleFold tables (tables.go SHA256 `ac0e5d5d1b58ed4b02d7cb4a0085bdefbdb4f7aae6fadddbc9477f8463da8bd8`), не Python full case folding или версией его Unicode categories. ß и SS различаются, а ß и ẞ конфликтуют. Source collision checks остаются намеренно консервативными. Версия `.info` и реальный calendar timestamp проверяются, SHA сохраняется, но это не подписанная или go.sum-authenticated metadata.

Offline `go list -m -json all` устанавливает selected graph, `go list -deps -json` — closure запрошенных packages/files. Каждому selected non-main module нужен verified `.mod`; каждому используемому module также ZIP, точный go.sum и фактический extracted-tree h1. Graph-only обозначен явно. Workspace/local replacements, undeclared files, Fortran/SWIG вне поддержки и missing inputs — отказ. `go mod verify` дополнительный, не главный источник суммы/полноты. `GOPROXY=off`, `GOSUMDB=off`, `GOENV=off`, `GOTOOLCHAIN=local` и фиксированный env запрещают Go download fallback, но не доказывают kernel egress isolation.

До и после каждой lane проверяются original seed/manifest FD, оба destination seed, реальные used module trees, source files, полный GOROOT, generator/schema/Python/Git identity и native inputs; после B снова проверяется A. Artifact FD удерживается и перехэшируется через Go build-ID и `version -m` metadata readers. Сам artifact не запускается. Runtime version и `/proc/pid/exe` остаются `deferred_to_owned_runtime`; source-declared Corsa version не является runtime observation.

### Native files и явная неподдержка

Native manifests задают полные непересекающиеся scoped roots внутри `/usr`, `/lib`, `/lib64`, `/opt`, прямые ELF CC/CXX/pkg-config/linker/archiver, hashes, наблюдаемые include/search/sysroot paths и все scoped regular/file-link inputs. Инспекции идут только через owned commands. Missing native support — отказ без CGO0 fallback. Cgo/pkg-config flags имеют finite allowlist; include/library operands остаются в fingerprinted source/module либо declared native roots. Response files, specs, plugin/include mechanisms и unknown flags отвергаются вместо импорта незаписанных файлов. Тот же selected flag/pkg-config gate применяется к Standard packages, включая runtime/cgo; полный source hash GOROOT не обходит его. Относительные std operands разрешаются от реального package directory в fingerprinted GOROOT scope. Это наблюдение записанных входов, не доказательство всех auxiliaries или каждого чтения компилятора.

Только native header/library records допускают закрытый `regular|symlink` union. У symlink отдельно хранятся SHA256/длина точных strict-UTF8 `readlink` bytes и lstat identity каждого шага, затем resolved regular public path, SHA256, size и реальная FD identity. Parent capabilities и final target FD удерживаются. Не более32 file-link hops внутри declared public-root union. Cycle/escape, изменившиеся link text/inode/target, encoding, undeclared entry и replacement по тому же имени — отказ. Directory symlinks — `NativeInputsUnsupported`. Source, seed и GOROOT исключения не получают. Top-level executable links разрешаются один раз до actual pinned regular executable FD.

Before/after observations не обнаруживают злонамеренный host flip-and-restore между проверками. Неподдерживаемый process-tree escape, недоступные `/proc`/subreaper/wait API, unknown native inputs, directory links и неполный inventory не могут стать successful receipt. Универсальная поддержка native hosts не заявляется.

### Owned tools, пределы и terminal receipt

Однопоточный wrapper не имеет прежних детей. Каждая команда получает exclusive0600 ordinary stdout/stderr и private stub. Ровно два неблокирующих CLOEXEC однонаправленных pipe несут READY и GO, не logs. Stub создаёт session/group, сообщает nonce; parent проверяет фактические direct-child PID/start, PPID/SID/PGID до reap и отправляет GO. До GO инструмент не запускается. Владение control FD начинается с первого успешного pipe: отказ второго pipe, nonce или fork закрывает все acquired ends, освобождает оба открытых log FD и повторно выдаёт исходное исключение. Для pre-fork failure не выдумываются child или joined command result. До GroupReady отказ/cancel сигналит только точный unreaped direct PID, не guessed group. Exec получает pinned executable FD, fixed argv и explicit env.

Выход лидера наблюдается WNOWAIT. Он удерживается как PID/PGID anchor до join subreaper direct/adopted children и group/session membership. Adopted exited children собираются поодиночке, лидер reaped последним. Потомок с regular log FD не даёт преждевременного завершения. TERM/KILL, wait, command failure и collection сохраняются неизменно; после reap сигналов нет. Unknown/overflow/escaped/unjoined membership отравляет supervisor, сохраняет возможно записываемые work, запрещает B/следующие инструменты и даёт incomplete. Это lifecycle обычных потомков, не containment злонамеренного kernel.

| Наблюдаемый предел / бюджет | Значение |
|---|---|
| Request / overlay / seed / receipt JSON | 1 / 16 / 16 / 16MiB |
| JSON depth / values / array / string / path / argv | 16 / 300000 / 100000 / 64KiB / 1024bytes и32segments / 4096bytes |
| Targets / source files; source file / total | 3 / 100000; 64MiB / 2GiB |
| Modules / seed objects; mod / info / ziphash | 4096 / 20480; 1MiB / 64KiB / 128bytes |
| ZIP compressed file / all seed; entries per module / all | 256MiB / 8GiB; 100000 / 500000 |
| Expansion per module / lane | 2GiB / 16GiB, дополнительно Go500MiB module ZIP и64MiB на файл |
| Fingerprint entries / JSON / file / total | 100000 / 32MiB / 512MiB / 8GiB |
| Closure files / packages / JSON | 250000 / 50000 / 32MiB |
| Stdout/stderr / Git или go-list stdout / nonbinary evidence | 8MiB / 64MiB / 512MiB |
| Artifact / lane artifacts / cache / pair work | 512MiB / 1536MiB / 8GiB / 64GiB |
| Children / kernel child list / control | 4096 / 128KiB / 256bytes |
| READY / ordinary command / graph / target build / lane / pair | 5 / 30 / 300 / 1800 / 2700 / 6000секунд |
| Monitor / TERM / KILL-to-join / total stop / safe deletion | 100ms / 2 / 3 / 10 / 10секунд |

Пределы действуют совместно: меньший byte/value/time bound может сработать до индивидуального максимума. Это наблюдаемые implementation ceilings и cooperative deadlines, не physical quota, D1 admission или жёсткий предел uninterruptible FS/kernel I/O. Между ticks logs могут превысить threshold; required oversize evidence не обрезается ради успеха.

`artifacts-manifest.json` содержит sorted retained evidence `(path,size,SHA256,mode,role,completeness)` и исключает себя, `receipt.json`, `receipt.sha256` и последующий runtime receipt. Output manifest hash использует domain `outputs`. Сохраняются raw request, origin/source/seed/toolchain/native/graph/closure/checksum records, ordinary command outputs/exit, checks, bytes artifacts, metadata и comparison/failure evidence. Checkpoint предшествует безопасному удалению work. Cleanup касается только known owned work после confirmed tree join либо явно сохраняет его. Referenced FD проверяются и закрываются, cleanup facts окончательны до immutable manifest и terminal receipt. Публикация: exclusive temp, file fsync, no-replace atomic rename, directory fsync. Sidecar — raw receipt hash, без цикла и внешней подписи.

`verified_equal` требует SAME source digest A.before/A.after/B.before/B.after, identical seed/toolchain/closure, обеих точных requested target sets, независимых empty caches, equal raw artifact hashes, join всех tool trees, полного required evidence и confirmed/intentional cleanup. `failed` — определённый joined отказ/build/mismatch. Cancel, deadline, input mutation, unknown I/O/writers/cleanup — `incomplete`; missing/unsynced receipt тоже incomplete для caller. Нет выдуманных zero hashes, exit0, успеха отсутствующей lane или success receipt до неопределённого cleanup.

### Проверки и оставшиеся допуски

Stdlib validator исполняет закрытые tables и динамические cross-field/FD authorities. Schema задаёт structural discriminants; dynamic hash equality, exact ownership и UTF-8 byte/count bounds остаются runtime gates. Tests используют отдельно bounded interpreter только выпущенного schema subset, отвергающий unknown keywords и external/cyclic refs. Fixture Git objects и Python child tools не становятся CLI authority или доказательством настоящей Go/native сборки.

Portable tests покрывают strict parser/schema, реальные own-temp FD/no-follow/FIFO/hardlink, original seed copies и corruption destination, x/mod h1 agreement, malformed/expanded ZIP, versioned/staged origin, same-source conjunction, native replacement и clean environment. Явные Linux-only tests задают будущие own-Python-child READY/GO, wrong nonce, pre-group cancel, inherited regular logs, TERM-ignoring descendants, anchor-last, output/deadline и poisoned membership. Им нужен отдельный qualified Linux run; Darwin skip не равен kernel PASS. Ниже зафиксирована native two-lane проверка конкретных входов; runtime binding и проверка финального кандидата остаются отдельными gates.

## Recorded native qualification - 2026-10-23

The Linux/arm64 pair ran on 2026-10-22 22:18:22-22:28:12 UTC
(2026-10-23 Europe/Sofia). Both sequential lanes used independent empty caches,
Go1.27.3 and CGO=1. Pinned image:
`sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e`.
The actual container had 2 CPU, 1536 MiB, no swap, 256 PID, network=none,
read-only root/inputs and only its own work RW. All 3727 owned commands exited0
and joined without signals; OOMfalse, own container removal exit0. The build CLI
took589.222 seconds within the6000-second ceiling and30-second cleanup allowance.

Inputs were base commit `c17a4961689c9e8c33bd5681ea7c7ec577bb53cf` plus seven
frozen staged harness overlays:1785 source files,54101039 B. Both lanes retained
the same source digest before/after:
`d92e7db07cbc22580f5a7afc3679f0114edc5e2839797bb85d08be66ff917a6b`.
This snapshot predates the current N-in Ready Service change. It does not qualify
the current checkout or a final release candidate.

| Target | Bytes in each lane | SHA256 of both copies |
|---|---:|---|
| fullnode | 20578567 | `4e54f28f38a5e363c54b1aa02aa62093b68bf83384806aae45da5a7eef1344b2` |
| linuxhelper | 7209223 | `d37fdc58e70d769ca6dc3796b31b296d1e19064ce2b73fd6ec054753db4054be` |
| sdkedge | 22544647 | `fc61cba988d59d2f48dcbe04251f2cce6df3b27fcc48eeeb8d416ff80306e385` |

The receipt is `verified_equal`, SHA256
`559f6a3075cae9c7d868c7540d70a5382c7e9628e1cab5cbd3646199a177b835`.
Independent TESTER32a91385 verified all14883 published records (220113084 B),
the output domain, all commands and the six artifact byte streams. Independent
REVIEWERea65253e confirmed receipt, metadata, run constraints and scope. Original
logs/work remain in the machine-local directories
`/private/tmp/corsa-D1-BuildPair-native-ROOT-v4` and
`/private/tmp/corsa-D1-BuildPair-work-ROOT-v4/build-result`. The three earlier
failed attempts remain separate unchanged evidence.

Each fresh lane writes and rechecks its own Go telemetry modefile `off\n` before
its first Go call; global user configuration stays untouched. `GOTELEMETRY=off`
is not used as configuration authority. Required installed tools are
compile/link/asm/cgo; the original supervised `go tool buildid` can use the
official pinned-GOROOT source fallback. No build ID is invented or rewritten.
Exactly two additional standard CGO flags are allowed:
`-fno-stack-protector` and `-Wdeclaration-after-statement`; path and other flag
guards remain intact.

Named target applications were never executed. Trusted compiler/Go metadata
tools ran under the original supervisor. Runtime version/executable binding,
C/ABI, kernel/helper acceptance, T5/D1 resource acceptance, G2, activation and
release remain separate.

## Current source app3 and static launcher qualification — 2026-10-23

### English

A later, separately qualified app stage covers the current C2 source projection:
1790 files, 54170860 B, source digest
`fee57a1ab37a24550b0fb3a0954a3d8300d28cbaa312029c23ba9ed6da860245`.
This is the selected SourcePlan closure of the 2042-file candidate, including
31 overlays with recorded origins; it is not the earlier 1785-file snapshot or
a hash of every checkout file. Its original two-lane CGO1 app receipt is
`verified_equal`, SHA256
`eaabd4bdad279039ec7b9be12111514d662eecfd09fa7a112b53a4fdacc4bb0d`.
Independent TESTER and REVIEWER accept this app stage only. The enclosing
attempt later exhausted container memory during test compilation and remains
FAILED; neither `node.test` nor `loadstand.test` was accepted. App PASS does
not promote that whole attempt or establish a single successful five-output run.

| Retained artifact | Bytes | SHA256 |
|---|---:|---|
| fullnode, identical in both app lanes | 20578567 | `fbcfec5c08d6e137b093b6963d028db43f35088cc7ca63703300972cf5e5d390` |
| linuxhelper, identical in both app lanes | 7209223 | `0a95be25e715ed6f2fba1ce618d88de6684eecec0c56413f6d4809552d15054a` |
| sdkedge, identical in both app lanes | 22610183 | `1fc8d8be8eec0a18c9b410cff63353810c7e7af05470416093bb7462c11f825f` |
| guest-restrict, separate C compile | 67184 | `0b16c410e68f46dc9c812f809fc24c8a027a0c0420b97bccdd7cf35c566ff703` |

The launcher has separate compile-only qualification: one static GCC command
with the complete signed Alpine `linux-headers` 7.0.0-r1 input, followed by an
independent retained-file check after its own container was removed. It is an
ELF64 little-endian AArch64 file with no PT_INTERP or DT_NEEDED. It was not
executed. Static linkage and those headers do not prove compatibility with the
6.18.58 guest, nor seccomp, capabilities, namespaces, application behavior,
T5/resource acceptance, G2, activation or release. The app binaries were not
executed either. Retrying test compilation or assembling a guest needs its own
recorded inputs and result; neither is claimed here.

### Русский

Отдельно принятый app stage относится к текущей C2-проекции: 1790 файлов,
54170860 B, digest `fee57a1ab37a24550b0fb3a0954a3d8300d28cbaa312029c23ba9ed6da860245`.
Это выбранный SourcePlan closure кандидата из 2042 файлов с 31 overlay и
записанными origins, а не прежний snapshot1785 или hash всего checkout.
Оригинальный CGO1 app receipt двух независимых lanes имеет `verified_equal`
и SHA256 `eaabd4bdad279039ec7b9be12111514d662eecfd09fa7a112b53a4fdacc4bb0d`;
bytes/SHA256 трёх равных пар приведены в таблице выше. TESTER и REVIEWER приняли
только app stage. Позднее контейнер исчерпал память при test compile: вся попытка
остаётся FAILED, принятых `node.test`/`loadstand.test` нет. App PASS не означает
успешную общую сборку пяти outputs.

`guest-restrict`67184 B принят отдельно только как результат одной static GCC
команды с полным signed Alpine `linux-headers`7.0.0-r1 input и независимой
проверки retained файла после удаления своего контейнера. ELF64LE/AArch64 без
PT_INTERP/DT_NEEDED не запускался, как и app binaries. Это не подтверждение ABI
гостя6.18.58, seccomp/caps/namespaces, поведения приложений, T5/resources, G2,
activation или release. Новая test compile и guest assembly требуют собственных
входов и результатов; их успех здесь не заявлен.

### Machine-local evidence / локальные исходные свидетельства

The immutable handoffs below retain the scoped verdicts and link to original
RAW, receipts and artifact checks. These paths identify retained local evidence,
not repository files or portable public downloads. / Ниже указаны immutable
handoffs с точными scoped verdicts и ссылками на исходные RAW/receipts; это
локальные свидетельства, не файлы репозитория и не публичные downloads.

- App3 TESTER: `/private/tmp/corsa-D1-current-five-second-OOM-appstage-independent-TESTER-DATA-v1/HANDOFF.json`; SHA256 `2e8d879a2da00f3f984ba6ed68f58d91d3948a21b719c5fd3ea51656730390e7`.

- App3 REVIEWER: `/private/tmp/corsa-D1-current-five-OOM-appstage-REVIEWER-DATA-v1/HANDOFF.json`; SHA256 `4241fdf7af4cc21ab7730e9735bbab4b059c19c4e13aee94dc42d87ccba00f43`.

- Static launcher TESTER: `/private/tmp/corsa-D1-guest-restrict-static-independent-TESTER-DATA-v1/HANDOFF.json`; SHA256 `78ed676b391d3a9438001586f433d5692574d28ace601be4527c80acfe63f7fa`.

- Static launcher REVIEWER: `/private/tmp/corsa-D1-guest-restrict-static-final-REVIEWER-DATA-v1/HANDOFF.json`; SHA256 `4de22373acea9d9fbaa41b43e87154c1c8dd422fa2e4f54db1f950e29322aa9a`.

## Current source test-only qualification — 2026-10-23

### English

A subsequent offline attempt compiled `node.test` and `loadstand.test` from
SourcePlan1790/`fee57a1a…` in one fresh Linux/arm64 CGO1 lane. All 1958 commands
completed with exit0 and joined; OOM was false and owned-container removal
completed with exit0. TESTER and REVIEWER accepted 7825 published records
(125893889 B), the retained source, artifacts and static ELF dependencies.

| Test artifact | Bytes | SHA256 |
|---|---:|---|
| node.test | 45482247 | `683eb0fa3c3a1d810f2e06c72603ea8a2515659d5f3528a033cbbd8d72868d3b` |
| loadstand.test | 15073543 | `feb02ebb7d01b1aaea6b124e861276a9984d9a19ec735501dc9c7bf1ace8f21d` |

The container retained the original 2CPU/1536MiB/no-swap/256PID limits and
6000s plus 30s cleanup budget. The test lane used `-p=1`, `GOMAXPROCS=1`,
`GOGC=50`, `GOMEMLIMIT=768MiB`; GC tuning is a soft per-Go-runtime policy.
The prior accepted app3 request, receipt and complete published evidence were
imported read-only, without rebuilding applications or using the failed cache.
Both complete schema2 native manifests and domains were retained. Their 4573
records matched; the full domain hashes happened to be equal and the computed
`same_native_capture` field was true. The container IDs differ. The private
receipt explicitly keeps `original_single_CID_five_output_proof=false`,
`comparison=not_requested`: test2 is a single-lane compile result, while app3
retains its separate two-lane equality receipt. The old whole OOM attempt
remains FAILED.

Both test ELFs request `/lib/ld-musl-aarch64.so.1` and need only
`libc.musl-aarch64.so.1`; the retained loader is 723480 B, SHA256
`53282acc2be8a08a9fdcf880334bf35bbfd0a13862b7f3616be3919fcda11793`.
No target or test binary was executed. Runtime ABI, guest/kernel acceptance,
T5/resource acceptance, G2, activation and release remain open.

### Русский

Следующая offline-попытка собрала `node.test` и `loadstand.test` из той же
SourcePlan1790/`fee57a1a…` в одной свежей Linux/arm64 CGO1 lane. Все 1958 команд
завершились exit0/joined, OOMfalse, свой контейнер удалён с exit0. TESTER и
REVIEWER приняли 7825 published records/125893889 B, retained source, оба ELF
из таблицы и их static dependency closure. Лимиты контейнера прежние:
2CPU/1536MiB/no-swap/256PID, общий срок6000с плюс30с очистки. Для Go test lane
заданы `-p=1`, `GOMAXPROCS=1`, `GOGC=50`, `GOMEMLIMIT=768MiB`; это мягкая
настройка Go GC, а не новый жёсткий предел памяти.

Прежние принятые app3 request/receipt и все published свидетельства импортированы
read-only, без пересборки приложений и failed cache. Полные native schema2
manifests/domains сохранены: 4573 записи совпали, полные domain hashes
равны, computed `same_native_capture=true`. CID разные; private receipt сохраняет
`original_single_CID_five_output_proof=false` и `comparison=not_requested`.
Test2 принят как single-lane compile, app3 сохраняет отдельное свидетельство
двух равных lanes. Прежняя whole OOM попытка остаётся FAILED.

Оба test ELF используют указанные выше loader/libc. Ни target, ни test binary
не запускались. Runtime ABI, guest/kernel, T5/resources, G2, activation и release
этим не приняты.

### Machine-local evidence / локальные свидетельства

- TESTER: `/private/tmp/corsa-D1-current-test-only-native-independent-TESTER-DATA-v1/HANDOFF.json`; SHA256 `2d13a98cfb2fba8e3a5c2e5dbbddc690b1992a3e5144e3bcfeeb745b18ff0b03`.

- REVIEWER: `/private/tmp/corsa-D1-current-test-only-final-REVIEWER-DATA-v1/HANDOFF.json`; SHA256 `b37df663eaf064b9012401b330c9cc1c9b6f64a4c884096f34ac7596ffaa9c23`.

## Current source offline guest archive — 2026-10-23

### English

An offline Python transformation assembled the accepted SourcePlan1790/`fee57a1a…`
application and test inputs, static launcher and pinned base initramfs into one
single-member gzip/newc archive. It preserves the base kernel and dependencies,
replaces PID1 and adds the fixed fixtures. The resulting archive has 1691 entries,
79201712 B and SHA256
`4f5d0164078442defaf9286ad320d8c96048a14eb8a42d9ac1e000d09a488b4b`.
The unchanged kernel Image is 36306944 B, SHA256
`256d5fff4bb892edcb9218c1e6353e388bc3646e65f76c2bc9d958b635d1b00e`.

The transform ran under the heavy-run lock with 1CPU/256MiB/no-swap/64PID,
network none, UID501:20, all capabilities dropped, no-new-privileges, read-only
root, two owned binds and a 64MiB RAM tmpfs. The internal lifecycle deadline was
120s plus 10s cleanup, with a separate parent fence of 130s (125s initial wait
and 5s termination/wait reserve); its direct driver completed normally in 13.59s. All eight
Docker CLI commands completed with exit0, OOM was false and owned-container
removal completed with exit0. Original retained host file descriptors then
verified exactly five outputs: Image, root.cpio.gz, run.sh, runtime.sha256 and
assembly-result.json. The conservative root charge is 174358528 B; observed
input/output files at the driver accounting checkpoint total 299962709 B. These are encoding/accounting bounds,
not measured guest peak RAM, a disk quota or resource acceptance.

The static recursive dependency declaration contains nine existing library
paths, with exact resolved payload hashes and alias traces. No target ELF or loader was executed;
no downloading or compilation occurred during this transformation. Assembly does not establish
installed seccomp, guest ABI, namespace/kernel behavior, T5, G2 or release.
The inputs remain SourcePlan1790, distinct from the later MAIN2047 inactive
Timing changes; the previous whole native OOM attempt remains FAILED. Image
packaging and guest application execution have their own evidence boundaries.

### Русский

Офлайн Python-преобразование объединило принятые app/test inputs из
SourcePlan1790/`fee57a1a…`, статический launcher и закреплённый базовый initramfs
в один gzip/newc архив. Ядро и зависимости базы сохранены, PID1 заменён,
добавлены фиксированные fixtures. Архив содержит1691 запись,79201712байт;
SHA256 приведён выше. Kernel Image не изменён:36306944байта с указанным SHA256.

Запуск выполнялся под heavy-run lock:1CPU/256MiB/no-swap/64PID, без сети,
UID501:20, cap-dropALL, no-new-privileges, read-only root, два своих bind и
RAM tmpfs64MiB. Внутренняя граница120с плюс10с очистки; отдельный внешний fence130с
состоит из125с initial wait и5с termination/wait reserve. Driver завершился
нормально за13,59с. Все восемь Docker CLI команд завершились exit0, OOMfalse, свой
контейнер удалён с exit0. Затем исходные сохранённые hostFD проверили ровно
пять outputs из списка выше. Conservative root charge174358528байт и
наблюдаемый на accounting checkpoint размер input/output299962709байт — границы кодирования и
учёта, а не измеренный пик RAM гостя, дисковая квота или ресурсная приёмка.

Статическое рекурсивное описание зависимостей включает девять уже имеющихся
путей библиотек, точные hashes разрешённых payloads и цепочки alias. Target ELF
и loader не исполнялись; при преобразовании скачиваний и компиляций нет. Установленный
seccomp, ABI гостя, namespaces/kernel, T5, G2 и выпуск этим не приняты.
Входы SourcePlan1790 отличаются от последующего MAIN2047 с неактивным Timing;
прежняя whole native OOM попытка остаётся FAILED. Упаковка образа и исполнение
приложений в госте требуют отдельных свидетельств.

### Machine-local evidence / локальные свидетельства

- TESTER: `/private/tmp/corsa-D1-offline-runtime-TESTER-DATA-v1/HANDOFF.TESTER.json`; SHA256 `4f5b36a67ee22c76728a712c776bca04eff9fdf4e7f7456d2b1a46dda5a8716c`.

- REVIEWER: `/private/tmp/corsa-D1-offline-assembly-final-REVIEWER-DATA-v1/HANDOFF.json`; SHA256 `ab2773a51e95006d715972fba2e930b5308e146214004e3bfc075c4e43278280`.


## Offline image packaging qualification — 2026-10-23

### English

The derived raw rootfs tar is 643110400 B, SHA256
`6bd339ae503a21a4e02423e8d7d9aaf8228187ef3266541cbb01ba453cfecadd`.
Independent comparison verified the same ordered 4929 entries and 1024-byte
zero tail. All 4925 unselected raw records are byte-identical to the pinned base.
Four selected replacement operations produced two actual changes:
`opt/probe/root.cpio.gz` and `opt/probe/runtime.sha256`. Image and run.sh remain
identical complete raw records. Original root ownership and modes 0644/run0755
are preserved. The embedded old assembly-result.json remains historical; the
current external receipt is SHA256 `ab0121daddba644efdad04b3fc59f92037dfbd65b4594ecbd886cade613e094c`.

The tar transformation completed normally in 4.91s with the same
1CPU/256MiB/no-swap/64PID offline container profile and owned removal0.
The retained original host work FD then verified all three outputs. Observed
owned input/output bytes at its accounting checkpoint were 1361260336 B;
this is not a quota or guest peak-memory measurement.

One subsequent local Docker import completed normally in 5.37s. The accepted
metadata identifies Linux/arm64 image
`sha256:3127a1f1cd0ab481e412f6debae477a6ebd5b9f1498a56181d6be5090b37c1a2`,
size 639610796 B, with one RootFS diffID equal to the raw tar SHA256 above.
Config contains exactly the owner/source/provenance labels; there is no default
Cmd, Entrypoint or Env. This is a newly imported single layer. Its base-image
label records content provenance, not Docker parent-image inheritance.

The import used the pinned local desktop-linux UNIX context. Four closed CLI
results were 0/1/0/0: context query, expected fresh-tag absence, import and final
inspection. Import has a 30s closed-child allowance and each metadata query 6s,
inside a 60s phase and separate 130s parent fence. These time limits do not cap
Docker-daemon CPU or RAM. Original tar FD identity and full SHA were rechecked
after import. The first attempt remains FAILED before import: Docker emitted
one LF on absent-tag stdout, which the initial empty-only predicate refused.
The second attempt accepts only empty or one-LF stdout with the same exact
normal exit1 and own-tag NoSuchImage error checks.

TESTER and REVIEWER accepted the TAR and metadata scopes separately. No image
or VM was started; no target ELF ran. This remains SourcePlan1790/`fee57a1a…`,
not execution of MAIN2047. Guest ABI, kernel tests, T5/resource acceptance,
G2, activation and release remain open.

### Русский

Производный raw rootfs tar:643110400байт, SHA256 указан выше. Независимое
сравнение подтвердило4929 записей в прежнем порядке и нулевой tail1024байта.
Все4925 невыбранных rawrecords сохранены побайтово. Четыре операции замены
дали две фактически изменённые записи: root.cpio.gz и runtime.sha256.
Полные rawrecords Image и run.sh совпадают с базой. Прежние root ownership
и modes0644/run0755 сохранены. Встроенный старый assembly-result.json остаётся
историческим; текущая внешняя квитанция имеет SHAab0121dadd…, приведённый выше.

Tar-преобразование завершилось нормально за4,91с при прежнем offline-профиле
1CPU/256MiB/no-swap/64PID; свой контейнер удалён с exit0. Затем исходный
сохранённый host workFD проверил все три outputs. На accounting checkpoint
наблюдалось1361260336байт своих input/output; это не квота и не пик RAM гостя.

Следующий локальный Docker import завершился нормально за5,37с. Принятый
Linux/arm64 image ID и размер639610796байт приведены выше; единственный RootFS
diffID равен SHA rawtar. Config содержит только точные owner/source/provenance
labels, без Cmd/Entrypoint/Env. Это новый импортированный слой; base-image label
означает происхождение содержимого, а не Docker parent inheritance.

Закреплён local desktop-linux UNIX context. Четыре закрытые CLI дали0/1/0/0:
context query, ожидаемое отсутствие fresh tag, import и итоговый inspect.
Closed-child allowance для import30с, для metadata query6с; внутренняя фаза60с,
отдельный внешний fence130с. CPU/RAM Docker daemon этим не ограничены.
После import повторно проверены исходный tarFD, identity и полный SHA.
Первая попытка остаётся FAILED до import: Docker вывел один LF в stdout
отсутствующего тега, а исходный predicate принимал только пустой вывод.
Вторая принимает только empty/oneLF с прежними точными normal exit1 и
сообщением NoSuchImage своего тега.

TESTER и REVIEWER приняли TAR и metadata отдельно. Image/VM не запускались,
target ELF не исполнялись. SourcePlan1790/fee57 отличается от MAIN2047;
ABI гостя, kernel tests, T5/resources, G2, activation и release ещё не приняты.

### Machine-local evidence / локальные свидетельства

- TAR TESTER: `/private/tmp/corsa-D1-offline-retar-TESTER-DATA-v1/HANDOFF.TESTER.json`; SHA256 `822b339867f8ec455035edb139a95a1559e8b19d518464d7f3b769a6f0a6f3df`.

- TAR REVIEWER: `/private/tmp/corsa-D1-offline-retar-final-REVIEWER-DATA-v1/HANDOFF.json`; SHA256 `8ef803a5e6828dd39a1275e3d6acd21ded47b10b3fec668fe0be3a1e47cda792`.

- Image TESTER: `/private/tmp/corsa-D1-offline-retar-import-attempt2-TESTER-DATA-v1/HANDOFF.TESTER.json`; SHA256 `c039c6ae13f6cfde4613ba83c280c69d6ef63c00dd9dd8ebf4da3dfd7b0fc0cb`.

- Image REVIEWER: `/private/tmp/corsa-D1-offline-image-import-final-REVIEWER-DATA-v1/HANDOFF.json`; SHA256 `8e512c08b392c56ee72becabb0de1eaaaa438092854a67b98e1d38b6cbf8222e`.


## 2026-10-23 — repaired guest package and first original fixture failure

### English

The first fixed-fixture VM attempt failed before Go: the sourced payload checker
called a missing `stat` alias. Seven missing aliases were changed to explicit
BusyBox commands. Static review also found an incorrect postflight assumption:
`AuditedProfileParent.Close()` closes its descriptor and retains its directory.
The repaired oracle expects one retained root:root/01777 parent on each original
mutable mount after the two successful fixture subcases, plus the immutable
`/profiles/fixtures` bind. It checks bounded top-level names, owners, modes and
original devices; it does not delete profiles or infer that their contents are
empty. Original namespace, process, FD, hash, mount and nonlazy-unmount checks
remain. This repaired postflight has not yet been reached by a successful test.

Independent DATA audits accepted the repaired archive (79203308 B,
SHA256 `35f032b285f1a646be07b55cb8ffe09a73e1608c378e7e6438d6a36b306b95fb`)
and raw tar (643111936 B,
SHA256 `cfd7cb31a04346ec9619bf4080c0b06a06d20b39561240f0b2260c1f9a082a71`).
The tar preserves all 4925 unselected records of the original 4929-entry stream.
Metadata-only import then completed normally in 5.42s: Linux/arm64 image
`sha256:6f8fd12299bfd5d57dbd7180006221d0d81a5652c71bb6debeebac3b3869872d`,
639612392 B, one diffID equal to the new tar SHA, labels-only Config.

The second fixed-fixture VM attempt was a **functional FAIL**, closed/reaped in
5.88s. Kernel 6.18.58 boot, distinct lo-only namespaces, nft operations and six
payload hashes passed. The restricted Go runner reached exactly the original
parent and `//profiles`; both failed at `linux_native_test.go:87` with
`actual helper/target initialization refused owned Linux fixture unavailable`.
`//unsupported`, retained-profile postflight and fixture PASS were not reached.
The available RAW cannot distinguish a pre-exec Dup/Start refusal from a
correlated helper refusal while creating the private profile. No filesystem or
seccomp cause is established. QEMU/container exit0 and no OOM do not override
this functional FAIL; removal of the exact owned container completed with0.

The VM profile remained 1 CPU, container1536MiB/no swap/64 PID, guest768MiB,
180s plus10s cleanup, offline and without privileged/KVM/host mounts. This is
SourcePlan1790/fee57 payload evidence, distinct from current MAIN2047. Both VM
failures remain recorded. No complete kernel7/Member4, T5/resource, G2,
activation or release acceptance is claimed.

### Русский

Первая fixed-fixture VM остановилась до Go: sourced checker вызвал отсутствующий
alias `stat`. Семь aliases заменены явными BusyBox-командами. Также исправлена
предпосылка postflight: `AuditedProfileParent.Close()` закрывает FD и сохраняет
каталог. После двух успешных subcases ожидается по одному root:root/01777 parent
на каждом исходном mutable mount, плюс неизменный bind `/profiles/fixtures`.
Проверка ограничена верхним уровнем, именами, владельцами, modes и исходными
устройствами; профили не удаляются, пустота их содержимого не обещается.
Прежние namespace/process/FD/hash/mount/nonlazy-unmount guards сохранены.
Успешный тест ещё не дошёл до этого исправленного postflight.

Независимо приняты DATA архива79203308байт/35f032b2 и rawtar643111936байт/cfd7cb31
с полными SHA выше. Сохранены все4925 невыбранных записей исходного stream4929.
Metadata-only import за5,42с дал Linux/arm64 image6f8fd122/639612392байта,
один diffID новогоtar и Config только с labels.

Вторая fixed-fixture VM — **functional FAIL**, closed/reaped за5,88с.
Boot6.18.58, разные lo-only namespaces, nft и шесть SHA payload прошли.
Restricted Go runner запустил исходный parent и//profiles; оба отказали
на native87 с приведённым выше generic initialization error. //unsupported,
retained-profile postflight и fixture PASS не достигнуты. RAW не различает
pre-exec Dup/Start и correlated отказ helper при private-profile creation;
причина filesystem/seccomp не доказана. QEMU/container exit0/noOOM не заменяет
functional FAIL; точный свой контейнер удалён с0.

Профиль VM прежний:1CPU, контейнер1536MiB/no-swap/64PID, гость768MiB,
180с+10с очистки, offline, без privileged/KVM/host mounts. Это payload
SourcePlan1790/fee57, отличный от MAIN2047. Оба исходных отказа сохранены;
kernel7/Member4/T5/resources/G2/activation/release ещё не приняты.

### Machine-local evidence / локальные свидетельства

- Archive: `/private/tmp/corsa-D1-profile-postflight-archive-final-ROOT-v3/HANDOFF.json`.
- TAR TESTER64bc079a and REVIEWER4f29e3f0: `/private/tmp/corsa-D1-profile-postflight-retar-final-ROOT-v3/HANDOFF.json`.
- Import TESTERf525118a and REVIEWER7aadde27: `/private/tmp/corsa-D1-profile-postflight-image-import-final-ROOT-v3/HANDOFF.json`.
- VM1 failure: `/private/tmp/corsa-D1-fixed-guest-fixture-VM-attempt1-ROOT-v1/HANDOFF.json`.
- VM2 failure ROOTf541773a: `/private/tmp/corsa-D1-profile-postflight-fixture-VM-failure-ROOT-v3/HANDOFF.json`.
- VM2 failure TESTER4dc0cb80: `/private/tmp/corsa-D1-profile-postflight-fixture-VM-failure-TESTER-DATA-v3/HANDOFF.TESTER.json`.

## 2026-10-23: rootfs admission and canonical fixture / допуск rootfs и исходный fixture

### English

A separate diagnostic variant rebuilt only the parent test binary and helper
with CGO disabled. Its failure log reported `stage=prepare-private-profile`,
`errno=0`, `component=2`, `violation=6`: the original secretfile audit rejected a
directory as `StickyChildNotOwned`. Zero means no errno was extracted from the
logged cause. The exact ancestor/LevelsAbove was not logged. This diagnostic
variant is not equivalent to the canonical five CGO1 inputs; its failure and the
previous canonical failures remain recorded.

The archive contains no root-directory record. The guest bootstrap now checks
literal `/` as a root:root directory with initial mode exactly0755 or01777,
sets0755, and verifies the result before starting the original fixture. Unknown
owners, groups or modes stop the attempt. It changes no production secretfile
rule, helper protocol, capability policy, selector, deadline or postflight.
The actual VM observed `guest-root-bootstrap before=0:0:1777` and
`guest-root-bootstrap after=0:0:755`. A root-owned component under a shared
sticky parent is precisely the condition the original audit rejects for the
unprivileged helper. The earlier generic canonical RAW alone does not identify
that ancestor retrospectively.

With this bootstrap and the unchanged five canonical CGO1 ELF payloads from
SourcePlan1790/fee57, the original `TestLinuxOwnedFixtureNative` completed with
3 RUN/3 PASS: parent, `//profiles`, `//unsupported`. The retained-profile
postflight was reached: one root:root/01777 parent on each mutable mount plus
the original read-only fixture bind. Namespace, process/FD, nft/module, payload
hash, mount and nonlazy-unmount guards passed. The fixed VM controller returned
normally with0; the exact owned container was removed with0, without OOM.
This is one functional fixture result, distinct from current MAIN2048. It is
not kernel7/Member4, whole T5, resource, G2, activation or release acceptance.

### Русский

Отдельный диагностический вариант пересобрал только test binary и helper с
CGO0. Получен исходный отказ secretfile: `prepare-private-profile`, directory
`StickyChildNotOwned` (component2/violation6). `errno=0` означает отсутствие
извлечённого errno; точный ancestor/LevelsAbove не выводился. Этот вариант не
эквивалентен пяти canonical CGO1 inputs; все прежние отказы сохранены.

В архиве нет записи корневого каталога. Новый bootstrap проверяет только
буквальный `/`: root:root, исходные0755 или01777; устанавливает0755 и проверяет
результат до fixture. Неизвестные owner/group/mode дают STOP. Production-аудит,
wire/helper, caps, selectors, deadlines и прежние postflight не ослаблены.
Actual VM показала `0:0:1777` до и `0:0:755` после настройки. Root-owned компонент
под общим sticky parent соответствует условию исходного отказа для непривилегированного
helper; старый generic canonical RAW сам по себе не определяет этот ancestor.

С теми же пятью CGO1 ELF SourcePlan1790/fee57 исходный fixture завершился
3 RUN/3 PASS (parent, //profiles, //unsupported). Достигнут retained-profile
postflight: по одному root:root/01777 parent на двух mutable mounts и прежний
read-only fixture bind. Namespace/process/FD/nft/module/hash/mount и nonlazy
unmount guards прошли. Controller normal0, точный свой CID удалён с0, OOM нет.
Это один functional fixture, отличный от MAIN2048; kernel7/Member4/T5/resources,
G2, activation и release этим не принимаются.

### Machine-local evidence / локальные свидетельства

- Diagnostic failure: `/private/tmp/corsa-D1-profile-errno-diagnostic-fixture-VM-failure-ROOT-v1/HANDOFF-final.json`.
- Bootstrap Source: `/private/tmp/corsa-D1-guest-root-bootstrap-SOURCE-v1/HANDOFF.json`.
- Canonical fixture result: `/private/tmp/corsa-D1-root-bootstrap-fixture-VM-final-ROOT-v1/HANDOFF.json`.
- New init SHA256: `08c6b4c8e372efee690c80384c152b2a3c0038fd0c84baa2cc0a4d948e302412`.

- Fixture TESTER: `/private/tmp/corsa-D1-root-bootstrap-fixture-VM-TESTER-DATA-v1/HANDOFF.TESTER.json` (SHA256 `e9c24ec63fcd53090d72052001f5d5ef443d7c8b3cfc3a4302a797bd985aff33`).
- Fixture REVIEWER: `/private/tmp/corsa-D1-root-bootstrap-fixture-VM-REVIEWER-DATA-v1/HANDOFF.json` (SHA256 `6b41453a668cb16dbc01f633536fac5023705986dfbfa7bda2a9ae2c5b61b291`).

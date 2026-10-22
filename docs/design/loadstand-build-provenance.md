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

One to three unique requested targets are all mandatory. Missing source is `TargetSourceMissing`, never a stub, moving checkout or smaller target set.

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

Portable tests cover strict parser/schema, real own-temp FD/no-follow/FIFO/hardlink behavior, original seed copies and destination corruption, x/mod h1 agreement, malformed/expanded ZIPs, versioned/staged origin, same-source conjunction, native link replacement and clean environment. Explicit Linux-only tests exercise future owned Python child READY/GO, wrong nonce, pre-group cancellation, inherited regular logs, TERM-ignoring descendants, anchor-last collection, output/deadline and poisoned membership. They require a separately qualified Linux run; Darwin skip is not a passed kernel check. Two actual independent Go builds and subsequent runtime binding remain separate pending gates.

---

## Русский

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

Portable tests покрывают strict parser/schema, реальные own-temp FD/no-follow/FIFO/hardlink, original seed copies и corruption destination, x/mod h1 agreement, malformed/expanded ZIP, versioned/staged origin, same-source conjunction, native replacement и clean environment. Явные Linux-only tests задают будущие own-Python-child READY/GO, wrong nonce, pre-group cancel, inherited regular logs, TERM-ignoring descendants, anchor-last, output/deadline и poisoned membership. Им нужен отдельный qualified Linux run; Darwin skip не равен kernel PASS. Два реальных независимых Go build и последующий runtime binding остаются отдельными pending gates.

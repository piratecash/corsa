# Releases

## Interim 2.5.71 publication — 2026-10-30

The owner requested deployment of **2.5.71 before completing S6**. This one
publication uses the separate, content-bound policy in
`packaging/interim-release-v2.5.71.json`. It does not complete the ordinary
17-condition S6 manifest or grant D1/G2 activation. The historical S6 requirements
below remain the normal policy for subsequent releases and tagless Android builds.

This exception applies only to explicit `RELEASE_TAG=v2.5.71`, release 2.5.71,
protocol 31, minimum 28 and secure-session activation 31. Its approval lists the
actual outstanding and deferred conditions; any change to that list refuses.
Independent cryptographic review, final latency/load measurements, unsafe-store
final-candidate checks, measured loopback budgets, owner session choices,
observation plan, final-candidate checks and release-note acceptance remain
uncompleted S6 obligations. Native Windows/DACL and Tor remain UNVERIFIED.
TLSv2, pins, downgrade protections and storage schema 8 remain active.

The approval binds the canonical SHA-256 of every tracked path, executable mode
and raw file body, excluding only the approval file itself to avoid recursive
hashing. The approval must itself be committed, regular and unchanged. Worktree
bytes and modes, the index, untracked inputs, HEAD, the peeled tag and GitHub event
must agree. Only the existing `.gocache/` and `.gomodcache/` caches are excluded
from the untracked-input check. A changed source or workflow needs a new digest
and owner review. The tool reports the actual full commit and **S6 admitted: false**.

**Owner's updated scope — Release workflow for 2.5.71 only:** publication requires
the exact content-bound admission gate, lint and all five daemon + Desktop
platform builds. This Release workflow runs **no product tests, Python preflight
tests or CGO SQLite tests**. Ordinary Checks still run independently; their test
results are not a publication dependency for this version. Subsequent versions
retain the normal mandatory release tests, Python controls and CGO checks. This
specific exception supersedes earlier testing requirements below only for the
2.5.71 Release workflow; skipped tests are not PASS and the full suite is not green.

Checks runs on every push to `dev` and `master`, and on pull requests. Its Tests
job runs independently of lint, so a lint failure does not prevent the tests from
starting. Each push checks its final commit; a push containing several commits
does not create a separate run for each intermediate commit.

The earlier `test-release` selector scope remains unchanged:
`internal/overlaysim` is deferred to D1, never PASS. For source version
**2.5.71 only**, exactly two inactive D1 diagnostics are also deferred:
`TestOwnedReadyTargetTimingFourGenuineSources` and
`TestOwnedReadyNormalTimedFactoryLoadsOriginalAndSignsWithoutGate`.
The first failed before its target read gate with a lawful `bridge domain: busy`
refusal; the second has no separately established passing result in that run.
Both remain required for D1 repair and acceptance. When invoked outside this
Release workflow, `test-release` still reports them as DEFERRED, runs every other
Node test, and passes no skip option to other packages. Other source versions and
ordinary `make test` / `make test-all` retain both tests. TLSv2, pins and storage
schema 8 remain active; this publication exception grants no D1/G2 activation.
Android is separate. These are requirements, not claims that the new commit has
passed them. The owner-reported file-transfer failure remains unresolved: this
test exception neither fixes it nor records it as PASS.

The owner reviews and commits the versioned approval with this disclosed scope,
then explicitly selects that commit for the tag. Re-running an older failed tag
cannot load newer source. Agents do not move tags or publish the release.

**Промежуточный выпуск 2.5.71:** это отдельный допуск до завершения S6.
Незавершённые условия не получают PASS; криптоэкспертиза и итоговая ресурсная
приёмка остаются открытыми. Действующие TLSv2/pins и ограничения отката schema 8
сохраняются. Владелец подтверждает конкретное содержимое своим коммитом и выбором
тега после просмотра этого ограничения. Будущие версии возвращаются к обычному
допуску S6.

По уточнённому решению владельца только workflow **Release для 2.5.71** требует
точный content-bound admission gate, lint и все пять платформенных сборок
демона + Desktop, но **не запускает продуктовые тесты, Python preflight-тесты
или CGO SQLite-тесты**. Обычные Checks продолжают выполняться независимо;
результаты их тестов не блокируют публикацию этой версии. Для последующих версий
обязательные release-тесты, Python controls и CGO-проверки сохраняются. Это
исключение заменяет прежние требования к тестированию ниже только для Release
2.5.71: пропуск не означает PASS, полный набор тестов не зелёный.

Checks запускается на каждый push в `dev` и `master`, а также для pull requests.
Задача Tests не зависит от lint: ошибка lint не отменяет запуск тестов. Проверяется
последний коммит push; несколько коммитов в одном push не создают отдельные
запуски для каждого промежуточного коммита.

Прежние отсрочки `test-release` до ремонта D1 сохраняются: overlaysim и, только
для версии исходников 2.5.71, два названных выше inactive D1 diagnostics получают
DEFERRED, не PASS. При отдельном запуске `test-release` выполняет остальные
Node-тесты и не передаёт skip другим пакетам; другие версии и обычные `make test`
/ `make test-all` сохраняют оба теста. Активация D1/G2 не разрешается. Сообщённая
владельцем ошибка передачи файлов остаётся неисправленной; исключение из тестов
не устраняет её и не превращает в PASS. Android по-прежнему отдельно.

> **Current S6 release scope — 2026-10-29.** The
> [urgent full-product scope](operations/urgent-full-product-release-scope.md)
> covers current DEV, daemon and clients. The owner deferred session
> K2/R1b/R2/N-in activation to the next release and accepted protocol-only
> compatibility with selected PirateCash 071855, satisfied by source 31≥31.
> The selected matrix is daemon+Desktop on Linux amd64, macOS arm64/amd64
> and Windows amd64/arm64; Android APK/AAB follow separately. Only overlaysim
> is DEFERRED to D1, not PASS; generic make test-all is not green. All other
> applicable emitted-product/routing/TLS/pin tests and builds remain required.
> D1/G2 activation is outside this S6 release; older G2 requirements below
> concern that separate activation boundary. Applicable conditions in the
> 17-condition gate, independent crypto review, the full suite and final
> release admission remain fail-closed until accepted evidence and owner
> decision; recorded Windows/Tor deferrals stay UNVERIFIED.
>
> **Текущая область S6 — 2026-10-29.**
> [Срочный полный релиз](operations/urgent-full-product-release-scope.ru.md)
> включает текущий DEV, демон и клиенты. Владелец перенёс активацию session
> K2/R1b/R2/N-in на следующий релиз; для выбранного PirateCash 071855 принят
> только протокольный критерий, выполненный проверкой исходников 31≥31.
> Матрица — демон+Desktop на Linux amd64, macOS arm64/amd64 и Windows
> amd64/arm64; Android APK/AAB позже отдельно. Только overlaysim DEFERRED
> до D1, не PASS; общий make test-all не зелёный. Все остальные применимые
> тесты выпускаемых продуктов, routing/TLS/pins и сборки сохраняются.
> Активация D1/G2 вне этого S6-релиза; прежние требования G2 ниже относятся
> к отдельной границе активации. Применимые условия gate из 17 пунктов,
> независимая crypto review, полный набор тестов и итоговый допуск остаются
> закрытыми до принятия свидетельств и решения владельца; записанные
> отсрочки Windows/Tor остаются UNVERIFIED.

## Current verification scope / Текущая область проверки — 2026-10-23

The [later owner decision](operations/d1-release-decision-proposal.md#measurement-scope-2026-10-23) defers real Windows/DACL and Tor measurements while work proceeds on macOS/Linux. Earlier unconditional Windows/Tor release prerequisites below are superseded only in that scope; the historical results are retained. Windows and Tor remain UNVERIFIED/UNKNOWN, never PASS. Schema 2 retains all 17 conditions: 7 done, 2 partial, 7 not_done and 1 deferred (`tor_phase_measurement`). The unsafe-store composite remains partial; clearnet/real-old-release first-contact timing and timeout derivation remain required. Proxy/no-direct-fallback, authentication, independent crypto review, other tests, G2 and separate release decisions remain required. The gate is closed. Any Tor observation role remains deferred; macOS/Linux data establishes no Tor threshold or guarantee.

[Последнее решение владельца](operations/d1-release-decision-proposal.md#measurement-scope-2026-10-23) переносит реальные Windows/DACL и Tor замеры; работа идёт на macOS/Linux. Прежние безусловные требования Windows/Tor ниже заменены только в этой области, исторические результаты сохранены. Фактический статус — UNVERIFIED/UNKNOWN, не PASS. Schema 2 сохраняет 17 условий: 7 done, 2 partial, 7 not_done и 1 deferred (`tor_phase_measurement`). Условие небезопасного хранилища остаётся partial; clearnet/настоящие старые выпуски, замеры первого контакта и вывод timeout остаются обязательными. Proxy/no-direct-fallback, аутентификация, независимая криптоэкспертиза, остальные проверки, G2 и отдельные решения о выпуске сохраняются. Gate закрыт. Tor-роль наблюдения отложена; данные macOS/Linux не доказывают пороги или гарантии Tor.

## English

### What publishes a release

| Path | What it publishes | Gated |
|---|---|---|
| `.github/workflows/release.yml`, on a pushed `v*` tag | binaries for every platform as workflow artifacts, then a GitHub release with them | yes, the `admission` job |
| `make build-android-release`, `make build-android-aab` | the release-signed APK and App Bundle meant for a store | yes, `release-admission` is a prerequisite |
| `.github/workflows/ci.yml`, on pushes to `dev`/`master` and pull requests | nothing: lint, vulnerability check and tests only | — |
| `Dockerfile`, `docker-compose.yaml` | nothing: images are built locally, no workflow pushes them | — |

There is no nightly or automatic release from a branch.

**Outside the gate:** `make build-node-*`, `build-desktop-*`, `build-cli-*`, `build-all`,
`build-android`, `build-android-debug` and `install-desktop-linux` are development builds and do not
run the gate. Uploading a locally built binary by hand is not covered by anything in the repository;
a release is what the release workflow publishes.

### Release admission gate

A build that runs the secure session (protocol v2, [session_v2.md](protocol/session_v2.md)) may be
published only after a fixed list of release conditions is met and the project owner has decided,
separately, to release that version. The gate is code, not a checklist:

- `make release-admission` runs `internal/tools/releaseadmission`. It reads the constants the node is
  compiled with (`internal/core/config`: protocol versions and the release version) and derives the
  session mode exactly as the node does.
- It reads `packaging/release-admission.json` and refuses — exit status 1, with the list of what is
  outstanding — unless every applicable condition has status `done` with evidence (only the exact recorded Tor
  measurement may be `deferred`, separately reported as UNVERIFIED) **and** `release_decision`
  names the owner's decision to release **this** version.
- A build whose protocol version is below the activation the manifest records is refused: turning the
  secure session off again is not a way around its conditions.
- `RELEASE_TAG` (the workflow passes the pushed tag) must equal the build's release version, and so
  must `release_decision.version`.
- Evidence of developer and expert work names the full 40-character commit it was checked on (owner
  decisions carry none). The final-candidate condition names the commit all checks ran on; it must
  stay reachable — an ancestor of the tagged commit, never rebased or force-pushed away. The gate
  compares the tree being released with it: only the manifest, `docs/release-notes/RELEASE-v<version>.md`
  and the test that pins the manifest may differ. Recording evidence and the decision after the
  checks therefore needs no re-run; any other change does. Evidence names the commit of the report,
  not the tag, for the same reason.
- The comparison does not trust local git settings: global and system config, `GIT_*` variables,
  excludes files and replace refs are ignored; an index entry marked assume-unchanged or
  skip-worktree refuses; every untracked or ignored file counts as a change except the build caches
  `.gocache/` and `.gomodcache/` (an ignored `.go` file is still compiled); names are read
  NUL-separated.
- `docs/release-notes/RELEASE-v<version>.md` must exist as a regular file, not a link. The release
  job publishes exactly that file as the release body; there is no fallback to generated notes.
- It is fail-closed. A missing or unreadable manifest, invalid JSON, a repeated key or a key in
  another case, an unknown field, an unknown executor or status, a condition missing, repeated or
  unknown to the gate, a `done` condition without evidence, a final-candidate condition without a
  commit, a malformed release decision, a manifest written for another activation, a history that
  cannot be read — each refuses.
- The set of conditions lives in code (`internal/release/admission`); the manifest records their
  state. Removing a condition takes a code change, not a JSON edit.

Manifest fields:

| Field | Meaning |
|---|---|
| `schema` | format version, `2` |
| `activation_protocol_version` | protocol version at which the secure session activates; must match the build |
| `release_decision` | `null`, or `{"number": N, "date": "YYYY-MM-DD", "version": "MAJOR.MINOR.BUILD"}` — the owner's decision to release that version |
| `conditions[].id` | condition identifier known to the gate |
| `conditions[].condition` | what must be true |
| `conditions[].executor` | `owner`, `developer` or `external_expert` |
| `conditions[].check` | how it is verified |
| `conditions[].evidence` | `null`, or `{"report": "...", "commit": "<hex>" or null}` — what confirms it and the commit it was checked on |
| `conditions[].status` | `not_done`, `partial`, `done` or the strictly scoped `deferred`; only the exact recorded Tor deferral is exempted, never reported done |

### How the gate is wired

Every job of the release workflow lists `admission` in `needs` **directly**, so a refusal stops the
builds and the artifact uploads, not just the GitHub release. Tests in `internal/release/admission`
check, and fail on each known way around it:

- every job of the release workflow needs the gate, and has no job-level `if` (any condition can be
  written to run after a failed dependency);
- any job in **any** workflow that uploads artifacts, creates a release, pushes an image, calls a
  reusable workflow or may write with its token needs the gate too — a job with no `permissions`
  block anywhere runs with the repository default token and counts as writing;
- the gate job and its steps are checked against allow-lists of keys: the job has only `name`,
  `runs-on: ubuntu-latest`, `steps` and a literal `timeout-minutes`; its steps are exactly checkout
  (only `fetch-depth: 0` — no other ref, repository or path), setup-go (`go-version-file: go.mod`,
  optional literal `cache`) and `run: make release-admission RELEASE_TAG="${GITHUB_REF_NAME:?}"`
  compared whole. So no `if`, `continue-on-error`, `shell` (`"true {0}"` would skip the command),
  `working-directory`, `env`, `defaults`, `container` or write token, and no `env` or `defaults` at
  workflow level either;
- the GitHub release publishes `body_path: docs/release-notes/RELEASE-${{ github.ref_name }}.md`
  and never generated notes;
- the signed Android targets depend on `release-admission`.

A test also pins the committed manifest to its current state, so opening the gate is a reviewed
change to the manifest and to that test together.

**Repository setting the gate relies on.** GitHub runs the workflow file of the tagged commit. A
`v*` tag pushed on an older commit — one from before the gate existed, or one whose workflow lost it
— would publish without being checked. Protect `v*` tags with a ruleset: creating, updating and
deleting them restricted to the owner, no force-update.

### Current state

The release order is: complete the secure-session admission checks, publish the secure session in
its own release, observe that release in the live network for the owner's approved period and
criteria, then consider the overlay responder release (D1). D1 also requires the accepted design and
wire contract and a load run on its final candidate against the approved resource budget. Passing
local tests or completing the secure-session implementation does not replace any of these steps.

The gate is **closed**. The build activates the secure session. Decided: the secure session ships in
a release of its own, and the overlay role follows in a later release. Confirmed on the commit their
evidence names: a pin store marker that turns a lost or moved store into an explicit error; push
notices and every dial through the common path with SOCKS/Tor and no direct fallback, the raw legacy
dial removed; compatibility tests against all five real releases with protocol 28–30 in both
directions (a message sent by the old node itself cannot be tested, the headless release has no
sending command; old-to-new delivery is proven through an old relay); the identity-less probe after a
failed TLS attempt, with per-endpoint back-off; the manual TLS-first switch; the minimum session
version in the node's own record.

Implemented, with checks still missing (a condition counts only when every check passes on the commit
it names): refusal to start, before listening, on an unsafe pin store, directory or marker (the
real Windows/DACL tests are deferred by the owner; their behaviour remains UNVERIFIED); a loopback inbound class (its budgets are not
measured yet).

Outstanding: an independent cryptographic review; clearnet first-connection latency on real old releases
that sets the TLS probe timeout; measured loopback
budgets; a load run on the final candidate; implementation and verification of the owner's
[operator pin-removal decision](design/identity-pin-removal-decision.md), including the external
startup fence and confirmed recovery; an approved plan for observing the release in the
network; all checks on the final candidate; release notes. No release decision is recorded. The list
with checks and evidence is in `packaging/release-admission.json`.

## Русский

### Что публикует выпуск

| Путь | Что публикует | Под гейтом |
|---|---|---|
| `.github/workflows/release.yml`, по тегу `v*` | бинарники всех платформ как артефакты workflow, затем GitHub-выпуск с ними | да, джоба `admission` |
| `make build-android-release`, `make build-android-aab` | APK и App Bundle, подписанные ключом выпуска, — для магазина | да, `release-admission` — пререквизит |
| `.github/workflows/ci.yml`, на pull request | ничего: только линтер, проверка уязвимостей и тесты | — |
| `Dockerfile`, `docker-compose.yaml` | ничего: образы собираются локально, ни один workflow их не публикует | — |

Ночных и автоматических выпусков из ветки нет.

**Вне гейта:** `make build-node-*`, `build-desktop-*`, `build-cli-*`, `build-all`, `build-android`,
`build-android-debug` и `install-desktop-linux` — сборки для разработки, гейт они не запускают. Ручная
выкладка локально собранного бинарника ничем в репозитории не покрыта; выпуск — это то, что
публикует release workflow.

### Гейт допуска выпуска

Сборку, в которой работает защищённая сессия (протокол v2, [session_v2.md](protocol/session_v2.md)),
можно опубликовать только после выполнения закреплённого списка условий выпуска и отдельного решения
владельца проекта о выпуске именно этой версии. Гейт — это код, а не чек-лист:

- `make release-admission` запускает `internal/tools/releaseadmission`. Он читает константы, с
  которыми собран узел (`internal/core/config`: версии протокола и версия выпуска), и выводит режим
  сессии так же, как узел.
- Он читает `packaging/release-admission.json` и отказывает — код выхода 1 и список невыполненного —
  если не все применимые условия имеют статус `done` с подтверждением (только точный записанный
  перенос замера Tor допускает `deferred`, отдельно выводимый как UNVERIFIED) **или** в `release_decision` нет
  решения владельца о выпуске **этой** версии.
- Сборка, у которой версия протокола ниже активации, записанной в манифесте, получает отказ: снова
  выключить защищённую сессию — не способ обойти её условия.
- `RELEASE_TAG` (workflow передаёт опубликованный тег) обязан совпадать с версией выпуска сборки, как и
  `release_decision.version`.
- Подтверждение работы разработчика и эксперта называет полный 40-символьный коммит, на котором она
  проверена (у решений владельца коммита нет). Условие о финальном кандидате называет коммит, на
  котором прошли все проверки; он обязан оставаться достижимым — предком коммита под тегом, без
  rebase и force-push. Гейт сравнивает с ним публикуемое дерево: отличаться могут только манифест,
  `docs/release-notes/RELEASE-v<версия>.md` и тест, закрепляющий манифест. Поэтому внесение
  подтверждений и решения после проверок перепрогона не требует, а любое другое изменение — требует.
  По той же причине подтверждение ссылается на коммит отчёта, а не на тег.
- Сравнение не доверяет локальным настройкам git: глобальный и системный конфиг, переменные `GIT_*`,
  файлы исключений и replace-ссылки игнорируются; запись индекса с флагом assume-unchanged или
  skip-worktree — отказ; любой неотслеживаемый или игнорируемый файл считается изменением, кроме
  кешей сборки `.gocache/` и `.gomodcache/` (игнорируемый `.go`-файл всё равно компилируется); имена
  читаются через NUL.
- `docs/release-notes/RELEASE-v<версия>.md` обязан существовать как обычный файл, не ссылка. Джоба
  выпуска публикует ровно его как текст выпуска; запасного варианта со сгенерированным текстом нет.
- Отказ — по умолчанию. Отсутствующий или нечитаемый манифест, некорректный JSON, повторённый ключ
  или ключ в другом регистре, неизвестное поле, неизвестный исполнитель или статус, условие
  пропущенное, повторённое или неизвестное гейту, условие `done` без подтверждения, условие о
  финальном кандидате без коммита, некорректное решение о выпуске, манифест для другой активации,
  недоступная история — каждый случай даёт отказ.
- Набор условий задан в коде (`internal/release/admission`), манифест хранит их состояние. Убрать
  условие можно только правкой кода, а не JSON.

Поля манифеста:

| Поле | Значение |
|---|---|
| `schema` | версия формата, `2` |
| `activation_protocol_version` | версия протокола, с которой включается защищённая сессия; должна совпадать со сборкой |
| `release_decision` | `null` или `{"number": N, "date": "YYYY-MM-DD", "version": "MAJOR.MINOR.BUILD"}` — решение владельца о выпуске этой версии |
| `conditions[].id` | идентификатор условия, известный гейту |
| `conditions[].condition` | что должно быть выполнено |
| `conditions[].executor` | `owner`, `developer` или `external_expert` |
| `conditions[].check` | как это проверяется |
| `conditions[].evidence` | `null` или `{"report": "...", "commit": "<hex>" или null}` — чем подтверждено и на каком коммите проверено |
| `conditions[].status` | `not_done`, `partial`, `done` или строго ограниченный `deferred`; исключается только точный записанный перенос Tor, без отметки done |

### Как подключён гейт

Каждая джоба release workflow перечисляет `admission` в `needs` **напрямую**, поэтому отказ
останавливает сборки и загрузку артефактов, а не только GitHub-выпуск. Тесты
`internal/release/admission` проверяют это и падают на каждом известном способе обхода:

- каждая джоба release workflow зависит от гейта и не имеет условия `if` уровня джобы (любое условие
  можно написать так, чтобы джоба шла после упавшей зависимости);
- любая джоба **любого** workflow, которая загружает артефакты, создаёт выпуск, публикует образ,
  вызывает reusable workflow или может писать своим токеном, тоже зависит от гейта; джоба без блока
  `permissions` нигде работает с токеном репозитория по умолчанию и считается пишущей;
- джоба гейта и её шаги проверяются по белым спискам ключей: у джобы только `name`,
  `runs-on: ubuntu-latest`, `steps` и литеральный `timeout-minutes`; шаги — ровно checkout (только
  `fetch-depth: 0`, без другого ref, репозитория или пути), setup-go (`go-version-file: go.mod`,
  необязательный литеральный `cache`) и `run: make release-admission RELEASE_TAG="${GITHUB_REF_NAME:?}"`,
  сравниваемый целиком. То есть никаких `if`, `continue-on-error`, `shell` (`"true {0}"` пропустил бы
  команду), `working-directory`, `env`, `defaults`, `container` и токена на запись, а на уровне
  workflow — никаких `env` и `defaults`;
- GitHub-выпуск публикует `body_path: docs/release-notes/RELEASE-${{ github.ref_name }}.md` и никогда
  не сгенерированный текст;
- подписанные Android-цели зависят от `release-admission`.

Ещё один тест закрепляет текущее состояние закоммиченного манифеста: открыть гейт можно только
проверяемой правкой манифеста вместе с этим тестом.

**Настройка репозитория, на которую опирается гейт.** GitHub выполняет файл workflow из коммита под
тегом. Тег `v*` на старом коммите — до появления гейта или там, где workflow его потерял, —
опубликует выпуск без проверки. Защитите теги `v*` ruleset-ом: создание, изменение и удаление —
только владелец, без force-update.

### Текущее состояние

Порядок выпуска: выполнить условия допуска защищённой сессии, опубликовать её отдельным выпуском,
наблюдать этот выпуск в живой сети в течение утверждённого владельцем срока по утверждённым
критериям, затем рассматривать выпуск оверлейного ответчика (D1). Для D1 также нужны принятый дизайн
и wire-контракт и нагрузочный прогон финального кандидата по утверждённому ресурсному бюджету.
Успешные локальные тесты и завершённая реализация защищённой сессии эти этапы не заменяют.

Гейт **закрыт**. Сборка включает защищённую сессию. Решено: защищённая сессия выходит отдельным
выпуском, overlay-роль — в одном из следующих. Подтверждено на коммите, названном в подтверждении:
маркер хранилища pin, превращающий потерю или перенос хранилища в явную ошибку; push-уведомления и все
дозвоны через общий путь с SOCKS/Tor без прямого обхода, сырой legacy-дозвон удалён; тесты совместимости
со всеми пятью настоящими выпусками протокола 28–30 в обе стороны (отправку сообщения самим старым узлом
проверить нельзя — в headless-выпуске нет команды отправки; доставка «старый → новый» доказана через
старый узел-ретранслятор); проба без identity после неудачной TLS-попытки, с отступом повторов по
endpoint; ручной выключатель «TLS-первым»; минимальная версия сессии в собственной записи узла.

Реализовано, но проверок ещё не хватает (условие засчитывается, только когда все проверки проходят на
названном коммите): отказ старта до открытия слушателей при небезопасных хранилище pin,
каталоге или маркере (настоящие Windows/DACL тесты отложены владельцем, статус UNVERIFIED); отдельный класс
loopback-входа (бюджеты пока не измерены).

Не выполнено: независимая криптографическая экспертиза; замер задержки первого подключения clearnet
на настоящих старых выпусках, по которому выбирается таймаут TLS-пробы; измеренные бюджеты
loopback-класса; нагрузочный прогон финального кандидата; реализация и проверка принятого
[решения владельца о снятии pin оператором](design/identity-pin-removal-decision.md), включая внешний
запрет перезапуска и подтверждённое восстановление; утверждённый план наблюдения выпуска в сети;
все проверки на финальном кандидате; описание выпуска. Решения о выпуске нет. Список с проверками и
подтверждениями — `packaging/release-admission.json`.

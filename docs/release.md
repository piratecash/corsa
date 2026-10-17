# Releases

## English

### What publishes a release

| Path | What it publishes | Gated |
|---|---|---|
| `.github/workflows/release.yml`, on a pushed `v*` tag | binaries for every platform as workflow artifacts, then a GitHub release with them | yes, the `admission` job |
| `make build-android-release`, `make build-android-aab` | the release-signed APK and App Bundle meant for a store | yes, `release-admission` is a prerequisite |
| `.github/workflows/ci.yml`, on pull requests | nothing: lint, vulnerability check and tests only | — |
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
  outstanding — unless **every** condition has status `done` with evidence **and** `release_decision`
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
| `schema` | format version, `1` |
| `activation_protocol_version` | protocol version at which the secure session activates; must match the build |
| `release_decision` | `null`, or `{"number": N, "date": "YYYY-MM-DD", "version": "MAJOR.MINOR.BUILD"}` — the owner's decision to release that version |
| `conditions[].id` | condition identifier known to the gate |
| `conditions[].condition` | what must be true |
| `conditions[].executor` | `owner`, `developer` or `external_expert` |
| `conditions[].check` | how it is verified |
| `conditions[].evidence` | `null`, or `{"report": "...", "commit": "<hex>" or null}` — what confirms it and the commit it was checked on |
| `conditions[].status` | `not_done`, `partial` or `done`; only `done` counts |

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

The gate is **closed**. The build activates the secure session. Decided: the secure session ships in
a release of its own, and the overlay role follows in a later release. Confirmed on the commit their
evidence names: a pin store marker that turns a lost or moved store into an explicit error; push
notices and every dial through the common path with SOCKS/Tor and no direct fallback, the raw legacy
dial removed; compatibility tests against all five real releases with protocol 28–30 in both
directions (a message sent by the old node itself cannot be tested, the headless release has no
sending command; old-to-new delivery is proven through an old relay).

Implemented, with checks still missing (a condition counts only when every check passes on the commit
it names): the identity-less probe after a failed TLS attempt, with per-endpoint back-off; the manual
TLS-first switch; refusal to start, before listening, on an unsafe pin store, directory or marker (the
Windows tests still have to run on a real Windows host); a loopback inbound class (its budgets are not
measured yet); the minimum session version in the node's own record.

Outstanding: an independent cryptographic review; a measurement of the handshake phases over Tor; a
measurement of first-connection latency over Tor that sets the TLS probe timeout; measured loopback
budgets; a load run on the final candidate; the
remaining owner choice on operator pin removal; an approved plan for observing the release in the
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
  если не **все** условия имеют статус `done` с подтверждением **или** в `release_decision` нет
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
| `schema` | версия формата, `1` |
| `activation_protocol_version` | версия протокола, с которой включается защищённая сессия; должна совпадать со сборкой |
| `release_decision` | `null` или `{"number": N, "date": "YYYY-MM-DD", "version": "MAJOR.MINOR.BUILD"}` — решение владельца о выпуске этой версии |
| `conditions[].id` | идентификатор условия, известный гейту |
| `conditions[].condition` | что должно быть выполнено |
| `conditions[].executor` | `owner`, `developer` или `external_expert` |
| `conditions[].check` | как это проверяется |
| `conditions[].evidence` | `null` или `{"report": "...", "commit": "<hex>" или null}` — чем подтверждено и на каком коммите проверено |
| `conditions[].status` | `not_done`, `partial` или `done`; засчитывается только `done` |

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

Гейт **закрыт**. Сборка включает защищённую сессию. Решено: защищённая сессия выходит отдельным
выпуском, overlay-роль — в одном из следующих. Подтверждено на коммите, названном в подтверждении:
маркер хранилища pin, превращающий потерю или перенос хранилища в явную ошибку; push-уведомления и все
дозвоны через общий путь с SOCKS/Tor без прямого обхода, сырой legacy-дозвон удалён; тесты совместимости
со всеми пятью настоящими выпусками протокола 28–30 в обе стороны (отправку сообщения самим старым узлом
проверить нельзя — в headless-выпуске нет команды отправки; доставка «старый → новый» доказана через
старый узел-ретранслятор).

Реализовано, но проверок ещё не хватает (условие засчитывается, только когда все проверки проходят на
названном коммите): проба без identity после неудачной TLS-попытки, с отступом повторов по endpoint;
ручной выключатель «TLS-первым»; отказ старта до открытия слушателей при небезопасных хранилище pin,
каталоге или маркере (Windows-тесты ещё должны пройти на настоящей Windows); отдельный класс
loopback-входа (бюджеты пока не измерены); минимальная версия сессии в собственной записи узла.

Не выполнено: независимая криптографическая экспертиза; замер фаз рукопожатия через Tor; замер
задержки первого подключения через Tor, по которому выбирается таймаут TLS-пробы; измеренные бюджеты
loopback-класса; нагрузочный прогон финального кандидата; оставшееся решение владельца о снятии pin оператором; утверждённый план наблюдения выпуска в сети;
все проверки на финальном кандидате; описание выпуска. Решения о выпуске нет. Список с проверками и
подтверждениями — `packaging/release-admission.json`.

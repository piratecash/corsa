# S6 component checks: loopback and CLI startup (2026-11-02)

## English

The bounded loopback Node-test component completed its nine selected checks.
The separate actual-CLI pin-store matrix stopped during its first launch and
has no completed case verdict. A standalone diagnostic S6 ELF was subsequently
built and inspected; it has not been executed. These are separate results,
with no overall S6, D1, 512 MB suitability or release verdict.

### Loopback component

One Linux arm64 CGO0 test ELF, SHA-256
`8344d23767d61dfdf9591d699f32e7eeab23f2200745f342dd18720925c4ae61`,
ran nine selected Node tests: **9 RUN/PASS, 0 FAIL/SKIP**, with no child tests
or background-drain warning. The saved Go stream contains 35 lines. The
component used one shared cgroup for keeper, sampler and test process:
1 CPU, 512,000,000 bytes, 64 PID, swap disabled. Its preparer had a separate
128 MiB budget and was removed before the working container started.

| Observed metric | Value |
|---|---:|
| Cumulative kernel charged cgroup peak, through final reading | 49,569,792 bytes |
| Group CPU usage, initial-to-final delta | 256,848 µs |
| Group CPU user / system | 167,623 / 89,225 µs |
| Initial-to-final elapsed-time bracket | 0.793963917–0.797140375 s |
| Qualified cgroup snapshots / periodic samples | 5 / 1 |
| Maximum consecutive read-end gap | 503,818,834 ns |

The gap stayed within the fixed 2-second oracle. All mandatory process,
executable, namespace, clock and owned-reap joins passed the original shared
verifier. The final reading followed both test-process and sampler reap;
no natural-exit process sample was unavailable. Saved container inspections
reported no OOM. Exact owned container and volume queries were empty after
cleanup, and Root separately confirmed closure and the unchanged Main/index.

The selected tests exercise the 64/65 active-connection boundary, the 100/101
connection-admission boundary with the production 10-second window, the 16/17
concurrent ClientHello share, cancellation/shutdown slot release, IPv4/IPv6
classification and offender isolation. The 16/17 case holds real ClientHello
handshakes; it does not establish 16 authenticated complete TLS sessions.
Synthetic IPv6 classification and an actual `::1` connection are separate
tests; the real IPv6 check did not skip. Existing policy evidence joins this measured component. The retained
[Tests job 124210372902](https://github.com/piratecash/corsa/actions/runs/41098494039/job/124210372902)
checked out exact ac53899 and completed the unfiltered default Node package
under `make test-release` with `-count=1`. Its Node package conclusion is PASS;
this non-verbose log supplies no per-test RUN/SKIP census. The release-test
scope still explicitly defers overlaysim to D1, rather than claiming full
repository PASS. Corresponding source fixtures include
[`TestTheLoopbackClassRateIsASlidingWindow` and
`TestTheLoopbackClassHeldBudgetIsReleasedOnceByEachSlot`](../../internal/core/node/loopback_inbound_test.go),
configured class-share/node-wide layering, the auth-state check
[`TestLoopback_DoesNotElevateAuth`](../../internal/core/node/command_scope_test.go),
and [restart pin/downgrade checks](../../internal/core/node/session_restart_v2_requirement_test.go).
The same CI package includes the secure/legacy compatibility and pin tests
in [`session_secure_test.go`](../../internal/core/node/session_secure_test.go).
These relevant Node/NetCore/sessionv2/connbudget sources are unchanged from ac53899.

Together with the nine fresh qualified component checks, this supports closing
only `loopback_inbound_class` within the accepted macOS/Linux measurement scope;
the Tor phase remains explicitly deferred and UNVERIFIED. It grants neither a
new transport guarantee nor full resource/S6 acceptance. Relevant source changes
still require reassessment under `final_candidate_checks`.

This is one current Node-test component run, not a standalone daemon or Desktop
measurement. There is no baseline comparison, statistical regression estimate
or per-test CPU measurement. CPU includes keeper/sampler/test work between the
initial and final readings; early keeper setup precedes that delta. The
cumulative memory peak can include that prefix. Late keeper exit-tail,
physical shared-page attribution, RSS peak and measured preparer cost remain
UNKNOWN. The configured container limits are not measured costs. Tor remains
outside this run under the existing owner deferral.

### Actual CLI matrix: INCOMPLETE

The matrix used the existing exact corsad
`3232a72d3562ce59efdfeae26a02a590fc6ffadbbafe7894d657c3325465d17f`
from production source ac53899. Its one authorized attempt returned
**INCOMPLETE**, with known owned cleanup and publication inside the original
software budget. Preparer and genuine UID502 fixture handoff completed.

Of 11 planned cases / 12 launches, only the first `safe` launch started.
It obtained a live PID/executable binding, emitted 16 console chunks and
reached an after-exit cgroup reading. No `CLI_FILE_LOG`, `CLI_RESULT` or
`CLI_FINAL` was published: **zero completed cases**, zero proven positive or
refusal outcomes; 11 launches never started. The saved 22-record worker stream
ends with `CLI_ERROR`, stage `cli-file-log`, status UNKNOWN, closed reason
`worker-condition-refused`, errno null and Adverse facts null.

Three valid cgroup prefix readings remain available. The last observed peak,
21,876,736 bytes, is a partial-prefix value, not the qualified final peak of
the matrix. The shared success verifier correctly refuses this incomplete
stream. No full memory or startup-conformance PASS follows from it.

A concrete source incompatibility was identified: the original POSIX
`redirectStderr` writes an owned-PID startup separator, while the worker
required zero appended stderr-file bytes. The captured console announces
stderr redirection. This explains why the worker's positive oracle needs a
narrow correction, but does not recover the unique historical inner failure
predicate: the closed error spans file custody/read, prefix, stderr, suffix
and first log-parse checks, and the temporary files were removed. The attempt
and its RAW remain retained. Any future correction must accept only the exact
expected separator and continue refusing additional runtime-error bytes;
no retry is authorized by this report.

### Standalone S6 diagnostic build

One separately approved build produced a Linux arm64/v8 CGO0 diagnostic ELF:
SHA-256 `4d27fd4e14e408b7bc2eed01640618129790be6254b7332e9da3cb1280b5e71b`,
22,020,256 bytes. Static inspection confirms AArch64, no dynamic loader,
Go 1.27.3, the `s6_observation_probe` tag and exactly 23 qualified
module/version/h1 dependencies. Source composition is HEAD2c plus the 11
reviewed staged paths, index `73616a6f…`, with 2,284 captured files; it is
not relabelled as an ac53899-only or published build. Seven compiler/metadata/
process commands exited normally with empty stderr. Source, scoped cache,
SDK and external Main/index bookends were preserved. **BUILT_NOT_EXECUTED**:
actual parent attachment to this ELF, useful-work readiness, process/clock
qualification and the finite OFF/ON cost pair remain pending.

### Retained local evidence

Private evidence is local to this workspace; it is not assumed available on
another machine:

- `docs/_internal/d1-artifacts/2026-10-28/s6-loopback-component-host-source-v1/TESTER-ACTUAL-DATA-ACK.json`
  and work nonce `348a50b070b1864349c13fbcac2725eb`.
- `docs/_internal/d1-artifacts/2026-10-28/s6-final-cli-pin-store-host-source-v1/TESTER-ACTUAL-INCOMPLETE-DATA-ACK.json`
  and work nonce `4d4959570be1846b8f7d4ce3ba4b77ad`.
- `docs/_internal/d1-artifacts/2026-10-28/s6-observation-standalone-build-source-v1/TESTER-ACTUAL-BUILD-DATA-ACK.json`
  and `after.json`.

Root executed the approved attempts. The tester independently replayed saved
loopback data; for CLI and build data the tester also authored the harness,
with independent CODE review provided separately. No piratecashd was started.

## Русский

Компонент loopback завершил девять выбранных Node-проверок. Отдельная матрица
настоящего CLI остановилась на первом запуске без завершённых case verdict.
После этого собран и статически проверен standalone S6 diagnostic ELF;
он не исполнялся. Общая готовность S6/D1, пригодность в 512 МБ и выпуск
этими разными результатами не утверждаются.

### Loopback

Точный test ELF `8344d237…` дал **9 RUN/PASS, 0 FAIL/SKIP**: дочерних тестов
и предупреждения незавершённого background drain нет. Сохранены 35 строк Go.
Keeper, sampler и test process вместе делили 1 CPU /512 000 000 байт /64 PID
без swap; preparer с отдельными 128 MiB завершён и удалён до рабочей роли.

Финальный cumulative kernel charged peak — **49 569 792 байта**. Разность
group CPU — **256 848 мкс**, user/system — **167 623/89 225 мкс**.
Интервал initial→final ограничен **0,793963917–0,797140375 с**.
Пять cgroup-срезов включают один periodic sample; максимальный разрыв концов
чтения **503 818 834 нс** при oracle 2 с. Исходный verifier принял точные
process/executable/namespace/clock/reap связи. Финальное чтение выполнено после
reap test process и sampler. Пропавших natural-exit process samples и OOM
в сохранённых inspect нет. Собственные контейнеры и volumes удалены; Root
отдельно подтвердил closure и сохранность Main/index.

Проверены границы active64/65, admission100/101 с production window10с,
ClientHello16/17, освобождение при cancellation/shutdown, IPv4/IPv6 и изоляция
нарушителя. ClientHello не равен полностью аутентифицированной TLS-сессии;
синтетический IPv6 и настоящий ::1 проверены отдельно, реальный тест не SKIP.
Эти измерения связаны с прежним CI именно ac53899: job124210372902 выполнил
полный default Node-пакет без фильтра `-run`, с `-count=1`, и получил package
PASS. Лог не verbose: индивидуальный RUN/SKIP census из него не заявляется;
overlaysim отдельно DEFERRED до D1, общего repository PASS нет. Source fixtures
покрывают скользящее окно (`TestTheLoopbackClassRateIsASlidingWindow`), once-only
освобождение (`TestTheLoopbackClassHeldBudgetIsReleasedOnceByEachSlot`),
configured share/node-wide layering, `TestLoopback_DoesNotElevateAuth`,
restart pin/downgrade и secure/legacy compatibility. Файлы/ссылки приведены
в EN выше; соответствующие production/test paths не изменились от ac53899.

Совместно с девятью свежими квалифицированными проверками это закрывает только
`loopback_inbound_class` в согласованной macOS/Linux measurement scope. Tor
остаётся DEFERRED/UNVERIFIED; полная ресурсная/S6-приёмка не утверждается.
Изменения соответствующих исходников требуют пересмотра final_candidate_checks.

Это один компонентный прогон Node-test, без baseline и статистического вывода
о регрессии. Per-test CPU не измерялся. CPU-разность включает keeper/sampler/
тесты внутри срезов, ранний keeper prefix — вне неё; cumulative peak может
включать его страницы. Поздний keeper exit-tail, physical shared pages,
RSS peak и измеренная стоимость preparer — UNKNOWN. Лимиты не являются
расходом. Tor остаётся за границей по принятому owner deferral.

### CLI: INCOMPLETE

Использован прежний corsad `3232a72d…`, production ac53899. Разовая попытка
завершилась **INCOMPLETE**, с известным собственным cleanup и публикацией
в исходном программном бюджете. Preparer/передача настоящих foreign UID502
fixtures прошли. Из 11 случаев/12 запусков начат только первый safe.
Есть live PID/executable binding, 16 console chunks и after-exit cgroup cut,
но нет CLI_FILE_LOG/CLI_RESULT/CLI_FINAL: **0 завершённых случаев**, ни одного
подтверждённого positive/refusal; остальные 11 запусков не начинались.

22-record RAW заканчивается CLI_ERROR: `cli-file-log`, UNKNOWN,
`worker-condition-refused`, errno/facts null. Три допустимых prefix-среза
сохранились; последний peak **21 876 736 байт** не является финальной метрикой
матрицы. Исходный success verifier правильно отвергает неполный поток.

Найдена несовместимость oracle: POSIX redirectStderr добавляет startup separator
с PID, а worker требовал отсутствие любых новых stderr-file bytes. Console
подтверждает переход через redirect. Однако точный исторический внутренний
predicate остаётся UNKNOWN: stage охватывает несколько проверок, исходные
временные файлы удалены. Будущее исправление должно принимать только точный
ожидаемый separator, сохранив отказ для других runtime bytes. RAW и отказ
сохранены; этот отчёт не разрешает повтор.

### Standalone build

Новый diagnostic ELF `4d27fd4e…` (**22 020 256 байт**, полный SHA выше) собран
из HEAD2c +11 проверенных staged paths/index73616a6f, 2284 файла. ABI — static
AArch64/Linux arm64/v8/CGO0, Go1.27.3, tag s6_observation_probe; фактические
23 module/version/h1 совпали с квалифицированным набором. Семь команд завершены
normal0/stderr0; source/cache/SDK/Main/index сохранены. **BUILT_NOT_EXECUTED**:
подключение parent к этому ELF, readiness/process/clock и finite OFF/ON cost
остаются открытыми. Это отдельный diagnostic artifact, не новый опубликованный
corsad. Пути локальных свидетельств и границы ролей приведены выше.

Прогоны исполнял Root; tester проверил сохранённые данные. Для CLI/build он
также автор обвязки, отдельное независимое CODE review сохранено. Piratecashd
не запускался. Ни одна метрика не закрывает общую S6/D1/512 МБ/G2-приёмку.


## CLI HOST v2 follow-up / Последующая попытка CLI HOST v2

**EN.** One separately authorized HOST v2 attempt with unchanged CLI3232 and
worker V3 ended **INCOMPLETE: 0 completed cases, 1 safe launch started**.
No CLI_FILE_LOG/CLI_RESULT/CLI_FINAL was published. CLI_ERROR reports
`cli-file-log-json-parse`, UNKNOWN, with null errno/facts. Owned cleanup and
Main/index preservation were confirmed; no final memory or conformance PASS.

Source review found the startup banner duplicates zerolog's reserved `level`
key. The actual bad file line was not archived: this source defect and separate
native RED/GREEN evidence cannot recover the historical inner predicate.
Fresh candidates retain strict parsing and need the producer fix, fresh builds
and reviewed bindings. First-contact READY and the unexecuted cost package are
on runtime HOLD; old c47 remains unchanged and needs a separate narrow startup
compatibility rule. No automatic retry or new S6 condition PASS follows.

**RU.** Отдельно разрешённая попытка HOST v2 с прежним CLI3232 и worker V3 дала
**INCOMPLETE: 0 завершённых случаев, начат 1 safe**. Нет CLI_FILE_LOG/CLI_RESULT/
CLI_FINAL; stage `cli-file-log-json-parse`, UNKNOWN, errno/facts null. Собственная
очистка и Main/index подтверждены; финальных метрик и conformance PASS нет.

Найден duplicate reserved `level` в startup source. Ошибочная фактическая строка
не сохранена: source-дефект и отдельные native RED/GREEN не восстанавливают
исторический predicate. Strict parser новых кандидатов не ослабляется; нужны
исправление producer, свежие сборки и bindings. First-contact READY отозван,
cost package на runtime HOLD; old c47 неизменен, его startup-совместимость
проверяется отдельно. Автоповтора и нового condition PASS нет.

Local evidence only / локальные свидетельства:
[HOST v2 T/R receipts](../_internal/d1-artifacts/2026-10-28/s6-final-cli-pin-store-host-source-v2/)
and [Root cleanup postflight](../_internal/d1-artifacts/2026-11-02/s6-authorized-runtime-bookends-v1/AFTER-cli-v2.json).
These private paths are not assumed available elsewhere / приватные пути
не предполагаются доступными на другой машине. Prior sections remain history /
предыдущие разделы сохранены как история.

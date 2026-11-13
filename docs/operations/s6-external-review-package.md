# S6 external review and native Windows evidence / Внешняя проверка S6 и свидетельства Windows

## Review decision of 2026-11-02 / Решение о ревью от 2026-11-02

The owner assigned S6 cryptographic review to Codex in the security reviewer
role and Claude Fable. This decision supersedes earlier reviewer assignments
below. Both models review the same full candidate SHA and produce separately
attributed reports. Record coverage, limitations and findings by severity,
resolve discrepancies against source and reproducible tests, fix every
Critical finding and record the disposition of each High. Both models
re-review affected paths after fixes. The evidence record references both
reports, the disposition record and the full candidate SHA.

Владелец поручил крипторевью S6 двум моделям. Codex выступает в роли
специалиста по безопасности, Claude Fable проводит вторую проверку.
Человеческая экспертиза исключена из плана. Это решение заменяет прежнее
назначение ревьюера в исторических разделах ниже. Обе модели проверяют один
полный SHA кандидата и дают отдельные отчёты с атрибуцией, покрытием,
ограничениями и оценкой тяжести находок. Расхождения сверяются по исходникам
и воспроизводимым тестам. Все Critical исправляются, по каждой High
записывается решение. После исправлений обе модели повторно проверяют
затронутые пути. В evidence указываются оба отчёта, решения по находкам
и полный SHA проверенного кандидата.

## Current verification scope / Текущая область проверки

The [accepted current scope](urgent-full-product-release-scope.md) is daemon and Desktop on Linux amd64, macOS arm64/amd64 and Windows amd64/arm64; Android APK/AAB follow separately. Only `internal/overlaysim` is DEFERRED to D1, not PASS; generic `make test-all` is not green. K2/R1b/R2/N-in remain OFF in ordinary constructors, configuration and RPC. D1/G2/FindNode/F1 activation is outside this release; internal source opt-in APIs do not activate shipped products. All other applicable product tests, proxy/no-direct-fallback and authentication protections, the two-model crypto review and the 17 admission conditions remain required. The [recorded Windows/DACL and real Tor deferral](d1-release-decision-proposal.md#measurement-scope-2026-10-23) remains UNVERIFIED, not PASS; applicable Windows builds and macOS/Linux checks remain required. Tor timing is not inferred from clearnet measurements.

[Принятая текущая область](urgent-full-product-release-scope.ru.md) — демон и Desktop на Linux amd64, macOS arm64/amd64 и Windows amd64/arm64; Android APK/AAB выпускаются отдельно позже. Только `internal/overlaysim` отложен до D1 как DEFERRED, не PASS; общий `make test-all` не зелёный. K2/R1b/R2/N-in выключены в обычных constructors, config и RPC. Активация D1/G2/FindNode/F1 вне этого релиза; внутренние source opt-in API не включают их в выпускаемых продуктах. Остальные применимые продуктовые тесты, proxy/no-direct-fallback, аутентификация, крипторевью двух моделей и все 17 условий допуска сохраняются. [Записанная отсрочка Windows/DACL и реального Tor](d1-release-decision-proposal.md#measurement-scope-2026-10-23) остаётся UNVERIFIED, не PASS; применимые Windows builds и macOS/Linux проверки обязательны. Tor timing нельзя выводить из clearnet-замеров.

### Admission checkpoint / Состояние допуска — 2026-11-13

The prepared intermediate Git index and the working-tree manifest both record
9 `done`, 2 `partial`, 5 `not_done` and 1 `deferred`. The two additional
completed entries record the accepted component evidence for
`load_run_on_candidate` and `loopback_inbound_class`. The final candidate
still requires evidence applicability checks. `release_decision` is null.

Подготовленный промежуточный Git index и манифест рабочего дерева содержат
9 `done`, 2 `partial`, 5 `not_done` и 1 `deferred`. Два дополнительных
завершённых условия отражают принятые компонентные свидетельства по
`load_run_on_candidate` и `loopback_inbound_class`. Для финального кандидата
предстоит сверить применимость свидетельств. `release_decision` остаётся null.

| Open condition / Открытое условие | Status | Evidence still required / Что остаётся подтвердить |
|---|---|---|
| `crypto_review` | `not_done` | Both attributed reports on one full candidate SHA, disposition of findings and follow-up review / Два отчёта на одном полном SHA, решения по находкам и повторное ревью |
| `first_contact_latency` | `not_done` | Clearnet timing and a justified timeout / Замер clearnet и обоснование таймаута |
| `unsafe_pin_store_refusal` | `partial` | Applicable startup and refusal checks on the release build / Применимые проверки старта и отказов на сборке выпуска |
| `owner_session_choices` | `partial` | Remaining session choices and candidate evidence / Оставшиеся решения по сессиям и свидетельства на кандидате |
| `network_observation_plan` | `not_done` | Approved full observation plan with criteria, sources and resources / Утверждённый полный план наблюдения с критериями, источниками и ресурсами |
| `final_candidate_checks` | `not_done` | Applicable tests, builds, lint and mixed-version integration on the candidate / Применимые тесты, сборки, lint и интеграция версий на кандидате |
| `release_notes` | `not_done` | Owner approval of the committed body for the exact tag / Утверждение владельцем текста, закоммиченного для точного тега |

The [manifest](../../packaging/release-admission.json) defines the complete
conditions. `tor_phase_measurement` is the deferred entry.

Полные условия заданы в [манифесте](../../packaging/release-admission.json).
Отложенное условие — `tor_phase_measurement`.

## English

### Status and source identity

This is a preparation package for `crypto_review` and the Windows part of
`unsafe_pin_store_refusal` in the [release manifest](../../packaging/release-admission.json).
It records review scope and proposed execution inputs. No final two-model verdict, new native Windows
execution or complete DACL evidence, resource authorisation, production deployment or release
approval is supplied here.

The source-only Claude Fable report covers protocol 31/minimum 28 at commit
**`2c5262a14510e26b18688d5c06821af4fe2e81d3`**, Git tree
`cb99235e29df34688517477f9cc245aa27be4333`, compared with v2.5.71 and using Go 1.27.3.
The [original report](../refactoring/report/corsa-crypto-review-2c5262a1-2026-11-02.md)
and its [Russian translation](../refactoring/report/corsa-crypto-review-2c5262a1-2026-11-02.ru.md)
are one model's evidence, not two separate reviews. Neither covers the later
uncommitted fixes or unrelated staged changes. Its reference to a human expert
reflects the earlier process, superseded by the owner decision above; preserve
the original report's attribution and limitations.

MSG-1/2/3 and TRUST-1 remain open High findings. Reproductions of message
attacks and existing full-store tests do not establish the remote pin-exhaustion
rate experimentally. No binaries, deployment, live network, Windows, Tor or
Android execution are covered. A clean handshake review does not establish
security of its message and persistent-trust consumers.

After fixes, give both models the same new full commit SHA and an exact diff
from this reviewed input. Each report must name that SHA or provide an
attributable affected-path addendum. See the admission checkpoint above for
the status and outstanding evidence.

The earlier inputs `ac53899e47970217662f6e45fe199196ea184072`, published
v2.5.71 (`f8139aa949df89e3f51f0a240eaf440b6f171255`) and the separate-project AI
report on `52cdd9af7ea059a69f479fd963698499ca8ec131` remain historical. Their
coverage cannot be relabelled as acceptance of the corrected S6 candidate.
Supply immutable commits rather than a moving DEV branch.

The old MAIN2047 snapshot based on `c17a4961689c9e8c33bd5681ea7c7ec577bb53cf`
(manifest SHA-256 `4dee2cbd54685ac0076afa325e0c65bf0326ffd465f28f71cb7ce069910efb80`)
is historical preparation, not the current source input.

An accepted review report must name the full 40-character candidate commit, the actual source
and any binary inputs reviewed (explicitly none if the review is source-only). A preliminary review of this snapshot can find defects, but cannot be
relabelled as final-candidate acceptance. The [release gate](../release.md) requires all conditions
and a separate version-specific owner decision. After the checked candidate commit, only the
manifest, that version's release notes and the manifest-pinning test may change without renewed
affected checks. Ignored/untracked files count as changes except the two documented build caches;
prepare a clean release tree rather than assuming this working directory is one.

### Two-model cryptographic review

Review the [session contract and published vectors](../protocol/session_v2.md), then follow these
implementation anchors from the repository root:

| Question | Source and existing test anchors |
|---|---|
| TLS policy and what authenticates identity | `internal/core/sessionv2/config.go`: `listenerConfig`, `dialerConfig`; `certificate.go`: `CertificateSource`, `mintCertificate`; `guards_test.go` |
| Exact signed bytes and strict frame | `proof.go`: `proofPayload`, `verifyProof`, `parseProofFrame`; `proof_test.go`: `TestProofPayloadAndSignaturesMatchTheVectors`, `TestProofRefusesTheNegativeVectors`, `TestProofFrameIsParsedStrictly` |
| Handshake state and identity publication | `handshake.go`: `runHandshake`, `sendIntroAndProof`, `receiveProvenPeer`, `checkIntro`; `session.go` and `proven_identity_test.go`; `node/session_secure.go`: inbound `sessionv2.Accept`, outbound `sessionv2.Dial`, `acceptProvenInboundOwned`, `provenTransport` |
| Active relay and reflection | `handshake_test.go`: `TestARelayedListenerProofDoesNotVerifyOnAnotherConnection`, `TestARelayedDialerProofIsRefusedWhereV1AcceptedIt`; `proof_test.go` negative exporter/role/key/network/v1-signature cases |
| Signing keys, records and liveness claims | `internal/core/identity/publickey.go`, `identity.go`; `internal/core/protocol/identity_record.go`, `identity_discovery.go`, `liveness_token.go`; `internal/core/node/identity_discovery.go`, `presence_service.go`: canonical/small-order acceptance, record/key binding, sealed target proof and bounded epoch checks |
| Downgrade, persistence and client trust | `internal/core/node/session_dial_choice.go`, `session_probe.go`, `secure_session_store.go`, `pin_store_marker.go`, `endpoint_binding_operator.go`, `endpoint_canonical.go`; `internal/core/secretfile/*.go`; ordinary `sdk/runtime.go`, `internal/app/desktop/runtime.go`, `internal/app/node/app.go` roots |
| Message and receipt claims | `internal/core/directmsg/message.go`, `file_command_crypto.go`; `internal/core/service/dm_crypto.go`, `dm_router.go`; `internal/core/node/message_identity.go`, `service.go`; `internal/core/protocol/message.go`: distinguish party-tuple/hop authentication from end-to-end content or delivery proof |

The implementation uses TLS 1.3 only, ALPN `corsa/2`, disabled session tickets and no client session
cache. The dialler deliberately does not authenticate the TLS certificate: identity is established
by an Ed25519 proof bound to that TLS connection, not by PKI. Check this boundary and every path
that consumes a proven identity; do not infer authentication from an intro, a certificate or a
version hint. The currently specified transcript is:

```text
E = TLS-Exporter("EXPORTER-corsa-session-v2", empty context, 32)
DOMAIN(tag) = tag || 0x00 || uint16be(len(network)) || network
signed = DOMAIN("corsa-session-v2-proof") || role || E
role = 0x01 (dialler) or 0x02 (listener), derived from the local TLS role
```

Check exporter label/context/length, domain separation and network encoding, role reflection,
intro key/address binding, expected-peer comparison, strict proof framing and refusal before
publication. Follow actual dependencies if the listed anchors lead outside this entry scope;
record those additions in the report.

The adversary may operate a peer, terminate two separate TLS sessions and relay messages, alter or
drop first-contact traffic, forge unauthenticated hints and present a different identity. The review
must preserve the documented first-contact limitation: a pair that has never proved v2 can still
be held in legacy by an on-path attacker where legacy is permitted. Possession of a valid identity
does not prove an operator, prevent Sybils or provide anonymity. Compromised endpoint keys, a
compromised local OS and a malicious trusted administrator are outside these proof guarantees.
Reviewers should state additional assumptions rather than silently importing certificate trust.

Existing test sources are review inputs, not a final review verdict. This package establishes no Tor
timing, measured loopback budget, Windows filesystem behaviour, C3 deployment admission or
network-wide closure of the legacy relayed-proof attack. Those retain their own conditions.

The review includes ordinary SDK/Desktop trust and storage consumers, not just the daemon handshake.
In particular, the dm-v1 outer message ID and `DeliveryReceipt` are not signed content receipts;
assess malicious authenticated-relay/replay assumptions and the actual status/retry claims without
silently declaring tuple-scoped state to be end-to-end delivery proof. Do not infer a need to
activate the deferred session/D1 mechanisms or redesign the wire protocol without an explicit finding.

Each model's deliverable is a separately attributable report naming the candidate commit,
reviewer and date, scope, method, assumptions, findings by severity, source locations, safe reproducible
examples, unreviewed areas and explicit limitations. Identify the actual source and any binary/dependency
inputs reviewed; explicitly list binary/dependency inputs not reviewed. Avoid live user identities, stores or messages.
Every Critical finding must be fixed and both models must re-review affected paths on the resulting candidate.
Record unresolved findings and their disposition; do not turn missing coverage into a passing result.

### Native Windows execution specification

The [native Windows amd64 filesystem/logging job](https://github.com/piratecash/corsa/actions/runs/41098494028/job/124210372432)
completed with `success` for the exact `ac53899` commit. The current readback establishes
that job conclusion, not an independently inspected per-case RUN/PASS/SKIP inventory.
Its leaf suites are not full release-build startup or Windows arm64 evidence; the composite
Windows/DACL obligation remains UNVERIFIED. Do not turn capability skips into PASS.

The executor must use an identified real Windows host and native Go toolchain matching the
candidate (`go.mod` currently requires Go 1.27.3). Record Windows build, architecture, filesystem and
volume-root security, effective user SID/token/elevation, Developer Mode/symlink capability, Go
version/settings, source commit/manifest and actual executable hashes. Use isolated disposable
test data and cached dependencies; externally deny and verify nonlocal network access. Native
execution is required; cross-compilation or POSIX mode checks do not establish DACL behaviour.

The shared `secretfile` suite uses `owner_windows_test.go` helpers to inspect real protected DACLs;
that file also contains `TestOwnerOnlyDescriptorOracleUsesBinarySIDs`, which checks the
binary-SID descriptor oracle separately from the six audit parents below. The six parents in
`audit_windows_test.go` set real descriptors and cover a second allow ACE, an allowed deny ACE,
creation rights on the holding directory, five dangerous ancestor rights, allowed sibling
directory creation and an actual `cmd /c mklink /J` junction. `audit_acl_test.go` supplies synthetic
judgement/reparse cases. Keep native observations and synthetic inputs distinct.

The following PowerShell commands are **proposed data, not executed commands or permission to run**.
The executor must first receive admission for the exact host, isolation and resource profile.
Go's `-timeout=10m` limits test-binary runtime, not compilation or whole-command wall time. The
proposed 10-minute whole-command ceiling additionally needs an admitted external wall-clock and
cleanup fence; neither is provided or executed here. Run from the accepted source root with separate
output files, no inherited GOOS/GOARCH override and an already provisioned dependency cache:

```powershell
$env:GOTOOLCHAIN = 'local'
$env:GOPROXY = 'off'
$env:GOSUMDB = 'off'
go test -json -count=1 -timeout=10m ./internal/core/secretfile 1> secretfile-windows.jsonl 2> secretfile-windows.stderr
$secretfileExit = $LASTEXITCODE
go test -json -count=1 -timeout=10m ./internal/core/node -run '^(TestAPinStore|TestARefusedPinStore|TestAServiceWithAnIntactPinStore)' 1> pin-startup-windows.jsonl 2> pin-startup-windows.stderr
$startupExit = $LASTEXITCODE
```

Preserve full stdout/stderr and the two native exit codes, elapsed time, every RUN/PASS/FAIL/SKIP and
source/binary identity. `fsprobe.RequireSymlinks` can skip when the token cannot create real
symlinks; `RequireWriteRefused` can skip when an elevated token bypasses a denial. A skipped case
does not satisfy its obligation. Choose a suitable admitted token/profile for each missing case,
report each run separately and keep earlier failures. The junction case fails on a failed `mklink`;
it is not one of those capability skips.

The selected Node tests include a **synthetic** foreign-owner verdict and a synthetic unsafe-marker
verdict. They do not prove native ownership transfer. `TestARefusedPinStoreStopsRunBeforeTheListener`
uses an inconsistent/missing-store marker to establish Service.Run's refusal order; it does not
by itself prove every real DACL violation reaches that order. A complete Windows admission report
must additionally map real file/marker/directory owner and DACL violations, ancestors through the
volume root, reparse substitution, safe restart with existing pins and damaged-store refusal to
release-build startup checks before node/RPC/pprof listeners. Mark any missing fixture or path as
uncovered and prepare an affected implementation/test change rather than declaring completion.

`TestUnsupportedDurableBackendRefusesBeforeMutation` checks that Windows durable write/remove
refuse and preserve the original bytes. It does not admit C3 pin removal or its external broker.
The existing POSIX real-owner receipt also closes only its own POSIX subcase. Native Windows
results supplement `unsafe_pin_store_refusal`; they cannot alone set that condition, or all S6,
to `done`. Final notes and candidate rechecks remain required by the manifest.

## Русский

### Статус и исходники

Пакет готовит криптопроверку двух моделей `crypto_review` и Windows-часть
`unsafe_pin_store_refusal` в [манифесте выпуска](../../packaging/release-admission.json).
Здесь нет итогового заключения двух моделей, нового Windows-прогона или полных DACL-свидетельств,
разрешения ресурса, deployment или выпуска.

Полученный source-only отчёт Claude Fable относится к protocol 31/minimum 28,
commit **`2c5262a14510e26b18688d5c06821af4fe2e81d3`**, Git tree
`cb99235e29df34688517477f9cc245aa27be4333`, Go 1.27.3; база сравнения — v2.5.71.
[Оригинал](../refactoring/report/corsa-crypto-review-2c5262a1-2026-11-02.md)
и [перевод](../refactoring/report/corsa-crypto-review-2c5262a1-2026-11-02.ru.md) —
свидетельство одной модели, а не два отдельных ревью. Они не покрывают поздние
незакоммиченные исправления или посторонние staged-правки. Упоминание эксперта-человека
в исходном отчёте относится к прежнему процессу: его заменило решение владельца
выше. Атрибуцию и ограничения исходного отчёта сохраняем.

MSG-1/2/3 и TRUST-1 остаются открытыми High. Воспроизведение атак на сообщения
и штатные тесты полного store не являются измерением скорости удалённого
заполнения pin. Бинарники, deployment, живая сеть, Windows, Tor и Android
не исполнялись. Чистый результат handshake не доказывает безопасность
потребителей сообщений и постоянного доверия.

После исправлений обеим моделям передаётся один новый полный SHA и точный diff
от проверенного среза. Каждая модель называет его в отчёте или атрибутируемом
дополнении по затронутым путям. Текущие статусы и оставшиеся свидетельства
указаны в разделе о допуске выше.

Прежний MAIN2047 поверх `c17a4961689c9e8c33bd5681ea7c7ec577bb53cf`
(SHA-256 манифеста `4dee2cbd54685ac0076afa325e0c65bf0326ffd465f28f71cb7ce069910efb80`)
сохранён как история подготовки, а не текущий исходный срез.

Принимаемый отчёт называет полный 40-символьный commit кандидата, фактические исходники и
проверенные binary inputs (явно указать отсутствие binary review при source-only экспертизе).
Предыдущие входы `ac53899e47970217662f6e45fe199196ea184072`, опубликованный
v2.5.71 (`f8139aa949df89e3f51f0a240eaf440b6f171255`) и AI-отчёт по
`52cdd9af7ea059a69f479fd963698499ca8ec131` остаются историческими.
Их покрытие нельзя считать приёмкой исправленного S6-кандидата.
Не подменять точный коммит меняющейся веткой.

Предварительную проверку этого среза нельзя переименовать в final-candidate acceptance.
[Гейт выпуска](../release.md) требует всех условий и отдельного решения владельца о конкретной
версии. После проверенного candidate commit без повторения затронутых проверок разрешены только
манифест, notes этой версии и manifest-pinning test. Ignored/untracked тоже считаются изменениями,
кроме двух документированных caches: для выпуска нужно отдельное чистое дерево.

### Крипторевью двух моделей

Исходный scope — [контракт с опубликованными векторами](../protocol/session_v2.md) и четыре строки
таблицы English выше: TLS/certificate policy; точные байты proof и parser; handshake/publication
в `sessionv2` и `node/session_secure.go`; relay/reflection controls. Таблица также охватывает canonical/small-order ключи,
records/liveness, downgrade/persistence и обычные SDK/Desktop trust/storage consumers.
`dm-v1` outer message ID и `DeliveryReceipt` не являются подписанными content receipts;
каждая модель оценивает malicious authenticated-relay/replay предпосылки и реальные status/retry гарантии,
не приравнивая tuple/hop-проверку к end-to-end доказательству доставки. Это не разрешает активацию
отложенных механизмов или перепроектирование wire protocol без конкретного finding.
Дополнительные реальные зависимости каждая модель включает в отчёт явно.

TLS ограничен 1.3, ALPN — `corsa/2`, tickets и client cache выключены. Сертификат намеренно не
аутентифицирует identity: Ed25519 proof связывает её с exporter именно этой TLS-сессии. Каждая модель
сверяет `EXPORTER-corsa-session-v2`, пустой context, 32 байта, DOMAIN с сетью, role 0x01/0x02,
key/address binding intro, ожидаемого пира, строгий frame и отказ до публикации proof. Формула
signed transcript приведена выше. Каждый потребитель proven identity должен сохранять эту границу.

Атакующий может владеть пиром, завершать две TLS-сессии и ретранслировать сообщения, менять или
блокировать first-contact traffic, подделывать неподтверждённые hints и предъявлять другую identity.
Сохраняется ограничение первого контакта: где legacy разрешён и пара ещё не доказала v2,
on-path атакующий может удерживать её в legacy. Identity не доказывает оператора, не даёт
Sybil-стойкости или анонимности. Компрометация endpoint key/ОС и злонамеренный доверенный
администратор вне этих гарантий; дополнительные предпосылки каждая модель указывает явно.

Исходники тестов не являются итоговым заключением ревью. Tor timing, измеренные loopback budgets,
Windows filesystem behaviour, C3 deployment и закрытие legacy-атаки для всей сети этим пакетом не
доказываются. От каждой модели нужен отдельный атрибутируемый отчёт с точным commit40, reviewer/date, scope/method/assumptions, severity,
source locations, безопасными воспроизводимыми примерами, непроверенными областями и границами.
Назвать фактически проверенный source и binary/dependency inputs, явно перечислить непроверенные binary/dependencies;
не использовать реальные identities, хранилища или сообщения пользователей. Все Critical исправляются, обе модели повторно проверяют затронутые пути на новом
кандидате; открытые finding и их решения остаются видимыми.

### Настоящий Windows-прогон

[Native Windows amd64 filesystem/logging job](https://github.com/piratecash/corsa/actions/runs/41098494028/job/124210372432)
завершился `success` для точного `ac53899`. Здесь сверено заключение job,
а не независимый полный разбор каждого RUN/PASS/SKIP. Leaf suites не доказывают
полный startup выпускаемых продуктов или Windows arm64; составное условие
Windows/DACL остаётся UNVERIFIED. Capability SKIP не считается PASS.

Исполнитель предъявляет Windows build/архитектуру, filesystem и security корня тома, SID и token/
elevation, Developer Mode/возможность symlink, Go version/settings, commit/manifest и SHA бинарников.
Нужны настоящий Windows, отдельные disposable данные и provisioned dependency cache, внешний
запрет и проверка nonlocal network. Текущий go.mod требует Go 1.27.3. Cross-compile и POSIX mode
не подтверждают DACL.

`owner_windows_test.go` предоставляет Windows helpers общим тестам и содержит самостоятельный
`TestOwnerOnlyDescriptorOracleUsesBinarySIDs`, проверяющий descriptor oracle с binary SID
отдельно от шести audit-родителей ниже. Шесть родителей `audit_windows_test.go` реально меняют DACL: второй allow ACE, допустимый
deny ACE, holding-directory до первой записи, пять опасных прав предка, допустимое создание
соседних каталогов и junction через `mklink /J`. `audit_acl_test.go` — синтетический judgement/
reparse, его нельзя выдавать за native fixture.

Две PowerShell-команды выше — **только DATA**. До исполнения отдельно допускаются host/isolation/
ресурсы. Go `-timeout=10m` ограничивает время test binary, а не компиляцию или wall-clock всей
команды. Предлагаемый потолок 10 минут для всей команды требует отдельно допущенного внешнего
wall-clock и cleanup fence; здесь они не предоставлены и не исполнены. Нельзя наследовать GOOS/GOARCH override;
нужны local toolchain и готовый cache. Сохраняются целые stdout/stderr, два exit code, elapsed,
каждый RUN/PASS/FAIL/SKIP и source/binary tuple. Symlink может быть SKIP из-за прав token,
write-denial — из-за обходящих запрет привилегий. SKIP не закрывает требование; подходящие
допущенные профили проверяются отдельно с сохранением исходных отказов. Ошибка `mklink /J` — FAIL.

В выбранных Node-тестах foreign owner и unsafe marker вводятся синтетически. Тест порядка
Service.Run использует inconsistent/missing-store marker; он сам не доказывает порядок отказа
для каждой настоящей DACL-ошибки. Полный Windows-отчёт дополнительно связывает реальные owner/DACL
файла, маркера и всех каталогов до корня тома, reparse, restart с pin и damaged store с отказом
release build до node/RPC/pprof listeners. Отсутствующие fixture/path остаются uncovered и требуют
затронутого изменения/проверки.

Windows durable-backend test доказывает отказ write/remove до мутации, а не допуск C3. POSIX
real-owner receipt относится только к своей POSIX-части. Windows evidence дополняет partial
`unsafe_pin_store_refusal`; итоговый статус требует всех подусловий, notes и candidate rechecks.


## Additional POSIX Service.Run evidence / Дополнительная POSIX-проверка Service.Run

A separate Linux arm64 Go1.27.3 CGO0 run tests the original
`NewService → StartupRefusal → Run` with actual foreign owners. The source is the
1794-file capture of `ecaebdca9b65e1dce681fea8a2791240ac5695d9` plus
`internal/core/node/service_real_owner_refusal_linux_test.go`. A capless offline container
creates the fixture as UID501, executes the sole selected parent as UID502, then checks
it again as UID501. All five RUN/PASS events passed: foreign pin-store, foreign marker,
foreign parent directory, and the own-owner first-start positive. The latter reaches a
real occupied loopback listener; the foreign cases return the original typed refusal
before listening. Original bounded service logs are retained and qualified, stderr is
empty, original background work is joined, and the foreign fixture remains unchanged.

The test is opt-in (`CORSA_S6_REAL_OWNER_SERVICE=1`) and requires the separately prepared
UID501/502 fixture; a default skip supplies no runtime evidence. The private ELF is
63,959,717 bytes, SHA256 `ec7267e9243d32015429b235107ff7d205d0df06fa0416fa79891558277a8a1c`.
TESTER: `/private/tmp/corsa-D1-S6-service-real-owner-TESTER-DATA-v1/HANDOFF.TESTER.json`,
SHA256 `7e467d3bbac8eaaa732a8ad275e40510e26371025f29fa15f9df13be0ae87f32`.
This strengthens the POSIX startup subcase only. Windows/DACL, remaining startup cases,
release notes, final-candidate checks and the S6 release decision remain required;
`unsafe_pin_store_refusal` stays partial and the release decision stays null.

Отдельный запуск Linux arm64 Go1.27.3 CGO0 проверил исходную цепочку
`NewService → StartupRefusal → Run` с настоящими чужими владельцами. Исходники —
1794 файла коммита `ecaebdca9b65e1dce681fea8a2791240ac5695d9` и новый
`internal/core/node/service_real_owner_refusal_linux_test.go`. Изолированный контейнер
без capabilities и сети создаёт fixture как UID501, выполняет единственного выбранного
родителя как UID502 и снова проверяет fixture как UID501. Все пять RUN/PASS прошли:
чужие pin-store, marker и каталог дают исходный типизированный отказ до listen;
положительный первый запуск с собственным владельцем достигает настоящего занятого
loopback listener. Исходные ограниченные логи сохранены и проверены, stderr пуст,
фоновые работы joined, метаданные и содержимое чужого fixture не изменились.

Тест требует `CORSA_S6_REAL_OWNER_SERVICE=1` и отдельно подготовленный UID501/502 fixture;
обычный skip не подтверждает исполнение. Размер и SHA256 private ELF, TESTER и его SHA256
указаны выше. Закрывается только этот POSIX startup-подслучай. Windows/DACL, остальные
startup-проверки, release notes, final-candidate проверки и решение о выпуске S6 остаются;
`unsafe_pin_store_refusal` сохраняет partial, release decision — null.

## Historical owner decision of 2026-10-23 / Историческое решение владельца от 2026-10-23

This earlier record is retained for provenance. Its unconditional Windows blocker and pre-deferral
status counts below are superseded by the current verification scope at the top; Windows remains
UNVERIFIED/deferred, not PASS. / Ранняя запись сохранена для истории. Её безусловный Windows-blocker
и статистика до отсрочки ниже заменены текущей областью в начале документа; Windows остаётся
UNVERIFIED/deferred, не PASS.

### English

A real Windows host is currently unavailable. Native Windows owner/DACL behaviour remains
**UNVERIFIED**. Cross-compilation, synthetic fixtures and POSIX results cannot close this
Windows obligation. Its absence blocks the S6 release under the existing admission terms;
the requirement is not waived or relaxed. It does not prevent committing the current
implementation area, and such a commit supplies no Windows evidence or S6 completion.

Independent cryptographic review remains **OPEN**, without an expert verdict.
`unsafe_pin_store_refusal` stays `partial`, `crypto_review` stays `not_done` with
`evidence: null`, and `release_decision` stays `null`. The manifest still has
**17 conditions: 7 done, 2 partial, 8 not_done**. The native Windows evidence specified
above, the expert review and the remaining release conditions are still required.

### Русский

Настоящий Windows-хост сейчас недоступен. Поведение владельцев и DACL на Windows остаётся
**НЕПРОВЕРЕННЫМ (UNVERIFIED)**. Cross-compile, синтетические fixture и POSIX-результаты
не закрывают это Windows-требование. Отсутствие проверки блокирует выпуск S6 по действующим
условиям допуска; требование не отменено и не ослаблено. Это не мешает закоммитить текущую
область реализации; такой коммит не подтверждает Windows и не завершает S6.

Независимая криптографическая экспертиза остаётся **ОТКРЫТОЙ (OPEN)**, заключения эксперта нет.
`unsafe_pin_store_refusal` сохраняет `partial`, `crypto_review` — `not_done` и
`evidence: null`, `release_decision` — `null`. В манифесте по-прежнему
**17 условий: 7 done, 2 partial, 8 not_done**. Описанные выше настоящие Windows-свидетельства,
экспертиза и остальные условия выпуска по-прежнему необходимы.

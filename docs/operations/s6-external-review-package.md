# S6 external review and native Windows evidence / Внешняя проверка S6 и свидетельства Windows

## English

### Status and source identity

This is a preparation package for `crypto_review` and the Windows part of
`unsafe_pin_store_refusal` in the [release manifest](../../packaging/release-admission.json).
It records review scope and proposed execution inputs. No expert verdict, native Windows run,
resource authorisation, production deployment or release approval is supplied here.

The preparation snapshot is **MAIN2047**, based on owner-committed HEAD
`c17a4961689c9e8c33bd5681ea7c7ec577bb53cf`, with additional accepted changes.
Its 2,047-file manifest SHA-256 is
`4dee2cbd54685ac0076afa325e0c65bf0326ffd465f28f71cb7ce069910efb80`.
HEAD and this working-tree snapshot are different identities. Neither is designated the final
release candidate. The manifest currently has **17 conditions: 7 done, 2 partial, 8 not_done**;
`release_decision` is `null`, `crypto_review` is `not_done` and
`unsafe_pin_store_refusal` is `partial`.

An accepted review report must name the full 40-character candidate commit and the actual source
and binary inputs reviewed. A preliminary review of this snapshot can find defects, but cannot be
relabelled as final-candidate acceptance. The [release gate](../release.md) requires all conditions
and a separate version-specific owner decision. After the checked candidate commit, only the
manifest, that version's release notes and the manifest-pinning test may change without renewed
affected checks. Ignored/untracked files count as changes except the two documented build caches;
prepare a clean release tree rather than assuming this working directory is one.

### Independent cryptographic review

Review the [session contract and published vectors](../protocol/session_v2.md), then follow these
implementation anchors from the repository root:

| Question | Source and existing test anchors |
|---|---|
| TLS policy and what authenticates identity | `internal/core/sessionv2/config.go`: `listenerConfig`, `dialerConfig`; `certificate.go`: `CertificateSource`, `mintCertificate`; `guards_test.go` |
| Exact signed bytes and strict frame | `proof.go`: `proofPayload`, `verifyProof`, `parseProofFrame`; `proof_test.go`: `TestProofPayloadAndSignaturesMatchTheVectors`, `TestProofRefusesTheNegativeVectors`, `TestProofFrameIsParsedStrictly` |
| Handshake state and identity publication | `handshake.go`: `runHandshake`, `sendIntroAndProof`, `receiveProvenPeer`, `checkIntro`; `session.go` and `proven_identity_test.go`; `node/session_secure.go`: inbound `sessionv2.Accept`, outbound `sessionv2.Dial`, `acceptProvenInboundOwned`, `provenTransport` |
| Active relay and reflection | `handshake_test.go`: `TestARelayedListenerProofDoesNotVerifyOnAnotherConnection`, `TestARelayedDialerProofIsRefusedWhereV1AcceptedIt`; `proof_test.go` negative exporter/role/key/network/v1-signature cases |

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

Existing test sources are review inputs, not an expert verdict. This package establishes no Tor
timing, measured loopback budget, Windows filesystem behaviour, C3 deployment admission or
network-wide closure of the legacy relayed-proof attack. Those retain their own conditions.

The expert's deliverable is a signed or otherwise attributable report naming the candidate commit,
scope, method, assumptions, findings by severity, reproducible examples and explicit limitations.
Every critical finding must be fixed and independently re-reviewed on the resulting candidate.
Record unresolved findings and their disposition; do not turn missing coverage into a passing result.

### Native Windows execution specification

The executor must use an identified real Windows host and native Go toolchain matching the
candidate (`go.mod` currently requires Go 1.27.3). Record Windows build, architecture, filesystem and
volume-root security, effective user SID/token/elevation, Developer Mode/symlink capability, Go
version/settings, source commit/manifest and actual executable hashes. Use isolated disposable
test data and cached dependencies; externally deny and verify nonlocal network access. Native
execution is required; cross-compilation or POSIX mode checks do not establish DACL behaviour.

The shared `secretfile` suite uses `owner_windows_test.go` helpers to inspect real protected DACLs;
that file intentionally contains no standalone `Test` functions. The six parents in
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

Пакет готовит независимую криптопроверку `crypto_review` и Windows-часть
`unsafe_pin_store_refusal` в [манифесте выпуска](../../packaging/release-admission.json).
Здесь нет заключения эксперта, настоящего Windows-прогона, разрешения ресурса, deployment или выпуска.

База подготовки — **MAIN2047** поверх закоммиченного владельцем HEAD
`c17a4961689c9e8c33bd5681ea7c7ec577bb53cf`; SHA-256 манифеста 2 047 файлов —
`4dee2cbd54685ac0076afa325e0c65bf0326ffd465f28f71cb7ce069910efb80`.
HEAD и рабочий срез различаются; ни один здесь не назначен финальным кандидатом. В манифесте
**17 условий: 7 done, 2 partial, 8 not_done**; `release_decision=null`,
`crypto_review=not_done`, `unsafe_pin_store_refusal=partial`.

Принимаемый отчёт называет полный 40-символьный commit кандидата, фактические исходники и бинарники.
Предварительную проверку этого среза нельзя переименовать в final-candidate acceptance.
[Гейт выпуска](../release.md) требует всех условий и отдельного решения владельца о конкретной
версии. После проверенного candidate commit без повторения затронутых проверок разрешены только
манифест, notes этой версии и manifest-pinning test. Ignored/untracked тоже считаются изменениями,
кроме двух документированных caches: для выпуска нужно отдельное чистое дерево.

### Независимый эксперт

Исходный scope — [контракт с опубликованными векторами](../protocol/session_v2.md) и четыре строки
таблицы English выше: TLS/certificate policy; точные байты proof и parser; handshake/publication
в `sessionv2` и `node/session_secure.go`; relay/reflection controls. Дополнительные реальные
зависимости эксперт включает в отчёт явно.

TLS ограничен 1.3, ALPN — `corsa/2`, tickets и client cache выключены. Сертификат намеренно не
аутентифицирует identity: Ed25519 proof связывает её с exporter именно этой TLS-сессии. Эксперт
сверяет `EXPORTER-corsa-session-v2`, пустой context, 32 байта, DOMAIN с сетью, role 0x01/0x02,
key/address binding intro, ожидаемого пира, строгий frame и отказ до публикации proof. Формула
signed transcript приведена выше. Каждый потребитель proven identity должен сохранять эту границу.

Атакующий может владеть пиром, завершать две TLS-сессии и ретранслировать сообщения, менять или
блокировать first-contact traffic, подделывать неподтверждённые hints и предъявлять другую identity.
Сохраняется ограничение первого контакта: где legacy разрешён и пара ещё не доказала v2,
on-path атакующий может удерживать её в legacy. Identity не доказывает оператора, не даёт
Sybil-стойкости или анонимности. Компрометация endpoint key/ОС и злонамеренный доверенный
администратор вне этих гарантий; дополнительные предпосылки эксперт указывает явно.

Исходники тестов не являются заключением эксперта. Tor timing, измеренные loopback budgets,
Windows filesystem behaviour, C3 deployment и закрытие legacy-атаки для всей сети этим пакетом не
доказываются. Нужен атрибутируемый отчёт с commit, scope/method/assumptions, severity, воспроизводимыми
примерами и границами. Все critical исправляются и повторно независимо проверяются на новом
кандидате; открытые finding и их решения остаются видимыми.

### Настоящий Windows-прогон

Исполнитель предъявляет Windows build/архитектуру, filesystem и security корня тома, SID и token/
elevation, Developer Mode/возможность symlink, Go version/settings, commit/manifest и SHA бинарников.
Нужны настоящий Windows, отдельные disposable данные и provisioned dependency cache, внешний
запрет и проверка nonlocal network. Текущий go.mod требует Go 1.27.3. Cross-compile и POSIX mode
не подтверждают DACL.

`owner_windows_test.go` предоставляет Windows helpers общим тестам и не содержит самостоятельных
Test. Шесть родителей `audit_windows_test.go` реально меняют DACL: второй allow ACE, допустимый
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

## Owner decision of 2026-10-23 / Решение владельца от 2026-10-23

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

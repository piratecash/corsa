# D1: observation calendar decision proposal / Предложение решения о календаре наблюдения

<a id="measurement-scope-2026-10-23"></a>

## Current measurement scope / Текущая область измерений — 2026-10-23

The owner subsequently resumed D1 and directed measurements on macOS/Linux, deferring real Windows/DACL and Tor checks and handling issues in those environments as they arise. This supersedes the earlier owner instruction to retain those unavailable checks as unconditional release blockers. It supplies no successful Windows or Tor evidence: their actual results remain UNVERIFIED/UNKNOWN. Schema 2 records only `tor_phase_measurement` as `deferred`, tied to this exact decision, separately from `done`. Windows is removed only from the native-Windows prerequisite of the composite unsafe-store check; POSIX and final-candidate checks remain required. Clearnet first-contact measurements and timeout derivation remain required. The gate remains closed on the other incomplete conditions and missing release decision. Independent cryptographic review, the remaining checks, numerical profiles, G2, activation and release decisions remain open. This scope decision alone authorises no new workload configuration or resource limit.

Владелец позднее возобновил D1 и указал выполнять замеры на macOS/Linux, отложив реальные Windows/DACL и Tor проверки; проблемы этих окружений решаются по мере поступления. Это заменяет прежнее указание сохранять недоступные проверки как безусловные препятствия выпуску. Свидетельств их успешного прохождения нет: фактические результаты остаются UNVERIFIED/UNKNOWN. Schema 2 записывает только `tor_phase_measurement` как `deferred` со ссылкой именно на это решение, отдельно от `done`. Windows исключается только из обязательной native-Windows части составного условия небезопасного хранилища; POSIX и проверки кандидата сохраняются. Замеры первого контакта clearnet и обоснование timeout сохраняются. Gate остаётся закрыт по остальным незавершённым условиям и отсутствующему решению о выпуске. Независимая криптоэкспертиза, остальные проверки, численные профили, G2, активация и решения о выпуске остаются открытыми. Это решение само по себе не разрешает новую конфигурацию нагрузки или ресурсные лимиты.

## English summary — calendar and spread approved on 2026-10-23

On 2026-10-23 the owner approved only the future S6 observation calendar and spread threshold. This is not a release decision, authorisation to tag, deployment, live polling, a measurement budget or G2/profile approval. The current secure-session admission remains17conditions, with Windows owner/DACL UNVERIFIED and independent cryptographic review OPEN. Implementing or committing verified work can continue.

Proposed sequence: seven-day baseline before the S6 tag;72-hour acute phase; spread phase;14days of stability; at least28days after the tag. Spread requires all three tests simultaneously for three consecutive days: at least40% of distinct connected peer identities advertise protocol_version≥31; at least30distinct identities prove v2 across anchors; each public anchor retains at least30pinned identities. At day45 without spread, the owner decides explicitly;60days is the ceiling, not automatic success.

A correction affecting sessionv2, secure_session_*, listener, ConnectionManager or delivery restarts the72-hour acute phase and14-day stability phase. Every release requires admission again and a new version-specific release decision; no emergency bypass is created. Calendar approval alone does not approve the complete K1–K17 criteria, resource thresholds or old API/metric assumptions. Those need a complete current public plan before S6; `network_observation_plan` stays incomplete until the whole requirement is met.

## Русский — решение владельца от 2026-10-23

**Утверждено владельцем 2026-10-23: только календарь и распространение.**

«Утверждаю только следующий календарь будущего наблюдения S6 и порог распространения:

1. До публикации тега S6 —7суток baseline на предыдущем выпуске; после тега —72часа острой фазы, затем распространение и14суток устойчивости. Итог возможен не раньше28суток от тега и только после14суток устойчивости.
2. Распространение достигнуто, если одновременно3суток подряд: не менее40%различных identity соединённых пиров в объединении опорных узлов объявляют `protocol_version≥31`; не менее30различных identity доказали v2 в объединении опорных узлов; на каждом публичном опорном узле не менее30pin. Заявленная версия не считается доказательством identity; переподключения не создают новые identity.
3. Если к45-м суткам порог не достигнут — отдельное решение: продлить, пересмотреть достаточность меньшей доли или признать наблюдение недостаточным. Потолок60суток не даёт автоматического PASS. Продление или изменение порога не происходит молча.
4. Исправляющий выпуск, затрагивающий `sessionv2`, `secure_session_*`, слушатель, ConnectionManager или доставку, снова начинает72часа острой фазы и14суток устойчивости. Незатрагивающий выпуск часы не сбрасывает. Каждый выпуск заново проходит допуск и требует нового решения о выпуске, привязанного к своей версии/тегу.
5. Это выбор календаря/распространения. Он не утверждает полные численные критерии наблюдения, ресурсный бюджет, конкретные узлы, credentials/live polling, прогоны, G2, активацию PR6 или выпуск S6/D1. До S6 необходим отдельный полный актуальный публичный план критериев и способов сбора».

## Полный критерийный план остаётся отдельным

Нельзя заменить K1–K17 несколькими удобными метриками. Обязательный перечень: K1v2-доля, K2legacy при заявленной v2, K3распространение, K4исходы дозвона, K5отказы слушателя, K6pin-capacity, K7endpoint-привязки, K8downgrade/backoff, K9store/время, K10соединения/разрывы, K11delivery-канарейка, K12delivery/очереди, K13ресурсы, K14падения/отказы старта, K15жалобы, K16старая сторона, K17TLS-first switch. Наличие и точную семантику источников сверяют на выбранном кандидате, а не выводят из старого черновика.

Сохранить в полном плане правила исхода: нет открытых STOP в последние14суток; пороговые нарушения в норме на7-суточном окне или разобраны с записанной внешней причиной; все review-сигналы разобраны и записаны; пороговое нарушение разбирается за≤48часов. STOP требует немедленного сообщения владельцу и блокирует положительный итог до исправления. Численные пороги, включая CPU/RSS/латентность, требуют baseline/noise и отдельного утверждения; настоящим предложением они не выбраны.

Минимальные предлагаемые роли опорных узлов: clearnet, Tor, NAT/Desktop и контрольный старый выпуск. Реальные hosts, ответственный за сбор, доступ и live polling допускаются отдельно. Collector должен быть проверен на старом выпуске до начала baseline; это не запуск сети данным документом.

Отдельный предлагаемый policy-text для репозитория: защитить теги `v*` ruleset-ом — создание, изменение и удаление только владельцем, без force-update. Правило не заменяет17условий [допуска](../../packaging/release-admission.json), version-specific release decision или [порядок выпуска](../release.md). [Пакет S6](s6-external-review-package.md) сохраняет WindowsUNVERIFIED/cryptoOPEN. Календарное решение не позволяет пометить весь `network_observation_plan` done и не является разрешением выпуска.

Решение получено от владельца 2026-10-23. Windows/DACL остаётся непроверенным, независимая криптоэкспертиза открыта. Владелец отдельно подтвердил: полный план критериев, ресурсы и разрешение выпуска согласуются позднее; доступная работа и подготовка проверенной области к коммиту продолжаются. Утверждение календаря не закрывает весь `network_observation_plan`.

# S6: соответствие текущей области решений владельца

Дата: 2026-11-02. [English](s6-active-session-scope-conformance-2026-11-02.md).

Проверены обычные точки создания демона, Desktop и SDK, выключенное состояние
K2/R1b/R2/N-in и отказы публичных разрушительных операций. Эти механизмы
**отложены, а не завершены**. Это свидетельство для согласованной
[области выпуска](urgent-full-product-release-scope.ru.md), не полная приёмка
`owner_session_choices`, S6 или разрешение выпуска. Эта сверка сама по себе не меняет алгоритм gate и не закрывает условия выпуска.

## Исходники и перенос свидетельств

Проверяемый код — `ac53899e47970217662f6e45fe199196ea184072`.
HEAD `2c5262a14510e26b18688d5c06821af4fe2e81d3` отличается от него только четырьмя
Markdown-файлами: отчётами Common32 и pin-write, пакетом внешней экспертизы
и предложением pin-write профиля. Go-код, тесты, модули и CI scripts не менялись.
Это перенос свидетельств для неизменённых исходников, не новая сборка или запуск.

| Решение | Проверенное состояние и исходная точка |
|---|---|
| K2/R1b выключены | [NewService](../../internal/core/node/service.go#L1961) проходит через пустой `recordAdmissionConstruction{}`; [NewServiceWithNetwork](../../internal/core/node/service.go#L2933) использует тот же путь. Явные private candidate factories не выбираются обычной конфигурацией или RPC. |
| Демон и Desktop используют обычный путь | [App.New](../../internal/app/node/app.go#L34) передаёт nil; [нулевой ObservationOptions](../../internal/core/node/s6_terminal_construction.go#L153) делегирует в NewService. [Desktop](../../internal/app/desktop/app.go#L145) вызывает NewService непосредственно. |
| R2 выключен в обычном SDK/клиенте | [NewWithContext](../../sdk/runtime.go#L242) передаёт nil profile; обычные [Node](../../sdk/runtime.go#L300) и [DesktopClient](../../sdk/runtime.go#L317) не получают own-DM port. [NewServiceForRuntime](../../internal/core/node/runtime_unstarted_cleanup.go#L13) делегирует в NewService. Source opt-in API существуют, но не являются активацией поставляемых продуктов. |
| N-in выключен | Обычная конструкция не устанавливает inbound producer или journal и не мигрирует store в format3. Обязательное обычное pinning остаётся доступным; это не N-in activation. |
| Активные pins сохраняют защиту | [Общий предел](../../internal/core/node/secure_session_store.go#L80) — 20 000 identity. Новая identity при заполнении отвергается без вытеснения прежних и без автоматического legacy fallback. |
| C3 removal недоступен | [ConfirmIdentityPinRemoval](../../internal/core/node/identity_pin_operator.go#L193) после проверки контекста безусловно возвращает `ErrIdentityPinRemovalUnavailable`; preview сообщает `RemovalAvailable=false`. |
| Endpoint Remove/Demote недоступны | [Remove](../../internal/core/node/endpoint_binding_operator.go#L652) и [Demote](../../internal/core/node/endpoint_binding_operator.go#L674) безусловно возвращают `ErrEndpointBindingMutationUnavailable` после проверки контекста. Чтение и preview не разрешают мутацию. |

Провижининг C3 broker/fence не требуется для выпуска с **недоступной публичной
операцией**. Это не отменяет внешнюю fencing/recovery-защиту: её реализация,
проверка всех startup paths и отдельный допуск обязательны до будущей активации
removal. Небезопасный pin store и состояние recovery по-прежнему должны запрещать
сетевой старт; недоступность removal не ослабляет эту защиту.

## Существующие проверки и пределы evidence

| Область | Конкретные существующие проверки |
|---|---|
| K2/R1b, закрытые точки активации | [TestRecordAdmissionClosedProductionConstructionAndReferences](../../internal/core/node/identity_record_admission_guard_test.go#L107), [TestRelationRecordServiceConstructorFinalStoreAndDefaults](../../internal/core/node/identity_record_admission_service_test.go#L182) |
| R2 и обычный SDK | [TestOwnDMRuntimeClosedOriginalComposition / TestOwnDMRuntimeRejectsDefaultActivationAndAlternateMint](../../internal/core/node/identity_pin_own_dm_runtime_guard_test.go#L30) проверяют исходные межпакетные пути и отрицательные варианты активации |
| N-in | [TestInboundN1DefaultConstructorsDoNotInstallOrMigrateJournal](../../internal/core/node/identity_pin_inbound_lifecycle_test.go#L1107), [TestInboundN2PublicDefaultsAndUnavailableFactoryRemainOrdinary](../../internal/core/node/identity_pin_inbound_install_test.go#L803) |
| Заполненный store и downgrade | [TestAFullPinStoreRefusesANewIdentityAndKeepsEveryExistingOne](../../internal/core/node/pin_store_full_test.go#L35), [реальная входящая сессия](../../internal/core/node/pin_store_full_test.go#L96), [отсутствие fallback у dialler](../../internal/core/node/pin_store_full_test.go#L139), [сохранность после restart](../../internal/core/node/secure_session_store_test.go#L46) |
| Недоступность мутаций | [TestIdentityPinPreviewIsReadOnlyAndRemovalRequiresFence](../../internal/core/node/identity_pin_operator_test.go#L93), [TestServiceEndpointMutationsStayUnavailableWhileReadsWork](../../internal/core/node/endpoint_binding_operator_test.go#L563); RPC и SDK дополнительно проверяют недоступность и границы transport/auth |

Для точного `ac53899` сохранён успешный conclusion
[CI Tests job](https://github.com/piratecash/corsa/actions/runs/41098494039/job/124210372902).
[Workflow](../../.github/workflows/ci.yml#L146) вызывает `make test-release`;
[script](../../scripts/test-release.sh) запускает `go test -count=1` без фильтра
имён и исключает только `internal/overlaysim`. Перечисленные обычные тестовые
пакеты входят в эту область. Это CI conclusion и проверенный состав команды,
**не загруженный per-test RAW census**. В этой сверке Go и приложения не запускались.
Локальное сохранённое свидетельство: `docs/_internal/d1-artifacts/2026-11-02/s6-ac53899-ci-resume-v1/RESULT.json`.

[Pin-write A/B](s6-pin-write-baseline-dev-2026-11-02.ru.md) дополнительно проверил
19 999→20 000, отказ следующему и reload в ограниченном компонентном сценарии.
Он не заменяет тесты настоящих session/downgrade путей и не доказывает готовность
отложенных классов или общую достаточность ресурсов.

Не закрываются этим документом: независимая криптоэкспертиза, ресурсные
loopback/first-contact проверки и их оценка, платформенный startup/DACL,
полный A-план наблюдения и итоговый кандидат/решение владельца. Windows/DACL
и Tor сохраняют записанные DEFERRED/UNVERIFIED ограничения. Будущие изменения
затронутого кода требуют повторной оценки применимости этого carry.

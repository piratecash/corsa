# Identity pin removal: owner decision for S6

## English

### Decision, 2026-10-18

Implement operator removal of **one specified identity pin** for the secure-session release.
This decision approves the behaviour below; implementation and release checks require their own
evidence. It does not approve release, bulk reset or a permanent legacy exception.

- The RPC operation is loopback-only with mandatory authentication. The UI operation is
  `InProcessOnly`.
- Before mutation, show the exact identity and warn that this removes **one of the grounds** for
  refusing downgrade. Require explicit operator confirmation and record an audit event.
- Report success only after the change has been durably saved. A write failure keeps the protection
  in force; neither the live store nor restart may silently lose the pin because a failed removal
  was reported as successful.
- Keep endpoint bindings and every other reason to require v2. Removing the identity pin does not
  promise that a legacy connection is immediately allowed.
- A later successful proof of v2 pins the identity again through the ordinary rules. Do not store
  an opt-out or permanent exception.
- There is no bulk-reset operation.

### Recovery refinement, 2026-10-18

The owner accepts `RecoveryRequired` as a distinct indeterminate outcome: keep the pin in memory,
stop networking, block further removals and do not return success. A confirmed protective rollback
preserves the pin across restart. An indeterminate outcome cannot promise that the pin was saved
on disk; this explicitly refines the write-failure rule above.

**Before enabling removal RPC**, present and implement a concrete external mechanism preventing
automatic networking restart until confirmed recovery. An instruction to the operator or another
file on the same failing storage is insufficient. Where that mechanism is not ensured, pin removal
is unavailable while ordinary node operation remains available. Functional tests must cover the
emergency outcomes and automatic restart. A different storage format alone does not solve an
indeterminate commit; the guarantee must be demonstrated under an explicit failure model.

The [persistence proposal](identity-pin-removal-persistence.md) records the transaction boundary.
The local durable transaction, external broker candidate and private supervised recovery adapter are implemented; see the [inactive candidate](../operations/identity-pin-recovery-candidate.md). The dedicated desktop confirmation dialog is implemented with unavailable production confirmation. Independent deployment provisioning, admission of every startup path and public activation remain outstanding. A local committed result is not full workflow success.

### Linux operation workflow decision, 2026-10-18

The owner approved one removal followed by node shutdown, verified exit zero and a sealed receipt,
separate authenticated external operator finalization, then permission to restart. Before mutation,
the operator must see the shutdown warning. Local persistence means **awaiting finalization**.
Final results distinguish confirmed removal from confirmed restoration of the pin: both can complete
correctly, but restoration is not successful removal. A crash or indeterminate outcome keeps the
startup fence and requires offline recovery. This approves the workflow only. Public RPC activation
still requires independently provisioned broker storage and admission of the environment and every
startup path. Removal remains unavailable in unsupported environments.

The implementation must test access restrictions, confirmation, audit, persistence across restart,
write failure and a concurrent new v2 proof. In particular, confirmation of a preview must not
silently remove a newer protection established by a racing proof. The exact concurrency and
confirmation API is subject to implementation review; the safety requirement is part of the owner
decision, not an invitation to weaken ordinary pin creation.

This concerns an **identity pin**, distinct from existing endpoint binding removal/demotion in
[Secure-session RPC](../rpc/secure_session.md). The pin-store safety and downgrade rules in
[Secure session v2](../protocol/session_v2.md) continue to apply. Final release admission remains in
[Releases](../release.md).

## Русский

### Решение от 2026-10-18

Для выпуска защищённой сессии реализуется операторское снятие **одного явно указанного identity pin**.
Утверждено поведение ниже; реализация и проверки выпуска требуют собственных подтверждений.
Решение не разрешает выпуск, массовый сброс или постоянное исключение для legacy.

- RPC доступен только на loopback с обязательной аутентификацией. Операция UI — `InProcessOnly`.
- До изменения показать точную identity и предупредить, что снимается **одно из оснований** запрета
  downgrade. Требуются явное подтверждение оператора и запись аудита.
- Успех возвращается только после надёжного сохранения. Ошибка записи сохраняет защиту; ни живое
  хранилище, ни перезапуск не должны молча терять pin из-за операции, ложно объявленной успешной.
- Endpoint-привязки и все остальные основания требовать v2 сохраняются. Снятие identity pin не
  обещает немедленного допуска legacy.
- Новое успешное доказательство v2 закрепляет identity заново по обычным правилам. Постоянное
  исключение или opt-out не создаётся.
- Массового сброса нет.

Проверки реализации включают ограничения доступа, подтверждение, аудит, сохранение после перезапуска,
ошибку записи и гонку с новым доказательством v2. В частности, подтверждение предварительного
просмотра не должно молча снимать более новую защиту, созданную конкурентным доказательством.
Конкретный API подтверждения и конкурентной операции проходит ревью; требование безопасности —
часть решения владельца, а не разрешение ослабить обычное создание pin.

Это **identity pin**, а не существующие операции удаления/понижения endpoint-привязок из
[RPC защищённой сессии](../rpc/secure_session.md). Правила безопасности хранилища и запрета downgrade
из [Защищённой сессии v2](../protocol/session_v2.md) сохраняются. Итоговый допуск выпуска описан
в [Выпусках](../release.md).

### Уточнение восстановления, 2026-10-18

Владелец принял `RecoveryRequired` как отдельный неопределённый исход: pin сохраняется в памяти,
сеть останавливается, новые снятия блокируются, успех не возвращается. Подтверждённый защитный
rollback сохраняет pin после перезапуска. При неопределённом исходе нельзя обещать сохранность
pin на диске; это явно уточняет прежнее требование при ошибке записи.

**До включения RPC снятия** нужно представить и реализовать конкретный внешний механизм запрета
автоматического сетевого перезапуска до подтверждённого восстановления. Инструкции оператору или
файла на том же отказавшем носителе недостаточно. Где механизм не обеспечен, снятие pin недоступно,
обычная работа узла доступна. Функциональные тесты проверяют аварийные исходы и автоматический
перезапуск. Другая модель хранения сама по себе не решает неопределённость commit: гарантия должна
проверяться в явно заданной модели отказов.

[Проект сохранения](identity-pin-removal-persistence.md) фиксирует границу транзакции. Конкретный
внешний broker candidate, локальная durable-транзакция и private supervised recovery adapter реализованы;
см. [неактивного кандидата](../operations/identity-pin-recovery-candidate.md). Независимое deployment
provisioning, проверка всех способов запуска и публичное включение ещё не выполнены.
Отдельный desktop-диалог реализован; production-подтверждение остаётся недоступным. Локальный committed не означает полного успеха workflow.

### Решение о порядке Linux-операции, 2026-10-18

Владелец утвердил: одно снятие → остановка узла → подтверждённый exit zero и запечатанная квитанция
→ отдельная аутентифицированная финализация внешним оператором → разрешение перезапуска. До операции
оператор видит предупреждение об остановке. Локальное сохранение означает **ожидание финализации**.
Итог различает подтверждённое снятие и подтверждённое восстановление pin: оба могут корректно
завершиться, но восстановление не считается успешным снятием. Аварийный или неопределённый исход
сохраняет запрет запуска и требует offline recovery. Утверждён только порядок операции. Включение
публичного RPC по-прежнему требует независимого размещения broker и проверки окружения и всех
способов запуска. В неподдерживаемых окружениях снятие недоступно.

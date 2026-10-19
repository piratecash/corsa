# Desktop identity pin review

Settings → Advanced → “Remove an identity pin…” opens a dedicated dialog. Enter one exact 40-character lowercase hexadecimal identity, including a pinned identity that is not a contact. The dialog never chooses the local identity automatically and offers no bulk selection or pin-listing command. Enter requests a preview only.

The preview shows the exact identity, original pin timestamp and reviewed consequence warning. Confirmation requires a separate acknowledgement and a separate confirm gesture after presentation. The UI accepts consequence version 2 only together with the exact known canonical consequence; an unknown version or changed text fails closed. English and Russian have reviewed UI text; the other supported languages explicitly use the English fallback for this feature.

The warning explains that removing one pin leaves endpoint bindings and every other v2 requirement intact, does not immediately authorize legacy, and allows later proof to pin the identity again. Before confirmation it also explains the stop, one-operation and external authenticated operator-finalize requirement. A local committed result is displayed as **pending**, never as completed removal. Retained or restored protection is not successful removal; recovery-required is a separate state.

**Removal remains unavailable in production.** The dialog uses only the existing `TransportInProcessOnly` preview/confirm commands. It respects `removal_available=false`, disables confirmation and does not change public RPC or Service admission. An available fake provider is used solely by isolated interaction tests. Independent deployment provisioning and admission of all startup paths remain required before activation; this UI supplies no activation override.

The opaque confirmation token is private transient UI state. It is never rendered, copied, logged, added to console history or persisted. Editing the identity, closing the dialog or replacing a request cancels the old request and discards its credential. Fresh bounded reply channels and generation checks reject late results. Closing an already dispatched confirmation does not prove transaction cancellation. See [UI ownership](../locking.md#desktop-identity-pin-dialog) and the [inactive recovery candidate](identity-pin-recovery-candidate.md).

---

# Проверка pin идентичности в Desktop

Настройки → Дополнительно → «Снять pin идентичности…» открывают отдельный диалог. Введите одну точную идентичность из 40 строчных шестнадцатеричных символов, в том числе сохранённую идентичность вне списка контактов. Диалог не подставляет собственную идентичность автоматически и не предоставляет массовый выбор или команду перечисления pins. Enter запрашивает только предварительный просмотр.

Просмотр показывает точную идентичность, исходное время установки pin и проверенное предупреждение о последствиях. Подтверждение требует отдельной отметки согласия после показа предупреждения и отдельного нажатия подтверждения. UI принимает только версию последствия 2 вместе с точным известным каноническим текстом; неизвестная версия или изменённый текст блокируют подтверждение. Есть проверенные английский и русский тексты; для остальных поддерживаемых языков явно используется английский fallback этой функции.

Предупреждение объясняет, что снятие одного pin сохраняет endpoint-привязки и остальные требования v2, не разрешает немедленный legacy и допускает повторный pin при следующем proof. До подтверждения также показаны требования остановки, одной операции и внешнего finalize аутентифицированным оператором. Локально зафиксированный результат показан как **незавершённая операция**, а не успешное снятие. Сохранённая или восстановленная защита не означает успешное снятие; необходимость recovery показана отдельно.

**В production снятие остаётся недоступным.** Диалог использует только существующие команды preview/confirm с `TransportInProcessOnly`. Он соблюдает `removal_available=false`, отключает подтверждение и не меняет допуск публичного RPC или Service. Доступный подставной провайдер используется только в изолированных тестах взаимодействия. До активации всё ещё нужны независимое provisioning окружения и допуск всех способов запуска; UI не предоставляет обхода активации.

Непрозрачный token подтверждения — закрытое временное состояние UI. Он не отображается, не копируется, не логируется, не попадает в историю консоли и не сохраняется. Изменение идентичности, закрытие или замена запроса отменяют старый запрос и удаляют credential. Отдельные ограниченные reply channels и проверка поколения отбрасывают поздние результаты. Закрытие уже отправленного подтверждения не доказывает отмену транзакции. См. [владение UI](../locking.md#desktop-identity-pin-dialog) и [неактивный recovery candidate](identity-pin-recovery-candidate.md).

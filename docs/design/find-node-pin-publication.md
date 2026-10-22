# Dormant pin memory publication / Неактивная публикация памяти pin

## English

> This section records the pin-only generation. The current exact importer
> set is extended to pin+trust by [trust publication](find-node-trust-publication.md);
> the pin memory bodies and opaque leaf API are unchanged.
> Ниже описано исходное pin-поколение. Текущий точный набор импортов расширен
> до pin+trust в [описании trust](find-node-trust-publication.md);
> тела pin-публикаций и API leaf сохранены.

A secure-session store can now receive a concrete immutable
`findnodebridge.Publisher` in its private initial constructor literal. Ordinary
store construction passes the zero value. The private nonzero composition is
used only by focused store tests; Service construction, transport, Ready,
configuration and live intake do not install it.

The central `putPinEntryLocked` and `deletePinEntryLocked` acquire one nonclock
publication after their caller's existing store, writer and stage waits. Their
original map and tally operations are unchanged. Completion releases W and
invalidates an earlier capture. The zero Publisher skips lease allocation,
never the map or tally operation. There is no callback, clock read, I/O or new
lock acquisition inside these guarded bodies. ACK and mirror loops acquire no
outer W, so their central put cannot recursively acquire W.

An uncertain rollback's fatal recovery pointer has its own publication after
the final store lock. The joined error and immutable recovery object are
prepared before W; logging and durable I/O stay outside it. Poison, closed
Done, construction and generation exhaustion cannot veto mandatory memory
cleanup. This does not change removal outcomes, durability, journal accounting,
metadata revisions, owed completion, quotas, class eligibility or expiry.

The historical leaf test selector `TestDomainHasNoExistingProductionImporter`
now asserts exactly one unaliased importer, `secure_session_store.go`. The new
Node source guard closes the initial literal, zero ordinary wrapper, helper
body and exact three publication callers, including aliases and signatures.
It supersedes only the former no-importer invariant, not the opaque leaf API.

Tests exercise direct real store/leaf composition, map/tally preservation,
reader/writer lock ordering, poisoned cleanup, a real strict durable snapshot
and COW ACK, stale captured revisions, and the actual fatal rollback leaf via
its existing durable failure port. These are focused store tests, not a live
C5 bridge. Unsupported strict backends remain explicit skips. Sources and
causal recipes are authored only; compilation, formatting and runtime results
are not claimed.

Full trust/F1/terminal and temporal closure, constructor plumbing, exclusive
consumer scope, owned intake cuts, startup policy, G2, activation and resource
acceptance remain separate. The separate standalone leaf's previous evidence
and all earlier candidate generations are retained.

## Русский

Хранилище сессий получает конкретный неизменяемый
`findnodebridge.Publisher` только в исходном литерале приватного конструктора.
Обычный конструктор передаёт нулевое значение. Ненулевая композиция используется
только направленными тестами хранилища; Service, транспорт, Ready, настройки и
живой intake её не устанавливают.

Центральные `putPinEntryLocked` и `deletePinEntryLocked` получают одну
публикацию без часов после прежних ожиданий store, writer и stage. Исходные
операции карты и tally сохранены. Завершение освобождает W и делает прежний
capture устаревшим. Нулевой Publisher пропускает только выделение lease, а не
изменение карты или счётчиков. Внутри W нет callback, часов, I/O или нового
получения блокировки. Циклы ACK и mirror не получают внешний W, поэтому
центральный put не захватывает его рекурсивно.

Fatal-указатель recovery при неопределённом rollback публикуется отдельно после
последнего store lock. Объединённая ошибка и неизменяемый recovery-объект
создаются до W; журналирование и дисковые операции остаются снаружи. Poison,
закрытый Done, состояние construction и исчерпание generation не запрещают
обязательную очистку памяти. Исходы удаления, durability, журнал квот, ревизии
metadata, owed completion, квоты, классы и сроки не меняются.

Историческое имя `TestDomainHasNoExistingProductionImporter` сохранено, но
проверяет ровно один неалиасный импорт в `secure_session_store.go`. Новый
SOURCE guard Node закрывает исходный литерал, нулевой обычный конструктор,
тело helper и ровно три места публикации, включая ссылки через aliases и
сигнатуры. Меняется только прежнее требование отсутствия импортов; API leaf
остаётся прежним.

Тесты проверяют прямую композицию настоящих store и leaf, карту и tally,
порядок блокировок читателя и писателя, обязательную очистку после poison,
настоящую строгую durable-запись с COW ACK, устаревшие ревизии и fatal-ветку
rollback через существующий порт ошибки хранения. Это тесты хранилища, а не
живой мост C5. Неподдерживаемый strict backend явно пропускается. Сейчас
подготовлены только исходники и причинные рецепты; компиляция, форматирование
и результаты запуска не заявляются.

Полное замыкание trust/F1/terminal и времени, передача зависимости через
Service, исключительный consumer scope, owned intake cut, стартовая политика,
G2, активация и ресурсная приёмка остаются отдельными. Прежние свидетельства
standalone leaf и все исходные поколения сохранены.

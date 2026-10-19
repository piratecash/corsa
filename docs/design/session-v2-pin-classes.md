# Secure session: pin classes and dial consumers

## English

### Implementation boundary

This records remaining S6 work; it does not activate a new pin policy or change a dial path.
The existing implementation pins every successfully published v2 identity, refuses a new identity
at the flat 20,000-pin limit without evicting existing pins, and keeps endpoint binding limits
separate. The protected endpoint total of 32,768, including the 8,192-place K reserve, is already
implemented. It is not a reserve for identity pins. Received identity-record `min_session` does not
yet install class-based protection. See [the current session contract](../protocol/session_v2.md).

The remaining design distinguishes K (contacts), N-sel (selected outbound neighbours), N-in
(inbound neighbours), and R (relations established by eligible outgoing actions). Classification,
provenance producers, class quotas, admission of received records and duration-dependent priority
are not implemented by the existing flat limit. The owner rejected automatic eviction of existing
identity pins on 2026-10-03; a class implementation must preserve that decision.

### Owner decision on the total bound, 2026-10-18

The owner approved a total of **20,000 unique identity pins**, including K and old unclassified
records. K is exempt only from a class quota; N-sel/N-in/R retain limits of 4,096/4,096/8,192.
All existing pins survive migration without invented provenance. Re-proving or classifying an
already pinned identity consumes no additional place in the total. Pins awaiting classification
also count toward the total.

At capacity, a new identity, including a contact, receives an explicit refusal without eviction or
automatic fallback to legacy. Existing pinned identities continue to be served. On a fresh store,
the other classes' maximum quotas leave 3,616 places for K; this is arithmetic, not a guaranteed
free reserve, especially after migration. The owner accepts the tradeoff: a new contact cannot
be pinned in a full store until the operator frees a place through an authorized operation.
This resolves the earlier conflict between unbounded K and the 20,000-pin storage estimate.
Class admission and provenance producers remain outstanding. The reviewed first storage increment
implements explicit format-2 metadata, format-1 migration to unclassified, preview invalidation and
full-entry recovery backup without enabling class policy; see [the metadata boundary](session-v2-pin-metadata.md).

### Remaining boundaries before implementing classes

- **Immediate protection and session age:** publication currently requires saving the identity pin
  immediately. N-sel/N-in classification refers to ten minutes of a live proven session. A future
  model must distinguish protection already installed from eligibility for a class or service
  priority. An early disconnect must never remove a pin merely because the session did not reach
  ten minutes. Admission before that point, quota accounting and restart semantics must be explicit.
- **Migration:** existing pins have no class provenance. Do not infer a contact, selected neighbour
  or outgoing relation from an endpoint binding, an address map, a remote claim or missing metadata.
  Preserve every old pin; specify how unclassified pins consume future quotas and how a new verified
  local fact can promote their provenance. A separate class map must not duplicate the pin owner.
- **Incoming recovery is not R:** sender-key recovery triggered by inbound traffic, incoming DM
  and foreign announcements do not create R. A received signed record alone does not establish
  an eligible outgoing action. Lookup provenance must survive through asynchronous completion.

These are implementation/design boundaries, not findings of active class-policy code. The total-bound
decision above authorizes no eviction or automatic provenance inference. Remaining migration and
admission semantics need an explicit reviewed implementation contract.

### Dial consumer matrix to complete

| Current socket path | Current intent | Decision needed before a named consumer is added |
| --- | --- | --- |
| Connection manager | Address | Distinguish selecting an address from a local action that requires one exact identity; a remembered mapping alone is insufficient. |
| `syncPeer`, including sender-key recovery | Address | Preserve an explicit caller's expected identity when the action actually requires it; distinguish a recovery neighbour from the message sender and keep inbound recovery outside R. |
| Notice dial | Address | Identify whether the local action addresses a specific identity or a transport endpoint; a gossip target does not establish that by itself. |

`senderKeySyncCandidates` currently projects live-session information to addresses. That projection
does not prove that every subsequent dial intends to reach the message sender, or that it should
create a relation. Each consumer needs an explicit purpose and expected-peer decision rather than
a module-wide replacement of `DialToAddress`.

In dual/v2-only mode, `DialToIdentity` already requires v2, including for an unpinned identity with
TLS-first disabled. Such a consumer therefore changes legacy compatibility as well as expected-peer
checking; contact dials can also create permanent endpoint protection. The existing production guard
must be replaced with a precise reviewed consumer allowlist and corresponding functional tests,
not simply removed. Legacy-only transport supplies no proof of the named identity.

## Русский

### Граница реализации

Это оставшаяся работа S6; документ не включает новую политику pin и не меняет пути дозвона.
Существующий код сохраняет pin каждой успешно опубликованной v2-identity, отказывает новой identity
при плоском пределе 20 000 без вытеснения прежних и отдельно ограничивает endpoint-привязки.
Предел 32 768 protected endpoint, включая резерв K на 8 192 места, уже реализован. Это не резерв
identity pins. Полученный `min_session` записи пока не устанавливает защиту по классам. См.
[действующий контракт сессии](../protocol/session_v2.md).

Оставшийся дизайн различает K (контакты), N-sel (выбранные исходящие соседи), N-in (входящие соседи)
и R (отношения из допустимых исходящих действий). Классификация, происхождение, классовые квоты,
приём записей и приоритет по длительности не реализованы плоским пределом. Владелец
2026-10-03 запретил автоматическое вытеснение существующих identity pins; классы сохраняют это решение.

### Решение владельца об общем пределе, 2026-10-18

Владелец утвердил общий предел **20 000 уникальных identity pins**, включая K и старые
неклассифицированные записи. K освобождён только от классовой квоты; N-sel/N-in/R сохраняют
пределы 4096/4096/8192. При миграции все существующие pin сохраняются без выдуманного происхождения.
Повторное доказательство или классификация уже закреплённой identity не занимают нового места
в общем лимите. Pin до получения класса также учитываются в общем пределе.

При заполнении новая identity, включая контакт, получает явный отказ без вытеснения и
автоматического fallback на legacy. Уже закреплённые identity продолжают обслуживаться.
На свежем хранилище максимальные квоты остальных классов оставляют K 3616 мест; это арифметический
остаток, а не гарантированно свободный резерв, особенно после миграции. Владелец принимает
компромисс: новый контакт нельзя закрепить в полном хранилище, пока оператор не освободит место
предусмотренным способом. Противоречие неограниченного K и оценки на 20 000 pin этим устранено.
Классовый допуск и создание происхождения ещё не реализованы. Первый проверенный срез хранения
реализует явные метаданные format 2, миграцию format 1 в unclassified, инвалидирование preview и полный
backup записи для recovery без включения классовой политики; см. [границу метаданных](session-v2-pin-metadata.md).

### Оставшиеся границы до реализации классов

- **Немедленная защита и стаж:** публикация сессии сейчас требует немедленного сохранения identity pin.
  N-sel/N-in описаны через десять минут живой доказанной сессии. Будущая модель должна отделять
  установленную защиту от допуска к классу или приоритету обслуживания. Ранний разрыв не снимает pin
  из-за недостигнутого стажа. Допуск до десяти минут, учёт квот и поведение при рестарте задаются явно.
- **Миграция:** у старых pin нет классового происхождения. Контакт, выбранный сосед или исходящее
  отношение не выводятся из endpoint-привязки, адресной карты, удалённого заявления или отсутствия
  метаданных. Все старые pin сохраняются; нужно определить учёт неклассифицированных pin в квотах
  и повышение происхождения по новому проверенному локальному факту. Отдельная карта классов не
  должна дублировать владельца состояния pin.
- **Входящее восстановление не является R:** восстановление ключей отправителя из входящего трафика,
  входящие DM и чужие анонсы R не создают. Одна подписанная запись не доказывает допустимое исходящее
  действие. Происхождение lookup сохраняется до асинхронного завершения.

Это границы будущей реализации, а не ошибки действующего кода классов. Решение об общем пределе
не разрешает вытеснение или автоматическое приписывание происхождения. Оставшейся семантике
миграции и допуска нужен явный проверенный контракт реализации.

### Матрица consumers дозвона, которую нужно завершить

| Действующий socket path | Намерение сейчас | Решение перед добавлением именованного consumer |
| --- | --- | --- |
| Менеджер соединений | Адресное | Отделить выбор адреса от локального действия к точной identity; одной сохранённой адресной карты недостаточно. |
| `syncPeer`, включая восстановление ключей отправителя | Адресное | Передавать ожидаемую identity, когда действие действительно требует её; отличать соседа восстановления от отправителя сообщения, не создавать R из входящего восстановления. |
| Дозвон уведомления | Адресное | Определить, обращается действие к конкретной identity или транспортному endpoint; одна цель gossip этого не устанавливает. |

`senderKeySyncCandidates` сейчас проецирует данные живой сессии в адреса. Из этой проекции не следует,
что каждый дальнейший дозвон обязан достичь отправителя сообщения или создать отношение. Каждому
consumer нужны явные назначение и ожидаемый peer вместо массовой замены `DialToAddress`.

В dual/v2-only `DialToIdentity` уже требует v2, в том числе без pin и при выключенном TLS-первым.
Поэтому consumer меняет совместимость с legacy наряду с проверкой ожидаемого peer; дозвон к контакту
также может создать бессрочную защиту endpoint. Существующий production-сторож заменяется точным
проверенным списком consumers и функциональными проверками, а не просто удаляется. Legacy-only
транспорт доказательства именованной identity не даёт.

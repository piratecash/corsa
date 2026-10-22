# Dormant trust publication / Неактивная публикация trust

## English

The private trust constructor can receive one immutable concrete
`findnodebridge.Publisher` in its initial literal, before load, refusal or save.
The existing ordinary constructors keep their signatures and forward zero.
There is no Service/options plumbing, late setter, Ready, live intake root or
activation. A zero Publisher skips only the no-op lease allocation.

The reviewed nine families now have concrete treatment. Construction and the
load-error latch remain unescaped and perform no W around I/O. The eight live
writer families use nine final memory leaves: remember refresh and add, forget,
record merge into a contact, per-row last-online replacement, bound local fact
stage, strict local-contact ACK, genuine proof cache and promotion proof-clear.
Existing evidence, save and trust mutex waits precede W. Row/token preparation,
COW allocation, key/current-owner checks, randomness and the existing wall-clock
callback precede W. Snapshotting, record/session cache work, store pin staging,
logging, callbacks and saves stay outside W. No new clock API is used.

A staged fact publishes its revision, fact, row and observed latch together.
Strict ACK obtains W only after actual `DurableCommitted`, the final trust lock
and both captured/current stamp checks. Its COW object is prepared before W;
an old published object is not mutated. Failed, unknown or stale work never
confirms a new fact. Remember/forget retain their previous live-memory result
when the later save fails. Poison, closed Done and generation exhaustion do not
veto mandatory trust writes. The existing leaf primitive still supplies that
cleanup guarantee; no availability check or Publisher API is added here.

This is a conservative contact-memory projection. Last-online changes invalidate
a capture even though they do not change K eligibility. Rejected/older online
observations and key conflicts do not publish a contact change. Persistent
record and session-cache transactions are not made atomic with the contact
projection, and no new security, expiry, class or transaction policy is implied.

The historical leaf importer selector now requires exactly two unaliased files:
`secure_session_store.go` and `trust.go`. The pin guard extends its global API
multiset only by `trust.go:beginTrustPublication`; its pin windows, constructor
and fatal memory body stay unchanged. The additional trust guard closes its
constructor dependency, exact nine helper sites and complete bounded W windows,
including signatures and aliases. No wildcard exemption is used.

Focused tests compose real trust writers with a private fixture domain. They
cover constructor/load and zero behavior, writer generation, failed saves,
actual strict durable ACK/COW, unconfirmed outcomes, stale replacement facts,
genuine TLS proof cache and existing producer proof-clear, last-wait order and
read-reservation blocking after I/O. A small direct Service method fixture is
used only for the existing promotion method; no Service constructor or worker
is wired to a Publisher. Strict-backend skips remain explicit. Tests and causal
recipes are SOURCE only: no execution or runtime PASS is asserted.

Full F1/registry/common/degree and temporal writer closure, constructor Ready
and shared domain plumbing, exclusive consumer scope, owned intake cut, G2,
activation and resource acceptance remain separate. Pin publication and leaf
production API bodies are preserved. Previous standalone generations and their
qualifications remain immutable, without becoming results for this new source.

## Русский

Приватный конструктор trust получает конкретный неизменяемый
`findnodebridge.Publisher` в исходном литерале до загрузки, отказа или сохранения.
Обычные конструкторы сохраняют сигнатуры и передают ноль. Нет передачи через
Service/options, позднего setter, Ready, живого intake root или активации.
Нулевой Publisher пропускает только выделение no-op lease.

Девять рассмотренных семейств получили конкретную реализацию. Конструктор и
latch ошибки загрузки не выходят наружу до завершения и не держат W над I/O.
Восемь семейств живых писателей используют девять конечных участков памяти:
refresh и add в remember, forget, merge записи в contact, last-online для
каждой строки, stage локального факта, строгий ACK, кеш подлинного proof и
очистка proof после promotion. Все прежние ожидания evidence, save и trust
mutex завершаются до W. Подготовка row/token и COW, проверки ключа и текущего
owner, случайные байты и прежний callback настенных часов остаются до W.
Снимки, record/session cache, pin stage, журналы, callbacks и сохранения идут
после W. Нового clock API нет.

Stage одновременно публикует revision, fact, row и observed latch. Строгий ACK
получает W только после настоящего `DurableCommitted`, последнего trust lock и
проверок captured/current stamp. COW создаётся до W; прежний опубликованный
объект не меняется. Ошибка, неопределённый результат или устаревшая работа не
подтверждают новый факт. Remember/forget сохраняют прежний результат живой
памяти при последующей ошибке save. Poison, закрытый Done и исчерпание generation
не запрещают обязательные записи trust; существующий leaf сохраняет эту
гарантию. Нового API или проверки доступности Publisher нет.

Это консервативная публикация contact-памяти. Last-online делает capture
устаревшим, хотя не меняет K eligibility. Отклонённые или старые наблюдения
online и конфликт ключей не публикуют изменение contact. Транзакции persistent
record и session cache не становятся атомарными с этим срезом; новая политика
защиты, срока, класса или транзакции не вводится.

Исторический importer-тест требует ровно два неалиасных файла:
`secure_session_store.go` и `trust.go`. Pin guard добавляет в глобальный API
multiset только `trust.go:beginTrustPublication`; его прежние pin-участки,
конструктор и fatal-публикация сохранены. Новый trust guard закрывает начальную
зависимость, ровно девять мест helper и полные конечные W-участки, включая
сигнатуры и aliases. Wildcard-исключений нет.

Направленные тесты соединяют настоящие trust-писатели с приватным fixture
доменом. Они проверяют конструктор и zero, generation, ошибки save, настоящий
строгий durable ACK/COW, неопределённые исходы, устаревший факт после замены,
подлинный TLS proof cache и прежнюю очистку при promotion, порядок последнего
ожидания и блокировку ACK читателем после I/O. Маленький Service-литерал нужен
только для вызова прежнего метода promotion; конструктор Service и worker не
подключаются к Publisher. Пропуски strict backend явные. Сейчас есть только
SOURCE тесты и причинные рецепты; запуск и runtime PASS не заявляются.

Полное замыкание F1/registry/common/degree и времени, Ready и общий домен через
конструкторы, исключительный consumer scope, owned intake cut, G2, активация и
ресурсная приёмка остаются отдельно. Тела pin-публикаций и production API leaf
сохранены. Прежние поколения и их результаты неизменны и не выдаются за
результаты этого нового исходного дерева.

# External startup fence: review candidate

## English

This is a **default-disabled implementation candidate**, not an enabled identity-pin-removal RPC,
an approved deployment or a release admission. The broker and supervisor commands are implemented;
the node includes an explicit private-channel/recovery CLI candidate, with public removal still unavailable. See [local integration and its gates](identity-pin-recovery-candidate.md).
The supplied systemd template intentionally refuses production startup without the explicit
`--candidate` switch. Do not deploy it over an existing node service yet.

### Failure model and required provisioning

The node data volume may fail, lose a recovery guard or leave a replacement's durability unknown.
The broker ledger must be on a different, independently provisioned storage fault domain. The
root-owned supervisor executable, mandatory service configuration and launcher credentials must also
remain outside the failing node volume. The broker does not infer independence from a URL, TLS
certificate, container, hostname or address. `Enrollment.FaultDomain` records the administrator's
attestation; software tests do not establish the physical fact.

The administrator and root supervisor are trusted. Intentional root bypass, simultaneous loss of
both storage domains and an administrator enrolling a false topology are outside the model.
Every automatic startup of an enrolled node must pass through this supervisor. A missing or damaged
mandatory supervisor configuration must stop startup, never fall back to an unfenced command.
An ordinary installation continues to work without this mechanism, with pin removal unavailable.

The first launcher backend is Linux, with root supervisor and a different, non-root child UID/GID.
It supports one enrolled launcher host and the host systemd mount namespace with a single shared,
local `/run`. Container replicas, separate `/run` mounts and NFS/SMB profile locks are unsupported.
The child executable and its directory chain must be root-owned and not writable by other users;
sticky root-owned temporary directories are allowed for functional tests. Credentials and
configuration are owner-only, audited before reading. Other platforms reject supervision before
requesting startup grants. No Windows or desktop provisioning is claimed.

### Implemented protocol

`cmd/corsa-fence-broker` runs a TLS 1.3 server requiring a chain-verified client certificate.
Enrollment fixes the node identity, store identity, launcher certificate fingerprint, fault-domain
attestation and `linux-systemd-v1` profile. A separate operator certificate is required for finalization.
The ledger uses the existing cached `modernc.org/sqlite` dependency, rollback journaling and
`synchronous=EXTRA`. Its directory is private and the database is created owner-only before SQLite
opens it. This is the external broker's ledger, not a replacement of the node pin store.

Each node has one bounded current record. Startup grants expire after 30 seconds, bind a fresh boot
nonce and generation, and are consumed once. A new trusted supervisor invalidates an abandoned
grant/lease; service containment must kill the previous child before that supervisor starts.
`BeginRemoval` atomically checks the running lease and generation, persists `Fenced` and returns the
operation permit. A failed or unknown acknowledgement returns **no permit** and cannot authorize
local destructive writes. Every later startup is refused while fenced, even if the local guard
is absent and the local main file already lacks the pin.

The supervisor holds one lifecycle mutex through prepare, consume and spawn. Only the existing live
child can request removal. Linux `Pdeathsig=SIGKILL` is established on a parent OS thread retained
through `Start` and `Wait`; the service template additionally uses `KillMode=control-group`.
Before contacting the broker, the supervisor also takes a canonical per-node flock under root-owned
0700 `/run/corsa-supervisor`. The same open-file description is inherited as FD4 and retained for the
child's entire lifetime, without `LOCK_UN`. Parent death alone cannot release it before the old child
has actually exited and closed that alias. The child must not close, unlock or repurpose FD4; there
is no Go finalizer wrapping that inherited descriptor. The parent closes its alias only after Wait.
The child receives a private inherited Unix socket. Its check of `SO_PEERCRED` establishes the
root creator and exact parent PID, not the identity of an arbitrary post-fork sender. Linux restricts
inspection of a different UID's `/proc/<pid>/exe`; this protocol does not claim that inspection.
Trust comes from the root parent, the mandatory launch configuration and private descriptor ownership.
The approved executable path is checked for ownership, not treated as proof of the caller's executable.

`SupervisedSession` is a concrete, zero-value-unverified capability. Injecting a `RemovalFence`
interface, HTTP client or boolean does not construct it. The production RPC must remain unavailable
until actual provisioning, the workflow and node integration have been validated. Presence of this
package or a successful loopback test is insufficient.

### Commit, receipts and operator finalization

Local guard/main writes start only after `BeginRemoval` acknowledged the durable external fence.
The node's runtime pin must remain until the complete local durable transaction succeeds. Local
uncertainty stops networking with the pin retained. The strict local store adapter and offline CLI are implemented as a candidate. A local committed result remains awaiting actual exit/seal and external operator finalize; full workflow success is not yet reported publicly.

`RecordReceipt` records either `clean_committed` or `protection_restored`, bound to the exact
operation, node/store, revision, generation and snapshot digest. **A receipt never unfences startup.**
A restored receipt may supersede a clean receipt of the same operation, never the reverse. A fresh, exactly active recovery grant may record a new restored snapshot digest through the separate launcher-only `recovery_receipt` action; generic receipt writes and stale grants cannot replace restored snapshots. Completion still requires that grant and exact latest receipt, with no un-fence.
While the child is alive, the broker marks the writer active and operator finalization is refused.
The child calls `SupervisedSession.Close` only after Run has returned through all teardown, final
flush and fatal-error handling, or after offline recovery has returned. Close exchanges an explicit
shutdown frame containing the exact last acknowledged receipt and a fresh supervisor acknowledgement
challenge received with that receipt. The challenge is not derived from the receipt and is redacted
in diagnostics. The parent acknowledges shutdown and allows at most two seconds for actual process
exit; the child closes FD3 and exits immediately, retaining FD4 until kernel exit. No local writes,
Run or listeners may follow Close. A Close error must produce nonzero exit and must never replace an
existing fatal outcome with success. Ordinary shutdown without a removal uses the same handshake
without a receipt. Local frames remain bounded to 16 KiB; supervisor writes and the child's Close
exchange also have two-second deadlines. Child Begin/receipt exchanges observe cancellation and a maximum five-second deadline; partial replies poison the session. Supervisor TERM requests child group TERM with a five-second WaitDelay bound for the direct child; deployed control-group policy contains descendants. Only exact graceful acknowledgement plus actual exit zero permits a detached two-second broker-completion attempt after cancellation. Raw EOF, malformed or oversized frames never enter grace.
Only after the explicit handshake and actual child exit0 does the trusted launcher seal the exact
last acknowledged receipt. Missing acknowledgements, forged/missing acknowledgement challenges,
crash, nonzero exit and exceeding the exit deadline never seal it. A fatal or recovery-required storage
outcome in the node must therefore produce a nonzero process exit, not ordinary cancellation success.
Normal successful removal must remain removed in the local file; it is not automatically restored
merely to finish the ordinary workflow.

The implemented `finalize` command is a **separate trusted operator action** against the exact
recorded receipt. A launcher certificate cannot invoke it. A wrong operation, store, generation,
receipt kind or digest fails its CAS. Its lost acknowledgement may mean the operator-authorized
startup permission committed; it is not part of a destructive RPC's promise that failure retains pin.
This extra finalization after every successful removal is a proposed operator workflow requiring
review before production enablement. Do not silently invoke it from the node.

The supervisor's `--recover` path obtains a separate one-use recovery grant, leaves the broker
fenced and starts the recovery child in a new network namespace. It accepts only a restored receipt.
The same lifetime flock excludes normal/recovery overlap; active recovery also blocks finalization.
Completing recovery is acknowledged to the broker only after actual successful child exit.
The production child adapter must restore the pin from the broker permit, durably save it, complete
local guard cleanup and then send that receipt. The candidate adapter performs these steps before App.Run and exits offline. A recovery grant
cannot be consumed as a network startup grant. Unsupported namespace containment refuses recovery.

### Candidate command surface

Build without downloading dependencies:

```sh
GOPROXY=off go build ./cmd/corsa-fence-broker ./cmd/corsa-supervisor
```

The broker supports `serve`, `enroll`, `status` and `finalize`. Use `-h` after the subcommand for
flags. Enrollment and receipt selections are explicit owner-only JSON files. TLS certificate,
private key and CA files are owner-only. The CLI never prints private keys, startup grants or lease
credentials; their formatting is redacted for every formatting verb. `status` contains the current
public selection and receipt, allowing the operator to inspect exactly what finalization changes.
There is no force-unfence command.

The supervisor accepts an owner-only JSON configuration with `broker`, `cert`, `key`, `ca`, `node`,
`store` and `child` (`Path`, `Args`, `UID`, `GID`). `--candidate` is required. `--recover` chooses the
isolated recovery path. These remain explicit review tools with public removal unavailable.
`child.Environment` is an explicit allowlist of node paths/listener/proxy/bootstrap settings and
node RPC credentials, with values redacted in formatted diagnostics. Broker credentials and the
fence descriptor cannot be supplied there; the parent's environment is not inherited. This initial
subset does not preserve every node configuration knob. Review the entire deployed profile before
production enablement, rather than silently replacing omitted security settings with defaults.

### Functional evidence and remaining admission

Targeted tests exercise actual SQLite transactions, actual TLS 1.3 client-certificate roles, lost
acknowledgement after committed fencing, restart against the persisted fence, grant replay,
receipt/finalize CAS and credential redaction. The Linux test runs real supervisor and separate-UID
child subprocesses: killing the supervisor kills its child; an ordinary restart creates a new child;
after fencing, restart is refused before any new child is spawned. Real child regressions also cover
receipt ACK followed by graceful exit0, fatal nonzero exit, a child that exceeds the shutdown deadline,
raw EOF, oversized frames and exiting without consuming the receipt ACK. A local guard is not involved in
that restart test. Linux tests can run in a bounded existing Alpine container with network disabled;
its loopback TLS proves protocol behavior, not a separate physical fault domain.

Still required before enabling removal: final independent review of combined node/store recovery and the failure matrix, verification of service/cgroup
deployment and recovery namespace behavior on the target, review of mandatory operator finalization,
and actual administrator-attested independent broker provisioning. The normal node remains usable
and the destructive RPC remains unavailable while those requirements are unmet.

## Русский

Это **выключенный по умолчанию кандидат реализации**, а не включённый RPC снятия pin, утверждённое
развёртывание или допуск выпуска. Команды broker и supervisor и явный private-channel/recovery CLI
candidate реализованы, но публичное снятие недоступно. См. [интеграцию и условия допуска](identity-pin-recovery-candidate.md). Шаблон systemd намеренно
отказывает без явного `--candidate`. Подменять им существующую службу узла пока нельзя.

### Модель отказов и необходимое развёртывание

Том данных узла может отказать, потерять recovery guard или оставить неизвестную надёжность замены.
Ledger broker обязан находиться в другом подтверждённом домене отказа storage. Root-owned бинарник
supervisor, обязательная конфигурация службы и credentials launcher также находятся вне отказавшего
тома узла. Независимость не выводится из URL, TLS, контейнера, имени или адреса. Поле
`Enrollment.FaultDomain` фиксирует утверждение администратора; тесты физический факт не доказывают.

Администратор и root-supervisor доверенные. Злонамеренный обход root, одновременная потеря обоих
доменов storage и ложное enrollment вне модели. Каждый автоматический старт enrolled-узла проходит
через supervisor. Потеря обязательной конфигурации означает отказ старта, а не fallback на обычную
команду. Обычная установка без механизма работает; снятие pin в ней недоступно.

Первый backend — Linux: root-supervisor и отдельный непривилегированный UID/GID ребёнка.
Поддерживаются один enrolled launcher-host и общий локальный `/run` host-systemd namespace.
Контейнерные replicas, разные mount namespace с разными `/run` и NFS/SMB-lock не поддерживаются.

Бинарник ребёнка и цепочка каталогов принадлежат root и не изменяются посторонними; root-owned sticky tmp
допускается для функциональных тестов. Credentials и конфигурация owner-only, проверяются до чтения.
Другие платформы отказывают до запроса startup grant. Windows/Desktop-развёртывание не заявляется.

### Реализованный протокол

`cmd/corsa-fence-broker` — TLS 1.3 с обязательной проверкой цепочки клиентского сертификата.
Enrollment фиксирует identity узла, store, fingerprint launcher, подтверждение fault domain и
профиль `linux-systemd-v1`. Для finalize нужен отдельный сертификат оператора. Внешний ledger
использует уже закэшированный `modernc.org/sqlite`, rollback journal и `synchronous=EXTRA`.
Каталог закрытый; база создаётся owner-only до открытия SQLite. Это ledger внешнего broker,
а не замена pin-store узла.

На узел — одна ограниченная текущая запись. Grant живёт 30 секунд, связан с nonce запуска и
generation и погашается один раз. Новый доверенный supervisor отменяет незавершённый grant/lease;
служба обязана завершить прежнего ребёнка до нового старта supervisor. `BeginRemoval` атомарно
проверяет действующий lease/generation, сохраняет `Fenced` и отдаёт permit. Ошибка или неизвестное
подтверждение не возвращают permit и не разрешают локальную разрушающую запись. Пока fence
действует, новый старт запрещён даже при отсутствии local guard и main без pin.

Один lifecycle mutex охватывает prepare, consume и spawn. Снятие запрашивает только живой ребёнок.
До обращения к broker берётся canonical per-node flock в root-owned0700 `/run/corsa-supervisor`.
Та же open-file description передаётся ребёнку как FD4 на всю жизнь процесса; `LOCK_UN` не
используется. Одна смерть родителя lock не снимает: нужен фактический выход ребёнка и закрытие его
alias. Ребёнок не закрывает, не меняет и не разблокирует FD4; Go-finalizer его не оборачивает.
Родитель закрывает собственный alias лишь после Wait.
Linux `Pdeathsig=SIGKILL` устанавливается на родительском OS thread, удерживаемом от `Start` до
`Wait`; шаблон службы дополнительно задаёт `KillMode=control-group`. Ребёнок получает private
inherited Unix socket. `SO_PEERCRED` подтверждает root-создателя и точный parent PID, а не произвольного
sender после fork. Linux ограничивает чтение чужого `/proc/<pid>/exe`; протокол не заявляет такую
проверку. Доверие — root-parent, обязательная конфигурация запуска и приватный descriptor. Ownership
approved path проверяется, но не выдаётся за доказательство исполняемого файла caller.

`SupervisedSession` — конкретная capability с непроверенным zero value. Интерфейс `RemovalFence`,
HTTP client или boolean её не создают. Production RPC остаётся недоступным до проверенных
provisioning, workflow и интеграции узла. Наличия пакета или успешного loopback-теста недостаточно.

### Commit, receipt и подтверждение оператора

Записи local guard/main начинаются лишь после подтверждённого внешнего fence. Runtime-pin остаётся
до полной успешной local durable transaction; неопределённость останавливает сеть с сохранённым
pin. Strict local store adapter и offline CLI реализованы как candidate. Локальный committed ещё
ожидает actual exit/seal и external operator finalize; полный успех workflow публично не объявляется.

`RecordReceipt` сохраняет `clean_committed` либо `protection_restored`, связанные с точными
operation, node/store, revision, generation и snapshot digest. **Receipt не снимает startup fence.**
Restored receipt может заменить clean receipt той же операции, обратный переход запрещён.
Новый точно активный recovery grant может записать новый restored snapshot digest отдельным
launcher-only действием `recovery_receipt`; generic receipt и устаревший grant не заменяют restored
snapshot. Completion требует этот grant и точный последний receipt, не снимая fence.
Пока ребёнок жив, writer active и operator-finalize запрещён. Ребёнок вызывает `SupervisedSession.Close` только после возврата Run через все teardown, final
flush и обработку fatal-ошибки либо после возврата offline recovery. Close передаёт явный shutdown
frame с точным последним подтверждённым receipt и свежим acknowledgement challenge supervisor,
полученным вместе с этим receipt. Challenge не выводится из receipt и закрыт в диагностике.
После подтверждения shutdown родитель даёт не более двух секунд на фактический выход процесса;
ребёнок закрывает FD3 и немедленно выходит, удерживая FD4 до kernel exit. После Close запрещены
local writes, Run и listeners. Ошибка Close требует ненулевого exit; прежнюю fatal-ошибку нельзя
заменить успешным результатом. Обычный shutdown без снятия использует тот же handshake без receipt.
Локальный frame ограничен 16 KiB; запись supervisor и обмен Close имеют deadline две секунды.
Raw EOF, malformed и oversized frame не разрешают grace. Только после явного handshake и фактического
exit0 доверенный launcher запечатывает точный последний подтверждённый receipt. Отсутствующий ACK,
неверный или отсутствующий acknowledgement challenge, crash, ненулевой exit и превышение срока
выхода его не запечатывают. Fatal-storage/RecoveryRequired у узла поэтому обязаны
давать ненулевой exit, а не успешный результат обычной отмены.
Обычное успешное снятие остаётся сохранённым; восстановление pin не используется как обязательное
завершение нормального сценария.

Команда `finalize` — **отдельное действие доверенного оператора** с точным записанным receipt.
Launcher её вызвать не может. Другие operation/store/generation/kind/digest не проходят CAS.
Потерянный ответ finalize может означать, что явно разрешённый оператором старт записан; этот шаг
не является частью обещания destructive RPC о сохранении pin при ошибке. Обязательный finalize
после каждого успешного снятия — предложенный workflow, требующий ревью до production enablement.
Узел не должен вызывать его незаметно.

`--recover` запрашивает отдельный одноразовый recovery grant, сохраняет fence и запускает ребёнка
в новом network namespace; допускается только restored receipt.
Тот же lifetime flock исключает пересечение normal/recovery; active recovery блокирует finalize.
Подтверждение завершения recovery отправляется broker только после успешного выхода ребёнка.

Production child adapter обязан восстановить pin по broker permit, надёжно записать store, завершить local guard cleanup и затем
передать receipt. Candidate adapter выполняет это до App.Run и завершает offline-процесс. Recovery grant не является разрешением сетевого
старта; при неподдерживаемой изоляции recovery отказывает.

### Команды кандидата

Supervisor TERM запрашивает TERM process group ребёнка; WaitDelay ограничивает прямого ребёнка
пятью секундами, а descendants ограничивает deployed control-group policy. Только точный graceful
ACK и реальный exit zero допускают отдельную попытку broker completion на две секунды после отмены.
Private Begin/receipt учитывают отмену и deadline не больше пяти секунд; незавершённый ответ делает
session framing непригодным.

Сборка без скачивания зависимостей:

```sh
GOPROXY=off go build ./cmd/corsa-fence-broker ./cmd/corsa-supervisor
```

Broker поддерживает `serve`, `enroll`, `status`, `finalize`; флаги показывает `-h` после подкоманды.
Enrollment/receipt передаются явным owner-only JSON-файлом. Сертификаты, private key и CA также
owner-only. CLI не печатает ключи, startup grants и lease credentials; их fmt-представление
редактируется при всех verb. `status` показывает текущий публичный selection/receipt для проверки
оператором. Команды force-unfence нет.

Supervisor принимает owner-only JSON с `broker`, `cert`, `key`, `ca`, `node`, `store`, `child`
(`Path`, `Args`, `UID`, `GID`). Требуется `--candidate`; `--recover` выбирает изолированное
восстановление. Это явные инструменты review; публичное снятие недоступно.
`child.Environment` — явный allowlist путей, listener/proxy/bootstrap и node RPC credentials;
fmt-представления значений закрыты. Broker credentials и fence descriptor туда не допускаются,
окружение родителя не наследуется. Начальная выборка не покрывает все настройки узла. До
production enablement нужен полный review профиля, чтобы пропущенные security settings не
подменялись дефолтами незаметно.

### Функциональные подтверждения и оставшийся допуск

Целевые тесты проверяют реальные SQLite-транзакции, TLS 1.3/mTLS и роли, потерянное подтверждение
после сохранённого fence, restart по сохранённому fence, replay grant, receipt/finalize CAS и
редактирование credentials. Linux-тест запускает настоящие supervisor/child subprocess с разными
UID: смерть supervisor завершает ребёнка, обычный restart создаёт нового, после fence restart
отказывает до spawn. Реальные child-regression проверяют receipt ACK с graceful exit0, fatal
nonzero exit, превышение срока shutdown, raw EOF, oversized frame и выход без чтения receipt ACK.
Local guard в этой проверке не используется. Прогон возможен в ограниченном
готовом Alpine-контейнере без сети; loopback TLS не доказывает физическую независимость storage.

До включения снятия остаются: итоговое независимое ревью совместной node/store recovery и матрицы
ошибок; проверка реальных service/cgroup и recovery namespace на целевой
платформе; ревью обязательного operator-finalize; настоящее независимое развёртывание broker,
подтверждённое администратором. Пока они не выполнены, обычный узел работает, destructive RPC
недоступен.

# Private D1 Runtime construction

This preparation adds one private candidate factory. Public `NewService`, default
capabilities and advertisement do not select it. The earlier partial candidate
factories remain Constructing. Construction Ready is an internal capability
boundary; activation, G2, observation/resource profiles and release decisions
remain separate.

The factory loads decoded peer state once, preserving the optional degree hint's
original value and date. A supplied empty decoded state is still supplied input;
the common builder must not reread the file after callbacks. Ordinary load-error
fallback semantics remain intact.

One original Runtime domain owns terminal publication, observation, intake,
forwarding, local target signing and both original-session retirement bindings.
They and the statistic producer exist before the first Service and builder
callback. Original Service/trust custody is recorded before that callback;
secure-session/store custody is recorded at its original creation.

The first bootstrap capacity epoch uses a genuine paired monotonic/wall sample
from that same original domain. The loaded hint is metadata only. Missing,
future or 24-hour-old hints fall back to 512; degree is clipped at 64 before the
existing capacity arithmetic. That hint controls only the first 600-second
capacity window. Later complete current cuts select subsequent windows; a
complete zero replaces an older nonzero degree rather than taking a maximum.

Persisted observation has a separate origin: coverage starts at the later final
statistic installation, after application lock waits, not at domain creation or
bootstrap sampling. Before 600 seconds of actual observer coverage, partial or
unavailable observation preserves the original tuple/date. The existing complete
observation and flush logic retains the approved genuine-zero and stale-hint law.

Before statistic installation, the builder checks the exact original bindings,
trust and secure-store startup refusals, background admission and cancellation.
After installation the finalizer compares immutable original identities and
performs the sole production Seal call. It performs no application lock check
after the coverage-start sample. Cancellation is checked immediately before and
after Seal; the latter check is the candidate-return boundary. Cancellation after
that boundary is ordinary Service lifecycle shutdown. Failure or panic cancels
the original lifecycle, closes background admission and joins callback work.
No failed factory call returns a usable Service; construction replay cannot
cancel an already returned candidate.

The new tests exercise genuine Runtime construction and real original TLS/F1
local request → actual local T → signing → mandatory completion → exact original
answer writing. They also cover pre-callback Constructing refusal, cancellation
and panic cleanup, foreign roles, same-domain replacement-store refusal, repeated
finalization, decoded-empty input, first-window capacity and stale/future/zero
hint boundaries. Existing partial-factory Constructing tests remain in the suite.

The D1 path remains A → R → T over existing ProvenSessions and exact known
neighbors. Unknown-target silence, original response correlation and strict
answer expiry are unchanged. There is no recursive D2 routing or public general
initiator here.

## Explicit loaded inbound composition

A private Ready options path can additionally request the existing loaded-only
N-in producer, with or without the existing own-DM candidate. Ordinary Ready,
own-DM and public Service constructors keep N-in absent. No configuration,
capability, installer, migration or Unknown-journal branch selects this option.
The original secure-session store must already contain valid format3 state.
Loaded state may still owe strict confirmation; it is serviced by the existing
Run-owned worker, not treated as a new constructor ACK requirement.

Attachment uses that same original Service/store after constructor callbacks
and application waits, immediately before the final Ready precheck. An attach
failure is preserved and returns no Service, with original cancellation and
callback joins; statistics and Seal are skipped. Production retains the existing
trusted elapsed clock. Tests use only the existing constructor clock seam before
store loading, never a replacement clock or synthetic runtime evidence.

The selected options and original producer pointer, Service, store and wake
channel are retained separately. Both preinstall and final immutable checks
refuse replacement, including same-pointer field changes; mutex/cursor worker
state is not copied. All store/file/lock checks precede statistic installation.
The original Run starts and joins the original N-in worker; no extra worker,
context, limit, retry or post-statistic application lock is introduced.

Directed tests reuse genuine TLS/F1 sessions and strict on-disk journals. The
controlled elapsed fixture advances the N-in observation window without a real
ten-minute wait; it does not represent measured elapsed-time/resource acceptance.
The same original session earns confirmed N-in class and participates in genuine
Runtime capture and local signed-answer writing. This remains inactive
construction preparation, separate from G2/default advertisement and release.

## Complete original-session chain coverage

`TestD1OriginalInitiatorReadyRelayReadyTargetChain` composes the existing test
initiator A with genuine Ready relay R and target T. Real TLS/F1 and the loaded
N-in producer feed the original forwarding, target signing and return writers;
the initiator verifies its retained challenge and restored source envelope. The
cases cover three successful lookups and the fourth-start limit, foreign-reader
and replay refusal, cancellation, deadline equality and a forged return copy.
Cleanup closes both socket pairs before joining either original fixture owner.

This fixture supplies Round and Slack of 100 ms before construction and Seal,
within the unchanged one-second Runtime budget. These are test inputs, not an
approved G2 timing profile or a service-time guarantee. Its controlled N-in and
initiator clocks do not replace the relay/target Runtime clocks. The accepted
ordinary/race checks exercise the original read/admission/dispatch paths and
workers; they do not establish whole `Service.Run`, a public lookup API, resource
acceptance or activation.


## Private loaded K reconciliation composition (authored; QA pending)

A closed private Ready option additionally composes the original K startup
reconciliation with loaded-format3 N-in on one Service, trust store and secure
store. It requires loaded inbound and excludes own-DM charged R. Public/default
construction and the existing R|K refusal remain unchanged. It supplies no new
producer, backend, clock, worker or completion policy.

The original K resolver/admission pair is captured before the datagram callback;
the original contact producer/service/wake is captured immediately after producer
creation. Ready checks those immutable identities and exact reconciliation-only
source before its final precheck/statistic/Seal and again at finalization, without
copying mutable worker state or acquiring a post-statistic application lock.
Actual Run remains the sole readiness and joined-worker owner. Authored functional
checks use distinct K-contact and genuine TLS/N-in peers, real strict persistence
and original writer/signature paths; they require independent CODE and ROOT QA.
This component does not activate public policy or establish resource/G2/release
acceptance.

## Частная композиция loaded K reconciliation (написана; QA впереди)

Замкнутая private Ready option соединяет прежнюю startup reconciliation K с
loaded-format3 N-in на одном Service, trust store и secure store. Она требует
loaded inbound и исключает own-DM charged R. Public/default construction и
прежний отказ R|K не меняются. Новых producer/backend/часов/worker или политики
completion нет.

Исходные resolver/admission K захватываются до datagram callback; исходные
contact producer/service/wake — сразу после создания producer. Ready сверяет
эти immutable identities и точный reconciliation-only source перед финальными
precheck/statistic/Seal и повторно при finalize, не копируя mutable worker state
и не добавляя application lock после statistic. Только actual Run открывает
readiness и владеет прежними joined workers. Написанные functional checks берут
разных K-contact и genuine TLS/N-in peers, настоящее strict persistence и
исходные writer/signature paths; независимые CODE и ROOT QA ещё обязательны.
Публичная политика, resource/G2/release acceptance этим не включаются.

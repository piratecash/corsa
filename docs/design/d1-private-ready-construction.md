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

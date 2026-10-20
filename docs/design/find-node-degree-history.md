# Inactive all-ProvenSession degree history / Неактивная история степени

The private `newDegreeCandidateService` is unused. Its required copied
`degreeProfile.MaxActive` has no production default. Ordinary constructors leave
`Service.degree` nil and start no new network, timer, queue or worker.

This phase implements a statistical observation only. It creates no F1 session,
FindNode admission, identity-pin class, R/R2 ownership, RPC/configuration or
activation. The constant intake capacity, first-window 512/hint rule and all
accepted intake obligations are untouched. Same-owner capacity epochs and the
optional peer-state hint are a separate next phase.

The original accepted or dialled v2 transport contributes at P1: successful
mandatory protection and existing secure-transport policy checks, before the
transport is returned to its caller. P1 follows the established-session rule in
v2 §7.3; it differs from the raw typed proof and from later registry/application,
CM Active or negotiated F1 publication. Accepted common id0 is valid for this
statistical witness. The original common pointer and Service token, immutable
proof identity/role/group, and a checked row generation bind its lifetime.

One fixed MaxActive table and a fixed 64-row newest rank basis retain observations.
OUT deduplicates identity. IN deduplicates identity within group and contributes
at most two per group. The basis is capped at 64; the cut's window is the capped
rank. At saturation its incoming/outgoing fields describe the retained basis,
not uncapped total physical degrees. Each existing transport has one constant
witness; this does not bound the memory of unlimited ordinary connections.

If the active table fills, the original common owns an untracked obligation.
While any untracked transport remains live, a cut returns unavailable with no
partial result. Every observed terminal contributes history before clearing
common evidence, releases only its exact slot or counter obligation and is
idempotent through the existing common End gate. A still-active overflow can
therefore refuse cuts indefinitely. Ordinary sessions continue.

The monotonic clock is a concrete owned value, never an injected function under
common/degree locks. Internal tests use a private atomic fixture clock. Clock
regression, counter/generation exhaustion or an ownership inconsistency poisons
new cuts; no reset silently drops debt and no zero-degree fallback is invented.
Observed terminal means local End/I/O-error/Close, not instant physical remote
TCP closure. Recent history uses the half-open 600-second boundary.

Lock order is common.mu → degree.mu, also reached below peerMu or CM.mu in
existing terminal paths. A cut holds only degree.mu. Degree code never takes a
common/registry/CM/store/pin lock or calls external code while degree.mu is held.
All memory/workspace is bounded by the explicit MaxActive and 64-row constants;
there is no claim of hard mutex/CPU latency or accepted RSS/disk cost.

The authored tests exercise genuine no-F1 TCP/TLS Accept/Dial before registry/CM,
mandatory writer wait/refusal, original pre-bind terminal replay, stale CM and
shutdown cleanup, table overflow, immutable identity/role/group accounting,
slot ABA, clock/counter failure and concurrent cuts/close. Separate algebra tests
compare the fixed history to an independent unbounded oracle. Source guards
close constructors, method escapes and lifecycle roots; they supplement real
functional oracles. Formatting, compilation, runtime/race, causal control REDs
and resource acceptance remain separate and are not claimed here.

## Русский

Это отключённый внутренний кандидат статистического учёта всех подлинных v2
соединений, включая ещё не зарегистрированные входящие и одноразовые исходящие
без F1. Новый приватный конструктор не используется обычным запуском; численное
значение MaxActive для развёртывания не выбрано. Профиль копируется до создания
одного Service. Сеть и фоновые задачи не добавляются.

Наблюдаемая точка установления P1 наступает после обязательного сохранения защиты
и прежних проверок транспорта, до возврата вызывающему коду. Она не требует CM,
F1 или ConnID входящего соединения. Подпись доказательства сама по себе до
успешного P1 степень не увеличивает. История не даёт права создавать pin,
принимать FindNode или считать сессию доступной для маршрутизации.

Общая таблица ограничена явным MaxActive, история — 64 строками. При переполнении
действующее соединение получает собственное обязательство неизвестного учёта;
новые снимки отвергаются целиком, пока такие соединения не завершатся. Завершение
каждого оригинального транспорта учитывается до очистки доказательств. Повторное
закрытие не освобождает чужой слот и не уменьшает счётчик второй раз.

Перевод настенных часов не используется для истории. Ошибка монотонного времени,
переполнение счётчика или поколения запрещает новые снимки, сохраняя обычную
работу узла. Нельзя выдавать неполное покрытие за нулевую степень. Физическое
закрытие удалённого TCP становится наблюдаемым через локальный End, Close или
ошибку ввода-вывода; мгновенное знание состояния сети не обещается.

Данный первый срез не меняет ёмкость текущего приёмника, его долги, начальное
правило 512/подсказка или сохранение peer-state. Окна ёмкости того же владельца
и необязательная подсказка относятся к следующему отдельному срезу. Численные
профили, G2, ресурсная приёмка и включение остаются отдельными условиями.

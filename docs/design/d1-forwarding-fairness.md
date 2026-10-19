# D1 forwarding fairness: deadline counterexample

## English

### Status

Design review finding, 2026-10-18. The overlay responder is inactive. This document records a
contradiction in the proposed admission criteria; it does not approve a wire contract, change limits,
or authorise activation. The release sequence remains in [Releases](../release.md).

### The proposed guarantee

The proposed responder gives requests from contacts and selected neighbours (level 1) priority over
lower levels. For one responder-to-target connection it proposes:

| Quantity | Proposed value |
|---|---|
| Forwarding rate | 1 request/s, burst 4 |
| Requests from one source session for one target | At most 3 per 30 s |
| Queue and pending requests for one target | At most 16 |
| Responder deadline and queue lifetime | 6 s |
| Initiator deadline | 12 s |

The proposed justification is that at most 10 live level-1 sessions generate at most 30 requests in
30 seconds, matching the forwarding rate, and therefore wait no more than 6 seconds. Matching the
average rate does not bound the waiting time of a burst.

### Counterexample

Take 10 level-1 sessions requesting the same target, with three different calls per session. Deliver
the 30 requests within one second, then send nothing else for the remainder of the 30-second window.
This respects the per-session limit of 3 per 30 seconds and can be paced below the proposed node
limit of 32 requests/s. No lower-priority attacker or network delay is needed.

Even starting with all four forwarding tokens, by seven seconds after the first request the
responder can forward at most `4 + 7 = 11` requests. Every request has already reached its six-second
deadline by then. Thus at least 19 of the 30 requests cannot meet that deadline, even with an
unbounded queue and instantaneous answers from the target. With an unbounded queue the last request
cannot be forwarded before 26 seconds; the actual queue limit of 16 instead rejects part of the
burst. Priority and round-robin selection do not increase the forwarding capacity.

The target's per-session signing budget does not repair this counterexample: the responder's own
forwarding limit is already sufficient to disprove the guarantee.

### Owner decision and remaining work

On 2026-10-18 the owner chose an explicit aggregate admission budget, fair between level-1 sessions,
and a derived bound for admitted requests. Existing resource limits stay in place. The unconditional
guarantee for all per-session-legal requests is removed; response timing is conditional on target
admission/service and explicit network-delay bounds. This narrows the guarantee and does not permit
activation. The formulas, proposed parameters, reference test and remaining pre-G2 decisions are in
[Aggregate admission and conditional deadlines](d1-admission-budget.md).

Tests of priority, isolation and bounded memory remain useful. A passing test with one privileged
request among lower-priority traffic does not establish the proposed guarantee for simultaneous
requests from several privileged sessions. Acceptance must include a legal burst at the stated
maximum. Under the former unconditional guarantee, refusals and expiry were failures. Under the
chosen admission rule, report pre-admission refusals separately from admitted requests: an expired
or refused admitted request violates its conditional service guarantee when the recorded target,
network and scheduling assumptions hold. Failed assumptions must be reported explicitly, not hidden
as successful service. Revoked lower-level admissions are counted separately too.

## Русский

### Статус

Находка проектного ревью, 2026-10-18. Оверлейный ответчик не активирован. Документ фиксирует
противоречие предлагаемых критериев приёмки; он не утверждает wire-контракт, не меняет лимиты и не
разрешает активацию. Порядок выпуска описан в [Выпусках](../release.md).

### Предлагаемая гарантия

Ответчик должен давать запросам контактов и выбранных соседей (уровень 1) приоритет над нижними
уровнями. Для одного соединения ответчика с целью предложены:

| Величина | Предлагаемое значение |
|---|---|
| Частота пересылки | 1 запрос/с, burst 4 |
| Запросы одной сессии об одной цели | Не больше 3 за 30 с |
| Очередь и ожидающие запросы к одной цели | Не больше 16 |
| Таймаут ответчика и срок записи в очереди | 6 с |
| Таймаут инициатора | 12 с |

Предлагаемое обоснование: не больше 10 живых сессий уровня 1 создают не больше 30 запросов за
30 секунд, что совпадает с темпом пересылки, а значит, ожидают не больше 6 секунд. Совпадение средних
темпов не ограничивает время ожидания всплеска запросов.

### Контрпример

Пусть 10 сессий уровня 1 запрашивают одну цель, каждая — три разных вызова. Доставим 30 запросов за
одну секунду и больше ничего не отправим до конца 30-секундного окна. Лимит 3 за 30 секунд на сессию
соблюдён; запросы можно распределить во времени, не превышая предлагаемые 32 запроса/с на узел.
Ни атакующий нижнего уровня, ни задержка сети для контрпримера не нужны.

Даже с четырьмя начальными токенами за семь секунд от первого запроса ответчик может переслать не
больше `4 + 7 = 11` запросов. К этому моменту шестисекундный срок каждого запроса уже наступил.
Следовательно, хотя бы 19 из 30 запросов не укладываются в срок даже при неограниченной очереди и
мгновенных ответах цели. При неограниченной очереди последний запрос нельзя переслать раньше
26 секунд; фактический предел очереди 16 вместо этого отвергает часть всплеска. Приоритет и
round-robin не увеличивают пропускную способность пересылки.

Подписной бюджет цели на сессию контрпример не устраняет: собственного ограничения ответчика уже
достаточно для опровержения гарантии.

### Решение владельца и оставшаяся работа

2026-10-18 владелец выбрал явный общий бюджет допуска со справедливостью между сессиями уровня 1
и выведенную границу для принятых запросов. Прежние ресурсные лимиты сохраняются. Безусловная
гарантия всем разрешённым по сессиям запросам снимается; срок ответа условен и требует допуска и
обслуживания на цели и явных границ задержки сети. Это сужение гарантии, а не разрешение активации.
Формулы, предлагаемые параметры, эталонный тест и оставшиеся решения до G2 — в документе
[Общий бюджет допуска и условные сроки](d1-admission-budget.md).

Тесты приоритета, изоляции и ограниченности памяти остаются полезными. Успешный тест с одним
приоритетным запросом среди трафика нижних уровней не подтверждает предлагаемую гарантию при
одновременных запросах нескольких приоритетных сессий. Приёмка должна включать допустимый всплеск
на заявленном максимуме. Для прежней безусловной гарантии отказы и истечения означали провал.
Для выбранного допуска отказы до приёма учитываются отдельно: истечение или отказ уже принятого
запроса нарушает условную гарантию при выполнении записанных предпосылок цели, сети и планирования.
Невыполненные предпосылки записываются явно, а не скрываются как успешное обслуживание. Отзыв допуска
нижнего уровня также считается отдельно.

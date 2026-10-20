# Private bounded FindNode class observation / Ограниченное C5-наблюдение

The private node observer composes existing registered F1 sessions with current strict K/N-sel/N-in classification. It is unused by ordinary constructors, handlers, RPC and configuration. The existing private F1 candidate remains the only construction path. No session or proof is minted by the observer.

## Required profile and cost

Every call takes a copied positive MaxMembers (M) and MaxRegistryVisits (V), with V >= M. There is no production default. Validation rejects counter overflow and element-byte overflow for the closed one-pointer Session shape before allocating. This check does not establish that a chosen allocation fits available RAM.

The bounded registry reader allocates at most M session slots before walking existing registry keys. Every visited key, including non-F1 entries, consumes a visit; an extra key or member causes a complete ErrCapacity result. It never obtains the old unbounded all-F1 snapshot first and never returns a truncated prefix. Target sessions count toward M and are removed before oldest-four selection. No 20,000-pin limit is treated as a session limit.

Retained workspace is O(M), class checks address only captured members, and the callback count is at most V+1. The existing registry implementation, hash-table iteration, allocation and owner locks do not provide a hard CPU, wall-time or mutex-wait bound. Cancellation is checked during traversal, after waits and after the core calculation before publication; it does not interrupt a blocked mutex. Numeric deployment profiles and resource acceptance remain separate.

## Authority and capture

Registry capture and recheck require the original live F1 Session, matching ConnID, exact connection entry, common lifecycle and Service proof token. Class capture uses pinGate read lock, trust evidence lock, a short trust data read, then store lock after releasing trust data lock. Peer/common locks never overlap pin/trust/store locks. After releasing all pin/trust locks, the original Session and owner are rechecked under registry/common locks; elapsed time is sampled after waits. Class facts are observational snapshots: later C-3 or contact changes can make priority stale. They are not a permit to stage a pin, online R/T admission, or evidence of the user's intent toward X.

K requires a current explicit local contact fact, its confirmed current strict pin metadata and completed K2 mirror owner where present. Historical KnownContact Reference does not prove a current contact. A genuine confirmed delete/re-add may retain that historical Reference while using its new current fact. Independently confirmed N-sel still qualifies when unrelated K completion is pending. N-in requires confirmed current incoming metadata. Eligible but unconfirmed priority evidence refuses with ErrNotReady; R-only, pending and legacy/unclassified pins otherwise remain ordinary. Pin incarnation, store epoch and whole metadata protection revision bind acknowledgements; loaded historical bits do not manufacture a current acknowledgement. Unreadable or uncertain authority is not silently downgraded.

The existing pure core chooses protected K/N-sel and up to four oldest remaining aged/incoming sessions. This adapter does not change the approved aggregate-budget A policy, compute AdmittedAtR/T, promise six seconds, or guarantee availability. Degree/history policy, actual X-intent, complete online composition, G2 and activation remain separate.

## Verification scope

Source-authored functional parents use real TLS/F1 accepted and CM publication paths, actual strict storage outcomes, existing K/N-sel/N-in producers and bounded profile/cancellation cases. Controlled destructive, unreadable, pending-mirror and nil-registry cases are explicitly negative fixtures, not public RPC or full recovery deployment acceptance. Fixed table shape and a filled pin store are structural checks, not memory/RSS or load acceptance. Source guards additionally close private references, prohibit an unbounded reader and require the final cancellation check; they do not replace functional tests. Formatting, compilation, runtime/race and causal-control outcomes are still pending this source-only handoff.

## Русский контракт

Неактивный private observer получает обязательные копируемые M/V без production default. Он ограничивает выделение до чтения F1, считает все посещённые registry keys и при переполнении возвращает только полный отказ. Target входит в M, затем исключается до выбора четырёх старейших. Ограничение pin 20 000 не ограничивает F1. O(M) памяти и V+1 callback не означают жёсткую задержку CPU, mutex или отмены.

Класс читается только для настоящего текущего F1 owner/session. K требует нового актуального явного факта контакта, strict ACK и завершённого K2 owner; старый Reference сам по себе права не даёт и не запрещает корректное повторное добавление. Независимый подтверждённый N-sel сохраняет свой приоритет при ожидающем K. N-in требует текущего подтверждённого metadata. Старый epoch/incarnation, pending и неопределённое сохранение не превращаются в подтверждение. R и неклассифицированные записи остаются обычными.

После освобождения pin/trust/store повторно проверяется исходный Session; перед возвратом проверяется отмена. Результат — историческое наблюдение, не защита от последующего C-3 и не authority для stage. Онлайн-допуск R/T, намерение к X, численные профили, ресурсная приёмка, G2 и активация остаются отдельными. Тесты и causal recipes в этом пакете пока только написаны, не исполнены.

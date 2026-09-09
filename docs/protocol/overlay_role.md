# Overlay role classification (`Q`)

## English

### 1. Status and scope

**Not accepted, not implemented, not announced.** This page fixes the *contract* of one function so
that two implementations cannot disagree about it. It does **not** say the mechanism that uses the
function is adopted.

- the mechanism (role separation on the anonymous search path) is **direction 2 of blocker O5**, taken
  by the owner **for detailing and verification only** — see
  [`../refactoring/dht/21-anonymity-transport.md`](../refactoring/dht/21-anonymity-transport.md) §0″
  and §0‴;
- **O5 is open and G2 is not closed.** No release may call the transport anonymous, and no node may
  announce a capability for it, until 21c has passed;
- nothing in this page changes wire format, routing, or any network behaviour. The function has no
  production caller and must not acquire one before 21b.

### 2. What the function is — and what it is not

```
Q(NodeID) ∈ {0, 1}
```

`Q` answers exactly one question: **which half of the identifier space does this NodeID lie in.**

> ⚠️ **Computing `Q` is CLASSIFICATION, never AUTHENTICATION.**
>
> A NodeID can be copied and asserted by anybody. So can a public key. Computing a role from either
> proves nothing about who is on the other end of a connection. A node becomes a *participant* in a
> role only after the admission gate of §5, and no caller may substitute the one for the other.

`Q = 1` is the **structural** half — nodes that may carry the structural leg of an anonymous search.
`Q = 0` (written `¬Q`) is the **first-hop** half — nodes that may be chosen as the initiator's first
hop. A NodeID has exactly one value and it never changes, because the input never changes.

### 3. The function

```
Q(NodeID) = lsb( SHA-256( "corsa/overlay/role/v1" ‖ NodeID ) )
```

| Element | Value |
|---|---|
| hash | **SHA-256**, as `crypto/sha256` (`sha256.Sum256`) |
| domain separator | the ASCII bytes of `corsa/overlay/role/v1` — 21 bytes, **no terminating NUL**, no length prefix |
| concatenation | separator bytes immediately followed by NodeID bytes, nothing between them |
| NodeID | **exactly 20 raw bytes** (`domain.PeerIdentity`) |
| extracted bit | the **least significant bit of the last (32nd) byte** of the digest, i.e. `digest[31] & 1` |
| result | `1` → structural half; `0` → first-hop half |

**Why a separate hash rather than a bit of the NodeID itself.** A separate hash **removes the direct
tie between the role and a bit of the coordinate.** The NodeID *is* the coordinate in the XOR metric,
so taking a bit of it makes the role a function of position by construction: with a high bit, every
structural node shares that bit, and the distance from a target in the other half to any of them is
bounded below by that bit's weight. A nearest structural node still exists — over a non-empty finite
set the minimum XOR distance always exists — but it is nearest only within a set that the split has
pushed away, and the geometry no longer says anything useful about it. A low bit of the NodeID is
weaker in that respect, yet the independence of role from position would stay implicit and would need
re-proving whenever the network size or bucket shape changed.

⚠️ **What the separate hash does NOT do.** It does not establish connectivity, reachability, or an
even split of the population. Those are properties of the network, not of the function, and they are
**measured**: connectivity and search length by M1/M2, targets without a `Q` neighbour by M5,
population skew by M3 — see
[`../refactoring/dht/21-anonymity-transport.md`](../refactoring/dht/21-anonymity-transport.md)
§4.3.4″.6.

**Why SHA-256.** It is the only hash primitive in the tree (`sha256.Sum256`); introducing a second one
for a single bit would have to be justified and cannot be.

### 4. Canonical input — the part that silently breaks implementations

A NodeID appears in this system in **two representations**:

| Representation | Where |
|---|---|
| 20 raw bytes | `domain.PeerIdentity`, an array of 20 bytes |
| 40 lowercase hex characters | the peer *address*, produced by `identity.Fingerprint` = `hex(sha256(pubkey)[:20])` |

> **Only the 20 raw bytes are hashed.** A hex string is decoded to 20 bytes before it reaches the
> hash.

This is not a formality. Feeding the 40-character hex text instead of the 20 bytes produces a
completely different digest, and for the all-zero NodeID it also produces the **opposite** role:

| Input actually hashed | Digest | `Q` |
|---|---|---|
| 20 raw zero bytes | `301e784ce0a1a025b83514831b0e3a097da1a859e9c3ac91da9b261982928ab6` | `0` |
| the 40-character text `0000…0000` | `7182b5842b1e866982cd35a4dcfd144c9eedff707f439d09baa7a2b5f73133a9` | `1` |

Two implementations that agree on the hash, the separator and the bit, and differ only here, will
classify the same node into different halves — with no attacker involved. Vector V9 in §6 exists to
catch exactly this.

### 5. Admission: three conditions, none of which follows from `Q`

A node counts as a participant in the role `Q(NodeID)` assigns **only** when all three hold:

| # | Condition | Why it is not optional |
|---|---|---|
| 1 | **proof of key possession** on the connection | any material can be copied and presented; without a proof the same machine takes both halves for free, with no Sybil |
| 2 | **the key matches the NodeID** — `identity.VerifyPublicKeyFingerprint`, i.e. `NodeID = hex(sha256(pubkey)[:20])` | ⚠️ a node can honestly prove possession of key `K` and still claim `NodeID X ≠ Fingerprint(K)`, picking `X` in whichever half it wants. Without this check it holds its own key and wears somebody else's role |
| 3 | **binding to the specific connection** | the role applies to the session on which identity was proven, not to "that node in general" |

Condition 2 is a direct consequence of the input being the NodeID. It is verifiable by anyone, needs
no trusted third party, and must sit **in the admission path** — not "somewhere earlier in the code".

### 6. Test vectors

The vectors below are the normative reference. An implementation is conformant when it reproduces
**the full digest**, not merely the bit.

> ⚠️ **A one-bit comparison proves almost nothing:** two implementations that have diverged still
> agree on a single bit with probability about ½. That is why every vector carries its digest.

Inputs are reproducible. `V1` and `V2` are the extreme values; `V3`–`V8` are derived by a rule anyone
can recompute:

```
NodeID(Vk) = first 20 bytes of SHA-256( "corsa/overlay/role/v1/vector/" ‖ decimal(k - 2) )
```

for `k = 3 … 8`, where `decimal(n)` is the ASCII decimal of `n` without padding.

| # | Origin | NodeID (20 bytes, hex) | SHA-256 digest | Last byte | `Q` |
|---|---|---|---|---|---|
| `V1` | all-zero | `0000000000000000000000000000000000000000` | `301e784ce0a1a025b83514831b0e3a097da1a859e9c3ac91da9b261982928ab6` | `b6` | **0** |
| `V2` | all-ones | `ffffffffffffffffffffffffffffffffffffffff` | `2c76d911998fd56f963f2e7f3514e88c42c1d7d969da9348b0ad232cbbfaabef` | `ef` | **1** |
| `V3` | derived, n=1 | `48312d0240502678af2c59455fa48fc04cfd751e` | `753afef6fc6bdb450254436bb2dea652eafeaa4867f52c89435be26428ce44b1` | `b1` | **1** |
| `V4` | derived, n=2 | `0d05f2139163b02febd289b3c492baeb4a58c8f2` | `b483e93337be551d1b4828dde005d3a37aa342c827b824bf7d6762a3bb117447` | `47` | **1** |
| `V5` | derived, n=3 | `67f31ef9992f5bd32de9e7d09f262c462778e5ee` | `e1cdeca809a3f93a5a77850d0c9ae8a9cb6b31389c72e93c17c1ee404eb326f1` | `f1` | **1** |
| `V6` | derived, n=4 | `2cab5346ac6fa5dda132ecde6febf62e08d69e63` | `ed82818e79f88f005627d1ec039c1c109d47f02d4c0445d67d6031a944ac7604` | `04` | **0** |
| `V7` | derived, n=5 | `ee792b613f849eb71c06179c85ab086efdbdc991` | `ba576a6d7d9c3b4e8193590e07e0f22069d72694b0968f62a2c17aa12abb7062` | `62` | **0** |
| `V8` | derived, n=6 | `dc1e3462e586bbd00bfabbe15f10cc970b690cf7` | `6542a4a9f84f85a7d49b8bb6d36d0e4042f43dcb6c98625a0cdea0eeca25c343` | `43` | **1** |

Both role values occur (`Q = 0` in V1, V6, V7; `Q = 1` in V2, V3, V4, V5, V8), so a stuck
implementation that always answers the same thing fails the table rather than half of it.

**V9 — the negative vector.** Not a NodeID: it fixes what the answer must **not** be.

| # | What is hashed | Digest | `Q` |
|---|---|---|---|
| `V9` | the 40-character hex **text** of V1's NodeID | `7182b5842b1e866982cd35a4dcfd144c9eedff707f439d09baa7a2b5f73133a9` | `1` |

An implementation that returns V9's digest for V1's NodeID is decoding its input wrongly. Note that
here the wrong input also flips the role, which is why the mistake is not self-announcing.

### 7. Executable checks

`internal/core/domain/overlay_role_contract_test.go` holds the reference implementation of §3 **inside
the test file** and checks this page against it. The implementation lives in a test on purpose: the
mechanism is not accepted, and a production function without a consumer is the speculative
infrastructure this project has already had to cut once.

| Check | What it fixes |
|---|---|
| vectors | every digest and bit of §6 reproduced from the documented inputs |
| both values | the table contains `Q = 0` and `Q = 1` |
| derivation | `V3`–`V8` recomputed from the rule of §6, not copied |
| raw vs hex | hashing the hex text gives V9's digest, not V1's — canonicalisation is load-bearing |
| **S26** stability | the same NodeID yields the same value across repeated calls; the function reads no clock, no config and no state |
| classifier partition | over a large sample the halves partition the identifiers. ⚠️ This is **not** S22 — it exercises no selection rule and would not notice a selector that trusted a peer's announced role |
| **S22** selection | a model of choosing the first hop and the structural hop of one search against peers that **announce the opposite of their computed role**: selection must follow the computed role, the same identity is never eligible for both, and a negative control shows an announcement-trusting selector gives a different answer. The hostile peer is **vector V1**, whose role is published rather than searched for, and the lying population is seeded with the vectors so both roles are present by construction |
| distribution | with random NodeIDs neither half is empty (a sanity check on the bit, **not** evidence for the 50/50 assumption, which is measured elsewhere) |

⚠️ **What these checks do NOT show.** They say the function is well defined and agreed upon. They say
nothing about whether the mechanism built on it holds: role separation on real paths, refusal
frequency, connectivity of the structural half, and the admission gate of §5 are all verified
elsewhere, and until then the guarantee is not promised.

### 8. References

- [`../refactoring/dht/21-anonymity-transport.md`](../refactoring/dht/21-anonymity-transport.md) —
  §0‴ (this decision), §4.3.4″.2 (the contract in context), §4.3.4″.7 (scenarios)
- [`../refactoring/dht/06-overlay-responder.md`](../refactoring/dht/06-overlay-responder.md) §4.0 —
  the admission gate of §5
- [`../refactoring/dht/16-neighbour-selection.md`](../refactoring/dht/16-neighbour-selection.md)
  §3.1′ — the `Q`-neighbour quota that depends on this classification

---

## Русский

### 1. Статус и объём

**Не принят, не реализован, не объявляется.** Эта страница фиксирует *контракт* одной функции, чтобы
две реализации не могли разойтись в ней. Она **не** утверждает, что механизм, использующий функцию,
принят.

- механизм (разделение ролей на пути анонимного поиска) — это **направление 2 блокера O5**, взятое
  владельцем **только в детализацию и проверку**, см.
  [`../refactoring/dht/21-anonymity-transport.md`](../refactoring/dht/21-anonymity-transport.md) §0″
  и §0‴;
- **O5 открыт, G2 не закрыт.** Ни один выпуск не называется анонимным и ни один узел не объявляет
  соответствующую capability, пока не пройден 21c;
- ничто на этой странице не меняет формат провода, маршрутизацию и вообще сетевое поведение. У функции
  нет производственного потребителя, и он не должен появиться раньше 21b.

### 2. Что эта функция — и чем она не является

```
Q(NodeID) ∈ {0, 1}
```

`Q` отвечает ровно на один вопрос: **в какой половине пространства идентификаторов лежит этот
NodeID.**

> ⚠️ **Вычисление `Q` — это КЛАССИФИКАЦИЯ, а не АУТЕНТИФИКАЦИЯ.**
>
> `NodeID` может скопировать и предъявить кто угодно. Публичный ключ — тоже. Вычисление роли из
> любого из них не доказывает ничего о том, кто находится на другом конце соединения. Участником
> роли узел становится только после гейта допуска §5, и подменять одно другим не вправе ни один
> вызывающий.

`Q = 1` — **структурная** половина: узлы, которым разрешено нести структурный участок анонимного
поиска. `Q = 0` (пишется `¬Q`) — половина **первых хопов**: узлы, которых инициатор вправе выбрать
первым хопом. У `NodeID` ровно одно значение, и оно не меняется, потому что не меняется вход.

### 3. Функция

```
Q(NodeID) = младший бит SHA-256( "corsa/overlay/role/v1" ‖ NodeID )
```

| Элемент | Значение |
|---|---|
| хеш | **SHA-256**, как `crypto/sha256` (`sha256.Sum256`) |
| доменный разделитель | ASCII-байты строки `corsa/overlay/role/v1` — 21 байт, **без завершающего нуля**, без префикса длины |
| конкатенация | байты разделителя, сразу за ними байты `NodeID`, между ними ничего |
| `NodeID` | **ровно 20 сырых байт** (`domain.PeerIdentity`) |
| извлекаемый бит | **младший бит последнего (32-го) байта** дайджеста, то есть `digest[31] & 1` |
| результат | `1` → структурная половина; `0` → половина первых хопов |

**Почему отдельный хеш, а не бит самого `NodeID`.** Отдельный хеш **устраняет прямую привязку роли к
биту координаты.** `NodeID` и есть координата в XOR-метрике, поэтому бит от него делает роль функцией
позиции по построению: при старшем бите все структурные узлы совпадают в нём, и расстояние от цели из
другой половины до любого из них снизу ограничено весом этого бита. Ближайший структурный узел при
этом **существует** — у непустого конечного множества минимум XOR-расстояния есть всегда, — но он
ближайший лишь внутри множества, которое разбиение отодвинуло, и геометрия о нём больше ничего
полезного не говорит. Младший бит `NodeID` в этом смысле слабее, но независимость роли от позиции
осталась бы неявной, и её пришлось бы передоказывать при каждой смене размера сети или формы бакетов.

⚠️ **Чего отдельный хеш НЕ делает.** Он не устанавливает связность, достижимость и равномерность
разбиения популяции. Это свойства сети, а не функции, и они **измеряются**: связность и длина поиска —
M1/M2, цели без `Q`-соседа — M5, перекос популяции — M3, см.
[`../refactoring/dht/21-anonymity-transport.md`](../refactoring/dht/21-anonymity-transport.md)
§4.3.4″.6.

**Почему SHA-256.** Это единственный хеш-примитив в дереве (`sha256.Sum256`); заводить второй ради
одного бита пришлось бы обосновывать, а обосновать нечем.

### 4. Каноническая форма входа — то, на чём реализации расходятся молча

`NodeID` живёт в системе в **двух представлениях**:

| Представление | Где |
|---|---|
| 20 сырых байт | `domain.PeerIdentity`, массив из 20 байт |
| 40 строчных hex-символов | адрес пира, который выдаёт `identity.Fingerprint` = `hex(sha256(pubkey)[:20])` |

> **В хеш подаются только 20 сырых байт.** Hex-строка декодируется в 20 байт до того, как дойдёт до
> хеша.

Это не формальность. Подача 40-символьного hex-текста вместо 20 байт даёт совершенно другой дайджест,
а для нулевого `NodeID` — ещё и **противоположную** роль:

| Что реально хешируется | Дайджест | `Q` |
|---|---|---|
| 20 сырых нулевых байт | `301e784ce0a1a025b83514831b0e3a097da1a859e9c3ac91da9b261982928ab6` | `0` |
| 40-символьный текст `0000…0000` | `7182b5842b1e866982cd35a4dcfd144c9eedff707f439d09baa7a2b5f73133a9` | `1` |

Две реализации, согласные в хеше, разделителе и бите и расходящиеся только здесь, разложат один и тот
же узел по разным половинам — без всякого атакующего. Вектор V9 в §6 существует ровно ради этого
случая.

### 5. Допуск: три условия, и ни одно не следует из `Q`

Узел считается участником роли, которую назначает `Q(NodeID)`, **только** при выполнении всех трёх:

| № | Условие | Почему оно не факультативно |
|---|---|---|
| 1 | **доказательство владения ключом** на соединении | любой материал копируется и предъявляется; без доказательства одна машина берёт обе половины бесплатно и без Sybil |
| 2 | **соответствие ключа и `NodeID`** — `identity.VerifyPublicKeyFingerprint`, то есть `NodeID = hex(sha256(pubkey)[:20])` | ⚠️ узел может честно доказать владение ключом `K` и при этом заявить `NodeID X ≠ Fingerprint(K)`, выбрав `X` в нужной половине. Без этой проверки он владеет своим ключом и носит чужую роль |
| 3 | **привязка к конкретному соединению** | роль действует для той сессии, на которой доказана identity, а не для «узла вообще» |

Условие 2 — прямое следствие того, что вход функции есть `NodeID`. Оно проверяется каждым, не требует
доверенной третьей стороны и обязано стоять **в пути допуска**, а не «где-то раньше по коду».

### 6. Тестовые векторы

Векторы ниже нормативны. Реализация соответствует контракту, когда воспроизводит **полный дайджест**,
а не только бит.

> ⚠️ **Сверка по одному биту не доказывает почти ничего:** две разошедшиеся реализации совпадают в
> одном бите с вероятностью около ½. Поэтому у каждого вектора приведён дайджест.

Входы воспроизводимы. `V1` и `V2` — крайние значения; `V3`–`V8` выводятся правилом, которое любой
может пересчитать:

```
NodeID(Vk) = первые 20 байт SHA-256( "corsa/overlay/role/v1/vector/" ‖ decimal(k − 2) )
```

для `k = 3 … 8`, где `decimal(n)` — ASCII-запись `n` десятичными цифрами без дополнения.

| № | Происхождение | NodeID (20 байт, hex) | Дайджест SHA-256 | Последний байт | `Q` |
|---|---|---|---|---|---|
| `V1` | все нули | `0000000000000000000000000000000000000000` | `301e784ce0a1a025b83514831b0e3a097da1a859e9c3ac91da9b261982928ab6` | `b6` | **0** |
| `V2` | все единицы | `ffffffffffffffffffffffffffffffffffffffff` | `2c76d911998fd56f963f2e7f3514e88c42c1d7d969da9348b0ad232cbbfaabef` | `ef` | **1** |
| `V3` | выведен, n=1 | `48312d0240502678af2c59455fa48fc04cfd751e` | `753afef6fc6bdb450254436bb2dea652eafeaa4867f52c89435be26428ce44b1` | `b1` | **1** |
| `V4` | выведен, n=2 | `0d05f2139163b02febd289b3c492baeb4a58c8f2` | `b483e93337be551d1b4828dde005d3a37aa342c827b824bf7d6762a3bb117447` | `47` | **1** |
| `V5` | выведен, n=3 | `67f31ef9992f5bd32de9e7d09f262c462778e5ee` | `e1cdeca809a3f93a5a77850d0c9ae8a9cb6b31389c72e93c17c1ee404eb326f1` | `f1` | **1** |
| `V6` | выведен, n=4 | `2cab5346ac6fa5dda132ecde6febf62e08d69e63` | `ed82818e79f88f005627d1ec039c1c109d47f02d4c0445d67d6031a944ac7604` | `04` | **0** |
| `V7` | выведен, n=5 | `ee792b613f849eb71c06179c85ab086efdbdc991` | `ba576a6d7d9c3b4e8193590e07e0f22069d72694b0968f62a2c17aa12abb7062` | `62` | **0** |
| `V8` | выведен, n=6 | `dc1e3462e586bbd00bfabbe15f10cc970b690cf7` | `6542a4a9f84f85a7d49b8bb6d36d0e4042f43dcb6c98625a0cdea0eeca25c343` | `43` | **1** |

Присутствуют оба значения роли (`Q = 0` — V1, V6, V7; `Q = 1` — V2, V3, V4, V5, V8), поэтому
залипшая реализация, всегда отвечающая одинаково, проваливает таблицу, а не половину её.

**V9 — отрицательный вектор.** Это не `NodeID`: он фиксирует, каким ответ быть **не** должен.

| № | Что хешируется | Дайджест | `Q` |
|---|---|---|---|
| `V9` | 40-символьный hex-**текст** `NodeID` из V1 | `7182b5842b1e866982cd35a4dcfd144c9eedff707f439d09baa7a2b5f73133a9` | `1` |

Реализация, выдающая дайджест V9 на `NodeID` из V1, декодирует свой вход неверно. Заметьте, что здесь
неверный вход ещё и переворачивает роль — поэтому ошибка о себе не сообщает.

### 7. Исполнимые проверки

`internal/core/domain/overlay_role_contract_test.go` держит эталонную реализацию §3 **внутри тестового
файла** и сверяет с ней эту страницу. Реализация лежит в тесте намеренно: механизм не принят, а
производственная функция без потребителя — та самая спекулятивная инфраструктура, которую в этом
проекте уже приходилось вырезать.

| Проверка | Что фиксирует |
|---|---|
| векторы | каждый дайджест и бит §6 воспроизводятся из документированных входов |
| оба значения | в таблице присутствуют и `Q = 0`, и `Q = 1` |
| вывод входов | `V3`–`V8` пересчитываются по правилу §6, а не копируются |
| сырые байты против hex | хеш от hex-текста даёт дайджест V9, а не V1 — канонизация несущая |
| **S26** стабильность | один и тот же `NodeID` даёт одно значение при повторных вызовах; функция не читает ни часы, ни конфиг, ни состояние |
| разбиение классификатора | на большой выборке половины разбивают идентификаторы. ⚠️ Это **не** S22 — правил выбора она не проверяет и не заметила бы селектор, доверяющий объявленной роли |
| **S22** выбор | модель выбора первого и структурного хопа одного поиска против пиров, которые **объявляют роль, противоположную вычисленной**: выбор обязан идти по вычисленной роли, одна identity не годится в обе, а отрицательный контроль показывает, что селектор, доверяющий объявлению, даёт другой ответ. Враждебный пир — **вектор V1**, чья роль опубликована, а не разыскивается; лгущая популяция засеяна векторами, поэтому обе роли присутствуют по построению |
| распределение | на случайных `NodeID` ни одна половина не пуста (проверка вменяемости бита, **а не** свидетельство в пользу допущения 50/50 — оно измеряется отдельно) |

⚠️ **Чего эти проверки НЕ показывают.** Они говорят, что функция определена и согласована. Они ничего
не говорят о том, держится ли построенный на ней механизм: разделение ролей на фактических путях,
частота отказов, связность структурной половины и гейт допуска §5 проверяются в других местах, и до
тех пор гарантия не обещается.

### 8. Ссылки

- [`../refactoring/dht/21-anonymity-transport.md`](../refactoring/dht/21-anonymity-transport.md) —
  §0‴ (это решение), §4.3.4″.2 (контракт в контексте), §4.3.4″.7 (сценарии)
- [`../refactoring/dht/06-overlay-responder.md`](../refactoring/dht/06-overlay-responder.md) §4.0 —
  гейт допуска из §5
- [`../refactoring/dht/16-neighbour-selection.md`](../refactoring/dht/16-neighbour-selection.md)
  §3.1′ — квота `Q`-соседей, зависящая от этой классификации

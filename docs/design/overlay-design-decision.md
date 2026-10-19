# Overlay design decision D

## English

### Decision and record

On **2026-10-18**, the owner approved the complete text of act D dated **2026-10-16**, including its
scope and measurement limits, resource qualifications, deferred obligations and exclusions. **O5 is
closed within the scope below.** This tracked technical account records that decision; it neither
replaces the original signed act nor extends its meaning. The signature records the owner's explicit
approval in the task chat; it is not a cryptographic signature.

| Record | SHA-256 |
|---|---|
| Complete act before its signature was entered | `b05cf3c242c1d07cf8d89ad625957d16f38c50c95a53659ebdb6b72f48c5f5c5` |
| Unchanged sections 1–7 accepted by the owner | `8a052ed34fa1a97ad34d7e7ca4256bd637531a1d31cf62aa66e035be80d35042` |

The second digest covers the original UTF-8 bytes from the line beginning `## 1.` through the line
before `## 8.`, including intervening blank lines. For the original file, the extraction is
`sed -n '/^## 1\./,/^## 8\./{ /^## 8\./!p; }'` piped to SHA-256. Sections 1–7 were unchanged when the
approval and signature were recorded. The digests identify the original text, not this account.

### Accepted candidate and scope

| Parameter | Accepted value |
|---|---|
| Neighbour policy | `initiated-limit`: at most `d` initiated links; inbound links do not stop seeking the node's own set; total degree bounded by `B` |
| Model implementation stamp | `baf45d4433ed1419`, in `internal/overlaysim` |
| Initiated degree / total degree bound | `d = 16`, `B = 32` |
| Q-neighbour quota | `q1`: one Q neighbour; chosen among nonzero quotas because q0 disables retention |
| Design scope | Networks of up to 10,000 nodes |
| Measured sizes | `1k×16` and `10k×16` for connectivity and search; `10k×16` for first-hop refusal and Q-fraction sensitivity |

The scope reduction and acceptance of two measured sizes are explicit changes to the requirements.
The former `64k×8` case is a recorded limitation: the old candidate has split structural components
and poor greedy-search reachability there. It did not pass. The accepted `d = 16` candidate was not
measured at 64k. Networks smaller than 1,000 and intermediate sizes were not separately measured.
Changing the policy, degree, quota or scope requires new candidate measurements; the obligations
below continue to apply.

### Accepted model evidence

The measurements use graph seeds 1–5. Each search form has 500 pairs per seed (2,500 pairs), with the
same pair selection for the full graph and structural half. Search is greedy and single-path in the
model; these results do not establish production-network lookup behaviour.

| Criterion | `1k×16`, q1 | `10k×16`, q1 |
|---|---|---|
| Q connectivity | One component on every seed; no isolated Q nodes | One component on every seed; no isolated Q nodes |
| Full-graph search success | 2,494/2,500 = 99.76%; Wilson 95% interval 99.48–99.89% | 2,484/2,500 = 99.36%; Wilson 95% interval 98.96–99.61% |
| Structural-half success, reported separately | 95.36% | 89.56% |
| Half/full success ratio | 0.956 | 0.901 |
| M2-G, pooled | 1.00 | 1.00 |

The accepted thresholds are M2-G ≤ 1.25 with at least 200 common successful pairs pooled and 20 per
seed; half/full success ≥ 0.5 with at least 100 full-graph successes; and full-graph success ≥ 95%
on each form by the pooled point estimate, accompanied by its 95% interval and each seed's result.
**No 95% threshold was assigned to structural-half success.** M2-G selects pairs successful in both
graphs and remains subject to that selection bias.

At `10k×16`, q1, the first Q fraction with connectivity on every seed of the measured grid is 0.25
(grid 0.05–0.95, step 0.05). This is a boundary on that grid and those seeds, not a general safe
fraction. First-hop refusal was measured and accepted in the confirmed-guard model as a lower
estimate: 0.0115% for uniform targets and 0.0350% for popular contacts. The separate case in which the
only guard is the target gives 100% refusal. The dependence on Q fraction was presented, but no M4
acceptability boundary or upper bound on real-network risk was established. These refusal and
fraction-sensitivity measurements were not performed at `1k×16`.

The model scenarios S22, S26 and S24a were executed. The Q input and formula remain specified in
[Overlay role](../protocol/overlay_role.md). This does not complete the production-path obligations
in the table below.

### Resource qualifications

The `10k×16` graph has mean degree 29.21 and maximum 32; 48.4% of nodes reach the degree bound. The
fixed-session memory calculation is about 12.9 MB per node, at most 14.2 MB, using the earlier
442,749-byte session estimate. These are model calculations, not a measured total node footprint.

After the idle-session timeout fix, a small bench of real v2 mesh sessions measured 15/26/49 bytes/s
per session (tx+rx) at 8/16/32 neighbours, with zero reconnects in its idle windows. It used a star
with at most 33 nodes, a route table of the star's size, loopback, one process, no Tor and no packet
loss. The approximate 1.35 KB/s per-node figure at 29.2 sessions is an extrapolation of that star,
not measured maintenance cost for a 10k network. Large-table periodic full sync was not measured.
The historical 103 MB/day and 2.8 MB/day estimates are not accepted full-cost figures. Compatibility
of the timeout fix in this act's bench was checked with emulation, not published binaries, and the
bench inherited its pending harness re-review.

The act does not approve the D1 resource budget. It does not establish whether D2's structural
sessions add to mesh sessions or overlap them. It does not establish churn resilience, real-network
resource compliance, an upper risk bound, multipath or random-choice search performance, G2, or
permission to claim anonymity. The guarantee is per verified identity, not per operator; Q
separation does not establish Sybil resistance.

### Obligations preserved by the approval

| ID | Required work and acceptance | Stage and responsible role |
|---|---|---|
| O-1a | Measure route probes (`route_probe_v1` / `route_probe_ack_v1`), `route_sync_digest_v1` and periodic full sync (`route_announce_v3`) on a route table at the accepted scale, up to 10,000 identities. Report bytes/s per session and node by frame class, with identical baseline/candidate bench configuration. The owner decides acceptability against the D1 budget. | 13b-D1 bench executor, on the final candidate before PR-6 merges and before D1 release |
| O-1b | Measure the same costs at the structural session count (mean about 29.2, bound 32); establish whether these sessions add to mesh sessions. Resource acceptability and the production `B` default need an owner decision. | 13b-D2 bench executor, before D2 activation |
| O-2 | Re-register and measure M6 for `initiated-limit`, d16/B32/q1: population, probe cost and churn recovery. Present the three required discovery axes. Earlier `C1/v1`, d8 results do not establish this candidate's behaviour. Runs need separate authorisation and a journal under the shared heavy-run lock. | Model-measurement executor, prerequisite to bucket-discovery / D2 |
| O-3 | Check D1 release notes before publication: the guarantee concerns one verified identity, not an operator; Q separation gives no Sybil resistance; no anonymity claim. Attach the check to release admission. | D1 release owner, mandatory for the first release containing PR-6 |
| O-4 | On actual paths, target exclusion must not feed guard replenishment (`topUpLocked`), and a substitute for the target must not enter through `NoteUsed`; separate the hot `primaryLocked` list from selection of an eligible guard; prove key possession bound to the connection and all three guard admission conditions (live, transit-capable, proven identity). Require green S24b and S27 for those paths. | Stage 2 implementation (21b), verification (21c); guard developer and responder developer for proof |

G2, secure-session admission, network observation, D1 activation and release remain separate
decisions and checks under [Releases](../release.md) and the
[release admission manifest](../../packaging/release-admission.json). This design approval changes
no production code, connection-budget defaults or `ProtocolVersion`. `B = 32` here bounds the model
graph; it does not set the production connection budget.

## Русский

### Решение и фиксация

**2026-10-18** владелец утвердил текст акта D от **2026-10-16 целиком**, включая область и ограничения
измерений, ресурсные оговорки, перенесённые обязательства и исключения. **O5 закрыт в указанной ниже
области.** Это отслеживаемое техническое изложение фиксирует решение, не заменяет исходный
подписанный акт и не расширяет его смысл. Подпись фиксирует явное утверждение владельца в чате
задачи; это не криптографическая подпись.

| Запись | SHA-256 |
|---|---|
| Акт целиком до оформления подписи | `b05cf3c242c1d07cf8d89ad625957d16f38c50c95a53659ebdb6b72f48c5f5c5` |
| Неизменённые §§1–7, принятые владельцем | `8a052ed34fa1a97ad34d7e7ca4256bd637531a1d31cf62aa66e035be80d35042` |

Второй отпечаток считается по исходным байтам UTF-8 от строки `## 1.` до строки перед `## 8.`,
включая промежуточные пустые строки. Для исходного файла извлечение:
`sed -n '/^## 1\./,/^## 8\./{ /^## 8\./!p; }'`, затем SHA-256. При фиксации утверждения и подписи
§§1–7 не менялись. Отпечатки идентифицируют исходный текст, а не это изложение.

### Принятый кандидат и область

| Параметр | Принятое значение |
|---|---|
| Политика соседства | `initiated-limit`: инициированных связей не больше `d`; входящие не останавливают поиск собственного набора; общая степень ограничена `B` |
| Штамп реализации модели | `baf45d4433ed1419`, `internal/overlaysim` |
| Инициированная степень / общий потолок | `d = 16`, `B = 32` |
| Квота Q-соседей | `q1`: один Q-сосед; выбрана среди ненулевых квот, поскольку q0 отключает удержание |
| Область проектирования | Сети до 10 000 узлов |
| Измеренные размеры | `1k×16` и `10k×16` для связности и поиска; `10k×16` для отказов первого хопа и зависимости от доли Q |

Сужение области и принятие двух измеренных размеров — явное изменение требований. Прежняя форма
`64k×8` сохранена как выявленное ограничение: старый кандидат имеет расколотые структурные компоненты
и низкую доходимость жадного поиска. Проверка не пройдена. Принятый кандидат с `d = 16` на 64k не
измерялся. Сети меньше 1 000 и промежуточные размеры отдельно не измерены. Изменение политики,
степени, квоты или области требует новых измерений кандидата; обязательства ниже сохраняются.

### Принятые результаты модели

Измерения используют graph seed 1–5. На каждой форме поиска — 500 пар на seed (2 500 пар), одна
выборка для полного графа и структурной половины. Поиск в модели жадный и однопутевой; эти результаты
не доказывают поведение поиска в реальной сети.

| Критерий | `1k×16`, q1 | `10k×16`, q1 |
|---|---|---|
| Связность Q | Одна компонента на каждом seed; изолированных Q нет | Одна компонента на каждом seed; изолированных Q нет |
| Доходимость полного графа | 2 494/2 500 = 99,76 %; интервал Уилсона 95 %: 99,48–99,89 % | 2 484/2 500 = 99,36 %; интервал Уилсона 95 %: 98,96–99,61 % |
| Доходимость половины, отдельно | 95,36 % | 89,56 % |
| Отношение успехов половина/полный | 0,956 | 0,901 |
| M2-G сводно | 1,00 | 1,00 |

Принятые пороги: M2-G ≤ 1,25 при ≥200 общих успешных парах сводно и ≥20 на каждом seed; отношение
доходимости половина/полный ≥0,5 при ≥100 успехах полного графа; доходимость полного графа ≥95 % на
каждой форме по сводной точечной оценке, рядом 95 %-интервал и результат каждого seed. **Порог 95 %
для доходимости половины не назначался.** M2-G выбирает пары, успешные в обоих графах, и сохраняет
смещение из-за этого отбора.

На `10k×16`, q1 первая доля Q со связностью на каждом seed исследованной сетки — 0,25 (сетка
0,05–0,95 с шагом 0,05). Это граница на этой сетке и этих seed, а не общая безопасная доля. Отказы
первого хопа измерены и приняты в модели подтверждённых guard как нижняя оценка: 0,0115 % для
равномерных целей и 0,0350 % для популярных контактов. Отдельный случай «единственный guard — цель»
даёт 100 % отказов. Зависимость от доли Q предъявлена, но граница допустимости M4 и верхняя граница
риска реальной сети не установлены. Эти измерения отказов и зависимости от доли Q на `1k×16` не
проводились.

Модельные сценарии S22, S26 и S24а исполнены. Вход Q и формула зафиксированы в
[контракте роли оверлея](../protocol/overlay_role.md). Это не выполняет обязательства на фактических
путях из таблицы ниже.

### Ресурсные оговорки

У графа `10k×16` средняя степень 29,21, максимум 32; 48,4 % узлов достигают потолка степени. Расчёт
фиксированной памяти сессий — около 12,9 МБ на узел, максимум 14,2 МБ, по прежней оценке 442 749 Б на
сессию. Это расчёт модели, а не измеренное полное потребление узла.

После исправления таймаута простаивающих сессий малый стенд реальных mesh-сессий v2 измерил
15/26/49 Б/с на сессию (tx+rx) при 8/16/32 соседях и ноль переподключений в окнах простоя. Условия:
звезда до 33 узлов, таблица маршрутов размера звезды, loopback, один процесс, без Tor и потерь.
Около 1,35 КБ/с на узел при 29,2 сессиях — экстраполяция этой звезды, а не измеренная стоимость
поддержания сети 10k. Периодический full sync на большой таблице не измерен. Исторические оценки
103 МБ/сут и 2,8 МБ/сут не приняты как полная стоимость. Совместимость исправления таймаута в замере
акта проверялась эмуляцией, а не опубликованными бинарниками; замер наследует ожидавшее повторного
ревью состояние обвязки стенда.

Акт не утверждает ресурсный бюджет D1. Не установлено, добавляются ли структурные сессии D2 к mesh
или пересекаются с ними. Не подтверждены устойчивость под churn, соблюдение ресурсного бюджета
реальной сети, верхняя граница риска, многопутевой поиск и случайный выбор, G2 и право заявлять
анонимность. Гарантия относится к одной проверенной identity, а не к оператору; разделение Q не
обеспечивает Sybil-стойкость.

### Сохранённые обязательства

| ID | Работа и условие приёмки | Этап и исполнитель |
|---|---|---|
| О-1а | Измерить пробы маршрутов (`route_probe_v1` / `route_probe_ack_v1`), `route_sync_digest_v1` и периодический full sync (`route_announce_v3`) на таблице масштаба принятой области, до 10 000 identity. Предъявить Б/с на сессию и узел по классам кадров при одинаковой конфигурации базы и кандидата. Приемлемость по бюджету D1 решает владелец. | Исполнитель 13b-D1, на финальном кандидате до слияния PR-6 и выпуска D1 |
| О-1б | Измерить ту же стоимость при числе структурных сессий кандидата (среднее около 29,2, потолок 32); установить аддитивность к mesh. Приемлемость и боевой дефолт `B` требуют решения владельца. | Исполнитель 13b-D2, до активации D2 |
| О-2 | Заново зарегистрировать и измерить M6 для `initiated-limit`, d16/B32/q1: наполнение, стоимость проб и восстановление после churn. Предъявить три требуемые оси наполнения. Прежние результаты `C1/v1`, d8 не подтверждают поведение этого кандидата. Запуски — по отдельному разрешению, с журналом и общим замком тяжёлых прогонов. | Исполнитель измерений модели, входное условие наполнения бакетов / D2 |
| О-3 | До публикации проверить release notes D1: гарантия для одной проверенной identity, а не оператора; разделение Q не даёт Sybil-стойкости; заявлений об анонимности нет. Сверку приложить к допуску выпуска. | Выпускающий D1, обязательно для первого выпуска с PR-6 |
| О-4 | На фактических путях исключение цели не питает пополнение guard-набора (`topUpLocked`), запасной вместо цели не принимается через `NoteUsed`; горячий список `primaryLocked` отделён от поиска пригодного guard; доказано владение ключом с привязкой к соединению и все три условия допуска (живость, транзитность, доказанная identity). Требуются зелёные S24б и S27 на этих путях. | Реализация ступени 2 (21b), проверка (21c); разработчик guard и разработчик ответчика для доказательства |

G2, допуск защищённой сессии, наблюдение сети, активация D1 и выпуск остаются отдельными решениями и
проверками по [порядку выпуска](../release.md) и
[манифесту допуска](../../packaging/release-admission.json). Это принятие дизайна не меняет боевой
код, дефолты бюджета соединений или `ProtocolVersion`. `B = 32` здесь — потолок степени графа модели,
а не установка боевого бюджета соединений.

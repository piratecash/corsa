# D1: исходный ANSWER relay и общий writer, неактивный Source срез

C3g соединяет настоящий обратный путь известной соседней цели после C3e и
C3f core. Обе bounded read lanes принимают request и ANSWER; исторический
successful-read point не обновляется после Node/admission waits. Strict parse
идёт после допуска. Исходная outbound owned ветка дополнительно проверяет свой
командный bucket до strict parser, inbound сохраняет прежний gate.

Одноразовый ANSWER связывается с точными исходными target Session, forwarded ID,
Target и pending generation. После coordinator/pending/calendar waits original
Consumer получает current clock→auth.R lease и один настоящий Sample. Старый
class/degree cut не переигрывает уже принятое обязательство. Проверяются
received и исходный expiry. Relay меняет только envelope ID на исходный ID
источника и не проверяет target signature. Dispatching сохраняется как
TerminalDispatch до прежнего Finish; AwaitingAnswer освобождается точно.

Общий private OwnedAnswerWork хранит owner/source/body и неизменный expiry.
Relay берёт entry.expires; local ReadyCopy — дедлайн этого receive; completion
— исходный signBy shared job до очистки. Нового now+6s нет. Public raw factory
не добавлен. Node выбирает исходный common/core до очереди. Один typed union
использует прежний NetCore writer. После deadline syscall Take проверяет
исходный source и f < expiry под настоящим current R; следующий action —
raw Write. Только полный n и nil error дают успех; Finish обязателен до capture
и clear deadline. End после Take не мешает Finish. Gate/drain отменяют оба
варианта без замены ConnID, повтора или возврата принятого долга.

Source-тест трёх узлов проводит request/ANSWER по настоящим защищённым исходным
соединениям и NetCore writers, сохраняя signed body/исходный ID и проверяя ответ
по retained challenge инициатора. Отдельно покрыты wrong Session/ID/replay,
свежий expiry после calendar wait и equality, End, исчерпанный настоящий
командный bucket до parse/Offer/reply, fault/drain обоих вариантов. Fault
endpoints — unit fixtures, не подмена доказательства исходного socket.

База — текущий физический C3f core1961 (manifest77643573), включая
FORMAT, точные7 pin-carriers и усиленные Take-wait fixtures; общий helper
уже в базе и добавляется один раз. Native
факты C3e/C3f и исторического QA не переносятся. Открыты local Node self/signing,
полный13b initiator с enumeration/timeout/cancellation, полная inverse closure,
PR5/accounting, construction writers/sealConstructionReady и final QA/release.
Unknown target в D1 требует молчания; рекурсия и public initiator относятся к
D2. Capability, activation и Ready не включены.

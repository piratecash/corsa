# Private owned round execution / SOURCE-only

Этот неактивный slice выполняет один C3b ticket под исходным BUSY claim. Все обычные выходы возвращают LIVE; отдельный consumed исходного plan запрещает replay всех копий ticket. Перед R проверяются полный scheduling snapshot и все оставшиеся исходные provisional pointer/generation/Session/request/deadline facts.

Каждый кандидат проходит общий C3a body с собственным оригинальным R: ожидания раньше R; Sample перед проверкой всех originals; strict q/f/frontier; последний source CAS и fixed suffix. Receipt и cursor/RR обновляются в этом suffix до Release под теми же mutex. После unlock сравниваются только локальные receipt values, не mutable committed row.

Refusal удаляет только точные original provisional generations. Reserved/Dispatching/Awaiting/TerminalDispatch, их debt/correlation/alias/source и обязательный Finish сохраняются. Stale/new round не закрывается. Root ещё должен независимо проверить Source, применить physical recipe, FORMAT, TYPE и causal tests. Ничего из этого здесь не запускалось.

Receive/strictdispatcher/admission provenance, writer/reader/inverse-edge Ready closure, Dispatch/Take/Finish/T, ресурсные профили, activation и G2 остаются NULL. Production callers/defaults не добавлены.

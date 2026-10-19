# Routing receive baseline ownership

A routing full accepted during outbound setup belongs to that exact connection.
Its receive evidence survives successful publication of the same session. Every
new actual connection starts without a baseline, including additional relay links
and grace reconnects. Neither an old connection's full nor identity-global send
state authorizes a new connection's delta. Missing, uninitialised, and closed
receive owners fail closed. Closing a failed or evicted setup closes its evidence;
late frames cannot reopen it. The outbound session and registered ConnID entry
share one owner; inbound dispatch resolves its own registered owner.

Receive state holds only active/baseline flags and an optional v3 epoch watermark.
It is bounded by actual connections. A higher epoch invalidates baseline validity
before any delta can apply. Repeated deltas of that epoch remain gated until a
validated full establishes its baseline. Lower epochs remain stale. The epoch
transition is shared with legacy registry state so explicit older test fixtures
exercise the same rule; production dispatch uses connection ownership exclusively.
Existing sender authentication, input validation, quarantine, limits, capability
gates, send-side reconnect resets, refcounts, and grace handling retain their
existing roles. Receive evidence does not authenticate a claimed legacy identity.

## Русский

Full, принятый во время outbound setup, принадлежит именно этому соединению.
Его receive evidence сохраняется при успешной публикации той же session. Новое
actual-соединение начинает без baseline, включая additional relay и grace
reconnect. Full прежнего соединения и identity-global send state не разрешают
новой session принимать delta. Отсутствующий, неинициализированный или закрытый
владелец отказывает. Закрытие неуспешного или вытесненного setup закрывает evidence;
поздний кадр не открывает его заново. Outbound session и ConnID entry разделяют
одного владельца; inbound dispatcher получает владельца своей записи ConnID.

Состояние ограничено флагами active/baseline и optional v3 epoch watermark на
соединение. Более высокий epoch сбрасывает baseline до применения delta.
Повторные delta того же epoch отказываются до принятого валидного full.
Меньший epoch остаётся stale. Одинаковая функция перехода используется старым
registry state для явных test-only фикстур; production получает evidence только
от actual connection. Аутентификация, валидация, quarantine, лимиты, capability,
send-side reset, refcounts и grace сохраняют прежние обязанности. Receive evidence
сам по себе не аутентифицирует identity, заявленную legacy peer.

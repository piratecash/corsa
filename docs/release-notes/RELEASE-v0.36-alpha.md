# What's New in v0.36-alpha

## Smoother Desktop Message Composer

The desktop DM composer now behaves more like a modern chat input: it auto-focuses when you move into message entry and it auto-resizes as the text grows.

This makes desktop messaging feel faster and less cramped, especially when switching between contacts or composing longer multi-line messages.

## New `getActiveConnections` Observability Command

RPC now exposes a `getActiveConnections` command for inspecting all currently live peer sessions. Unlike slot-oriented views, this command is connection-oriented and reports the active TCP sockets that have completed the handshake, including direction, peer identity, network classification, connection ID, health state, and related slot state when available.

That gives operators and tooling a clearer picture of what the node is actually holding open right now, which is especially useful when debugging multi-session peers, live connection churn, or mismatches between connection-manager slots and real sockets.

## Desktop Peer View No Longer Freezes During Probe Cycles

The desktop peer-health path now narrows lock scope during `ProbeNode`, preventing the UI from blocking behind heavier peer-state reads.

In practice, this makes the desktop peers view feel more responsive under load and reduces the chance of noticeable freezes while the application is polling node state.

---

# Что нового в v0.36-alpha

## Более плавный desktop composer для сообщений

Desktop DM composer теперь ведёт себя ближе к современному chat input: он автоматически получает фокус, когда пользователь переходит к вводу сообщения, и автоматически увеличивает свою высоту по мере роста текста.

Это делает messaging в desktop быстрее и менее тесным, особенно при переключении между контактами и наборе более длинных multi-line сообщений.

## Новая observability-команда `getActiveConnections`

В RPC появилась команда `getActiveConnections` для просмотра всех текущих живых peer sessions. В отличие от slot-oriented представлений, эта команда ориентирована именно на соединения и показывает активные TCP-сокеты, которые уже прошли handshake, включая направление, peer identity, классификацию сети, connection ID, health state и связанное состояние slot'а, если оно есть.

Это даёт операторам и инструментам более ясную картину того, какие соединения нода реально держит открытыми прямо сейчас, что особенно полезно при отладке multi-session peers, live connection churn и расхождений между connection-manager slots и реальными сокетами.

## Desktop peer view больше не зависает во время ProbeNode

В desktop peer-health path был сужен lock scope во время `ProbeNode`, поэтому UI больше не блокируется из-за более тяжёлых чтений peer state.

На практике peers view стал отзывчивее под нагрузкой, а вероятность заметных подвисаний во время очередного polling node state уменьшилась.

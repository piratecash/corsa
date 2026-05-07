# What's New in v0.43-alpha

## Reconnect Storms From Oversized Routing Frames Are Broken Up

This release fixes an important reconnect-storm failure mode that could be triggered by oversized announce traffic and retry feedback loops. Announce frames are now chunked by byte size instead of only by entry count, the announce plane gets clearer size boundaries, oversize routing frames are rejected earlier, and route-flap hold-down now uses a more resilient exponential backoff model.

At the same time, push-message dedup paths now acknowledge deletes correctly, and `get_peers` responses are capped more conservatively. Together, these changes make dense-network behavior much calmer and prevent a single oversize routing event from turning into repeated disconnect/retry churn.

## The Daemon Is Quieter by Default

The default daemon log level now starts at `warn` instead of `info`.

This makes a fresh node much less noisy in steady state and helps operators notice the warnings and errors that actually matter. If you want the previous verbosity, `CORSA_LOG_LEVEL=info` restores it.

## Release Builds Now Use `trimpath`

Release builds now enable `trimpath`.

This helps make produced binaries cleaner and more reproducible by removing unnecessary local path information from build outputs.

---

# Что нового в v0.43-alpha

## Reconnect storms из-за oversized routing frames теперь разрушаются раньше

В этом релизе исправлен важный failure mode, при котором reconnect storm мог запускаться oversized announce traffic и петлями retry-обратной связи. Announce frames теперь чанкуются по размеру в байтах, а не только по количеству записей, announce plane получил более чёткие size boundaries, oversized routing frames отклоняются раньше, а route-flap hold-down теперь использует более устойчивую модель exponential backoff.

Одновременно с этим push-message dedup paths теперь корректно подтверждают delete-операции, а ответы `get_peers` ограничиваются более консервативно. Вместе это делает поведение плотной сети заметно спокойнее и не даёт одному oversized routing-событию превратиться в повторяющийся disconnect/retry churn.

## Daemon по умолчанию стал тише

Уровень логирования daemon по умолчанию теперь стартует с `warn`, а не с `info`.

Это делает свежезапущенную ноду заметно менее шумной в steady state и помогает оператору лучше видеть реально важные warnings и errors. Если нужна прежняя подробность, её возвращает `CORSA_LOG_LEVEL=info`.

## Release builds теперь используют `trimpath`

В release builds теперь включён `trimpath`.

Это помогает делать итоговые бинарники чище и воспроизводимее, убирая из build outputs лишнюю информацию о локальных путях.

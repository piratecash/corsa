# Debugging

## English

### Log Levels

CORSA uses [zerolog](https://github.com/rs/zerolog) for structured logging. The log level is controlled by the `CORSA_LOG_LEVEL` environment variable.

| Level   | Value   | Description                                                    |
|---------|---------|----------------------------------------------------------------|
| `error` | highest | Only errors                                                    |
| `warn`  |         | Warnings and above                                             |
| `info`  | default | Key lifecycle events (start, stop, peer connect/disconnect)    |
| `debug` |         | Network protocol traces (`json/tcp`), routing decisions        |
| `trace` | lowest  | Everything above + local RPC traces (`json/local`) from the UI |

### Usage

```bash
# Default — info level, no protocol traces
CORSA_LOG_LEVEL=info ./corsa

# Network debugging — shows TCP protocol traces between peers
CORSA_LOG_LEVEL=debug ./corsa

# Full tracing — also shows local UI→Node RPC calls (fetch_messages, fetch_network_stats, etc.)
CORSA_LOG_LEVEL=trace ./corsa
```

### Protocol Traces

All protocol interactions are logged with `protocol_trace` message and the following fields:

| Field       | Description                                             |
|-------------|---------------------------------------------------------|
| `protocol`  | `json/tcp` (network) or `json/local` (UI RPC)          |
| `addr`      | Remote peer address or `local`                          |
| `direction` | `recv` (inbound) or `send` (outbound)                  |
| `command`   | Frame type (`hello`, `ping`, `fetch_messages`, etc.)    |
| `accepted`  | `true` if processed successfully, `false` on error      |

**Level separation:**

- `json/tcp` traces are logged at **debug** level — useful for diagnosing network issues between peers.
- `json/local` traces are logged at **trace** level — these are frequent UI polling requests (e.g. `fetch_network_stats` every second) that create noise at debug level.

### Log Output

Logs are written to `corsa.log` in the application data directory. Stderr is also captured to `stderr.log` for crash diagnostics.

---

## Русский

### Уровни логирования

CORSA использует [zerolog](https://github.com/rs/zerolog) для структурированного логирования. Уровень управляется переменной окружения `CORSA_LOG_LEVEL`.

| Уровень | Значение     | Описание                                                                   |
|---------|--------------|----------------------------------------------------------------------------|
| `error` | наивысший    | Только ошибки                                                              |
| `warn`  |              | Предупреждения и выше                                                      |
| `info`  | по умолчанию | Ключевые события жизненного цикла (старт, стоп, подключение/отключение)    |
| `debug` |              | Трассировка сетевого протокола (`json/tcp`), решения маршрутизации          |
| `trace` | наименьший   | Всё выше + локальные RPC-трассировки (`json/local`) от UI                  |

### Использование

```bash
# По умолчанию — уровень info, без трассировки протокола
CORSA_LOG_LEVEL=info ./corsa

# Отладка сети — показывает TCP-трассировки между пирами
CORSA_LOG_LEVEL=debug ./corsa

# Полная трассировка — также показывает локальные UI→Node RPC-вызовы (fetch_messages, fetch_network_stats и т.д.)
CORSA_LOG_LEVEL=trace ./corsa
```

### Трассировка протокола

Все взаимодействия протокола логируются с сообщением `protocol_trace` и следующими полями:

| Поле        | Описание                                                |
|-------------|---------------------------------------------------------|
| `protocol`  | `json/tcp` (сеть) или `json/local` (UI RPC)            |
| `addr`      | Адрес удалённого пира или `local`                       |
| `direction` | `recv` (входящий) или `send` (исходящий)                |
| `command`   | Тип фрейма (`hello`, `ping`, `fetch_messages` и т.д.)  |
| `accepted`  | `true` если обработан успешно, `false` при ошибке       |

**Разделение по уровням:**

- Трассировки `json/tcp` логируются на уровне **debug** — полезны для диагностики сетевых проблем между пирами.
- Трассировки `json/local` логируются на уровне **trace** — это частые polling-запросы от UI (например, `fetch_network_stats` каждую секунду), которые создают шум на уровне debug.

### Вывод логов

Логи записываются в `corsa.log` в директории данных приложения. Stderr также перенаправляется в `stderr.log` для диагностики падений.

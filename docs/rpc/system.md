# System Commands

## English

### POST /rpc/v1/system/help

List all available commands.

Response:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "List all available RPC commands", "category": "system"},
    {"name": "getPeers", "description": "Get list of connected peers", "category": "network"}
  ]
}
```

`version` is the help response schema version (currently `"1.0"`). It tracks the structure of the help response itself — not the protocol or client version. Clients can use it to detect format changes (e.g. new fields in command metadata). Bump when the help response structure changes.

### POST /rpc/v1/system/ping

Send local ping, returns pong response. The same semantic result regardless of transport (HTTP RPC or in-process desktop).

### POST /rpc/v1/system/hello

Send hello frame for identification. The handler populates `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion`, and `ClientBuild` before forwarding to the node — without these fields the handshake is rejected. The standalone node handler identifies as `Client: "rpc"`; in desktop mode, `RegisterDesktopOverrides` registers a transport-level override that identifies as `Client: "desktop"` with the desktop application version. This is not a semantic fork — it changes only transport identity metadata. `ClientBuild` is a monotonically increasing integer that peers compare to detect newer software releases.

### POST /rpc/v1/system/version

Client and protocol version.

Response:
```json
{
  "client_version": "0.21-alpha",
  "protocol_version": 3,
  "node_address": "a1b2c3d4..."
}
```

---

## Русский

### POST /rpc/v1/system/help

Список всех доступных команд.

Ответ:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "Список всех доступных RPC команд", "category": "system"},
    {"name": "getPeers", "description": "Получить список подключённых пиров", "category": "network"}
  ]
}
```

`version` — версия схемы ответа help (сейчас `"1.0"`). Отслеживает структуру самого ответа help, а не версию протокола или клиента. Клиенты могут использовать это поле для обнаружения изменений формата (например, новых полей в метаданных команд). Инкрементируется при изменении структуры ответа help.

### POST /rpc/v1/system/ping

Локальный пинг, возвращает pong-ответ. Одинаковый семантический результат вне зависимости от транспорта (HTTP RPC или in-process desktop).

### POST /rpc/v1/system/hello

Отправка hello-фрейма для идентификации. Обработчик заполняет `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion` и `ClientBuild` перед отправкой ноде — без этих полей handshake отклоняется. Standalone-нода идентифицируется как `Client: "rpc"`; в desktop-режиме `RegisterDesktopOverrides` регистрирует transport-level override, который идентифицируется как `Client: "desktop"` с версией desktop-приложения. Это не семантический форк — меняются только transport identity метаданные. `ClientBuild` — монотонно возрастающий целочисленный номер сборки, по которому пиры определяют наличие новых версий ПО.

### POST /rpc/v1/system/version

Версия клиента и протокола.

Ответ:
```json
{
  "client_version": "0.21-alpha",
  "protocol_version": 3,
  "node_address": "a1b2c3d4..."
}
```

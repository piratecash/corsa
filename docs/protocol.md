# CORSA Protocol

This document has been restructured into the [protocol/](protocol/) folder for easier navigation.

## English

See [protocol/README.md](protocol/README.md) for the full protocol specification, command index, and navigation.

Individual command groups:

- [Handshake](protocol/handshake.md) — `hello`, `welcome`, `auth_session`, `auth_ok`, `ping`, `pong`
- [Messaging](protocol/messaging.md) — `send_message`, `import_message`, `fetch_messages`, `fetch_message`, `fetch_message_ids`, `fetch_inbox`, `fetch_pending_messages`
- [Realtime delivery](protocol/realtime.md) — `push_message`, `push_delivery_receipt`, `ack_delete`
- [Delivery receipts](protocol/delivery.md) — `send_delivery_receipt`, `fetch_delivery_receipts`
- [Contacts](protocol/contacts.md) — `fetch_contacts`, `fetch_trusted_contacts`, `import_contacts`, `fetch_identities`, `fetch_dm_headers`
- [Peers](protocol/peers.md) — `get_peers`, `announce_peer`, `add_peer`, `fetch_peer_health`, `fetch_network_stats`
- [Datagrams](protocol/datagram.md) — `datagram`: the routed / request / response transport plane, gated by the `mesh_datagram_v1` and `mesh_datagram_transit_v1` capabilities and by the `CORSA_ENABLE_DATAGRAM_V1` feature flag (default ON, kill switch `=0`). A node with the flag off advertises neither capability, declares no `dtypes` in the handshake and constructs no conveyor, so a peer never sends it a `datagram` in the first place. With the flag on both capabilities follow it (transit only on a full node): `mesh_datagram_v1` states that the envelope is understood, nothing more, while the `dtypes` field states which types the node can handle — an empty registry declares an explicitly empty set.
- [Gazeta](protocol/gazeta.md) — `publish_notice`, `fetch_notices`
- [Errors](protocol/errors.md) — all error codes

---

## Русский

Документ перенесён в папку [protocol/](protocol/) для удобной навигации.

См. [protocol/README.md](protocol/README.md) — полная спецификация протокола, индекс команд и навигация.

Группы команд:

- [Handshake](protocol/handshake.md) — `hello`, `welcome`, `auth_session`, `auth_ok`, `ping`, `pong`
- [Сообщения](protocol/messaging.md) — `send_message`, `import_message`, `fetch_messages`, `fetch_message`, `fetch_message_ids`, `fetch_inbox`, `fetch_pending_messages`
- [Realtime-доставка](protocol/realtime.md) — `push_message`, `push_delivery_receipt`, `ack_delete`
- [Delivery receipts](protocol/delivery.md) — `send_delivery_receipt`, `fetch_delivery_receipts`
- [Контакты](protocol/contacts.md) — `fetch_contacts`, `fetch_trusted_contacts`, `import_contacts`, `fetch_identities`, `fetch_dm_headers`
- [Пиры](protocol/peers.md) — `get_peers`, `announce_peer`, `add_peer`, `fetch_peer_health`, `fetch_network_stats`
- [Датаграммы](protocol/datagram.md) — `datagram`: транспортная плоскость режимов routed / request / response за возможностями `mesh_datagram_v1` и `mesh_datagram_transit_v1` и за фиче-флагом `CORSA_ENABLE_DATAGRAM_V1` (по умолчанию ВКЛЮЧЁН, выключатель `=0`). Узел с выключенным флагом не рекламирует ни одной из двух возможностей, не объявляет `dtypes` в рукопожатии и не конструирует конвейер, поэтому `datagram` ему просто никто не пришлёт. При включённом флаге за ним следуют обе возможности (транзитная — только на полном узле): `mesh_datagram_v1` утверждает понимание конверта и ничего сверх, а какие типы узел умеет обработать, говорит поле `dtypes` — пустой реестр объявляет явное пустое множество.
- [Gazeta](protocol/gazeta.md) — `publish_notice`, `fetch_notices`
- [Ошибки](protocol/errors.md) — все коды ошибок

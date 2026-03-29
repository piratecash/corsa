# CORSA Protocol

This document has been restructured into the [protocol/](protocol/) folder for easier navigation.

## English

See [protocol/README.md](protocol/README.md) for the full protocol specification, command index, and navigation.

Individual command groups:

- [Handshake](protocol/handshake.md) — `hello`, `welcome`, `auth_session`, `auth_ok`, `ping`, `pong`
- [Messaging](protocol/messaging.md) — `send_message`, `import_message`, `fetch_messages`, `fetch_message`, `fetch_message_ids`, `fetch_inbox`, `fetch_pending_messages`
- [Realtime delivery](protocol/realtime.md) — `subscribe_inbox`, `subscribed`, `push_message`, `push_delivery_receipt`, `ack_delete`, `request_inbox`
- [Delivery receipts](protocol/delivery.md) — `send_delivery_receipt`, `fetch_delivery_receipts`
- [Contacts](protocol/contacts.md) — `fetch_contacts`, `fetch_trusted_contacts`, `import_contacts`, `fetch_identities`, `fetch_dm_headers`
- [Peers](protocol/peers.md) — `get_peers`, `announce_peer`, `add_peer`, `fetch_peer_health`, `fetch_network_stats`
- [Gazeta](protocol/gazeta.md) — `publish_notice`, `fetch_notices`
- [Errors](protocol/errors.md) — all error codes

---

## Русский

Документ перенесён в папку [protocol/](protocol/) для удобной навигации.

См. [protocol/README.md](protocol/README.md) — полная спецификация протокола, индекс команд и навигация.

Группы команд:

- [Handshake](protocol/handshake.md) — `hello`, `welcome`, `auth_session`, `auth_ok`, `ping`, `pong`
- [Сообщения](protocol/messaging.md) — `send_message`, `import_message`, `fetch_messages`, `fetch_message`, `fetch_message_ids`, `fetch_inbox`, `fetch_pending_messages`
- [Realtime-доставка](protocol/realtime.md) — `subscribe_inbox`, `subscribed`, `push_message`, `push_delivery_receipt`, `ack_delete`, `request_inbox`
- [Delivery receipts](protocol/delivery.md) — `send_delivery_receipt`, `fetch_delivery_receipts`
- [Контакты](protocol/contacts.md) — `fetch_contacts`, `fetch_trusted_contacts`, `import_contacts`, `fetch_identities`, `fetch_dm_headers`
- [Пиры](protocol/peers.md) — `get_peers`, `announce_peer`, `add_peer`, `fetch_peer_health`, `fetch_network_stats`
- [Gazeta](protocol/gazeta.md) — `publish_notice`, `fetch_notices`
- [Ошибки](protocol/errors.md) — все коды ошибок

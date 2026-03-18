# Mobile plan

This folder is reserved for mobile-specific bindings and integration helpers.

Suggested next steps:

- expose a stable Go client API for Android and iOS light clients
- keep mobile transport optional so the app can run as a light client
- move platform bridges here instead of leaking them into `internal/core`

# Loadstand: exact nft EtherType verification

Status: implementation and focused ordinary/race checks completed on the current-source candidate, 2026-10-24; Darwin/Linux scoped CGO0 lint passed; independent acceptance is required before staging. This preserves the existing private Linux stand policy. It does not change node defaults, authorize workloads or establish T5, G2 or release readiness.

## Observed failure

An earlier private Member4 attempt failed at nft readback verification. The later private Source1811 `abde7582` reached route readiness and FullNode sampling, then failed the unchanged cadence check; it is not a current-index kernel qualification. Loading the existing matching `nft_meta_bridge` module before the unchanged module freeze allowed the batch to create its table and three rules. The returned JSON omits the leading `ether/type == ip` and `ether/type == arp` matches. In that earlier attempt, setup and cleanup both refused the mismatch; successful postflight and nft-table deletion inside that guest were not proved. See the [diagnostic record](../operations/d1-linux-member4-diagnostic.md).

The nft producer must retain both explicit Ethernet-header guards. Removing those guards, ignoring unknown JSON fields or accepting a successful batch exit would change the required verification.

## Why JSON and debug text are insufficient

The official [nftables 1.1.6 source archive](https://www.netfilter.org/projects/nftables/files/nftables-1.1.6.tar.xz), SHA-256 `372931bda8556b310636a2f9020adc710f9bab66f47efe0ce90bff800ac2530c`, shows protocol-dependency elimination during delinearization. Typed IP/ARP payload output alone does not prove the original Ethernet-header guard: `meta protocol` and Ethernet payload type can differ on VLAN/offload paths. Therefore matching the observed JSON after deleting two expected expressions is insufficient.

The official [libnftnl 1.3.1 source archive](https://www.netfilter.org/projects/libnftnl/files/libnftnl-1.3.1.tar.xz), SHA-256 `607da28dba66fbdeccf8ef1395dded9077e8d19f2995f9a4d45a9c2f0bcffba8`, shows that debug comparison data are printed as padded 32-bit words without their exact byte length. A one-byte comparison `08` and a two-byte comparison `08 00` can have the same printed word. `--debug=netlink` alone therefore does not prove the required two-byte comparison either.

These archives were read for protocol and printer analysis; no downloaded source was built or executed.

## Selected implementation approach

Add a private, bounded, read-only `NETLINK_NETFILTER` witness using the existing stand's socket-exchange conventions. Do not add a dependency, service, worker or general netfilter framework. Existing route operations remain unchanged.

Within the existing caller deadline and owning namespace/thread, perform:

1. Read the nft generation with `GETGEN`.
2. Read and validate the existing table identity, JSON shape and counters. Defer equality with the supported expected JSON form until the raw witness succeeds.
3. Read rules for that exact bridge table and `forward` chain with `GETRULE`.
4. Read `GETGEN` again. Refuse a changed, missing or invalid generation; do not retry.

The binary dump must complete and contain the same unique rule handles in the same chain order as the JSON read. For each of the two accept rules, require the leading payload load from the link-layer header at offset 12, length 2, into a valid destination register, immediately followed by equality comparison of that same register against exactly two bytes: `08 00` for IP and `08 06` for ARP. A meta-protocol substitute, shorter comparison or overwritten register is not equivalent evidence. Keep full strict JSON checking for the remaining policy, table identity, counters and final drop.

Only a successful raw witness permits comparison with the narrowly defined JSON readback form that omits the two source-owned printer dependencies. Explicit producer-shaped JSON also requires the raw witness. Neither representation is accepted by itself.

Parsing must preserve attribute lengths, nesting, order and register identity. Reject malformed, duplicate, unexpected, missing, oversized or incomplete evidence, including interrupted dumps, truncation, overrun, errors and invalid completion status. Bound bytes and rule count; preserve the caller deadline and close the owned socket on every outcome. A close failure contributes to failure. Requests are restricted to reads; evidence collection grants no mutation or cleanup authority.

Use the exact [Linux v6.18.58 response ABI](https://github.com/gregkh/linux/blob/v6.18.58/net/netfilter/nf_tables_api.c). In particular, the sender socket's kernel identity and the netlink header's destination port are different checks; an ACK is not the typed generation response or complete dump. Validate generation attributes, response family, flags, sequence, resource ID and completion against that ABI rather than assuming zero fields.

## Failure model

The owner serializes its own mutations. Generation bracketing detects intervening changes conservatively and assumes no full generation cycle between the reads. This kernel uses a nonzero 32-bit generation counter and skips zero: the cycle is 2³²−1 advances, not 2³². It is not an unforgeable capability and does not promise protection against an arbitrary privileged actor cycling generations or changing rules after verification. The namespace/socket identity and bounded operation are part of the witness. Ambiguous evidence refuses; it cannot authorize deletion of a replacement table.

## Required verification

Focused ordinary and race checks must cover the actual captured JSON with explicitly constructed binary fixtures, both supported JSON forms, IP/ARP guards and the empty plan. Constructed packets are parser evidence, not observed kernel wire data.

Negative checks must include the same printable JSON with a meta-protocol replacement; padded debug-word collisions with a wrong comparison length; wrong bytes, operation, header base, offset, load length, register or ordering; missing, extra, duplicate, foreign or reordered rules and attributes; malformed framing, sender, sequence and completion; interrupted/truncated dumps; generation failures and changes; cancellation, bounds and close errors. Record request order and enforce that no mutation request is emitted.

CODE review accepted the finite ten-file current-source network port. Its focused suite passed 276 ordinary and 276 race RUN/PASS events, 51 parents per mode, without failures or skips. This includes first-alias ownership, exact owned multicast-route handling and the raw NFT witness, together with existing regressions. No namespace, node, application or guest VM was executed by this suite. Scoped Darwin/Linux CGO0 lint passed with zero issues; independent final acceptance is required before staging.

Next, rebuild the current-source test and application artifacts and repeat the original Member4 case under the existing approved profile, retaining the matching `nft_meta_bridge` preload before module freeze. Only that run can qualify the real kernel path; earlier failed attempts remain failures. Same-source final artifacts, calibration, resource acceptance and release conditions remain separate.

## Русский

Исправление сохраняет явные Ethernet guards и прежнюю политику стенда. JSON скрывает protocol dependencies, а debug libnftnl скрывает длину cmp; ни один из этих форматов отдельно не доказывает исходную защиту. Выбран ограниченный read-only GETRULE witness с GETGEN до и после чтений: те же rule handles и порядок, точные LL offset12/load2/register/cmp EQ с двумя байтами0800/0806. Остальная JSON-проверка остаётся строгой. Неполнота, неизвестные данные или изменение generation дают отказ без повторов.

CODE review конечного переноса десяти файлов принят; focused suite дал276 ordinary+276 race PASS, по51 родителю, без FAIL/SKIP. Проверены first-alias, точные owned multicast routes и raw NFT вместе с регрессиями. Scoped Darwin/Linux CGO0 lint прошёл без замечаний; перед staging требуется независимая итоговая приёмка. Затем нужны сборка актуальных артефактов и исходный Member4 на гостевом ядре с прежним preload `nft_meta_bridge`. Синтетические binary fixtures не считаются фактическим wire capture. Нынешний Member4 остаётся FAIL; документ не разрешает нагрузку, активацию, G2 или выпуск.

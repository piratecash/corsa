#!/usr/bin/env bash
# Exact reviewed lexical boundary, documented in docs/design/netcore-boundary-baseline.md.
# Known legacy debt is a static multiset, not a numerical allowance. Go build,
# tests and lint remain independent gates; this script is not a Go AST verifier.

set -euo pipefail

# Resolve repository root from the script location so `make` / CI runs from
# any working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if ! command -v rg >/dev/null 2>&1; then
    echo "FAIL: rg (ripgrep) is not installed; cannot run §2.9 gate" >&2
    exit 2
fi

# Accumulators — a single non-zero exit at the end covers all failures so the
# operator sees every category in one pass, not one at a time.
FAILED=0
REPORT=()

# run_rg executes rg and echoes raw output. rg exits 1 on no matches (not
# an error for our use). We swallow exit 1 and surface rg-internal errors
# (exit ≥2) as a hard failure.
run_rg() {
    local output status
    output="$("$@" 2>/dev/null)" || status=$?
    status=${status:-0}
    if [ "$status" -gt 1 ]; then
        echo "FAIL: rg internal error (exit $status) in: $*" >&2
        FAILED=1
        return 1
    fi
    printf '%s' "$output"
}

# strip_comment_lines drops lines that are pure Go line-comments. rg returns
# `path:lineno:content`; we examine the content portion (after the second
# colon) and skip lines whose first non-whitespace chars are `//`. Leaves
# block comments (`/* ... */`) alone — the grep gates do not currently need
# that granularity, and block comments are rare in the relevant files.
strip_comment_lines() {
    awk -F ':' '{
        # Rejoin content beyond the second colon in case content itself
        # contains a colon.
        content = $3
        for (i = 4; i <= NF; i++) content = content ":" $i
        # Trim leading whitespace.
        sub(/^[[:space:]]+/, "", content)
        if (index(content, "//") == 1) next
        print
    }'
}

count_lines() {
    # Reads stdin; prints the count of non-empty lines.
    awk 'NF{c++} END{print c+0}'
}

# expect_count runs an rg command, strips Go line-comment-only lines, and
# asserts the result count equals expected. Arguments:
#   <label> <expected> -- <rg ...>
# The comment filter is unconditional because every §2.9 gate targets code,
# not documentation; a pattern like `s.conns` or `map[net.Conn]*connEntry`
# legitimately appears in block-comment'ы in conn_registry.go / service.go
# and must not trip the gate.
expect_count() {
    local label="$1"; shift
    local expected="$1"; shift
    [ "$1" = "--" ] || { echo "internal: expect_count missing --" >&2; exit 2; }
    shift
    local raw got
    raw=$(run_rg "$@")
    got=$(printf '%s\n' "$raw" | strip_comment_lines | count_lines)
    if [ "$got" != "$expected" ]; then
        REPORT+=("FAIL: $label: expected $expected, got $got")
        printf '%s\n' "$raw" | strip_comment_lines | sed 's/^/    /' >&2
        FAILED=1
    fi
}

BASELINE="$SCRIPT_DIR/netcore-boundary-baseline.tsv"
if [ ! -f "$BASELINE" ]; then
    echo "FAIL: missing static netcore boundary baseline" >&2
    exit 2
fi

normalize_source() {
    awk '{$1=$1; print}'
}

# Go declarations and their closing braces are column-zero in gofmt sources.
# Matching source is tied to its enclosing declaration, never a line number.
function_at() {
    awk -v target="$2" '
        /^func[[:space:]]/ { signature=$0; sub(/[[:space:]]*\{.*/, "", signature) }
        NR==target { if (signature=="") print "<package>"; else print signature; exit }
        /^}/ { signature="" }
    ' "$1" | normalize_source
}

canonical_records() {
    local mode="$1" path number source signature
    while IFS=: read -r path number source; do
        [ -n "$path" ] || continue
        source=$(printf '%s\n' "$source" | normalize_source)
        if [ "$mode" = "import" ]; then
            printf '%s | %s\n' "$path" "$source"
        else
            signature=$(function_at "$path" "$number")
            if [ "$mode" = "signature" ]; then
                printf '%s | %s\n' "$path" "$signature"
            else
                printf '%s | %s | %s\n' "$path" "$signature" "$source"
            fi
        fi
    done | LC_ALL=C sort
}

expect_records() {
    local label="$1" categories="$2" mode="$3" expected actual raw
    shift 3
    expected=$(awk -F '\t' -v wanted="$categories" '
        (" " wanted " ") ~ (" " $1 " ") {sub(/^[^\t]*\t/, ""); print}
    ' "$BASELINE" | LC_ALL=C sort)
    raw=$(run_rg "$@")
    actual=$(printf '%s\n' "$raw" | strip_comment_lines | canonical_records "$mode")
    if [ "$actual" != "$expected" ]; then
        REPORT+=("FAIL: $label: exact reviewed multiset differs")
        echo "  expected records:" >&2
        printf '%s\n' "$expected" | sed 's/^/    /' >&2
        echo "  actual records:" >&2
        printf '%s\n' "$actual" | sed 's/^/    /' >&2
        FAILED=1
    fi
}

# Gate 1: direct socket write outside transport owner.
# Expected baseline after PR 9.6 / 10.x:
#   service.go → 0, peer_management.go → 0,
#   routing_relay.go → 3 (the notice dial's raw hello, auth_session and
#                         push_notice writes — noticeDialWelcome and
#                         sendNoticeToPeer; see the "internal/core/node/"
#                         file map in docs/routing.md for the per-file scope
#                         table),
#   socks5.go → 2 (SOCKS5 greeting + CONNECT).
expect_count "direct socket write in service.go" 0 -- \
    rg -nH --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/service.go
expect_count "direct socket write in peer_management.go" 0 -- \
    rg -nH --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/peer_management.go
expect_count "direct socket write in routing_relay.go" 3 -- \
    rg -nH --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/routing_relay.go
expect_count "direct socket write in socks5.go" 2 -- \
    rg -nH --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/socks5.go

# Gate 2: raw session.conn write through outbound legacy path.
# §2.9 rule 2 targets writes that bypass the transport owner. The regex is
# tightened from the §2.9 literal `session\.conn` (which overmatches
# SetReadDeadline / SetDeadline / `session.connID`) to a write-only form.
# The looser pattern is retained as a review-time audit — automation asserts
# the stricter invariant.
expect_count "raw session.conn write in peer_management.go" 0 -- \
    rg -nH --glob '!**/*_test.go' 'session\.conn\.(Write|WriteTo)\(' \
       internal/core/node/peer_management.go

# Gate 3: parallel map[net.Conn]*NetCore registry.
expect_count "parallel map[net.Conn]*NetCore" 0 -- \
    rg -nH --glob '!**/*_test.go' 'map\[net\.Conn\]\*NetCore' \
       internal/core/node

# Gate 4: primary registry keyed by net.Conn (regressed from PR 9.7).
expect_count "primary map[net.Conn]*connEntry" 0 -- \
    rg -nH --glob '!**/*_test.go' 'map\[net\.Conn\]\*connEntry' \
       internal/core/node

# Gate 5: direct access to s.conns / s.connIDByNetConn outside conn_registry.go.
expect_records "legacy registry reads" registry site \
    rg -nH --glob '!**/*_test.go' --glob '!**/conn_registry.go' \
       's\.conns\b|s\.connIDByNetConn\b' \
       internal/core/node

# Gate 6: un-ack'd write-wrapper call-sites. Primary check is errcheck in
# `make lint`; this is a fast backup. ByID variants are the canonical names
# after PR 10.6.
expect_count "un-ack'd write-wrappers (async + sync: writeJSONFrame*ByID / enqueueFrame*ByID / sendFrameViaNetwork + sendFrameViaNetworkSync / sendFrameBytesViaNetwork + sendFrameBytesViaNetworkSync / sendSessionFrameViaNetwork)" 0 -- \
    rg -nHU --glob '!**/*_test.go' \
       '^\s*(svc|s)\.(writeJSONFrameByID|writeJSONFrameSyncByID|enqueueFrameByID|enqueueFrameSyncByID|sendFrameViaNetwork|sendFrameViaNetworkSync|sendFrameBytesViaNetwork|sendFrameBytesViaNetworkSync|sendSessionFrameViaNetwork)\(' \
       internal/core/node

# Gate 7: untyped uint64 identity regression.
expect_records "legacy uint64 identity helper" uint64 site \
    rg -nH --glob '!**/*_test.go' --glob '!**/conn_id.go' \
       'connID\s+uint64|ConnID\s+uint64' \
       internal/core/node internal/core/domain

# Gate 8: deleted net.Conn-first public lookup wrappers.
expect_count "legacy netCoreFor / meteredFor / isInboundTracked" 0 -- \
    rg -nH --glob '!**/*_test.go' \
       '\bs\.(netCoreFor|meteredFor|isInboundTracked)\(' \
       internal/core/node

# Gate 9: deleted setTrackedLocked mutation.
expect_count "legacy setTrackedLocked" 0 -- \
    rg -nH --glob '!**/*_test.go' '\bs\.setTrackedLocked\(' \
       internal/core/node

# Gate 10: legacy forEach callback signature taking net.Conn.
expect_count "legacy forEach…Locked(func(net.Conn,…))" 0 -- \
    rg -nH --glob '!**/*_test.go' \
       'forEach(Inbound|TrackedInbound)ConnLocked\(func\([^)]*net\.Conn' \
       internal/core/node

# Gate 11: exact frozen declaration membership; same-count substitutions fail.
expect_records "net.Conn-accepting functions" conn signature \
    rg -nH --glob '!**/*_test.go' \
       'func (\([^)]*\) )?[A-Za-z_][A-Za-z0-9_]*\([^)]*net\.Conn' \
       internal/core/node

# Gate 13: four specific legacy metrics reads, never a whole-file exclusion.
expect_records "legacy entry.core metrics reads" entry site \
    rg -nH --glob '!**/*_test.go' --glob '!**/conn_registry.go' \
       'entry\.core\.' \
       internal/core/node

# Gate 14: scope (v) — walker callback signature taking *netcore.NetCore.
# The three forEach…ConnLocked walkers hand callers a connInfo snapshot,
# never the live *netcore.NetCore. A regression to the old shape would let
# call sites mutate identity/address/auth concurrently with handshake-time
# writes — exactly the race that PR 10.16 closed.
expect_count "legacy forEach…ConnLocked(func(…*netcore.NetCore…))" 0 -- \
    rg -nH --glob '!**/*_test.go' \
       'forEach(Inbound|TrackedInbound|)?ConnLocked\(func\([^)]*\*netcore\.NetCore' \
       internal/core/node

# Gate 15: scope (v) — legacy package-level inboundConnKey helper.
# After PR 10.16 the only routing-key constructor is the ConnID-first method
# inboundConnKeyForID. The bare-name helper accepted *netcore.NetCore and
# was the last consumer of *NetCore inside routing_integration.go (now split
# into routing_announce.go / routing_relay.go / routing_resolver.go / …).
expect_count "legacy inboundConnKey(*netcore.NetCore) helper" 0 -- \
    rg -nH --glob '!**/*_test.go' \
       'inboundConnKey\(' \
       internal/core/node

# Gate 12: eighteen legacy importers plus one sentinel-only readiness import.
# Matching the complete import source also refuses aliases/dot imports.
expect_records "net import whitelist" "legacy_import readiness_import" import \
    rg -nH --glob '!**/*_test.go' '"net"' internal/core/node
expect_records "readiness net.ErrClosed sentinel in stop only" readiness site \
    rg -nH --glob '!**/*_test.go' '\bnet[[:space:]]*\.' \
       internal/core/node/peer_session_readiness.go

# Final report and exit.
if [ "$FAILED" -ne 0 ]; then
    echo "" >&2
    printf '%s\n' "${REPORT[@]}" >&2
    echo "" >&2
    echo "enforce-netcore-boundary: FAILED (see categories above)" >&2
    exit 1
fi

echo "enforce-netcore-boundary: PASS (all §2.9 gates green)"

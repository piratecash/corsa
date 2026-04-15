#!/usr/bin/env bash
# enforce-netcore-boundary.sh — automated gate for §2.9 forbidden patterns.
#
# This script is the canonical runner for the grep-based part of the Phase 2
# compile-time enforcement (§6.4(b)). It runs the exact rg commands listed in
# docs/netcore-migration.md §2.9 and asserts each against the expected
# baseline. Any drift — a new occurrence of a forbidden pattern, or the
# carve-out function set growing beyond the frozen 14 — exits non-zero.
#
# Output format on failure:
#   FAIL: <category>: expected <N>, got <M>
# followed by the offending lines for human diff.
#
# The script is intentionally reliant on rg (ripgrep). rg is already a
# toolchain dependency for developer workflows; CI uses the ubuntu-latest
# runner which ships it. Callers that lack rg see a single actionable error.
#
# The script is the pair to a separate architectural boundary lint that
# checks `net` imports in internal/core/node (see scripts/ or tools/
# subdirectory). The two gates are independent by design — one proves
# "no new code added to forbidden places", the other proves "no forbidden
# imports on the whole package".

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

# Gate 1: direct socket write outside transport owner.
# Expected baseline after PR 9.6 / 10.x:
#   service.go → 0, peer_management.go → 0,
#   routing_integration.go → 3 (hello/auth/ping sentinel edges),
#   socks5.go → 2 (SOCKS5 greeting + CONNECT).
expect_count "direct socket write in service.go" 0 -- \
    rg -n --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/service.go
expect_count "direct socket write in peer_management.go" 0 -- \
    rg -n --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/peer_management.go
expect_count "direct socket write in routing_integration.go" 3 -- \
    rg -n --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/routing_integration.go
expect_count "direct socket write in socks5.go" 2 -- \
    rg -n --glob '!**/*_test.go' 'conn\.Write\(|io\.WriteString\(' \
       internal/core/node/socks5.go

# Gate 2: raw session.conn write through outbound legacy path.
# §2.9 rule 2 targets writes that bypass the transport owner. The regex is
# tightened from the §2.9 literal `session\.conn` (which overmatches
# SetReadDeadline / SetDeadline / `session.connID`) to a write-only form.
# The looser pattern is retained as a review-time audit — automation asserts
# the stricter invariant.
expect_count "raw session.conn write in peer_management.go" 0 -- \
    rg -n --glob '!**/*_test.go' 'session\.conn\.(Write|WriteTo)\(' \
       internal/core/node/peer_management.go

# Gate 3: parallel map[net.Conn]*NetCore registry.
expect_count "parallel map[net.Conn]*NetCore" 0 -- \
    rg -n --glob '!**/*_test.go' 'map\[net\.Conn\]\*NetCore' \
       internal/core/node

# Gate 4: primary registry keyed by net.Conn (regressed from PR 9.7).
expect_count "primary map[net.Conn]*connEntry" 0 -- \
    rg -n --glob '!**/*_test.go' 'map\[net\.Conn\]\*connEntry' \
       internal/core/node

# Gate 5: direct access to s.conns / s.connIDByNetConn outside conn_registry.go.
expect_count "s.conns / s.connIDByNetConn outside conn_registry.go" 0 -- \
    rg -n --glob '!**/*_test.go' --glob '!**/conn_registry.go' \
       's\.conns\b|s\.connIDByNetConn\b' \
       internal/core/node

# Gate 6: un-ack'd write-wrapper call-sites. Primary check is errcheck in
# `make lint`; this is a fast backup. ByID variants are the canonical names
# after PR 10.6.
expect_count "un-ack'd write-wrappers (async + sync: writeJSONFrame*ByID / enqueueFrame*ByID / writeSessionFrame / sendFrameViaNetwork + sendFrameViaNetworkSync)" 0 -- \
    rg -nU --glob '!**/*_test.go' \
       '^\s*(svc|s)\.(writeJSONFrameByID|writeJSONFrameSyncByID|enqueueFrameByID|enqueueFrameSyncByID|writeSessionFrame|sendFrameViaNetwork|sendFrameViaNetworkSync)\(' \
       internal/core/node

# Gate 7: untyped uint64 identity regression.
expect_count "untyped uint64 ConnID identity in node/domain" 0 -- \
    rg -n --glob '!**/*_test.go' --glob '!**/conn_id.go' \
       'connID\s+uint64|ConnID\s+uint64' \
       internal/core/node internal/core/domain

# Gate 8: deleted net.Conn-first public lookup wrappers.
expect_count "legacy netCoreFor / meteredFor / isInboundTracked" 0 -- \
    rg -n --glob '!**/*_test.go' \
       '\bs\.(netCoreFor|meteredFor|isInboundTracked)\(' \
       internal/core/node

# Gate 9: deleted setTrackedLocked mutation.
expect_count "legacy setTrackedLocked" 0 -- \
    rg -n --glob '!**/*_test.go' '\bs\.setTrackedLocked\(' \
       internal/core/node

# Gate 10: legacy forEach callback signature taking net.Conn.
expect_count "legacy forEach…Locked(func(net.Conn,…))" 0 -- \
    rg -n --glob '!**/*_test.go' \
       'forEach(Inbound|TrackedInbound)ConnLocked\(func\([^)]*net\.Conn' \
       internal/core/node

# Gate 11: carve-out membership — net.Conn-accepting functions and methods.
# Expected baseline = 14 exactly (13 frozen §2.6.26 Service methods + 1
# socket-infra package-level enableTCPKeepAlive). Any growth is either
# regression or a new net.Conn entry point outside the frozen list.
# The regex is broadened over §2.9 literal to include package-level funcs:
# `func ( ... )? Name(... net.Conn ...)` — otherwise enableTCPKeepAlive is
# not counted and the baseline would drift to 13.
expect_count "carve-out membership (net.Conn-accepting methods / functions)" 14 -- \
    rg -n --glob '!**/*_test.go' \
       'func (\([^)]*\) )?[A-Za-z_][A-Za-z0-9_]*\([^)]*net\.Conn' \
       internal/core/node

# Gate 12: architectural boundary — `net` stdlib import in internal/core/node.
# The whitelist captures the frozen carve-out files plus network-level
# helpers that legitimately need net.IP / net.ParseCIDR / net.Listener:
#   conn_registry.go     — frozen lifecycle binding (§2.6.26)
#   service.go           — frozen entry/lifecycle surface (§2.6.26)
#   peer_management.go   — enableTCPKeepAlive (socket-infra)
#   routing_integration.go — §4.4 bootstrap handshake edges
#   socks5.go            — §5.3.2 sub-NetCore handshake
#   peer_provider.go     — net.IP / net.ParseCIDR for peer-address policy
#   netgroup.go          — net.IP grouping helpers for reachability
# Any new file in internal/core/node that imports `net` is a boundary
# violation and must either move the functionality into netcore or be
# added explicitly to this whitelist with review.
check_net_import_whitelist() {
    local actual expected diff
    actual=$(
        rg -l --glob '!**/*_test.go' '^\s*"net"\s*$|^import\s+"net"\s*$' \
           internal/core/node 2>/dev/null \
        | sort
    )
    expected=$(cat <<'EOF' | sort
internal/core/node/conn_registry.go
internal/core/node/netgroup.go
internal/core/node/peer_management.go
internal/core/node/peer_provider.go
internal/core/node/routing_integration.go
internal/core/node/service.go
internal/core/node/socks5.go
EOF
    )
    if [ "$actual" != "$expected" ]; then
        REPORT+=("FAIL: net-import whitelist: internal/core/node files importing net do not match the carve-out whitelist")
        echo "  expected whitelist:" >&2
        printf '    %s\n' $expected >&2
        echo "  actual importers:" >&2
        printf '    %s\n' $actual >&2
        FAILED=1
    fi
}
check_net_import_whitelist

# Final report and exit.
if [ "$FAILED" -ne 0 ]; then
    echo "" >&2
    printf '%s\n' "${REPORT[@]}" >&2
    echo "" >&2
    echo "enforce-netcore-boundary: FAILED (see categories above)" >&2
    exit 1
fi

echo "enforce-netcore-boundary: PASS (all §2.9 gates green)"

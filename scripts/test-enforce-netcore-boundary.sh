#!/usr/bin/env bash
# Minimal lexical fixtures: no checkout/archive/full repository copy.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/corsa-netcore-guard.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
BASE="$TEMP_ROOT/base"
CASE="$TEMP_ROOT/case"
mkdir -p "$BASE/scripts" "$BASE/internal/core/node" "$BASE/internal/core/domain"
cp "$SCRIPT_DIR/enforce-netcore-boundary.sh" "$SCRIPT_DIR/netcore-boundary-baseline.tsv" "$BASE/scripts/"
# Build tiny noncompiled Go-shaped sources from the declared static contracts.
awk -F '\t' -v root="$BASE/" '
 /^[#]/ {next}
 NF==2 {
  count=split($2,parts,/ \| /);path=parts[1];files[path]=1
  if($1=="legacy_import" || $1=="readiness_import"){imports[path]=parts[2];next}
  key=path SUBSEP parts[2];headers[key]=parts[2];owners[key]=path
  if(count==3 && parts[3]!=parts[2] " {") {
   bodies[key]=bodies[key] "\t" parts[3] "\n"
   if(parts[3] ~ /\{$/) bodies[key]=bodies[key] "\t}\n"
  }
 }
 END {
  for(path in files){print "package node\n" > (root path);if(path in imports)print "import (\n\t" imports[path] "\n)\n" >> (root path)}
  for(key in headers)print headers[key] " {\n" bodies[key] "}\n" >> (root owners[key])
 }
' "$SCRIPT_DIR/netcore-boundary-baseline.tsv"
printf 'package domain\n' > "$BASE/internal/core/domain/domain.go"
for spec in 'routing_relay.go 3' 'socks5.go 2'; do
    read -r name count <<< "$spec"
    {
        printf '\nfunc fixtureSocketWrites() {\n'
        for ((i=0; i<count; i++)); do printf '\tio.WriteString(conn, "")\n'; done
        printf '}\n'
    } >> "$BASE/internal/core/node/$name"
done

reset_case() { rm -rf "$CASE"; cp -R "$BASE" "$CASE"; }
run_case() { bash "$CASE/scripts/enforce-netcore-boundary.sh" > "$TEMP_ROOT/output" 2>&1; }
expect_failure() {
    local name="$1" category="$2"
    if run_case; then echo "FAIL: mutant escaped: $name" >&2; exit 1; fi
    if ! rg -q "$category" "$TEMP_ROOT/output"; then
        echo "FAIL: mutant $name failed for an unexpected reason" >&2
        cat "$TEMP_ROOT/output" >&2; exit 1
    fi
    printf 'PASS negative: %s\n' "$name"
}
append_go() { cat >> "$CASE/internal/core/node/$1"; }
replace_source() {
    local path="$CASE/internal/core/node/$1"
    sed "$2" "$path" > "$TEMP_ROOT/replaced"
    mv "$TEMP_ROOT/replaced" "$path"
}
reset_case
if ! run_case; then cat "$TEMP_ROOT/output" >&2; exit 1; fi
printf 'PASS positive: minimal static baseline\n'
# Line shifts and indentation do not change reviewed records.
{ printf '\n\n// harmless line shift\n'; cat "$CASE/internal/core/node/resource_breakdown.go"; } > "$TEMP_ROOT/shifted"
mv "$TEMP_ROOT/shifted" "$CASE/internal/core/node/resource_breakdown.go"
replace_source resource_breakdown.go 's/queued, capacity :=/    queued,   capacity :=/'
if ! run_case; then cat "$TEMP_ROOT/output" >&2; exit 1; fi
printf 'PASS positive: line numbers and whitespace ignored\n'

reset_case
append_go peer_management.go <<'GO'
func unreviewedSocket(conn net.Conn) {}
GO
expect_failure 'new Conn member' 'net.Conn-accepting functions'
reset_case
replace_source peer_management.go 's/enableTCPKeepAlive/replacementWithSameCount/'
expect_failure 'same-count Conn removal plus replacement' 'net.Conn-accepting functions'
reset_case
append_go peer_management.go <<'GO'
func enableTCPKeepAlive(conn net.Conn) {}
GO
expect_failure 'duplicate Conn member multiplicity' 'net.Conn-accepting functions'
reset_case
replace_source peer_management.go '/^func enableTCPKeepAlive/,/^}/d'
expect_failure 'missing legacy member' 'net.Conn-accepting functions'
# D1 degree is an exact new member, not a whole-file/count allowance.
reset_case
replace_source find_node_degree.go '/^func .* publishDegreeInbound(/,/^}/d'
expect_failure 'missing D1 degree Conn member' 'net.Conn-accepting functions'
reset_case
append_go find_node_degree.go <<'GO'
func (s *Service) publishDegreeInbound(conn net.Conn) {}
GO
expect_failure 'duplicate D1 degree Conn member multiplicity' 'net.Conn-accepting functions'
reset_case
replace_source find_node_degree.go 's/publishDegreeInbound/replacementDegreeInbound/'
expect_failure 'same-count D1 degree Conn replacement' 'net.Conn-accepting functions'
reset_case
replace_source find_node_degree.go '/^[[:space:]]*"net"$/d'
expect_failure 'missing D1 degree net import' 'net import whitelist'
reset_case
append_go find_node_degree.go <<'GO'
import "net"
GO
expect_failure 'duplicate D1 degree net import multiplicity' 'net import whitelist'
reset_case
replace_source find_node_degree.go 's/"net"/n "net"/'
expect_failure 'same-count D1 degree net import replacement' 'net import whitelist'
reset_case
append_go degree_unreviewed_import.go <<'GO'
package node
import "net"
GO
expect_failure 'new importer beyond reviewed D1 degree import' 'net import whitelist'

reset_case
append_go resource_breakdown.go <<'GO'
func newRegistryRead() {
 _ = len(s.conns)
}
GO
expect_failure 'extra registry read in legacy debt file' 'legacy registry reads'
reset_case
replace_source resource_breakdown.go 's/conns := len/changed := len/'
expect_failure 'same-count registry expression replacement' 'legacy registry reads'
reset_case
replace_source resource_breakdown.go 's/sessionsUsage()/movedMetricBody()/'
expect_failure 'same source moved to a different enclosing function' 'legacy registry reads'
reset_case
append_go resource_breakdown.go <<'GO'
func newCoreRead() {
 _ = entry.core.Auth()
}
GO
expect_failure 'extra entry.core read in debt file' 'legacy entry.core metrics reads'
reset_case
append_go peer_management.go <<'GO'
func newUntypedIdentity(connID uint64) {}
GO
expect_failure 'extra uint64 helper' 'legacy uint64 identity helper'
reset_case
append_go new_importer.go <<'GO'
package node
import "net"
GO
expect_failure 'new net importer' 'net import whitelist'
reset_case
append_go peer_session_readiness.go <<'GO'
func forbiddenDial() {
 _, _ = net.Dial("tcp", "127.0.0.1:1")
}
GO
expect_failure 'readiness net.Dial' 'readiness net.ErrClosed sentinel in stop only'
reset_case
append_go peer_session_readiness.go <<'GO'
func sentinelOutsideStop() {
 b.state, b.failure = peerSessionReadClosed, net.ErrClosed
}
GO
expect_failure 'readiness sentinel outside stop' 'readiness net.ErrClosed sentinel in stop only'
reset_case
replace_source peer_session_readiness.go 's/stop()/movedStop()/'
expect_failure 'same-count sentinel moved out of stop' 'readiness net.ErrClosed sentinel in stop only'
reset_case
replace_source peer_session_readiness.go 's/"net"/n "net"/; s/net.ErrClosed/n.ErrClosed/'
expect_failure 'readiness aliased net import' 'net import whitelist'
reset_case
append_go service.go <<'GO'
func forbiddenParallelRegistry() {
 var registry map[net.Conn]*NetCore
}
GO
expect_failure 'zero parallel registry gate retained' 'parallel map'
reset_case
append_go service.go <<'GO'
func forbiddenWrite() {
 io.WriteString(conn, "")
}
GO
expect_failure 'zero direct write gate retained' 'direct socket write in service.go'
printf 'enforce-netcore-boundary regression controls: PASS\n'

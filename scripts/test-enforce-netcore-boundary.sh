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
   # One physical source row may match multiple independent categories.
   # Emit their maximum multiplicity, never their sum or a global set.
   sourcekey=key SUBSEP parts[3];categorykey=$1 SUBSEP sourcekey
   occurrences[categorykey]++
   if(occurrences[categorykey]>emitted[sourcekey]) {
    emitted[sourcekey]=occurrences[categorykey]
    bodies[key]=bodies[key] "\t" parts[3] "\n"
    if(parts[3] ~ /\{$/) bodies[key]=bodies[key] "\t}\n"
   }
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
# Reviewed K3/K10/S6/F1 additions retain exact signatures/sites, not quotas.
# The same K3 line belongs to registry AND entry; it is physically emitted once.
shared_k3='if entry := s.conns[id]; entry != nil && entry.core != nil && entry.core.Dir() == netcore.Outbound && s.k3AdvertisedOtherSessionLocked(id, entry.core, "", false) {'
shared_count=$(rg -F -c -- "$shared_k3" "$BASE/internal/core/node/advertised_connection_observation.go")
if [ "$shared_count" != 1 ]; then
    echo 'FAIL: shared registry/entry fixture row multiplicity' >&2
    exit 1
fi
printf 'PASS positive: dual-category K3 row has one physical occurrence\n'
for member in k10RegisterLocked k10ForRegistered; do
    reset_case
    replace_source conn_registry.go "/^func .* $member(/,/^}/d"
    expect_failure "missing reviewed K10 $member" 'net.Conn-accepting functions'
    reset_case
    replace_source conn_registry.go "s/$member/replacement$member/"
    expect_failure "same-count K10 $member substitution" 'net.Conn-accepting functions'
    reset_case
    case "$member" in
        k10RegisterLocked) signature='func (s *Service) k10RegisterLocked(conn net.Conn, entry *connEntry)' ;;
        k10ForRegistered) signature='func (s *Service) k10ForRegistered(conn net.Conn) *k10TerminalCapsule' ;;
        *) echo 'FAIL: unreviewed K10 duplicate fixture' >&2; exit 2 ;;
    esac
    printf '\n%s {}\n' "$signature" >> "$CASE/internal/core/node/conn_registry.go"
    expect_failure "duplicate K10 $member declaration" 'net.Conn-accepting functions'
done
reset_case
append_go advertised_connection_observation.go <<'GO'
func (s *Service) k3AdvertisedRegistryRemovedLocked(id domain.ConnID) {
if entry := s.conns[id]; entry != nil && entry.core != nil && entry.core.Dir() == netcore.Outbound && s.k3AdvertisedOtherSessionLocked(id, entry.core, "", false) {
 }
}
GO
expect_failure 'duplicate physical K3 row affects both independent categories' 'legacy registry reads'
if ! rg -q 'legacy entry.core metrics reads' "$TEMP_ROOT/output"; then
    echo 'FAIL: duplicate shared K3 row escaped entry category' >&2
    exit 1
fi
reset_case
replace_source advertised_connection_observation.go 's/entry\.core\.Dir()/entry.core.UnknownDir()/'
expect_failure 'same-count K3 exact core-method substitution' 'legacy entry.core metrics reads'
reset_case
replace_source s6_terminal_construction.go 's/len(s.conns) != 0/len(s.conns) != 1/'
expect_failure 'changed terminal empty-registry predicate' 'legacy registry reads'
reset_case
replace_source find_node_membership_checkpoint.go 's/entry\.core\.ConnID()/entry.core.UnknownConnID()/'
expect_failure 'changed tagged F1 original ConnID proof' 'legacy entry.core metrics reads'
reset_case
replace_source service.go 's/entry\.core\.ClearAuth()/entry.core.SetAuth()/'
expect_failure 'changed observed auth clear operation' 'legacy entry.core metrics reads'
reset_case
replace_source service.go 's/entry\.core\.SetAuth(state)/entry.core.ClearAuth(state)/'
expect_failure 'changed observed auth set operation' 'legacy entry.core metrics reads'
reset_case
append_go advertised_connection_observation.go <<'GO'
func extraDiagnosticRegistryRead() { _ = len(s.conns) }
GO
expect_failure 'new registry site in reviewed diagnostic file' 'legacy registry reads'
reset_case
append_go advertised_connection_observation.go <<'GO'
func extraDiagnosticCoreWrite() { entry.core.SetAuth(state) }
GO
expect_failure 'new core write in reviewed diagnostic file' 'legacy entry.core metrics reads'
printf 'enforce-netcore-boundary regression controls: PASS\n'

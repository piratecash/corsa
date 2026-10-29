#!/usr/bin/env bash
# The owner's urgent-release scope defers overlaysim to D1, plus two exact
# inactive D1 timing diagnostics in 2.5.71 only. Ordinary make test/test-all
# include them. A deferral is never full-suite PASS.
set -euo pipefail

readonly module=github.com/piratecash/corsa
readonly deferred="$module/internal/overlaysim"
go_command=${GO:-go}

# A failed inventory must not turn partial stdout into a successful test run.
inventory=$("$go_command" list ./...) || exit "$?"
if [[ -z "$inventory" ]]; then
    echo 'release tests: empty package inventory' >&2
    exit 1
fi

packages=()
seen=()
deferred_count=0
while IFS= read -r package; do
    if [[ ! "$package" =~ ^github\.com/piratecash/corsa(/[A-Za-z0-9_.-]+)*$ ]] ||
       [[ "$package" == */. || "$package" == */.. || "$package" == */./* || "$package" == */../* ]]; then
        echo 'release tests: malformed package inventory' >&2
        exit 1
    fi
    for previous in "${seen[@]+"${seen[@]}"}"; do
        if [[ "$previous" == "$package" ]]; then
            echo 'release tests: duplicate package inventory' >&2
            exit 1
        fi
    done
    seen+=("$package")
    if [[ "$package" == "$deferred" ]]; then
        deferred_count=$((deferred_count + 1))
    else
        packages+=("$package")
    fi
done <<< "$inventory"

if [[ "$deferred_count" -ne 1 || "${#packages[@]}" -eq 0 ]]; then
    echo 'release tests: expected one explicit D1 deferral and nonempty remaining scope' >&2
    exit 1
fi
printf 'DEFERRED to D1 (not PASS): %s\n' "$deferred" >&2

# Bind the temporary exception to source constants, never an environment flag
# or a mutable tag. Missing/duplicate/malformed version declarations refuse.
readonly config=internal/core/config/config.go
if [[ ! -f "$config" || -L "$config" ]]; then
    echo 'release tests: regular source version configuration is required' >&2
    exit 1
fi
version_parts=()
for component in Major Minor Build; do
    count=0
    value=''
    while IFS= read -r line || [[ -n "$line" ]]; do
        if [[ "$line" =~ ^[[:space:]]*ClientVersion${component}[[:space:]]*=[[:space:]]*([0-9]+)[[:space:]]*$ ]]; then
            count=$((count + 1))
            value=${BASH_REMATCH[1]}
        fi
    done < "$config"
    if [[ "$count" -ne 1 ]]; then
        echo 'release tests: missing, ambiguous or malformed source version' >&2
        exit 1
    fi
    version_parts+=("$value")
done
version="${version_parts[0]}.${version_parts[1]}.${version_parts[2]}"
if [[ "$version" == 2.5.71 ]]; then
    readonly node_package="$module/internal/core/node"
    readonly diagnostics='^(TestOwnedReadyTargetTimingFourGenuineSources|TestOwnedReadyNormalTimedFactoryLoadsOriginalAndSignsWithoutGate)$'
    remaining=()
    node_count=0
    for package in "${packages[@]}"; do
        if [[ "$package" == "$node_package" ]]; then
            node_count=$((node_count + 1))
        else
            remaining+=("$package")
        fi
    done
    if [[ "$node_count" -ne 1 ]]; then
        echo 'release tests: interim 2.5.71 requires the exact Node package' >&2
        exit 1
    fi
    printf 'DEFERRED to D1 for 2.5.71 (not PASS): %s\n' \
        TestOwnedReadyTargetTimingFourGenuineSources \
        TestOwnedReadyNormalTimedFactoryLoadsOriginalAndSignsWithoutGate >&2
    if [[ "${#remaining[@]}" -ne 0 ]]; then
        "$go_command" test -count=1 -timeout=15m "${remaining[@]}" || exit "$?"
    fi
    exec "$go_command" test -count=1 -timeout=15m -skip "$diagnostics" "$node_package"
fi
exec "$go_command" test -count=1 -timeout=15m "${packages[@]}"

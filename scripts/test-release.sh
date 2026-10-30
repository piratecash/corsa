#!/usr/bin/env bash
# The owner's urgent-release scope defers exactly overlaysim to D1.
# Ordinary make test/test-all continue to include it. Never call this full-suite PASS.
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
exec "$go_command" test -count=1 -timeout=15m "${packages[@]}"

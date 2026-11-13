#!/usr/bin/env bash
# Bind a Checks test log to the checkout and workflow attempt that produced it.
set -euo pipefail

if [[ $# != 1 ]]; then
    exit 2
fi
case "$1" in
    test-release|test-s6-observation|test-cgo) target=$1 ;;
    *) exit 2 ;;
esac

sha=${GITHUB_SHA:?}
run_id=${GITHUB_RUN_ID:?}
attempt=${GITHUB_RUN_ATTEMPT:?}
[[ "$sha" =~ ^[0-9a-f]{40}$ && "$run_id" =~ ^[1-9][0-9]*$ && "$attempt" =~ ^[1-9][0-9]*$ ]]
[[ "$(git rev-parse HEAD)" == "$sha" ]]

printf 'S6 test begin %s %s %s %s\n' "$target" "$sha" "$run_id" "$attempt"
make "$target"
printf 'S6 test end %s %s %s %s\n' "$target" "$sha" "$run_id" "$attempt"

#!/bin/sh
# Run on the Docker host. The container supplies corsa-cli and jq.
set -eu

if [ "$#" -ne 2 ]; then
    printf 'Usage: %s CONTAINER OUTPUT.jsonl\n' "$0" >&2
    exit 1
fi
container=$1
output=$2
# Pin the container so a replacement cannot silently continue this pilot.
container_id=$(docker inspect --type container --format '{{.Id}}' -- "$container")
case "$container_id" in
    ''|*[!0123456789abcdef]*) exit 1 ;;
esac
[ "${#container_id}" -eq 64 ] || exit 1
umask 077
set -C

docker exec -i "$container_id" sh -s > "$output" <<'IN_CONTAINER'
set -eu
command -v corsa-cli >/dev/null
command -v jq >/dev/null
export NO_PROXY=127.0.0.1 no_proxy=127.0.0.1
start=$(cut -d. -f1 /proc/uptime)
index=0
incomplete=0
while [ "$index" -lt 16 ]; do
    now=$(cut -d. -f1 /proc/uptime)
    wait=$((start + 60 * index - now))
    if [ "$wait" -gt 0 ]; then
        sleep "$wait"
    fi
    round=$(
        printf '{"index":%s,"producer":"corsa-cli","rpc":[' "$index"
        separator=
        for command in getBaselineClock getNodeStatus fetchRouteSummary getNodeStatus getBaselineClock; do
            printf '%s{"command":"%s"' "$separator" "$command"
            if body=$(corsa-cli -host 127.0.0.1 "$command" 2>/dev/null) &&
                printf '%s\n' "$body" | jq -es 'length == 1 and (.[0] | type == "object" and (has("error") | not))' >/dev/null; then
                # Keep original decimal digits. jq 1.6 can round nanosecond integers.
                printf ',"state":"observed","body":'
                printf '%s' "$body" | tr -d '\r\n'
            else
                printf ',"state":"incomplete"'
            fi
            printf '}'
            separator=,
        done
        printf ']}'
    )
    printf '%s\n' "$round"
    if ! printf '%s\n' "$round" | jq -e 'all(.rpc[];
        .state == "observed" and (.command != "getBaselineClock" or .body.status == "sample"))' >/dev/null; then
        incomplete=2
    fi
    index=$((index + 1))
done
exit "$incomplete"
IN_CONTAINER

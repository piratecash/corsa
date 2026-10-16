#!/bin/zsh
# heavy-run-test.sh — rule 8 of heavy-run.sh (containers labelled
# corsa.loadstand are members of the job), checked against a FAKE docker.
#
#   scripts/heavy-run-test.sh            # tests scripts/heavy-run.sh
#   HEAVY_RUN_SCRIPT=<path> scripts/heavy-run-test.sh
#
# The real docker is never called and the machine-wide lock is never touched:
# the fake docker is first in a PATH that has no other docker, its containers
# are files in a temporary directory, and HEAVY_LOCK points into that
# directory. Exit code: 0 — every case passed; 1 — a case failed; 2 — the
# environment would reach a real docker or the real lock, nothing was run.
set -u
zmodload zsh/datetime

SCRIPT=${HEAVY_RUN_SCRIPT:-${0:A:h}/heavy-run.sh}
LABEL=corsa.loadstand
GRACE_S=2
# No /usr/local/bin and no Homebrew: that is where a real docker lives.
SYSTEM_PATH=/usr/bin:/bin:/usr/sbin:/sbin

work=$(mktemp -d "${TMPDIR:-/tmp}/heavy-run-test.XXXXXX") || exit 2
fake_bin=$work/bin
mkdir -p $fake_bin

if [[ -n $(PATH=$SYSTEM_PATH command -v docker) ]]; then
  print -u2 -r -- "heavy-run-test: a real docker is on $SYSTEM_PATH; refusing to run"
  exit 2
fi

cat > $fake_bin/docker <<'FAKE'
#!/bin/zsh
# Fake docker: a container is a file under $FAKE_DOCKER_STATE/running whose
# lines are its labels. Every call is appended to $FAKE_DOCKER_STATE/calls.
set -u
state=${FAKE_DOCKER_STATE:?}
print -r -- "$*" >> $state/calls
ids_after_dashes() {
  while (( $# )) && [[ $1 != -- ]]; do shift; done
  (( $# )) && shift
  print -r -- "$@"
}
cmd=$1; shift
case $cmd in
  context)
    # `context inspect --format …` — the endpoint of the current context.
    [[ -e $state/context_fails ]] && { print -u2 -r -- "context: cannot read the docker config"; exit 1 }
    if [[ -e $state/endpoint ]]; then print -r -- "$(<$state/endpoint)"; else print -r -- "unix://${FAKE_DOCKER_SOCK:?}"; fi
    exit 0 ;;
  ps)
    [[ -e $state/ps_hangs ]] && exec sleep 60
    [[ -e $state/ps_down ]] && { print -u2 -r -- "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?"; exit 1 }
    [[ -e $state/ps_fails ]] && { print -u2 -r -- "Error response from daemon: internal server error"; exit 1 }
    want=
    while (( $# )); do
      case $1 in
        --filter) want=$2; shift 2 ;;
        *) shift ;;
      esac
    done
    for f in $state/running/*(N); do
      grep -qx -- "$want" $f && print -r -- ${f:t}
    done
    exit 0 ;;
  stop)
    [[ -e $state/stop_ignored ]] && exit 0
    for id in ${=$(ids_after_dashes "$@")}; do rm -f $state/running/$id; done
    exit 0 ;;
  kill)
    for id in ${=$(ids_after_dashes "$@")}; do rm -f $state/running/$id; done
    exit 0 ;;
  run)
    # An attached `docker run --rm`: the CLI lives while the container does.
    labels=()
    while (( $# )); do
      case $1 in
        --label) labels+=("label=$2"); shift 2 ;;
        *) shift ;;
      esac
    done
    id=$(printf '%012x' $$)
    print -l -- $labels > $state/running/$id
    print -r -- $$ > $state/cli.pid
    if [[ -e $state/cli_ignores_term ]]; then
      trap '' TERM
    else
      trap 'rm -f $state/running/$id; exit 143' TERM
    fi
    while [[ -e $state/running/$id ]]; do sleep 0.1; done
    exit 0 ;;
esac
print -u2 -r -- "fake docker: unsupported command $cmd"
exit 1
FAKE
chmod +x $fake_bin/docker

# A real unix socket for the fake daemon: heavy-run.sh tests the endpoint with
# -S. Its own short directory, because a socket path is limited to ~104 bytes.
sock_dir=$(mktemp -d /tmp/hrt-sock.XXXXXX) || exit 2
FAKE_SOCK=$sock_dir/docker.sock
perl -MIO::Socket::UNIX -e 'IO::Socket::UNIX->new(Type => SOCK_STREAM(), Local => $ARGV[0], Listen => 1) or die "socket: $!\n"' $FAKE_SOCK || exit 2

wrappers=()
cleanup() {
  rm -f $work/case-*/state/running/*(N)
  rm -f $FAKE_SOCK; rmdir $sock_dir 2>/dev/null
  local pid
  for pid in $wrappers; do kill -KILL $pid 2>/dev/null; done
  [[ -n ${KEEP_WORK:-} ]] && print -r -- "kept $work" || rm -rf $work
}
trap cleanup EXIT

failures=0
current=
fail() { print -r -- "FAIL $current: $*"; (( failures++ )) }

# Per case: its own state, lock, log and DOCKER_HOST (empty = the context).
new_case() {
  current=$1
  docker_host=
  docker_timeout=1
  case_dir=$work/case-$1
  state=$case_dir/state
  lock=$case_dir/lock
  mkdir -p $state/running
  : > $state/calls
}

# heavy-run.sh with the fake docker first in PATH (or none, when $1 == none).
heavy_env() {
  local docker_path=$fake_bin:$SYSTEM_PATH
  [[ $1 == none ]] && docker_path=$SYSTEM_PATH
  reply=(HEAVY_LOCK=$lock HEAVY_STOP_GRACE_S=$GRACE_S HEAVY_DOCKER_TIMEOUT_S=$docker_timeout
         FAKE_DOCKER_STATE=$state FAKE_DOCKER_SOCK=$FAKE_SOCK DOCKER_HOST=$docker_host
         DOCKER_CONTEXT= PATH=$docker_path)
}

run_heavy() {
  heavy_env $1; shift
  env $reply zsh $SCRIPT "$@" 2>>$case_dir/stderr
}

# Polls a condition for up to $1 seconds.
wait_until() {
  local limit=$1; shift
  local tenths=0
  until eval "$*"; do
    (( tenths >= limit * 10 )) && return 1
    sleep 0.1; (( tenths++ ))
  done
  # A loop's status is its body's last one: `(( tenths++ ))` from 0 is 1.
  return 0
}

# Waits for the stand wrapper for up to $1 seconds and returns its code; a
# wrapper that hangs is a failure, not a hung test run.
wait_wrapper() {
  wait_until $1 '! kill -0 $wrapper 2>/dev/null' || {
    fail "wrapper $wrapper still runs after $1s"
    kill -KILL $wrapper 2>/dev/null
  }
  wait $wrapper
}

expect_code() {
  local want=$1 got=$2 what=$3
  [[ $got == $want ]] || fail "$what: exit $got, want $want (stderr: $(tail -3 $case_dir/stderr 2>/dev/null | tr '\n' '|'))"
}

container_count() { local files=($state/running/*(N)); print -r -- $#files }

# The wrapper is exec'd so that $! is heavy-run.sh itself, not a subshell
# around it: the tree-wide kill must hit the real wrapper.
start_stand() {
  heavy_env fake
  (
    exec env $reply zsh $SCRIPT stand docker run --rm --label $LABEL img 2>>$case_dir/stderr
  ) &
  wrapper=$!
  wrappers+=($wrapper)
  wait_until 5 '[[ -s $state/cli.pid && $(container_count) == 1 ]]' || fail "stand did not start its container"
  cli=$(<$state/cli.pid)
}

# The case the rule exists for: the docker CLI killed with -9 leaves the
# container running, and the lock must stay held while it does.
case_cli_kill9_keeps_lock() {
  new_case cli-kill9
  start_stand
  kill -KILL $cli
  sleep 1.5
  run_heavy fake probe touch $case_dir/ran; expect_code 75 $? "probe while the container runs"
  [[ -e $case_dir/ran ]] && fail "probe job ran beside the container"
  rm -f $state/running/*(N)
  wait_wrapper 10; expect_code 137 $? "stand wrapper (the CLI's own code)"
  [[ -e $lock ]] && fail "lock left after the container exited"
  run_heavy fake probe true; expect_code 0 $? "probe after the container exited"
}

# A tree-wide kill -9 (wrapper and CLI): the lock must read as held, not stale.
case_tree_kill9_reads_held() {
  new_case tree-kill9
  start_stand
  kill -KILL $wrapper $cli
  sleep 0.5
  run_heavy fake probe true; expect_code 75 $? "probe while the orphaned container runs"
  rm -f $state/running/*(N)
  run_heavy fake probe true; expect_code 76 $? "probe after the container exited (stale lock)"
  rm -rf $lock
}

# No lock at all (removed by hand) but a labelled container runs: refused,
# and the refusal does not leave a lock behind. Unlabelled containers do not count.
case_orphan_container_without_lock() {
  new_case orphan
  print -r -- "label=$LABEL" > $state/running/aaaaaaaaaaaa
  run_heavy fake probe touch $case_dir/ran; expect_code 75 $? "probe beside an orphaned container"
  [[ -e $case_dir/ran ]] && fail "probe job ran beside the container"
  [[ -e $lock ]] && fail "refusal left the lock behind"
  rm -f $state/running/aaaaaaaaaaaa
  print -r -- "label=someone.else" > $state/running/bbbbbbbbbbbb
  run_heavy fake probe true; expect_code 0 $? "probe beside an unlabelled container"
}

# A stop of a wrapper whose CLI is gone reaches the container through docker stop.
case_term_stops_container() {
  new_case term-stop
  start_stand
  local id=$(print -r -- $state/running/*(N:t))
  kill -KILL $cli
  sleep 1.5
  kill -TERM $wrapper
  wait_wrapper 10; expect_code 143 $? "stopped wrapper"
  grep -qx -- "stop --time $GRACE_S -- $id" $state/calls || fail "docker stop was not sent to $id"
  [[ $(container_count) == 0 ]] || fail "container still runs after the stop"
  [[ -e $lock ]] && fail "lock left after the stop"
}

# A container that outlives docker stop is killed after the grace.
case_term_kills_stubborn_container() {
  new_case term-kill
  start_stand
  local id=$(print -r -- $state/running/*(N:t))
  : > $state/stop_ignored
  kill -KILL $cli
  sleep 1.5
  kill -TERM $wrapper
  wait_wrapper 10; expect_code 143 $? "stopped wrapper"
  grep -qx -- "kill -- $id" $state/calls || fail "docker kill was not sent to $id"
  [[ $(container_count) == 0 ]] || fail "container still runs after the kill"
  [[ -e $lock ]] && fail "lock left after the kill"
}

# Docker silent through the whole grace of a stop: the containers are not
# taken as gone, and once docker answers they are killed, not waited on for ever.
case_term_outlasts_silent_docker() {
  new_case term-silent
  start_stand
  local id=$(print -r -- $state/running/*(N:t))
  kill -KILL $cli
  sleep 1.5
  : > $state/ps_fails
  kill -TERM $wrapper
  sleep $(( GRACE_S + 1.5 ))
  kill -0 $wrapper 2>/dev/null || fail "wrapper released the lock while docker did not answer"
  [[ -e $lock ]] || fail "lock gone while docker did not answer"
  rm -f $state/ps_fails
  wait_wrapper 10; expect_code 143 $? "stopped wrapper"
  grep -qx -- "kill -- $id" $state/calls || fail "docker kill was not sent to $id once docker answered"
  [[ $(container_count) == 0 ]] || fail "container still runs"
  [[ -e $lock ]] && fail "lock left after the kill"
}

# A daemon that answers with an error: unknown is not "none".
case_unanswered_docker_refuses() {
  new_case daemon-error
  : > $state/ps_fails
  run_heavy fake probe touch $case_dir/ran; expect_code 75 $? "probe while docker does not answer"
  [[ -e $case_dir/ran ]] && fail "probe job ran while docker did not answer"
  [[ -e $lock ]] && fail "refusal left the lock behind"
}

# A daemon that does not answer within the deadline: refused, and promptly.
case_hung_docker_refuses() {
  new_case daemon-hangs
  : > $state/ps_hangs
  local began=$SECONDS
  run_heavy fake probe touch $case_dir/ran; expect_code 75 $? "probe while docker hangs"
  (( SECONDS - began < 10 )) || fail "the hung query was not cut by the deadline ($(( SECONDS - began ))s)"
  [[ -e $case_dir/ran ]] && fail "probe job ran while docker hung"
  grep -q "did not answer within" $case_dir/stderr || fail "no reason given for the refusal"
}

# Daemon not running ("Cannot connect…", socket present): nothing can run on
# it, so a job starts, with one line saying why.
case_stopped_daemon_runs_job() {
  new_case daemon-stopped
  : > $state/ps_down
  run_heavy fake probe true; expect_code 0 $? "probe while the daemon is stopped"
  [[ $(grep -c "docker daemon is not running" $case_dir/stderr) == 1 ]] || fail "want exactly one 'not running' line"
}

# No socket at the endpoint at all: the same, and docker ps is not even asked.
case_missing_socket_runs_job() {
  new_case no-socket
  print -r -- "unix://$sock_dir/absent.sock" > $state/endpoint
  run_heavy fake probe true; expect_code 0 $? "probe without a daemon socket"
  grep -q "docker daemon is not running (no daemon socket" $case_dir/stderr || fail "no 'not running' line"
  grep -q '^ps ' $state/calls && fail "docker ps was asked although there is no socket"
}

# A stopped daemon also makes an orphaned lock read as stale, not as held.
case_stopped_daemon_lock_reads_stale() {
  new_case stopped-stale
  start_stand
  kill -KILL $wrapper $cli
  sleep 0.5
  : > $state/ps_down
  run_heavy fake probe true; expect_code 76 $? "probe beside a dead holder on a stopped daemon"
  rm -rf $lock
}

# A remote endpoint, or a context that cannot be read: refused, the job does
# not run, and the reason is named — a missing local socket says nothing about
# a daemon on another host.
expect_refused_with_reason() {
  run_heavy fake probe touch $case_dir/ran; expect_code 75 $? "probe ($1)"
  [[ -e $case_dir/ran ]] && fail "probe job ran ($1)"
  grep -q -- "$2" $case_dir/stderr || fail "the refusal does not say '$2' ($1)"
}

case_remote_docker_host_refuses() {
  new_case remote-host
  docker_host=tcp://192.0.2.1:2376
  expect_refused_with_reason "remote DOCKER_HOST" "tcp://192.0.2.1:2376 is not a local socket"
}

case_remote_context_refuses() {
  new_case remote-context
  print -r -- "ssh://builder@192.0.2.1" > $state/endpoint
  expect_refused_with_reason "remote context" "ssh://builder@192.0.2.1 is not a local socket"
}

case_unreadable_context_refuses() {
  new_case context-unreadable
  : > $state/context_fails
  expect_refused_with_reason "unreadable context" "the docker context could not be read"
}

# A hung docker must not postpone the group's KILL: it comes STOP_GRACE_S
# after the TERM by the clock, however long each docker query takes. The lock
# is then held until docker answers, and the container is stopped.
case_group_kill_not_delayed_by_hung_docker() {
  new_case kill-vs-hung
  docker_timeout=3
  : > $state/cli_ignores_term
  start_stand
  : > $state/ps_hangs
  local t0=$EPOCHREALTIME
  kill -TERM $wrapper
  wait_until 15 '! kill -0 $cli 2>/dev/null' || fail "the CLI outlived the stop"
  local took=$(( EPOCHREALTIME - t0 ))
  (( took <= GRACE_S + 1.5 )) || fail "KILL came $(printf %.1f $took)s after TERM, want <= $(( GRACE_S + 1.5 ))s"
  grep -q "ignored TERM for ${GRACE_S}s; sending KILL" $case_dir/stderr || fail "no KILL line"
  sleep 1
  kill -0 $wrapper 2>/dev/null || fail "wrapper released the lock while docker hung"
  rm -f $state/ps_hangs
  wait_wrapper 15; expect_code 143 $? "stopped wrapper"
  [[ $(container_count) == 0 ]] || fail "container still runs after docker answered"
  [[ -e $lock ]] && fail "lock left after the stop"
}

# No docker at all: the wrapper behaves as before rule 8.
case_no_docker_runs_plainly() {
  new_case no-docker
  run_heavy none probe true; expect_code 0 $? "plain job"
  run_heavy none probe zsh -c 'exit 3'; expect_code 3 $? "job's own code"
  run_heavy none holder zsh -c "touch $case_dir/holder-up; sleep 2" &
  local holder=$!
  wait_until 5 '[[ -e $case_dir/holder-up ]]' || fail "plain holder did not start"
  run_heavy none probe true; expect_code 75 $? "probe beside a plain live job"
  wait $holder; expect_code 0 $? "plain holder"
  run_heavy none probe true; expect_code 0 $? "probe after the plain job"
  [[ -s $state/calls ]] && fail "docker was called although it is not installed"
}

for c in case_cli_kill9_keeps_lock case_tree_kill9_reads_held case_orphan_container_without_lock \
         case_term_stops_container case_term_kills_stubborn_container case_term_outlasts_silent_docker \
         case_unanswered_docker_refuses case_hung_docker_refuses case_stopped_daemon_runs_job \
         case_missing_socket_runs_job case_stopped_daemon_lock_reads_stale case_remote_docker_host_refuses \
         case_remote_context_refuses case_unreadable_context_refuses case_group_kill_not_delayed_by_hung_docker \
         case_no_docker_runs_plainly; do
  before=$failures
  $c
  (( failures == before )) && print -r -- "ok   $current"
done

(( failures == 0 )) && { print -r -- "PASS"; exit 0 }
print -r -- "FAIL ($failures)"
exit 1

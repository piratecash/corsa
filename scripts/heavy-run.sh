#!/bin/zsh
# heavy-run.sh — runs one heavy job under the machine-wide exclusive lock.
#
#   heavy-run.sh <name> <command...>
#
# Heavy jobs on the owner's machine — the 13b load stand and the overlaysim
# stage-1 campaign, whose 64k×8 jobs cannot be resumed — are MUTUALLY
# EXCLUSIVE (owner's decision 2026-10-02). The exclusion is enforced BEFORE a
# job starts: the memory guard stops a job after the fact, and for a job that
# cannot be resumed that stop is the loss the rule exists to prevent.
#
# Eight rules, each of which a previous version broke:
#   1. The job runs in its OWN process group and a stop signals the whole
#      group: `go test` dies on TERM without passing it on, so signalling only
#      the direct child left the test binary running with the lock released.
#   2. The lock is released only after the group is empty — otherwise the
#      wrapper's exit released the lock under a job that was still running.
#      HUP does not end the wrapper at all (rule 7).
#   3. Liveness of the holder is read with `ps` (works for any owner) and
#      matched against the holder's recorded start time — `kill -0` answers
#      EPERM for a foreign process and a reused pid would look alive.
#   4. A lock without an owner file is the holder's own write window, not a
#      stale lock, until it is older than the window can be.
#   5. The lock is removed only by the holder whose token it carries — after a
#      manual removal and a new start, a finishing job must not remove the
#      lock of the next one.
#   6. The holder is alive while EITHER the wrapper OR any member of the job's
#      process group is: a wrapper killed with -9 (OOM, a tree-wide kill) left
#      a running job behind a lock that looked stale.
#   7. HUP is ignored and the job reads /dev/null: closing the terminal must
#      not stop a campaign that cannot be resumed, and a background group that
#      reads the tty would stop on SIGTTIN and hold the lock for ever.
#   8. A running container labelled corsa.loadstand is a member of the job:
#      a container lives outside the process group, and a docker CLI killed
#      with -9 leaves it running — the lock was released under it. The lock
#      is held while one runs, a stop sends `docker stop` and then
#      `docker kill` before the lock is released, and a start is refused while
#      one runs without a holder. Docker is asked with a deadline, and its
#      silence is read three ways:
#        - no docker, or a local daemon that is NOT RUNNING (no socket, or the
#          CLI's "Cannot connect to the Docker daemon") — no container can run
#          on it: none, with one line saying so. Docker Desktop is often quit
#          on this machine, and refusing then would stop every heavy job,
#          docker or not;
#        - a daemon that answers with an error or does not answer within
#          HEAVY_DOCKER_TIMEOUT_S — the containers are assumed alive: an
#          unanswered question is not an empty list;
#        - a context that is not a local unix socket (DOCKER_HOST, a remote
#          DOCKER_CONTEXT) or that cannot be read — assumed alive as well: "no
#          socket here" says nothing about a daemon on another host.
#      "Assumed alive" holds the lock after the job's own processes are gone
#      too: the wrapper keeps waiting until docker answers (fail-closed).
#      A stop never waits for docker while the process group lives: the
#      group's KILL comes STOP_GRACE_S after the TERM by the clock, and
#      docker is asked only once the group is empty, with its own grace for
#      `docker stop` before `docker kill`.
#
# Contract with a job that runs containers (the 13b stand, T5): every
# container it starts carries `--label corsa.loadstand`, through the caller's
# docker context. An unlabelled container is invisible to the lock.
#
# Known limits: a member that leaves the group (setsid, a daemonising tool) is
# no longer counted, and the lock is released while it runs; a wrapper killed
# with -9 in the few milliseconds before it records the group id leaves a lock
# that reads as stale. Containers are asked of the daemon of the caller's
# docker context (DOCKER_HOST, DOCKER_CONTEXT): one started through another
# daemon, or without the label, is not seen, and a created but not started
# container is not counted. "Not running" rests on the socket and on the CLI's
# wording, since its exit code does not tell a stopped daemon from a failing
# one: a CLI that rewords the message turns a stopped daemon into a refusal,
# never into a missed container. A dockerd that crashed while its containers
# live on under containerd-shim (live-restore) leaves no answering socket and
# reads as "not running" — those containers are missed. The endpoint is the
# caller's: a DOCKER_HOST set only inside the stand's shell is not seen by a
# heavy-run.sh started from another shell, so start the stand's wrapper and
# every other heavy job with the same docker environment. The job's stdout/stderr
# stay where the caller put them: redirect them to a log, or output written
# after the terminal closes is lost.
#
# A stale lock is NOT reclaimed automatically: two reclaimers could each remove
# the other's fresh lock. It is reported and left for a human, because a
# refusal costs a restart and a wrong reclaim costs a run.
#
# Exit codes: the job's own code; 75 — the lock is held, or a labelled
# container runs (or docker answers with an error, hangs, or is remote); 76 — the lock is stale, remove
# it by hand after checking; 77 — the lock path is not ours
# (not a plain directory owned by this user); 64 — usage.
set -u

LOCK=${HEAVY_LOCK:-/tmp/corsa-heavy-run.lock}
OWNER_WRITE_WINDOW_S=60
(( $# >= 2 )) || { print -u2 -r -- "usage: heavy-run.sh <name> <command...>"; exit 64 }
name=$1; shift
job_command="$*"
STOP_GRACE_S=${HEAVY_STOP_GRACE_S:-60}
# Checked before the lock is taken: in zsh arithmetic a typo is 0, and a zero
# grace turns the first TERM into a KILL of a job that cannot be resumed.
[[ $STOP_GRACE_S == <-> ]] || { print -u2 -r -- "heavy-run.sh: HEAVY_STOP_GRACE_S must be a whole number of seconds"; exit 64 }
DOCKER_TIMEOUT_S=${HEAVY_DOCKER_TIMEOUT_S:-15}
# A zero deadline would read every query as a hang and refuse every start.
[[ $DOCKER_TIMEOUT_S == <-> ]] && (( DOCKER_TIMEOUT_S > 0 )) || { print -u2 -r -- "heavy-run.sh: HEAVY_DOCKER_TIMEOUT_S must be a positive whole number of seconds"; exit 64 }

# The value of a key=value line of the owner file; empty when absent.
owner_field() { sed -n "s/^$1=//p" $LOCK/owner 2>/dev/null | head -1 }

# Bytes from a shared directory are printed quoted, never raw: the owner file
# could be written by anyone who can write to /tmp.
print_owner() {
  [[ -f $LOCK/owner && ! -L $LOCK/owner ]] || { print -u2 -r -- "  <no owner file>"; return }
  local line
  while IFS= read -r line; do print -u2 -r -- "  ${(q+)line}"; done < $LOCK/owner
}

# The start time is read in the C locale: `lstart` follows LC_TIME, and a
# holder started from one locale must not look dead to a checker in another.
process_lstart() { LC_ALL=C ps -p $1 -o lstart= 2>/dev/null }

group_has_members() { [[ $1 == <-> ]] && ps -A -o pgid= | grep -qx " *$1" }

# A group id is reused once the group is empty, and any new pipeline leader
# takes it: the recorded group counts only while its leader, if still present,
# is the process that was started for the job.
recorded_group_alive() {
  local pgid=$(owner_field pgid) recorded=$(owner_field leader_lstart) actual
  group_has_members $pgid || return 1
  actual=$(process_lstart $pgid)
  [[ -z $actual || -z $recorded || $actual == $recorded ]]
}

wrapper_alive() {
  local pid=$(owner_field pid) recorded=$(owner_field pid_lstart) actual
  [[ $pid == <-> ]] || return 1
  actual=$(process_lstart $pid) || return 1
  [[ -n $actual ]] || return 1
  # Owners written before rule 3 carry no start time: alive means present.
  [[ -z $recorded || $actual == $recorded ]]
}

CONTAINER_LABEL=corsa.loadstand

# Runs docker with a deadline of $1 seconds; 124 when it ran out. The CLI gets its own group
# and the whole group is killed: a hung helper that kept the output pipe open
# would otherwise hold the caller's command substitution after the CLI died.
docker_with_deadline() {
  perl -e '
    my $deadline = shift;
    defined(my $pid = fork) or exit 125;
    if (!$pid) { setpgrp(0, 0); exec { $ARGV[0] } @ARGV; exit 127 }
    $SIG{ALRM} = sub { kill "KILL", -$pid; waitpid $pid, 0; exit 124 };
    alarm $deadline;
    waitpid $pid, 0;
    exit($? & 127 ? 128 + ($? & 127) : $? >> 8);
  ' "$@"
}

# The endpoint docker would use: DOCKER_HOST wins over any context, exactly as
# in the CLI.
docker_endpoint() {
  [[ -n ${DOCKER_HOST:-} ]] && { print -r -- $DOCKER_HOST; return 0 }
  docker_with_deadline $DOCKER_TIMEOUT_S docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null
}

# The last answer about labelled containers. A call, not a command
# substitution: callers branch on the state and the one-time notice below
# must be remembered.
#   container_query — answered | down | unknown
#   container_ids   — running labelled containers (answered only)
#   container_why   — why the answer is down or unknown
container_query=
container_why=
typeset -a container_ids
docker_down_noticed=

query_labelled_containers() {
  setopt localoptions extendedglob
  container_ids=() container_why=
  if (( ! $+commands[docker] )); then container_query=answered; return; fi
  local endpoint
  if ! endpoint=$(docker_endpoint) || [[ -z $endpoint ]]; then
    container_query=unknown container_why="the docker context could not be read"
    return
  fi
  if [[ $endpoint != unix://* ]]; then
    container_query=unknown container_why="the docker endpoint ${(q+)endpoint} is not a local socket"
    return
  fi
  if [[ ! -S ${endpoint#unix://} ]]; then
    container_query=down container_why="no daemon socket at ${(q+)${endpoint#unix://}}"
    return
  fi
  local errors output code
  errors=$(mktemp "${TMPDIR:-/tmp}/heavy-run-docker.XXXXXX") || {
    container_query=unknown container_why="no temporary file for docker's errors"
    return
  }
  output=$(docker_with_deadline $DOCKER_TIMEOUT_S docker ps -q --filter "label=$CONTAINER_LABEL" 2>$errors)
  code=$?
  local first_error=$(head -1 $errors)
  rm -f $errors
  if (( code == 0 )); then
    container_query=answered
    container_ids=(${(M)${(f)output}:#[[:xdigit:]]##})
  elif (( code == 124 )); then
    container_query=unknown container_why="docker did not answer within ${DOCKER_TIMEOUT_S}s"
  elif [[ $first_error == *"Cannot connect to the Docker daemon"* ]]; then
    container_query=down container_why=${(q+)first_error}
  else
    container_query=unknown container_why="docker answered with an error: ${(q+)first_error}"
  fi
}

notice_docker_down() {
  [[ -n $docker_down_noticed ]] && return
  docker_down_noticed=1
  print -u2 -r -- "heavy-run.sh: the docker daemon is not running ($container_why); no labelled container can run on it"
}

labelled_containers_may_run() {
  query_labelled_containers
  case $container_query in
    answered) (( $#container_ids )) ;;
    down) notice_docker_down; return 1 ;;
    *) return 0 ;;
  esac
}

print_labelled_containers() {
  query_labelled_containers
  case $container_query in
    answered)
      local id
      for id in $container_ids; do print -u2 -r -- "  container ${(q+)id} (label $CONTAINER_LABEL)"; done ;;
    down) notice_docker_down ;;
    *) print -u2 -r -- "  containers labelled $CONTAINER_LABEL are assumed alive: $container_why" ;;
  esac
}

holder_alive() { wrapper_alive || recorded_group_alive || labelled_containers_may_run }

# GNU first: GNU `stat -f` is the file-system mode and prints output before
# failing, while BSD `stat -c` fails silently.
mtime_s() { stat -c %Y $1 2>/dev/null || stat -f %m $1 2>/dev/null || print 0 }
lock_age_s() { print -r -- $(( $(date +%s) - $(mtime_s $LOCK) )) }

refuse_taken_lock() {
  if [[ -L $LOCK || ! -d $LOCK || ! -O $LOCK ]]; then
    print -u2 -r -- "heavy-run.sh: refusing to start '$name': $LOCK is not a plain directory owned by this user."
    exit 77
  fi
  if [[ ! -e $LOCK/owner ]] && (( $(lock_age_s) < OWNER_WRITE_WINDOW_S )); then
    print -u2 -r -- "heavy-run.sh: refusing to start '$name': the heavy-job lock is being taken right now."
    exit 75
  fi
  if holder_alive; then
    print -u2 -r -- "heavy-run.sh: refusing to start '$name': the heavy-job lock is held by a live job:"
    print_owner
    print_labelled_containers
    exit 75
  fi
  print -u2 -r -- "heavy-run.sh: refusing to start '$name': the heavy-job lock $LOCK is stale (its holder is gone)."
  print -u2 -r -- "Check that no heavy job is running, then remove it by hand. Recorded holder:"
  print_owner
  exit 76
}

if ! mkdir -m 0700 $LOCK 2>/dev/null; then
  [[ -e $LOCK || -L $LOCK ]] || { print -u2 -r -- "heavy-run.sh: cannot create $LOCK (not a held lock — check the directory)"; exit 77 }
  refuse_taken_lock
fi

token=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')
# The owner file appears whole or not at all: a reader in the write window
# sees "no owner file" (rule 4), never a file without its pid.
write_owner() {
  {
    print -r -- "name=$name"
    print -r -- "pid=$$"
    print -r -- "pid_lstart=$(process_lstart $$)"
    print -r -- "started=$(date '+%Y-%m-%d %H:%M:%S')"
    print -r -- "token=$token"
    [[ -n ${1:-} ]] && print -r -- "pgid=$1"
    [[ -n ${2:-} ]] && print -r -- "leader_lstart=$2"
    print -r -- "command=$job_command"
  } > $LOCK/owner.next && mv -f $LOCK/owner.next $LOCK/owner
}
write_owner

release_lock() {
  [[ $(owner_field token) == $token ]] && rm -rf $LOCK
}
trap release_lock EXIT

# A non-interactive zsh cannot enable job control, so the new group is made by
# setpgrp before exec: the job's pid is then its process group id.
# Traps are in place BEFORE the job starts: a TERM in the start window must
# not take the default action and release the lock under a running group.
job=
pgid=
job_members_remain() { group_has_members $pgid || labelled_containers_may_run }

# Containers are asked about on every second of the container phase of a stop,
# not once: docker may answer only after a pause. Each id gets one stop and,
# after the grace, one kill. The requests run in the background, each with its
# own deadline, and their outcome is not read here: the loop asks docker again.
typeset -A container_requests
typeset -a unrequested

# Sets $unrequested to the containers of the LAST answer that have not had the
# request $1 (stop or kill) yet, and marks them as having had it. It reads the
# answer the loop condition has just obtained rather than asking docker a
# second time. Not a command substitution: the marks must survive into the
# next second.
collect_unrequested_containers() {
  local id
  unrequested=()
  [[ $container_query == answered ]] || return 0
  for id in $container_ids; do
    [[ -n ${container_requests[$1:$id]:-} ]] && continue
    container_requests[$1:$id]=1
    unrequested+=($id)
  done
}

stop_new_containers() {
  collect_unrequested_containers stop
  (( $#unrequested )) || return 0
  docker_with_deadline $(( STOP_GRACE_S + DOCKER_TIMEOUT_S )) docker stop --time $STOP_GRACE_S -- $unrequested >/dev/null 2>&1 &!
}

kill_new_containers() {
  collect_unrequested_containers kill
  (( $#unrequested )) || return 0
  print -u2 -r -- "heavy-run.sh: '$name' containers ${(j: :)${(q+)unrequested}} outlived docker stop for ${STOP_GRACE_S}s; sending docker kill"
  docker_with_deadline $DOCKER_TIMEOUT_S docker kill -- $unrequested >/dev/null 2>&1 &!
}

# Rule 8 made visible: a job whose CLI died while its container runs would
# otherwise hold the lock with no word about why.
wait_for_job_members() {
  local reported=
  while job_members_remain; do
    if [[ -z $reported ]] && ! group_has_members $pgid; then
      print -u2 -r -- "heavy-run.sh: '$name' exited; the lock is held while its containers run:"
      print_labelled_containers
      reported=1
    fi
    sleep 1
  done
}

# The process group first, by the clock and without a single docker call: a
# hung daemon must not postpone the group's KILL (each query may take up to
# HEAVY_DOCKER_TIMEOUT_S, and counting loop turns once stretched a 60 s grace
# into a quarter of an hour). Containers only after the group is empty.
stop_job() {
  [[ -n $job ]] || exit $1
  # The direct signal covers the instant before setpgrp, when the job is
  # still in this wrapper's group and a group signal would miss it. Only then:
  # a pid outside our group may already belong to another process.
  local own_pgid=$(ps -p $$ -o pgid= | tr -d ' ')
  [[ $(ps -p $job -o pgid= 2>/dev/null | tr -d ' ') == $own_pgid ]] && kill -TERM $job 2>/dev/null
  kill -TERM -- -$pgid 2>/dev/null
  stop_group_by_deadline $(( SECONDS + STOP_GRACE_S ))
  stop_containers_by_deadline
  exit $1
}

stop_group_by_deadline() {
  local deadline=$1 killed=
  while group_has_members $pgid; do
    if [[ -z $killed ]] && (( SECONDS >= deadline )); then
      print -u2 -r -- "heavy-run.sh: '$name' group $pgid ignored TERM for ${STOP_GRACE_S}s; sending KILL"
      kill -KILL -- -$pgid 2>/dev/null
      killed=1
    fi
    sleep 1
  done
}

# Each container gets one `docker stop` and, once STOP_GRACE_S has passed by
# the clock, one `docker kill`; a container docker reports only later gets
# its own requests when it appears.
stop_containers_by_deadline() {
  local kill_after=$(( SECONDS + STOP_GRACE_S ))
  while labelled_containers_may_run; do
    if (( SECONDS < kill_after )); then stop_new_containers; else kill_new_containers; fi
    sleep 1
  done
}

trap 'stop_job 143' TERM
trap 'stop_job 130' INT
trap '' HUP
# Checked under the lock and its traps, not before them: a container started
# between a check and the mkdir would otherwise run beside this job. A holder
# that died and a lock removed by hand leave exactly this — a labelled
# container with no lock.
if labelled_containers_may_run; then
  print -u2 -r -- "heavy-run.sh: refusing to start '$name': a container labelled $CONTAINER_LABEL may run without a lock holder:"
  print_labelled_containers
  print -u2 -r -- "Check by hand (docker ps --filter label=$CONTAINER_LABEL), stop what runs, then start again."
  exit 75
fi
perl -e 'setpgrp(0, 0) or die "setpgrp: $!\n"; exec { $ARGV[0] } @ARGV or die "exec $ARGV[0]: $!\n"' -- "$@" < /dev/null &
job=$!
pgid=$job
write_owner $pgid "$(process_lstart $job)"

wait $job
code=$?
# The leader may exit before its children, and a docker CLI before its
# container; the lock outlives every member (rules 6 and 8).
wait_for_job_members
exit $code

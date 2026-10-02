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
# Five rules, each of which a previous version broke:
#   1. The job runs in its OWN process group and a stop signals the whole
#      group: `go test` dies on TERM without passing it on, so signalling only
#      the direct child left the test binary running with the lock released.
#   2. The lock is released only after the group is empty, and HUP is handled
#      like TERM — otherwise the wrapper's exit released the lock under a job
#      that was still running.
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
#
# Known limits: a member that leaves the group (setsid, a daemonising tool) is
# no longer counted, and the lock is released while it runs; a wrapper killed
# with -9 in the few milliseconds before it records the group id leaves a lock
# that reads as stale. The job's stdout/stderr stay where the caller put them:
# redirect them to a log, or output written after the terminal closes is lost.
#
# A stale lock is NOT reclaimed automatically: two reclaimers could each remove
# the other's fresh lock. It is reported and left for a human, because a
# refusal costs a restart and a wrong reclaim costs a run.
#
# Exit codes: the job's own code; 75 — the lock is held; 76 — the lock is
# stale, remove it by hand after checking; 77 — the lock path is not ours
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

holder_alive() { wrapper_alive || recorded_group_alive }

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
stop_job() {
  [[ -n $job ]] || exit $1
  # The direct signal covers the instant before setpgrp, when the job is
  # still in this wrapper's group and a group signal would miss it. Only then:
  # a pid outside our group may already belong to another process.
  local own_pgid=$(ps -p $$ -o pgid= | tr -d ' ')
  [[ $(ps -p $job -o pgid= 2>/dev/null | tr -d ' ') == $own_pgid ]] && kill -TERM $job 2>/dev/null
  kill -TERM -- -$pgid 2>/dev/null
  local waited=0
  while group_has_members $pgid; do
    (( waited == STOP_GRACE_S )) && {
      print -u2 -r -- "heavy-run.sh: '$name' group $pgid ignored TERM for ${STOP_GRACE_S}s; sending KILL"
      kill -KILL -- -$pgid 2>/dev/null
    }
    sleep 1; (( waited++ ))
  done
  exit $1
}
trap 'stop_job 143' TERM
trap 'stop_job 130' INT
trap '' HUP
perl -e 'setpgrp(0, 0) or die "setpgrp: $!\n"; exec { $ARGV[0] } @ARGV or die "exec $ARGV[0]: $!\n"' -- "$@" < /dev/null &
job=$!
pgid=$job
write_owner $pgid "$(process_lstart $job)"

wait $job
code=$?
# The leader may exit before its children; the lock outlives every member.
while group_has_members $pgid; do sleep 1; done
exit $code

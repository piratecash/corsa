#!/bin/bash
# ONLY through scripts/heavy-run.sh. Bounded native properties, not E0-E5/T7.
set -eu
(( $# == 1 || $# == 2 )) || { printf '%s\n' 'usage: test-loadstand-linux-owner.sh ABSOLUTE_BINARY_DIRECTORY [restore|pdeath|fds|blocked-sample|bootstrap|runtime-start]' >&2; exit 64; }
native_case=default
if (( $# == 2 )); then native_case=$2; fi
case $native_case in
  default|restore|pdeath|fds|blocked-sample|bootstrap|runtime-start) ;;
  *) printf '%s\n' 'unsupported native control; no resources created' >&2; exit 64 ;;
esac
case $native_case in
  default) native_selector='^(TestLinuxHelperClosedPayload|TestLinuxHelperHeaderBounds|TestLinuxHelperSpontaneousStatusDoesNotAcknowledgeSentStop|TestLinuxHelperRequestOverflowPermanentlyPoisons|TestLinuxPacketManagedCloseInterruptsParkedRead|TestLinuxPacketBootstrapAliasesDoNotMaskEOF|TestLinuxPacketSCMRightsDisposedForEveryVerdict|TestLinuxPacketReceivedRightsAreCloexec|TestLinuxPacketBackpressureStopAndManagedClose|TestLinuxPacketManagedCloseInterruptsBackpressuredWrite|TestLinuxProfileZeroAndUnownedValuesRefuse|TestLinuxNamespaceWrongKindAndZeroClose|TestLinuxNeverStartedDoesNotInventHelperWait|TestLinuxLifecycleDefectStopsButNeverInventsWait|TestLinuxSampleTimeoutPoisonsAndNeverReusesReply|TestLinuxNamespaceTransferredTCPRetainsSingleCallerOwner|TestLinuxStartPreflightReaderObservesStopBeforeClaim|TestLinuxNamespaceRepeatedManagedCloseRetainsResult|TestLinuxInitializationFailureSurvivesStoppingFirst|TestLinuxInitializationReadyDoesNotOverrideStopOrCancel|TestLinuxInitializationErrorSurvivesLaterCancellation|TestLinuxInitializationReadyReleasesWorkOwner|TestLinuxOwnedFixtureNative|TestLinuxStartObservedStopBeforeClaimDoesNotStart|TestLinuxStartClaimWinsStopBeforePublicationStopsActualProcess|TestLinuxStartStopCoalescingKeepsEarliestDeadlineAndDefect|TestLinuxStartExpiredStopRetainsActualSignalFacts|TestLinuxStartFailedClaimCannotBeReused|TestLinuxRPCActualTransportCarriesFiniteDialBound|TestLinuxRPCDirectParentBoundsAndCancellation|TestLinuxRPCTransportParentCancellationJoinsDial|TestLinuxRPCRequestReturnDoesNotInventHeldDialJoin|TestLinuxRPCAdapterCancelDoesNotCloseTransferredConnection)$' ;;
  runtime-start) native_selector='^(TestLinuxRuntimePublicTupleValidation|TestLinuxRuntimeJointFactsRequired|TestLinuxRuntimeSharedWorkAndCopiedHistory|TestLinuxRuntimePublicationRejectsTerminalRaces|TestLinuxRuntimeReplyCorrelationAndOriginalNamespace|TestLinuxRuntimeTimeoutNeverReplaysLateSuccess|TestLinuxRuntimeClosedAuthorityInventory|TestLinuxRuntimeClosedAuthorityNegativeReferences|TestLinuxRuntimePublicationContextIsolatedFromOuterPoison|TestLinuxRuntimePinnedNamespaceDescriptors|TestLinuxRuntimeOwnedObservationNative|TestLinuxRuntimeClosedObservationPayload)$' ;;
  restore) native_selector='^TestLinuxAdversarialRestoreRetiresThread$' ;;
  bootstrap) native_selector='^(TestLinuxNamespaceOffMainBootstrap|TestLinuxNamespaceTaskRetirementConjunction|TestLinuxNamespaceTaskStateParsing)$' ;;
  pdeath) native_selector='^TestLinuxAdversarialPdeathKillsTarget$' ;;
  fds) native_selector='^TestLinuxAdversarialForbiddenFDRefusesReady$' ;;
  blocked-sample) native_selector='^TestLinuxAdversarialExitPrecedesBlockedSampleJoin$' ;;
esac
script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || exit $?
binary_dir=$1
[[ $binary_dir == /* && -d $binary_dir && ! -L $binary_dir ]] || exit 64
perl -e '
  use strict; use warnings; use Fcntl qw(O_RDONLY O_NOFOLLOW S_ISDIR S_ISREG); use Time::HiRes qw(time sleep);
  my ($lock,$wrapper)=@ARGV; my $until=time()+2; $ENV{LC_ALL}="C";
  sub normalized {my $value=shift;$value =~ s/^\s+|\s+$//g;return $value}
  sub process_field {
    my ($pid,$field)=@_; open my $ps,"-|","/bin/ps","-p",$pid,"-o","$field=" or return "";
    local $/; my $value=<$ps>//""; close $ps or return ""; $value =~ s/^\s+|\s+$//g; return $value;
  }
  sub owner_row {
    my @dir=lstat($lock); return unless @dir && S_ISDIR($dir[2]) && $dir[4]==$> && ($dir[2]&0777)==0700;
    my @before=lstat("$lock/owner"); return unless @before && S_ISREG($before[2]) && $before[4]==$> && !($before[2]&0022) && $before[7]<=16384;
    sysopen(my $file,"$lock/owner",O_RDONLY|O_NOFOLLOW) or return;
    my $count=read($file,my $raw,16385); close $file or return;
    my @after=lstat("$lock/owner"); return unless defined($count) && $count<=16384 && @after && $before[0]==$after[0] && $before[1]==$after[1];
    my %row; for(split /\n/,$raw){my($key,$value)=split /=/,$_,2; return if exists $row{$key};$row{$key}=$value//""} return ($raw,\%row);
  }
  while(1){
    my($raw,$row)=owner_row(); exit 75 unless $row;
    if(!exists($row->{pgid})){exit 75 if time()>=$until;sleep .05;next}
    exit 75 unless ($row->{token}//"") =~ /^[0-9a-f]{16}$/ && ($row->{pid}//"") =~ /^[1-9][0-9]*$/ && ($row->{pgid}//"") =~ /^[1-9][0-9]*$/;
    my($pid,$pgid)=@$row{qw(pid pgid)};
    exit 75 unless getpgrp(0)==$pgid && process_field($pid,"lstart") eq normalized($row->{pid_lstart}//"") && process_field($pgid,"lstart") eq normalized($row->{leader_lstart}//"");
    exit 75 unless process_field($pgid,"ppid") eq $pid && process_field($pid,"command") =~ /\Q$wrapper\E(?:\s|$)/;
    my($again)=owner_row();exit 75 unless defined($again) && $again eq $raw;exit 0;
  }
' "${HEAVY_LOCK:-/tmp/corsa-heavy-run.lock}" "$script_dir/heavy-run.sh" || { printf '%s\n' 'this live heavy-run job ownership was not established' >&2; exit 75; }
for name in loadstand.test linuxhelper sdkedge; do
  [[ -f $binary_dir/$name && ! -L $binary_dir/$name ]] || exit 64
done

# Every Docker call has its own CLI deadline. Unknown create/start outcomes are
# never interpreted as absence: exact labels remain visible to heavy-run.
docker_bounded() {
  local allowance=$1; shift
  # The group leader remains alive until the supervisor acknowledges cleanup,
  # reserving its PID/PGID. CLI/plugin output goes to private regular files,
  # never to the caller substitution pipe. A stray inherited writer cannot
  # hold that pipe or survive a nominal direct-CLI success.
  perl -e '
    use strict;use warnings;use File::Temp qw(tempfile);use Fcntl qw(F_SETFD FD_CLOEXEC);
    my $deadline=shift;
    my($out)=tempfile(UNLINK=>1);my($err)=tempfile(UNLINK=>1);
    pipe(my $status_read,my $status_write) or exit 125;pipe(my $ack_read,my $ack_write) or exit 125;
    pipe(my $group_read,my $group_write) or exit 125;
    fcntl($_,F_SETFD,FD_CLOEXEC) or exit 125 for($status_read,$status_write,$ack_read,$ack_write,$group_read,$group_write);
    defined(my $leader=fork) or exit 125;
    if(!$leader){
      close $group_read;
      setpgrp(0,0) or exit 125;close $status_read;close $ack_write;
      print $group_write "group-ready\n";close $group_write;
      # No CLI fork is permitted until the parent observed the owned group.
      my $permission;
      sysread($ack_read,$permission,1)==1 && $permission eq "g" or exit 125;
      defined(my $cli=fork) or exit 125;
      if(!$cli){close $status_write;close $ack_read;open STDOUT,">&",$out or exit 125;open STDERR,">&",$err or exit 125;exec { $ARGV[0] } @ARGV;exit 127}
      waitpid($cli,0);my $status=$?;print $status_write "$status\n";close $status_write;
      read($ack_read,my $ack,1);exit 0;
    }
    close $status_write;close $ack_read;close $group_write;
    sub emit {seek($out,0,0);seek($err,0,0);while(read($out,my $b,65536)){print STDOUT $b}while(read($err,my $b,65536)){print STDERR $b}}
    # The direct leader is still unreaped, so its PID cannot be recycled. Kill
    # that exact child AND any group it established before waiting, including
    # the fork→setpgrp/readiness window. Permission has not escaped that window.
    sub terminate_owned {kill "KILL",$leader;kill "KILL",-$leader;waitpid($leader,0)}
    $SIG{ALRM}=sub{terminate_owned();emit();print STDERR "owned CLI group deadline; outcome unknown\n";exit 124};alarm $deadline;
    my $group_ready=<$group_read>;
    defined($group_ready) && $group_ready eq "group-ready\n" or do{terminate_owned();emit();exit 125};
    syswrite($ack_write,"g",1)==1 or do{terminate_owned();emit();exit 125};
    my $status=<$status_read>;defined($status) && $status =~ /^\d+\n$/ or do{kill "KILL",-$leader;waitpid($leader,0);emit();exit 125};
    open my $ps,"-|","/bin/ps","-A","-o","pid=","-o","pgid=","-o","stat=" or exit 125;
    my $stray=0;while(<$ps>){if(/^\s*(\d+)\s+(\d+)\s+(\S+)/ && $2==$leader && $1!=$leader && $3!~/^Z/){$stray=1}}close $ps or do{kill "KILL",-$leader;waitpid($leader,0);emit();exit 125};
    if($stray){kill "KILL",-$leader;waitpid($leader,0);emit();print STDERR "owned CLI left a live pipe/plugin child; outcome unknown\n";exit 125}
    print $ack_write "x";close $ack_write;waitpid($leader,0);alarm 0;emit();
    exit($status & 127 ? 128+($status&127) : $status>>8);
  ' "$allowance" docker "$@"
}
run_uuid=$(/usr/bin/uuidgen) || exit $?
[[ $run_uuid =~ ^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$ ]] || exit 64
run_uuid=$(printf '%s' "$run_uuid" | /usr/bin/tr 'A-F' 'a-f') || exit $?
run_nonce="t5linux-v4-$run_uuid"
container_name="corsa-$run_nonce"
volume_name="corsa-$run_nonce"
expected_image='sha256:33bee74c45f307e3268adc2010c0f55c48e7a6041e12cd12432bb1a46e498e43'
volume_attempted=0
container_attempted=0
native_start_attempted=0
native_collection_attempted=0
native_start_cli_rc=unknown
native_wait_cli_rc=unknown
native_inspect_cli_rc=unknown
native_actual_exit=unknown
native_inspect_exit=unknown
native_observed_state=unknown
native_running=unknown
native_restarting=unknown
native_dead=unknown
native_ownership=unknown
native_outcome_owner=unknown
native_execution=not_started
native_execution_rc=125
native_container_resource=not_attempted
native_volume_resource=not_attempted
native_stop_cli_rc=unknown
native_kill_cli_rc=unknown
native_container_rm_rc=unknown
native_volume_rm_rc=unknown
native_cleanup_rc=unknown
native_final_rc=125

valid_native_exit() {
  local candidate=$1
  [[ $candidate =~ ^(0|[1-9][0-9]{0,2})$ ]] || return 1
  (( 10#$candidate <= 255 ))
}
verify_volume() {
  local ownership
  ownership=$(docker_bounded 5 volume inspect --format '{{.Driver}}|{{index .Labels "corsa.owner"}}' "$volume_name") || return 1
  [[ $ownership == "local|$run_nonce" ]] || return 1
  native_volume_resource=present_verified
}
verify_container() {
  local ownership
  ownership=$(docker_bounded 5 inspect --format '{{index .Config.Labels "corsa.owner"}}|{{.Image}}' "$container_name") || { native_ownership=unknown; return 1; }
  [[ $ownership == "$run_nonce|$expected_image" ]] || { native_ownership=mismatch; return 1; }
  native_ownership=verified
  native_container_resource=present_verified
}
collect_outcome() {
  (( native_collection_attempted == 0 )) || return 0
  native_collection_attempted=1
  native_execution=unknown
  native_execution_rc=125
  (( native_start_attempted )) && [[ $native_ownership == verified ]] || return 125
  local waited inspected
  native_wait_cli_rc=0
  waited=$(docker_bounded 5 wait "$container_name") || native_wait_cli_rc=$?
  if (( native_wait_cli_rc == 0 )) && valid_native_exit "$waited"; then native_actual_exit=$waited; fi
  # A failed wait never skips this independent PRE-CLEANUP inspection.
  native_inspect_cli_rc=0
  inspected=$(docker_bounded 5 inspect --format '{{.State.Status}}|{{.State.Running}}|{{.State.Restarting}}|{{.State.Dead}}|{{.State.ExitCode}}|{{index .Config.Labels "corsa.owner"}}|{{.Image}}' "$container_name") || native_inspect_cli_rc=$?
  if (( native_inspect_cli_rc == 0 )); then
    local -a fields=()
    IFS='|' read -r -a fields <<< "$inspected" || return 125
    if [[ $inspected != *$'\n'* ]] && (( ${#fields[@]} == 7 )); then
      case ${fields[0]} in created|running|paused|restarting|removing|exited|dead) native_observed_state=${fields[0]} ;; esac
      [[ ${fields[1]} == true || ${fields[1]} == false ]] && native_running=${fields[1]}
      [[ ${fields[2]} == true || ${fields[2]} == false ]] && native_restarting=${fields[2]}
      [[ ${fields[3]} == true || ${fields[3]} == false ]] && native_dead=${fields[3]}
      if valid_native_exit "${fields[4]}"; then native_inspect_exit=${fields[4]}; fi
      if [[ ${fields[5]} == $run_nonce && ${fields[6]} == $expected_image ]]; then
        native_outcome_owner=verified
      else
        native_outcome_owner=mismatch
      fi
    fi
  fi
  [[ $native_outcome_owner == verified && $native_observed_state == exited && $native_running == false && $native_restarting == false && $native_dead == false ]] || return 125
  [[ $native_start_cli_rc != unknown && $native_start_cli_rc != 124 && $native_start_cli_rc != 125 && $native_actual_exit != unknown && $native_inspect_exit != unknown ]] || return 125
  (( native_wait_cli_rc == 0 && native_inspect_cli_rc == 0 )) || return 125
  if [[ $native_start_cli_rc != $native_actual_exit || $native_actual_exit != $native_inspect_exit ]]; then
    native_execution=contradictory
    return 125
  fi
  native_execution_rc=$native_actual_exit
  if (( native_actual_exit == 0 )); then native_execution=known_success; else native_execution=known_failure; fi
  return 0
}
cleanup() {
  if (( container_attempted )); then
    verify_container || return 1
    native_stop_cli_rc=0
    docker_bounded 5 stop --time 1 "$container_name" >/dev/null 2>&1 || native_stop_cli_rc=$?
    if (( native_stop_cli_rc != 0 )); then
      native_kill_cli_rc=0
      docker_bounded 5 kill "$container_name" >/dev/null 2>&1 || native_kill_cli_rc=$?
      (( native_kill_cli_rc == 0 )) || return 1
    fi
    native_container_rm_rc=0
    docker_bounded 5 rm "$container_name" >/dev/null || native_container_rm_rc=$?
    (( native_container_rm_rc == 0 )) || return 1
    native_container_resource=removed_acknowledged
    local container_remaining
    container_remaining=$(docker_bounded 5 ps --all --filter "label=corsa.owner=$run_nonce" --format '{{.Names}}') || return 1
    [[ -z $container_remaining ]] || return 1
    native_container_resource=absent_confirmed
    container_attempted=0
  fi
  if (( volume_attempted )); then
    verify_volume || return 1
    native_volume_rm_rc=0
    docker_bounded 5 volume rm "$volume_name" >/dev/null || native_volume_rm_rc=$?
    (( native_volume_rm_rc == 0 )) || return 1
    native_volume_resource=removed_acknowledged
    local volume_remaining
    volume_remaining=$(docker_bounded 5 volume ls --filter "label=corsa.owner=$run_nonce" --format '{{.Name}}') || return 1
    [[ -z $volume_remaining ]] || return 1
    native_volume_resource=absent_confirmed
    volume_attempted=0
  fi
  return 0
}
write_diagnostic() {
  # Scalar public data only. File::Temp creates0600, and rename publishes one
  # complete record. A write/close/rename error never claims a saved receipt.
  perl -MJSON::PP -MFile::Temp=tempfile -MFile::Basename=dirname -e '
    use strict;use warnings;
    my $destination=shift;my %record;while(@ARGV){my $key=shift;my $value=shift;$record{$key}=$value}
    my($file,$temporary)=tempfile(".native-outcome-XXXXXXXX",DIR=>dirname($destination),UNLINK=>0);
    my $encoded=JSON::PP->new->canonical->encode(\%record)."\n";
    if(!print {$file} $encoded){close($file);unlink($temporary);exit 1}
    if(!close($file)){unlink($temporary);exit 1}
    rename($temporary,$destination) or do{unlink($temporary);exit 1};
  ' "$1" owner "$run_nonce" image "$expected_image" start_attempted "$native_start_attempted" start_cli_rc "$native_start_cli_rc" wait_cli_rc "$native_wait_cli_rc" actual_exit "$native_actual_exit" inspect_cli_rc "$native_inspect_cli_rc" inspect_exit "$native_inspect_exit" observed_state "$native_observed_state" running "$native_running" restarting "$native_restarting" dead "$native_dead" outcome_owner "$native_outcome_owner" execution "$native_execution" execution_rc "$native_execution_rc" cleanup_rc "$native_cleanup_rc" container_resource "$native_container_resource" volume_resource "$native_volume_resource" stop_cli_rc "$native_stop_cli_rc" kill_cli_rc "$native_kill_cli_rc" container_rm_rc "$native_container_rm_rc" volume_rm_rc "$native_volume_rm_rc" final_rc "$native_final_rc"
}
finish() {
  local previous_rc=$?
  trap - EXIT
  if (( native_start_attempted && !native_collection_attempted )); then
    local collected_rc=0
    collect_outcome || collected_rc=$?
  fi
  if (( native_start_attempted )); then
    native_final_rc=$native_execution_rc
    if (( native_final_rc == 0 && previous_rc != 0 )); then native_final_rc=$previous_rc; fi
  else
    native_final_rc=$previous_rc
    (( native_final_rc != 0 )) || native_final_rc=125
  fi
  native_cleanup_rc=0
  cleanup || native_cleanup_rc=125
  (( native_cleanup_rc == 0 )) || native_final_rc=125
  if ! write_diagnostic "$binary_dir/outcome-$run_nonce.json"; then
    printf '%s\n' "native diagnostic write failed; owner=$run_nonce container=$native_container_resource volume=$native_volume_resource execution=$native_execution final=125" >&2
    exit 125
  fi
  if (( native_cleanup_rc != 0 )); then
    if ! write_diagnostic "$binary_dir/retention-$run_nonce.json"; then
      printf '%s\n' "native retention write failed; owner=$run_nonce container=$native_container_resource volume=$native_volume_resource final=125" >&2
      exit 125
    fi
  fi
  exit "$native_final_rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

manifest="$binary_dir/sha256.expected"
: > "$manifest" || exit $?
for name in loadstand.test linuxhelper sdkedge; do
  digest=$(/usr/bin/shasum -a 256 "$binary_dir/$name") || exit $?
  digest=${digest%% *}
  [[ ${#digest} == 64 && $digest != *[^0-9a-f]* ]] || exit 64
  printf '%s  %s\n' "$digest" "$name" >> "$manifest" || exit $?
done
helper_sha=$(sed -n '2s/ .*//p' "$manifest") || exit $?
target_sha=$(sed -n '3s/ .*//p' "$manifest") || exit $?
actual_image=$(docker_bounded 5 image inspect --format '{{.Id}}' alpine:3) || exit $?
[[ $actual_image == $expected_image ]] || exit 65
volume_attempted=1
native_volume_resource=unknown
docker_bounded 5 volume create --label corsa.loadstand --label "corsa.owner=$run_nonce" "$volume_name" >/dev/null || exit $?
verify_volume || exit 65
container_attempted=1
native_container_resource=unknown
docker_bounded 10 create --pull=never --name "$container_name" --label corsa.loadstand --label "corsa.owner=$run_nonce" \
  --network none --cpus 2 --memory 768m --pids-limit 128 --init --read-only \
  --cap-drop ALL --cap-add NET_ADMIN --cap-add SYS_ADMIN --cap-add SETUID --cap-add SETGID \
  --security-opt no-new-privileges:true --ulimit core=0 \
  --tmpfs /tmp:rw,nosuid,noexec,size=32m --tmpfs /run:rw,nosuid,noexec,size=4m --tmpfs /unsupported:rw,nosuid,size=8m \
  --mount "type=volume,src=$volume_name,dst=/profiles" --mount "type=bind,src=$binary_dir,dst=/inputs,readonly" \
  --env LOADSTAND_LINUX_NATIVE=1 --env LOADSTAND_NATIVE_HELPER=/profiles/fixtures/linuxhelper --env LOADSTAND_NATIVE_TARGET=/profiles/fixtures/sdkedge \
  --env "LOADSTAND_NATIVE_HELPER_SHA=$helper_sha" --env "LOADSTAND_NATIVE_TARGET_SHA=$target_sha" \
  "$expected_image" /bin/sh -eu -c '
    mkdir -m 0755 /profiles/fixtures
    for name in loadstand.test linuxhelper sdkedge; do
      cp "/inputs/$name" "/profiles/fixtures/$name"
      chmod 0555 "/profiles/fixtures/$name"
      test "$(stat -c %u /profiles/fixtures/$name)" = 0
      test "$(stat -c %a /profiles/fixtures/$name)" = 555
    done
    test "$(stat -c %u /profiles)" = 0
    test ! -L /profiles
    test "$(stat -c %a /profiles)" = 755
    test ! -L /profiles/fixtures
    test "$(stat -c %u /profiles/fixtures)" = 0
    test "$(stat -c %a /profiles/fixtures)" = 755
    test ! -L /
    test "$(stat -c %u /)" = 0
    test "$(stat -c %a /)" = 755
    cd /profiles/fixtures
    sha256sum -c /inputs/sha256.expected
    exec ./loadstand.test -test.v -test.run "$1" -test.count=1 -test.timeout=60s
  ' corsa-owned "$native_selector" >/dev/null || exit $?
verify_container || exit 125
native_start_attempted=1
native_start_cli_rc=0
docker_bounded 75 start -a "$container_name" || native_start_cli_rc=$?
native_collector_rc=0
collect_outcome || native_collector_rc=$?
exit "$native_execution_rc"

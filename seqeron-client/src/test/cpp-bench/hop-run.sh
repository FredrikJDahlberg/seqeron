#!/usr/bin/env bash
# Hosted-façade hop: C++ direct against C++ hosted, interleaved, against three member JVMs and the prototype host JVM.
# Env: HOP_BENCH (hop_bench binary), SEQERON_JAR, HOP_TEST_CLASSES, HOP_RATES, HOP_COUNT, HOP_RUNS, HOP_LOG,
#      HOP_HOST_CPUS / HOP_BENCH_CPUS / HOP_MEMBER_CPUS (taskset lists, Linux only), HOP_HOST_OPTS (host JVM, e.g.
#      -Dhost.spin=true: a backing-off host adds 66-107 us at p50), HOP_MEMBER_OPTS.
# Linux: docker build -t seqeron-hop <this dir>; configure and build hop_bench into a volume (cmake -S /repo -B /build/out
# -DCMAKE_BUILD_TYPE=Release, Aeron and googletest sources copied there: Aeron's Gradle needs a writable tree); then
# docker run --shm-size=2g -v $PWD:/repo:ro -v hopbuild:/build -e HOP_BENCH=/build/out/hop_bench
#   -e SEQERON_JAR=/repo/build/libs/seqeron-<version>-uber.jar -e HOP_HOST_OPTS=-Dhost.spin=true seqeron-hop
#   bash /repo/seqeron-client/src/test/cpp-bench/hop-run.sh. Needs uberJar and :seqeron-service:testClasses built.
set -uo pipefail
REPO="${HOP_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)}"
MAIN="${REPO}/seqeron-service/src/main/scripts"
source "${MAIN}/ports.sh"; source "${MAIN}/paths.sh"; source "${MAIN}/seqeron-home.sh"
seqeron_require_jar; JAR="${SEQERON_JAR}"
BENCH="${HOP_BENCH:?}"
TESTCLASSES="${HOP_TEST_CLASSES:-${REPO}/seqeron-service/build/classes/java/test}"
LOG="${HOP_LOG:-${TMPDIR:-/tmp}/hop}"; RATES="${HOP_RATES:-5000 20000}"; COUNT="${HOP_COUNT:-60000}"; RUNS="${HOP_RUNS:-3}"
pin() { local cpus=$1; shift; if [[ -n "$cpus" ]]; then taskset -c "$cpus" "$@"; else "$@"; fi; }
rm -rf "$LOG"; mkdir -p "$LOG"
BASE="${TMP_DIR}/seqeron-hop"; rm -rf "$BASE"
JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
CLUSTER_HOSTS="$(cluster_hosts_string 3)"
for m in 0 1 2; do rm -rf "$(seqeron_aeron_dir $m)"; done
declare -a PIDS
for m in 0 1 2; do
  pin "${HOP_MEMBER_CPUS:-}" java "${JAVA_OPTS[@]}" ${HOP_MEMBER_OPTS:-} -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE" -Dsequencer.hosts="$CLUSTER_HOSTS" \
       -jar "$JAR" > "$LOG/seq-$m.log" 2>&1 &
  PIDS+=($!)
done
trap 'kill "${PIDS[@]}" 2>/dev/null; wait 2>/dev/null' EXIT
for m in 0 1 2; do wait_for_log "$LOG/seq-$m.log" "Running" 60 || { echo "seq $m not up"; exit 1; }; done
"${MAIN}/clusterctl.sh" load-topology "${REPO}/seqeron-service/src/test/resources/topology-test-hosted-quiet.xml" \
  > "$LOG/load.log" 2>&1 || { echo "load-topology failed"; cat "$LOG/load.log"; exit 1; }
for m in 0 1 2; do wait_for_log "$LOG/seq-$m.log" "serving replay" 60 || true; done
sleep 3
L=$(grep -h "isLeader=true" "$LOG"/seq-*.log | grep -oE 'SequencerService/[0-9]+' | head -1 | cut -d/ -f2)
echo "leader member $L"
DIR="$(seqeron_aeron_dir "$L")"
pin "${HOP_HOST_CPUS:-}" java "${JAVA_OPTS[@]}" ${HOP_HOST_OPTS:-} -Dprobe.memberId="$L" -Dprobe.aeronDir="$DIR" -Dprobe.sourceId=16 \
  -Dprobe.clientId=31 -Dprobe.snapshotDir="$BASE/hostsnap" -Dhost.clientTimeoutMs=2000 \
  -cp "$JAR:$TESTCLASSES" org.limitless.seqeron.tools.HostedTestApplicationHost > "$LOG/host.log" 2>&1 &
PIDS+=($!)
wait_for_log "$LOG/host.log" "host ready" 60 || { echo "host not up"; exit 1; }
for rate in $RATES; do
  for run in $(seq 1 "$RUNS"); do
    for arm in direct hosted; do
      pin "${HOP_BENCH_CPUS:-}" "$BENCH" $arm --member "$L" --aeron-dir "$DIR" --rate "$rate" --count "$COUNT" \
        --client-id 11 --egress-port 9230 2>&1 | tee -a "$LOG/results.log" | grep -E "^arm=|FENCED|failed|no ATT" || echo "$arm $rate run $run: no result"
      sleep 2
      grep -h "queue:" "$LOG/host.log" | tail -1 | sed "s/^/    host /"
      grep -c CLIENT_WINDOW "$LOG/host.log" | sed "s/^/    CLIENT_WINDOW lines: /"
    done
  done
done

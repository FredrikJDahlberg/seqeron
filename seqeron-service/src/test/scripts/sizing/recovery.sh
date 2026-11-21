#!/usr/bin/env bash
# Restart time against a long log (doc/plans/low-rate-sizing-plan.md, step 3). A restart replays the whole log,
# so its time grows with the log, not with the rate. This writes FRAMES synthetic frames at full speed, then
# kills a follower and times its restart until its queue is current, with the CPU (cpu_s: the restarted container's
# cgroup is new, so it counts from the restart) and memory it used.
# Run it at two sizes and extrapolate: the replay is linear in the log.
#
#   FRAMES=5000000 ./recovery.sh
#
# Environment (defaults in brackets): FRAMES [5000000], FILLER_BYTES [100], CPUS [2], HEAP [512m], SHM [1g],
# MEM [3g], THREADING [SHARED], OUT [build/sizing/recovery-<timestamp>].
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

FRAMES="${FRAMES:-5000000}"
FILLER_BYTES="${FILLER_BYTES:-100}"
CPUS="${CPUS:-2}"
HEAP="${HEAP:-512m}"
SHM="${SHM:-1g}"
MEM="${MEM:-3g}"
THREADING="${THREADING:-SHARED}"
OUT="${OUT:-${SIZING_ROOT}/build/sizing/recovery-$(date +%Y%m%d-%H%M%S)}"

mkdir -p "${OUT}"
trap cluster_down EXIT
cluster_down
sizing_env "${CPUS}" "${HEAP}" "${MEM}" "${SHM}" "${THREADING}"
cluster_up

leader="$(wait_leader -1 30)"
victim=$(( (leader + 1) % MEMBERS ))
loader=$(( (victim + 1) % MEMBERS ))

echo "== writing ${FRAMES} frames of ${FILLER_BYTES} bytes through member ${loader}"
written="$(now_ms)"
probe "${loader}" submit "probe.count=${FRAMES}" "probe.fillerBytes=${FILLER_BYTES}" >"${OUT}/submit.log" 2>&1
write_s=$(( ($(now_ms) - written) / 1000 ))
sleep 10
data_kb="$(sample "${victim}" | cut -d, -f11)"

echo "== killing member ${victim}, log ${data_kb} KiB"
docker kill -s KILL "node-${victim}" >/dev/null
sleep 12   # a restart within about 10 s of a SIGKILL fails on its active mark file (ops.md)
began="$(now_ms)"
docker start "node-${victim}" >/dev/null
until [[ "$(docker inspect -f '{{.State.Health.Status}}' "node-${victim}" 2>/dev/null)" == healthy ]]; do sleep 1; done
healthy_ms=$(( $(now_ms) - began ))
until probe "${victim}" ping >/dev/null 2>&1; do sleep 2; done
current_ms=$(( $(now_ms) - began ))
row="$(sample "${victim}")"

echo "frames,filler_bytes,write_s,log_kib,healthy_ms,current_ms,cpu_s,mem_peak_mib,anon_mib,shmem_mib" | tee "${OUT}/summary.csv"
echo "${FRAMES},${FILLER_BYTES},${write_s},${data_kb},${healthy_ms},${current_ms},$(awk -v u="$(cut -d, -f3 <<<"${row}")" 'BEGIN { printf "%.1f", u / 1e6 }'),$(( $(cut -d, -f7 <<<"${row}") / 1048576 )),$(( $(cut -d, -f8 <<<"${row}") / 1048576 )),$(( $(cut -d, -f9 <<<"${row}") / 1048576 ))" | tee -a "${OUT}/summary.csv"
docker logs "node-${victim}" >"${OUT}/node-${victim}.log" 2>&1 || true
echo "results: ${OUT}/summary.csv"

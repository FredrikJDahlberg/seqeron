#!/usr/bin/env bash
# CPU and memory sweep at a production message rate (doc/plans/low-rate-sizing-plan.md, step 1). One cell per
# combination of CPUS, HEAPS and SHMS: a fresh three-member cluster under those limits carries RATE msg/s for
# DURATION seconds, then BURST_RATE msg/s for BURST_SECONDS, while the members' cgroups are sampled. The same
# script is the soak: one cell, a long DURATION.
#
#   CPUS="1 2 3 4" ./sweep.sh
#   CPUS=2 HEAPS=512m DURATION=7200 SAMPLE_SECONDS=60 ./sweep.sh      # soak
#
# Environment (defaults in brackets): CPUS [1 2 3 4], HEAPS [512m], SHMS [1g], MEM_EXTRA [1g] (memory limit =
# heap + shm + MEM_EXTRA, the JVM's off-heap and the page cache's headroom), THREADING [SHARED], RATE [100],
# DURATION [300], BURST_RATE [2000], BURST_SECONDS [30], SAMPLE_SECONDS [5], OUT [build/sizing/<timestamp>].
#
# Writes OUT/summary.csv, one row per cell, and OUT/<cell>/samples.csv. A cell passes when the order check
# passed, no election happened, and no member was CPU-throttled.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

CPUS="${CPUS:-1 2 3 4}"
HEAPS="${HEAPS:-512m}"
SHMS="${SHMS:-1g}"
MEM_EXTRA="${MEM_EXTRA:-1g}"
THREADING="${THREADING:-SHARED}"
RATE="${RATE:-100}"
DURATION="${DURATION:-300}"
BURST_RATE="${BURST_RATE:-2000}"
BURST_SECONDS="${BURST_SECONDS:-30}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-5}"
OUT="${OUT:-${SIZING_ROOT}/build/sizing/$(date +%Y%m%d-%H%M%S)}"

# to_mib <size>: 512m, 1g → MiB.
to_mib() {
    case "$1" in
        *g) echo $(( ${1%g} * 1024 )) ;;
        *m) echo "${1%m}" ;;
        *) echo "unsupported size $1" >&2; return 1 ;;
    esac
}

# load <rate> <seconds> <log>: the order-checked load on member 0's driver; returns its exit status.
load() {
    probe 0 confirm "probe.count=$(( $1 * $2 ))" "probe.pacingMicros=$(( 1000000 / $1 ))" \
        "probe.fillerBytes=${FILLER_BYTES:-100}" >"$3" 2>&1
}

# csv_col <csv> <member> <column>: the column's value, for one member.
csv_col() { awk -F, -v m="$2" -v c="$3" '$2 == m { print $c }' "$1"; }

mkdir -p "${OUT}"
trap cluster_down EXIT
echo "cpus,heap,shm,mem,threading,rate,seconds,order_ok,elections,throttled_periods,throttled_ms,cpu_cores_avg,cpu_cores_max,working_set_mib_max,gc_max_ms,pass" >"${OUT}/summary.csv"

for cpus in ${CPUS}; do
    for heap in ${HEAPS}; do
        for shm in ${SHMS}; do
            mem="$(( $(to_mib "${heap}") + $(to_mib "${shm}") + $(to_mib "${MEM_EXTRA}") ))m"
            cell="cpu${cpus}-heap${heap}-shm${shm}"
            mkdir -p "${OUT}/${cell}"
            echo "== ${cell} (memory limit ${mem})"

            cluster_down
            sizing_env "${cpus}" "${heap}" "${mem}" "${shm}" "${THREADING}"
            cluster_up
            changes_before="$(leadership_changes)"
            sample_all >"${OUT}/${cell}/start.csv"
            samples="${OUT}/${cell}/samples.csv"
            echo "${SAMPLE_HEADER}" >"${samples}"

            started="$(now_ms)"
            ok=0
            load "${RATE}" "${DURATION}" "${OUT}/${cell}/steady.log" &
            steady=$!
            while kill -0 "${steady}" 2>/dev/null; do
                sample_all >>"${samples}"
                sleep "${SAMPLE_SECONDS}"
            done
            wait "${steady}" || ok=1
            if (( BURST_SECONDS > 0 )); then
                load "${BURST_RATE}" "${BURST_SECONDS}" "${OUT}/${cell}/burst.log" &
                burst=$!
                while kill -0 "${burst}" 2>/dev/null; do
                    sample_all >>"${samples}"
                    sleep "${SAMPLE_SECONDS}"
                done
                wait "${burst}" || ok=1
            fi
            seconds=$(( ($(now_ms) - started) / 1000 ))
            sample_all >"${OUT}/${cell}/end.csv"

            elections=$(( $(leadership_changes) - changes_before ))
            throttled_periods=0; throttled_us=0; cpu_avg=0; cpu_max=0; ws_max=0; gc_max=0
            for (( m = 0; m < MEMBERS; m++ )); do
                s="$(csv_col "${OUT}/${cell}/start.csv" "${m}" 3)"
                e="$(csv_col "${OUT}/${cell}/end.csv" "${m}" 3)"
                cores="$(awk -v s="${s}" -v e="${e}" -v t="${seconds}" 'BEGIN { printf "%.3f", (e - s) / 1e6 / t }')"
                cpu_avg="$(awk -v a="${cpu_avg}" -v c="${cores}" 'BEGIN { printf "%.3f", a + c / 3 }')"
                cpu_max="$(awk -v a="${cpu_max}" -v c="${cores}" 'BEGIN { print (c > a) ? c : a }')"
                throttled_periods=$(( throttled_periods + $(csv_col "${OUT}/${cell}/end.csv" "${m}" 4) - $(csv_col "${OUT}/${cell}/start.csv" "${m}" 4) ))
                throttled_us=$(( throttled_us + $(csv_col "${OUT}/${cell}/end.csv" "${m}" 5) - $(csv_col "${OUT}/${cell}/start.csv" "${m}" 5) ))
                peak="$(awk -F, -v m="${m}" '$2 == m && NR > 1 { w = ($8 + $9) / 1048576; if (w > x) x = w } END { printf "%d", x }' "${samples}")"
                (( peak > ws_max )) && ws_max="${peak}"
                gc="$(gc_max_ms "${m}")"
                gc_max="$(awk -v a="${gc_max}" -v g="${gc:-0}" 'BEGIN { print (g > a) ? g : a }')"
            done
            pass=0
            (( ok == 0 && elections == 0 && throttled_periods == 0 )) && pass=1
            echo "${cpus},${heap},${shm},${mem},${THREADING},${RATE},${seconds},$(( 1 - ok )),${elections},${throttled_periods},$(( throttled_us / 1000 )),${cpu_avg},${cpu_max},${ws_max},${gc_max},${pass}" \
                | tee -a "${OUT}/summary.csv"
            docker logs node-0 >"${OUT}/${cell}/node-0.log" 2>&1 || true
            cluster_down
        done
    done
done
echo "results: ${OUT}/summary.csv"

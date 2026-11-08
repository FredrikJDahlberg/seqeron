# shellcheck shell=bash
# hop-common.sh — sourced by hop-matrix.sh and hop-knee.sh after common.sh: run_mode, one bench run. Meant to be sourced,
# not executed.

HOST_CLASS=org.limitless.seqeron.proxy.ClientProxy

# run_mode <mode> <leader> <rate> <count> <payload> — one bench run; prints its result line.
run_mode() {
    local mode="$1" id="$2" rate="$3" count="$4" payload="$5" host="${MEMBERS[$2]}" spin arm=hosted output extra=()
    case "${mode}" in
    direct)
        arm=direct
        extra=(--client-id 12 --payload "${payload}")
        ;;
    hosted-spin | hosted-backoff)
        spin=false
        [[ "${mode}" == hosted-spin ]] && spin=true
        node "${host}" start --cpus "${MULTIHOST_HOST_CPUS:-}" host "${HOST_CLASS}" \
            -Dproxy.memberId="${id}" -Dproxy.sourceId=16 -Dproxy.clientId=31 \
            -Dproxy.snapshotDir='{data}/snap' -Dproxy.spin="${spin}"
        extra=(--payload "${payload}")
        node "${host}" wait-log host "host ready" 60 || {
            echo "ERROR: the host on member ${id} not ready after 60 s" >&2
            return 1
        }
        ;;
    *)
        echo "ERROR: unknown mode ${mode}" >&2
        return 2
        ;;
    esac
    output="$(node "${host}" bench --cpus "${MULTIHOST_BENCH_CPUS:-}" "${arm}" --member "${id}" --rate "${rate}" \
              --count "${count}" "${extra[@]}")" || true
    [[ "${arm}" == hosted ]] && node "${host}" kill host
    grep '^arm=' <<< "${output}" || echo "FAILED: ${output//$'\n'/ | }"
}

# check_order <label> — digests each member's queue and compares them (hop-order-check.py). The cluster's 1 Hz heartbeat
# keeps adding frames, so when the members stopped at different frames they are digested again up to the last frame
# all hold. Prints the verdict as a "# order" line, and fails on a divergence or a gap.
check_order() {
    local dir id status=0 verdict upto=""
    dir="$(mktemp -d)"
    for pass in 1 2; do
        for id in 0 1 2; do
            MULTIHOST_DIGEST_UPTO="${upto:-9223372036854775807}" node "${MEMBERS[$id]}" queue-digest "${id}" \
                > "${dir}/${id}" &
        done
        wait || true
        verdict="$("${MH_DIR}/hop-order-check.py" "${dir}/0" "${dir}/1" "${dir}/2")" && status=0 || status=$?
        (( status == 3 )) || break
        upto="$(sed -n 's/.* min=\([0-9]*\) .*/\1/p' <<< "${verdict}")"
    done
    rm -rf "${dir}"
    echo "# order $1 ${verdict:-order=FAIL}"
    (( status == 0 ))
}

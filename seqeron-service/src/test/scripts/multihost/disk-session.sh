#!/usr/bin/env bash
# disk-session.sh — the measurements of doc/disk-load-plan.md, step 2a, in one launch (doc/ec2-tests.md, "The disk
# session"). Each part writes its results and fetched logs under <results>/disk-<stamp>/<part>:
#
#   knee            hop-knee.sh
#   knee-writeback  knee with the kernel's default writeback (node.sh writeback default)
#   knee-volume     knee with each member's archive in DISK_DEFAULT_VOLUME; skipped when it is unset
#   replay          replay-remote.sh with REPLAY_COLD=1, quiet and then under load
#
# The knees run direct only with 100-byte markers from 50k msg/s in 10 s steps, and the replays preload 4,000,000
# frames; HOP_MODES, KNEE_* and REPLAY_PRELOAD override them. The members need passwordless sudo, for the writeback
# and the cold replays' cache drops. The session ends by setting the image's even writeback on every member.
#
#   DISK_DEFAULT_VOLUME   a directory on each member on a gp3 volume at its default 125 MB/s   (default unset)
#   DISK_PARTS            the parts to run, space-separated                                    (default all, as above)
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 MULTIHOST_CLIENT=10.0.0.14 DISK_DEFAULT_VOLUME=/mnt/gp3-default \
#       ./disk-session.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

ALL_PARTS="knee knee-writeback knee-volume replay"
PARTS="${DISK_PARTS:-${ALL_PARTS}}"
SESSION="${MULTIHOST_RESULTS}/disk-$(date -u +%Y%m%dT%H%M%SZ)"
export HOP_MODES="${HOP_MODES:-direct}" KNEE_PAYLOADS="${KNEE_PAYLOADS:-100}" KNEE_START="${KNEE_START:-50000}" \
       KNEE_SECONDS="${KNEE_SECONDS:-10}" REPLAY_PRELOAD="${REPLAY_PRELOAD:-4000000}"

for name in ${PARTS}; do
    if [[ " ${ALL_PARTS} " != *" ${name} "* ]]; then
        echo "ERROR: unknown part ${name}; the parts are ${ALL_PARTS}" >&2
        exit 2
    fi
done

# writeback <default|even> — the kernel's writeback timers on every member host.
writeback() {
    local host
    for host in $(printf '%s\n' "${MEMBERS[@]}" | sort -u); do
        node "${host}" writeback "$1"
    done
}

# part <name> <script> [VAR=value…] — runs a multi-host script with those variables, its results under the part's.
part() {
    local name="$1" script="$2"
    shift 2
    mh_log "part ${name}: ${script} $*"
    env MULTIHOST_RESULTS="${SESSION}/${name}" "$@" "${MH_DIR}/${script}"
}

trap 'writeback even' EXIT

for name in ${PARTS}; do
    case "${name}" in
    knee)
        part "${name}" hop-knee.sh
        ;;
    knee-writeback)
        writeback default
        part "${name}" hop-knee.sh
        writeback even
        ;;
    knee-volume)
        if [[ -z "${DISK_DEFAULT_VOLUME:-}" ]]; then
            mh_log "part ${name}: skipped, DISK_DEFAULT_VOLUME is unset"
            continue
        fi
        part "${name}" hop-knee.sh MULTIHOST_ARCHIVE_DIR="${DISK_DEFAULT_VOLUME}"
        ;;
    replay)
        for during in 0 1; do
            part "${name}" replay-remote.sh REPLAY_COLD=1 REPLAY_LOAD_DURING="${during}"
        done
        ;;
    esac
done

mh_log "results under ${SESSION}"

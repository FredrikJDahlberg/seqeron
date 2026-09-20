# seqeron-home.sh — resolves the installation root and the uber jar for every launcher beside it.
# Meant to be sourced, not executed.
#
# Two layouts hold these scripts, and they are told apart by what sits next to them rather than by
# counting `..`: a distribution, where bin/ sits beside lib/, and this source checkout, where the
# scripts sit at seqeron-service/src/main/scripts and build/ is at the repo root above them.
# Counting was the old way and it failed silently — `cd ../../..` succeeds in any tree deep enough,
# so a script run from anywhere else resolved some unrelated directory as the root and then reported
# the jar missing from it.
#
#   SEQERON_HOME   installation root; set it to override the layout guess
#   SEQERON_JAR    the uber jar; set it to override SEQERON_HOME's lib/ or build/libs/

_seqeron_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${SEQERON_HOME:-}" ]]; then
    if [[ -d "${_seqeron_script_dir}/../lib" ]]; then
        SEQERON_HOME="$(cd "${_seqeron_script_dir}/.." && pwd)"
    elif [[ "${_seqeron_script_dir}" == */seqeron-service/src/main/scripts ]]; then
        # A checkout: the scripts are the node module's, and the uber jar every one of them resolves is
        # the root project's, so this climbs past the module to the repo root.
        SEQERON_HOME="$(cd "${_seqeron_script_dir}/../../../.." && pwd)"
    else
        echo "ERROR: cannot tell where seqeron is installed from ${_seqeron_script_dir}" >&2
        echo "       (expected a distribution's bin/ or a checkout's seqeron-service/src/main/scripts) — set SEQERON_HOME" >&2
        exit 1
    fi
fi
export SEQERON_HOME

# The JVM flags every launcher that loads Aeron passes, for its off-heap access.
SEQERON_JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

# Call from a script that launches Java; the ones that only signal or delete files do not.
# A checkout names its jar by the VERSION file, since build/libs keeps every version ever built; a
# distribution has no VERSION file and exactly one jar in lib/, so a second match there is an error.
seqeron_require_jar() {
    if [[ -z "${SEQERON_JAR:-}" ]]; then
        local version="*" dir jars=()
        if [[ -f "${SEQERON_HOME}/VERSION" ]]; then
            version="$(tr -d '[:space:]' < "${SEQERON_HOME}/VERSION")"
        fi
        for dir in lib build/libs; do
            jars=("${SEQERON_HOME}/${dir}"/seqeron-${version}-uber.jar)
            [[ -f "${jars[0]}" ]] && break
        done
        if (( ${#jars[@]} > 1 )); then
            echo "ERROR: more than one seqeron uber jar under ${SEQERON_HOME}/${dir}: ${jars[*]}" >&2
            echo "       — set SEQERON_JAR to the one to run" >&2
            exit 1
        fi
        SEQERON_JAR="${jars[0]}"
    fi

    if [[ -z "${SEQERON_JAR:-}" || ! -f "${SEQERON_JAR}" ]]; then
        echo "ERROR: no seqeron uber jar under ${SEQERON_HOME} (lib/ or build/libs/)" >&2
        echo "       — run: ./gradlew uberJar, or set SEQERON_JAR" >&2
        exit 1
    fi
    export SEQERON_JAR
}

# wait_for_log <log> <pattern> <seconds> — polls until <pattern> appears in <log>; returns 1 on timeout,
# so the caller decides whether that is fatal.
wait_for_log() {
    local log="$1" pattern="$2" deadline=$(( SECONDS + $3 ))
    until grep -q "${pattern}" "${log}" 2>/dev/null; do
        (( SECONDS < deadline )) || return 1
        sleep 0.5
    done
}

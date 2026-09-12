# seqeron-home.sh — resolves the installation root and the uber jar for every launcher beside it.
# Meant to be sourced, not executed.
#
# Two layouts hold these scripts, and they are told apart by what sits next to them rather than by
# counting `..`: a distribution, where bin/ sits beside lib/, and this source checkout, where the
# scripts sit at src/main/scripts beside build/. Counting was the old way and it failed silently —
# `cd ../../..` succeeds in any tree deep enough, so a script run from anywhere else resolved some
# unrelated directory as the root and then reported the jar missing from it.
#
#   SEQERON_HOME   installation root; set it to override the layout guess
#   SEQERON_JAR    the uber jar; set it to override SEQERON_HOME's lib/ or build/libs/

_seqeron_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${SEQERON_HOME:-}" ]]; then
    if [[ -d "${_seqeron_script_dir}/../lib" ]]; then
        SEQERON_HOME="$(cd "${_seqeron_script_dir}/.." && pwd)"
    elif [[ "${_seqeron_script_dir}" == */src/main/scripts ]]; then
        SEQERON_HOME="$(cd "${_seqeron_script_dir}/../../.." && pwd)"
    else
        echo "ERROR: cannot tell where seqeron is installed from ${_seqeron_script_dir}" >&2
        echo "       (expected a distribution's bin/ or a checkout's src/main/scripts) — set SEQERON_HOME" >&2
        exit 1
    fi
fi
export SEQERON_HOME

# Call from a script that launches Java; the ones that only signal or delete files do not.
# The jar is globbed rather than named: its version is build.gradle's, and hand-copying it into each
# launcher is how four scripts came to carry the same literal.
seqeron_require_jar() {
    if [[ -z "${SEQERON_JAR:-}" ]]; then
        local candidate
        for candidate in "${SEQERON_HOME}"/lib/seqeron-*-uber.jar \
                         "${SEQERON_HOME}"/build/libs/seqeron-*-uber.jar; do
            if [[ -f "${candidate}" ]]; then
                SEQERON_JAR="${candidate}"
                break
            fi
        done
    fi

    if [[ -z "${SEQERON_JAR:-}" || ! -f "${SEQERON_JAR}" ]]; then
        echo "ERROR: no seqeron uber jar under ${SEQERON_HOME} (lib/ or build/libs/)" >&2
        echo "       — run: ./gradlew uberJar, or set SEQERON_JAR" >&2
        exit 1
    fi
    export SEQERON_JAR
}

# paths.sh — canonical temporary-directory resolution shared by every seqeron launch/test
# script, alongside ports.sh. Meant to be sourced, not executed.
#
# macOS's $TMPDIR ends in a slash and Linux's /tmp does not, and $TMPDIR is unset in a normal
# RHEL login shell, so the "${TMPDIR}seqeron-seq" idiom this replaces was both an unbound-variable
# abort under `set -u` and, in its "${TMPDIR:-/tmp}" form, a silent wrong path (/tmpseqeron-seq)
# that the cluster never writes to. Join, never concatenate:
# the shell mirror of OrderGatewayConfig's new File(tmpdir, ...) and Env.hpp's joinPath.

TMP_DIR="${TMPDIR:-/tmp}"
TMP_DIR="${TMP_DIR%/}"

# Aeron's own default directory — what a driver or client that is given no explicit directory
# uses: /dev/shm/aeron-<user> where /dev/shm exists (Linux), ${TMPDIR:-/tmp}/aeron-<user>
# otherwise (macOS). Mirrors aeron_fileutil.c's aeron_default_path and CommonContext.getAeronDirectoryName.
# The launchers pass this to the standalone aeronmd so it and any fallback client agree, and so
# the `rm -rf` cleanups purge the directory a run actually left behind (§2e).
aeron_default_dir() {
    if [[ -d /dev/shm ]]; then
        echo "/dev/shm/aeron-$(whoami)"
    else
        echo "${TMP_DIR}/aeron-$(whoami)"
    fi
}

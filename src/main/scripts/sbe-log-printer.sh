#!/usr/bin/env bash
# sbe-log-printer.sh — dump an Aeron Archive recording as JSON using an SBE schema.
#
# Reads archive.catalog + segment files under the given archive directory
# (see start-cluster.sh / SequencerServer, default $TMPDIR/phixeron-seq/archive-<id>)
# and prints every recorded SBE message as JSON.
#
# The schema is one of the IR files packaged in the uber jar: --schema sequenced
# (the tap, the default), --schema unsequenced (cluster ingress) or --schema cluster
# (the Raft consensus log on stream 100). --spec <file> decodes against an IR file
# outside the jar instead.
#
# sbe-cluster.xml is a trimmed mirror of io.aeron.cluster.codecs — only what the C++
# cluster client needs — so on the Raft log the consensus-module events it does not
# define (TimerEvent, SessionOpenEvent/SessionCloseEvent, NewLeadershipTermEvent, …)
# print as "<not in schema>" with their template id. SessionMessageHeader, the
# envelope around every ingress message, does decode.
#
# By default every recording in the catalog is dumped, not just the one matching
# the spec. The archive holds both the sequenced tap (stream 205, schema 202) and
# the cluster log (schema 111), so with the sequenced spec the cluster-log
# recording fails to decode and is reported on stderr as
#   Exception parsing segment .../<id>-0.rec: Required schema id 202 but was 111
# That is expected: the scan skips it and continues with the next recording.
#
# --stream <id> selects the newest recording on that stream instead. Prefer it
# for the tap: a node restart mints a new tap recording that replays the whole
# history from globalSeqNo 1, so older ones are strict prefixes of the newest and
# dumping them all just repeats the same history.
#
# The cluster does not need to be stopped — an in-progress recording is
# printed up to whatever has been written so far.
#
# --oneline collapses each message onto a single line instead of the default
# multi-line pretty print — easier to grep, and compact enough that a whole
# recording scrolls.
#
# Usage:
#   ./sbe-log-printer.sh [--schema <name>|--spec <file.sbeir>] <archive-dir> [--stream <id>] [--oneline]
#
# Example:
#   ./gradlew uberJar
#   ./sbe-log-printer.sh "${TMPDIR}phixeron-seq/archive-0" --stream 205

set -euo pipefail

usage() {
    echo "Usage: $0 [--schema <name>|--spec <file.sbeir>] <archive-dir> [--stream <id>] [--oneline]"
    echo "  --schema <name>  bundled schema to decode against (default sequenced): sequenced, unsequenced, cluster"
    echo "  --spec <file>    decode against an IR file outside the jar instead"
    echo "  --stream <id>    dump only the newest recording on that stream"
    echo "  --oneline        print each message as a single line of JSON"
    echo "  --list-schemas   list the bundled schema names and exit"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ $# -lt 1 ]]; then
    usage >&2
    exit 1
fi

JAR="build/libs/phixeron-0.1.0-uber.jar"

# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────
# Argument validation (including the archive dir) is the printer's; it holds the option table.

exec java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    -cp "${JAR}" org.limitless.phixeron.tools.SbeLogPrinter "$@"

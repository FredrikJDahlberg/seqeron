#!/usr/bin/env bash
# sbe-log-printer.sh — dump an Aeron Archive recording as JSON using an SBE schema.
#
# Reads archive.catalog + segment files under the given archive directory
# (see start-cluster.sh / SequencerNode, default $TMPDIR/phixeron-seq/archive-<id>)
# and prints every recorded SBE message as JSON, decoded against the given
# SBE IR (.sbeir) spec file.
#
# Every recording in the catalog is dumped, not just the one matching the spec.
# The archive holds both the sequenced tap (stream 205, schema 202) and the
# cluster log (schema 111), so with the sequenced spec the cluster-log recording
# fails to decode and is reported on stderr as
#   Exception parsing segment .../<id>-0.rec: Required schema id 202 but was 111
# That is expected: the scan skips it and continues with the next recording.
#
# The cluster does not need to be stopped — an in-progress recording is
# printed up to whatever has been written so far.
#
# Usage:
#   ./sbe-log-printer.sh <spec.sbeir> <archive-dir>
#
# Example:
#   ./gradlew uberJar generateSequencedSbe
#   ./sbe-log-printer.sh build/generated/sources/sbe/main/java/sbe-sequenced.sbeir \
#       "${TMPDIR}phixeron-seq/archive-0"

set -euo pipefail

usage() {
    echo "Usage: $0 <spec.sbeir> <archive-dir>"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ $# -lt 2 ]]; then
    usage >&2
    exit 1
fi

SPEC="$1"
ARCHIVE_DIR="$2"
JAR="build/libs/phixeron-0.1.0-uber.jar"

# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
fi

if [[ ! -f "${SPEC}" ]]; then
    echo "ERROR: ${SPEC} not found — run: ./gradlew generateSequencedSbe" >&2
    exit 1
fi

if [[ ! -d "${ARCHIVE_DIR}" ]]; then
    echo "ERROR: ${ARCHIVE_DIR} is not a directory" >&2
    exit 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────

exec java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    -cp "${JAR}" org.limitless.phixeron.tools.SbeLogPrinter "${SPEC}" "${ARCHIVE_DIR}"

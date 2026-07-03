#!/usr/bin/env bash
# logprint.sh — dump an Aeron Archive recording as JSON using an SBE schema.
#
# Reads archive.catalog + segment files under the given archive directory
# (see cluster.sh / SequencerNode, default /tmp/phixeron-seq/archive-<id>)
# and prints every recorded SBE message as JSON, decoded against the given
# SBE IR (.sbeir) spec file.
#
# The cluster does not need to be stopped — an in-progress recording is
# printed up to whatever has been written so far.
#
# Usage:
#   ./logprint.sh <spec.sbeir> <archive-dir>
#
# Example:
#   ./gradlew generateSbe
#   ./logprint.sh build/generated/sources/sbe/main/java/sequencer.sbeir /tmp/phixeron-seq/archive-0

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
    echo "ERROR: ${SPEC} not found — run: ./gradlew generateSbe" >&2
    exit 1
fi

if [[ ! -d "${ARCHIVE_DIR}" ]]; then
    echo "ERROR: ${ARCHIVE_DIR} is not a directory" >&2
    exit 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────

exec java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    -cp "${JAR}" org.limitless.phixeron.tool.SbeLogPrinter "${SPEC}" "${ARCHIVE_DIR}"

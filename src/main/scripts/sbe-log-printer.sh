#!/usr/bin/env bash
# sbe-log-printer.sh — dump an Aeron Archive recording as JSON using an SBE schema.
#
# Reads archive.catalog + segment files under the given archive directory
# (see start-cluster.sh / SequencerServer, default $TMPDIR/seqeron-seq/archive-<id>)
# and prints every recorded SBE message as JSON.
#
# Every IR file packaged in the jar is loaded by default — frame (the
# envelope and core, on the tap, stream 205), order/session/basicdata (the
# payloads inside it), replay (the node-local control plane) and cluster (the Raft
# consensus log, stream 100) — and each frame is decoded against the schema its
# own header names. The set is whatever is on the classpath, not a list in the
# printer: each module stages its own schemas' IR, so it is the deployment that
# decides which payloads can be named. --list-schemas prints what a run has. So one run reads an archive dir end to end, whichever mix of
# recordings it holds. --schema <name> narrows the run to one of them;
# --spec <file> decodes against an IR file outside the jar instead.
#
# A frame the run cannot decode is labelled and skipped, never fatal:
#   "<schema N not loaded>"  — its schema was excluded by --schema/--spec
#   "<not in schema>"        — its template is absent from that schema
#
# By default every recording in the catalog is dumped, not just one.
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
# -o <payloadId> writes that protocol's payloads to stdout, raw and back to back,
# for piping to a decoder that owns the schema (doc/seqeron-protocol-spec.md
# §13.1). The stream carries no framing of its own — the decoder's schema is what
# delimits each payload — so every text line, the dump included, moves to stderr
# for the run.
#
# Usage:
#   ./sbe-log-printer.sh [--schema <name>|--spec <file.sbeir>] <archive-dir> [--stream <id>] [--oneline] [-o <payloadId>]
#
# Examples:
#   ./gradlew uberJar
#   ./sbe-log-printer.sh "${TMPDIR:-/tmp}/seqeron-seq/archive-0" --stream 205
#   ./sbe-log-printer.sh "${TMPDIR:-/tmp}/seqeron-seq/archive-0" --stream 205 -o 2 \
#       2>frames.log | order-decode

set -euo pipefail

usage() {
    echo "Usage: $0 [--schema <name>|--spec <file.sbeir>] <archive-dir> [--stream <id>] [--oneline] [-o <payloadId>]"
    echo "  --schema <name>  decode only this schema (default: every bundled one)"
    echo "  --spec <file>    decode against an IR file outside the jar instead"
    echo "  --stream <id>    dump only the newest recording on that stream"
    echo "  --oneline        print each message as a single line of JSON"
    echo "  --list-schemas   list the bundled schema names and exit"
    echo "  -o <payloadId>   write that protocol's payloads to stdout, raw and back to back,"
    echo "                   for piping to its own decoder; every text line moves to stderr"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ $# -lt 1 ]]; then
    usage >&2
    exit 1
fi

# SEQERON_JAR overrides, as it does for clusterctl.sh and the metrics scripts. Which IR the run has in
# front of it is the classpath's, not the printer's (doc/future-arch.md §11 step 7a.4): the uber jar holds
# every module's and names every payload, the cluster tier's own artifact holds three and names none.
JAR="${SEQERON_JAR:-build/libs/seqeron-0.1.0-uber.jar}"

# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────
# Argument validation (including the archive dir) is the printer's; it holds the option table.

exec java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    -cp "${JAR}" org.limitless.seqeron.tools.SbeLogPrinter "$@"

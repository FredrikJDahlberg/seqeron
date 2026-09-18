---
paths:
  - "src/main/scripts/**"
---

# Operator scripts

`src/main/scripts` holds the operator and cluster-lifecycle scripts; `ports.sh` (the port formula, mirrored
by `PortLayout`, both languages), `paths.sh` and `seqeron-home.sh` are sourced by every other script.

**They do not resolve paths relative to the repository root.** `seqeron-home.sh` sets `SEQERON_HOME` by
recognising which layout it is in — a distribution, where `bin/` sits beside `lib/`, or this checkout, where
the scripts sit at `src/main/scripts` — and `seqeron_require_jar` then globs the uber jar out of `lib/` or
`build/libs`. Both are overridable (`SEQERON_HOME`, `SEQERON_JAR`). That replaced a
`REPO_ROOT="${SCRIPT_DIR}/../../.."` in four scripts, which was wrong silently rather than loudly (`cd`
up three succeeds in any tree deep enough) and carried the version literal `0.1.0` in each of them.
`./gradlew operatorDist` lays the distribution out under `build/install/seqeron`, `operatorDistZip`
archives it, and the application plugin's own `distZip`/`distTar`/`installDist` are disabled so there is
one answer to how seqeron is installed.

`sbe-log-printer.sh` puts a whole deployment's IR in front of `SbeLogPrinter` (`SEQERON_JAR` picks the
jar); `-o <payloadId>` — the spec §13.1 pipe — is the wrapper's only, because Gradle re-encodes a child's
stdout and would corrupt the payload bytes.

`stop-cluster.sh` stops everything either start script launched, plus whatever
`SEQERON_EXTRA_PROCESSES` names (`"label|pgrep-pattern"` entries, semicolon-separated), so a consumer's
harness can clean up its own processes through the same sweep without core naming them.

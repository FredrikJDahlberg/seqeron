# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

phixeron is an Aeron Cluster–based sequencer for a FIX gateway: a Java Raft cluster service
assigns a global total order to inbound FIX messages, and a C++ edge process bridges real FIX
TCP sessions to that cluster. It depends on a sibling project, **simdfix**
(`git@github.com:FredrikJDahlberg/simdfix.git`, fetched via CMake `FetchContent`), which provides
the generic FIX wire-format codec, session state machine base classes, and the code generator
used to turn `fix-session.xml`/`fix-application.xml` into C++ FIX message headers. phixeron
generates its own copy of those headers (see "SBE / FIX code generation" below) rather than
reusing simdfix's test fixtures.

## Build

### C++
```bash
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug      # AddressSanitizer + coverage
cmake --build cmake-build-debug

cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release
```
C++23, requires Java (Runtime) on PATH for the SBE tool and the code generator, and a
`git@github.com:...` SSH remote reachable for the simdfix FetchContent clone.

Executables: `fix_session_client`, `application_stream_client`, `fix_test_server`,
`phixeron_tests` (GoogleTest). Build a single target with `cmake --build cmake-build-debug --target <name>`.

### Java
```bash
./gradlew compileJava
./gradlew uberJar              # fat jar: build/libs/phixeron-<version>-uber.jar
./gradlew generateUnsequencedSbe generateSequencedSbe   # regenerate SBE Java codecs (also runs on compileJava)
```
There is no JUnit suite on the Java side — all automated tests are the C++ GoogleTest suite.

## Tests

```bash
cd cmake-build-debug && cmake --build . --target run_tests   # preferred
```
**Do not use plain `ctest`** here: simdfix's own `gtest_discover_tests` test suite is registered
too (its targets are `EXCLUDE_FROM_ALL` and never get built in this project), so `ctest` reports
those as spurious `..._NOT_BUILT` failures alongside phixeron's real results. If you must use
`ctest`, filter them out: `ctest --output-on-failure -E "_NOT_BUILT"`.

Run a single test: `./cmake-build-debug/phixeron_tests --gtest_filter='ClusterIngressHandler*'`
(GoogleTest name-filter syntax; test suite/case names are visible in the `ctest`/`run_tests` output).

## Architecture

### Data flow
```
FIX client (TCP) ⇄ fix_session_client (C++)  ⇄  Aeron Cluster (Java, Raft-replicated)
                                                        │
                                       global sequenced stream (multicast, archived)
                                                        │
                          ┌─────────────────────────────┼─────────────────────────────┐
                          ▼                             ▼                             ▼
                 fix_session_client            application_stream_client      RiskEngineClient (Java)
              (delivers ExecutionReports        (app messages only, no          (tracks positions from
               back to the originating           lifecycle events)              fills, answers
               TCP client)                                                      PortfolioQueryRequest)
```
`fix_test_server` (C++, under `src/test/cpp/.../session/`) is a standalone FIX TCP client used to
drive the whole pipeline end-to-end (Logon → Heartbeat → NewOrderSingle → Logout, plus a direct
cluster-ingress risk-query test) — see README.md for the full runbook and port table.

### Aeron Cluster sequencer (Java) — `SequencerNode` / `SequencerService`
`SequencerService` (`ClusteredService`) is the replicated state machine: every ingress message
gets a cluster-wide monotone `globalSeqNo` plus the Raft consensus timestamp, then is
republished on the **global stream** (`GLOBAL_STREAM_CHANNEL` = `224.0.1.1:9200` multicast, stream
1), which is simultaneously recorded by the co-located Aeron Archive so clients can replay full
history on (re)connect. Only the current leader publishes; all nodes keep identical sequencing
state so a new leader resumes exactly where the last one left off. Snapshots are a single
little-endian `int64 globalSeqNo`.

The key trick making this cheap: ingress messages arrive already SBE-encoded as
`sbe-unsequenced.xml` (schema 200), and `sbe-sequenced.xml` (schema 202) is deliberately kept
byte-identical past the shared `header` composite (same field order/types/ids, same var-data
layout). `onSessionMessage` therefore only ever decodes the outer `MessageHeader` + `header`
composite and copies everything else through as opaque bytes — it never needs to know about
individual FIX message types.

### C++ FIX gateway — `fix_session_client` / `FixSessionClient.cpp`
Deliberately stateless proxy: authoritative FIX session state (sequence numbers, session status)
lives in the cluster, not in this process, so it can crash and restart without losing anything.
Three cooperating pieces:
- **`ClusterIngressSender`** — the Aeron Cluster client session state machine
  (`SessionConnectRequest → SessionEvent(OK) → send/keep-alive → SessionCloseRequest`), talking to
  the cluster only through an `IngressTransport`/`EgressTransport` seam so tests can substitute
  in-memory fakes.
- **`ClusterIngressHandler`** — pure byte-level logic: FIX frame/tag parsing helpers, SBE
  encode/decode, and application-message routing, built on top of `ClusterIngressSender`.
- **`GlobalStreamClient`** — replays the archived global stream from a given position, then
  follows it live; used identically by `fix_session_client`, `application_stream_client`, and
  `fix_test_server`.

`src/main/cpp/.../session/` (`Session`, `ClientSession`, `ServerSession`, `ResendCache`) is a
role-agnostic (CRTP) FIX session-layer base shared with simdfix-generated message handlers —
sequence tracking, resend/gap-fill handling — independent of the Aeron Cluster plumbing above.

### SBE / FIX code generation
Three SBE schemas under `src/main/resources/`, each generating into a distinct namespace so one
include path covers all of them (`org.limitless.phixeron.{sbe.unsequenced, sbe.sequenced}`,
`org.limitless.phixeron.cluster.sbe`):
- `sbe-cluster.xml` — trimmed mirror of `io.aeron.cluster.codecs` (SessionConnectRequest,
  SessionEvent, SessionKeepAlive, …), replacing a hand-written `ClusterProtocol.hpp`.
- `sbe-unsequenced.xml` (schema 200) — every FIX message the gateway can receive, plus
  `sourceId`/`sessionId` identifying the submitting TCP connection/cluster session. Field/type
  definitions mirror the FIX wire format directly (see `fix-session.xml`/`fix-application.xml`
  below); there's no separate SBE schema per FIX layer.
- `sbe-sequenced.xml` (schema 202) — same messages, byte-identical past `header`, plus
  `globalSeqNo`/`timestamp` (see above).

Separately, `fix-session.xml` / `fix-application.xml` / `config.xml` are simdfix Generator input
(not SBE), producing the C++ FIX message encoders/decoders/handler dispatch
(`FixMessageHandler.hpp`, etc.) under `${CMAKE_BINARY_DIR}/phixeron_generated`. Deliberately
**not** simdfix's own generated-headers location, to avoid colliding with simdfix's own
(excluded-from-build) test-fixture generation.

Both the Java (`generateUnsequencedSbe`/`generateSequencedSbe` Gradle tasks) and C++
(`GenerateUnsequencedSbeCodecs`/`GenerateSequencedSbeCodecs`/`GenerateClusterSbeCodecs` CMake
targets) sides regenerate independently from the same XML — keep both in sync when editing a
schema.

### Risk engine — `RiskEngineClient` / `MockRiskEngine` (Java only; C++ port pending, see todo.md)
Replays the global stream to track per-account positions from `NewOrderSingle`/`ExecutionReport`
fills, answers `PortfolioQueryRequest` using `MockRiskEngine` (synchronous, 5-request concurrency
cap), and submits the reply back to cluster ingress. Throttling beyond 5 concurrent requests
works by leaving the request fragment unconsumed on the global stream until a slot frees, not by
blocking or dropping it.

### Known gaps
`todo.md` tracks known incomplete pieces (e.g. ExecutionReport→TCP routing in
`FixSessionClient.cpp` is stubbed, connection IDs aren't stable across gateway restarts, cluster
ingress doesn't reconnect after leader failover, no real ResendRequest replay backing store yet).
Check it before assuming a code path is complete.

`doc/` contains deeper background/design docs (`0-overview.md` … `6-detailed-architecture.md`)
for the larger target system this project implements a slice of (full buy-side/sell-side gateway
with a separate Application Engine, Risk Thread, and Egress process). They describe an aspirational
superset, not this repo's current state — cross-check against the source before trusting specifics.

## Code style

- **Mandatory bracing**: all C++ and Java `if`/`else`/`for`/`while`/`do` bodies must use braces,
  even single-statement ones. Never `if (cond) stmt;` — always `if (cond) { stmt; }`.
- **C++ namespace aliases**: SBE schema namespaces are aliased to short names —
  `usq` = `org::limitless::phixeron::sbe::unsequenced`, `seq` = `org::limitless::phixeron::sbe::sequenced`.
  Where a file also needs the `org::limitless::phixeron::sequencer` component namespace alongside
  `sbe::sequenced`, that one is aliased `sequencer` (not `seq`) to avoid colliding with the SBE alias.

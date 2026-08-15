# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

1. Think Before Coding

Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

State your assumptions explicitly. If uncertain, ask.
If multiple interpretations exist, present them - don't pick silently.
If a simpler approach exists, say so. Push back when warranted.
If something is unclear, stop. Name what's confusing. Ask.

2. Simplicity First

Minimum code that solves the problem. Nothing speculative.

No features beyond what was asked.
No abstractions for single-use code.
No "flexibility" or "configurability" that wasn't requested.
No error handling for impossible scenarios.
If you write 200 lines and it could be 50, rewrite it.
Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

3. Surgical Changes

Touch only what you must. Clean up only your own mess.

When editing existing code:

Don't "improve" adjacent code, comments, or formatting.
Don't refactor things that aren't broken.
Match existing style, even if you'd do it differently.
If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

Remove imports/variables/functions that YOUR changes made unused.
Don't remove pre-existing dead code unless asked.
The test: Every changed line should trace directly to the user's request.

4. Goal-Driven Execution

Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

"Add validation" → "Write tests for invalid inputs, then make them pass"
"Fix the bug" → "Write a test that reproduces it, then make it pass"
"Refactor X" → "Ensure tests pass before and after"
For multi-step tasks, state a brief plan:

1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
   Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.


## What this is

See the [Overview](README.md#overview) in README.md for a description of what phixeron is and
its relationship to the sibling **simdfix** project.

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

Executables: `FixGateway`, `OrderExecClient`, `fix_test_server`,
`phixeron_tests` (GoogleTest). Build a single target with `cmake --build cmake-build-debug --target <name>`.

### Java
```bash
./gradlew compileJava
./gradlew uberJar              # fat jar: build/libs/phixeron-<version>-uber.jar
./gradlew generateUnsequencedSbe generateSequencedSbe   # regenerate SBE Java codecs (also runs on compileJava)
```
```bash
./gradlew test                 # JUnit 5 unit tests for the Java state machines
```
The Java suite covers the deterministic decision-making — `Sequencer`, and `ReplayerService` through
its `Replayer` seam — and deliberately touches no Aeron runtime: no media driver, no
cluster, no Aeron mocks, so it runs in ~1s. Everything Aeron-shaped stays covered by the C++
GoogleTest suite and the end-to-end scripts in `src/test/scripts/`.

## Tests

```bash
cd cmake-build-debug && cmake --build . --target run_tests   # preferred
```
**Do not use plain `ctest`** here: simdfix's own `gtest_discover_tests` test suite is registered
too (its targets are `EXCLUDE_FROM_ALL` and never get built in this project), so `ctest` reports
those as spurious `..._NOT_BUILT` failures alongside phixeron's real results. If you must use
`ctest`, filter them out: `ctest --output-on-failure -E "_NOT_BUILT"`.

Run a single test: `./cmake-build-debug/phixeron_tests --gtest_filter='FixIngressHandler*'`
(GoogleTest name-filter syntax; test suite/case names are visible in the `ctest`/`run_tests` output).

## Architecture

### Data flow
```
FIX client (TCP) ⇄ FixGateway (C++)  ──output──▶  Aeron Cluster (Java, Raft-replicated)
                                                              │
                              IPC (aeron:ipc 205), sequenced frames — recorded on every node
                               live tap: read directly · history/gaps: replay via co-located ReplayerService
                                                              │
                                  ┌───────────────────────────┴───────────────────────────┐
                                  ▼                                                       ▼
                                  FixGateway                          OrderExecClient (C++)
                                (delivers ExecutionReports           (prints app messages, tracks
                                 back to the originating             positions from fills, answers
                                 TCP client)                          PortfolioQueryRequest)
```
`fix_test_server` (C++, under `src/test/cpp/.../session/`) is a standalone FIX TCP client used to
drive the whole pipeline end-to-end (Logon → Heartbeat → NewOrderSingle → Logout, plus a direct
cluster-ingress risk-query test) — see README.md for the full runbook and port table.

### Aeron Cluster sequencer (Java) — `SequencerNode` / `SequencerService` / `Sequencer`
`Sequencer` is the replicated state machine proper — it owns `globalSeqNo` and every frame encode,
has no Aeron dependency, and is unit-tested directly (`SequencerTest`). `SequencerService`
(`ClusteredService`) is its Aeron adapter: it decides *when* to call the sequencer and publishes
what comes back, holding no replicated state itself. Every ingress message
gets a cluster-wide monotone `globalSeqNo` plus the Raft consensus timestamp, then is
republished on the **node-local tap** (`FEEDER_CHANNEL` = `aeron:ipc`, `FEEDER_STREAM_ID` = 205),
which this node's co-located Aeron Archive records. Every frame on it is sequenced: all 21
messages in `sbe-sequenced.xml` carry the `header` composite, and the sequencer is the stream's only
publisher, so `globalSeqNo` + consensus `timestamp` are stamped on ingress messages and on the
lifecycle/tick/leadership frames it synthesizes alike.

**Every node publishes and records its own tap** — leader and follower alike. All nodes process the
same committed log in the same order and keep identical sequencing state (so a new leader resumes
exactly where the last one left off), which makes the taps byte-identical across nodes: each
archive independently holds complete history, with no cross-node replication. The tap publication
is created once in `onStart` and never re-created on a leadership change (`aeron:ipc` has no port
to collide on), so a node's recording is one continuous run spanning every leader tenure.

Consumers split live from history: co-located apps subscribe to the tap **directly** for the live
feed (untethered, so a slow app is dropped and heals via replay rather than back-pressuring), while
the co-located `ReplayerService` serves cold-start/gap replay off the same recording. `emit` is
reliable — it spins until the offer lands, since a dropped frame would be an unrecoverable hole —
and can only block on local-archive write back-pressure, because the recording is the tap's one
tethered subscriber. Reliable is not unbounded, though: `TapStallPolicy` watches the archive's
`RecordingPos` counter, and a node whose recording has stopped or stopped advancing **terminates
itself** (exit 70) rather than sequence history it cannot keep — peers keep quorum, and the restart
rebuilds its recording over the full-log replay it does anyway. The 1 Hz tick runs the same liveness
check, because a *stopped* recording back-pressures nothing at all (the untethered app subscribers
keep the publication connected) and would otherwise be silent. Note that no cluster callback may
signal failure by throwing: `Image.boundedControlledPoll` has already advanced the log position past
the message and `AgentRunner` keeps the agent alive, so a throw drops the frame and carries on.

> This replaced a UDP multi-destination-cast "global stream" (leader-only publisher, stream 1),
> retired in Phase 2 — see `doc/router-archive.md` and `doc/todo.md` items 1/2c. The tap's identity is
> `FEEDER_CHANNEL`/`FEEDER_STREAM_ID` on both sides: Java in `SequencerService`, C++ in
> `ClusterStreamClient.hpp` (the one definition of `FEEDER_STREAM_ID`) plus `ReplayerClient.hpp`'s
> `FEEDER_CHANNEL`, which addresses the same stream with the consumer-side `?tether=false` option.
> Named to pair with the `Replayer`: the **Feeder** stream is the live feed, the Replayer serves
> history off its recording. Older names for it (`GLOBAL_STREAM_ID`, `REPLAYER_STREAM_ID`,
> `REPLAYER_TAP_STREAM_ID`, `TAP_STREAM_ID`, `SEQUENCED_STREAM_ID`) are gone; they survive only in
> the dated entries in `doc/todo.md` and `doc/audit.md`.

Besides forwarded ingress, the sequencer synthesizes its own frames on the same `globalSeqNo`
counter: `ClientConnected`/`ClientDisconnected` (cluster session lifecycle), `LeadershipChanged`
(de-duplicated per leader), and a **1 Hz `Tick`** — the cluster clock, so consumers have a
consensus-driven time source that keeps advancing while a FIX session is silent, which is exactly
when the gateway's keepalive watchdog must probe (`TICK_INTERVAL_MS`).

**Snapshots are not supported**, and both `ClusteredService` hooks refuse: `onTakeSnapshot` throws,
and `onStart` refuses a snapshot image rather than restoring from one. `clusterctl shutdown` uses
`ABORT`, and recovery is always full-log replay from `globalSeqNo` 1. That is deliberate: replaying
the whole log is what keeps each node's tap recording complete and gap-free — a node restored from a
snapshot would record only from wherever it resumed. The cost is that recovery time and archive size
grow with uptime (the 1 Hz tick alone is ~86.4k frames/day) — see `doc/todo.md`.

The key trick making this cheap: ingress messages arrive already SBE-encoded as
`sbe-unsequenced.xml` (schema 200), and `sbe-sequenced.xml` (schema 202) is deliberately kept
byte-identical past the shared `header` composite (same field order/types/ids, same var-data
layout). `Sequencer.sequenceMessage` therefore only ever decodes the outer `MessageHeader` +
`header` composite and copies everything else through as opaque bytes — it never needs to know
about individual FIX message types.

### C++ FIX gateway — `FixGateway` / `FixGateway.cpp`
Deliberately stateless proxy: authoritative FIX session state (sequence numbers, session status)
lives in the cluster, not in this process, so it can crash and restart without losing anything.
Three cooperating pieces:
- **`ClusterStreamSender`** (`sequencer/`) — the Aeron Cluster client session state machine
  (`SessionConnectRequest → SessionEvent(OK) → send/keep-alive → SessionCloseRequest`), talking to
  the cluster only through an `IngressTransport`/`EgressTransport` seam so tests can substitute
  in-memory fakes.
- **`FixIngressHandler`** — pure byte-level logic: FIX frame/tag parsing helpers, SBE
  encode/decode, and application-message routing, built on top of `ClusterStreamSender`.
- **`ClusterStreamReceiver`** (`sequencer/`) / **`ReplayerStreamReceiver`** (`replayer/`) — follow the
  sequenced stream: replay history from a given position via the Replayer, then follow the tap live;
  used the same way by `FixGateway`, `OrderExecClient`, and `fix_test_server`.

`src/main/cpp/.../fix/` (`Session`, `ClientSession`, `ServerSession`, `ResendCache`) is a
role-agnostic (CRTP) FIX session-layer base shared with simdfix-generated message handlers —
sequence tracking, resend/gap-fill handling — independent of the Aeron Cluster plumbing above.

**Two unrelated things are both called a "session"**, and the distinction is load-bearing:
the **Aeron Cluster session** (`ClusterStreamSender::clusterSessionId()`, `header.sessionId`,
`SequencerService.onSessionClose`) and the **FIX session** (the classes just above, `m_sessions`,
`m_recoveredSessions`). There is **one cluster session per gateway process, carrying every FIX
session** — `FixIngressHandler` stamps the same `sessionId` on every message and tells connections
apart by `header.connectionId`. `ClientSession` unhelpfully names both (`io.aeron.cluster.service.ClientSession`
vs `org::limitless::phixeron::fix::ClientSession`).

**Fencing: the gateway stops serving TCP clients when it loses its place in the cluster.**
`FixGateway::fence()` closes the accept gate and drops every client socket on three signals — a
`GatewayActive` naming a sibling instance (it was superseded), the cluster closing its cluster session,
or no `Tick` from the co-located tap for `TAP_STALL_TIMEOUT_MS`. Without it a demoted primary kept
serving alongside the standby that replaced it, since `m_gateOpen` only ever latched true. It publishes
no `ClientDisconnected` — the fence deliberately looks to the cluster exactly like this process dying,
which is the state the recovery path is built for — and snapshots live FIX session state into
`m_recoveredSessions` on the way out. Being superseded keeps the cluster session, so that fence just
drops the instance back to standby and the gate can re-open on a later promotion; the two fences that
lose the session **exit the process**, because `connect()` runs only at startup and an instance with no
session could never be promoted again (the accept gate requires `isConnected()` — fail closed). A leader
failover is *not* session loss: `NewLeaderEvent` swaps the ingress publication and keeps the session id.
See `doc/todo.md` "Gateway HA / multi-instance".

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

### Order execution client — `OrderExecClient` (C++, under `src/main/cpp/.../order/OrderExecClient.cpp`)
Combines what used to be two separate binaries — `application_stream_client` and the C++
`RiskEngineClient` — into one. Replays the cluster stream then follows it live, printing every
`NewOrderSingle`/`ExecutionReport` it decodes (lifecycle events are filtered out), while also
tracking per-account positions from those same fills, answering `PortfolioQueryRequest` using
`MockRiskEngine` (under `src/main/cpp/.../risk/`, synchronous, 5-request concurrency cap), and
submitting the reply back to cluster ingress. Throttling beyond 5 concurrent requests works by
leaving the request fragment unconsumed on the cluster stream until a slot frees, not by blocking
or dropping it. The original Java `RiskEngineClient`/`MockRiskEngine` are dead code, already
removed (`src/main/java/org/limitless/phixeron/risk/` deleted).

### Exchange-facing FIX gateway — `ExchangeGateway` (Java, under `src/main/java/.../exchange/`)
The **venue** leg, and the only edge that is not simdfix: an [Artio](https://github.com/real-logic/artio)
**initiator** toward an exchange, added because the system had a client-facing acceptor and nothing facing
out. Additive — the C++/simdfix client gateway is untouched. See `doc/artio-integration.md` §13, which
records what the memo (written for replacing the *acceptor*) gets wrong about the mechanism.

The invariant is the same one the C++ edge holds: **nothing un-sequenced reaches the wire.** Artio owns
TCP, codecs, the session FSM and the timers — it decides *what* to send and *when* — but every decision
goes through `ClusterSessionProxy` (`isAsync() = true`) to cluster ingress as an opaque
`SessionProtocolMessage` (template 22, carrying pre-encoded FIX bytes), and reaches the venue only when it
comes back on the node tap, emitted by a `SessionWriter` at the `MsgSeqNum` the log recorded. Inbound venue
traffic is published too, so the log is a complete session record.

Three Artio 0.177 facts this depends on, each of which fails **silently** if got wrong:
- The writer must come from `FixLibrary.followerSession(...)`, **not** `sessionWriter(...)` — only the
  former is registered where Artio links it to the `Session`. An unlinked writer advances no
  `lastSentMsgSeqNum` and never fires `onSessionWriterLogout()`.
- That follower header needs its **comp-ids swapped** (the engine resolves it with `onAcceptLogon`, which
  reads local = `TargetCompID`; an initiator's key comes from `onInitiateLogon`). Get it wrong and the
  writer is registered under a different `sessionId`, never linked, and fills the local log while nothing
  reaches the venue.
- Binding and seeding happen in the `sessionAcquireHandler` (fires at **connect**), never on the `initiate`
  reply (completes only after logon) — the latter deadlocks.

`ReplayerStreamReceiver` (Java, `replayer/`) is a faithful port of the C++ client of the same name — same
protocol, same walk/resume/retain state machine. Keep the two in step. Identity is env config
(`PHIXERON_EXCHANGE_*`), not a BasicData row: `Sequencer` holds one `designatedPrimaryGatewayId`, so an
exchange row would hijack the client-facing election. Session layer only — no order flow, no standby.

```bash
./gradlew mockExchange                     # FIX acceptor standing in for the venue (port 9010)
./gradlew exchangeGateway                  # the gateway itself (needs a running cluster)
src/test/scripts/exchange-gateway-test.sh  # all-Java e2e; no C++ build needed
```
The e2e purges Artio's own log dir alongside `purgelog.sh` — the two hold the same session's sequence
numbers, and purging one alone trips the gateway's "sent-sequence disagreement" check.

### Reference-data gateway — `BasicDataClient` / `Gateways` (C++, under `src/main/cpp/.../basicdata/`)
Dual-role per-node process (`doc/basicdata-design.md`): on the **leader** it's a producer — reads
static reference data (FIX session comp-id pairs, gateway topology, the trading-day calendar;
currently hardcoded in `BasicDataConstants.hpp`, standing in for a real DB read) and publishes it as
ordinary sequenced `BasicData*` messages, an external-input adapter exactly like the FIX gateway is
for TCP. On **every node** it's a consumer — follows the co-located tap (like `OrderExecClient`
tracks positions) to build an identical in-memory reference-data view, giving reference data the
same node-loss fault tolerance as business state.

A load is a bracketed, fixed-order run: `StartBasicData(sectionCount=3)` → Gateway rows → session
rows → TradingDay rows → `EndBasicData`, each section's `remainingItems` counting down to 0 — the
completion contract that makes an interrupted load detectable and resumable (recovery replays the
same code path as first load, resuming at the first incomplete section). Gateways load first so a
gateway resolves its own `{gatewayId, gatewaySourceId}` (`Gateways::resolve`, keyed on the
launch-time `PHIXERON_FIX_GATEWAY_NAME`) before session rows arrive, letting it drop, on ingest,
every session a different logical gateway owns. `resolve()` returns `nullopt` on no match — callers
must fail closed rather than default to sourceId 0.

This is also where FIX session identity comes from at runtime: CompIDs are never hardcoded per
process — the gateway starts with none and resolves them from the BasicData SessionMap (ingress via
`resolveSession`, outbound via `handleLogon` → `applyResolvedIdentity`). Reference rows are static
for the trading day — no add/remove/re-point while a session is live.

### Known gaps
`todo.md` tracks known incomplete pieces (e.g. ExecutionReport→TCP routing in
`FixGateway.cpp` is stubbed, connection IDs aren't stable across gateway restarts, no real
ResendRequest replay backing store yet). Check it before assuming a code path is complete.

`doc/` contains deeper background/design docs (`0-overview.md` … `6-detailed-architecture.md`)
for the larger target system this project implements a slice of (full buy-side/sell-side gateway
with a separate Application Engine, Risk Thread, and Egress process). They describe an aspirational
superset, not this repo's current state — cross-check against the source before trusting specifics.

## Code Formatting Mandate
- Explicitly respect all style, brace, and indentation configurations found in the local `.clang-format` file.
- Before completing an edit or creating a file, ensure it complies with our Clang-Format criteria.
- Keep code comments short and to the point. Do not explain design that is already documented.
- 

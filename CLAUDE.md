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

Executables: `FixGateway`, `OrderExecServer`, `BasicDataServer`, `fix_test_server`,
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
                                  FixGateway                          OrderExecServer (C++)
                                (delivers ExecutionReports           (prints app messages, tracks
                                 back to the originating             positions from fills, answers
                                 TCP client)                          PortfolioQueryRequest)
```
`fix_test_server` (C++, `src/test/cpp/.../fix/FixTestServer.cpp`) is a standalone FIX TCP client used to
drive the whole pipeline end-to-end (Logon → Heartbeat → NewOrderSingle → Logout, plus a direct
cluster-ingress risk-query test) — see README.md for the full runbook and port table.

### Aeron Cluster sequencer (Java) — `SequencerServer` / `SequencerService` / `Sequencer`
`Sequencer` is the replicated state machine proper — it owns `globalSeqNo` and every frame encode,
has no Aeron dependency, and is unit-tested directly (`SequencerTest`). `SequencerService`
(`ClusteredService`) is its Aeron adapter: it decides *when* to call the sequencer and publishes
what comes back, holding no replicated state itself. Every ingress message
gets a cluster-wide monotone `globalSeqNo` plus the Raft consensus timestamp, then is
republished on the **node-local tap** (`FEEDER_CHANNEL` = `aeron:ipc`, `FEEDER_STREAM_ID` = 205),
which this node's co-located Aeron Archive records. Every frame on it is sequenced: every
message in `sbe-sequenced.xml` carries the `header` composite, and the sequencer is the stream's only
publisher, so `globalSeqNo` + consensus `timestamp` are stamped on ingress messages and on the
lifecycle/tick/leadership frames it synthesizes alike.

**Every node publishes and records its own tap** — leader and follower alike. All nodes process the
same committed log in the same order and keep identical sequencing state (so a new leader resumes
exactly where the last one left off), which makes the taps byte-identical across nodes: each
archive independently holds complete history, with no cross-node replication. The tap publication
is created once in `onStart` and never re-created on a leadership change (`aeron:ipc` has no port
to collide on), so a node's recording is one continuous run spanning every leader tenure.

The replay protocol has two sides and they live in two namespaces — **`replayer.server`** (Java only:
`ReplayerServer`/`ReplayerService` and their pure seams `Replayer`, `ReplaySlotAllocator`,
`ReplayRecordings`, `ReplayClientIdCollisions`) and **`replayer.client`** (`ReplayerStreamReceiver` and its
pure seam `ReplayerRecovery`, plus `RecoveryProgressPolicy`, `SequencedEvent` — Java, and C++ in
`org::limitless::phixeron::replayer::client`). The only edge across is client→server: the client reads
`ReplayerService`'s channel/stream-id constants, which are the wire contract between them. There is no
C++ replay server; the C++ side is all client.

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
> retired in Phase 2 — see `doc/audit.md` (finding S4, which it closed structurally). The tap's identity is
> `FEEDER_CHANNEL`/`FEEDER_STREAM_ID` on both sides: Java in `SequencerService`, C++ in
> `sequencer/SequencedFrame.hpp` (the one definition of `FEEDER_STREAM_ID`) plus
> `replayer/client/ReplayerStreamReceiver.hpp`'s
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
- **`ClusterStreamClient`** (`sequencer/`) / **`ReplayerStreamReceiver`** (`replayer/client/`) — follow the
  sequenced stream: replay history from a given position via the Replayer, then follow the tap live;
  used the same way by `FixGateway`, `OrderExecServer`, and `fix_test_server`. `ReplayerStreamReceiver` is
  the Aeron adapter only — subscriptions, the replay image, the clocks; every decision it makes about them
  lives in **`ReplayerRecovery`**, which holds none of them and is where the unit suite drives the
  walk/resume/gap state machine (`ReplayerRecoveryTest`, both languages).

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
`FixGateway::closeSessions()` closes the accept gate and drops every client socket on four signals — a
`GatewayActive` naming a sibling instance (it was superseded), the cluster closing its cluster session,
no `Tick` from the co-located tap for `TAP_STALL_TIMEOUT_MS`, or recovery dispatching nothing for
`RECOVERY_STALL_TIMEOUT_MS` (`GatewayRecoveryStallPolicy`). Without it a demoted primary kept
serving alongside the standby that replaced it, since `m_gateOpen` only ever latched true. It publishes
no `ClientDisconnected` — the fence deliberately looks to the cluster exactly like this process dying,
which is the state the recovery path is built for — and snapshots live FIX session state into
`m_recoveredSessions` on the way out. Being superseded keeps the cluster session, so that fence just
drops the instance back to standby and the gate can re-open on a later promotion; the other three
lose the session — the cluster closed it, or the two stall fences close it themselves so standby
promotion can take over — and **exit the process**, because `connect()` runs only at startup and an instance with no
session could never be promoted again (the accept gate requires `isConnected()` — fail closed). A leader
failover is *not* session loss: `NewLeaderEvent` swaps the ingress publication and keeps the session id.
See `doc/todo.md` "Gateway HA / multi-instance".

### SBE / FIX code generation
Three SBE schemas under `src/main/resources/`, each generating into a distinct namespace so one
include path covers all of them (`org.limitless.phixeron.{sbe.unsequenced, sbe.sequenced}`,
`org.limitless.phixeron.cluster.sbe`):
- `sbe-cluster.xml` — trimmed mirror of `io.aeron.cluster.codecs` (SessionConnectRequest,
  SessionEvent, SessionKeepAlive, …), replacing a hand-written `ClusterProtocol.hpp`. Its last
  section is decode-only — the consensus-module log entries (`TimerEvent`, `SessionOpenEvent`, …)
  that `SbeLogPrinter --schema cluster` reads out of a Raft-log recording; the client never sends
  them. Only this schema's Java side is IR-only (`generateClusterSbeIr`, no codecs).
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

### Order execution client — `OrderExecServer` (C++, under `src/main/cpp/.../order/OrderExecServer.cpp`)
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
goes through `ClusterSessionProxy` (`isAsync() = true`, in the shared `fixgateway` package) to cluster
ingress as an opaque `SessionProtocolMessage` (template 22, carrying pre-encoded FIX bytes), and reaches
the venue only when it comes back on the node tap, emitted by a `SessionWriter` at the `MsgSeqNum` the log
recorded. Inbound venue traffic is published too, so the log is a complete session record.

Four Artio 0.177 facts this depends on, each of which fails **silently** if got wrong:
- The writer must come from `FixLibrary.followerSession(...)`, **not** `sessionWriter(...)` — only the
  former is registered where Artio links it to the `Session`. An unlinked writer advances no
  `lastSentMsgSeqNum` and never fires `onSessionWriterLogout()`.
- That follower header needs its **comp-ids swapped** (the engine resolves it with `onAcceptLogon`, which
  reads local = `TargetCompID`; an initiator's key comes from `onInitiateLogon`). Get it wrong and the
  writer is registered under a different `sessionId`, never linked, and fills the local log while nothing
  reaches the venue.
- Binding and seeding happen in the `sessionAcquireHandler` (fires at **connect**), never on the `initiate`
  reply (completes only after logon) — the latter deadlocks.
- `sessionProxyFactory` is a **library** setting, so `FixEngine.close()` — which logs out every session the
  engine still owns — goes out through Artio's own `DirectSessionProxy`, straight to the wire, past the
  cluster. `ExchangeGateway.close()` drops the venue socket first (`detachFromVenue`) so the engine finds
  nothing logged on. Without it the venue counts a `MsgSeqNum` the log never recorded and the next instance
  to hold the session logs on one behind, forever.

The Aeron rule the sequencer section states — no cluster callback may signal failure by throwing — holds on
the **client** side too, and the gateway's fences depend on it: `Image.poll` hands any exception its fragment
handler raises to the Aeron error handler and advances the subscriber position anyway, so a throw from
`EgressListener.onSessionEvent` is swallowed and the process carries on. `ExchangeGateway` records the fault
and raises it from `doWork` (`checkClusterSession`), between polling the cluster and acting on what was
polled. It checks `isConnected()` as well as the recorded fault because `AeronCluster` also closes *itself*,
with no event at all, when a new leader does not arrive before its timeout — and an `ERROR` event, unlike
`CLOSED`, leaves the client open.

Two further fences run in the same place in `doWork`, ported from the C++ edge: the co-located tap
delivering no `Tick` for `TAP_STALL_TIMEOUT_MS` (20 tick periods) while caught up, and recovery dispatching
nothing for `RECOVERY_STALL_TIMEOUT_MS` (3x that) — the latter decided by `GatewayRecoveryStallPolicy`, a
port of the C++ class of the same name, so keep both files and both `GatewayRecoveryStallPolicyTest`s in
step. Both are fatal for the reason session loss is: everything this gateway decides reaches the venue only
by coming back off the tap, so a frozen view is a held session nothing is being written to — and the
keep-alive would go on holding the *cluster* session open, so the sequencer would never promote the standby.
The throw unwinds through `close()`, which drops the venue socket before releasing the cluster session, so
the standby is promoted against a venue that is already free. Neither arms until the first catch-up: a cold
start replays the whole log (no snapshots) and has no useful time bound.

`ReplayerStreamReceiver`/`ReplayerRecovery` (Java, `replayer/client/`) are faithful ports of the C++ classes of
the same names — same protocol, same walk/resume/retain state machine, same adapter/seam split. Keep the two
in step (all four files, and both `ReplayerRecoveryTest`s).

It is an **active/passive pair** (`EGW-A`/`EGW-B` under `gatewaySourceId` 5), the second logical gateway
alongside the client-facing one — see `doc/basicdata-design.md` §2 for the Gateway-table shape and the
per-`gatewaySourceId` election it needs from the sequencer. Identity is reference data, not config: the
`Gateway` row named by `PHIXERON_EXCHANGE_GATEWAY_NAME`, plus the one `BasicDataSession` row that
gateway's `gatewaySourceId` owns for the venue's comp-ids (everything else stays `PHIXERON_EXCHANGE_*`
deployment config). Session rows read **as the inbound message does** — `senderComp` is the counterparty,
`targetComp` is us — which is why one convention serves both edges: the acceptor keys its admit-filter on
`senderComp`, the initiator logs on as `targetComp` to `senderComp`, and the follower header's swapped
comp-ids are the row verbatim. The comp-ids therefore arrive mid-replay, so `followerSession` is requested
when the row lands rather than at start-up and `emit` waits for the reply (`awaitWriter`) rather than
dropping the frames behind that row in the same poll batch. The passive instance follows the tap and fills
its local Artio log through a `NO_CONNECTION_ID` follower writer, so promotion is a dial-out, not a
live-session hand-over. Session layer only — no order flow.

```bash
./gradlew mockExchange                     # FIX acceptor standing in for the venue (port 9010)
./gradlew exchangeGateway                  # the gateway itself (needs a running cluster)
src/test/scripts/exchange-gateway-test.sh  # all-Java e2e; no C++ build needed
```
The e2e purges Artio's own log dir alongside `purgelog.sh` — the two hold the same session's sequence
numbers, and purging one alone trips the gateway's "sent-sequence disagreement" check.

### Shared Artio-leg pieces — `org.limitless.phixeron.fixgateway`
The four classes that are genuinely direction-agnostic, and nothing else: `ClusterSessionProxy` (the Artio
`SessionProxy` that publishes instead of writing, built per session by both legs' `sessionProxyFactory`),
`SessionProtocolPublisher` (its seam onto a leg's `ClusterIngress`), `GatewayRecoveryStallPolicy` (the
recovery fence, a port of the C++ class of the same name — keep the two files and both tests in step) and
`FixtConfiguration`.

**Both Artio legs speak FIXT.1.1**, as the C++ edge does, and `FixtConfiguration` holds both halves of it
(`doc/artio-integration.md` §18): the dictionary (`artio-session-fixt-codecs`), which an acceptor takes as
`acceptorfixDictionary` and an initiator as `SessionConfiguration.fixDictionary` — **both legs set the
acceptor one**, because `followerSession` resolves the session context's dictionary from the follower
header's `BeginString` through that lookup, so the header encoder must come from the same dictionary — and
`DefaultApplVerID`(1137)=6, which FIXT.1.1 requires on the Logon and Artio never sets. 1137 has to be a
`SessionCustomisationStrategy` rather than a preset: `LogonEncoder.resetMessage()` clears it and the proxy
resets after every send.
**Everything leg-specific stays in its own package**: each leg has its own `ClusterIngress`,
`GatewayLifecycle` and `GatewayLifecycleActions`. The two `GatewayLifecycle`s share a name because they hold
the same role in each direction; the package tells them apart.

### Client-facing Artio FIX gateway — `OrderGateway` (Java, under `src/main/java/.../order/`)
The **inbound** leg: an Artio **acceptor** toward order-entry clients, the same proxy → cluster → tap →
`SessionWriter` loop as the venue leg with the direction and the *multiplicity* flipped. See
`doc/artio-integration.md` §17, which records what §16's costing got wrong.

**Session layer only, and additive.** No application message is decoded or routed, and the C++ `FixGateway`
is untouched and remains the production client edge. This runs as a **third logical gateway** —
`gatewaySourceId` 6, instances `OGW-A`/`OGW-B` under `PHIXERON_ORDER_GATEWAY_NAME`, listening on 9020 —
beside the C++ pair (sourceId 0) and the venue pair (sourceId 5), so sessions migrate one
`BasicDataSession` row at a time and the C++ edge retires when the last one moves.

**N sessions on one cluster session.** There is still one Aeron Cluster session per gateway process; client
connections are told apart by `header.connectionId`, which the composite header already carried — so this
leg needed **no schema change** in either direction. `connectionId`s are allocated from a counter that
resumes past the highest the replay held, and that resume point is `GatewayStarted.firstConnectionId`.
Sequence state (`ClientSessions`) is keyed on the comp-id pair, not the connection, because that is what a
FIX session is: a reconnect resumes rather than restarts.

Two Artio 0.177 facts on top of the venue leg's four, both silent if got wrong:
- **`initialAcceptedSessionOwner(SOLE_LIBRARY)` is required.** With the engine owning accepted sessions the
  Logon *reply* is composed engine-side and never reaches the proxy, so the first message of every session
  would go to the wire un-sequenced.
- **`sessionPersistenceStrategy(alwaysPersistent())` is required.** Artio's default for an acceptor is
  `alwaysTransient`: every logon resets the session's sequence numbers to 1 and the sent-sequence index is
  ignored. That silently undoes the whole standby mechanism — the follower writer fills the passive
  instance's Artio log correctly, and the promoted instance answers the client's Logon at 1 anyway, which
  the client refuses as `MSG_SEQ_NO_TOO_LOW`. The initiator leg gets this from
  `SessionConfiguration.sequenceNumbersPersistent(true)`; an acceptor's equivalent is an **engine** setting.

**The dial becomes an accept gate.** `bindAtStartup(false)` (set *after* `bindTo`, which defaults it true)
so a cold instance is bound to nothing; `FixEngine.bind()` on activation, after the `GatewayStarted` that
registers the instance — the order matters, since the sequencer releases every connection open under this
`gatewaySourceId` when that frame lands. Closing it is `unbind(false)` plus an explicit `requestDisconnect`
per session: **never `unbind(true)`**, which is Artio's end-of-day operation and logs every counterparty out
through the engine's own proxy, straight to the wire past the cluster.

A counterparty no `BasicDataSession` row names is **admitted and then logged out through the proxy**, on the
first message rather than in an `AuthenticationStrategy` — an engine-level rejection bypasses the proxy, so
it would reach the wire without ever being sequenced, breaking the invariant the C++ edge holds for every
client-facing refusal. Follower writers are requested off `ClientConnected` frames, identically on the
active instance and the standby, so promotion is a live-session takeover.

The three fences are the venue leg's, in the same place in `doWork` and fatal for the same reason: cluster
session lost, tap silent for `TAP_STALL_TIMEOUT_MS`, recovery dispatching nothing for
`RECOVERY_STALL_TIMEOUT_MS`. A fourth is new — an asynchronous `FixEngine.bind()` that fails leaves an
instance the cluster holds active with nothing listening, so `checkBind` raises it.

```bash
./gradlew orderGateway                  # the gateway itself (needs a running cluster)
./gradlew mockOrderClient -Pargs="OCLIENT PHIXERON 127.0.0.1:9020"
src/test/scripts/order-gateway-test.sh  # all-Java e2e; no C++ build needed
```
Like the venue leg's, the e2e purges Artio's log dirs — the gateways' and the clients' — alongside
`purgelog.sh`, or the two rebuild paths disagree.

### Reference-data gateway — `BasicDataServer` / `Gateways` (C++, under `src/main/cpp/.../basicdata/`)
Dual-role per-node process (`doc/basicdata-design.md`): on the **leader** it's a producer — reads
static reference data (FIX session comp-id pairs, gateway topology, the trading-day calendar;
currently hardcoded in `BasicDataConstants.hpp`, standing in for a real DB read) and publishes it as
ordinary sequenced `BasicData*` messages, an external-input adapter exactly like the FIX gateway is
for TCP. On **every node** it's a consumer — follows the co-located tap (like `OrderExecServer`
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
`todo.md` tracks known incomplete pieces (e.g. no matching engine — orders get a `New` ack and
nothing else; no pre-trade risk gating; no edge authentication; connection IDs aren't stable across
gateway restarts; only one in-flight resend per connection). Check it before assuming a code path is
complete.

`doc/` contains the deeper docs: `architecture-primer.md` (the short tour, written for readers
without a financial-systems background), `design.md` (the full current design), `fault-tolerance.md`,
`basicdata-design.md`, `gap.md` (FIXT.1.1 session-protocol coverage), `audit.md` (open findings),
`portability-linux.md` (what a RHEL 9/10 bring-up has to fix — the tree is macOS/clang-only today),
`cli-guide.md`/`ops.md`/`clusterctl.md` (runbooks). Two are **proposals, not descriptions of this
repo** — `artio-integration.md` and `fix-test-artio.md` — so cross-check them against the source
before trusting specifics.

## Code Formatting Mandate
- Explicitly respect all style, brace, and indentation configurations found in the local `.clang-format` file.
- Before completing an edit or creating a file, ensure it complies with our Clang-Format criteria.
- Keep code comments short and to the point. Do not explain design that is already documented.
- 
